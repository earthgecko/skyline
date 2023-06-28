"""
mirage_vortex.py
"""
import logging
from time import time, sleep, strftime, gmtime
from threading import Thread
from collections import defaultdict
from multiprocessing import Process, Queue
import traceback
import re
import json
import sys
import os
from ast import literal_eval
from math import ceil
import copy
import zlib
import gzip
import shutil

import settings
from skyline_functions import (
    write_data_to_file, send_anomalous_metric_to, mkdir_p, filesafe_metricname,
    get_redis_conn, get_redis_conn_decoded, nonNegativeDerivative,
)

from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.database.queries.insert_new_metric import insert_new_metric
from functions.database.queries.get_ionosphere_fp_ids_for_full_duration import get_ionosphere_fp_ids_for_full_duration
from functions.timeseries.determine_data_frequency import determine_data_frequency
from functions.timeseries.downsample import downsample_timeseries
from functions.timeseries.strictly_increasing_monotonicity import strictly_increasing_monotonicity

# @added 20220504 - Feature #2580: illuminance
from functions.illuminance.add_illuminance_entries import add_illuminance_entries

from functions.graphite.send_graphite_metric import send_graphite_metric
from custom_algorithms import run_custom_algorithm_on_timeseries

# from custom_algorithm_sources.sigma.sigma import run_sigma_algorithms

skyline_app = 'mirage_vortex'
# skyline_app_logger = '%sLog' % skyline_app
skyline_app_logger = 'mirageLog'
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

python_version = int(sys.version_info[0])

this_host = str(os.uname()[1])

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

try:
    MIRAGE_CHECK_REPETITIVE_DAILY_PEAKS = settings.MIRAGE_CHECK_REPETITIVE_DAILY_PEAKS
except:
    MIRAGE_CHECK_REPETITIVE_DAILY_PEAKS = True

try:
    VORTEX_ALGORITHMS = copy.deepcopy(settings.VORTEX_ALGORITHMS)
except Exception as outer_err:
    logger.error('error :: mirage_vortex CRITICAL error failed to load VORTEX_ALGORITHMS from settings - %s' % outer_err)
    sys.exit(1)

try:
    MIRAGE_VORTEX_DEFAULT_ALGORITHMS = copy.deepcopy(settings.VORTEX_ALGORITHMS['default'])
except:
    MIRAGE_VORTEX_DEFAULT_ALGORITHMS = {
        'sigma': {'sigma': 3, 'consensus': settings.MIRAGE_CONSENSUS},
        'spectral_residual': {},
        'consensus': [['sigma', 'spectral_residual']],
    }
try:
    MIRAGE_VORTEX_DEFAULT_CONSENSUS = settings.VORTEX_ALGORITHMS['default']['consensus']
except:
    MIRAGE_VORTEX_DEFAULT_CONSENSUS = [['sigma', 'spectral_residual']]

try:
    VORTEX_TIMESERIES_JSON_TO_DISK = settings.VORTEX_TIMESERIES_JSON_TO_DISK
except:
    VORTEX_TIMESERIES_JSON_TO_DISK = True
try:
    VORTEX_SAVE_RESULTS_FOR = settings.VORTEX_SAVE_RESULTS_FOR
except:
    VORTEX_SAVE_RESULTS_FOR = 86400

# Force downsampling which is requires to ensure speed and that features
# profiles are effective
try:
    VORTEX_FULL_DURATION_RESOLUTIONS = settings.VORTEX_FULL_DURATION_RESOLUTIONS
except:
    VORTEX_FULL_DURATION_RESOLUTIONS = {
        86400: 60,
        604800: 600,
    }

try:
    HORIZON_SHARDS = copy.deepcopy(settings.HORIZON_SHARDS)
except:
    HORIZON_SHARDS = {}
try:
    HORIZON_SHARD_DEBUG = settings.HORIZON_SHARD_DEBUG
except:
    HORIZON_SHARD_DEBUG = True

number_of_horizon_shards = 0
this_host = str(os.uname()[1])
HORIZON_SHARD = 0
if HORIZON_SHARDS:
    number_of_horizon_shards = len(HORIZON_SHARDS)
    HORIZON_SHARD = HORIZON_SHARDS[this_host]

ALLOWED_ALGORITHMS = list(VORTEX_ALGORITHMS.keys())

skyline_app_graphite_namespace = 'skyline.mirage%s.vortex' % (SERVER_METRIC_PATH)
failed_checks_dir = '%s_vortex_failed' % settings.MIRAGE_CHECK_PATH


class MirageVortex(Thread):
    """
    The MirageVortex thread
    """
    def __init__(self, parent_pid):
        """
        Initialize the MirageVortex
        """
        super().__init__()
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = os.getpid()
        self.mirage_vortex_exceptions_q = Queue()
        self.mirage_vortex_anomaly_breakdown_q = Queue()
        self.redis_conn = get_redis_conn(skyline_app)
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            os.kill(self.current_pid, 0)
            os.kill(self.parent_pid, 0)
        except:
            logger.warning('warning :: parent or current process dead')
            sys.exit(0)

    def check_valid_algorithms(self, consensus):
        """
        Verify that the algortihms are valid.
        """
        unknown_algorithms = []
        for item in consensus:
            if isinstance(item, list):
                for i_item in item:
                    if i_item not in ALLOWED_ALGORITHMS:
                        unknown_algorithms.append(i_item)
            else:
                if item not in ALLOWED_ALGORITHMS:
                    unknown_algorithms.append(item)
        return unknown_algorithms

    def check_consensus(self, consensus, triggered_algorithms, algorithms_run):
        """
        Check is consensus is achieved and still possible
        """
        consensus_impossible = False
        for item in consensus:
            consensus_reached = True
            for algo in item:
                if algo not in triggered_algorithms:
                    consensus_reached = False
            if consensus_reached:
                return item, consensus_impossible
        consensus_possibles = []
        for item in consensus:
            consensus_possible = True
            for algo in item:
                if algo in algorithms_run:
                    if not algorithms_run[algo]:
                        consensus_possible = False
                        break
            if consensus_possible:
                consensus_possibles.append(item)
        if len(consensus_possibles) == 0:
            consensus_impossible = True
        return [], consensus_impossible

    def create_echo_timeseries(self, vortex_metric_data, timeseries):
        """
        Add timeseries for echo
        """
        metric_data_dir = None
        use_base_name = vortex_metric_data['labelled_metric_name']
        metric_timestamp = vortex_metric_data['metric_timestamp']
        timeseries_dir = use_base_name.replace('.', '/')
        metric_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, metric_timestamp,
            timeseries_dir)
        if not os.path.exists(metric_data_dir):
            try:
                mkdir_p(metric_data_dir)
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to create dir - %s - %s' % (
                    metric_data_dir, err))
                return False

        echo_json = '%s/%s.mirage.redis.24h.json' % (metric_data_dir, use_base_name)
        echo_timeseries = list(timeseries)
        start_timestamp = metric_timestamp - 86400
        echo_timeseries = [item for item in timeseries if item[0] > start_timestamp]
        if vortex_metric_data['resolution'] < 60:
            try:
                echo_timeseries = downsample_timeseries('mirage', echo_timeseries, vortex_metric_data['resolution'], 60, 'mean', 'end')
            except Exception as err:
                logger.error('error :: mirage_vortex :: downsample_timeseries failed for echo data for request_id %s - %s' % (
                    vortex_metric_data['request_id'], err))
                return False
        # Convert the timeseries to json and save
        try:
            with open(echo_json, 'w') as f:
                f.write(json.dumps(echo_timeseries))
        except Exception as err:
            logger.error('error :: mirage_vortex :: failed to dump echo_timeseries to echo_json %s for request_id %s - %s' % (
                echo_json, vortex_metric_data['request_id'], err))
            return False
        logger.info('mirage_vortex :: dumped echo_timeseries to echo_json %s for request_id %s' % (
            echo_json, vortex_metric_data['request_id']))
        return echo_json

    def add_training_data(self, request_id, vortex_metric_data, timeseries, ionosphere_enabled):
        """
        Add training_data
        """

        metric_data_dir = None
        use_base_name = vortex_metric_data['internal_metric_name']
        labelled_metric_name = vortex_metric_data['labelled_metric_name']
        if labelled_metric_name:
            use_base_name = str(labelled_metric_name)
        metric_timestamp = vortex_metric_data['metric_timestamp']
        timeseries_dir = use_base_name.replace('.', '/')
        metric_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, metric_timestamp,
            timeseries_dir)
        anomaly_json = '%s/%s.json' % (metric_data_dir, use_base_name)
        check_file = '%s/%s.txt' % (metric_data_dir, use_base_name)

        logger.info('mirage_vortex :: saving data for request_id %s to %s' % (
            request_id, metric_data_dir))
        if not os.path.exists(metric_data_dir):
            try:
                mkdir_p(metric_data_dir)
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to create dir - %s - %s' % (
                    metric_data_dir, err))
                return False

        # if not ionosphere_enabled:
        # Add a metric vars file
        anomaly_data = None
        try:
            value = vortex_metric_data['value']
            from_timestamp = vortex_metric_data['from_timestamp']
            algorithms = list(vortex_metric_data['algorithms'].keys())
            triggered_algorithms = [i for i in list(vortex_metric_data['results']['triggered_algorithms'].keys()) if vortex_metric_data['results']['triggered_algorithms'][i]]
            full_duration = vortex_metric_data['nearest_full_duration']
            parent_id = 0
            algorithms_run = vortex_metric_data['results']['algorithms_run']
            anomaly_data = 'metric = \'%s\'\n' \
                           'value = \'%s\'\n' \
                           'from_timestamp = \'%s\'\n' \
                           'metric_timestamp = \'%s\'\n' \
                           'algorithms = %s\n' \
                           'triggered_algorithms = %s\n' \
                           'anomaly_dir = \'%s\'\n' \
                           'graphite_metric = False\n' \
                           'run_crucible_tests = False\n' \
                           'added_by = \'%s\'\n' \
                           'added_at = \'%s\'\n' \
                           'full_duration = \'%s\'\n' \
                           'ionosphere_parent_id = \'%s\'\n' \
                           'algorithms_run = %s\n' \
                % (str(use_base_name), str(value), str(from_timestamp),
                    str(vortex_metric_data['metric_timestamp']),
                    str(algorithms), str(triggered_algorithms),
                    metric_data_dir, 'mirage_vortex',
                    str(int(time())), str(full_duration),
                    str(parent_id), str(algorithms_run))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: mirage_vortex :: failed to create anomaly_data - %s' % (
                err))
            return False

        if anomaly_data:
            try:
                write_data_to_file('mirage', check_file, 'w', anomaly_data)
                logger.info('mirage_vortex :: added check_file - %s' % check_file)
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to add check_file %s - %s' % (
                    check_file, err))
                return False

        if VORTEX_TIMESERIES_JSON_TO_DISK and vortex_metric_data['downsampled']:
            jsonfile = None
            gzip_jsonfile = '%s/undownsampled.%s.json.gz' % (metric_data_dir, use_base_name)
            try:
                jsonfile = vortex_metric_data['timeseries']
            except Exception as err:
                logger.error('error :: mirage_vortex :: no timeseries (jsonfile) in vortex_metric_data - %s' % (
                    err))
            if os.path.isfile(jsonfile):
                try:
                    with open(jsonfile, 'rb') as f_in:
                        with gzip.open(gzip_jsonfile, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    logger.info('mirage_vortex :: added undownsampled timeseries - %s' % gzip_jsonfile)
                except Exception as err:
                    logger.error('error :: mirage_vortex :: failed to gzip jsonfile %s for request_id %s - %s' % (
                        jsonfile, request_id, err))

        try:
            if not isinstance(vortex_metric_data['timeseries'], str):
                del vortex_metric_data['timeseries']
        except:
            pass
        jsonfile = '%s/vortex.metric_data.%s.json' % (metric_data_dir, request_id)
        gzip_jsonfile = '%s.gz' % jsonfile
        try:
            with open(jsonfile, 'w') as fw:
                json.dump(vortex_metric_data, fw)
        except Exception as err:
            logger.error('error :: mirage_vortex :: failed to save metric_data for request_id %s to %s - %s' % (
                request_id, jsonfile, err))
            return False

        if os.path.isfile(jsonfile):
            try:
                with open(jsonfile, 'rb') as f_in:
                    with gzip.open(gzip_jsonfile, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to gzip jsonfile %s for request_id %s - %s' % (
                    jsonfile, request_id, err))
        if os.path.isfile(gzip_jsonfile):
            try:
                os.remove(jsonfile)
            except OSError:
                logger.error(
                    'error :: listen :: mirage_vortex - failed to remove file - %s' % jsonfile)
        # Convert the timeseries to json and save
        try:
            with open(anomaly_json, 'w') as f:
                f.write(json.dumps(timeseries))
        except Exception as err:
            logger.error('error :: mirage_vortex :: failed to dump timeseries to anomaly_json %s for request_id %s - %s' % (
                anomaly_json, request_id, err))
            return False

        redis_set = 'ionosphere.training_data'
        data = [use_base_name, int(metric_timestamp), vortex_metric_data['nearest_full_duration']]
        try:
            logger.info('adding to Redis set %s - %s' % (
                redis_set, str(data)))
            self.redis_conn.sadd(redis_set, str(data))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: mirage_vortex :: failed to add %s to %s Redis set' % (str(data), redis_set))

        return metric_data_dir

    def add_results(self, request_id, metric_data, analysis_start_time):
        """
        Add result to the mirage.vortex Redis hash
        """
        added_results = 0

        request_id_elements = request_id.split('.')
        request_id_timestamp = request_id_elements[0]
        request_id_time = '.'.join(request_id_elements[0:2])
        request_id_timestamp_aligned = int(request_id_timestamp) // 3600 * 3600

        vortex_save_path = None
        if VORTEX_SAVE_RESULTS_FOR:
            save_path = '%s/%s' % (str(request_id_timestamp_aligned), str(request_id_time))
            vortex_save_path = '%s/flux/vortex/results/%s/%s' % (
                settings.SKYLINE_DIR, str(request_id_timestamp_aligned),
                str(request_id_time))

        if vortex_save_path:
            logger.info('mirage_vortex :: saving data for request_id %s to %s' % (
                request_id, vortex_save_path))
            if not os.path.exists(vortex_save_path):
                try:
                    mkdir_p(vortex_save_path)
                except Exception as err:
                    logger.error('error :: flux :: failed to create dir - %s - %s' % (
                        vortex_save_path, err))
            jsonfile = '%s/vortex.%s.json' % (vortex_save_path, request_id)
            gzip_jsonfile = '%s.gz' % jsonfile
            try:
                with open(jsonfile, 'w') as fw:
                    json.dump(metric_data, fw)
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to save metric_data for request_id %s to %s - %s' % (
                    request_id, jsonfile, err))

            if os.path.isfile(jsonfile):
                try:
                    with open(jsonfile, 'rb') as f_in:
                        with gzip.open(gzip_jsonfile, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                except Exception as err:
                    logger.error('error :: mirage_vortex :: failed to gzip jsonfile %s for request_id %s - %s' % (
                        jsonfile, request_id, err))
            if os.path.isfile(gzip_jsonfile):
                try:
                    os.remove(jsonfile)
                except OSError:
                    logger.error('error :: mirage_vortex - failed to remove file - %s' % jsonfile)
            try:
                added_save = self.redis_conn_decoded.hset('mirage.vortex_saved', request_id, save_path)
                if added_save:
                    logger.info('mirage_vortex :: added key for %s to mirage.vortex_saved' % (
                        request_id))
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to hset key for %s in mirage.vortex_saved - %s' % (
                    request_id, err))

        if VORTEX_TIMESERIES_JSON_TO_DISK:
            jsonfile = None
            try:
                jsonfile = metric_data['timeseries']
            except Exception as err:
                logger.error('error :: mirage_vortex :: no timeseries in metric_data for %s' % (
                    request_id))
            if os.path.isfile(jsonfile):
                try:
                    os.remove(jsonfile)
                except OSError:
                    logger.error('error :: mirage_vortex - failed to remove file - %s' % jsonfile)
        try:
            del metric_data['timeseries']
        except:
            pass
        analysis_runtime = time() - analysis_start_time
        metric_data['results']['total_analysis_runtime'] = analysis_runtime

        # Remove results AFTER saving if return_results is not declared
        for algorithm in list(metric_data['algorithms'].keys()):
            return_results = False
            try:
                return_results = metric_data['algorithms'][algorithm]['algorithm_parameters']['return_results']
            except:
                pass
            if return_results:
                continue
            try:
                del metric_data['results']['algorithms'][algorithm]['anomalies']
            except:
                pass
            try:
                del metric_data['results']['algorithms'][algorithm]['scores']
            except:
                pass

        if metric_data['source_app'] == 'flux':
            try:
                added_results = self.redis_conn_decoded.hset('mirage.vortex', request_id, str(metric_data))
                if added_results:
                    logger.info('mirage_vortex :: added results for %s to mirage.vortex, analysis_runtime: %s' % (
                        request_id, str(analysis_runtime)))
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to hest results for %s to mirage.vortex - %s' % (
                    request_id, err))

        return added_results

    def return_shard_test(self, request_id, metric_data, analysis_start_time):
        """
        Return a shard test.
        """
        logger.info('mirage_vortex :: SHARD_TEST returning results')
        metric_data['anomalous'] = None
        metric_data['results']['success'] = True
        added_results = self.add_results(request_id, metric_data, analysis_start_time)
        if added_results:
            logger.info('mirage_vortex :: processed request_id: %s' % str(request_id))
        return added_results

    def shard_host(self, metric, check_horizon_shards):
        """
        Return the shard host to which a metric belongs.
        """
        if not check_horizon_shards:
            return HORIZON_SHARD
        metric_as_bytes = str(metric).encode()
        value = zlib.adler32(metric_as_bytes)
        modulo_result = value % len(check_horizon_shards)
        for shost in check_horizon_shards:
            if modulo_result == check_horizon_shards[shost]:
                return shost

    def is_labelled_metric(self, metric):
        """
        Check whether a metric is a labelled_metric
        """
        labelled_metric = False
        if '{' in metric:
            if metric.endswith('}'):
                labelled_metric = True
        return labelled_metric

    def generate_labelled_metric_name(self, metric, metric_namespace_prefix, server_id=None):
        if server_id:
            server_id_str = str(server_id)
        else:
            server_id_str = '1'
        if metric_namespace_prefix:
            skyline_labels = '{_tenant_id="%s",_server_id="%s",' % (str(metric_namespace_prefix), server_id_str)
            labelled_metric_name = metric.replace('{', skyline_labels)
        return labelled_metric_name

    def get_metric_data_from_archive(self, request_id, metric, metric_timestamp):
        metric_data = {}
        try:
            timeseries_dir = metric.replace('.', '/')
            metric_data_dir = '%s/%s/%s' % (
                settings.IONOSPHERE_DATA_FOLDER, metric_timestamp,
                timeseries_dir)
            metric_data_archive = '%s/vortex.metric_data.%s.json.gz' % (metric_data_dir, request_id)
            with gzip.open(metric_data_archive, 'rb') as f:
                file_content = f.read()
            metric_data = json.loads(file_content)
        except Exception as err:
            logger.error('error :: mirage_vortex :: failed ungzip and load metric_data from %s - %s' % (
                metric_data_archive, err))
        return metric_data

    def process_ionosphere_results(self, run_timestamp, ionosphere_results):
        """
        Process results from Ionosphere.
        """
        processed_results = []
        all_ionosphere_results = {}
        try:
            all_ionosphere_results = self.redis_conn_decoded.hgetall('ionosphere.vortex_results')
        except Exception as err:
            logger.error('error :: mirage_vortex :: failed to hgetall ionosphere.vortex_results - %s' % (
                err))
        logger.info('mirage_vortex :: ionosphere.vortex_results has %s items' % (
            str(len(all_ionosphere_results))))

        mirage_vortex_sent_to_ionosphere = {}
        try:
            mirage_vortex_sent_to_ionosphere = self.redis_conn_decoded.hgetall('mirage_vortex.sent_to_ionosphere')
        except Exception as err:
            logger.error('error :: mirage_vortex :: failed to hgetall ionosphere.vortex_results - %s' % (
                err))

        for i_result in ionosphere_results:
            results_dict = {}
            try:
                results_dict_str = all_ionosphere_results[i_result]
                if results_dict_str:
                    results_dict = literal_eval(results_dict_str)
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to literal_eval %s in all_ionosphere_results - %s' % (
                    str(i_result), err))
            try:
                self.redis_conn_decoded.hdel('ionosphere.vortex_results', i_result)
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to hdel %s from ionosphere.vortex_results - %s' % (
                    str(i_result), err))
            if not results_dict:
                logger.error('error :: mirage_vortex :: not results_dict for %s' % (
                    str(i_result)))
                continue
            try:
                metric = results_dict['metric']
                anomalous = results_dict['anomalous']
                metric_timestamp = results_dict['metric_timestamp']
                matched = results_dict['matched']
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to interpolate data from %s results_dict - %s' % (
                    str(i_result), err))
                continue

            sent_dict = {}
            for request_id in list(mirage_vortex_sent_to_ionosphere.keys()):
                sent_dict_str = None
                try:
                    sent_dict_str = mirage_vortex_sent_to_ionosphere[request_id]
                    if sent_dict_str:
                        i_sent_dict = literal_eval(sent_dict_str)
                    if metric == i_sent_dict['metric']:
                        if int(metric_timestamp) == int(i_sent_dict['timestamp']):
                            sent_dict = copy.deepcopy(i_sent_dict)
                            break
                except Exception as err:
                    logger.error('error :: mirage_vortex :: error iterating %s from mirage_vortex_sent_to_ionosphere - %s' % (
                        str(i_result), err))
            if not sent_dict:
                logger.error('error :: mirage_vortex :: failed to find metric: %s and metric_timestamp: %s in any item in mirage_vortex.sent_to_ionosphere, cannot reconcile' % (
                    str(metric)))
                continue
            try:
                request_id = sent_dict['request_id']
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to determine request_id for %s from sent_dict - %s' % (
                    str(i_result), err))
                continue

            metric_data = {}
            try:
                metric_data = self.get_metric_data_from_archive(request_id, metric, metric_timestamp)
            except Exception as err:
                logger.error('error :: mirage_vortex :: get_metric_data_from_archive failed for %s - %s' % (
                    request_id, err))
                continue

            added_results = False

            if metric_data:
                metric_data['matched'] = matched
                metric_data['anomalous'] = anomalous
                metric_data['results']['anomalous'] = anomalous
                metric_data['results']['matched'] = matched
                # Resave the metric_data_archive with the Ionosphere results
                timeseries_dir = metric.replace('.', '/')
                metric_data_dir = '%s/%s/%s' % (
                    settings.IONOSPHERE_DATA_FOLDER, metric_timestamp,
                    timeseries_dir)
                jsonfile = '%s/vortex.metric_data.%s.json' % (metric_data_dir, request_id)
                gzip_jsonfile = '%s.gz' % jsonfile
                try:
                    with open(jsonfile, 'w') as fw:
                        json.dump(metric_data, fw)
                except Exception as err:
                    logger.error('error :: mirage_vortex :: process_ionosphere_results - failed to save metric_data for request_id %s to %s - %s' % (
                        request_id, jsonfile, err))
                if os.path.isfile(jsonfile):
                    try:
                        with open(jsonfile, 'rb') as f_in:
                            with gzip.open(gzip_jsonfile, 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                    except Exception as err:
                        logger.error('error :: mirage_vortex :: process_ionosphere_results - failed to gzip jsonfile %s for request_id %s - %s' % (
                            jsonfile, request_id, err))
                if os.path.isfile(gzip_jsonfile):
                    try:
                        os.remove(jsonfile)
                    except OSError:
                        logger.error('error :: mirage_vortex - process_ionosphere_results - failed to remove file - %s' % jsonfile)
                try:
                    analysis_start_time = metric_data['begin_analysis']
                    added_results = self.add_results(request_id, metric_data, analysis_start_time)
                except Exception as err:
                    logger.error('error :: mirage_vortex :: add_results failed for request_id: %s - %s' % (
                        request_id, err))

            if added_results:
                logger.info('mirage_vortex :: results added with Ionosphere results of anomalous: %s, matched: %s and removed %s from mirage_vortex.sent_to_ionosphere' % (
                    str(anomalous), str(matched), request_id))
                try:
                    self.redis_conn_decoded.hdel('mirage_vortex.sent_to_ionosphere', request_id)
                except Exception as err:
                    logger.error('error :: mirage_vortex :: failed to hdel %s from mirage_vortex.sent_to_ionosphere - %s' % (
                        request_id, err))
            if anomalous:
                logger.info('mirage_vortex :: anomaly detected - %s' % metric)
                try:
                    value = metric_data['value']
                    triggered_algorithms = [i for i in list(metric_data['results']['triggered_algorithms'].keys()) if metric_data['results']['triggered_algorithms'][i]]
                    current_illuminance_dict = self.add_to_illuminance(run_timestamp, metric, metric_timestamp, value, triggered_algorithms)
                except Exception as err:
                    logger.error('error :: mirage_vortex :: add_illuminance_entries failed - %s' % (
                        err))
                if current_illuminance_dict:
                    logger.info('mirage_vortex :: added %s anomaly to illuminance' % metric)

                logger.info('mirage_vortex :: adding %s to mirage_vortex.anomalous_metrics to be alerted on by mirage' % metric)
                added_to_mirage_vortex_anomalous_metrics = False
                try:
                    added_to_mirage_vortex_anomalous_metrics = self.add_to_mirage_vortex_anomalous_metrics(metric_data)
                except Exception as err:
                    logger.error('error :: mirage_vortex :: failed to hdel %s from mirage_vortex.sent_to_ionosphere - %s' % (
                        request_id, err))
                logger.info('mirage_vortex :: added mirage_vortex.anomalous_metrics: %s' % str(added_to_mirage_vortex_anomalous_metrics))

            processed_results.append(i_result)
        return processed_results

    def return_request_results(self, run_timestamp, return_results_for):
        """
        Assign a metrics for a process to analyze.
        """
        processed_results = []

        for request_id in list(return_results_for.keys()):
            sent_dict = {}
            try:
                sent_dict_str = return_results_for[request_id]
                if sent_dict_str:
                    sent_dict = literal_eval(sent_dict_str)
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to literal_eval %s in return_results_for - %s' % (
                    request_id, err))
            try:
                self.redis_conn_decoded.hdel('mirage_vortex.sent_to_ionosphere', request_id)
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to hdel %s from imirage_vortex.sent_to_ionosphere - %s' % (
                    request_id, err))
            if not sent_dict:
                logger.error('error :: mirage_vortex :: no sent_dict for %s' % (
                    str(request_id)))
                continue
            try:
                request_id = sent_dict['request_id']
                metric = sent_dict['metric']
                metric_timestamp = sent_dict['timestamp']
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to interpolate data from %s results_dict - %s' % (
                    str(request_id), err))
                continue

            metric_data = {}
            try:
                metric_data = self.get_metric_data_from_archive(request_id, metric, metric_timestamp)
            except Exception as err:
                logger.error('error :: mirage_vortex :: get_metric_data_from_archive failed for %s - %s' % (
                    request_id, err))
                continue
            anomalous = False
            try:
                anomalous = metric_data['results']['anomalous']
            except KeyError:
                anomalous = metric_data['anomalous']
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to determine if %s is anomalous from metric_data - %s' % (
                    str(metric), err))

            added_results = False
            metric_data['matched'] = None
            metric_data['results']['matched'] = None
            try:
                analysis_start_time = metric_data['begin_analysis']
                added_results = self.add_results(request_id, metric_data, analysis_start_time)
            except Exception as err:
                logger.error('error :: mirage_vortex :: add_results failed for request_id: %s - %s' % (
                    request_id, err))
            if added_results:
                logger.info('mirage_vortex :: results added with anomalous: %s, matched: None' % (
                    str(anomalous)))
            if anomalous:
                logger.info('mirage_vortex :: anomaly detected - %s' % metric)

                if settings.PANORAMA_ENABLED:
                    send_to_panorama = True
                if send_to_panorama:
                    try:
                        triggered_algorithms = []
                        value = metric_data['value']
                        from_timestamp = metric_data['from_timestamp']
                        algorithms_run = metric_data['results']['algorithms_run']
                        triggered_algorithms = [i for i in list(metric_data['results']['triggered_algorithms'].keys()) if metric_data['results']['triggered_algorithms'][i]]
                        sent_to_panorama = self.send_anomaly_to_panorama(
                            metric, value, from_timestamp,
                            metric_timestamp, algorithms_run, triggered_algorithms)
                    except Exception as err:
                        logger.error('error :: mirage_vortex :: send_anomaly_to_panorama failed - %s' % (
                            err))
                    if sent_to_panorama:
                        logger.info('mirage_vortex :: sent %s anomaly to panorama' % metric)
                current_illuminance_dict = {}
                try:
                    current_illuminance_dict = self.add_to_illuminance(run_timestamp, metric, metric_timestamp, value, triggered_algorithms)
                except Exception as err:
                    logger.error('error :: mirage_vortex :: add_illuminance_entries failed - %s' % (
                        err))
                if current_illuminance_dict:
                    logger.info('mirage_vortex :: added %s anomaly to illuminance' % metric)
                logger.info('mirage_vortex :: adding %s to mirage_vortex.anomalous_metrics to be alerted on by mirage' % metric)
                added_to_mirage_vortex_anomalous_metrics = False
                try:
                    added_to_mirage_vortex_anomalous_metrics = self.add_to_mirage_vortex_anomalous_metrics(metric_data)
                except Exception as err:
                    logger.error('error :: mirage_vortex :: failed to hdel %s from mirage_vortex.sent_to_ionosphere - %s' % (
                        request_id, err))
                logger.info('mirage_vortex :: added mirage_vortex.anomalous_metrics: %s' % str(added_to_mirage_vortex_anomalous_metrics))

        return processed_results

    def add_to_mirage_vortex_anomalous_metrics(self, metric_data):
        added = False
        try:
            triggered_algorithms = [algo for algo in list(metric_data['results']['triggered_algorithms'].keys()) if metric_data['results']['triggered_algorithms'][algo]]
            anomalous_metric = [
                float(metric_data['value']), str(metric_data['internal_metric_name']),
                metric_data['metric_timestamp'], metric_data['nearest_full_duration'],
                triggered_algorithms, metric_data['results']['algorithms_run']]
            redis_set = 'mirage_vortex.anomalous_metrics'
            data = str(anomalous_metric)
            try:
                self.redis_conn_decoded.sadd(redis_set, data)
                added = True
            except Exception as err:
                logger.error('error :: failed to add %s to mirage_vortex.anomalous_metrics Redis set - %s' % (
                    str(data), err))
        except Exception as err:
            logger.error('error :: failed to interpolate metric_data to add to mirage_vortex.anomalous_metrics - %s' % (
                err))
        return added

    def add_to_illuminance(self, run_timestamp, labelled_metric_name, metric_timestamp, value, triggered_algorithms):
        current_illuminance_dict = {}
        illuminance_dict = {}
        try:
            illuminance_dict[labelled_metric_name] = {
                'timestamp': int(metric_timestamp),
                'value': float(value),
                'triggered_algorithms_count': len(triggered_algorithms)}
            logger.info('mirage_vortex :: calling add_illuminance_entries with %s entries to add' % (
                str(len(illuminance_dict))))
            try:
                current_illuminance_dict = add_illuminance_entries(self, skyline_app, int(run_timestamp), illuminance_dict)
            except Exception as err:
                logger.error('error :: mirage_vortex :: add_illuminance_entries failed - %s' % (
                    err))
            logger.info('mirage_vortex :: illuminance Redis hash now has %s entries' % (
                str(len(current_illuminance_dict))))
        except Exception as err:
            logger.error('error :: mirage_vortex :: add_to_illuminance failed - %s' % (
                err))
        return current_illuminance_dict

    def send_anomaly_to_panorama(
        self, labelled_metric_name, value, from_timestamp, metric_timestamp, algorithms_run,
            triggered_algorithms):

        sent_to_panorama = False
        send_to_panorama = False
        if settings.PANORAMA_ENABLED:
            send_to_panorama = True
        if send_to_panorama:
            if not os.path.exists(settings.PANORAMA_CHECK_PATH):
                mkdir_p(settings.PANORAMA_CHECK_PATH)
            # Note:
            # The values are enclosed is single quoted intentionally
            # as the imp.load_source used results in a shift in the
            # decimal position when double quoted, e.g.
            # value = "5622.0" gets imported as
            # 2016-03-02 12:53:26 :: 28569 :: metric variable - value - 562.2
            # single quoting results in the desired,
            # 2016-03-02 13:16:17 :: 1515 :: metric variable - value - 5622.0
            source = 'vortex'
            added_at = int(time())
            panoroma_anomaly_data = 'metric = \'%s\'\n' \
                                    'value = \'%s\'\n' \
                                    'from_timestamp = \'%s\'\n' \
                                    'metric_timestamp = \'%s\'\n' \
                                    'algorithms = %s\n' \
                                    'triggered_algorithms = %s\n' \
                                    'app = \'%s\'\n' \
                                    'source = \'%s\'\n' \
                                    'added_by = \'%s\'\n' \
                                    'added_at = \'%s\'\n' \
                % (labelled_metric_name, str(value), str(from_timestamp),
                   str(metric_timestamp), str(algorithms_run),
                   triggered_algorithms, 'vortex', source, this_host,
                   str(added_at))
            # Create an anomaly file with details about the anomaly
            sane_metricname = filesafe_metricname(str(labelled_metric_name))
            panoroma_anomaly_file = '%s/%s.%s.txt' % (
                settings.PANORAMA_CHECK_PATH, added_at, sane_metricname)
            try:
                write_data_to_file(
                    'mirage', panoroma_anomaly_file, 'w',
                    panoroma_anomaly_data)
                logger.info('mirage_vortex :: added panorama anomaly file :: %s' % (panoroma_anomaly_file))
                sent_to_panorama = True
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to add panorama anomaly file - %s - %s' % (
                    panoroma_anomaly_file, err))
            redis_set = 'mirage.sent_to_panorama'
            data = str(labelled_metric_name)
            try:
                self.redis_conn.sadd(redis_set, data)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_vortex :: failed to add %s to Redis set %s' % (
                    str(data), str(redis_set)))
        return sent_to_panorama

    def spin_process(self, i, run_timestamp, assigned_checks, ionosphere_results, return_results_for):
        """
        Assign a metrics or results to process.
        """

        if not assigned_checks and not ionosphere_results and not return_results_for:
            logger.info('mirage_vortex :: no checks to assign or results to process, nothing to do')
            return

        process_start_timestamp = int(time())

        now = int(process_start_timestamp)

        if return_results_for:
            logger.info('mirage_vortex :: returning %s results' % str(len(return_results_for)))
            processed_results = []
            try:
                processed_results = self.return_request_results(run_timestamp, return_results_for)
            except Exception as err:
                logger.error('error :: mirage_vortex :: return_request_results failed - %s' % (
                    err))
            logger.info('mirage_vortex :: returned %s results' % str(len(processed_results)))
            return

        if ionosphere_results:
            logger.info('mirage_vortex :: %s results from ionosphere to process' % str(len(ionosphere_results)))
            processed_results = []
            try:
                processed_results = self.process_ionosphere_results(run_timestamp, ionosphere_results)
            except Exception as err:
                logger.error('error :: mirage_vortex :: process_ionosphere_results failed - %s' % (
                    err))
            logger.info('mirage_vortex :: processed and returned %s results from ionosphere' % str(len(processed_results)))
            return

        redis_metrics_processed_key = 'mirage_vortex.%s.metrics_processed' % str(i)
        try:
            exists = self.redis_conn_decoded.exists(redis_metrics_processed_key)
            if exists:
                last_redis_metrics_processed_key = 'mirage_vortex.%s.metrics_processed.last' % str(i)
                self.redis_conn_decoded.rename(redis_metrics_processed_key, last_redis_metrics_processed_key)
        except Exception as err:
            logger.error('error :: mirage_vortex :: failed to rename %s Redis hash - %s' % (
                redis_metrics_processed_key, err))

        checks_processed = 0

        ionosphere_unique_metrics = []

        for check_item in assigned_checks:
            if int(time()) >= (process_start_timestamp + 50):
                logger.info('mirage_vortex :: run time limit reached - stopping')
                break

            checks_processed += 1
            analysis_start_time = time()

            results = {
                'success': False,
                'algorithms': {}
            }

            check_str = None
            try:
                check_str = self.redis_conn_decoded.hget('flux.vortex', check_item)
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to hget %s from flux.vortex - %s' % (
                    check_item, err))
            if check_str:
                try:
                    self.redis_conn_decoded.hdel('flux.vortex', check_item)
                except Exception as err:
                    logger.error('error :: mirage_vortex :: failed to hdel %s from flux.vortex - %s' % (
                        check_item, err))

            metric_data = {}
            if check_str:
                try:
                    metric_data = literal_eval(check_str)
                except Exception as err:
                    logger.error('error :: mirage_vortex :: failed to literal_eval data for %s - %s' % (
                        check_item, err))
            if not metric_data:
                logger.error('error :: mirage_vortex :: no metric_data for %s' % (
                    check_item))
                continue

            request_id = check_item

            try:
                del metric_data['key']
            except:
                pass

            # The source_app can be flux, mirage or luminosity
            try:
                source_app = metric_data['source_app']
            except:
                source_app = 'flux'

            metric_data['begin_analysis'] = time()
            metric_data['status_code'] = 200

            logger.info('mirage_vortex :: processing request_id: %s' % str(request_id))

            consensus = list(MIRAGE_VORTEX_DEFAULT_CONSENSUS)
            algorithms = {'default': {}}
            try:
                algorithms = metric_data['algorithms']
            except:
                algorithms = {'default': {}}

            if len(list(algorithms.keys())) == 1:
                if list(algorithms.keys())[0] == 'default':
                    logger.info('mirage_vortex :: using default algorithms for request_id: %s' % str(request_id))
                    algorithms = copy.deepcopy(MIRAGE_VORTEX_DEFAULT_ALGORITHMS)
            algorithms_to_be_run = list(algorithms.keys())

            if 'test_anomaly' in list(metric_data.keys()):
                if metric_data['test_anomaly']:
                    logger.info('mirage_vortex :: test_anomaly request for request_id: %s' % str(request_id))
                    metric_data['anomalous'] = True
                    results = {
                        'success': True,
                        'anomalous': True,
                        'additional info': 'TEST ANOMALY ONLY - no analysis done',
                        'algorithms': {}
                    }
                    for algorithm in algorithms:
                        results['algorithms'][algorithm] = True
                    metric_data['results'] = results
                    added_results = self.add_results(request_id, metric_data, analysis_start_time)
                    if added_results:
                        logger.info('mirage_vortex :: processed test_anomaly request_id: %s' % str(request_id))
                    continue

            try:
                consensus = list(metric_data['consensus'])
            except:
                consensus = list(MIRAGE_VORTEX_DEFAULT_CONSENSUS)
            for item in consensus:
                if isinstance(item, str):
                    consensus.remove(item)
                    consensus.append([item])

            # @added 20230616 - Feature #4952: vortex - consensus_count
            try:
                consensus_count = int(metric_data['consensus_count'])
            except:
                consensus_count = 0

            triggered_algorithms = []

            unknown_algorithms = []
            try:
                unknown_algorithms = self.check_valid_algorithms(consensus)
            except:
                unknown_algorithms = []
            if unknown_algorithms:
                metric_data['anomalous'] = None
                additional_info = 'unknown algorithm values passed - %s' % str(unknown_algorithms)
                logger.warning('warning :: mirage_vortex :: not processing request_id: %s, %s' % (
                    str(request_id), additional_info))
                results = {
                    'success': False,
                    'anomalous': None,
                    'additional info': additional_info,
                    'algorithms': {}
                }
                metric_data['results'] = results
                metric_data['error'] = additional_info
                metric_data['success'] = False
                metric_data['status_code'] = 400
                added_results = self.add_results(request_id, metric_data, analysis_start_time)
                if added_results:
                    logger.info('mirage_vortex :: processed request_id: %s' % str(request_id))
                continue

            unknown_algorithms = []
            try:
                unknown_algorithms = self.check_valid_algorithms(algorithms_to_be_run)
            except:
                unknown_algorithms = []
            if unknown_algorithms:
                metric_data['anomalous'] = None
                additional_info = 'unknown algorithm values passed - %s' % str(unknown_algorithms)
                logger.warning('warning :: mirage_vortex :: not processing request_id: %s, %s' % (
                    str(request_id), additional_info))
                results = {
                    'success': False,
                    'anomalous': None,
                    'additional info': additional_info,
                    'algorithms': {}
                }
                metric_data['results'] = results
                metric_data['error'] = additional_info
                metric_data['success'] = False
                metric_data['status_code'] = 400
                added_results = self.add_results(request_id, metric_data, analysis_start_time)
                if added_results:
                    logger.info('mirage_vortex :: processed request_id: %s' % str(request_id))
                continue

            # Check that the algorithms_to_be_run are in consensus
            in_consensus = False
            for consensus_item in consensus:
                for item in consensus_item:
                    if item in algorithms_to_be_run:
                        in_consensus = True
            if not in_consensus:
                metric_data['anomalous'] = None
                additional_info = 'input error - algorithms passed do not match any algorithms in consensus no consensus could ever be achieved'
                logger.warning('warning :: mirage_vortex :: not processing request_id: %s, %s' % (
                    str(request_id), additional_info))
                results = {
                    'success': False,
                    'anomalous': None,
                    'additional info': additional_info,
                    'algorithms': {}
                }
                metric_data['results'] = results
                metric_data['error'] = additional_info
                metric_data['success'] = False
                metric_data['status_code'] = 400
                added_results = self.add_results(request_id, metric_data, analysis_start_time)
                if added_results:
                    logger.info('mirage_vortex :: processed request_id: %s' % str(request_id))
                continue

            try:
                metric = metric_data['metric']
            except Exception as err:
                logger.error('error :: mirage_vortex :: no metric in metric_data for %s' % (
                    check_item))
                metric_data['anomalous'] = False
                additional_info = 'no metric name passed'
                results = {
                    'success': False,
                    'anomalous': False,
                    'additional info': additional_info,
                    'algorithms': {}
                }
                for algorithm in algorithms:
                    results['algorithms'][algorithm] = None
                metric_data['results'] = results
                metric_data['error'] = additional_info
                metric_data['success'] = False
                metric_data['status_code'] = 400
                added_results = self.add_results(request_id, metric_data, analysis_start_time)
                if added_results:
                    logger.info('mirage_vortex :: processed request_id: %s' % str(request_id))
                continue

            timeseries = []
            if VORTEX_TIMESERIES_JSON_TO_DISK:
                jsonfile = None
                try:
                    jsonfile = metric_data['timeseries']
                except Exception as err:
                    logger.error('error :: mirage_vortex :: no timeseries in metric_data for %s' % (
                        check_item))
                file_metric_data = {}
                try:
                    with open(jsonfile) as fh:
                        file_metric_data = json.load(fh)
                except Exception as err:
                    logger.error('error :: mirage_vortex :: failed to load file_metric_data from %s for %s - %s' % (
                        jsonfile, request_id, err))
                try:
                    timeseries = file_metric_data['timeseries']
                    logger.info('mirage_vortex :: loaded timeseries from %s' % jsonfile)
                except Exception as err:
                    logger.error('error :: mirage_vortex :: no timeseries in file_metric_data for %s- %s' % (
                        check_item, err))
            else:
                try:
                    timeseries = metric_data['timeseries']
                except Exception as err:
                    logger.error('error :: mirage_vortex :: no timeseries in metric_data for %s' % (
                        check_item))

            if isinstance(timeseries, dict):
                new_timeseries = []
                for ts in list(timeseries.keys()):
                    value = None
                    try:
                        value = float(timeseries[ts])
                    except:
                        continue
                    if isinstance(value, float):
                        try:
                            new_timeseries.append([int(ts), timeseries[ts]])
                        except:
                            continue
                timeseries = list(new_timeseries)
                del new_timeseries

            if not timeseries:
                logger.warning('warning :: mirage_vortex :: no timeseries in metric_data for %s' % (
                    check_item))
                metric_data['anomalous'] = False
                additional_info = 'no timeseries data found'
                results = {
                    'success': False,
                    'anomalous': False,
                    'additional info': additional_info,
                    'algorithms': {}
                }
                for algorithm in algorithms:
                    results['algorithms'][algorithm] = None
                metric_data['results'] = results
                metric_data['error'] = additional_info
                metric_data['success'] = False
                metric_data['status_code'] = 400
                added_results = self.add_results(request_id, metric_data, analysis_start_time)
                if added_results:
                    logger.info('mirage_vortex :: processed request_id: %s' % str(request_id))
                continue

            shard_test = False
            try:
                shard_test = metric_data['shard_test']
            except:
                shard_test = False
            if shard_test:
                try:
                    TEST_HORIZON_SHARDS = {
                        this_host: 0,
                        'another-test-node-1': 1,
                        'another-test-node-2': 2,
                    }
                    shost = self.shard_host(metric, TEST_HORIZON_SHARDS)
                    metric_data['results'] = {}
                    metric_data['results']['shard_test'] = shard_test
                    metric_data['results']['shard_host'] = shost
                    metric_data['results']['processing_shard'] = TEST_HORIZON_SHARDS[shost]
                    shard_test_done = self.return_shard_test(request_id, metric_data, analysis_start_time)
                    if shard_test_done:
                        logger.info('mirage_vortex :: SHARD_TEST processed request_id: %s' % str(request_id))
                        continue
                except Exception as err:
                    logger.error('error :: mirage_vortex :: shard_test failed - %s' % (
                        err))

            metric_timestamp = None
            try:
                metric_timestamp = int(timeseries[-1][0])
            except Exception as err:
                logger.error('error :: mirage_vortex :: failed to determine last timestamp for %s' % (
                    request_id))

            if not metric_timestamp:
                logger.error('error :: mirage_vortex :: failed no metric_timestamp for %s' % request_id)
                metric_data['anomalous'] = False
                additional_info = 'no valid timeseries data found'
                results = {
                    'success': False,
                    'anomalous': False,
                    'additional info': additional_info,
                    'algorithms': {}
                }
                for algorithm in algorithms:
                    results['algorithms'][algorithm] = None
                metric_data['results'] = results
                metric_data['error'] = additional_info
                metric_data['success'] = False
                metric_data['status_code'] = 400
                added_results = self.add_results(request_id, metric_data, analysis_start_time)
                if added_results:
                    logger.info('mirage_vortex :: processed request_id: %s' % str(request_id))
                continue

            is_unix_timestamp = False
            if isinstance(metric_timestamp, int):
                is_unix_timestamp = True
            if isinstance(metric_timestamp, float):
                is_unix_timestamp = True
            if not is_unix_timestamp:
                logger.error('error :: mirage_vortex :: not unix timestamps for %s' % request_id)
                metric_data['anomalous'] = False
                additional_info = 'date format incorrect, unix timestamps are required'
                results = {
                    'success': False,
                    'anomalous': False,
                    'additional info': additional_info,
                    'algorithms': {}
                }
                for algorithm in algorithms:
                    results['algorithms'][algorithm] = None
                metric_data['results'] = results
                metric_data['error'] = additional_info
                metric_data['success'] = False
                metric_data['status_code'] = 400
                added_results = self.add_results(request_id, metric_data, analysis_start_time)
                if added_results:
                    logger.info('mirage_vortex :: processed request_id: %s' % str(request_id))
                continue

            self.check_if_parent_is_alive()

            # Transform the timeseries data to ensure all non values are removed
            # and that timestamps are coerced into int and values into floats
            use_timeseries = []
            coerce_errors = []
            for index, item in enumerate(timeseries):
                try:
                    use_timeseries.append([int(item[0]), float(item[1])])
                except Exception as err:
                    coerce_errors.append([index, err])
            if coerce_errors:
                logger.info('mirage_vortex :: request_id: %s encountered %s coerce_errors, last err: %s' % (
                    str(request_id), str(len(coerce_errors)), str(coerce_errors[-1])))
            if not use_timeseries:
                logger.error('error :: mirage_vortex :: no use_timeseries for request_id: %s' % request_id)
                metric_data['anomalous'] = None
                additional_info = 'no data to analyse after preprocessing, invalid timestamps and values - hint: %s' % str(coerce_errors[-1])
                results = {
                    'success': False,
                    'anomalous': False,
                    'additional info': additional_info,
                    'algorithms': {}
                }
                for algorithm in algorithms:
                    results['algorithms'][algorithm] = None
                metric_data['results'] = results
                metric_data['error'] = additional_info
                metric_data['success'] = False
                metric_data['status_code'] = 400
                added_results = self.add_results(request_id, metric_data, analysis_start_time)
                if added_results:
                    logger.info('mirage_vortex :: processed request_id: %s' % str(request_id))
                continue

            try:
                return_image_urls = metric_data['return_image_urls']
            except:
                return_image_urls = False

            try:
                trigger_anomaly = metric_data['trigger_anomaly']
            except:
                trigger_anomaly = False

            try:
                algorithms_test_only = metric_data['algorithms_test_only']
            except:
                algorithms_test_only = False

            full_duration = int(use_timeseries[-1][0] - use_timeseries[0][0])
            metric_data['full_duration'] = full_duration
            hours_to_resolve = int(full_duration / 3600)
            nearest_full_duration = 86400 * round(full_duration / 86400)
            metric_data['nearest_full_duration'] = nearest_full_duration

            timeseries_length = len(timeseries)
            metric_data['timeseries_length'] = timeseries_length
            from_timestamp = int(use_timeseries[0][0])
            metric_data['from_timestamp'] = from_timestamp
            value = use_timeseries[-1][1]
            metric_data['value'] = value

            resolution = determine_data_frequency('mirage', timeseries, False)
            metric_data['resolution'] = resolution

            # Set whether the timeseries can be trained on.  It is not valid for
            # training if it has negative values or large gaps in the data.
            trainable = True

            # Determine whether the appropriate analysis period and whether the
            # timeseries needs to be downsampled
            downsample = False
            downsample_resolution = int(resolution)
            analysis_period = int(full_duration)
            # Analyse the data at 1 or 7 days
            if full_duration < ((86400 * 7) - 7200):
                analysis_period = 86400
            if full_duration > (86400 * 7):
                analysis_period = 86400 * 7

            # @added 20230129
            try:
                override_7_day_limit = metric_data['override_7_day_limit']
            except:
                override_7_day_limit = False
            if override_7_day_limit:
                analysis_period = timeseries[-1][0] - timeseries[0][0]

            metric_data['analysis_period'] = analysis_period
            # Remove any data before the start of the analysis period
            analysis_start_timestamp = metric_timestamp - analysis_period
            metric_data['analysis_period_start_timestamp'] = analysis_start_timestamp
            if timeseries[0][0] < analysis_start_timestamp:
                use_timeseries = [item for item in timeseries if item[0] >= analysis_start_timestamp]
            # Determine the downsample resolution
            if resolution < 60:
                downsample = True
                downsample_resolution = 60
            if resolution < 600 and analysis_period > 86400:
                downsample = True
                downsample_resolution = 600
            downsampled_timeseries = None
            downsampled = False

            no_downsample = False
            try:
                no_downsample = metric_data['no_downsample']
            except:
                no_downsample = False
            if no_downsample and downsample:
                logger.info('mirage_vortex :: no_downsample passed for request_id: %s NOT downsampling timeseries from %s data points at %s to %s at %s' % (
                    str(request_id), str(timeseries_length), str(resolution),
                    str(int(full_duration / downsample_resolution)),
                    str(downsample_resolution)))
                downsample = False
                # Not trainable if timeseries should be downsample and is not
                # trainable = False

            check_all_consensuses = False
            try:
                check_all_consensuses = metric_data['check_all_consensuses']
            except:
                check_all_consensuses = False

            # @added 20230616 - Feature #4952: vortex - consensus_count
            if consensus_count:
                check_all_consensuses = True
                consensus_count_results = {}
                consensus_count_results['algorithms'] = {}

            downsample_method = 'mean'
            is_strictly_increasing_monotonicity = False
            try:
                is_strictly_increasing_monotonicity = strictly_increasing_monotonicity(use_timeseries)
            except Exception as err:
                logger.error('error :: mirage_vortex :: is_strictly_increasing_monotonicity failed for request_id: %s - %s' % (
                    request_id, err))
            metric_data['monotonic'] = is_strictly_increasing_monotonicity
            if is_strictly_increasing_monotonicity:
                downsample_method = 'sum'

            if downsample:
                logger.info('mirage_vortex :: downsampling request_id: %s timeseries from %s data points at %s to %s at %s using the %s' % (
                    str(request_id), str(timeseries_length), str(resolution),
                    str(int(full_duration / downsample_resolution)),
                    str(downsample_resolution), downsample_method))
                try:
                    downsampled_timeseries = downsample_timeseries('mirage', use_timeseries, resolution, 600, downsample_method, 'end')
                except Exception as err:
                    logger.error('error :: mirage_vortex :: downsample_timeseries failed for request_id %s - %s' % (
                        request_id, err))
            if downsampled_timeseries:
                # Coerce timestamps to ints
                downsampled_timeseries = [[int(t), v] for t, v in downsampled_timeseries]
                downsampled = True
                metric_data['downsampled_resolution'] = downsample_resolution
                metric_data['downsampled_length'] = len(downsampled_timeseries)
                use_timeseries = downsampled_timeseries
                metric_data['downsample_resolution'] = downsample_resolution
            metric_data['downsampled'] = downsampled

            expected_timeseries_length = (86400 * 7) / 600
            if analysis_period == 86400:
                expected_timeseries_length = 86400 / 60

            use_timeseries_length = len(use_timeseries)
            metric_data['use_timeseries_length'] = use_timeseries_length

            if is_strictly_increasing_monotonicity:
                # Calculate the derivate AFTER downsampling
                try:
                    use_timeseries = nonNegativeDerivative(use_timeseries)
                except Exception as err:
                    logger.error('error :: mirage_vortex :: nonNegativeDerivative failed for request_id: %s - %s' % (
                        request_id, err))

            metric_data_no_timeseries = copy.deepcopy(metric_data)
            try:
                del metric_data_no_timeseries['timeseries']
            except:
                pass
            logger.info('mirage_vortex :: request_id: %s metric_data (excl timeseries): %s' % (
                str(request_id), str(metric_data_no_timeseries)))
            del metric_data_no_timeseries

            # Return if there is insufficient data
            if int(100 * (len(use_timeseries) / expected_timeseries_length)) < 90:
                logger.info('mirage_vortex :: after preprocessing there is insufficient data for analysis - request_id: %s, len(use_timeseries): %s, expected length: %s' % (
                    request_id, str(len(use_timeseries)), str(expected_timeseries_length)))
                metric_data['anomalous'] = False
                additional_info = 'after preprocessing there insufficient data for analysis'
                results = {
                    'success': False,
                    'anomalous': False,
                    'additional info': additional_info,
                    'algorithms': {}
                }
                for algorithm in algorithms:
                    results['algorithms'][algorithm] = None
                metric_data['results'] = results
                metric_data['error'] = additional_info
                metric_data['success'] = False
                metric_data['status_code'] = 400
                added_results = self.add_results(request_id, metric_data, analysis_start_time)
                if added_results:
                    logger.info('mirage_vortex :: processed request_id: %s' % str(request_id))
                continue

            # Determine if there are any negatives values
            negative_values = [v for t, v in use_timeseries if v < 0]
            if negative_values:
                logger.info('mirage_vortex :: processed request_id: %s' % str(request_id))
                trainable = False
            run_negatives_present = False

            anomalous = None
            triggered_algorithms = []
            algorithms_run = {}

            metric_data['results'] = {}
            metric_data['results']['success'] = False
            metric_data['results']['anomalous'] = None
            metric_data['results']['algorithms'] = {}
            metric_data['results']['triggered_algorithms'] = {}
            metric_data['metric_timestamp'] = int(timeseries[-1][0])

            added_results = None
            sent_to_ionosphere = False

            last_algorithm = algorithms_to_be_run[-1]

            # for algorithm in algorithms:
            for algorithm in algorithms_to_be_run:
                try:
                    if added_results or sent_to_ionosphere:
                        break

                    custom_algorithm = algorithm
                    if algorithm == 'prophet':
                        custom_algorithm = 'skyline_prophet'

                    if algorithm == 'matrixprofile':
                        custom_algorithm = 'skyline_matrixprofile'

                    if algorithm == 'sigma':
                        logger.info('mirage_vortex :: running sigma for request_id: %s' % str(request_id))
                        algorithm_parameters = {}
                        try:
                            algorithm_parameters = VORTEX_ALGORITHMS[algorithm]['algorithm_parameters']
                        except:
                            algorithm_parameters = {}
                        return_results = False
                        anomalous = None
                        anomalyScore = None
                        custom_algorithm = algorithm
                        try:
                            if 'algorithm_parameters' in list(algorithms['sigma'].keys()):
                                algorithm_parameters = copy.deepcopy(algorithms['sigma']['algorithm_parameters'])
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: mirage_vortex :: error determining algorithm_parameters - %s' % err)
                        try:
                            return_results = algorithm_parameters['return_results']
                        except:
                            return_results = False
                        try:
                            return_anomalies_only = algorithm_parameters['return_anomalies_only']
                        except:
                            return_anomalies_only = False
                        try:
                            anomaly_window = algorithm_parameters['anomaly_window']
                        except:
                            anomaly_window = 1
                        algorithm_parameters['anomaly_window'] = anomaly_window
                        algorithm_parameters['base_name'] = metric
                        algorithm_parameters['debug_print'] = False
                        algorithm_parameters['debug_logging'] = True
                        algorithm_parameters['return_results'] = True
                        algorithm_parameters['return_anomalies'] = True
                        custom_algorithms_to_run = {
                            'sigma': {
                                'algorithm_source': '/opt/skyline/github/skyline/skyline/custom_algorithms/sigma.py',
                                'max_execution_time': 30.0,
                                'algorithm_parameters': algorithm_parameters,
                            },
                        }
                        sigma_analysis_start_time = time()
                        sigma_anomalous = anomalyScore = None
                        success = False
                        anomalies = []
                        try:
                            use_debug_logging = False
                            sigma_anomalous, anomalyScore, anomalies = run_custom_algorithm_on_timeseries('mirage', os.getpid(), metric, use_timeseries, custom_algorithm, custom_algorithms_to_run[custom_algorithm], use_debug_logging)
                            # Try direct
                            # sigma_anomalous, anomalies = run_sigma_algorithms('mirage', timeseries, algorithm_parameters['sigma_value'], algorithm_parameters['consensus'], anomaly_window)
                            if sigma_anomalous:
                                anomalyScore = 1.0
                            else:
                                anomalyScore = 0.0
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: mirage_vortex :: unhandled error - %s' % err)
                        algorithms_run['sigma'] = sigma_anomalous
                        if sigma_anomalous:
                            triggered_algorithms.append('sigma')
                        sigma_completed_at = time()
                        sigma_run_time = sigma_completed_at - sigma_analysis_start_time
                        logger.info('mirage_vortex :: sigma analysis on %s completed in %.2f seconds with anomalous: %s' % (
                            request_id, sigma_run_time, str(sigma_anomalous)))
                        anomalies_in_window = len(anomalies)
                        if sigma_anomalous is not None:
                            success = True
                        metric_data['results']['algorithms']['sigma'] = {
                            'success': success,
                            'anomalous': sigma_anomalous,
                            'anomalyScore': anomalyScore,
                            'anomalies_in_window': anomalies_in_window,
                            'analysis_runtime': sigma_run_time,
                        }
                        if return_results:
                            metric_data['results']['algorithms']['sigma']['anomalies'] = anomalies

                        if not success:
                            metric_data['results']['algorithms']['sigma']['error'] = 'an error occurred during sigma analysis'

                        # @added 20230616 - Feature #4952: vortex - consensus_count
                        if consensus_count:
                            consensus_count_results['algorithms']['sigma'] = {}
                            consensus_count_results['algorithms']['sigma']['anomalies'] = anomalies
                            anomalyScore_list = []
                            anomalies_timestamps = [int(ts) for ts in list(anomalies.keys())]
                            for ts, v in timeseries:
                                score = 0
                                if int(ts) in anomalies_timestamps:
                                    score = 1
                                anomalyScore_list.append(score)
                            consensus_count_results['algorithms']['sigma']['scores'] = anomalyScore_list

                    else:
                        try:
                            if algorithm == 'prophet':
                                custom_algorithm = 'skyline_prophet'

                            if algorithm == 'matrixprofile':
                                custom_algorithm = 'skyline_matrixprofile'

                            logger.info('mirage_vortex :: running %s for request_id: %s' % (
                                custom_algorithm, str(request_id)))
                            algorithm_parameters = {}
                            try:
                                algorithm_parameters = VORTEX_ALGORITHMS[algorithm]['algorithm_parameters']
                            except:
                                algorithm_parameters = {}
                            outlier_value = VORTEX_ALGORITHMS[algorithm]['outlier_value']
                            return_results = False
                            anomalous = None
                            anomalyScore = None
                            custom_algorithm = algorithm
                            if algorithm == 'prophet':
                                custom_algorithm = 'skyline_prophet'
                            if algorithm == 'matrixprofile':
                                custom_algorithm = 'skyline_matrixprofile'

                            try:
                                if 'algorithm_parameters' in list(algorithms[algorithm].keys()):
                                    algorithm_parameters = copy.deepcopy(algorithms[algorithm]['algorithm_parameters'])
                            except Exception as err:
                                logger.error(traceback.format_exc())
                                logger.error('error :: mirage_vortex :: error determining algorithm_parameters - %s' % err)
                            try:
                                return_results = algorithm_parameters['return_results']
                                if 'return_results' in metric_data:
                                    return_results = metric_data['return_results']
                            except:
                                return_results = False
                            try:
                                return_anomalies_only = algorithm_parameters['return_anomalies_only']
                            except:
                                return_anomalies_only = False
                            try:
                                anomaly_window = algorithm_parameters['anomaly_window']
                            except:
                                anomaly_window = 1

                            if algorithm == 'pca':
                                try:
                                    outlier_value = algorithm_parameters['threshold']
                                except:
                                    outlier_value = VORTEX_ALGORITHMS[algorithm]['outlier_value']

                            algorithm_parameters['anomaly_window'] = anomaly_window
                            algorithm_parameters['base_name'] = metric
                            algorithm_parameters['debug_print'] = False
                            algorithm_parameters['debug_logging'] = True
                            algorithm_parameters['return_results'] = True
                            algorithm_parameters['return_anomalies'] = True
                            if algorithm == 'prophet':
                                custom_algorithm = 'skyline_prophet'
                            if algorithm == 'matrixprofile':
                                custom_algorithm = 'skyline_matrixprofile'

                            algorithm_source = '/opt/skyline/github/skyline/skyline/custom_algorithms/%s.py' % custom_algorithm
                            custom_algorithms_to_run = {
                                custom_algorithm: {
                                    'algorithm_source': algorithm_source,
                                    'max_execution_time': 30.0,
                                    'algorithm_parameters': algorithm_parameters,
                                },
                            }

                            if algorithm == 'mstl':
                                try:
                                    custom_algorithms_to_run[custom_algorithm]['max_execution_time'] = algorithm_parameters['max_execution_time']
                                except:
                                    custom_algorithms_to_run[custom_algorithm]['max_execution_time'] = 180

                            ca_analysis_start_time = time()
                            anomalous = anomalyScore = None
                            success = False
                            results = {'anomalies': {}, 'anomalyScore_list': [], 'scores': []}
                            anomalyScore_list = []
                            try:
                                use_debug_logging = False
                                anomalous, anomalyScore, results = run_custom_algorithm_on_timeseries('mirage', os.getpid(), metric, use_timeseries, custom_algorithm, custom_algorithms_to_run[custom_algorithm], use_debug_logging)
                            except Exception as err:
                                logger.error(traceback.format_exc())
                                logger.error('error :: mirage_vortex :: %s unhandled error - %s' % (custom_algorithm, err))
                                anomalous = anomalyScore = None
                                success = False
                                results = {'anomalies': {}, 'anomalyScore_list': [], 'scores': []}

                            algorithms_run[algorithm] = anomalous
                            if anomalous:
                                triggered_algorithms.append(algorithm)
                            ca_run_time = time() - ca_analysis_start_time
                            logger.info('mirage_vortex :: %s analysis on %s completed in %.2f seconds with anomalous: %s' % (
                                custom_algorithm, request_id, ca_run_time, str(anomalous)))
                            try:
                                anomalyScore_list = results['anomalyScore_list']
                            except:
                                try:
                                    anomalyScore_list = results['scores']
                                except:
                                    pass
                            total_anomalies = len([x for x in anomalyScore_list if x == 1])
                            anomalies_in_window = len([x for x in anomalyScore_list[-anomaly_window:] if x == 1])
                            if anomalous is not None:
                                success = True
                            unreliable = False
                            if algorithm == 'dbscan':
                                if anomalous is not None:
                                    success = True
                                else:
                                    try:
                                        if 'unreliable' in results['error']:
                                            success = True
                                            unreliable = True
                                    except:
                                        pass
                            metric_data['results']['algorithms'][algorithm] = {
                                'success': success,
                                'anomalous': anomalous,
                                'anomalyScore': anomalyScore,
                                'anomalies_in_window': anomalies_in_window,
                                'total_anomalies': total_anomalies,
                                'analysis_runtime': ca_run_time,
                                'unreliable': unreliable,
                            }
                            if return_results and not return_anomalies_only:
                                metric_data['results']['algorithms'][algorithm]['anomalies'] = results['anomalies']
                                metric_data['results']['algorithms'][algorithm]['scores'] = results['scores']
                            if return_anomalies_only:
                                anomalies = {}
                                for index, item in enumerate(timeseries):
                                    try:
                                        if results['scores'][index]:
                                            anomalies[item[0]] = {'value': item[1], 'index': index, 'score': results['scores'][index]}
                                    except:
                                        pass
                                metric_data['results']['algorithms'][algorithm]['anomalies'] = anomalies
                            if not success:
                                metric_data['results']['algorithms'][algorithm]['error'] = 'an error occurred during %s analysis' % algorithm
                            if algorithm == 'dbscan':
                                if unreliable:
                                    metric_data['results']['algorithms'][algorithm]['error'] = results['error']
                                    metric_data['results']['algorithms'][algorithm]['unreliable'] = True
                            if algorithm == 'spectral_entropy':
                                metric_data['results']['algorithms'][algorithm]['low_entropy_value'] = results['low_entropy_value']

                            # @added 20230616 - Feature #4952: vortex - consensus_count
                            if consensus_count:
                                consensus_count_results['algorithms'][algorithm] = {}
                                consensus_count_results['algorithms'][algorithm]['anomalies'] = results['anomalies']
                                consensus_count_results['algorithms'][algorithm]['scores'] = anomalyScore_list

                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: mirage_vortex :: %s unhandled error - %s' % (custom_algorithm, err))

                    # Determine consensus status
                    try:
                        consensus_reached, consensus_impossible = self.check_consensus(consensus, triggered_algorithms, algorithms_run)
                    except Exception as err:
                        logger.error('error :: mirage_vortex :: check_consensus failed for request_id: %s - %s' % (
                            request_id, err))

                    if check_all_consensuses:
                        if algorithm != last_algorithm:
                            consensus_reached = []
                            consensus_impossible = False

                    if consensus_reached or consensus_impossible:
                        if consensus_reached:
                            anomalous = True
                            metric_data['results']['consensus_achieved'] = True
                            logger.info('mirage_vortex :: consensus reached with %s for request_id: %s' % (
                                str(consensus_reached), str(request_id)))
                        if consensus_impossible:
                            anomalous = False
                            metric_data['results']['consensus_achieved'] = False
                            logger.info('mirage_vortex :: consensus cannot be reached for request_id: %s' % (
                                str(request_id)))
                            save_training_data = False
                        try:
                            metric_data['results']['consensus_reached'] = consensus_reached
                            metric_data['results']['success'] = True
                            metric_data['results']['anomalous'] = anomalous
                            metric_data['results']['algorithms_run'] = list(algorithms_run.keys())
                            for i_algorithm in algorithms:
                                triggered = i_algorithm in triggered_algorithms
                                if i_algorithm not in algorithms_run:
                                    triggered = None
                                if triggered is not None:
                                    metric_data['results']['triggered_algorithms'][i_algorithm] = triggered
                                if i_algorithm == 'dbscan':
                                    try:
                                        if anomalous is None:
                                            metric_data['results']['triggered_algorithms'][i_algorithm] = None
                                    except:
                                        pass
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: mirage_vortex :: check_consensus failed for request_id: %s - %s' % (
                                request_id, err))

                        consensus_anomalies = {}
                        consensus_scores = []
                        if return_results and not return_anomalies_only:
                            anomalies = {}
                            algorithms_with_results = list(metric_data['results']['algorithms'].keys())
                            try:
                                for index, item in enumerate(timeseries):
                                    ts = int(item[0])
                                    value = item[1]
                                    score = 0
                                    consensus_anomalies[ts] = {'value': value, 'triggered': [], 'score': 0}
                                    for i_algorithm in algorithms_with_results:
                                        algo_anomaly = None
                                        try:
                                            algo_anomaly = metric_data['results']['algorithms'][i_algorithm]['anomalies'][ts]
                                        except:
                                            pass
                                        if algo_anomaly:
                                            consensus_anomalies[ts]['triggered'].append(i_algorithm)
                                    if consensus_anomalies[ts]['triggered']:
                                        try:
                                            i_consensus_reached, i_consensus_impossible = self.check_consensus(consensus, consensus_anomalies[ts]['triggered'], [])
                                        except Exception as err:
                                            logger.error('error :: mirage_vortex :: check_consensus failed for request_id: %s - %s' % (
                                                request_id, err))
                                        if i_consensus_reached:
                                            score = len(i_consensus_reached)
                                            consensus_anomalies[ts]['score'] = score
                                            anomalies[str(ts)] = {'value': value, 'index': index, 'score': score, 'triggered': list(consensus_anomalies[ts]['triggered'])}
                                    consensus_scores.append(score)
                            except Exception as err:
                                logger.error('error :: mirage_vortex :: failed building consensus_anomalies for request_id: %s - %s' % (
                                    request_id, err))
                        if consensus_anomalies:
                            metric_data['results']['consensus_results'] = {}
                            metric_data['results']['consensus_results']['anomalous'] = True
                            metric_data['results']['consensus_results']['anomaly_window'] = anomaly_window
                            anomalies_in_window = len([x for x in consensus_scores[-anomaly_window:] if x >= 1])
                            metric_data['results']['consensus_results']['anomalies_in_window'] = anomalies_in_window
                            metric_data['results']['consensus_results']['unreliable'] = False
                            metric_data['results']['consensus_results']['anomalies'] = copy.deepcopy(anomalies)
                            metric_data['results']['consensus_results']['scores'] = list(consensus_scores)
                            # metric_data['results']['consensus_results']['consensus_anomalies'] = copy.deepcopy(consensus_anomalies)

                        ionosphere_enabled = False
                        labelled_metric_name = str(metric)
                        metric_data['labelled_metric_name'] = labelled_metric_name

                        save_training_data = False
                        if 'save_training_data_on_false' in list(metric_data.keys()):
                            if metric_data['save_training_data_on_false']:
                                logger.info('mirage_vortex :: save_training_data_on_false pased for request_id: %s, saving' % (
                                    str(request_id)))
                                save_training_data = True

                        if trigger_anomaly:
                            logger.info('mirage_vortex :: trigger_anomaly was passed for request_id: %s, setting to anomalous' % (
                                str(request_id)))
                            save_training_data = True
                            anomalous = True
                            metric_data['results']['anomalous'] = anomalous
                            metric_data['anomalous'] = anomalous

                        if algorithms_test_only:
                            logger.info('mirage_vortex :: algorithms_test_only was passed for request_id: %s, saving training_data' % (
                                str(request_id)))
                            save_training_data = True

                        if anomalous or save_training_data:
                            save_training_data = True
                            metric_namespace_prefix = metric_data['metric_namespace_prefix']
                            skyline_metric = None
                            labelled_metric = False
                            prefixed_metric = '%s' % str(metric)
                            possible_metric_names = [str(metric)]
                            try:
                                labelled_metric = self.is_labelled_metric(metric)
                            except:
                                labelled_metric = False
                            if labelled_metric:
                                if '_tenant_id="' not in metric:
                                    try:
                                        server_id = metric_data['server_id']
                                    except:
                                        server_id = 1
                                    try:
                                        labelled_metric_name = self.generate_labelled_metric_name(metric, metric_namespace_prefix, server_id=server_id)
                                    except Exception as err:
                                        logger.error('error :: mirage_vortex :: generate_labelled_metric_name failed for request_id: %s metric: %s - %s' % (
                                            request_id, str(metric), err))
                                possible_metric_names.append(labelled_metric_name)

                            if not labelled_metric:
                                if metric_namespace_prefix:
                                    if not metric.startswith(metric_namespace_prefix):
                                        prefixed_metric = '%s.%s' % (metric_namespace_prefix, metric)
                                        possible_metric_names.append(prefixed_metric)

                            metric_id = 0
                            for possible_metric_name in possible_metric_names:
                                try:
                                    metric_id = get_metric_id_from_base_name('mirage', possible_metric_name)
                                except:
                                    metric_id = 0
                                if metric_id:
                                    skyline_metric = possible_metric_name
                                    break

                            if not metric_id:
                                if labelled_metric:
                                    skyline_metric = labelled_metric_name
                                    labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
                                else:
                                    skyline_metric = prefixed_metric
                                    labelled_metric_name = None
                                logger.info('mirage_vortex :: inserting new metric into DB for request_id: %s' % str(request_id))
                                try:
                                    metric_id = insert_new_metric('mirage', skyline_metric)
                                    if metric_id:
                                        logger.info('mirage_vortex :: inserted new metric with id %s for request_id: %s, metric: %s' % (
                                            str(metric_id), str(request_id), str(metric)))
                                except Exception as err:
                                    logger.error('error :: mirage_vortex :: insert_new_metric failed for request_id: %s metric: %s - %s' % (
                                        request_id, str(skyline_metric), err))
                            if metric_id and skyline_metric:
                                if labelled_metric:
                                    labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
                                metric_data['internal_metric_name'] = skyline_metric
                                metric_data['labelled_metric_name'] = labelled_metric_name
                                metric_data['metric_id'] = metric_id

                                try:
                                    ionosphere_enabled = get_ionosphere_fp_ids_for_full_duration('mirage', metric_id, full_duration=nearest_full_duration, enabled=True)
                                except Exception as err:
                                    logger.error('error :: mirage_vortex :: get_ionosphere_fp_ids_for_full_duration failed for metric_id: %s request_id: %s - %s' % (
                                        str(metric_id), request_id, err))
                                save_training_data = True
                                logger.info('mirage_vortex :: adding results for for request_id: %s and saving training_data' % str(request_id))
                            else:
                                logger.error('error :: mirage_vortex :: failed to determine a metric_id or skyline_metric for request_id: %s metric: %s, training_data will not be saved' % (
                                    request_id, str(metric)))
                        saved_training_data = False

                        if save_training_data:
                            if not trainable:
                                logger.info('mirage_vortex :: not saving training_data for request_id: %s, trainable: %s' % (
                                    str(request_id), str(trainable)))
                                save_training_data = False
                        send_to_ionosphere = False
                        if 'send_to_ionosphere' in list(metric_data.keys()):
                            send_to_ionosphere = metric_data['send_to_ionosphere']
                            if send_to_ionosphere:
                                save_training_data = True

                        # @added 20230616 - Feature #4952: vortex - consensus_count
                        if consensus_count:
                            logger.info('mirage_vortex :: adding consensus_count results for request_id: %s' % (
                                str(request_id)))
                            consensus_count_anomalies = {}
                            consensus_count_scores = []
                            consensus_count_algos = list(consensus_count_results['algorithms'].keys())
                            for index, item in enumerate(timeseries):
                                score = 0
                                ts = int(item[0])
                                triggered = []
                                for algo in consensus_count_algos:
                                    try:
                                        if consensus_count_results['algorithms'][algorithm]['scores'][index] == 1:
                                            triggered.append(algo)
                                    except:
                                        pass
                                if len(triggered) >= consensus_count:
                                    consensus_count_anomalies[ts] = {'value': item[1], 'index': index, 'score': len(triggered), 'triggered': triggered}
                                    score = len(triggered)
                                consensus_count_scores.append(score)
                            anomalies_in_window = len([x for x in consensus_count_scores[-anomaly_window:] if x >= 1])
                            anomalous = False
                            if anomalies_in_window:
                                anomalous = True
                            metric_data['results']['consensus_count_results'] = {}
                            metric_data['results']['consensus_count_results']['anomalous'] = anomalous
                            metric_data['results']['consensus_count_results']['anomaly_window'] = anomaly_window
                            metric_data['results']['consensus_count_results']['anomalies_in_window'] = anomalies_in_window
                            metric_data['results']['consensus_count_results']['unreliable'] = False
                            metric_data['results']['consensus_count_results']['anomalies'] = copy.deepcopy(consensus_count_anomalies)
                            metric_data['results']['consensus_count_results']['scores'] = list(consensus_count_scores)

                        if return_image_urls:
                            image_urls = []
                            full_duration_in_hours = int(metric_data['nearest_full_duration'] / 3600)
                            use_base_name = str(metric_data['labelled_metric_name'])
                            timeseries_dir = use_base_name.replace('.', '/')
                            metric_data_dir = '%s/%s/%s' % (
                                settings.IONOSPHERE_DATA_FOLDER, metric_timestamp,
                                timeseries_dir)
                            for i_algorithm in metric_data['results']['algorithms_run']:
                                image_file = '%s/vortex.algorithm.%s.%s.%sh.png' % (metric_data_dir, i_algorithm, metric_data['labelled_metric_name'], str(full_duration_in_hours))
                                image_url = '%s/ionosphere_images?image=%s' % (settings.SKYLINE_URL, image_file)
                                image_urls.append(image_url)

                            if consensus_anomalies:
                                image_file = '%s/vortex.algorithm.consensus.%s.%sh.png' % (metric_data_dir, metric_data['labelled_metric_name'], str(full_duration_in_hours))
                                image_url = '%s/ionosphere_images?image=%s' % (settings.SKYLINE_URL, image_file)
                                image_urls.append(image_url)

                            # @added 20230616 - Feature #4952: vortex - consensus_count
                            if consensus_count:
                                image_file = '%s/vortex.algorithm.consensus_count.%s.%sh.png' % (metric_data_dir, metric_data['labelled_metric_name'], str(full_duration_in_hours))
                                image_url = '%s/ionosphere_images?image=%s' % (settings.SKYLINE_URL, image_file)
                                image_urls.append(image_url)

                            metric_data['results']['image_urls'] = image_urls

                        if save_training_data:
                            create_echo_data = False
                            if metric_data['resolution'] <= 60 and metric_data['full_duration'] >= 86400:
                                if metric_data['analysis_period'] > 86400:
                                    create_echo_data = True
                            if create_echo_data:
                                try:
                                    created_echo_data = self.create_echo_timeseries(metric_data, timeseries)
                                except Exception as err:
                                    logger.error('error :: mirage_vortex :: create_echo_timeseries failed for request_id: %s - %s' % (
                                        request_id, err))
                                if not created_echo_data:
                                    logger.warning('warning :: mirage_vortex :: create_echo_timeseries failed for request_id: %s' % (
                                        request_id))

                            try:
                                vortex_metric_data = copy.deepcopy(metric_data)
                                saved_training_data = self.add_training_data(request_id, vortex_metric_data, use_timeseries, ionosphere_enabled)
                                if saved_training_data:
                                    logger.info('mirage_vortex :: saved training_data for request_id: %s to %s' % (
                                        str(request_id), saved_training_data))
                                else:
                                    logger.info('mirage_vortex :: failed to save training_data for request_id: %s' % (
                                        str(request_id)))
                                    trainable = False
                                try:
                                    del vortex_metric_data
                                except:
                                    pass
                            except Exception as err:
                                logger.error('error :: mirage_vortex :: add_training_data failed for request_id: %s - %s' % (
                                    request_id, err))
                            if saved_training_data and return_image_urls:
                                image_urls = []
                                full_duration_in_hours = int(metric_data['nearest_full_duration'] / 3600)
                                for i_algorithm in metric_data['results']['algorithms_run']:
                                    image_file = '%s/vortex.algorithm.%s.%s.%sh.png' % (saved_training_data, i_algorithm, metric_data['labelled_metric_name'], str(full_duration_in_hours))
                                    image_url = '%s/ionosphere_images?image=%s' % (settings.SKYLINE_URL, image_file)
                                    image_urls.append(image_url)

                                if consensus_anomalies:
                                    image_file = '%s/vortex.algorithm.consensus.%s.%sh.png' % (saved_training_data, metric_data['labelled_metric_name'], str(full_duration_in_hours))
                                    image_url = '%s/ionosphere_images?image=%s' % (settings.SKYLINE_URL, image_file)
                                    image_urls.append(image_url)

                                metric_data['results']['image_urls'] = image_urls
                        metric_data['training_data'] = saved_training_data
                        metric_data['results']['trainable'] = False
                        add_results = True

                        if trigger_anomaly:
                            logger.info('mirage_vortex :: trigger_anomaly was passed for request_id: %s, setting to send_to_ionosphere and ionosphere_enabled to False' % (
                                str(request_id)))
                            ionosphere_enabled = send_to_ionosphere = False
                            add_results = True

                        if algorithms_test_only:
                            logger.info('mirage_vortex :: algorithms_test_only was passed for request_id: %s, setting to anomalous, send_to_ionosphere and ionosphere_enabled to False' % (
                                str(request_id)))
                            anomalous = ionosphere_enabled = send_to_ionosphere = False
                            add_results = True

                        if ionosphere_enabled or send_to_ionosphere:
                            add_results = False
                            # Send to ionosphere
                            ionosphere_parent_id = 0
                            try:
                                timeseries_dir = labelled_metric_name.replace('.', '/')
                                send_anomalous_metric_to(
                                    'mirage_vortex', 'ionosphere', timeseries_dir,
                                    str(metric_timestamp), labelled_metric_name,
                                    str(timeseries[-1][1]), str(int(timeseries[0][0])),
                                    triggered_algorithms, timeseries,
                                    nearest_full_duration, str(ionosphere_parent_id),
                                    list(algorithms_run.keys()))
                                sent_to_ionosphere = True
                                logger.info('mirage_vortex :: sent %s to ionosphere for request_id: %s' % (
                                    str(labelled_metric_name), request_id))
                            except Exception as err:
                                logger.error('error :: mirage_vortex :: add_training_data failed for request_id: %s - %s' % (
                                    request_id, err))

                        if sent_to_ionosphere:
                            metric_data_file = '%s/vortex.metric_data.%s.json' % (saved_training_data, request_id)
                            key_data = {
                                'request_id': request_id,
                                'metric': labelled_metric_name,
                                'timestamp': metric_timestamp,
                                'training_data': saved_training_data,
                                'metric_data_file': metric_data_file,
                                'timeout': metric_data['timeout'],
                            }
                            try:
                                self.redis_conn_decoded.hset('mirage_vortex.sent_to_ionosphere', request_id, str(key_data))
                            except Exception as err:
                                logger.error('error :: mirage_vortex :: failed to add request_id: %s to mirage_vortex.sent_to_ionosphere - %s' % (
                                    request_id, err))
                            continue

                        if add_results:
                            try:
                                added_results = self.add_results(request_id, metric_data, analysis_start_time)
                            except Exception as err:
                                logger.error('error :: mirage_vortex :: add_results failed for request_id: %s - %s' % (
                                    request_id, err))
                                break
                        if added_results:
                            logger.info('mirage_vortex :: processed request_id: %s' % str(request_id))
                            if anomalous:
                                logger.info('mirage_vortex :: anomaly detected - %s' % labelled_metric_name)
                                # If the metric has no features profiles and is anomalous
                                # add a panorama anomaly because Ionosphere will not add
                                # one.
                                send_to_panorama = False
                                if not ionosphere_enabled:
                                    if settings.PANORAMA_ENABLED:
                                        send_to_panorama = True
                                if send_to_panorama:
                                    try:
                                        sent_to_panorama = self.send_anomaly_to_panorama(
                                            labelled_metric_name, value, from_timestamp,
                                            metric_timestamp, algorithms_run, triggered_algorithms)
                                    except Exception as err:
                                        logger.error('error :: mirage_vortex :: send_anomaly_to_panorama failed - %s' % (
                                            err))
                                if sent_to_panorama:
                                    logger.info('mirage_vortex :: sent %s anomaly to panorama' % labelled_metric_name)
                                current_illuminance_dict = {}
                                try:
                                    current_illuminance_dict = self.add_to_illuminance(run_timestamp, labelled_metric_name, metric_timestamp, value, triggered_algorithms)
                                except Exception as err:
                                    logger.error('error :: mirage_vortex :: add_illuminance_entries failed - %s' % (
                                        err))
                                if current_illuminance_dict:
                                    logger.info('mirage_vortex :: added %s anomaly to illuminance' % labelled_metric_name)
                                logger.info('mirage_vortex :: adding %s to mirage_vortex.anomalous_metrics to be alerted on by mirage' % labelled_metric_name)
                                added_to_mirage_vortex_anomalous_metrics = False
                                try:
                                    added_to_mirage_vortex_anomalous_metrics = self.add_to_mirage_vortex_anomalous_metrics(metric_data)
                                except Exception as err:
                                    logger.error('error :: mirage_vortex :: failed to hdel %s from mirage_vortex.sent_to_ionosphere - %s' % (
                                        request_id, err))
                                logger.info('mirage_vortex :: added mirage_vortex.anomalous_metrics: %s' % str(added_to_mirage_vortex_anomalous_metrics))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_vortex :: algorithm %s failed - %s' % str(err))

            try:
                self.redis_conn.incr('mirage_vortex.checks.done')
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_vortex :: failed to increment mirage_vortex.checks.done Redis key - %s' % str(err))

            # @added 20220420 - Feature #4530: namespace.analysed_events
            parent_namespace = metric_data['metric_namespace_prefix']
            if not parent_namespace:
                try:
                    parent_namespace = metric.split('.')[0]
                except:
                    pass
            date_string = str(strftime('%Y-%m-%d', gmtime()))
            namespace_analysed_events_hash = 'namespace.analysed_events.mirage.%s' % (date_string)
            try:
                self.redis_conn.hincrby(namespace_analysed_events_hash, parent_namespace, 1)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_vortex :: failed to increment %s Redis hash - %s' % (
                    namespace_analysed_events_hash, err))
            try:
                self.redis_conn.expire(namespace_analysed_events_hash, (86400 * 15))
                logger.info('mirage_vortex :: updated %s Redis hash' % namespace_analysed_events_hash)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_vortex :: failed to set expire %s Redis hash - %s' % (
                    namespace_analysed_events_hash, err))

            if added_results or sent_to_ionosphere:
                continue

        completed_at = time()
        analysis_run_time = completed_at - process_start_timestamp
        logger.info('mirage_vortex :: %s checks processed in in %.2f seconds' % (
            str(checks_processed), analysis_run_time))

    def run(self):
        """
        Called when the process intializes.
        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        if os.path.isfile(skyline_app_logwait):
            try:
                os.remove(skyline_app_logwait)
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_logwait)

        now = time()
        log_wait_for = now + 5
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time()
            else:
                now = log_wait_for + 1

        logger.info('mirage_vortex :: starting %s run' % skyline_app)

        last_sent_to_graphite = int(time())
        filesafe_names_dict = {}
        last_sleep_log = int(time())

        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except:
                logger.error('error :: mirage_vortex :: skyline can not connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                sleep(10)
                logger.info('mirage_vortex :: attempting to connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                try:
                    self.redis_conn = get_redis_conn(skyline_app)
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                except Exception as err:
                    logger.error('error :: mirage_vortex :: failed to connect to Redis - %s' % err)
                try:
                    self.redis_conn.ping()
                    logger.info('mirage_vortex :: connected to redis')
                except Exception as err:
                    logger.error('error :: mirage_vortex :: failed to ping Redis - %s' % err)

            # Determine if any metric to analyze or Ionosphere alerts to be sent
            while True:

                # Report app up
                try:
                    redis_is_up = self.redis_conn.setex(skyline_app, 120, now)
                    if redis_is_up:
                        try:
                            self.redis_conn.setex('redis', 120, now)
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: mirage_vortex :: could not update the Redis redis key - %s' % (
                                e))
                except Exception as e:
                    logger.error('error :: mirage_vortex :: failed to update Redis key for %s up - %s' % (skyline_app, e))

                items_to_analyse = []
                ionosphere_results = []
                return_results_for = {}

                if len(items_to_analyse) == 0 and len(ionosphere_results) == 0 and len(return_results_for) == 0:
                    sleep_for = 1
                    next_send_to_graphite = last_sent_to_graphite + 60
                    seconds_to_next_send_to_graphite = next_send_to_graphite - int(time())
                    if seconds_to_next_send_to_graphite < 10:
                        if seconds_to_next_send_to_graphite > 1:
                            sleep_for = seconds_to_next_send_to_graphite
                        else:
                            break
                    if int(time()) > last_sleep_log + 10:
                        logger.info('mirage_vortex :: sleeping no metrics...')
                        last_sleep_log = int(time())
                    sleep(sleep_for)

                # Return results that are about to timeout
                return_results_for = {}
                mirage_vortex_sent_to_ionosphere = {}
                c_time = int(time())
                try:
                    mirage_vortex_sent_to_ionosphere = self.redis_conn_decoded.hgetall('mirage_vortex.sent_to_ionosphere')
                except Exception as err:
                    logger.error('mirage_vortex :: hkeys failed on ionosphere.vortex_results - %s' % err)
                for request_id in list(mirage_vortex_sent_to_ionosphere.keys()):
                    try:
                        timeout = int(mirage_vortex_sent_to_ionosphere['request_id']['timeout'])
                    except:
                        timeout = 55
                    try:
                        sent_timestamp_str = request_id.split('.')[0]
                    except:
                        sent_timestamp_str = '60'
                    if c_time > (int(sent_timestamp_str) + ((timeout - 3) * 3)):
                        return_results_for[request_id] = copy.deepcopy(mirage_vortex_sent_to_ionosphere[request_id])

                # Send ionosphere results
                ionosphere_results = []
                if not return_results_for:
                    try:
                        ionosphere_results = self.redis_conn_decoded.hkeys('ionosphere.vortex_results')
                    except Exception as err:
                        logger.error('mirage_vortex :: hkeys failed on failed ionosphere.vortex_results - %s' % err)

                # Get added checks
                items_to_analyse = []
                if not return_results_for and not ionosphere_results:
                    try:
                        items_to_analyse = self.redis_conn_decoded.hkeys('flux.vortex')
                    except Exception as err:
                        logger.error('mirage_vortex :: hkeys failed on failed flux.vortex - %s' % err)

                if len(items_to_analyse) > 0 or len(ionosphere_results) > 0 or len(return_results_for) > 0:
                    break

            process_metric_checks = False
            if items_to_analyse:
                items_to_analyse_sorted = sorted(items_to_analyse)
                if items_to_analyse_sorted:
                    process_metric_checks = True

            if process_metric_checks or ionosphere_results or return_results_for:

                # @added 20200903 - Task #3730: Validate Mirage running multiple processes
                checks_to_process = len(items_to_analyse_sorted)
                if items_to_analyse:
                    logger.info('mirage_vortex :: %s checks to process' % str(checks_to_process))

                if ionosphere_results:
                    logger.info('mirage_vortex :: %s ionosphere_results to process' % str(len(ionosphere_results)))

                if return_results_for:
                    logger.info('mirage_vortex :: returning results for %s request_ids about to timeout' % str(len(return_results_for)))

                # Remove any existing algorithm.error files from any previous runs
                # that did not cleanup for any reason
                pattern = '%s.*.algorithm.error' % skyline_app
                try:
                    for f in os.listdir(settings.SKYLINE_TMP_DIR):
                        if re.search(pattern, f):
                            try:
                                os.remove(os.path.join(settings.SKYLINE_TMP_DIR, f))
                                logger.info('mirage_vortex :: cleaning up old error file - %s' % (str(f)))
                            except OSError:
                                pass
                except:
                    logger.error('failed to cleanup mirage_algorithm.error files - %s' % (traceback.format_exc()))

                # Spawn processes
                pids = []
                spawned_pids = []
                pid_count = 0

                if len(items_to_analyse) > 1:
                    try:
                        MIRAGE_PROCESSES = int(settings.MIRAGE_PROCESSES)
                        if len(items_to_analyse) < MIRAGE_PROCESSES:
                            MIRAGE_PROCESSES = len(items_to_analyse)
                    except:
                        MIRAGE_PROCESSES = 1
                else:
                    MIRAGE_PROCESSES = 1

                MIRAGE_PROCESSES = 1

                run_timestamp = int(time())
                for i in range(1, MIRAGE_PROCESSES + 1):

                    assigned_checks = []
                    if items_to_analyse:
                        checks_per_processor = int(ceil(float(len(items_to_analyse_sorted)) / float(MIRAGE_PROCESSES)))
                        if i == MIRAGE_PROCESSES:
                            assigned_max = len(items_to_analyse_sorted)
                        else:
                            assigned_max = min(len(items_to_analyse_sorted), i * checks_per_processor)
                        assigned_min = (i - 1) * checks_per_processor
                        assigned_keys = range(assigned_min, assigned_max)
                        # Compile assigned metrics
                        assigned_checks = [items_to_analyse_sorted[index] for index in assigned_keys]
                        logger.info('mirage_vortex :: processing %s checks' % str(len(assigned_checks)))
                    if ionosphere_results:
                        logger.info('mirage_vortex :: processing %s ionosphere_results' % str(len(ionosphere_results)))
                    if return_results_for:
                        logger.info('mirage_vortex :: returning %s results' % str(len(return_results_for)))

                    p = Process(target=self.spin_process, args=(i, run_timestamp, assigned_checks, ionosphere_results, return_results_for))

                    pids.append(p)
                    pid_count += 1
                    logger.info('mirage_vortex :: starting %s of %s spin_process/es' % (
                        str(pid_count), str(MIRAGE_PROCESSES)))
                    p.start()
                    spawned_pids.append([p.pid, i])
                    logger.info('mirage_vortex :: started spin_process %s with pid %s' % (str(pid_count), str(p.pid)))

                # Self monitor processes and terminate if any spin_process has run
                # for longer than 180 seconds - 20160512 @earthgecko
                p_starts = time()
                while time() - p_starts <= (settings.MAX_ANALYZER_PROCESS_RUNTIME * 3):
                    if any(p.is_alive() for p in pids):
                        # Just to avoid hogging the CPU
                        sleep(.1)
                    else:
                        # All the processes are done, break now.
                        time_to_run = time() - p_starts
                        logger.info('mirage_vortex :: %s :: %s spin_process/es completed in %.2f seconds' % (
                            skyline_app, str(MIRAGE_PROCESSES), time_to_run))
                        break
                else:
                    # We only enter this if we didn't 'break' above.
                    logger.info('mirage_vortex :: %s :: timed out, killing all spin_process processes' % (skyline_app))
                    for p in pids:
                        p.terminate()
                        # p.join()

                for p in pids:
                    if p.is_alive():
                        logger.info('mirage_vortex :: %s :: stopping spin_process - %s' % (skyline_app, str(p.is_alive())))
                        p.join()

                # @added 20200607 - Feature #3508: ionosphere.untrainable_metrics
                # Check to non 3sigma algorithm errors too
                check_algorithm_errors = ['negatives_present']
                for algorithm in list(settings.MIRAGE_ALGORITHMS):
                    check_algorithm_errors.append(algorithm)

                for completed_pid, mirage_process in spawned_pids:
                    logger.info('mirage_vortex :: spin_process with pid %s completed' % (str(completed_pid)))
                    # Check to non 3sigma algorithm errors too and wrapped in try
                    try:
                        # for algorithm in settings.MIRAGE_ALGORITHMS:
                        for algorithm in check_algorithm_errors:
                            algorithm_error_file = '%s/%s.%s.%s.algorithm.error' % (
                                settings.SKYLINE_TMP_DIR, skyline_app,
                                str(completed_pid), algorithm)
                            if os.path.isfile(algorithm_error_file):
                                logger.info(
                                    'error :: mirage_vortex :: spin_process with pid %s has reported an error with the %s algorithm' % (
                                        str(completed_pid), algorithm))
                                try:
                                    with open(algorithm_error_file, 'r') as f:
                                        error_string = f.read()
                                    logger.error('%s' % str(error_string))
                                except:
                                    logger.error('error :: mirage_vortex :: failed to read %s error file' % algorithm)
                                try:
                                    os.remove(algorithm_error_file)
                                except OSError:
                                    pass
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_vortex :: failed to check algorithm errors')

                    redis_metrics_processed_key = 'mirage_vortex.%s.metrics_processed' % str(mirage_process)
                    redis_metrics_processed = {}
                    try:
                        redis_metrics_processed = self.redis_conn_decoded.hgetall(redis_metrics_processed_key)
                        # if redis_metrics_processed:
                        #     self.redis_conn_decoded.delete(redis_metrics_processed_key)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_vortex :: %s Redis hash operation failed - %s' % (redis_metrics_processed_key, err))

                    if items_to_analyse:
                        logger.info('mirage_vortex :: process %s checked %s metrics' % (
                            str(mirage_process), str(len(redis_metrics_processed))))

                    # Remove checks from flux.vortex
                    if redis_metrics_processed:
                        try:
                            self.redis_conn.hdel('flux.vortex', *set(list(redis_metrics_processed.keys())))
                        except Exception as err:
                            logger.error('error :: mirage_vortex :: hdel on flux.vortex failed - %s' % err)

            mirage_vortex_anomalous_metrics = []
            try:
                literal_mirage_vortex_anomalous_metrics = list(self.redis_conn_decoded.smembers('mirage_vortex.anomalous_metrics'))
                for metric_list_string in literal_mirage_vortex_anomalous_metrics:
                    metric = literal_eval(metric_list_string)
                    mirage_vortex_anomalous_metrics.append(metric)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_vortex :: failed to determine list from mirage_vortex.anomalous_metrics Redis set')
                mirage_vortex_anomalous_metrics = []

            mirage_not_anomalous_metrics = []
            try:
                literal_mirage_not_anomalous_metrics = list(self.redis_conn_decoded.smembers('mirage_vortex.not_anomalous_metrics'))
                for metric_list_string in literal_mirage_not_anomalous_metrics:
                    metric = literal_eval(metric_list_string)
                    mirage_not_anomalous_metrics.append(metric)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_vortex :: failed to determine list from mirage_vortex.not_anomalous_metrics Redis set')
                mirage_not_anomalous_metrics = []

            # Log progress
            logger.info('mirage_vortex :: total anomalies    :: %d' % len(mirage_vortex_anomalous_metrics))

            # Log to Graphite
            if process_metric_checks:
                run_time = time() - run_timestamp
                logger.info('mirage_vortex :: seconds to run    :: %.2f' % run_time)
                graphite_run_time = '%.2f' % run_time
                send_metric_name = skyline_app_graphite_namespace + '.run_time'
                send_graphite_metric(self, skyline_app, send_metric_name, graphite_run_time)

            if int(time()) >= (last_sent_to_graphite + 60):
                checks_done = 0
                try:
                    # @modified 20230205 - Task #4844: Replace Redis getset with set with get
                    # As of Redis version 6.2.0, this command is regarded as deprecated.
                    # It can be replaced by SET with the GET argument when migrating or writing new code.
                    # checks_done_str = self.redis_conn_decoded.getset('mirage_vortex.checks.done', 0)
                    checks_done_str = self.redis_conn_decoded.set('mirage_vortex.checks.done', 0, get=True)

                    if checks_done_str:
                        checks_done = int(checks_done_str)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_vortex :: failed to get mirage_vortex.checks.done key from Redis')
                    checks_done = 0
                logger.info('mirage_vortex :: checks.done   :: %s' % str(checks_done))
                send_metric_name = '%s.checks.done' % skyline_app_graphite_namespace
                send_graphite_metric(self, skyline_app, send_metric_name, str(checks_done))

                last_sent_to_graphite = int(time())

            # Sleep if it went too fast
            if time() - now < 59:
                logger.info('mirage_vortex :: sleeping due to low run time...')
                sleep(1)
