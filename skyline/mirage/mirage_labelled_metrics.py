"""
mirage_labelled_metrics.py
"""
import logging
try:
    from Queue import Empty
except:
    from queue import Empty
# from redis import StrictRedis
from time import time, sleep, strftime, gmtime
from threading import Thread
from collections import defaultdict
# @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
# Use Redis sets in place of Manager().list() to reduce memory and number of
# processes
# from multiprocessing import Process, Manager, Queue
from multiprocessing import Process, Queue
import traceback
import re
# imports required for surfacing graphite JSON formatted timeseries for use in
# Mirage
import json
import sys
import os
from shutil import rmtree
from ast import literal_eval
from math import ceil
import datetime

# @added 20220722 - Task #4624: Change all dict copy to deepcopy
import copy

import settings
# @modified 20160922 - Branch #922: Ionosphere
# Added the send_anomalous_metric_to skyline_functions.py
from skyline_functions import (
    write_data_to_file, fail_check, send_anomalous_metric_to,
    # @modified 20220726 - Task #2732: Prometheus to Skyline
    #                      Branch #4300: prometheus
    # Moved send_graphite_metric
    # mkdir_p, send_graphite_metric, filesafe_metricname,
    mkdir_p, filesafe_metricname,
    # @added 20191030 - Bug #3266: py3 Redis binary objects not strings
    #                   Branch #3262: py3
    # Added a single functions to deal with Redis connection and the
    # charset='utf-8', decode_responses=True arguments required in py3
    get_redis_conn, get_redis_conn_decoded,
)

from mirage_algorithms import run_selected_algorithm
from algorithm_exceptions import TooShort, Stale, Boring

# @added 20220504 - Feature #2580: illuminance
from functions.illuminance.add_illuminance_entries import add_illuminance_entries

# @added 20220715 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric
from functions.prometheus.metric_name_labels_parser import metric_name_labels_parser

# @added 20220726 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.graphite.send_graphite_metric import send_graphite_metric

from functions.metrics.get_base_name_from_labelled_metrics_name import get_base_name_from_labelled_metrics_name

# @added 20221105 - Feature #4724: custom_algorithms - anomalous_daily_peak
from custom_algorithms import run_custom_algorithm_on_timeseries

# @added 20230419 - Feature #4848: mirage - analyse.irregular.unstable.timeseries.at.30days
from functions.timeseries.normalized_variance import normalized_variance

# @added 20230522 - metric_type.longterm_expire
from functions.timeseries.strictly_increasing_monotonicity import strictly_increasing_monotonicity

skyline_app = 'mirage'
skyline_app_logger = '%sLog' % skyline_app
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
    # @modified 20200606 - Bug #3572: Apply list to settings import
    BATCH_PROCESSING_NAMESPACES = list(settings.BATCH_PROCESSING_NAMESPACES)
except:
    BATCH_PROCESSING_NAMESPACES = []

# @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
# Determine if any metrcs have negatives values some they can be
# added to the ionosphere.untrainable_metrics Redis set
try:
    # @modified 20200606 - Bug #3572: Apply list to settings import
    # from settings import KNOWN_NEGATIVE_METRICS
    KNOWN_NEGATIVE_METRICS = list(settings.KNOWN_NEGATIVE_METRICS)
except:
    KNOWN_NEGATIVE_METRICS = []

# @added 20200607 - Feature #3566: custom_algorithms
try:
    CUSTOM_ALGORITHMS = settings.CUSTOM_ALGORITHMS
except:
    CUSTOM_ALGORITHMS = None
try:
    DEBUG_CUSTOM_ALGORITHMS = settings.DEBUG_CUSTOM_ALGORITHMS
except:
    DEBUG_CUSTOM_ALGORITHMS = False

# @added 20200913 - Branch #3068: SNAB
#                   Task #3744: POC matrixprofile
#                   Info #1792: Shapelet extraction
try:
    SNAB_ENABLED = settings.SNAB_ENABLED
except:
    SNAB_ENABLED = False
try:
    # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
    # SNAB_CHECKS = settings.SNAB_CHECKS.copy()
    SNAB_CHECKS = copy.deepcopy(settings.SNAB_CHECKS)
except:
    SNAB_CHECKS = {}
# @added 20200916 - Branch #3068: SNAB
#                   Task #3744: POC matrixprofile
mirage_snab_only_checks_redis_set = 'mirage.snab_only_checks'

# @added 20201026 - Task #3800: Handle feedback metrics in Mirage and waterfall alerts
# Handle feedback metrics in a similar style to Ionosphere
try:
    SKYLINE_FEEDBACK_NAMESPACES = list(settings.SKYLINE_FEEDBACK_NAMESPACES)
except:
    # Let us take a guess
    try:
        graphite_host = str(settings.GRAPHITE_HOST)
        graphite_hostname = graphite_host.split('.', -1)[0]
        SKYLINE_FEEDBACK_NAMESPACES = [settings.SERVER_METRICS_NAME, graphite_hostname]
    except:
        SKYLINE_FEEDBACK_NAMESPACES = [this_host]

# @added 20210701 - Feature #4152: DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES
try:
    DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES = list(settings.DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES)
except:
    DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES = []

# @added 20201208 - Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
#                   Task #3868: POC MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
# @modified 20220414 - Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
#                   Task #3868: POC MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
# Not introduced as a settings, making this the default behaviour
# try:
#     MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS = settings.MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
# except:
#     MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS = False
MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS = True

# @added 20210323 - Feature #3642: Anomaly type classification
try:
    LUMINOSITY_CLASSIFY_ANOMALIES = settings.LUMINOSITY_CLASSIFY_ANOMALIES
except:
    LUMINOSITY_CLASSIFY_ANOMALIES = False

# @added 20221105 - Feature #4724: custom_algorithms - anomalous_daily_peak
try:
    MIRAGE_CHECK_REPETITIVE_DAILY_PEAKS = settings.MIRAGE_CHECK_REPETITIVE_DAILY_PEAKS
except:
    MIRAGE_CHECK_REPETITIVE_DAILY_PEAKS = 0
MIRAGE_CHECK_REPETITIVE_DAILY_PEAKS = 3

# @added 20230424 - Feature #4724: custom_algorithms - anomalous_daily_peak
# Added expiry to record metrics identified as normal by anomalous_daily_peaks
# to allow Mirage to set a key in a hash and to allow Analyzer labelled_metrics
# to skip analysis of those metrics for the expiry period.  This is to reduce
# metrics that are experiencing a normal anomalous daily peak to not have to
# be analysed by Analyzer every run and pushed to Mirage to check as this
# results in Mirage getting lots of unnecessary checks which caused feedback
# for the period in question.
anomalous_daily_peak_expiry = 180

skyline_app_graphite_namespace = 'skyline.%s%s.labelled_metrics' % (skyline_app, SERVER_METRIC_PATH)
failed_checks_dir = '%s_failed' % settings.MIRAGE_CHECK_PATH
# @added 20191107 - Branch #3262: py3
alert_test_file = '%s/%s_alert_test.txt' % (settings.SKYLINE_TMP_DIR, skyline_app)

# In Skyline a metric is either a counter (derivative) or a gauge
skyline_metric_types = {'COUNTER': 1, 'GAUGE': 0}
skyline_metric_types_by_id = {}
for o_key in list(skyline_metric_types.keys()):
    skyline_metric_types_by_id[skyline_metric_types[o_key]] = o_key

MIRAGE_LABELLED_CHECK_PATH = '%s_labelled_metrics' % settings.MIRAGE_CHECK_PATH


class MirageLabelledMetrics(Thread):
    """
    The MirageLabelledMetrics thread
    """
    def __init__(self, parent_pid):
        """
        Initialize the Mirage
        """
        super().__init__()
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = os.getpid()
        self.mirage_labelled_metrics_exceptions_q = Queue()
        self.mirage_labelled_metrics_anomaly_breakdown_q = Queue()
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
            # @added 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
            # Log warning
            logger.warning('warning :: parent or current process dead')
            sys.exit(0)

    # @added 20170127 - Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
    #                   Bug #1460: panorama check file fails
    #                   Panorama check file fails #24
    # Get rid of the skyline_functions imp as imp is deprecated in py3 anyway
    def mirage_load_metric_vars(self, metric_vars_file):
        """
        Load the metric variables for a check from a metric check variables file

        :param metric_vars_file: the path and filename to the metric variables files
        :type metric_vars_file: str
        :return: the metric_vars list or ``False``
        :rtype: list

        """
        if os.path.isfile(metric_vars_file):
            logger.info(
                'loading metric variables from metric_check_file - %s' % (
                    str(metric_vars_file)))
        else:
            logger.error(
                'error :: loading metric variables from metric_check_file - file not found - %s' % (
                    str(metric_vars_file)))
            return False

        metric_vars = []
        with open(metric_vars_file) as f:
            for line in f:
                no_new_line = line.replace('\n', '')
                no_equal_line = no_new_line.replace(' = ', ',')
                array = str(no_equal_line.split(',', 1))
                add_line = literal_eval(array)
                metric_vars.append(add_line)

        # @added 20200429 - Feature #3486: analyzer_batch
        #                   Feature #3480: batch_processing
        # Allow the check file to already hold a valid python list on one line
        # so that a check can be added by simply echoing to debug metric_vars
        # line from to log for any failed checks into a new Mirage check file
        # The original above pattern is still the default, this is for the check
        # files to be added by the operator from the log or for debugging.
        try_literal_eval = False
        if metric_vars:
            if isinstance(metric_vars, list):
                pass
            else:
                try_literal_eval = True
                logger.info('mirage_labelled_metrics :: metric_vars is not a list, set to try_literal_eval')
            if len(metric_vars) < 2:
                try_literal_eval = True
                logger.info('mirage_labelled_metrics :: metric_vars is not a list of lists, set to try_literal_eval')
        else:
            try_literal_eval = True
            logger.info('mirage_labelled_metrics :: metric_vars is not defined, set to try_literal_eval')
        if try_literal_eval:
            try:
                with open(metric_vars_file) as f:
                    for line in f:
                        metric_vars = literal_eval(line)
                        if metric_vars:
                            break
            except:
                logger.error(traceback.format_exc())
                logger.error('metric_vars not loaded with literal_eval')
                metric_vars = []

        string_keys = ['metric']
        float_keys = ['value']
        int_keys = ['hours_to_resolve', 'metric_timestamp']
        # @added 20200916 - Branch #3068: SNAB
        #                   Task #3744: POC matrixprofile
        boolean_keys = ['snab_only_check']

        # @added 20210304 - Feature #3642: Anomaly type classification
        #                   Feature #3970: custom_algorithm - adtk_level_shift
        # Added triggered_algorithms to mirage_check_file
        list_keys = ['triggered_algorithms']

        metric_vars_array = []
        for var_array in metric_vars:
            # @modified 20181023 - Feature #2618: alert_slack
            # Wrapped in try except for debugging issue where the
            # hours_to_resolve was interpolating to hours_to_resolve = "t"
            try:
                key = None
                value = None
                if var_array[0] in string_keys:
                    key = var_array[0]
                    _value_str = str(var_array[1]).replace("'", '')
                    value_str = str(_value_str).replace('"', '')
                    value = str(value_str)
                    if var_array[0] == 'metric':
                        metric = value
                if var_array[0] in float_keys:
                    key = var_array[0]
                    _value_str = str(var_array[1]).replace("'", '')
                    value_str = str(_value_str).replace('"', '')
                    value = float(value_str)
                if var_array[0] in int_keys:
                    key = var_array[0]
                    _value_str = str(var_array[1]).replace("'", '')
                    value_str = str(_value_str).replace('"', '')
                    value = int(float(value_str))
                # @added 20200916 - Branch #3068: SNAB
                #                   Task #3744: POC matrixprofile
                # Handle new snab_only_check boolean
                if var_array[0] in boolean_keys:
                    key = var_array[0]
                    logger.debug(
                        'debug :: boolean key - key: %s, value: %s' % (
                            str(var_array[0]), str(var_array[1])))
                    if str(var_array[1]) == '"True"':
                        value = True
                    else:
                        value = False

                # @added 20210304 - Feature #3642: Anomaly type classification
                #                   Feature #3970: custom_algorithm - adtk_level_shift
                # Added triggered_algorithms to mirage_check_file
                if var_array[0] in list_keys:
                    key = var_array[0]
                    logger.debug(
                        'debug :: list key - key: %s, value: %s' % (
                            str(var_array[0]), str(var_array[1])))
                    _value_str = str(var_array[1]).replace("'", '')
                    try:
                        value = literal_eval(var_array[1])
                    except Exception as e:
                        logger.error(
                            'error :: loading metric variables - failed to literal_eval list for %s, %s - %s' % (
                                str(key), str(var_array[1]), e))
                        value = []

                if key:
                    metric_vars_array.append([key, value])

                if len(metric_vars_array) == 0:
                    logger.error(
                        'error :: loading metric variables - none found from %s' % (
                            str(metric_vars_file)))
                    return False
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_labelled_metrics :: failed to load metric variables from check file - %s' % (metric_vars_file))
                return False

        logger.info('mirage_labelled_metrics :: debug :: metric_vars for %s' % str(metric))
        logger.info('mirage_labelled_metrics :: debug :: %s' % str(metric_vars_array))

        return metric_vars_array

    def create_check_file_from_hash_key(self, check_item):
        """
        Create a check file from an analyzer_labelled_metrics.mirage_check key
        """
        check = None
        hash_key = None
        redis_hash = 'analyzer_labelled_metrics.mirage_check'
        metric_prefix = None
        if check_item.startswith('analyzer_labelled_metrics.mirage_check.'):
            hash_key = check_item.replace('analyzer_labelled_metrics.mirage_check.', '', 1)
            metric_prefix = 'labelled_metrics'
        metric_data = {}
        try:
            metric_data_str = self.redis_conn_decoded.hget(redis_hash, hash_key)
            if metric_data_str:
                metric_data = literal_eval(metric_data_str)
        except Exception as err:
            logger.error('error :: mirage_labelled_metrics :: create_check_file_from_hash_key :: failed to get %s from %s from Redis hash - %s' % (
                str(hash_key), redis_hash, err))
        if metric_data:
            try:
                self.redis_conn_decoded.hdel(redis_hash, hash_key)
                logger.info('mirage_labelled_metrics :: create_check_file_from_hash_key :: removed %s from %s from Redis hash' % (
                    str(hash_key), redis_hash))
            except Exception as err:
                logger.error('error :: mirage_labelled_metrics :: create_check_file_from_hash_key :: failed to hdel %s from %s from Redis hash - %s' % (
                    str(hash_key), redis_hash, err))
            try:
                base_name = metric_data['metric']
                metric_id = metric_data['metric_id']
                if metric_prefix:
                    metric = '%s.%s' % (metric_prefix, str(metric_id))
                else:
                    metric = str(base_name)
                metric_timestamp = metric_data['timestamp']
                value = metric_data['value']
                triggered_algorithms = metric_data['triggered_algorithms']
                try:
                    use_hours_to_resolve = metric_data['hours_to_resolve']
                except:
                    use_hours_to_resolve = 168
                try:
                    snab_only_check = metric_data['snab_only_check']
                except:
                    snab_only_check = False
                if metric_prefix == 'labelled_metrics':
                    metric_data['source'] = 'victoriametrics'

                metric_dict = {}
                try:
                    metric_dict = metric_name_labels_parser(skyline_app, base_name)
                except Exception as err:
                    logger.error('error :: mirage_labelled_metrics :: create_check_file_from_hash_key :: metric_name_labels_parser failed for %s - %s' % (
                        base_name, err))
                for i_key in list(metric_dict.keys()):
                    metric_data['metric_dict'][i_key] = metric_dict[i_key]

                anomaly_check_file = '%s/%s.%s.txt' % (MIRAGE_LABELLED_CHECK_PATH, str(metric_timestamp), metric)
                try:
                    with open(anomaly_check_file, 'w') as fh:
                        fh.write('metric = "%s"\nvalue = "%s"\nhours_to_resolve = "%s"\nmetric_timestamp = "%s"\nsnab_only_check = "%s"\ntriggered_algorithms = %s\n' % (
                            metric, str(value), str(use_hours_to_resolve),
                            str(metric_timestamp), str(snab_only_check),
                            str(triggered_algorithms)))
                    mirage_anomaly_check_file_created = True
                except Exception as err:
                    logger.error('error :: mirage_labelled_metrics :: failed to write anomaly_check_file %s - %s' % (
                        anomaly_check_file, err))
                if mirage_anomaly_check_file_created:
                    os.chmod(anomaly_check_file, mode=0o644)
                check = '%s.%s.txt' % (str(metric_timestamp), metric)
            except Exception as err:
                logger.error('error :: mirage_labelled_metrics :: failed to create anomaly check file from metric_data: %s - %s' % (
                    str(metric_data), err))
        return check, metric_data

    # @added 20221105 - Feature #4724: custom_algorithms - anomalous_daily_peak
    def clear_trigger_history(self, metric):
        """
        Clear last item from the trigger history
        """
        trigger_history = {}
        try:
            raw_trigger_history = self.redis_conn_decoded.hget('mirage.trigger_history', metric)
            if raw_trigger_history:
                trigger_history = literal_eval(raw_trigger_history)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: mirage_labelled_metrics :: failed to evaluate data from mirage.trigger_history Redis hash key - %s' % (
                str(err)))
            return False
        if trigger_history:
            last_history_key = list(trigger_history.keys())[0]
            new_trigger_history = {}
            for history_timestamp in list(trigger_history.keys()):
                if history_timestamp != last_history_key:
                    new_trigger_history[history_timestamp] = trigger_history[history_timestamp]
            if new_trigger_history:
                try:
                    self.redis_conn_decoded.hset('mirage.trigger_history', metric, str(new_trigger_history))
                    logger.info('mirage_labelled_metrics :: removed last event for %s from mirage.trigger_history' % metric)
                except Exception as err:
                    logger.error('error :: mirage_labelled_metrics :: failed to set key in mirage.trigger_history Redis hash key - %s' % (
                        str(err)))
        return True

    # @modified 20200909 - Task #3730: Validate Mirage running multiple processes
    # def spin_process(self, i, run_timestamp):
    def spin_process(self, i, run_timestamp, assigned_checks):
        """
        Assign a metrics for a process to analyze.
        """

        if not assigned_checks:
            logger.info('mirage_labelled_metrics :: no checks to assign to process, nothing to do')
            return

        process_start_timestamp = int(time())

        # Make process-specific dicts
        exceptions = defaultdict(int)
        anomaly_breakdown = defaultdict(int)

        # TODO - testing
        redis_metrics_processed_key = 'mirage_labelled_metrics.%s.metrics_processed' % str(i)
        try:
            exists = self.redis_conn_decoded.exists(redis_metrics_processed_key)
            if exists:
                last_redis_metrics_processed_key = 'mirage_labelled_metrics.%s.metrics_processed.last' % str(i)
                self.redis_conn_decoded.rename(redis_metrics_processed_key, last_redis_metrics_processed_key)
        except Exception as err:
            logger.error('error :: mirage_labelled_metrics :: failed to rename %s Redis hash - %s' % (
                redis_metrics_processed_key, err))

        # @added 20230425 - Feature #4894: labelled_metrics - SKYLINE_FEEDBACK_NAMESPACES
        feedback_labelled_metric_ids = []
        feedback_labelled_metric_ids_skipped = []
        analyzer_labelled_metrics_busy = False
        if settings.SKYLINE_FEEDBACK_NAMESPACES:
            try:
                analyzer_labelled_metrics_busy = self.redis_conn_decoded.get('analyzer_labelled_metrics.busy')
            except Exception as err:
                logger.error('error :: mirage_labelled_metrics :: failed to get analyzer_labelled_metrics.busy Redis key' % (
                    err))

        # @added 20230605 - Feature #4932: mute_alerts_on
        mute_alerts_on = []
        if not analyzer_labelled_metrics_busy:
            mute_alerts_on_dict = {}
            try:
                mute_alerts_on_dict = self.redis_conn_decoded.hgetall('metrics_manager.mute_alerts_on')
            except Exception as err:
                logger.error('error :: failed to hgetall metrics_manager.mute_alerts_on - %s' % (
                    err))
            mute_alerts_on = [i_metric for i_metric in list(mute_alerts_on_dict.keys()) if i_metric.startswith('labelled_metrics.')]
            if mute_alerts_on:
                logger.info('mirage_labelled_metrics :: there are %s mute_alert_on labelled_metrics currently which shall be add to feedback_labelled_metric_ids if not set' % str(len(mute_alerts_on)))

        if analyzer_labelled_metrics_busy:
            logger.info('mirage_labelled_metrics :: analyzer_labelled_metrics_busy found')
            try:
                feedback_labelled_metric_ids = list(self.redis_conn_decoded.smembers('aet.metrics_manager.feedback.labelled_metric_ids'))
            except Exception as err:
                logger.error('error :: mirage_labelled_metrics :: smembers failed on Redis set aet.metrics_manager.feedback.labelled_metric_ids - %s' % (
                    err))

        # @added 20230605 - Feature #4932: mute_alerts_on
        if not feedback_labelled_metric_ids and mute_alerts_on:
            for labelled_metric in mute_alerts_on:
                metric_id = labelled_metric.split('.')[-1]
                feedback_labelled_metric_ids.append(str(metric_id))
            logger.info('mirage_labelled_metrics :: added %s mute_alert_on metric ids to feedback_labelled_metric_ids if not set' % str(len(mute_alerts_on)))
            if not analyzer_labelled_metrics_busy:
                analyzer_labelled_metrics_busy = True
                logger.info('mirage_labelled_metrics :: analyzer_labelled_metrics_busy set to True because mute_alert_on labelled_metrics were found')

        redis_set = 'analyzer.waterfall_alerts.sent_to_mirage'
        literal_analyzer_waterfall_alerts = []
        try:
            literal_analyzer_waterfall_alerts = list(self.redis_conn_decoded.smembers(redis_set))
        except:
            literal_analyzer_waterfall_alerts = []
        analyzer_waterfall_alerts = []
        for literal_waterfall_alert in literal_analyzer_waterfall_alerts:
            waterfall_alert = literal_eval(literal_waterfall_alert)
            analyzer_waterfall_alerts.append(waterfall_alert)

        # @added 20230424 - Feature #4724: custom_algorithms - anomalous_daily_peak
        # Added expiry to record metrics identified as normal by anomalous_daily_peaks
        current_now = int(time())
        current_aligned_ts = int(process_start_timestamp // 60 * 60)
        normal_daily_peaks_keys = []
        for i in sorted(list(range(1, 3)), reverse=True):
            key_ts = current_aligned_ts - (60 * i)
            key = 'mirage.normal_daily_peak_metrics.%s' % str(key_ts)
            normal_daily_peaks_keys.append(key)
        normal_daily_peak_metrics_expiry = {}
        for normal_daily_peaks_key in normal_daily_peaks_keys:
            try:
                current_key_data  = self.redis_conn_decoded.hgetall(normal_daily_peaks_key)
            except Exception as err:
                logger.error('error :: mirage_labelled_metrics :: hgetall failed on Redis hash %s - %s' % (
                    str(normal_daily_peaks_key), err))
            for labelled_metric in list(current_key_data):
                if current_now > int(float(current_key_data[labelled_metric])):
                    try:
                        self.redis_conn_decoded.hdel(normal_daily_peaks_key, labelled_metric)
                    except Exception as err:
                        logger.error('error :: mirage_labelled_metrics :: failed to remove %s from Redis hash %s - %s' % (
                            labelled_metric, normal_daily_peaks_key, err))
                else:
                    normal_daily_peak_metrics_expiry[labelled_metric] = int(float(current_key_data[labelled_metric]))

        checks_processed = 0

        ionosphere_unique_metrics = []

        for check_item in assigned_checks:
            check = str(check_item)

            if int(time()) >= (process_start_timestamp + 50):
                logger.info('mirage_labelled_metrics :: run time limit reached - stopping')
                break

            checks_processed += 1

            metric_data = {
                'source': 'graphite',
            }
            is_labelled_metric = False
            if check_item.startswith('analyzer_labelled_metrics.mirage_check.'):
                is_labelled_metric = True
            if is_labelled_metric:
                # Create a check file for backwards compatibility
                try:
                    check, metric_data = self.create_check_file_from_hash_key(check_item)
                except Exception as err:
                    logger.error('error :: mirage_labelled_metrics :: create_check_file_from_hash_key failed for %s - %s' % (
                        str(check_item), err))
                    continue
            else:
                continue

            logger.info('mirage_labelled_metrics :: checking metric_data: %s' % str(metric_data))

            metric_id = 0
            try:
                metric_id = metric_data['metric_id']
            except Exception as err:
                logger.error('error :: mirage_labelled_metrics :: failed to determine metric_id from metric_data for %s - %s' % (
                    check_item, err))

            # TODO - testing
            try:
                key = 'labelled_metrics.%s-%s' % (str(metric_id), str(metric_data['timestamp']))
                self.redis_conn_decoded.hset(redis_metrics_processed_key, key, str(metric_data))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_labelled_metrics :: failed to add metric to %s Redis hash - %s' % (redis_metrics_processed_key, err))

            metric_check_file = '%s/%s' % (
                # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                # settings.MIRAGE_CHECK_PATH, str(metric_var_files_sorted[0]))
                MIRAGE_LABELLED_CHECK_PATH, check)

            check_file_name = os.path.basename(str(metric_check_file))
            check_file_timestamp = check_file_name.split('.', 1)[0]
            check_file_metricname_txt = check_file_name.split('.', 1)[1]
            check_file_metricname = check_file_metricname_txt.replace('.txt', '')
            check_file_metricname_dir = check_file_metricname.replace('.', '/')
            metric_failed_check_dir = '%s/%s/%s' % (failed_checks_dir, check_file_metricname_dir, check_file_timestamp)

            # Load metric variables
            # @modified 20160822 - Bug #1460: panorama check file fails
            # Changed to panorama style skyline_functions load_metric_vars
            # self.load_metric_vars(metric_check_file)
            # Load and validate metric variables
            try:
                # @modified 20170127 - Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
                #                      Bug #1460: panorama check file fails
                #                      Panorama check file fails #24
                # Get rid of the skyline_functions imp as imp is deprecated in py3 anyway
                # metric_vars = load_metric_vars(skyline_app, str(metric_check_file))
                metric_vars_array = self.mirage_load_metric_vars(str(metric_check_file))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_labelled_metrics :: failed to load metric variables from check file - %s' % str(metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                continue

            metric = None

            # @added 20200106 - Branch #3262: py3
            #                   Task #3034: Reduce multiprocessing Manager list usage
            # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
            # redis_set_to_delete = 'mirage.metric_variables'
            redis_metric_variables_set = 'mirage_labelled_metrics.%s.metric_variables' % str(i)
            redis_set_to_delete = redis_metric_variables_set
            try:
                self.redis_conn.delete(redis_set_to_delete)
                logger.info('mirage_labelled_metrics :: deleted Redis set - %s' % redis_set_to_delete)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_labelled_metrics :: failed to delete Redis set - %s' % redis_set_to_delete)

            try:
                key = 'metric'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                metric = str(value_list[0])
                metric_name = ['metric_name', metric]
                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # self.metric_variables.append(metric_name)
                # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                redis_set = 'mirage_labelled_metrics.metric_variables'
                data = str(metric_name)
                try:
                    # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                    # self.redis_conn.sadd(redis_set, data)
                    self.redis_conn.sadd(redis_metric_variables_set, data)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_labelled_metrics :: failed to add %s to Redis set %s' % (
                        str(data), str(redis_set)))

                logger.info('mirage_labelled_metrics :: debug :: added metric_name %s from check file - %s' % (str(metric_name), metric_check_file))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_labelled_metrics :: failed to read metric variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                continue
            if not metric:
                logger.error('error :: mirage_labelled_metrics :: failed to load metric variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                continue

            value = None
            try:
                key = 'value'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                value = float(value_list[0])
                metric_value = ['metric_value', value]
                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # self.metric_variables.append(metric_value)
                redis_set = 'mirage_labelled_metrics.metric_variables'
                data = str(metric_value)
                try:
                    # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                    # self.redis_conn.sadd(redis_set, data)
                    self.redis_conn.sadd(redis_metric_variables_set, data)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_labelled_metrics :: failed to add %s to Redis set %s' % (
                        str(data), str(redis_set)))
            except Exception as err:
                logger.error('error :: mirage_labelled_metrics :: failed to read value variable from check file - %s - %s' % (metric_check_file, err))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                continue
            if not value:
                # @modified 20181119 - Bug #2708: Failing to load metric vars
                if value == 0.0:
                    pass
                else:
                    logger.error('error :: mirage_labelled_metrics :: failed to load value variable from check file - %s' % (metric_check_file))
                    fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                    continue

            hours_to_resolve = None
            try:
                key = 'hours_to_resolve'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                hours_to_resolve = int(value_list[0])
                hours_to_resolve_list = ['hours_to_resolve', hours_to_resolve]
                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # self.metric_variables.append(hours_to_resolve_list)
                redis_set = 'mirage_labelled_metrics.metric_variables'
                data = str(hours_to_resolve_list)
                try:
                    # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                    # self.redis_conn.sadd(redis_set, data)
                    self.redis_conn.sadd(redis_metric_variables_set, data)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_labelled_metrics :: failed to add %s to Redis set %s' % (
                        str(data), str(redis_set)))
            except:
                logger.error('error :: mirage_labelled_metrics :: failed to read hours_to_resolve variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                continue
            if not hours_to_resolve:
                logger.error('error :: mirage_labelled_metrics :: failed to load hours_to_resolve variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                continue

            metric_timestamp = None
            try:
                key = 'metric_timestamp'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                metric_timestamp = int(value_list[0])
                metric_timestamp_list = ['metric_timestamp', metric_timestamp]
                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # self.metric_variables.append(metric_timestamp_list)
                redis_set = 'mirage_labelled_metrics.metric_variables'
                data = str(metric_timestamp_list)
                try:
                    # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                    # self.redis_conn.sadd(redis_set, data)
                    self.redis_conn.sadd(redis_metric_variables_set, data)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_labelled_metrics :: failed to add %s to Redis set %s' % (
                        str(data), str(redis_set)))
            except:
                logger.error('error :: mirage_labelled_metrics :: failed to read metric_timestamp variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                continue
            if not metric_timestamp:
                logger.error('error :: mirage_labelled_metrics :: failed to load metric_timestamp variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                continue

            # @added 20200916 - Branch #3068: SNAB
            #                   Task #3744: POC matrixprofile
            snab_only_check = None
            try:
                key = 'snab_only_check'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                snab_only_check = value_list[0]
            except:
                snab_only_check = None

            snab_only_check_list = ['snab_only_check', snab_only_check]
            redis_set = 'mirage_labelled_metrics.metric_variables'
            data = str(snab_only_check_list)
            try:
                self.redis_conn.sadd(redis_metric_variables_set, data)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_labelled_metrics :: failed to add %s to Redis set %s' % (
                    str(data), str(redis_set)))

            # @added 20210304 - Feature #3642: Anomaly type classification
            #                   Feature #3970: custom_algorithm - adtk_level_shift
            # Added triggered_algorithms to mirage_check_file
            try:
                key = 'triggered_algorithms'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                triggered_algorithms = value_list[0]
            except:
                triggered_algorithms = []

            metric_data_dir = '%s/%s' % (settings.MIRAGE_DATA_FOLDER, str(metric))

            # Ignore any metric check with a timestamp greater than MIRAGE_STALE_SECONDS
            int_metric_timestamp = int(metric_timestamp)
            int_run_timestamp = int(run_timestamp)
            metric_timestamp_age = int_run_timestamp - int_metric_timestamp

            if metric_timestamp_age > settings.MIRAGE_STALE_SECONDS:
                logger.info('mirage_labelled_metrics :: stale check :: %s check request is %s seconds old - discarding' % (metric, str(metric_timestamp_age)))
                # Remove metric check file
                if os.path.isfile(metric_check_file):
                    os.remove(metric_check_file)
                    logger.info('mirage_labelled_metrics :: removed check file - %s' % (metric_check_file))
                else:
                    logger.info('mirage_labelled_metrics :: could not remove check file - %s' % (metric_check_file))

                # Remove the metric directory
                if os.path.exists(metric_data_dir):
                    try:
                        rmtree(metric_data_dir)
                        logger.info('mirage_labelled_metrics :: removed data dir - %s' % metric_data_dir)
                    except:
                        logger.error('error :: mirage_labelled_metrics :: failed to rmtree - %s' % metric_data_dir)

                # @added 20200903 - Task #3730: Validate Mirage running multiple processes
                redis_set = 'mirage_labelled_metrics.stale_check_discarded'
                try:
                    self.redis_conn.sadd(redis_set, str(metric))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_labelled_metrics :: failed to add %s to Redis set %s' % (
                        str(metric), str(redis_set)))
                continue

            # Calculate hours second order resolution to seconds
            second_order_resolution_seconds = int(hours_to_resolve) * 3600
            int_second_order_resolution_seconds = int(float(second_order_resolution_seconds))
            second_resolution_timestamp = int_metric_timestamp - int_second_order_resolution_seconds

            labelled_metric_base_name = None
            labelled_metric_name = str(metric)
            try:
                labelled_metric_base_name = get_base_name_from_labelled_metrics_name(skyline_app, labelled_metric_name)
            except Exception as err:
                logger.error('error :: mirage_labelled_metrics :: get_base_name_from_labelled_metrics_name failed for %s - %s' % (
                    str(labelled_metric_name), str(err)))

            trigger_anomaly = False
            test_alert_and_trigger = False
            test_alert = False
            if 'test_alert' in metric_data:
                test_alert = metric_data['test_alert']
            if 'trigger_anomaly' in metric_data:
                trigger_anomaly = metric_data['trigger_anomaly']
                test_alert_and_trigger = True
            if test_alert:
                logger.info('test_alert found for %s set trigger_anomaly: %s' % (
                    str(metric), str(trigger_anomaly)))
                alert_tested_key = 'mirage.test_alerts.done.%s' % metric
                try:
                    self.redis_conn_decoded.setex(alert_tested_key, 300, int(time()))
                    logger.info('test_alert created Redis key %s' % alert_tested_key)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to create Redis key %s - %s' % (
                        alert_tested_key, err))

            # Remove any old json file related to the metric
            metric_json_file = '%s/%s.json' % (metric_data_dir, str(metric))
            try:
                os.remove(metric_json_file)
            except OSError:
                pass

            # @added 20230425 - Feature #4894: labelled_metrics - SKYLINE_FEEDBACK_NAMESPACES
            feedback_metric = False
            feedback_key = 'mirage_labelled_metrics.feedback.expiry.%s' % str(metric_id)
            if analyzer_labelled_metrics_busy:
                if str(metric_id) in feedback_labelled_metric_ids:
                    feedback_metric = True
            feedback_metric_expiry = 0
            if feedback_metric:
                try:
                    feedback_metric_expiry = self.redis_conn_decoded.get(feedback_key)
                except Exception as err:
                    logger.error('error :: mirage_labelled_metrics :: failed to get Redis key %s - %s' % (
                        str(feedback_key), err))
                    feedback_metric_expiry = 0
            if feedback_metric_expiry:
                logger.info('mirage_labelled_metrics :: feedback metric expiry exists, removing check :: %s ' % metric)
                # Remove metric check file
                if os.path.isfile(metric_check_file):
                    os.remove(metric_check_file)
                    logger.info('mirage_labelled_metrics :: removed check file - %s' % (metric_check_file))
                else:
                    logger.info('mirage_labelled_metrics :: could not remove check file - %s' % (metric_check_file))
                # Remove the metric directory
                if os.path.exists(metric_data_dir):
                    try:
                        rmtree(metric_data_dir)
                        logger.info('mirage_labelled_metrics :: removed data dir - %s' % metric_data_dir)
                    except:
                        logger.error('error :: mirage_labelled_metrics :: failed to rmtree - %s' % metric_data_dir)
                redis_set = 'analyzer.waterfall_alerts.sent_to_mirage'
                for waterfall_alert in analyzer_waterfall_alerts:
                    if waterfall_alert[0] == metric:
                        if int(waterfall_alert[1]) == metric_timestamp:
                            try:
                                self.redis_conn.srem(redis_set, str(waterfall_alert))
                                logger.info('mirage_labelled_metrics :: removed waterfall alert item from Redis set %s - %s' % (
                                    redis_set, str(waterfall_alert)))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: mirage_labelled_metrics :: failed to remove waterfall alert item for %s at %s from Redis set %s' % (
                                    metric, str(metric_timestamp), redis_set))
                continue

            data_source = metric_data['source']

            # Get data from graphite
            if data_source == 'graphite':
                logger.error('error :: mirage_labelled_metrics :: data_source set to graphite for %s' % (
                    metric))

            # Get data from victoriametrics
            if data_source == 'victoriametrics':
                base_name = metric_data['metric']

                logger.info(
                    'retrieve data :: surfacing %s time series from victoriametrics for %s seconds' % (
                        base_name, str(second_order_resolution_seconds)))

                # @added 20230522 - metric_type.longterm_expire
                # Check that monotonicity of the metric at second_order_resolution_seconds
                # to determine whether it is different from the recorded longterm type
                current_metric_type = None
                try:
                    current_metric_type = self.redis_conn_decoded.hget('skyline.labelled_metrics.id.type', str(metric_id))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_labelled_metrics :: failed to determine metric_type from skyline.labelled_metrics.id.type - %s' % (
                        err))
                test_timeseries = []
                update_metric_type = False
                save_test_data = False
                if current_metric_type:
                    try:
                        test_timeseries = get_victoriametrics_metric(
                            skyline_app, base_name, second_resolution_timestamp,
                            metric_timestamp, 'list', 'object', metric_data={},
                            plot_parameters={}, do_not_type=True)
                    except Exception as err:
                        logger.error('error :: mirage_labelled_metrics :: get_victoriametrics_metric failed getting test_timeseries for %s - %s' % (
                            base_name, err))
                if test_timeseries:
                    is_strictly_increasing_monotonic = None
                    try:
                        is_strictly_increasing_monotonic = strictly_increasing_monotonicity(test_timeseries)
                    except Exception as err:
                        logger.error('error :: mirage_labelled_metrics :: get_victoriametrics_metric failed getting test_timeseries for %s - %s' % (
                            base_name, err))
                    determined_metric_type = None
                    if str(is_strictly_increasing_monotonic) != 'None':
                        if is_strictly_increasing_monotonic:
                            determined_metric_type = '1'
                        else:
                            determined_metric_type = '0'
                    if str(determined_metric_type) != 'None':
                        if current_metric_type != determined_metric_type:
                            update_metric_type = True
                        else:
                            if determined_metric_type == '0':
                                save_test_data = False
                if update_metric_type:
                    logger.info('mirage_labelled_metrics :: detected change in metric_type updating from %s to %s for %s ' % (
                        current_metric_type, determined_metric_type, metric))
                    try:
                        self.redis_conn_decoded.hset('skyline.labelled_metrics.id.type', str(metric_id), determined_metric_type)
                    except Exception as err:
                        logger.error('error :: mirage_labelled_metrics :: failed to determine update skyline.labelled_metrics.id.type with metric_type - %s' % (
                            err))
                    # Set a new expire in the longterm_expire hash to have metrics_manager
                    # recheck the metric at 30 days.
                    try:
                        new_expire = int(metric_timestamp) - 3600
                        self.redis_conn_decoded.hset('skyline.labelled_metrics.id.type.longterm_expire', str(metric_id), str(new_expire))
                    except Exception as err:
                        logger.error('error :: mirage_labelled_metrics :: failed to update skyline.labelled_metrics.id.type.longterm_expire with new expire value - %s' % (
                            err))
                if save_test_data:
                    output_object_path = os.path.dirname(metric_json_file_saved)
                    if not os.path.isdir(output_object_path):
                        try:
                            mkdir_p(output_object_path)
                            logger.info(
                                'output_object_path - %s' % str(output_object_path))
                        except Exception as err:
                            logger.error(
                                'error :: failed to create output_object_path - %s - %s' % (
                                str(output_object_path,), err))
                    with open(metric_json_file_saved, 'w') as f:
                        f.write(json.dumps(test_timeseries))
                    os.chmod(metric_json_file_saved, mode=0o644)

                # @modified 20230522 - metric_type.longterm_expire
                # Only surface if the test data was not saved
                if not save_test_data:
                    try:
                        # get_victoriametrics_metric automatically applies the rate and
                        # step required no downsampling or nonNegativeDerivative is
                        # required.
                        metric_json_file_saved = get_victoriametrics_metric(
                            skyline_app, base_name, second_resolution_timestamp,
                            metric_timestamp, 'json', metric_json_file, metric_data)
                        if metric_json_file_saved:
                            logger.info('mirage_labelled_metrics :: %s time series data saved to %s' % (metric, metric_json_file_saved))
                    except Exception as err:
                        logger.error('error :: mirage_labelled_metrics :: get_victoriametrics_metric failed for %s - %s' % (
                            str(metric_json_file), err))

            # Check there is a json timeseries file to test
            if not os.path.isfile(metric_json_file):
                logger.error(
                    'error :: retrieve failed - failed to surface %s time series from %s' % (
                        metric, data_source))

                # @added 20200905 - Feature #3734: waterfall alerts
                # Try a metric 3 times before removing the check file
                remove_check_file = True
                check_failed_key = 'mirage.check.data_retrieval_failed.%s.%s' % (
                    str(int_metric_timestamp), metric)
                fail_count = 0
                try:
                    fail_count = self.redis_conn.get(check_failed_key)
                except:
                    fail_count = 0
                if not fail_count:
                    fail_count = 0
                fail_count += 1
                if fail_count < 3:
                    remove_check_file = False
                    try:
                        self.redis_conn.setex(check_failed_key, 300, fail_count)
                        logger.info('mirage_labelled_metrics :: updated fail_count to %s in %s' % (str(fail_count), check_failed_key))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_labelled_metrics :: failed to set Redis key %s with %s' % (
                            str(check_failed_key), str(fail_count)))
                else:
                    logger.error('error :: mirage_labelled_metrics :: fail_count is %s in %s, removing check file' % (str(fail_count), check_failed_key))

                if remove_check_file:
                    # Remove metric check file
                    try:
                        os.remove(metric_check_file)
                    except OSError:
                        pass
                    # Remove the metric directory
                    try:
                        rmtree(metric_data_dir)
                        logger.info('mirage_labelled_metrics :: removed data dir - %s' % metric_data_dir)
                    except:
                        logger.error('error :: mirage_labelled_metrics :: failed to rmtree %s' % metric_data_dir)
                continue

            logger.info('mirage_labelled_metrics :: retrieved data :: for %s at %s seconds' % (
                metric, str(second_order_resolution_seconds)))

            # Make process-specific dicts
            # exceptions = defaultdict(int)
            # anomaly_breakdown = defaultdict(int)

            self.check_if_parent_is_alive()

            timeseries = []
            try:
                with open((metric_json_file), 'r') as f:
                    timeseries = json.loads(f.read())
                logger.info('mirage_labelled_metrics :: data points surfaced :: %s' % (str(len(timeseries))))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_labelled_metrics :: failed to create timeseries from %s - %s' % (
                    str(metric_json_file), err))
                timeseries = []

            # @added 20170212 - Feature #1886: Ionosphere learn
            # Only process if the metric has sufficient data
            first_timestamp = None
            try:
                first_timestamp = int(timeseries[0][0])
            except Exception as err:
                logger.error('error :: mirage_labelled_metrics :: could not determine first timestamp - %s' % err)
            timestamp_now = int(time())
            valid_if_before_timestamp = timestamp_now - int(settings.FULL_DURATION)
            valid_mirage_timeseries = True
            if first_timestamp:
                if first_timestamp > valid_if_before_timestamp:
                    valid_mirage_timeseries = False
            else:
                valid_mirage_timeseries = False
                logger.warning('warning :: no first_timestamp, valid_mirage_timeseries: %s' % str(valid_mirage_timeseries))

            valid_mirage_timeseries = True

            redis_metric_name = '%s' % str(metric)
            metric_id = 0
            if is_labelled_metric:
                metric_id = metric_data['metric_id']
                redis_metric_name = 'labelled_metrics.%s' % str(metric_id)

            # @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
            # Determine if any metrcs have negatives values some they can be
            # added to the ionosphere.untrainable_metrics Redis set
            run_negatives_present = False

            # @added 20200607 - Feature #3566: custom_algorithms
            algorithms_run = list(settings.MIRAGE_ALGORITHMS)

            # @added 20200904 - Feature #3734: waterfall alerts
            anomalous = None

            # @added 20201001 - Branch #3068: SNAB
            #                   Task #3748: POC SNAB
            # Add timings
            analysis_start_time = time()

            try:
                if valid_mirage_timeseries:
                    logger.info('mirage_labelled_metrics :: analyzing :: %s at %s seconds' % (metric, second_order_resolution_seconds))
                    # @modified 20230118 - Task #4786: Switch from matrixprofile to stumpy
                    #                      Task #4778: v4.0.0 - update dependencies
                    # Added current_func
                    anomalous, ensemble, datapoint, negatives_found, algorithms_run = run_selected_algorithm(timeseries, metric, second_order_resolution_seconds, run_negatives_present, triggered_algorithms, current_func='mirage_labelled_metrics')

                    logger.info('mirage_labelled_metrics :: analysed :: %s - anomalous: %s' % (metric, str(anomalous)))
                else:
                    logger.info('mirage_labelled_metrics :: not analyzing :: %s at %s seconds as there is not sufficiently older datapoints in the timeseries - not valid_mirage_timeseries' % (metric, second_order_resolution_seconds))
                    anomalous = False
                    if timeseries:
                        datapoint = timeseries[-1][1]
                    else:
                        datapoint = 0
                    # @added 20220315 - Feature #4482: Test alerts
                    # Allow to test on sparse metrics
                    if test_alert or test_alert_and_trigger:
                        anomalous = True
                        logger.info('mirage_labelled_metrics :: test_alert - setting anomalous to True for %s' % metric)
                        ensemble = [True]
                        triggered_algorithms = ['testing']
                        algorithms_run = ['histogram_bins']
                        negatives_found = False

            # It could have been deleted by the Roomba
            except TypeError:
                # @added 20200430 - Feature #3480: batch_processing
                # Added logging here as the DeletedByRoomba exception is
                # generally not related to that but related to some other fail
                # in the processing of the run algorithms phase.
                # It could have been deleted by the Roomba, but Mirage does not use
                # Redis data so probably, definitely was not :)
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_labelled_metrics :: added as DeletedByRoomba but possibly not see traceback above')

                exceptions['DeletedByRoomba'] += 1
                logger.info('mirage_labelled_metrics :: exceptions        :: DeletedByRoomba')
            except TooShort:
                exceptions['TooShort'] += 1
                logger.info('mirage_labelled_metrics :: exceptions        :: TooShort')
            except Stale:
                exceptions['Stale'] += 1
                logger.info('mirage_labelled_metrics :: exceptions        :: Stale')
            except Boring:
                exceptions['Boring'] += 1
                logger.info('mirage_labelled_metrics :: exceptions        :: Boring')
            except Exception as err:
                exceptions['Other'] += 1
                logger.info('mirage_labelled_metrics :: exceptions        :: Other')
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_labelled_metrics :: unhandled error - %s' % err)


            # @added 20230425 - Feature #4894: labelled_metrics - SKYLINE_FEEDBACK_NAMESPACES
            if feedback_metric:
                try:
                    self.redis_conn_decoded.setex(feedback_key, 300, int(time()))
                except Exception as err:
                    logger.error('error :: mirage_labelled_metrics :: setex failed on %s - %s' % (
                        feedback_key, err))

            # @added 20220420 - Feature #4530: namespace.analysed_events
            parent_namespace = metric.split('.', maxsplit=1)[0]

            if is_labelled_metric:
                try:
                    parent_namespace = metric_data['metric_dict']['labels']['_tenant_id']
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_labelled_metrics :: failed to determine parent_namespace from metric_data: %s - %s' % (
                        str(metric_data), err))

            date_string = str(strftime('%Y-%m-%d', gmtime()))
            namespace_analysed_events_hash = 'namespace.analysed_events.%s.%s' % (skyline_app, date_string)
            try:
                self.redis_conn.hincrby(namespace_analysed_events_hash, parent_namespace, 1)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_labelled_metrics :: failed to increment %s Redis hash - %s' % (
                    namespace_analysed_events_hash, err))
            try:
                self.redis_conn.expire(namespace_analysed_events_hash, (86400 * 15))
                logger.info('mirage_labelled_metrics :: updated %s Redis hash' % namespace_analysed_events_hash)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_labelled_metrics :: failed to set expire %s Redis hash - %s' % (
                    namespace_analysed_events_hash, err))

            # @added 20201001 - Branch #3068: SNAB
            #                   Task #3748: POC SNAB
            # Add timings
            analysis_run_time = time() - analysis_start_time
            logger.info('mirage_labelled_metrics :: algorithms analysis completed in %.2f seconds' % (
                analysis_run_time))

            # @added 20210309 - Task #3730: Validate Mirage running multiple processes
            # Reimplement mirage.checks.done count
            try:
                self.redis_conn.incr('mirage_labelled_metrics.checks.done')
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_labelled_metrics :: failed to increment mirage_labelled_metrics.checks.done Redis key - %s' % str(err))

            # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
            # base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
            if metric.startswith(settings.FULL_NAMESPACE):
                base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
            else:
                base_name = metric

            # @added 20200904 - Feature #3734: waterfall alerts
            # Remove the metric from the waterfall_alerts Redis set
            # [metric, timestamp, value, added_to_waterfall_timestamp]
            # waterfall_data = [metric[1], metric[2], metric[0], added_to_waterfall_timestamp. waterfall_panorama_data]
            redis_set = 'analyzer.waterfall_alerts.sent_to_mirage'
            get_waterfall_alerts_once = True
            if not get_waterfall_alerts_once:
                literal_analyzer_waterfall_alerts = []
                try:
                    literal_analyzer_waterfall_alerts = list(self.redis_conn_decoded.smembers(redis_set))
                except:
                    literal_analyzer_waterfall_alerts = []
                analyzer_waterfall_alerts = []
                for literal_waterfall_alert in literal_analyzer_waterfall_alerts:
                    waterfall_alert = literal_eval(literal_waterfall_alert)
                    analyzer_waterfall_alerts.append(waterfall_alert)

            if test_alert or test_alert_and_trigger:
                anomalous = True
                logger.info('mirage_labelled_metrics :: test_alert - setting anomalous to True for %s' % metric)
                ensemble = [True]
                triggered_algorithms = ['testing']
                algorithms_run = ['histogram_bins']
                negatives_found = False

            # @added 20230419 - Feature #4848: mirage - analyse.irregular.unstable.timeseries.at.30days
            irregular_unstable_timeseries = False
            if anomalous:
                low_variance = 0.009
                normalized_var = None
                start_normalized_var = time()
                try:
                    normalized_var = normalized_variance(timeseries)
                except Exception as err:
                    logger.error('error :: mirage_labelled_metrics :: normalized_variance failed on timeseries for %s - %s' % (
                        metric, err))
                if isinstance(normalized_var, dict):
                    err = normalized_var['error']
                    logger.error('error :: mirage_labelled_metrics :: normalized_variance reported an error with timeseries for %s - %s' % (
                        metric, err))
                    normalized_var = None
                if isinstance(normalized_var, float):
                    if normalized_var <= low_variance:
                        irregular_unstable_timeseries = True
                logger.info('mirage_labelled_metrics :: normalized_variance ran with result: %s (took %.6f seconds), for %s' % (
                    str(normalized_var), (time() - start_normalized_var), metric))

            # @added 20230515 - Feature #4848: mirage - analyse.irregular.unstable.timeseries.at.30days
            if anomalous and irregular_unstable_timeseries:
                logger.info('mirage_labelled_metrics :: checking irregular_unstable %s' % (
                    metric))
                result = True
                start_irregular_unstable = time()
                try:
                    custom_algorithm = 'irregular_unstable'
                    custom_algorithms_to_run = {}
                    custom_algorithms_to_run[custom_algorithm] = {
                        'namespaces': ['labelled_metrics'],
                        'algorithm_source': '/opt/skyline/github/skyline/skyline/custom_algorithms/irregular_unstable.py',
                        'algorithm_parameters': {
                            'low_variance': 0.009, 'labelled_metric_name': labelled_metric_name,
                            'metric': labelled_metric_base_name,
                            'debug_logging': True,
                        },
                        'max_execution_time': 3.0,
                        'consensus': 1,
                        'algorithms_allowed_in_consensus': ['irregular_unstable'],
                        # 'debug_logging': False,
                        'debug_logging': True,
                        'run_3sigma_algorithms': False,
                        'run_before_3sigma': False,
                        'run_only_if_consensus': False,
                        'trigger_history_override': False,
                        'use_with': ['mirage'],
                    }
                    # use_debug_logging_here = False
                    use_debug_logging_here = True
                    result, anomalyScore = run_custom_algorithm_on_timeseries(skyline_app, os.getpid(), labelled_metric_name, timeseries, 'irregular_unstable', custom_algorithms_to_run[custom_algorithm], use_debug_logging_here, current_func='mirage_labelled_metrics')
                    algorithms_run.append(custom_algorithm)
                    ensemble.append(result)
                    if DEBUG_CUSTOM_ALGORITHMS or use_debug_logging_here:
                        logger.debug('debug :: mirage_labelled_metrics :: run_custom_algorithm_on_timeseries run irregular_unstable with result - %s, anomalyScore - %s' % (
                            str(result), str(anomalyScore)))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_labelled_metrics :: run_custom_algorithm_on_timeseries irregular_unstable failed on %s - %s' % (
                        str(metric), err))
                    result = None
                logger.info('mirage_labelled_metrics :: irregular_unstable ran with result: %s (took %.6f seconds), for %s - %s' % (
                    str(result), (time() - start_irregular_unstable), labelled_metric_name, labelled_metric_base_name))
                # Although fine in a notebook does not have the desired effect
                # in the runtime so convert to a str and check
                # if result is False:
                if str(result) == 'False':
                    logger.info('mirage_labelled_metrics :: irregular_unstable is overrriding anomalous result for %s' % (
                        labelled_metric_name))
                    anomalous = False
                    # Clear last item from the trigger history as anomalous_daily_peak
                    # is a 3sigma method after all
                    try:
                        cleared_trigger_history = self.clear_trigger_history(labelled_metric_name)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_labelled_metrics :: clear_trigger_history failed on %s - %s' % (
                            str(labelled_metric_name), err))

            # @added 20221105 - Feature #4724: custom_algorithms - anomalous_daily_peak
            # Determine if an anomaly is a normal peak value of normal magnitude
            # that occurs daily in a 7 day period
            if anomalous and MIRAGE_CHECK_REPETITIVE_DAILY_PEAKS:
                logger.info('mirage_labelled_metrics :: checking anomalous_daily_peak %s' % (
                    metric))
                result = True
                start_anomalous_daily_peak = time()
                try:
                    custom_algorithm = 'anomalous_daily_peak'
                    custom_algorithms_to_run = {}
                    custom_algorithms_to_run[custom_algorithm] = {
                        'namespaces': ['labelled_metrics'],
                        'algorithm_source': '/opt/skyline/github/skyline/skyline/custom_algorithms/anomalous_daily_peak.py',
                        'algorithm_parameters': {
                            'number_of_daily_peaks': MIRAGE_CHECK_REPETITIVE_DAILY_PEAKS,
                            'within_percent_of_normal_peaks': 20.0,
                            # @added 20230424 - Feature #4724: custom_algorithms - anomalous_daily_peak
                            # Added expiry to record metrics identified as normal by anomalous_daily_peaks
                            'expiry': anomalous_daily_peak_expiry,
                            'debug_logging': True,
                        },
                        'max_execution_time': 2.0,
                        'consensus': 1,
                        'algorithms_allowed_in_consensus': ['anomalous_daily_peak'],
                        # 'debug_logging': False,
                        'debug_logging': True,
                        'run_3sigma_algorithms': False,
                        'run_before_3sigma': False,
                        'run_only_if_consensus': False,
                        'trigger_history_override': False,
                        'use_with': ['mirage'],
                    }
                    # use_debug_logging_here = False
                    use_debug_logging_here = True
                    # @modified 20230118 - Task #4786: Switch from matrixprofile to stumpy
                    #                      Task #4778: v4.0.0 - update dependencies
                    # Added current_func
                    result, anomalyScore = run_custom_algorithm_on_timeseries(skyline_app, os.getpid(), metric, timeseries, 'anomalous_daily_peak', custom_algorithms_to_run[custom_algorithm], use_debug_logging_here, current_func='mirage_labelled_metrics')
                    algorithms_run.append(custom_algorithm)
                    ensemble.append(result)
                    if DEBUG_CUSTOM_ALGORITHMS or use_debug_logging_here:
                        logger.debug('debug :: mirage_labelled_metrics :: run_custom_algorithm_on_timeseries run anomalous_daily_peak with result - %s, anomalyScore - %s' % (
                            str(result), str(anomalyScore)))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_labelled_metrics :: run_custom_algorithm_on_timeseries anomalous_daily_peak failed on %s - %s' % (
                        str(metric), err))
                    result = None
                logger.info('mirage_labelled_metrics :: anomalous_daily_peak ran with result: %s (took %.6f seconds), for %s' % (
                    str(result), (time() - start_anomalous_daily_peak), metric))
                # Although fine in a notebook does not have the desired effect
                # in the runtime so convert to a str and check
                # if result is False:
                if str(result) == 'False':
                    logger.info('mirage_labelled_metrics :: anomalous_daily_peak is overrriding anomalous result for %s' % (
                        metric))
                    anomalous = False
                    # Clear last item from the trigger history as anomalous_daily_peak
                    # is a 3sigma method after all
                    try:
                        cleared_trigger_history = self.clear_trigger_history(metric)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_labelled_metrics :: clear_trigger_history failed on %s - %s' % (
                            str(metric), err))

                    # @added 20230424 - Feature #4724: custom_algorithms - anomalous_daily_peak
                    # Added expiry to record metrics identified as normal by anomalous_daily_peaks
                    current_now = int(time())
                    current_aligned_ts = int(process_start_timestamp // 60 * 60)
                    expire_at = current_now + anomalous_daily_peak_expiry
                    redis_hash = 'mirage.normal_daily_peak_metrics.%s' % str(current_aligned_ts)
                    try:
                        self.redis_conn_decoded.hset(redis_hash, redis_metric_name, str(expire_at))
                        self.redis_conn_decoded.expire(redis_hash, str(anomalous_daily_peak_expiry * 2))
                    except Exception as err:
                        logger.error('error :: mirage_labelled_metrics :: failed to add %s to Redis hash %s - %s' % (
                            str(redis_metric_name), str(redis_hash), err))

            # @added 20220504 - Feature #2580: illuminance
            # @modified 20230419 - Feature #2580: illuminance
            # Moved out of the if anomalous block.  Record illuminance for all
            # Get the anomaly breakdown - who returned True?
            triggered_algorithms = []
            for index, boolean_value in enumerate(ensemble):
                if boolean_value:
                    # @modified 20200607 - Feature #3566: custom_algorithms
                    # algorithm = settings.MIRAGE_ALGORITHMS[index]
                    algorithm = algorithms_run[index]
                    anomaly_breakdown[algorithm] += 1
                    triggered_algorithms.append(algorithm)
            if test_alert:
                triggered_algorithms = ['testing']
            if triggered_algorithms:
                illuminance_dict = {}
                use_key = str(base_name)
                if is_labelled_metric:
                    use_key = str(metric_id)
                illuminance_dict[use_key] = {
                    'timestamp': int(metric_timestamp),
                    'value': float(datapoint),
                    'triggered_algorithms_count': len(triggered_algorithms)}
                logger.info('mirage_labelled_metrics :: calling add_illuminance_entries with %s entries to add' % (
                    str(len(illuminance_dict))))
                current_illuminance_dict = {}
                try:
                    current_illuminance_dict = add_illuminance_entries(self, skyline_app, int(run_timestamp), illuminance_dict)
                except Exception as err:
                    logger.error('error :: mirage_labelled_metrics :: add_illuminance_entries failed - %s' % (
                        err))
                logger.info('mirage_labelled_metrics :: illuminance Redis hash now has %s entries' % (
                    str(len(current_illuminance_dict))))

            if not anomalous:

                not_anomalous_metric = [datapoint, base_name]
                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # self.not_anomalous_metrics.append(not_anomalous_metric)
                redis_set = 'mirage_labelled_metrics.not_anomalous_metrics'
                data = str(not_anomalous_metric)
                try:
                    self.redis_conn.sadd(redis_set, data)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_labelled_metrics :: failed to add %s to Redis set %s' % (
                        str(data), str(redis_set)))

                # @added 20200904 - Feature #3734: waterfall alerts
                # Remove the metric from the waterfall_alerts Redis set
                # [metric, timestamp, value, added_to_waterfall_timestamp]
                # waterfall_data = [metric[1], metric[2], metric[0], added_to_waterfall_timestamp, waterfall_panorama_data]
                redis_set = 'analyzer.waterfall_alerts.sent_to_mirage'
                for waterfall_alert in analyzer_waterfall_alerts:
                    if waterfall_alert[0] == base_name:
                        if int(waterfall_alert[1]) == metric_timestamp:
                            try:
                                self.redis_conn.srem(redis_set, str(waterfall_alert))
                                logger.info('mirage_labelled_metrics :: removed waterfall alert item from Redis set %s - %s' % (
                                    redis_set, str(waterfall_alert)))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: mirage_labelled_metrics :: failed to remove waterfall alert item for %s at %s from Redis set %s' % (
                                    base_name, str(metric_timestamp), redis_set))
                        # @added 20201128 - Feature #3734: waterfall alerts
                        # If the check just done is new than an existing analyzer
                        # waterfall alert metric timestamp remove those keys as well
                        if int(waterfall_alert[1]) < metric_timestamp:
                            try:
                                self.redis_conn.srem(redis_set, str(waterfall_alert))
                                logger.info('mirage_labelled_metrics :: removed waterfall alert item with older timestamp from Redis set %s - %s' % (
                                    redis_set, str(waterfall_alert)))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: mirage_labelled_metrics :: failed to remove waterfall alert item for %s at %s from Redis set %s' % (
                                    base_name, str(metric_timestamp), redis_set))

                # @added 20210330 - Feature #3994: Panorama - mirage not anomalous
                # A hash is added to the mirage.panorama.not_anomalous_metrics for
                # every metric that is found to be not anomalous.  This provides
                # data for /panorama?not_anomalous and /panorama?not_anomalous_metric
                # method which are used for plots in the webapp and json response.
                # The mirage.panorama.not_anomalous_metrics Redis hash is managed in
                # analyzer/metrics_manager
                not_anomalous_timestamp = None
                try:
                    not_anomalous_timestamp = int(timeseries[-1][0])
                except:
                    not_anomalous_timestamp = int(metric_timestamp)
                redis_hash = 'mirage_labelled_metrics.panorama.not_anomalous_metrics'
                try:
                    data = {
                        base_name: {
                            'timestamp': not_anomalous_timestamp,
                            'value': datapoint,
                            'hours_to_resolve': int(hours_to_resolve),
                        }
                    }
                    self.redis_conn.hset(redis_hash, time(), str(data))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_labelled_metrics :: failed to add %s to Redis hash %s' % (
                        str(data), str(redis_hash)))

                logger.info('mirage_labelled_metrics :: not anomalous :: %s with %s (at full duration), %s (at SECOND_ORDER_RESOLUTION_HOURS)' % (
                    metric, value, str(datapoint)))

            # If it's anomalous, add it to list
            if anomalous:
                # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
                # base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
                if metric.startswith(settings.FULL_NAMESPACE):
                    base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
                else:
                    base_name = metric
                # metric_timestamp = int(timeseries[-1][0])
                metric_timestamp = int_metric_timestamp

                # Get the anomaly breakdown - who returned True?
                # @modified 20230419 - Feature #2580: illuminance
                # Moved out of the if anomalous block.  Determine
                # triggered_algorithms for all to record illuminance
                # triggered_algorithms = []
                # for index, boolean_value in enumerate(ensemble):
                #     if boolean_value:
                #         # @modified 20200607 - Feature #3566: custom_algorithms
                #         # algorithm = settings.MIRAGE_ALGORITHMS[index]
                #         algorithm = algorithms_run[index]
                #         anomaly_breakdown[algorithm] += 1
                #         triggered_algorithms.append(algorithm)
                # if test_alert:
                #     triggered_algorithms = ['testing']

                # @modified 20201007 - Feature #3772: Add the anomaly_id to the http_alerter json
                #                      Branch #3068: SNAB
                # Added second_order_resolution_seconds, triggered_algorithms and algorithms_run
                # anomalous_metric = [datapoint, base_name, metric_timestamp]
                # @modified 20230419 - Feature #4848: mirage - analyse.irregular.unstable.timeseries.at.30days
                # Added snab_algorithms_to_run
                snab_algorithms_to_run = []
                if SNAB_ENABLED:
                    if irregular_unstable_timeseries:
                        snab_algorithms_to_run = ['irregular_unstable']

                anomalous_metric = [datapoint, base_name, metric_timestamp, second_order_resolution_seconds, triggered_algorithms, algorithms_run, snab_algorithms_to_run]

                if not ionosphere_unique_metrics:
                    try:
                        ionosphere_unique_metrics = list(self.redis_conn_decoded.smembers('ionosphere.unique_metrics'))
                    except:
                        ionosphere_unique_metrics = []

                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # self.anomalous_metrics.append(anomalous_metric)
                if metric not in ionosphere_unique_metrics:
                    redis_set = 'mirage.anomalous_metrics'
                    data = str(anomalous_metric)
                    try:
                        self.redis_conn.sadd(redis_set, data)
                        logger.info('mirage_labelled_metrics :: add %s to mirage.anomalous_metrics Redis set' % (
                            str(data)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_labelled_metrics :: failed to add %s to mirage.anomalous_metrics Redis set' % (
                            str(data)))
                redis_set = 'mirage_labelled_metrics.anomalous_metrics'
                data = str(anomalous_metric)
                try:
                    self.redis_conn.sadd(redis_set, data)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_labelled_metrics :: failed to add %s to mirage.anomalous_metrics Redis set' % (
                        str(data)))

                # @added 20220504 - Feature #2580: illuminance
                # @modified 20230419 - Feature #2580: illuminance
                # Moved out of the if anomalous block to above.  Determine
                # triggered_algorithms and record illuminance for all that
                # triggered
                # illuminance_dict = {}
                # use_key = str(base_name)
                # if is_labelled_metric:
                #     use_key = str(metric_id)
                # illuminance_dict[use_key] = {
                #     'timestamp': int(metric_timestamp),
                #     'value': float(datapoint),
                #     'triggered_algorithms_count': len(triggered_algorithms)}
                # logger.info('mirage_labelled_metrics :: calling add_illuminance_entries with %s entries to add' % (
                #     str(len(illuminance_dict))))
                # current_illuminance_dict = {}
                # try:
                #     current_illuminance_dict = add_illuminance_entries(self, skyline_app, int(run_timestamp), illuminance_dict)
                # except Exception as err:
                #     logger.error('error :: mirage_labelled_metrics :: add_illuminance_entries failed - %s' % (
                #         err))
                # logger.info('mirage_labelled_metrics :: illuminance Redis hash now has %s entries' % (
                #     str(len(current_illuminance_dict))))

                logger.info('mirage_labelled_metrics :: anomaly detected :: %s with %s (at SECOND_ORDER_RESOLUTION_HOURS), %s (at FULL_DURATION)' % (
                    metric, str(datapoint), str(value)))
                # It runs so fast, this allows us to process 30 anomalies/min
                # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                # Removed limit
                # sleep(2)

                # @added 20170206 - Bug #1904: Handle non filesystem friendly metric names in check files
                sane_metricname = filesafe_metricname(str(base_name))

                # @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
                # Determine if any metrcs have negatives values some they can be
                # added to the ionosphere.untrainable_metrics Redis set
                if run_negatives_present and negatives_found:
                    redis_set = 'ionosphere.untrainable_metrics'
                    try:
                        last_negative_timestamp = int(negatives_found[-1][0])
                        last_negative_value = negatives_found[-1][1]
                        remove_after_timestamp = int(last_negative_timestamp + second_order_resolution_seconds)
                        data = str([base_name, metric_timestamp, datapoint, last_negative_timestamp, last_negative_value, second_order_resolution_seconds, remove_after_timestamp])
                        self.redis_conn.sadd(redis_set, data)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_labelled_metrics :: failed to add %s to Redis set %s' % (
                            str(data), str(redis_set)))

                # If Crucible or Panorama are enabled determine details
                determine_anomaly_details = False
                if settings.ENABLE_CRUCIBLE and settings.MIRAGE_CRUCIBLE_ENABLED:
                    determine_anomaly_details = True
                if settings.PANORAMA_ENABLED:
                    determine_anomaly_details = True

                # If Ionosphere is enabled determine details
                try:
                    ionosphere_enabled = settings.IONOSPHERE_ENABLED
                    if settings.IONOSPHERE_ENABLED:
                        determine_anomaly_details = True
                except:
                    ionosphere_enabled = False

                if determine_anomaly_details:
                    # metric_timestamp = str(int(timeseries[-1][0]))
                    from_timestamp = str(int(timeseries[1][0]))
                    timeseries_dir = base_name.replace('.', '/')

                cache_key = 'mirage.last_alert.smtp.%s' % (base_name)
                last_alert = False
                try:
                    # @modified 20200805 - Task #3662: Change mirage.last_check keys to timestamp value
                    #                      Feature #3486: analyzer_batch
                    #                      Feature #3480: batch_processing
                    # Changed the last_alert cache key to hold the last
                    # anomaly timestamp
                    # last_alert = self.redis_conn.get(cache_key)
                    last_alert = self.redis_conn_decoded.get(cache_key)
                except Exception as e:
                    logger.error('error :: mirage_labelled_metrics :: could not query Redis for cache_key: %s' % str(e))

                # @added 20170308 - Feature #1960: ionosphere_layers
                # Allow Ionosphere to send Panorama checks, it is an ionosphere_metric
                if not ionosphere_unique_metrics:
                    try:
                        # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                        #                      Branch #3262: py3
                        # ionosphere_unique_metrics = list(self.redis_conn.smembers('ionosphere.unique_metrics'))
                        ionosphere_unique_metrics = list(self.redis_conn_decoded.smembers('ionosphere.unique_metrics'))
                    except:
                        ionosphere_unique_metrics = []

                added_at = str(int(time()))
                # If Panorama is enabled - create a Panorama check
                # @modified 20170308 - Feature #1960: ionosphere_layers
                # Allow Ionosphere to send Panorama checks for ionosphere_metrics
                # if settings.PANORAMA_ENABLED:
                send_to_panorama = False
                redis_metric_name = '%s%s' % (str(settings.FULL_NAMESPACE), str(base_name))

                # TODO - testing
                if is_labelled_metric:
                    redis_metric_name = 'labelled_metrics.%s' % str(metric_id)

                if settings.PANORAMA_ENABLED:
                    send_to_panorama = True
                if redis_metric_name in ionosphere_unique_metrics:
                    send_to_panorama = False

                # @added 20220315 - Feature #4482: Test alerts
                # Allow for full testing with the injection of an anomaly on a
                # metric
                if test_alert or test_alert_and_trigger:
                    logger.info('mirage_labelled_metrics :: test_alert sending triggered anomaly on %s to Panorama' % (
                        metric))
                    send_to_panorama = True
                    # Panorama must have at least one triggered algorithm
                    original_triggered_algorithms = list(triggered_algorithms)
                    if len(triggered_algorithms) == 0:
                        triggered_algorithms = [algorithms_run[0]]

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
                    source = 'graphite'
                    if base_name.startswith('labelled_metrics.'):
                        source = 'victoriametrics'
                    panaroma_anomaly_data = 'metric = \'%s\'\n' \
                                            'value = \'%s\'\n' \
                                            'from_timestamp = \'%s\'\n' \
                                            'metric_timestamp = \'%s\'\n' \
                                            'algorithms = %s\n' \
                                            'triggered_algorithms = %s\n' \
                                            'app = \'%s\'\n' \
                                            'source = \'%s\'\n' \
                                            'added_by = \'%s\'\n' \
                                            'added_at = \'%s\'\n' \
                        % (base_name, str(datapoint), from_timestamp,
                           # @modified 20200607 - Feature #3566: custom_algorithms
                           # str(int_metric_timestamp), str(settings.MIRAGE_ALGORITHMS),
                           str(int_metric_timestamp), str(algorithms_run),
                           triggered_algorithms, skyline_app, source,
                           this_host, added_at)

                    # Create an anomaly file with details about the anomaly
                    panaroma_anomaly_file = '%s/%s.%s.txt' % (
                        settings.PANORAMA_CHECK_PATH, added_at, sane_metricname)
                    try:
                        write_data_to_file(
                            skyline_app, panaroma_anomaly_file, 'w',
                            panaroma_anomaly_data)
                        logger.info('mirage_labelled_metrics :: added panorama anomaly file :: %s' % (panaroma_anomaly_file))
                        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        # Move to Redis set block below
                        # self.sent_to_panorama.append(base_name)
                    except:
                        logger.error('error :: mirage_labelled_metrics :: failed to add panorama anomaly file :: %s' % (panaroma_anomaly_file))
                        logger.error(traceback.format_exc())
                    # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # Moved from the above self.sent_to_panorama
                    redis_set = 'mirage_labelled_metrics.sent_to_panorama'
                    data = str(base_name)
                    try:
                        self.redis_conn.sadd(redis_set, data)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_labelled_metrics :: failed to add %s to Redis set %s' % (
                            str(data), str(redis_set)))

                    # @added 20210323 - Feature #3642: Anomaly type classification
                    if LUMINOSITY_CLASSIFY_ANOMALIES:
                        redis_set = 'luminosity.classify_anomalies'
                        data_dict = {
                            'metric': metric,
                            'timestamp': int_metric_timestamp,
                            'value': datapoint,
                            'algorithms': algorithms_run,
                            'triggered_algorithms': triggered_algorithms,
                            'app': skyline_app,
                            'added_at': int(added_at),
                        }
                        data = [metric, int_metric_timestamp, int(added_at), data_dict]
                        try:
                            self.redis_conn.sadd(redis_set, str(data))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: mirage_labelled_metrics :: failed to add %s to Redis set %s' % (
                                str(data), str(redis_set)))

                    # @added 20220315 - Feature #4482: Test alerts
                    # Allow for full testing with the injection of an anomaly on a
                    # metric
                    if test_alert or test_alert_and_trigger:
                        triggered_algorithms = list(original_triggered_algorithms)

                    # @added 20200904 - Feature #3734: waterfall alerts
                    # Remove the metric from the waterfall_alerts Redis set
                    # [metric, timestamp, value, added_to_waterfall_timestamp]
                    # waterfall_data = [metric[1], metric[2], metric[0], added_to_waterfall_timestamp, waterfall_panorama_data]
                    redis_set = 'analyzer.waterfall_alerts.sent_to_mirage'
                    for waterfall_alert in analyzer_waterfall_alerts:
                        if waterfall_alert[0] == base_name:
                            if int(waterfall_alert[1]) == metric_timestamp:
                                try:
                                    self.redis_conn.srem(redis_set, str(waterfall_alert))
                                    logger.info('mirage_labelled_metrics :: removed waterfall alert item from Redis set %s - %s' % (
                                        redis_set, str(waterfall_alert)))
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: mirage_labelled_metrics :: failed to remove waterfall alert item for %s at %s from Redis set %s' % (
                                        base_name, str(metric_timestamp), redis_set))

                # If crucible is enabled - save timeseries and create a
                # crucible check
                if settings.ENABLE_CRUCIBLE and settings.MIRAGE_CRUCIBLE_ENABLED:
                    from_timestamp = str(int(timeseries[1][0]))
                    timeseries_dir = base_name.replace('.', '/')
                    crucible_anomaly_dir = str(settings.CRUCIBLE_DATA_FOLDER) + '/' + timeseries_dir + '/' + metric_timestamp
                    if not os.path.exists(crucible_anomaly_dir):
                        mkdir_p(crucible_anomaly_dir)

                    # Note:
                    # The value is enclosed is single quoted intentionally
                    # as the imp.load_source used in crucible results in a
                    # shift in the decimal position when double quoted, e.g.
                    # value = "5622.0" gets imported as
                    # 2016-03-02 12:53:26 :: 28569 :: metric variable - value - 562.2
                    # single quoting results in the desired,
                    # 2016-03-02 13:16:17 :: 1515 :: metric variable - value - 5622.0

                    crucible_anomaly_data = 'metric = \'%s\'\n' \
                                            'value = \'%s\'\n' \
                                            'from_timestamp = \'%s\'\n' \
                                            'metric_timestamp = \'%s\'\n' \
                                            'algorithms = %s\n' \
                                            'triggered_algorithms = %s\n' \
                                            'anomaly_dir = \'%s\'\n' \
                                            'graphite_metric = True\n' \
                                            'run_crucible_tests = False\n' \
                                            'added_by = \'%s\'\n' \
                                            'added_at = \'%s\'\n' \
                        % (base_name, str(datapoint), from_timestamp,
                           # @modified 20200607 - Feature #3566: custom_algorithms
                           # str(int_metric_timestamp), str(settings.MIRAGE_ALGORITHMS),
                           str(int_metric_timestamp), str(algorithms_run),
                           triggered_algorithms, crucible_anomaly_dir,
                           skyline_app, added_at)

                    # Create an anomaly file with details about the anomaly
                    crucible_anomaly_file = '%s/%s.txt' % (crucible_anomaly_dir, sane_metricname)
                    try:
                        write_data_to_file(
                            skyline_app, crucible_anomaly_file, 'w',
                            crucible_anomaly_data)
                        logger.info('mirage_labelled_metrics :: added crucible anomaly file :: %s' % (crucible_anomaly_file))
                        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        # self.sent_to_crucible.append(base_name)
                    except:
                        logger.error('error :: mirage_labelled_metrics :: failed to add crucible anomaly file :: %s' % (crucible_anomaly_file))
                        logger.error(traceback.format_exc())

                    # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # Moved from the above self.sent_to_crucible
                    redis_set = 'mirage.sent_to_crucible'
                    data = str(base_name)
                    try:
                        self.redis_conn.sadd(redis_set, data)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_labelled_metrics :: failed to add %s to Redis set %s' % (
                            str(data), str(redis_set)))

                    # Create timeseries json file with the timeseries
                    json_file = '%s/%s.json' % (crucible_anomaly_dir, base_name)
                    timeseries_json = str(timeseries).replace('[', '(').replace(']', ')')
                    try:
                        write_data_to_file(skyline_app, json_file, 'w', timeseries_json)
                        logger.info('mirage_labelled_metrics :: added crucible timeseries file :: %s' % (json_file))
                    except:
                        logger.error('error :: mirage_labelled_metrics :: failed to add crucible timeseries file :: %s' % (json_file))
                        logger.error(traceback.format_exc())

                    # Create a crucible check file
                    crucible_check_file = '%s/%s.%s.txt' % (settings.CRUCIBLE_CHECK_PATH, metric_timestamp, sane_metricname)
                    try:
                        write_data_to_file(
                            skyline_app, crucible_check_file, 'w',
                            crucible_anomaly_data)
                        logger.info('mirage_labelled_metrics :: added crucible check :: %s,%s' % (base_name, metric_timestamp))
                    except:
                        logger.error('error :: mirage_labelled_metrics :: failed to add crucible check file :: %s' % (crucible_check_file))
                        logger.error(traceback.format_exc())

                # @added 20230510 - Feature #4902: Prevent training on metrics newer than 7 days
                new_metric_added_at = False
                if ionosphere_enabled and not last_alert:
                    try:
                        new_metric_added_at = self.redis_conn_decoded.hget('metrics_manager.untrainable_new_metrics', labelled_metric_base_name)
                    except Exception as err:
                        logger.error('error :: failed to hget from metrics_manager.untrainable_new_metrics - %s' % (
                            err))
                    if new_metric_added_at:
                        try:
                            new_until = int(float(new_metric_added_at)) + (86400 * 7)
                            new_until_date = datetime.datetime.fromtimestamp(new_until).strftime('%Y-%m-%d %H:%M:%S')
                            logger.info('not sending %s to Ionosphere as still a new metric until %s' % (
                                labelled_metric_base_name, new_until_date))
                        except Exception as err:
                            logger.error('error :: failed to determine when %s matures - %s' % (
                                labelled_metric_base_name, err))
                        ionosphere_enabled = False

                # @added 20160922 - Branch #922: Ionosphere
                # Also added the send_anomalous_metric_to skyline_functions.py
                # function
                if ionosphere_enabled:
                    if not last_alert:
                        # @modified 20161228 Feature #1830: Ionosphere alerts
                        # Added full_duration which needs to be recorded to allow Mirage metrics
                        # to be profiled on Redis timeseries data at FULL_DURATION
                        # e.g. mirage.redis.24h.json
                        full_duration = str(second_order_resolution_seconds)
                        # @modified 20170127 - Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
                        # Added ionosphere_parent_id, always zero from Analyzer and Mirage
                        ionosphere_parent_id = 0
                        send_anomalous_metric_to(
                            skyline_app, 'ionosphere', timeseries_dir,
                            str(int_metric_timestamp), base_name, str(datapoint),
                            from_timestamp, triggered_algorithms, timeseries,
                            full_duration, str(ionosphere_parent_id),
                            # @added 20201001 - Task #3748: POC SNAB
                            # Added algorithms_run required to determine the anomalyScore
                            # so this needs to be sent to Ionosphere so Ionosphere
                            # can send it back on an alert
                            algorithms_run)
                        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        # Moved to the mirage.sent_to_ionosphere Redis set Redis set
                        # block below
                        # self.sent_to_ionosphere.append(base_name)

                        # @added 20200804 - Feature #3462: Add IONOSPHERE_MANAGE_PURGE
                        #                   Feature #3472: ionosphere.training_data Redis set
                        #                   Feature #3474: webapp api - training_data
                        # Add training data to the ionosphere.training_data so that
                        # the ionosphere purge_old_data_dirs can happen less
                        # frequently for reduced I/O
                        redis_set = 'ionosphere.training_data'
                        data = [base_name, int(int_metric_timestamp), second_order_resolution_seconds]
                        try:
                            logger.info('mirage_labelled_metrics :: adding to Redis set %s - %s' % (
                                redis_set, str(data)))
                            self.redis_conn.sadd(redis_set, str(data))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: mirage_labelled_metrics :: failed to add %s to %s Redis set' % (str(data), redis_set))
                    else:
                        logger.info('mirage_labelled_metrics :: alert expiry key exists not sending to Ionosphere :: %s' % base_name)

                    # @added 20200904 - Feature #3734: waterfall alerts
                    # Remove the metric from the waterfall_alerts Redis set
                    # [metric, timestamp, value, added_to_waterfall_timestamp]
                    # waterfall_data = [metric[1], metric[2], metric[0], added_to_waterfall_timestamp, waterfall_panorama_data]
                    # Do not remove if this is only for training_data creation
                    if redis_metric_name in ionosphere_unique_metrics:
                        redis_set = 'analyzer.waterfall_alerts.sent_to_mirage'
                        mirage_waterfall_data = []
                        for waterfall_alert in analyzer_waterfall_alerts:
                            if waterfall_alert[0] == base_name:
                                if int(waterfall_alert[1]) == metric_timestamp:
                                    mirage_waterfall_data = waterfall_alert
                                    try:
                                        self.redis_conn.srem(redis_set, str(waterfall_alert))
                                        logger.info('mirage_labelled_metrics :: removed waterfall alert item from Redis set %s - %s' % (
                                            redis_set, str(waterfall_alert)))
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: mirage_labelled_metrics :: failed to remove waterfall alert item for %s at %s from Redis set %s' % (
                                            base_name, str(metric_timestamp), redis_set))

                    # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # Moved from the above self.sent_to_ionosphere
                    if not last_alert:
                        redis_set = 'mirage_labelled_metrics.sent_to_ionosphere'
                        data = str(base_name)
                        try:
                            self.redis_conn.sadd(redis_set, data)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: mirage_labelled_metrics :: failed to add %s to Redis set %s' % (
                                str(data), str(redis_set)))

                        # @added 20220315 - Feature #4482: Test alerts
                        # Allow for full testing with the injection of an anomaly on a
                        # metric
                        if test_alert or test_alert_and_trigger:
                            logger.info('mirage_labelled_metrics :: test_alert not sending anomaly on %s to ionosphere' % (
                                metric))
                            ionosphere_unique_metrics = []

                        # @added 20200904 - Feature #3734: waterfall alerts
                        # Add mirage waterfall alert
                        # Only add if this is an ionosphere_enabled metric_check_file
                        if redis_metric_name in ionosphere_unique_metrics:
                            if mirage_waterfall_data:
                                # waterfall_data = [metric[1], metric[2], metric[0], added_to_waterfall_timestamp, waterfall_panorama_data]
                                waterfall_data = mirage_waterfall_data
                                redis_set = 'mirage.waterfall_alerts.sent_to_ionosphere'
                                try:
                                    self.redis_conn.sadd(redis_set, str(waterfall_data))
                                    logger.info('mirage_labelled_metrics :: added to Redis set %s - %s' % (redis_set, str(waterfall_data)))
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: mirage_labelled_metrics :: failed to add %s to Redis set %s' % (
                                        str(waterfall_data), str(redis_set)))

            metric_var_files = []
            timeseries = []

            if os.path.isfile(metric_check_file):
                # Remove metric check file
                try:
                    os.remove(metric_check_file)
                    logger.info('mirage_labelled_metrics :: removed check file - %s' % metric_check_file)
                except OSError:
                    logger.error('error :: mirage_labelled_metrics :: failed to remove check file - %s' % metric_check_file)

            # Remove the metric directory
            if os.path.exists(metric_data_dir):
                try:
                    rmtree(metric_data_dir)
                    logger.info('mirage_labelled_metrics :: removed data dir - %s' % metric_data_dir)
                except:
                    logger.error('error :: mirage_labelled_metrics :: failed to rmtree %s' % metric_data_dir)

        # Add values to the queue so the parent process can collate
        try:
            for key, ab_value in anomaly_breakdown.items():
                self.mirage_labelled_metrics_anomaly_breakdown_q.put((key, ab_value))
        except Exception as err:
            logger.error('error :: mirage_labelled_metrics :: failed iterate to self.mirage_labelled_metrics_anomaly_breakdown_q - %s' % err)

        try:
            for key, e_value in exceptions.items():
                self.mirage_labelled_metrics_exceptions_q.put((key, e_value))
        except Exception as err:
            logger.error('error :: mirage_labelled_metrics :: failed iterate to self.mirage_labelled_metrics_exceptions_q - %s' % err)

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

        logger.info('mirage_labelled_metrics :: starting %s run' % skyline_app)
        if os.path.isfile(skyline_app_loglock):
            logger.error('error - bin/%s.d log management seems to have failed, continuing' % skyline_app)
            try:
                os.remove(skyline_app_loglock)
                logger.info('mirage_labelled_metrics :: log lock file removed')
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_loglock)
        else:
            logger.info('mirage_labelled_metrics :: bin/%s.d log management done' % skyline_app)

        if not os.path.exists(MIRAGE_LABELLED_CHECK_PATH):
            mkdir_p(MIRAGE_LABELLED_CHECK_PATH)

        # @added 20200903 - Task #3730: Validate Mirage running multiple processes
        last_sent_to_graphite = int(time())

        # @added 20220421 - Task #3800: Handle feedback metrics in Mirage and waterfall alerts
        filesafe_names_dict = {}

        last_redis_self_key_update = 0

        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except:
                logger.error('error :: mirage_labelled_metrics :: skyline can not connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                sleep(10)
                logger.info('mirage_labelled_metrics :: attempting to connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                try:
                    self.redis_conn = get_redis_conn(skyline_app)
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                except Exception as err:
                    logger.error('error :: mirage_labelled_metrics :: failed to connect to Redis - %s' % err)
                try:
                    self.redis_conn.ping()
                    logger.info('mirage_labelled_metrics :: connected to redis')
                except Exception as err:
                    logger.error('error :: mirage_labelled_metrics :: failed to ping Redis - %s' % err)

            # Determine if any metric to analyze or Ionosphere alerts to be sent
            while True:

                now = time()
                # Report app up
                update_redis_self_key = False
                if not last_redis_self_key_update:
                    update_redis_self_key = True
                if last_redis_self_key_update and now >= (last_redis_self_key_update + 20):
                    update_redis_self_key = True
                if update_redis_self_key:
                    try:
                        # redis_is_up = self.redis_conn.setex(skyline_app, 120, now)
                        redis_is_up = self.redis_conn.setex('mirage_labelled_metrics', 120, now)
                        last_redis_self_key_update = int(now)
                        if redis_is_up:
                            try:
                                self.redis_conn.setex('redis', 120, now)
                            except Exception as err:
                                logger.error(traceback.format_exc())
                                logger.error('error :: mirage_labelled_metrics :: could not update the Redis redis key - %s' % (
                                    err))
                    except Exception as err:
                        logger.error('error :: mirage_labelled_metrics :: failed to update Redis key for mirage_labelled_metrics up - %s' % err)

                # @added 20161228 - Feature #1828: ionosphere - mirage Redis data features
                # If Ionosphere is going to pass alerts back to the app
                # here we are going to have break out and force a alerting
                # only run.
                ionosphere_alerts_returned = False

                # @added 20220315 - Feature #4482: Test alerts
                # Allow for full testing with the injection of an anomaly on a
                # metric
                test_alerts = {}
                test_alert_metrics = []

                metric_var_files = []
                ionosphere_alerts_returned = False

                # @modified 20190408 - Bug #2904: Initial Ionosphere echo load and Ionosphere feedback
                #                      Feature #2484: FULL_DURATION feature profiles
                # Move this len(metric_var_files) from above and apply the
                # appropriatte sleep
                if len(metric_var_files) == 0:
                    sleep_for = 10
                    next_send_to_graphite = last_sent_to_graphite + 60
                    seconds_to_next_send_to_graphite = next_send_to_graphite - int(time())
                    if seconds_to_next_send_to_graphite < 10:
                        if seconds_to_next_send_to_graphite > 1:
                            sleep_for = seconds_to_next_send_to_graphite
                        else:
                            break

                    logger.info('mirage_labelled_metrics :: sleeping no metrics...')
                    # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                    # sleep(10)
                    sleep(sleep_for)

                batch_processing_metrics = []

                # Clean up old files
                now_timestamp = time()
                stale_age = now_timestamp - settings.MIRAGE_STALE_SECONDS
                for current_file in os.listdir(MIRAGE_LABELLED_CHECK_PATH):
                    if os.path.isfile(MIRAGE_LABELLED_CHECK_PATH + "/" + current_file):
                        t = os.stat(MIRAGE_LABELLED_CHECK_PATH + "/" + current_file)
                        c = t.st_ctime

                        # @added 20220113 - Feature #3486: analyzer_batch
                        #                   Feature #3480: batch_processing
                        # Do not remove batch_processing checks
                        for b_metric in batch_processing_metrics:
                            if b_metric in current_file:
                                continue

                        # delete file if older than a week
                        if c < stale_age:
                            os.remove(MIRAGE_LABELLED_CHECK_PATH + "/" + current_file)
                            logger.info('mirage_labelled_metrics :: removed stale check - %s' % (current_file))
                            # @added 20200903 - Task #3730: Validate Mirage running multiple processes
                            redis_set = 'mirage_labelled_metrics.stale_check_discarded'
                            try:
                                self.redis_conn.sadd(redis_set, str(current_file))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: mirage_labelled_metrics :: failed to add %s to Redis set %s' % (
                                    str(current_file), str(redis_set)))

                # @added 20220421 - Task #3800: Handle feedback metrics in Mirage and waterfall alerts
                # Handle filesafe names
                filesafe_names_list = []
                if filesafe_names_dict:
                    filesafe_names_list = list(filesafe_names_dict.keys())

                # Discover metric to analyze
                metric_var_files = []

                labelled_metrics_to_check_dict = {}
                try:
                    labelled_metrics_to_check_dict = self.redis_conn_decoded.hgetall('analyzer_labelled_metrics.mirage_check')
                except Exception as err:
                    logger.error('failed to cleanup mirage_algorithm.error files - %s' % (traceback.format_exc()))
                if labelled_metrics_to_check_dict:
                    for key in list(labelled_metrics_to_check_dict.keys()):
                        check_key = 'analyzer_labelled_metrics.mirage_check.%s' % key
                        metric_var_files.append(check_key)

                if len(metric_var_files) > 0:
                    break

            process_metric_check_files = False

            metric_var_files_sorted = sorted(metric_var_files)
            # metric_check_file = settings.MIRAGE_CHECK_PATH + "/" + metric_var_files_sorted[0]
            if metric_var_files_sorted:
                process_metric_check_files = True

            # @added 20221014 - Bug #4696: analyzer - anomalous metrics sets not flushing
            #                   Task #4614: Support labelled metrics
            # Set the default dicts before the if as it was not being set inside
            # at times causing the log entries to hang.
            exceptions = {}
            anomaly_breakdown = {}

            if process_metric_check_files:

                # @added 20200903 - Task #3730: Validate Mirage running multiple processes
                check_files_to_process = len(metric_var_files_sorted)
                logger.info('mirage_labelled_metrics :: %s checks to process' % str(check_files_to_process))

                # Remove any existing algorithm.error files from any previous runs
                # that did not cleanup for any reason
                pattern = '%s.*.algorithm.error' % skyline_app
                try:
                    for f in os.listdir(settings.SKYLINE_TMP_DIR):
                        if re.search(pattern, f):
                            try:
                                os.remove(os.path.join(settings.SKYLINE_TMP_DIR, f))
                                logger.info('mirage_labelled_metrics :: cleaning up old error file - %s' % (str(f)))
                            except OSError:
                                pass
                except:
                    logger.error('failed to cleanup mirage_algorithm.error files - %s' % (traceback.format_exc()))

                # Spawn processes
                pids = []
                spawned_pids = []
                pid_count = 0

                # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                # MIRAGE_PROCESSES = 1
                if len(metric_var_files) > 1:
                    try:
                        MIRAGE_PROCESSES = int(settings.MIRAGE_PROCESSES)
                        if len(metric_var_files) < MIRAGE_PROCESSES:
                            MIRAGE_PROCESSES = len(metric_var_files)
                    except:
                        MIRAGE_PROCESSES = 1
                else:
                    MIRAGE_PROCESSES = 1

                # Was testing just 1 process
                # MIRAGE_PROCESSES = 1
                # @modified 20230120 - Task #4786: Switch from matrixprofile to stumpy
                #                      Task #4778: v4.0.0 - update dependencies
                # Removed this limitation because mirage_labelled_metrics can be
                # assigned 100s of checks and with the change to stumpy analysis
                # with skyline_matrixprofile has increased the run time.
                # With matrixprofile being run with skyline_matrixprofile via
                # run_custom_algorithm_on_timeseries was achieving around 91
                # checks in 41 seconds, now running skyline_matrixproile direct
                # with "stumpy-mp.stump" is taking around 65 seconds to do 88
                # checks.  Do not limit to 1 process unless there are under 10
                # checks.  The reason being that initialisation of stumpy.stump
                # even with jit caching takes between 1 and 3 seconds,
                # thereafter any further metrics analysed with stump in the run
                # take between 0.02 and 0.6 seconds (no init required), this is
                # more on the 0.6 second side when busy.  Therefore if there are
                # less than 6 checks (ballpark figure, depends on load) it is
                # more efficient and quicker to just use 1 process rather than
                # more.
                # MIRAGE_PROCESSES = 1
                if len(metric_var_files) <= 6:
                    MIRAGE_PROCESSES = 1

                run_timestamp = int(time())
                for i in range(1, MIRAGE_PROCESSES + 1):

                    checks_per_processor = int(ceil(float(len(metric_var_files_sorted)) / float(MIRAGE_PROCESSES)))
                    if i == MIRAGE_PROCESSES:
                        assigned_max = len(metric_var_files_sorted)
                    else:
                        assigned_max = min(len(metric_var_files_sorted), i * checks_per_processor)
                    assigned_min = (i - 1) * checks_per_processor
                    assigned_keys = range(assigned_min, assigned_max)

                    # Compile assigned metrics
                    assigned_checks = [metric_var_files_sorted[index] for index in assigned_keys]

                    logger.info('mirage_labelled_metrics :: processing %s checks' % str(len(assigned_checks)))
                    p = Process(target=self.spin_process, args=(i, run_timestamp, assigned_checks))

                    pids.append(p)
                    pid_count += 1
                    logger.info('mirage_labelled_metrics :: starting %s of %s spin_process/es' % (str(pid_count), str(MIRAGE_PROCESSES)))
                    p.start()
                    # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                    # spawned_pids.append(p.pid)
                    spawned_pids.append([p.pid, i])
                    logger.info('mirage_labelled_metrics :: started spin_process %s with pid %s' % (str(pid_count), str(p.pid)))

                # Self monitor processes and terminate if any spin_process has run
                # for longer than 180 seconds - 20160512 @earthgecko
                p_starts = time()
                while time() - p_starts <= settings.MAX_ANALYZER_PROCESS_RUNTIME:
                    if any(p.is_alive() for p in pids):
                        # Just to avoid hogging the CPU
                        sleep(.1)
                    else:
                        # All the processes are done, break now.
                        time_to_run = time() - p_starts
                        logger.info('mirage_labelled_metrics :: %s :: %s spin_process/es completed in %.2f seconds' % (
                            skyline_app, str(MIRAGE_PROCESSES), time_to_run))
                        break
                else:
                    # We only enter this if we didn't 'break' above.
                    logger.info('mirage_labelled_metrics :: %s :: timed out, killing all spin_process processes' % (skyline_app))
                    for p in pids:
                        try:
                            # @modified 20230410 - moved p.join before p.terminate
                            p.join()
                            p.terminate()
                            # @modified 20221125 - added join back
                            # p.join()
                        except Exception as err:
                            logger.error('error :: mirage_labelled_metrics :: %s :: error terminating pid - %s' % (
                                skyline_app, err))

                for p in pids:
                    try:
                        if p.is_alive():
                            logger.info('mirage_labelled_metrics :: %s :: stopping spin_process - %s' % (skyline_app, str(p.is_alive())))
                            p.join()
                    except Exception as err:
                        logger.error('error :: mirage_labelled_metrics :: %s :: error joining pid - %s' % (
                            skyline_app, err))

                # @added 20200607 - Feature #3508: ionosphere.untrainable_metrics
                # Check to non 3sigma algorithm errors too
                check_algorithm_errors = ['negatives_present']
                for algorithm in list(settings.MIRAGE_ALGORITHMS):
                    check_algorithm_errors.append(algorithm)
                # @added 20200607 - Feature #3566: custom_algorithms
                if CUSTOM_ALGORITHMS:
                    for custom_algorithm in settings.CUSTOM_ALGORITHMS:
                        check_algorithm_errors.append(custom_algorithm)

                # Grab data from the queue and populate dictionaries
                exceptions = {}
                anomaly_breakdown = {}
                while 1:
                    try:
                        key, value = self.mirage_labelled_metrics_anomaly_breakdown_q.get_nowait()
                        if key not in list(anomaly_breakdown.keys()):
                            anomaly_breakdown[key] = value
                        else:
                            anomaly_breakdown[key] += value
                    except Empty:
                        # @added 20191113 - Branch #3262: py3
                        # Log
                        logger.info('mirage_labelled_metrics :: anomaly_breakdown.keys are empty')
                        break
                    except Exception as err:
                        logger.error('error :: mirage_labelled_metrics :: grabbing data from the queue and populating anomaly_breakdown - %s' % err)
                        break

                while 1:
                    try:
                        key, value = self.mirage_labelled_metrics_exceptions_q.get_nowait()
                        if key not in list(exceptions.keys()):
                            exceptions[key] = value
                        else:
                            exceptions[key] += value
                    except Empty:
                        # @added 20191113 - Branch #3262: py3
                        # Log
                        logger.info('mirage_labelled_metrics :: exceptions.keys are empty')
                        break
                    except Exception as err:
                        logger.error('error :: mirage_labelled_metrics :: grabbing data from the queue and populating exceptions - %s' % err)
                        break

                # @added 20191021 - Bug #3288: Always send anomaly_breakdown and exception metrics
                #                   Branch #3262: py3
                exceptions_metrics = ['Boring', 'Stale', 'TooShort', 'Other']
                for i_exception in exceptions_metrics:
                    if i_exception not in list(exceptions.keys()):
                        exceptions[i_exception] = 0
                # @modified 20200607 - Feature #3566: custom_algorithms
                # for i_anomaly_breakdown in settings.MIRAGE_ALGORITHMS:
                for i_anomaly_breakdown in check_algorithm_errors:
                    if i_anomaly_breakdown not in list(anomaly_breakdown.keys()):
                        anomaly_breakdown[i_anomaly_breakdown] = 0

                # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                # for completed_pid in spawned_pids:
                for completed_pid, mirage_process in spawned_pids:
                    logger.info('mirage_labelled_metrics :: spin_process with pid %s completed' % (str(completed_pid)))
                    # @modified 20200607 - Feature #3566: custom_algorithms
                    #                      Feature #3508: ionosphere.untrainable_metrics
                    # Check to non 3sigma algorithm errors too and wrapped in try
                    try:
                        # for algorithm in settings.MIRAGE_ALGORITHMS:
                        for algorithm in check_algorithm_errors:
                            algorithm_error_file = '%s/%s.%s.%s.algorithm.error' % (
                                settings.SKYLINE_TMP_DIR, skyline_app,
                                str(completed_pid), algorithm)
                            if os.path.isfile(algorithm_error_file):
                                logger.info(
                                    'error :: spin_process with pid %s has reported an error with the %s algorithm' % (
                                        str(completed_pid), algorithm))
                                try:
                                    with open(algorithm_error_file, 'r') as f:
                                        error_string = f.read()
                                    logger.error('%s' % str(error_string))
                                except:
                                    logger.error('error :: mirage_labelled_metrics :: failed to read %s error file' % algorithm)
                                try:
                                    os.remove(algorithm_error_file)
                                except OSError:
                                    pass
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_labelled_metrics :: failed to check algorithm errors')

                    redis_metrics_processed_key = 'mirage_labelled_metrics.%s.metrics_processed' % str(mirage_process)
                    redis_metrics_processed = {}
                    try:
                        redis_metrics_processed = self.redis_conn_decoded.hgetall(redis_metrics_processed_key)
                        # if redis_metrics_processed:
                        #     self.redis_conn_decoded.delete(redis_metrics_processed_key)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_labelled_metrics :: %s Redis hash operation failed - %s' % (redis_metrics_processed_key, err))

                    # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # Use Redis set and not self.metric_variables

                    metric_variables = []
                    # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # literal_metric_variables = list(self.redis_conn.smembers('mirage.metric_variables'))
                    # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                    # Handle per process
                    # literal_metric_variables = list(self.redis_conn_decoded.smembers('mirage.metric_variables'))
                    metric_variable_redis_set = 'mirage_labelled_metrics.%s.metric_variables' % str(mirage_process)
                    try:
                        literal_metric_variables = list(self.redis_conn_decoded.smembers(metric_variable_redis_set))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_labelled_metrics :: smembers failed on Redis %s - %s' % (
                            metric_variable_redis_set, err))
                        literal_metric_variables = []

                    for item_list_string in literal_metric_variables:
                        list_item = literal_eval(item_list_string)
                        metric_variables.append(list_item)

                    # @added 20200903 - Task #3730: Validate Mirage running multiple processes
                    # Handle per process
                    try:
                        self.redis_conn.delete(metric_variable_redis_set)
                        # logger.info('mirage_labelled_metrics :: deleted Redis set - %s' % metric_variable_redis_set)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_labelled_metrics :: failed to delete Redis set - %s' % metric_variable_redis_set)

                    # @added 20191113 - Branch #3262: py3
                    # Set default values
                    metric_name = None
                    metric_value = None
                    hours_to_resolve = 0

                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # for metric_variable in self.metric_variables:
                    for metric_variable in metric_variables:
                        if metric_variable[0] == 'metric_name':
                            metric_name = metric_variable[1]
                        if metric_variable[0] == 'metric_value':
                            metric_value = metric_variable[1]
                        if metric_variable[0] == 'hours_to_resolve':
                            hours_to_resolve = metric_variable[1]
                        # if metric_variable[0] == 'metric_timestamp':
                        #     metric_timestamp = metric_variable[1]

                    # logger.info('mirage_labelled_metrics :: analysis done - %s' % str(metric_name))
                    logger.info('mirage_labelled_metrics :: process %s checked %s metrics' % (
                        str(mirage_process), str(len(redis_metrics_processed))))

                    # Send alerts
                    # Calculate hours second order resolution to seconds
                    # @modified 20191113 - Branch #3262: py3
                    # Only if set
                    if hours_to_resolve:
                        logger.info('mirage_labelled_metrics :: analyzed at %s hours resolution' % hours_to_resolve)
                        second_order_resolution_seconds = int(hours_to_resolve) * 3600
                        logger.info('mirage_labelled_metrics :: analyzed at %s seconds resolution' % str(second_order_resolution_seconds))

                    # Remove metric check files
                    for check_item in list(redis_metrics_processed.keys()):
                        metric_check_file = 'None'
                        try:
                            metric_data = literal_eval(redis_metrics_processed[check_item])
                            metric_id = metric_data['metric_id']
                            metric_name = 'labelled_metrics.%s' % str(metric_id)
                            processing_check_file = '%s.txt' % str(metric_name)
                            metric_check_file = '%s/%s' % (MIRAGE_LABELLED_CHECK_PATH, processing_check_file)
                        except Exception as err:
                            logger.error('error :: mirage_labelled_metrics :: failed to interpolate metric_check_file - %s' % err)
                        if os.path.isfile(metric_check_file):
                            try:
                                os.remove(metric_check_file)
                                logger.info('mirage_labelled_metrics :: removed check file - %s' % metric_check_file)
                            except OSError:
                                pass
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: mirage_labelled_metrics :: failed to remove metric_check_file - %s' % metric_check_file)

                        # Remove the metric directory
                        # @modified 20191113 - Branch #3262: py3
                        # Convert None to str
                        # timeseries_dir = metric_name.replace('.', '/')
                        metric_data_dir = 'None'
                        try:
                            metric_name_str = str(metric_name)
                            timeseries_dir = metric_name_str.replace('.', '/')
                            metric_data_dir = '%s/%s' % (MIRAGE_LABELLED_CHECK_PATH, timeseries_dir)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: mirage_labelled_metrics :: failed to interpolate metric_data_dir')
                            metric_data_dir = 'None'

                        if os.path.exists(metric_data_dir):
                            try:
                                rmtree(metric_data_dir)
                                logger.info('mirage_labelled_metrics :: removed - %s' % metric_data_dir)
                            except:
                                logger.error('error :: mirage_labelled_metrics :: failed to rmtree %s' % metric_data_dir)

            # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            mirage_anomalous_metrics = []
            try:
                # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # literal_mirage_anomalous_metrics = list(self.redis_conn.smembers('mirage.anomalous_metrics'))
                literal_mirage_anomalous_metrics = list(self.redis_conn_decoded.smembers('mirage_labelled_metrics.anomalous_metrics'))
                for metric_list_string in literal_mirage_anomalous_metrics:
                    metric = literal_eval(metric_list_string)
                    mirage_anomalous_metrics.append(metric)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_labelled_metrics :: failed to determine list from mirage_labelled_metrics.anomalous_metrics Redis set')
                mirage_anomalous_metrics = []

            # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            mirage_not_anomalous_metrics = []
            try:
                # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # literal_mirage_not_anomalous_metrics = list(self.redis_conn.smembers('mirage.not_anomalous_metrics'))
                literal_mirage_not_anomalous_metrics = list(self.redis_conn_decoded.smembers('mirage_labelled_metrics.not_anomalous_metrics'))
                for metric_list_string in literal_mirage_not_anomalous_metrics:
                    metric = literal_eval(metric_list_string)
                    mirage_not_anomalous_metrics.append(metric)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_labelled_metrics :: failed to determine list from mirage_labelled_metrics.not_anomalous_metrics Redis set')
                mirage_not_anomalous_metrics = []

            # Log progress
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # if len(self.anomalous_metrics) > 0:
            if len(mirage_anomalous_metrics) > 0:
                logger.info('mirage_labelled_metrics :: seconds since last anomaly :: %.2f' % (time() - now))
                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # logger.info('mirage_labelled_metrics :: total anomalies    :: %d' % len(self.anomalous_metrics))
                logger.info('mirage_labelled_metrics :: total anomalies    :: %d' % len(mirage_anomalous_metrics))
                logger.info('mirage_labelled_metrics :: exception stats    :: %s' % str(exceptions))
                logger.info('mirage_labelled_metrics :: anomaly breakdown  :: %s' % str(anomaly_breakdown))

            # Log to Graphite
            if process_metric_check_files:
                n_time = time()
                run_time = n_time - run_timestamp
                logger.info('mirage_labelled_metrics :: process took %.2f seconds to run' % run_time)
                # graphite_run_time = '%.2f' % run_time
                # send_metric_name = skyline_app_graphite_namespace + '.run_time'
                # send_graphite_metric(self, skyline_app, send_metric_name, graphite_run_time)
                try:
                    self.redis_conn_decoded.hset('mirage_labelled_metrics.run_times', str(n_time), str(run_time))
                except Exception as err:
                    logger.error('error :: mirage_labelled_metrics :: failed to add run_time to mirage_labelled_metrics.run_times Redis hash - %s' % err)

            # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # Use Redis sets instead of Manager().list()
            delete_redis_sets = [
                'mirage_labelled_metrics.anomalous_metrics',
                'mirage_labelled_metrics.not_anomalous_metrics',
                'mirage_labelled_metrics.metric_variables',
                # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                # Handle once per minute
                # 'mirage.sent_to_crucible',
                # 'mirage.sent_to_panorama',
                # 'mirage.sent_to_ionosphere',
            ]
            for i_redis_set in delete_redis_sets:
                redis_set_to_delete = i_redis_set
                try:
                    self.redis_conn.delete(redis_set_to_delete)
                    logger.info('mirage_labelled_metrics :: deleted Redis set - %s' % redis_set_to_delete)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_labelled_metrics :: failed to delete Redis set - %s' % redis_set_to_delete)

            # @added 20200903 - Task #3730: Validate Mirage running multiple processes
            # Send checks.stale_discarded and checks.pending metrics
            if int(time()) >= (last_sent_to_graphite + 60):
                stale_check_discarded = []
                try:
                    stale_check_discarded = list(self.redis_conn_decoded.smembers('mirage_labelled_metrics.stale_check_discarded'))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_labelled_metrics :: failed to get mirage_labelled_metrics.stale_check_discarded set from Redis')
                    stale_check_discarded = []
                stale_check_discarded_count = len(stale_check_discarded)
                logger.info('mirage_labelled_metrics :: checks.stale_discarded   :: %s' % str(stale_check_discarded_count))
                send_metric_name = '%s.checks.stale_discarded' % skyline_app_graphite_namespace
                send_graphite_metric(self, skyline_app, send_metric_name, str(stale_check_discarded_count))
                checks_pending = [f_pending for f_pending in os.listdir(MIRAGE_LABELLED_CHECK_PATH) if os.path.isfile(os.path.join(MIRAGE_LABELLED_CHECK_PATH, f_pending))]
                checks_pending_count = len(checks_pending)
                logger.info('mirage_labelled_metrics :: checks.pending   :: %s' % str(checks_pending_count))
                send_metric_name = '%s.checks.pending' % skyline_app_graphite_namespace
                send_graphite_metric(self, skyline_app, send_metric_name, str(checks_pending_count))

                run_times = []
                try:
                    run_times_dict = self.redis_conn_decoded.hgetall('mirage_labelled_metrics.run_times')
                    self.redis_conn_decoded.delete('mirage_labelled_metrics.run_times')
                    if run_times_dict:
                        for n_time, run_time_str in run_times_dict.items():
                            run_times.append(float(run_time_str))
                except Exception as err:
                    logger.error('error :: mirage_labelled_metrics :: failed to determine run_times from mirage_labelled_metrics.run_times Redis hash - %s' % err)
                if run_times:
                    run_time = sum(run_times)
                else:
                    run_time = 0
                logger.info('mirage_labelled_metrics :: seconds to run    :: %.2f' % run_time)
                graphite_run_time = '%.2f' % run_time
                send_metric_name = skyline_app_graphite_namespace + '.run_time'
                send_graphite_metric(self, skyline_app, send_metric_name, graphite_run_time)

                try:
                    self.redis_conn_decoded.hset('mirage_labelled_metrics.run_time', 'timestamp', str(time()))
                    self.redis_conn_decoded.hset('mirage_labelled_metrics.run_time', 'value', str(run_time))
                except Exception as err:
                    logger.error('error :: mirage_labelled_metrics :: failed to add keys to mirage_labelled_metrics.run_time Redis hash - %s' % err)

                # @modified 20210309 - Task #3730: Validate Mirage running multiple processes
                # Reimplement mirage.checks.done count as increment key
                # checks_done = []
                # try:
                #     checks_done = list(self.redis_conn_decoded.smembers('mirage.checks.done'))
                checks_done = 0
                try:
                    # @modified 20230205 - Task #4844: Replace Redis getset with set with get
                    # As of Redis version 6.2.0, this command is regarded as deprecated.
                    # It can be replaced by SET with the GET argument when migrating or writing new code.
                    # checks_done_str = self.redis_conn_decoded.getset('mirage_labelled_metrics.checks.done', 0)
                    checks_done_str = self.redis_conn_decoded.set('mirage_labelled_metrics.checks.done', 0, get=True)

                    if checks_done_str:
                        checks_done = int(checks_done_str)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_labelled_metrics :: failed to get mirage_labelled_metrics.checks.done key from Redis')
                    checks_done = 0
                # checks_done_count = len(checks_done)
                # logger.info('mirage_labelled_metrics :: checks.done   :: %s' % str(checks_done_count))
                logger.info('mirage_labelled_metrics :: checks.done   :: %s' % str(checks_done))
                send_metric_name = '%s.checks.done' % skyline_app_graphite_namespace
                # send_graphite_metric(self, skyline_app, send_metric_name, str(checks_done_count))
                send_graphite_metric(self, skyline_app, send_metric_name, str(checks_done))
                # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                # Only send panorama, ionosphere and crucible metrics once a minute
                if settings.ENABLE_CRUCIBLE and settings.MIRAGE_CRUCIBLE_ENABLED:
                    try:
                        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        # sent_to_crucible = str(len(self.sent_to_crucible))#
                        # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                        #                      Branch #3262: py3
                        # sent_to_crucible = str(len(list(self.redis_conn.smembers('mirage.sent_to_crucible'))))
                        sent_to_crucible = str(len(list(self.redis_conn_decoded.smembers('mirage.sent_to_crucible'))))
                    except:
                        sent_to_crucible = '0'
                    logger.info('mirage_labelled_metrics :: sent_to_crucible   :: %s' % sent_to_crucible)
                    send_metric_name = '%s.sent_to_crucible' % skyline_app_graphite_namespace
                    send_graphite_metric(self, skyline_app, send_metric_name, sent_to_crucible)

                if settings.PANORAMA_ENABLED:
                    try:
                        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        # sent_to_panorama = str(len(self.sent_to_panorama))
                        # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                        #                      Branch #3262: py3
                        # sent_to_panorama = str(len(list(self.redis_conn.smembers('mirage.sent_to_panorama'))))
                        sent_to_panorama = str(len(list(self.redis_conn_decoded.smembers('mirage_labelled_metrics.sent_to_panorama'))))
                    except:
                        sent_to_panorama = '0'
                    logger.info('mirage_labelled_metrics :: sent_to_panorama   :: %s' % sent_to_panorama)
                    send_metric_name = '%s.sent_to_panorama' % skyline_app_graphite_namespace
                    send_graphite_metric(self, skyline_app, send_metric_name, sent_to_panorama)

                if settings.IONOSPHERE_ENABLED:
                    try:
                        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        # sent_to_ionosphere = str(len(self.sent_to_ionosphere))
                        # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                        #                      Branch #3262: py3
                        # sent_to_ionosphere = str(len(list(self.redis_conn.smembers('mirage.sent_to_ionosphere'))))
                        sent_to_ionosphere = str(len(list(self.redis_conn_decoded.smembers('mirage_labelled_metrics.sent_to_ionosphere'))))
                    except Exception as e:
                        logger.error('error :: mirage_labelled_metrics :: could not determine sent_to_ionosphere: %s' % e)
                        sent_to_ionosphere = '0'
                    logger.info('mirage_labelled_metrics :: sent_to_ionosphere :: %s' % sent_to_ionosphere)
                    send_metric_name = '%s.sent_to_ionosphere' % skyline_app_graphite_namespace
                    send_graphite_metric(self, skyline_app, send_metric_name, sent_to_ionosphere)
                last_sent_to_graphite = int(time())
                delete_redis_sets = [
                    'mirage_labelled_metrics.sent_to_crucible',
                    'mirage_labelled_metrics.sent_to_panorama',
                    'mirage_labelled_metrics.sent_to_ionosphere',
                    'mirage_labelled_metrics.stale_check_discarded',
                    # @modified 20210309 - Task #3730: Validate Mirage running multiple processes
                    # Reimplement mirage.checks.done count as increment key
                    # 'mirage.checks.done',
                    # @added 20200916 - Branch #3068: SNAB
                    #                   Task #3744: POC matrixprofile
                    # The main mirage process deletes this set not mirage_labelled_metrics
                    # mirage_snab_only_checks_redis_set,
                ]
                for i_redis_set in delete_redis_sets:
                    redis_set_to_delete = i_redis_set
                    try:
                        self.redis_conn.delete(redis_set_to_delete)
                        logger.info('mirage_labelled_metrics :: deleted Redis set - %s' % redis_set_to_delete)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_labelled_metrics :: failed to delete Redis set - %s' % redis_set_to_delete)

                # @added 20220421 - Task #3800: Handle feedback metrics in Mirage and waterfall alerts
                # Refresh
                try:
                    filesafe_names_dict = self.redis_conn_decoded.hgetall('metrics_manager.filesafe_base_names')
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_labelled_metrics :: hgetall metrics_manager.filesafe_base_names failed - %s' % err)

            # Sleep if it went too fast
            # if time() - now < 1:
            if time() - now < 59:
                logger.info('mirage_labelled_metrics :: sleeping due to low run time...')
#                sleep(10)
                sleep(1)
