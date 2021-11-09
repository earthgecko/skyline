import logging
from time import time, sleep
from threading import Thread
from multiprocessing import Process
import os
import sys
from os import kill, getpid
import traceback
from timeit import default_timer as timer
from math import ceil
from ast import literal_eval

from msgpack import Unpacker
from sqlalchemy.sql import select

import settings
from skyline_functions import (
    get_redis_conn, get_redis_conn_decoded, nonNegativeDerivative)
from functions.redis.get_metric_timeseries import get_metric_timeseries
from functions.graphite.get_metrics_timeseries import get_metrics_timeseries
from custom_algorithms import run_custom_algorithm_on_timeseries
from database import (
    get_engine, cloudburst_table_meta, ionosphere_matched_table_meta,
    ionosphere_layers_matched_table_meta, ionosphere_table_meta,
    anomalies_table_meta)
from functions.database.queries.metric_id_from_base_name import metric_id_from_base_name

skyline_app = 'luminosity'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

this_host = str(os.uname()[1])

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)

full_uniques = '%sunique_metrics' % settings.FULL_NAMESPACE
LOCAL_DEBUG = False

current_path = os.path.dirname(__file__)
root_path = os.path.dirname(current_path)

LUMINOSITY_CLOUDBURST_OPTS = {
    'default': {
        'window': 4,
        'nth_median': 6,
        'n_sigma': 6,
    }
}

try:
    LUMINOSITY_CLOUDBURST_PROCESSES = settings.LUMINOSITY_CLOUDBURST_PROCESSES
except KeyError:
    LUMINOSITY_CLOUDBURST_PROCESSES = 2
except:
    LUMINOSITY_CLOUDBURST_PROCESSES = 2

try:
    run_every = settings.LUMINOSITY_CLOUDBURST_RUN_EVERY
except KeyError:
    run_every = 900
except:
    run_every = 900


# @added 20210730 - Feature #4164: luminosity - cloudbursts
class Cloudburst(Thread):
    """
    The Cloudbursts class which controls the luminosity/cloudburst thread and
    spawned processes. luminosity/cloudburst analyses metrics to identify
    significant changepoints using the m66 algorithm.
    """

    def __init__(self, parent_pid):
        """
        Initialize Cloudburst
        """
        super(Cloudburst, self).__init__()
        self.redis_conn = get_redis_conn(skyline_app)
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            sys.exit(0)

    def find_cloudbursts(self, i, unique_metrics):
        """
        Create and manage the required lists and Redis sets
        """
        process_number = i
        spin_start = time()
        logger.info('cloudburst :: find_cloudbursts :: process %s started' % str(i))

        def get_an_engine():
            try:
                engine, log_msg, trace = get_engine(skyline_app)
                return engine, log_msg, trace
            except Exception as e:
                trace = traceback.format_exc()
                logger.error(trace)
                log_msg = 'cloudburst :: find_cloudbursts :: failed to get MySQL engine - %s' % e
                logger.error('error :: cloudburst :: find_cloudbursts :: failed to get MySQL engine - %s' % e)
                return None, log_msg, trace

        def engine_disposal(engine):
            if engine:
                try:
                    engine.dispose()
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cloudburst :: find_cloudbursts :: calling engine.dispose() - %s' % e)

        last_run_timestamp = 0
        luminosity_cloudburst_last_timestamp_key = 'luminosity.cloudburst.last_run_timestamp.process.%s' % str(i)
        try:
            last_run_timestamp = self.redis_conn_decoded.get(luminosity_cloudburst_last_timestamp_key)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: cloudburst :: find_cloudbursts :: failed to get timestamp from %s Redis key - %s' % (
                luminosity_cloudburst_last_timestamp_key, e))
            last_run_timestamp = 0
        if last_run_timestamp:
            logger.info('cloudburst :: find_cloudbursts :: %s Redis key has not expired, not running' % (
                luminosity_cloudburst_last_timestamp_key))
            return

        # Set Redis key
        try:
            self.redis_conn_decoded.setex(luminosity_cloudburst_last_timestamp_key, 60, str(int(spin_start)))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: cloudburst :: find_cloudbursts :: failed set timestamp for %s Redis key - %s' % (
                luminosity_cloudburst_last_timestamp_key, e))

        if len(unique_metrics) == 0:
            logger.error('error :: cloudburst :: find_cloudbursts :: failed to determine unique_metrics from %s Redis key' % (
                full_uniques))
            return

        # Discover assigned metrics
        keys_per_processor = int(ceil(float(len(unique_metrics)) / float(LUMINOSITY_CLOUDBURST_PROCESSES)))
        if i == LUMINOSITY_CLOUDBURST_PROCESSES:
            assigned_max = len(unique_metrics)
        else:
            assigned_max = min(len(unique_metrics), i * keys_per_processor)
        assigned_min = (i - 1) * keys_per_processor
        assigned_keys = range(assigned_min, assigned_max)

        # Compile assigned metrics
        assigned_metrics = [unique_metrics[index] for index in assigned_keys]
        use_mget = True

        # assigned_metrics = unique_metrics[500:503]
        # use_mget = False

        # align = True
        truncate_last_datapoint = True
        window = 10
        nth_median = 6
        n_sigma = 6

        # long_period_high_res = True
        long_period_high_res = False

        custom_algorithm = 'm66'
        algorithm_source = '%s/custom_algorithms/%s.py' % (root_path, custom_algorithm)
        m66_candidate_metrics = {}
        now_timestamp = int(time())
        key_reference_timestamp = (int(now_timestamp) // run_every * run_every)
        processed_metrics_key = 'luminosity.cloudburst.processed_metrics.%s' % str(key_reference_timestamp)
        cloudburst_info_key = 'luminosity.cloudburst.info.%s' % str(key_reference_timestamp)
        # This key is NOT managed in metrics manager it is managed below
        cloudburst_anomalies_processed_key = 'luminosity.cloudburst.anomalies_processed'

        # check_last = 3600
        # check_last = 5400
        check_last = (3600 * 4)
        # check_last = 86400

        long_period_check_last = (3600 * 6)
        # long_period_check_last = 86400

        if long_period_high_res:
            long_period_check_last = (3600 * 4)

        logger.info('cloudburst :: find_cloudbursts :: checking %s metrics with %s' % (
            str(len(assigned_metrics)), custom_algorithm))

        redis_hash_key = 'analyzer.metrics_manager.resolutions'
        resolutions_dict = {}
        try:
            resolutions_dict = self.redis_conn_decoded.hgetall(redis_hash_key)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: cloudburst :: find_cloudbursts :: fail to query Redis hash key %s - %s' % (
                redis_hash_key, e))
            return

        # Manage the luminosity.cloudburst.anomalies_processed delete entries
        # older than a week.  This Redis hash contains both short_period and
        # long_period identified AND processed metric timestamps to increase
        # efficiency in pushing short_period cloudbursts to second stage
        # analysis.
        cloudburst_anomalies_processed = {}
        try:
            cloudburst_anomalies_processed = self.redis_conn_decoded.hgetall(cloudburst_anomalies_processed_key)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: cloudburst :: find_cloudbursts :: fail to query Redis hash key %s - %s' % (
                cloudburst_anomalies_processed_key, e))
        for key in list(cloudburst_anomalies_processed.keys()):
            key_ts = None
            try:
                key_ts = int(str(key.split('.')[-1]))
            except Exception as e:
                logger.error('error :: cloudburst :: find_cloudbursts :: fail to determine key_ts from key %s - %s' % (
                    str(key), e))
            if isinstance(key_ts, int):
                # if key_ts < (now_timestamp - (86400 * 7)):
                if key_ts < (now_timestamp - long_period_check_last):
                    try:
                        self.redis_conn_decoded.hdel(cloudburst_anomalies_processed_key, key)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: cloudburst :: find_cloudbursts :: fail to delete %s from Redis hash key %s - %s' % (
                            str(key), cloudburst_anomalies_processed_key, e))

        raw_assigned = None
        derivative_metrics = []
        if use_mget:
            try:
                raw_assigned = self.redis_conn.mget(assigned_metrics)
                logger.info('cloudburst :: find_cloudbursts :: got raw_assigned metric data from Redis for %s metrics' % str(len(assigned_metrics)))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: find_cloudbursts :: failed to get raw_assigned from Redis - %s' % e)
            try:
                # @modified 20211012 - Feature #4280: aet.metrics_manager.derivative_metrics Redis hash
                # derivative_metrics = list(self.redis_conn_decoded.smembers('derivative_metrics'))
                derivative_metrics = list(self.redis_conn_decoded.smembers('aet.metrics_manager.derivative_metrics'))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: find_cloudbursts :: failed to get derivative_metrics from Redis - %s' % e)
                derivative_metrics = []
            try:
                non_derivative_monotonic_metrics = list(settings.NON_DERIVATIVE_MONOTONIC_METRICS)
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: find_cloudbursts :: failed to get non_derivative_monotonic_metrics from Redis - %s' % e)
                non_derivative_monotonic_metrics = []

        timer_start = timer()
        processed = 0
        no_data = 0
        too_short = 0
        too_old = 0
        not_analysed = 0
        analysed = 0

        for item_index, metric in enumerate(assigned_metrics):
            if processed:
                if (processed % 100) == 0:
                    logger.info('cloudburst :: find_cloudbursts :: processed %s of %s metrics with %s algorithm, %s significant changepoint candidates at %s hours found so far' % (
                        str(processed), str(len(assigned_metrics)), custom_algorithm,
                        str(len(m66_candidate_metrics)), str(int(check_last / 3600))))

            metric_name = metric
            if metric_name.startswith(settings.FULL_NAMESPACE):
                base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
            else:
                base_name = metric_name
                metric_name = '%s%s' % (settings.FULL_NAMESPACE, base_name)

            try:
                self.redis_conn.sadd(processed_metrics_key, metric_name)
            except Exception as e:
                logger.error('error :: cloudburst :: find_cloudbursts :: failed to add %s to %s Redis set - %s' % (
                    metric_name, processed_metrics_key, e))

            timeseries = []
            if raw_assigned:
                try:
                    raw_series = raw_assigned[item_index]
                    unpacker = Unpacker(use_list=False)
                    unpacker.feed(raw_series)
                    timeseries = list(unpacker)
                except Exception as e:
                    logger.error('error :: cloudburst :: find_cloudbursts :: failed to unpack %s timeseries - %s' % (
                        metric_name, e))
                    timeseries = []
            if timeseries:
                calculate_derivative = False
                if metric_name in derivative_metrics:
                    calculate_derivative = True
                if metric_name in non_derivative_monotonic_metrics:
                    calculate_derivative = False
                if calculate_derivative:
                    try:
                        derivative_timeseries = nonNegativeDerivative(timeseries)
                        timeseries = derivative_timeseries
                    except Exception as e:
                        logger.error('error :: cloudburst :: find_cloudbursts :: nonNegativeDerivative failed on %s - %s' % (
                            metric_name, e))
                        continue
            if not timeseries:
                try:
                    timeseries = get_metric_timeseries(skyline_app, base_name, False)
                except Exception as e:
                    logger.error('error :: cloudburst :: find_cloudbursts :: get_metric_timeseries failed for %s - %s' % (
                        base_name, e))
                    timeseries = []
            if not timeseries:
                no_data += 1
                continue
            if len(timeseries) < 3:
                too_short += 1
                continue
            timeseries_last_timestamp = int(timeseries[-1][0])
            if timeseries_last_timestamp < (now_timestamp - (run_every * 2)):
                too_old += 1
                continue

            if truncate_last_datapoint:
                timeseries_length = len(timeseries)
                timeseries = timeseries[1:(timeseries_length - 2)]

            resolution = 0
            try:
                resolution = int(resolutions_dict[base_name])
            except Exception as e:
                logger.error('error :: cloudburst :: find_cloudbursts :: failed get resolution for %s from resolutions_dict - %s' % (
                    base_name, e))
                continue

#            first_timestamp = now_timestamp - (3600 * 4)
#            timeseries = [item for item in timeseries if int(item[0]) >= first_timestamp]

            aligned_timeseries = []
            for ts, value in timeseries:
                aligned_timeseries.append([int(int(ts) // resolution * resolution), value])
            timeseries = aligned_timeseries

            custom_check_last = check_last
            window = 5
            if resolution > 60:
                custom_check_last = check_last + 1800
                window = 4
            if resolution > 600:
                custom_check_last = check_last + 3600
                window = 3
            nth_median = 6
            n_sigma = 6

            use_debug_logging = False
            debug_algorithms = False

            custom_algorithm_dict = {
                'base_name': base_name,
                'algorithm_source': algorithm_source,
                'algorithm_parameters': {
                    'nth_median': nth_median, 'sigma': n_sigma, 'window': window,
                    'minimum_sparsity': 75, 'resolution': resolution,
                    'determine_duration': True, 'return_anomalies': True,
                    'save_plots_to': False, 'save_plots_to_absolute_dir': False,
                    'filename_prefix': False, 'debug_logging': use_debug_logging,
                    'shift_to_start_of_window': False,
                },
                'max_execution_time': 0.5,
                'consensus': 1,
                'algorithms_allowed_in_consensus': [custom_algorithm],
                'run_3sigma_algorithms': False,
                'run_before_3sigma': False,
                'run_only_if_consensus': False,
                'use_with': [skyline_app],
                'debug_logging': False
            }
            result = None
            anomalyScore = None
            anomalies = []
            try:
                result, anomalyScore, anomalies = run_custom_algorithm_on_timeseries(skyline_app, self.current_pid, base_name, timeseries, custom_algorithm, custom_algorithm_dict, debug_algorithms)
                if result and debug_algorithms:
                    logger.info('cloudburst :: find_cloudbursts :: %s -  result: %s, anomalyScore: %s, anomalies: %s' % (
                        base_name, str(result), str(anomalyScore), str(len(anomalies))))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: find_cloudbursts :: failed to run custom_algorithm %s on %s - %s' % (
                    custom_algorithm, base_name, e))
                continue
            if result is None:
                not_analysed += 1
            else:
                analysed += 1
            processed += 1
            new_anomalies = []
            if anomalies:
                anomaly_timestamps = [int(item[0]) for item in anomalies]
                anomalies_present_in_period = [ts for ts in anomaly_timestamps if int(ts) > (now_timestamp - custom_check_last)]
                if len(anomalies_present_in_period) == 0:
                    continue
                for item in anomalies:
                    if int(item[0]) not in anomalies_present_in_period:
                        continue
                    already_added = None
                    processed_anomaly_key = '%s.%s' % (base_name, str(int(item[0])))
                    try:
                        already_added = self.redis_conn_decoded.hget(cloudburst_anomalies_processed_key, processed_anomaly_key)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: cloudburst :: find_cloudbursts :: failed at determine already_added from %s - %s' % (
                            processed_anomaly_key, e))
                    if not already_added:
                        new_anomalies.append(item)
            if new_anomalies:
                m66_candidate_metrics[base_name] = {}
                m66_candidate_metrics[base_name][custom_algorithm] = {}
                m66_candidate_metrics[base_name][custom_algorithm]['anomalies'] = new_anomalies
        timer_end = timer()
        logger.info('cloudburst :: find_cloudbursts :: found %s candidate_metrics with %s algorithm from %s processed metrics in %.6f seconds' % (
            str(len(m66_candidate_metrics)), custom_algorithm,
            str(processed), (timer_end - timer_start)))

        info_data_dict = {
            'processed': processed,
            'analysed': analysed,
            'not_analysed': not_analysed,
            'no_data': no_data,
            'too_short': too_short,
            'too_old': too_old,
        }
        try:
            self.redis_conn_decoded.hset(cloudburst_info_key, process_number, str(info_data_dict))
        except Exception as e:
            logger.error('error :: cloudburst :: find_cloudbursts :: failed to add %s Redis hash key %s - %s' % (
                str(info_data_dict), cloudburst_info_key, e))

        candidate_metrics_key = 'luminosity.cloudburst.candidate_metrics.short_period.%s' % str(key_reference_timestamp)
        key_created = False
        for base_name in list(m66_candidate_metrics.keys()):
            try:
                self.redis_conn.hset(candidate_metrics_key, base_name, str(m66_candidate_metrics[base_name]))
                self.redis_conn.expire(candidate_metrics_key, 3600)
                key_created = True
            except Exception as e:
                logger.error('error :: cloudburst :: find_cloudbursts :: failed to create %s Redis hash key - %s' % (
                    candidate_metrics_key, e))
        if key_created:
            logger.info('cloudburst :: find_cloudbursts :: created %s Redis hash key' % (
                candidate_metrics_key))

        # Second stage analysis, check to see if the changepoint is anomalous
        # at 7 days
        second_stage_m66_candidate_metrics = {}
        timer_start = timer()
        if len(m66_candidate_metrics) > 0:
            logger.info('cloudburst :: find_cloudbursts :: checking %s candidate_metrics with %s algorithm at 7 days' % (
                str(len(m66_candidate_metrics)), custom_algorithm))
        from_timestamp = int(now_timestamp) - (86400 * 7)
        until_timestamp = int(now_timestamp)
        window = 4
        summarize_intervalString = '15min'
        summarize_func = 'median'
        nth_median = 6
        n_sigma = 6
        resolution = 900

        if long_period_high_res:
            window = 10
            nth_median = 6
            n_sigma = 6
            resolution = 60

        truncate_last_datapoint = True
        metrics_to_do = list(m66_candidate_metrics.keys())
        while len(metrics_to_do) > 0:
            current_base_names = []
            while len(current_base_names) < 50 and len(metrics_to_do) > 0:
                current_base_names.append(metrics_to_do.pop(0))
            metrics_functions = {}
            truncate_last_datapoint = False
            for base_name in current_base_names:
                metrics_functions[base_name] = {}
                if long_period_high_res:
                    metrics_functions[base_name]['functions'] = None
                else:
                    metrics_functions[base_name]['functions'] = {'summarize': {'intervalString': summarize_intervalString, 'func': summarize_func}}

                # If the timeseries is summarized truncate the last datapoint
                # so it does not fall off a cliff
                truncate_last_datapoint = True
            try:
                metrics_timeseries = get_metrics_timeseries(skyline_app, metrics_functions, from_timestamp, until_timestamp, log=False)
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: find_cloudbursts :: get_metrics_timeseries failed - %s' % e)

            for base_name in current_base_names:
                timeseries = []
                try:
                    timeseries = metrics_timeseries[base_name]['timeseries']
                except KeyError:
                    timeseries = []

                if not timeseries:
                    logger.error('error :: cloudburst :: find_cloudbursts :: no timeseries from Graphite for %s' % base_name)
                    continue

                if truncate_last_datapoint:
                    timeseries_length = len(timeseries)
                    timeseries = timeseries[1:(timeseries_length - 2)]

                if long_period_high_res:
                    aligned_timeseries = []
                    for ts, value in timeseries:
                        aligned_timeseries.append([int(int(ts) // resolution * resolution), value])
                    timeseries = aligned_timeseries

                some_debug = False
                if some_debug:
                    logger.debug('debug :: cloudburst :: find_cloudbursts :: checking timeseries of length %s from Graphite for %s' % (
                        str(len(timeseries)), base_name))

                custom_algorithm_dict = {
                    'base_name': base_name,
                    'algorithm_source': algorithm_source,
                    'algorithm_parameters': {
                        'nth_median': nth_median, 'sigma': n_sigma, 'window': window,
                        'minimum_sparsity': 0, 'resolution': resolution,
                        'determine_duration': True, 'return_anomalies': True,
                        'save_plots_to': False, 'save_plots_to_absolute_dir': False,
                        'filename_prefix': False, 'debug_logging': False,
                    },
                    'max_execution_time': 0.5,
                    'consensus': 1,
                    'algorithms_allowed_in_consensus': [custom_algorithm],
                    'run_3sigma_algorithms': False,
                    'run_before_3sigma': False,
                    'run_only_if_consensus': False,
                    'use_with': [skyline_app],
                    'debug_logging': False
                }
                debug_algorithms = False
                result = None
                anomalyScore = None
                anomalies = []
                try:
                    result, anomalyScore, anomalies = run_custom_algorithm_on_timeseries(skyline_app, self.current_pid, base_name, timeseries, custom_algorithm, custom_algorithm_dict, debug_algorithms)
                    if result and debug_algorithms:
                        logger.info('cloudburst :: find_cloudbursts :: %s -  result: %s, anomalyScore: %s, anomalies: %s' % (
                            base_name, str(result), str(anomalyScore), str(len(anomalies))))
                    if some_debug:
                        logger.debug('debug :: cloudburst :: find_cloudbursts :: %s -  result: %s, anomalyScore: %s, anomalies: %s' % (
                            base_name, str(result), str(anomalyScore), str(len(anomalies))))
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cloudburst :: find_cloudbursts :: failed to run custom_algorithm %s on %s - %s' % (
                        custom_algorithm, base_name, e))
                    continue

                new_anomalies = []
                if anomalies:
                    anomaly_timestamps = [int(item[0]) for item in anomalies]
                    anomalies_present_in_period = [ts for ts in anomaly_timestamps if int(ts) > (now_timestamp - long_period_check_last)]
                    if len(anomalies_present_in_period) == 0:
                        if some_debug:
                            logger.debug('debug :: cloudburst :: find_cloudbursts :: %s has no cloudbursts in period' % base_name)
                        continue
                    for item in anomalies:
                        if int(item[0]) not in anomalies_present_in_period:
                            continue
                        already_added = None
                        processed_anomaly_key = '%s.%s' % (base_name, str(int(item[0])))
                        try:
                            already_added = self.redis_conn_decoded.hget(cloudburst_anomalies_processed_key, processed_anomaly_key)
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: cloudburst :: find_cloudbursts :: failed at determine already_added from %s - %s' % (
                                processed_anomaly_key, e))
                        if not already_added:
                            new_anomalies.append(item)
                        else:
                            if some_debug:
                                logger.debug('debug :: cloudburst :: find_cloudbursts :: all %s cloudbursts in period for %s have already been processed' % (
                                    str(len(anomalies)), base_name))
                if new_anomalies:
                    logger.info('cloudburst :: find_cloudbursts :: %s found %s new anomalies in window - %s -  result: %s, anomalyScore: %s, anomalies: %s' % (
                        custom_algorithm, str(len(new_anomalies)),
                        base_name, str(result), str(anomalyScore),
                        str(len(new_anomalies))))
                    second_stage_m66_candidate_metrics[base_name] = {}
                    second_stage_m66_candidate_metrics[base_name][custom_algorithm] = {}
                    second_stage_m66_candidate_metrics[base_name][custom_algorithm]['anomalies'] = new_anomalies
        timer_end = timer()
        logger.info('cloudburst :: find_cloudbursts :: found %s 2nd stage candidate_metrics with %s algorithm at 7 days from the 24 hour candidate metrics in %.6f seconds' % (
            str(len(second_stage_m66_candidate_metrics)), custom_algorithm,
            (timer_end - timer_start)))

        candidate_metrics_key = 'luminosity.cloudburst.identified.long_period.%s' % str(key_reference_timestamp)
        key_created = False
        for base_name in list(second_stage_m66_candidate_metrics.keys()):
            try:
                self.redis_conn.hset(candidate_metrics_key, base_name, str(second_stage_m66_candidate_metrics[base_name]))
                self.redis_conn.expire(candidate_metrics_key, 14400)
                key_created = True
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: find_cloudbursts :: failed to create %s Redis hash key - %s' % (
                    candidate_metrics_key, e))
        if key_created:
            logger.info('cloudburst :: find_cloudbursts :: created %s Redis hash key' % (
                candidate_metrics_key))

        # OK so...
        # Add to DB
        # eating-snow
        # https://soundcloud.com/user-76297227/eating-snow-this-emptiness-is-mine-andhim-remix-1?in_system_playlist=weekly%3A%3Aearthgecko
        if len(second_stage_m66_candidate_metrics) > 0:
            logger.info('cloudburst :: find_cloudbursts :: added %s 2nd stage candidate_metrics to %s Redis hash' % (
                str(len(second_stage_m66_candidate_metrics)), candidate_metrics_key))

        # Define each cloudburst incident by each period in terms of a start
        # and end period of the cloudburst.  A period is defined by a
        # continuous triggering of m66, if there is a single m66 anomaly
        # triggered then the period will be from anomaly_timestamp, until
        # (anomaly_timestamp + resolution)
        full_duration = until_timestamp - from_timestamp
        found_cloudbursts = {}
        for base_name in list(second_stage_m66_candidate_metrics.keys()):
            try:
                found_cloudbursts[base_name] = {}
                anomalies = second_stage_m66_candidate_metrics[base_name][custom_algorithm]['anomalies']
                initial_start_ts = int(anomalies[0][0])
                start_ts = int(anomalies[0][0])
                last_ts = start_ts
                for ts in [int(ts) for ts, value in anomalies]:
                    if ts == initial_start_ts:
                        last_ts = ts
                        continue
                    if ts == (last_ts + resolution):
                        last_ts = ts
                        continue
                    if ts > (last_ts + resolution):
                        found_cloudbursts[base_name][start_ts] = {}
                        found_cloudbursts[base_name][start_ts]['resolution'] = resolution
                        found_cloudbursts[base_name][start_ts]['full_duration'] = full_duration
                        found_cloudbursts[base_name][start_ts]['from'] = from_timestamp
                        found_cloudbursts[base_name][start_ts]['until'] = until_timestamp
                        found_cloudbursts[base_name][start_ts]['start'] = start_ts
                        found_cloudbursts[base_name][start_ts]['end'] = last_ts
                        found_cloudbursts[base_name][start_ts]['duration'] = last_ts - start_ts
                        # This new timestamp that is more than one step from
                        # the last_ts defines the start of a new period
                        start_ts = ts
                    last_ts = ts
                if initial_start_ts == last_ts:
                    last_ts = initial_start_ts + resolution
                found_cloudbursts[base_name][start_ts] = {}
                found_cloudbursts[base_name][start_ts]['resolution'] = resolution
                found_cloudbursts[base_name][start_ts]['full_duration'] = full_duration
                found_cloudbursts[base_name][start_ts]['from'] = from_timestamp
                found_cloudbursts[base_name][start_ts]['until'] = until_timestamp
                found_cloudbursts[base_name][start_ts]['start'] = start_ts
                found_cloudbursts[base_name][start_ts]['end'] = last_ts
                found_cloudbursts[base_name][start_ts]['duration'] = last_ts - start_ts
                logger.info('cloudburst :: find_cloudbursts :: %s cloudburst periods identified for %s' % (
                    str(len(found_cloudbursts[base_name])), base_name))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: find_cloudbursts :: failed to add %s to found_cloudbursts dict - %s' % (base_name, e))

        new_cloudburst_ids = []
        known_cloudbursts = {}
        matches_in_period = {}
        matches_in_period['fp'] = {}
        matches_in_period['layer'] = {}
        matched_metric_ids = []
        found_metric_cloudbursts = {}
        period_anomalies = {}
        cloudburst_metrics_key_created = False

        if len(found_cloudbursts) > 0:
            try:
                engine, log_msg, trace = get_an_engine()
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: find_cloudbursts :: could not get a MySQL engine to update cloudburst table - %s' % e)

            if not engine:
                logger.error('error :: cloudburst :: find_cloudbursts :: engine not obtained to update cloudburst table')

            cloudburst_table = None
            try:
                cloudburst_table, log_msg, trace = cloudburst_table_meta(skyline_app, engine)
                logger.info('cloudburst :: find_cloudbursts :: cloudburst_table OK')
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: find_cloudbursts :: failed to get cloudburst_table meta - %s' % e)
                cloudburst_table = None

            if cloudburst_table is None:
                logger.error('error :: cloudburst :: find_cloudbursts :: cloudburst_table not defined')

            # @added 20210819 - Feature #4164: luminosity - cloudbursts
            # Determine first matched id in period to reduce the
            # mysql.handler_read_next count from doing index scans and use
            # primary key instead
            check_period = now_timestamp - long_period_check_last
            first_cloudburst_id_in_period = None
            logger.info('cloudburst :: find_cloudbursts :: determining first cloudburst id in period')
            try:
                connection = engine.connect()
                stmt = select([cloudburst_table.c.id]).\
                    where(cloudburst_table.c.timestamp >= check_period).\
                    order_by(cloudburst_table.c.id.asc()).limit(1)
                result = connection.execute(stmt)
                for row in result:
                    first_cloudburst_id_in_period = row['id']
                    break
                connection.close()
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: find_cloudbursts :: could not determine first cloudburst id - %s' % e)
            logger.info('cloudburst :: find_cloudbursts :: first cloudburst id in the period is: %s' % (
                str(first_cloudburst_id_in_period)))

            cloudburst_metrics_key = 'luminosity.cloudburst.metrics.%s' % str(key_reference_timestamp)
            new_cloudburst_ids = []
            known_cloudbursts = {}
            period_cloudbursts = 0
            # one_day_ago = now_timestamp - 86400
            if first_cloudburst_id_in_period:
                try:
                    connection = engine.connect()
                    # @modified 20210819 - Feature #4164: luminosity - cloudbursts
                    # Added end so that the period can be compared
                    # stmt = select([cloudburst_table.c.metric_id, cloudburst_table.c.timestamp, cloudburst_table.c.end]).\
                    # @modified 20210819 - Feature #4164: luminosity - cloudbursts
                    # Reduce the mysql.handler_read_next count from doing
                    # index scans and use primary key
                    # stmt = select([cloudburst_table.c.metric_id, cloudburst_table.c.timestamp, cloudburst_table.c.end]).\
                    #     where(cloudburst_table.c.timestamp >= one_day_ago)
                    stmt = select([cloudburst_table.c.metric_id, cloudburst_table.c.timestamp, cloudburst_table.c.end]).\
                        where(cloudburst_table.c.id >= first_cloudburst_id_in_period)
                    result = connection.execute(stmt)
                    for row in result:
                        metric_id = row['metric_id']
                        if metric_id not in list(known_cloudbursts.keys()):
                            known_cloudbursts[metric_id] = {}
                        known_timestamp = row['timestamp']
                        # known_cloudbursts[metric_id][known_timestamp] = metric_id
                        # @added 20210819 - Feature #4164: luminosity - cloudbursts
                        known_cloudbursts[metric_id][known_timestamp] = {}
                        known_cloudbursts[metric_id][known_timestamp][metric_id] = metric_id
                        known_cloudbursts[metric_id][known_timestamp]['end'] = row['end']
                        period_cloudbursts += 1
                    connection.close()
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cloudburst :: find_cloudbursts :: could not get a MySQL engine to update cloudburst table - %s' % e)
                logger.info('cloudburst :: find_cloudbursts :: found %s cloudbursts recorded in the DB in the %s seconds period' % (
                    str(period_cloudbursts), str(long_period_check_last)))

####
# ONLY make the DB queries if there are unknown cloudbursts
            for base_name in list(found_cloudbursts.keys()):
                # logger.info('cloudburst :: find_cloudbursts :: checking cloudbursts to insert for %s' % base_name)
                metric_id = None
                try:
                    metric_id = metric_id_from_base_name(skyline_app, base_name)
                except Exception as e:
                    logger.error('error :: cloudburst :: find_cloudbursts :: failed to get metric id for %s from db: %s' % (base_name, e))
                    continue
                logger.info('cloudburst :: find_cloudbursts :: checking cloudbursts to insert for %s with metric_id: %s' % (
                    base_name, str(metric_id)))
                known_metric_cloudburst_timestamps = []
                try:
                    known_metric_cloudburst_timestamps = list(known_cloudbursts[metric_id].keys())
                except KeyError:
                    known_metric_cloudburst_timestamps = []
                except Exception as e:
                    logger.error('error :: cloudburst :: find_cloudbursts :: failed to determine known_metric_timestamps for %s - %s' % (base_name, e))
                too_old = 0
                for timestamp in list(found_cloudbursts[base_name].keys()):
                    try:
                        if timestamp < (now_timestamp - long_period_check_last):
                            too_old += 1
                            continue
                        if timestamp in known_metric_cloudburst_timestamps:
                            logger.info('cloudburst :: find_cloudbursts :: known cloudburst - skipping cloudburst at %s for %s' % (
                                str(timestamp), base_name))
                            continue
                        for known_metric_cloudburst_timestamp in known_metric_cloudburst_timestamps:
                            end = known_cloudbursts[metric_id][known_metric_cloudburst_timestamp]['end']
                            if timestamp >= known_metric_cloudburst_timestamp and timestamp <= end:
                                logger.info('cloudburst :: find_cloudbursts :: known cloudburst - skipping cloudburst at %s for %s, found in a period of cloudburst starting at %s and ending %s' % (
                                    str(timestamp), base_name,
                                    str(known_metric_cloudburst_timestamp), str(end)))
                                continue
                        if base_name not in list(found_metric_cloudbursts.keys()):
                            found_metric_cloudbursts[base_name] = {}
                        found_metric_cloudbursts[base_name][timestamp] = found_cloudbursts[base_name][timestamp]
                        # @modified 20210819 - Feature #4164: luminosity - cloudbursts
                        # Added end so that the period can be compared
                        found_metric_cloudbursts[base_name][timestamp]['end'] = found_cloudbursts[base_name][timestamp]['end']
                        logger.info('cloudburst :: find_cloudbursts :: new cloudburst found for %s: %s' % (
                            base_name, str(found_metric_cloudbursts[base_name][timestamp])))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: cloudburst :: find_cloudbursts :: failed to determine if known cloudburst for %s - %s' % (base_name, err))

                if base_name not in list(found_metric_cloudbursts.keys()):
                    logger.info('cloudburst :: find_cloudbursts :: all identified cloudbursts for %s are known and %s are too old' % (
                        base_name, str(too_old)))
                else:
                    logger.info('cloudburst :: find_cloudbursts :: %s new cloudbursts for %s are not known' % (
                        str(len(list(found_metric_cloudbursts[base_name].keys()))), base_name))

        # Consolidate back to back periods
        if len(found_metric_cloudbursts) > 0:
            for base_name in list(found_metric_cloudbursts.keys()):
                cloudburst_tss = sorted(list(found_metric_cloudbursts[base_name].keys()))
                merged = []
                for c_index, cloudburst_ts in enumerate(cloudburst_tss):
                    if c_index == 0:
                        continue
                    if cloudburst_ts in merged:
                        continue
                    for c2_index, c_ts in enumerate(cloudburst_tss):
                        # if c2_index == 0:
                        #     continue
                        if c_ts in merged:
                            continue
                        if c2_index == c_index:
                            continue
                        c_start = found_metric_cloudbursts[base_name][c_ts]['start']
                        c_end = found_metric_cloudbursts[base_name][c_ts]['end']
                        if cloudburst_ts >= c_start and cloudburst_ts <= c_end:
                            logger.info('%s falls within period of (%s, %s)' % (str(cloudburst_ts), str(c_start), str(c_end)))
                            ts_list = [ts for c_i, ts in enumerate(cloudburst_tss) if c_i == c2_index]
                            ts = ts_list[0]
                            # start = found_metric_cloudbursts[base_name][c_ts]['start']
                            end = found_metric_cloudbursts[base_name][c_ts]['end']
                            if end > found_metric_cloudbursts[base_name][cloudburst_ts]['end']:
                                logger.info('merging %s with %s' % (
                                    str(found_metric_cloudbursts[base_name][ts]),
                                    str(found_metric_cloudbursts[base_name][cloudburst_ts])))
                                new_end = found_metric_cloudbursts[base_name][c_ts]['end']
                                found_metric_cloudbursts[base_name][cloudburst_ts]['end'] = new_end
                                found_metric_cloudbursts[base_name][cloudburst_ts]['duration'] = new_end - cloudburst_ts
                                merge = True
                        if merge:
                            del found_metric_cloudbursts[base_name][c_ts]
                            merged.append(c_ts)

        if len(found_metric_cloudbursts) > 0:
            # Determine what Ionosphere matches occurred in the cloudburst
            # period
            # SELECT id,layer_id,fp_id,metric_id,anomaly_timestamp FROM ionosphere_layers_matched WHERE anomaly_timestamp > (now_timestamp - long_period_check_last)
            # SELECT id,fp_id,metric_timestamp FROM ionosphere_matched WHERE metric_timestamp > (now_timestamp - long_period_check_last)
            newer_than_timestamp = now_timestamp - long_period_check_last
            ionosphere_matched_table = None
            try:
                ionosphere_matched_table, log_msg, trace = ionosphere_matched_table_meta(skyline_app, engine)
                logger.info('cloudburst :: find_cloudbursts :: ionosphere_matched_table OK')
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: find_cloudbursts :: failed to get ionosphere_matched_table meta - %s' % e)
                ionosphere_matched_table = None

            # @modified 20210819 - Feature #4164: luminosity - cloudbursts
            # Reduce the mysql.handler_read_next count from doing index scans
            # when the query uses metric_timestamp >= newer_than_timestamp, by
            # first identifying the firsted matched id in the period and then
            # basing the query on id >= first_matched_id_in_period, reduces the
            # handler_read_next from 242561
            # EXPLAIN SELECT ionosphere_matched.id, ionosphere_matched.fp_id, ionosphere_matched.metric_timestamp
            # FROM ionosphere_matched
            # WHERE ionosphere_matched.metric_timestamp >= 1629272047"
            # +------+-------------+--------------------+-------+---------------+--------------------------+---------+------+--------+--------------------------+
            # | id   | select_type | table              | type  | possible_keys | key                      | key_len | ref  | rows   | Extra                    |
            # +------+-------------+--------------------+-------+---------------+--------------------------+---------+------+--------+--------------------------+
            # |    1 | SIMPLE      | ionosphere_matched | index | NULL          | features_profile_matched | 13      | NULL | 242561 | Using where; Using index |
            # +------+-------------+--------------------+-------+---------------+--------------------------+---------+------+--------+--------------------------+
            # to 339
            # "EXPLAIN SELECT id FROM ionosphere_matched WHERE metric_timestamp >= 1629272047 ORDER BY id ASC LIMIT 1"
            # +------+-------------+--------------------+-------+---------------+---------+---------+------+------+-------------+
            # | id   | select_type | table              | type  | possible_keys | key     | key_len | ref  | rows | Extra       |
            # +------+-------------+--------------------+-------+---------------+---------+---------+------+------+-------------+
            # |    1 | SIMPLE      | ionosphere_matched | index | NULL          | PRIMARY | 4       | NULL | 1    | Using where |
            # +------+-------------+--------------------+-------+---------------+---------+---------+------+------+-------------+
            # "SELECT id FROM ionosphere_matched WHERE metric_timestamp >= 1629272047 ORDER BY id ASC LIMIT 1"
            # +--------+
            # | id     |
            # +--------+
            # | 249655 |
            # +--------+
            # "EXPLAIN SELECT ionosphere_matched.id, ionosphere_matched.fp_id, ionosphere_matched.metric_timestamp
            # FROM ionosphere_matched
            # WHERE ionosphere_matched.id >= 249655"
            # +------+-------------+--------------------+-------+----------------------------------+--------------------------+---------+------+------+--------------------------+
            # | id   | select_type | table              | type  | possible_keys                    | key                      | key_len | ref  | rows | Extra                    |
            # +------+-------------+--------------------+-------+----------------------------------+--------------------------+---------+------+------+--------------------------+
            # |    1 | SIMPLE      | ionosphere_matched | range | PRIMARY,features_profile_matched | features_profile_matched | 4       | NULL | 338  | Using where; Using index |
            # +------+-------------+--------------------+-------+----------------------------------+--------------------------+---------+------+------+--------------------------+
            # Determine first matched id in period
            first_matched_id_in_period = None
            logger.info('cloudburst :: find_cloudbursts :: determining first ionosphere match id in period')
            try:
                connection = engine.connect()
                stmt = select([ionosphere_matched_table.c.id]).\
                    where(ionosphere_matched_table.c.metric_timestamp >= newer_than_timestamp).\
                    order_by(ionosphere_matched_table.c.id.asc()).limit(1)
                result = connection.execute(stmt)
                for row in result:
                    first_matched_id_in_period = row['id']
                    break
                connection.close()
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: find_cloudbursts :: could not determine ionosphere matches - %s' % e)
            logger.info('cloudburst :: find_cloudbursts :: the first fp match id for the period is %s' % (
                str(first_matched_id_in_period)))

            if first_matched_id_in_period:
                logger.info('cloudburst :: find_cloudbursts :: determining ionosphere matches in period')
                try:
                    connection = engine.connect()
                    # @modified 20210819 - Feature #4164: luminosity - cloudbursts
                    # Reduce the mysql.handler_read_next count from doing
                    # index scans and use primary key
                    # stmt = select([ionosphere_matched_table.c.id, ionosphere_matched_table.c.fp_id, ionosphere_matched_table.c.metric_timestamp]).\
                    #    where(ionosphere_matched_table.c.metric_timestamp >= newer_than_timestamp)
                    stmt = select([ionosphere_matched_table.c.id, ionosphere_matched_table.c.fp_id, ionosphere_matched_table.c.metric_timestamp]).\
                        where(ionosphere_matched_table.c.id >= first_matched_id_in_period)
                    result = connection.execute(stmt)
                    for row in result:
                        match_id = row['id']
                        matches_in_period['fp'][match_id] = {}
                        matches_in_period['fp'][match_id]['fp_id'] = row['fp_id']
                        matches_in_period['fp'][match_id]['timestamp'] = match_id = row['metric_timestamp']
                    connection.close()
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cloudburst :: find_cloudbursts :: could not determine ionosphere matches - %s' % e)
                logger.info('cloudburst :: find_cloudbursts :: %s Ionosphere fp matches recorded in the DB for the period' % (
                    str(len(list(matches_in_period['fp'].keys())))))

            ionosphere_layers_matched_table = None
            try:
                ionosphere_layers_matched_table, log_msg, trace = ionosphere_layers_matched_table_meta(skyline_app, engine)
                logger.info('cloudburst :: find_cloudbursts :: ionosphere_layers_matched_table OK')
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: find_cloudbursts :: failed to get ionosphere_layers_matched_table meta - %s' % e)
                ionosphere_layers_matched_table = None

            # @added 20210819 - Feature #4164: luminosity - cloudbursts
            # Determine first matched id in period to reduce the
            # mysql.handler_read_next count from doing index scans and use
            # primary key instead
            first_layers_matched_id_in_period = None
            logger.info('cloudburst :: find_cloudbursts :: determining first ionosphere layers match id in period')
            try:
                connection = engine.connect()
                stmt = select([ionosphere_layers_matched_table.c.id]).\
                    where(ionosphere_layers_matched_table.c.anomaly_timestamp >= newer_than_timestamp).\
                    order_by(ionosphere_layers_matched_table.c.id.asc()).limit(1)
                result = connection.execute(stmt)
                for row in result:
                    first_layers_matched_id_in_period = row['id']
                    break
                connection.close()
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: find_cloudbursts :: could not determine first ionosphere match id - %s' % e)
            logger.info('cloudburst :: find_cloudbursts :: first ionosphere layers match id in the period is: %s' % (
                str(first_layers_matched_id_in_period)))

            if first_layers_matched_id_in_period:
                try:
                    connection = engine.connect()
                    # @modified 20210819 - Feature #4164: luminosity - cloudbursts
                    # Reduce the mysql.handler_read_next count from doing
                    # index scans and use primary key
                    # stmt = select([ionosphere_layers_matched_table.c.id, ionosphere_layers_matched_table.c.layer_id, ionosphere_layers_matched_table.c.fp_id, ionosphere_layers_matched_table.c.metric_id, ionosphere_layers_matched_table.c.anomaly_timestamp]).\
                    #     where(ionosphere_layers_matched_table.c.anomaly_timestamp >= newer_than_timestamp)
                    stmt = select([ionosphere_layers_matched_table.c.id, ionosphere_layers_matched_table.c.layer_id, ionosphere_layers_matched_table.c.fp_id, ionosphere_layers_matched_table.c.metric_id, ionosphere_layers_matched_table.c.anomaly_timestamp]).\
                        where(ionosphere_layers_matched_table.c.id >= first_layers_matched_id_in_period)
                    result = connection.execute(stmt)
                    for row in result:
                        match_id = row['id']
                        matches_in_period['layer'][match_id] = {}
                        matches_in_period['layer'][match_id]['match_id'] = match_id
                        matches_in_period['layer'][match_id]['layer_id'] = row['layer_id']
                        matches_in_period['layer'][match_id]['fp_id'] = row['fp_id']
                        matches_in_period['layer'][match_id]['metric_id'] = row['metric_id']
                        matches_in_period['layer'][match_id]['timestamp'] = row['anomaly_timestamp']
                    connection.close()
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cloudburst :: find_cloudbursts :: could not determine ionosphere layers matches - %s' % e)
                logger.info('cloudburst :: find_cloudbursts :: %s Ionosphere layers matches recorded in the DB for the period' % (
                    str(len(list(matches_in_period['layer'].keys())))))

            # matched_metric_ids = []
            for layer_match_id in list(matches_in_period['layer'].keys()):
                matched_metric_ids.append(matches_in_period['layer'][layer_match_id]['metric_id'])

            # Determine metric_id from the ionosphere table from fps of matches
            # but these are expensive queries, maybe not so much with IN
            ionosphere_table = None
            try:
                ionosphere_table, log_msg, trace = ionosphere_table_meta(skyline_app, engine)
                logger.info('cloudburst :: find_cloudbursts :: ionosphere_table OK')
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: find_cloudbursts :: failed to get ionosphere_table meta - %s' % e)
                ionosphere_table = None
            fp_metric_ids_list = []
            for match_id in list(matches_in_period['fp'].keys()):
                # SELECT metric_id FROM
                metric_id = None
                fp_id = matches_in_period['fp'][match_id]['fp_id']
                try:
                    connection = engine.connect()
                    stmt = select([ionosphere_table.c.metric_id]).\
                        where(ionosphere_table.c.id == fp_id)
                    result = connection.execute(stmt)
                    for row in result:
                        metric_id = row['metric_id']
                        matches_in_period['fp'][match_id]['metric_id'] = metric_id
                        fp_metric_ids_list.append(metric_id)
                    connection.close()
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cloudburst :: find_cloudbursts :: could not determine metric_id from ionosphere - %s' % e)
            fp_metric_ids_list = list(set(fp_metric_ids_list))
            if fp_metric_ids_list:
                matched_metric_ids = matched_metric_ids + fp_metric_ids_list
            matched_metric_ids = list(set(matched_metric_ids))

            anomalies_table = None
            try:
                anomalies_table, log_msg, trace = anomalies_table_meta(skyline_app, engine)
                logger.info('cloudburst :: find_cloudbursts :: anomalies_table OK')
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: find_cloudbursts :: failed to get anomalies_table meta - %s' % e)
                anomalies_table = None

            # @added 20210819 - Feature #4164: luminosity - cloudbursts
            # Determine first anomaly id in period to reduce the
            # mysql.handler_read_next count from doing index scans and use
            # primary key instead
            first_anomaly_id_in_period = None
            logger.info('cloudburst :: find_cloudbursts :: determining first anomaly id in period')
            try:
                connection = engine.connect()
                stmt = select([anomalies_table.c.id]).\
                    where(anomalies_table.c.anomaly_timestamp >= newer_than_timestamp).\
                    order_by(anomalies_table.c.id.asc()).limit(1)
                result = connection.execute(stmt)
                for row in result:
                    first_anomaly_id_in_period = row['id']
                    break
                connection.close()
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: find_cloudbursts :: could not determine first anomaly id - %s' % e)
            logger.info('cloudburst :: find_cloudbursts :: first anomaly id in the period is: %s' % (
                str(first_anomaly_id_in_period)))

            anomaly_id = None
            period_anomalies = {}
            if first_anomaly_id_in_period:
                try:
                    connection = engine.connect()
                    # @modified 20210819 - Feature #4164: luminosity - cloudbursts
                    # Reduce the mysql.handler_read_next count from doing
                    # index scans and use primary key
                    # stmt = select([anomalies_table.c.id, anomalies_table.c.metric_id, anomalies_table.c.anomaly_timestamp, ]).\
                    #     where(anomalies_table.c.anomaly_timestamp >= newer_than_timestamp)
                    stmt = select([anomalies_table.c.id, anomalies_table.c.metric_id, anomalies_table.c.anomaly_timestamp, ]).\
                        where(anomalies_table.c.id >= first_anomaly_id_in_period)
                    result = connection.execute(stmt)
                    for row in result:
                        metric_id = row['metric_id']
                        if metric_id not in list(period_anomalies.keys()):
                            period_anomalies[metric_id] = {}
                        anomaly_id = int(row['id'])
                        anomaly_timestamp = row['anomaly_timestamp']
                        period_anomalies[metric_id][anomaly_timestamp] = {}
                        period_anomalies[metric_id][anomaly_timestamp]['id'] = anomaly_id
                    connection.close()
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cloudburst :: find_cloudbursts :: could not determine metric_id from ionosphere - %s' % e)
                logger.info('cloudburst :: find_cloudbursts :: %s anomalies found in the period' % str(len(period_anomalies)))

        found_cloudbursts_to_add = {}
#        cloudburst_metrics_key = 'luminosity.cloudburst.metrics.%s' % str(key_reference_timestamp)
#        for base_name in list(found_cloudbursts.keys()):
        for base_name in list(found_metric_cloudbursts.keys()):
            metric_id = None
            try:
                metric_id = metric_id_from_base_name(skyline_app, base_name)
            except Exception as e:
                logger.error('error :: cloudburst :: find_cloudbursts :: failed to get metric id for %s from db: %s' % (base_name, e))
                continue
            logger.info('cloudburst :: find_cloudbursts :: checking anomalies and matches in period for %s with metric_id: %s' % (
                base_name, str(metric_id)))

            # Extract the matches for the metric and create a dictionary
            # based on the match timestamp not the match id.  Although there
            # are both fp and layer matches they will never have the same
            # timestamp because only a fp or a layer can match a potential
            # anomaly, not both.
            metric_matches = {}
            if metric_id in matched_metric_ids:
                for match_type in list(matches_in_period.keys()):
                    for match_id in list(matches_in_period[match_type].keys()):
                        try:
                            if metric_id == matches_in_period[match_type][match_id]['metric_id']:
                                if base_name not in list(metric_matches.keys()):
                                    metric_matches[base_name] = {}
                                match_timestamp = matches_in_period[match_type][match_id]['timestamp']
                                metric_matches[base_name][match_timestamp] = matches_in_period[match_type][match_id]
                                metric_matches[base_name][match_timestamp]['match_type'] = match_type
                                metric_matches[base_name][match_timestamp]['match_id'] = match_id
                        except Exception as e:
                            logger.error('error :: cloudburst :: find_cloudbursts :: failed to populate fmetric_matches for %s - %s' % (base_name, e))
            # Sort by end timestamps?
            for cloudburst_ts in list(found_metric_cloudbursts[base_name].keys()):
                cloudburst_ts_range = list(range((cloudburst_ts - resolution), (cloudburst_ts + resolution)))
                try:
                    found_metric_cloudbursts[base_name][cloudburst_ts]['match_id'] = 0
                    found_metric_cloudbursts[base_name][cloudburst_ts]['fp_id'] = 0
                    found_metric_cloudbursts[base_name][cloudburst_ts]['layer_id'] = 0
                    found_metric_cloudbursts[base_name][cloudburst_ts]['anomaly_id'] = 0
                    metric_matches_present = 0
                    try:
                        metric_matches_present = len(list(metric_matches[base_name].keys()))
                    except KeyError:
                        metric_matches_present = 0
                    except Exception as e:
                        logger.error('error :: cloudburst :: find_cloudbursts :: failed to determine if the are matches for %s - %s' % (base_name, e))
                    if metric_matches_present:
                        for matched_ts in list(metric_matches[base_name].keys()):
                            # if matched_ts == cloudburst_ts:
                            if matched_ts in cloudburst_ts_range:
                                match_id = metric_matches[base_name][matched_ts]['match_id']
                                found_metric_cloudbursts[base_name][cloudburst_ts]['match_id'] = match_id
                                match_type = metric_matches[base_name][matched_ts]['match_type']
                                found_metric_cloudbursts[base_name][cloudburst_ts]['fp_id'] = metric_matches[base_name][matched_ts]['fp_id']
                                if match_type == 'layer':
                                    found_metric_cloudbursts[base_name][cloudburst_ts]['layer_id'] = metric_matches[base_name][matched_ts]['layer_id']
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cloudburst :: find_cloudbursts :: failed to populate found_metric_cloudbursts for %s - %s' % (base_name, e))

                anomaly_id = None
                # Use the period_anomalies single query rather than a query for
                # every cloudburst timestamp which index scans the anomalies
                # table and results in 700K mysql.handler_read_next operations,
                # Skyline eating its own performance monitoring dog food.
                # try:
                #     connection = engine.connect()
                #     stmt = select([anomalies_table.c.id]).\
                #         where(anomalies_table.c.anomaly_timestamp == cloudburst_ts).\
                #         where(anomalies_table.c.metric_id == metric_id)
                #     result = connection.execute(stmt)
                #     for row in result:
                #         anomaly_id = int(row['id'])
                #     connection.close()
                cloudburst_ts_range_start = cloudburst_ts - resolution
                cloudburst_ts_range_end = cloudburst_ts + resolution
                cloudburst_ts_range = list(range(cloudburst_ts_range_start, cloudburst_ts_range_end))
                try:
                    if metric_id in list(period_anomalies.keys()):
                        for anomaly_timestamp in list(period_anomalies[metric_id].keys()):
                            if anomaly_timestamp in cloudburst_ts_range:
                                try:
                                    anomaly_id = period_anomalies[metric_id][anomaly_timestamp]['id']
                                    if isinstance(anomaly_id, int):
                                        break
                                except KeyError:
                                    logger.warning('warning :: cloudburst :: find_cloudbursts :: due to KeyError in period_anomalies[%s][%s] could not determine anomaly_id in cloudburst_ts_range (%s, %s) for %s with metric_id %s from period_anomalies: %s' % (
                                        str(metric_id), str(anomaly_timestamp),
                                        str(cloudburst_ts_range_start),
                                        str(cloudburst_ts_range_end), base_name,
                                        str(metric_id),
                                        str(period_anomalies[metric_id])))
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cloudburst :: find_cloudbursts :: due to KeyError in period_anomalies[%s][%s] could not determine anomaly_id in cloudburst_ts_range (%s, %s) for %s with metric_id %s from period_anomalies: %s - %s' % (
                        str(metric_id), str(anomaly_timestamp),
                        str(cloudburst_ts_range_start),
                        str(cloudburst_ts_range_end), base_name,
                        str(metric_id),
                        str(period_anomalies[metric_id]), e))
                if isinstance(anomaly_id, int):
                    found_metric_cloudbursts[base_name][cloudburst_ts]['anomaly_id'] = anomaly_id

                if base_name not in list(found_cloudbursts_to_add.keys()):
                    found_cloudbursts_to_add[base_name] = {}
                # This ensures that the longest cloudburst for the timestamp
                # will be recorded because multiple cloudburst anomalies
                # can be identified for a period but we only want to record
                # one, the last one for each timestamp, for example if this
                # not not done it results in:
                # +----+-----------+------------+------------+----------+----------------+------------+---------------+------------+----------+-------+----------+
                # | id | metric_id | timestamp  | end        | duration | from_timestamp | resolution | full_duration | anomaly_id | match_id | fp_id | layer_id |
                # +----+-----------+------------+------------+----------+----------------+------------+---------------+------------+----------+-------+----------+
                # | 87 |      1099 | 1628600400 | 1628171100 |     2700 |     1628068572 |        900 |        604800 |          0 |        0 |     0 |        0 |
                # | 88 |      1099 | 1628600400 | 1628343900 |     3600 |     1628068572 |        900 |        604800 |          0 |        0 |     0 |        0 |
                # +----+-----------+------------+------------+----------+----------------+------------+---------------+------------+----------+-------+----------+
                # Here a cloudburt was identified at 1628600400
                found_cloudbursts_to_add[base_name][cloudburst_ts] = found_metric_cloudbursts[base_name][cloudburst_ts]

                # BREAK OUT HERE - before inserting into DB - testing
                # continue

        logger.info('cloudburst :: find_cloudbursts :: %s found_cloudbursts_to_add' % (
            str(len(found_cloudbursts_to_add))))

        db_added_at = int(time())

        for base_name in list(found_cloudbursts_to_add.keys()):
            # Sort by newest timestamps first
            # cloudburst_tss = sorted(list(found_cloudbursts_to_add[base_name].keys()), reverse=True)
            cloudburst_tss = sorted(list(found_cloudbursts_to_add[base_name].keys()))
            for cloudburst_ts in cloudburst_tss:
                # if cloudburst_ts > (now_timestamp - custom_check_last):
                if cloudburst_ts < (now_timestamp - long_period_check_last):
                    continue
                ts_added_for = 0
                try:
                    ts_added_for = known_cloudbursts[metric_id][cloudburst_ts]
                except KeyError:
                    ts_added_for = 0
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cloudburst :: find_cloudbursts :: could not determine if cloudburst_ts was added to known_cloudbursts[metric_id][cloudburst_ts] - %s' % e)
                    ts_added_for = 0
                if ts_added_for:
                    logger.info('cloudburst :: find_cloudbursts :: cloudburst at %s for %s is present in known_cloudbursts not adding to the DB' % (
                        str(cloudburst_ts), base_name))
                    continue

                # Do not just check timestamp, check to see if the cloudburst_ts
                # is between timestamp and end
                # So preprocess all the cloudbursts for a metric pre insertion
                # combining cloudburst that are next to each other and those
                # which overlap.  These detections describe the same event only
                # the event when detected in the next 5 minute run could
                # describe the event diferently as it is describing it in a
                # different dataset.  The dataset starts and ends 5 minutes
                # later therefore

                new_cloudburst_id = None
                try:
                    logger.info('cloudburst :: find_cloudbursts :: inserting cloudburst at %s for %s' % (
                        str(cloudburst_ts), base_name))
                    end_ts = found_cloudbursts_to_add[base_name][cloudburst_ts]['end']
                    from_ts = found_cloudbursts_to_add[base_name][cloudburst_ts]['from']
                    duration = found_cloudbursts_to_add[base_name][cloudburst_ts]['duration']
                    resolution = found_cloudbursts_to_add[base_name][cloudburst_ts]['resolution']
                    full_duration = found_cloudbursts_to_add[base_name][cloudburst_ts]['full_duration']
                    match_id = found_cloudbursts_to_add[base_name][cloudburst_ts]['match_id']
                    fp_id = found_cloudbursts_to_add[base_name][cloudburst_ts]['fp_id']
                    layer_id = found_cloudbursts_to_add[base_name][cloudburst_ts]['layer_id']
                    anomaly_id = found_cloudbursts_to_add[base_name][cloudburst_ts]['anomaly_id']
                    connection = engine.connect()
                    ins = cloudburst_table.insert().values(
                        metric_id=metric_id,
                        timestamp=cloudburst_ts, end=end_ts, duration=duration,
                        from_timestamp=from_ts, resolution=resolution,
                        full_duration=full_duration, match_id=match_id,
                        fp_id=fp_id, layer_id=layer_id,
                        anomaly_id=anomaly_id, added_at=db_added_at)
                    result = connection.execute(ins)
                    new_cloudburst_id = result.inserted_primary_key[0]
                    new_cloudburst_ids.append(new_cloudburst_id)
                    connection.close()
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cloudburst :: find_cloudbursts :: could not insert cloudburst record into DB for %s: %s - %s' % (
                        base_name, str(found_cloudbursts_to_add[base_name][cloudburst_ts]), e))
                if new_cloudburst_id:
                    try:
                        self.redis_conn.hset(cloudburst_metrics_key, base_name, str(found_cloudbursts_to_add[base_name][cloudburst_ts]))
                        self.redis_conn.expire(cloudburst_metrics_key, 604800)
                        cloudburst_metrics_key_created = True
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: cloudburst :: find_cloudbursts :: failed to add to %s Redis hash key - %s' % (
                            cloudburst_metrics_key, e))
                    # Add it to the known_cloudbursts dict
                    if metric_id not in list(known_cloudbursts.keys()):
                        known_cloudbursts[metric_id] = {}
                    known_cloudbursts[metric_id][cloudburst_ts] = metric_id

                # Add both short_period and long_period timestamp keys to
                # the luminosity.cloudburst.processed.anomalies Redis hash
                # key so that the timestamps identified in later short_period
                # analysis can be looked up and skipped if already processed
                # rather than sent through to the second stage to improve
                # efficiency.
                processed_timestamps = []
                try:
                    short_period_anomalies = m66_candidate_metrics[base_name][custom_algorithm]['anomalies']
                    processed_timestamps = [int(ts) for ts, value in short_period_anomalies]
                    long_period_anomalies = second_stage_m66_candidate_metrics[base_name][custom_algorithm]['anomalies']
                    long_period_processed_timestamps = [int(ts) for ts, value in long_period_anomalies]
                    processed_timestamps = processed_timestamps + long_period_processed_timestamps
                    processed_timestamps = list(set(processed_timestamps))
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cloudburst :: find_cloudbursts :: failed to determine processed_timestamps for %s - %s' % (
                        base_name, e))

                for processed_timestamp in processed_timestamps:
                    try:
                        key_data = {'cloudburst_id': new_cloudburst_id, 'processed_at': now_timestamp}
                        processed_anomaly_key = '%s.%s' % (base_name, str(int(processed_timestamp)))
                        self.redis_conn_decoded.hset(cloudburst_anomalies_processed_key, processed_anomaly_key, str(key_data))
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: cloudburst :: find_cloudbursts :: failed to add to %s Redis hash key - %s' % (
                            cloudburst_anomalies_processed_key, e))

        if cloudburst_metrics_key_created:
            logger.info('cloudburst :: find_cloudbursts :: created %s Redis hash key' % (
                cloudburst_metrics_key))

        logger.info('cloudburst :: find_cloudbursts :: added %s new cloudbursts to the database' % (
            str(len(new_cloudburst_ids))))

        if engine:
            engine_disposal(engine)

        spin_end = time() - spin_start
        logger.info('cloudburst :: find_cloudbursts :: %s metrics took %.2f seconds' % (
            str(len(assigned_metrics)), spin_end))

        # cloudburst table
        # id, source_metric_id, timestamp, full_duration, resolution, processed
        # cloudbursts table
        # id, cloudburst_id, related_metric_id, ppscore_1, ppscore_2
        # Maybe do not just do ppscore maybe use ruptures to identify metrics
        # that have changespoints in the same window
        return

    def run(self):
        """
        - Called when the process intializes.

        - Determine if Redis is up

        - Spawn a find_cloudbursts process to do analysis

        - Wait for the process to finish.

        - run_every 300 seconds
        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        now = time()
        log_wait_for = now + 5
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time()
            else:
                now = log_wait_for + 1

        logger.info('luminosity/cloudburst :: starting find_cloudbursts')

        if SERVER_METRIC_PATH == '':
            logger.warning('warning :: luminosity/cloudburst :: settings.SERVER_METRICS_NAME is not declared in settings.py, defaults to \'\'')

        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst cannot connect to redis at socket path %s - %s' % (
                    settings.REDIS_SOCKET_PATH, e))
                sleep(10)
                try:
                    self.redis_conn = get_redis_conn(skyline_app)
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                except Exception as e:
                    logger.info(traceback.format_exc())
                    logger.error('error :: cloudburst cannot connect to get_redis_conn - %s' % e)
                continue

            # Report app up
            try:
                self.redis_conn.setex('luminosity.cloudburst', 120, now)
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: could not update the Redis luminosity.cloudburst key - %s' % e)

            # Get all Redis metrics
            unique_metrics = []
            try:
                unique_metrics = list(self.redis_conn_decoded.smembers(full_uniques))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: failed get unique_metrics from %s Redis key - %s' % (
                    full_uniques, e))
                unique_metrics = []

            now_timestamp = int(time())
            key_reference_timestamp = (int(now_timestamp) // run_every * run_every)
            processed_metrics_key = 'luminosity.cloudburst.processed_metrics.%s' % str(key_reference_timestamp)
            cloudburst_info_key = 'luminosity.cloudburst.info.%s' % str(key_reference_timestamp)

            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            for i in range(1, LUMINOSITY_CLOUDBURST_PROCESSES + 1):
                if i > len(unique_metrics):
                    logger.warning('warning :: cloudburst :: skyline is set for more cores than needed.')
                    break
                try:
                    p = Process(target=self.find_cloudbursts, args=(i, unique_metrics))
                    pids.append(p)
                    pid_count += 1
                    logger.info('starting %s of %s find_cloudbursts processes' % (str(pid_count), str(LUMINOSITY_CLOUDBURST_PROCESSES)))
                    p.start()
                    spawned_pids.append(p.pid)
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cloudburst :: failed to spawn find_cloudbursts process - %s' % e)

            # Self monitor processes and terminate if any find_cloudbursts
            # has run for longer than run_every - 10
            p_starts = time()
            while time() - p_starts <= (run_every - 10):
                if any(p.is_alive() for p in pids):
                    # Just to avoid hogging the CPU
                    sleep(.1)
                else:
                    # All the processes are done, break now.
                    time_to_run = time() - p_starts
                    logger.info('cloudburst :: %s find_cloudbursts processes completed in %.2f seconds' % (
                        str(LUMINOSITY_CLOUDBURST_PROCESSES), time_to_run))
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('cloudburst :: timed out, killing find_cloudbursts process')
                for p in pids:
                    logger.info('cloudburst :: killing find_cloudbursts process')
                    p.terminate()
                    logger.info('cloudburst :: killed find_cloudbursts process')

            for p in pids:
                if p.is_alive():
                    try:
                        logger.info('cloudburst :: stopping find_cloudbursts - %s' % (str(p.is_alive())))
                        p.terminate()
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: cloudburst :: failed to stop find_cloudbursts - %s' % e)

            processed_metrics = []
            try:
                processed_metrics = list(self.redis_conn_decoded.smembers(processed_metrics_key))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: failed get %s set from Redis - %s' % (
                    processed_metrics_key, e))
                processed_metrics = []
            if processed_metrics:
                try:
                    self.redis_conn.expire(processed_metrics_key, 600)
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cloudburst :: failed set expire on %s Redis set - %s' % (
                        processed_metrics_key, e))

            processed = 0
            no_data = 0
            too_short = 0
            too_old = 0
            not_analysed = 0
            analysed = 0
            cloudburst_info_dict = {}
            try:
                cloudburst_info_dict = self.redis_conn_decoded.hgetall(cloudburst_info_key)
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: failed get %s set from Redis - %s' % (
                    cloudburst_info_dict, e))
                cloudburst_info_dict = {}
            if cloudburst_info_dict:
                try:
                    self.redis_conn.expire(cloudburst_info_key, 600)
                except Exception as e:
                    logger.error('error :: cloudburst :: failed set expire on %s Redis set - %s' % (
                        cloudburst_info_key, e))
                for key in list(cloudburst_info_dict.keys()):
                    info_dict = {}
                    try:
                        info_dict = literal_eval(cloudburst_info_dict[key])
                    except Exception as e:
                        logger.error('error :: cloudburst :: failed literal_eval cloudburst_info_dict[%s] - %s' % (
                            str(key), e))
                        info_dict = {}
                    if info_dict:
                        processed += info_dict['processed']
                        no_data += info_dict['no_data']
                        too_short += info_dict['too_short']
                        too_old += info_dict['too_old']
                        not_analysed += info_dict['not_analysed']
                        analysed += info_dict['analysed']
            info_data_dict = {
                'processed': processed,
                'analysed': analysed,
                'not_analysed': not_analysed,
                'no_data': no_data,
                'too_short': too_short,
                'too_old': too_old,
            }
            logger.info('cloudburst :: info: %s' % str(info_data_dict))

            try:
                unique_metrics_set = set(list(unique_metrics))
                processed_metrics_set = set(list(processed_metrics))
                if unique_metrics_set == processed_metrics_set:
                    logger.info('cloudburst :: all %s unique_metrics were processed' % str(len(unique_metrics)))
                else:
                    not_processed_metrics_key = 'luminosity.cloudburst.not_processed_metrics.%s' % str(key_reference_timestamp)
                    not_processed_metrics = []
                    set_difference = unique_metrics_set.difference(processed_metrics_set)
                    for metric_name in set_difference:
                        not_processed_metrics.append(metric_name)
                        try:
                            self.redis_conn.sadd(not_processed_metrics_key, metric_name)
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: cloudburst :: failed to add %s to %s Redis set - %s' % (
                                metric_name, not_processed_metrics_key, e))
                    try:
                        self.redis_conn.expire(not_processed_metrics_key, 3600)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: cloudburst :: failed to set expire on %s Redis set - %s' % (
                            not_processed_metrics_key, e))
                    logger.warning('warning :: cloudburst :: there are %s metrics that were not processed of the %s unique_metrics' % (
                        str(len(not_processed_metrics)),
                        str(len(unique_metrics))))
                    del set_difference
                del unique_metrics_set
                del processed_metrics_set
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudburst :: failed to determine whether the unique_metrics_set and processed_metrics_set are different - %s' % e)

            process_runtime = time() - now
            if process_runtime < run_every:
                sleep_for = (run_every - process_runtime)

                process_runtime_now = time() - now
                sleep_for = (run_every - process_runtime_now)

                logger.info('cloudburst :: sleeping for %.2f seconds due to low run time...' % sleep_for)
                sleep(sleep_for)
                try:
                    del sleep_for
                except Exception as e:
                    logger.error('error :: cloudburst :: failed to del sleep_for - %s' % e)
            try:
                del process_runtime
            except Exception as e:
                logger.error('error :: cloudburst :: failed to del process_runtime - %s' % e)
