import os
from os import getpid
import logging
import traceback
from operator import itemgetter
from timeit import default_timer as timer
from time import time
from collections import Counter
from ast import literal_eval

import settings
from skyline_functions import (
    get_redis_conn, get_redis_conn_decoded, get_graphite_metric,
    write_data_to_file)

try:
    from custom_algorithms import run_custom_algorithm_on_timeseries
except:
    run_custom_algorithm_on_timeseries = None

# @added 20210316 - Feature #3978: luminosity - classify_metrics
#                   Feature #3642: Anomaly type classification
try:
    luminosity_data_folder = settings.LUMINOSITY_DATA_FOLDER
except:
    luminosity_data_folder = '/opt/skyline/luminosity'
save_plots_to = '%s/classify_metrics' % luminosity_data_folder

try:
    LUMINOSITY_CLASSIFY_METRICS_LEVEL_SHIFT = settings.LUMINOSITY_CLASSIFY_METRICS_LEVEL_SHIFT
except:
    LUMINOSITY_CLASSIFY_METRICS_LEVEL_SHIFT = False
try:
    LUMINOSITY_CLASSIFY_METRICS_VOLATILITY_SHIFT = settings.LUMINOSITY_CLASSIFY_METRICS_VOLATILITY_SHIFT
except:
    LUMINOSITY_CLASSIFY_METRICS_VOLATILITY_SHIFT = False
try:
    LUMINOSITY_LEVEL_SHIFT_SKIP_NAMESPACES = settings.LUMINOSITY_LEVEL_SHIFT_SKIP_NAMESPACES
except:
    LUMINOSITY_LEVEL_SHIFT_SKIP_NAMESPACES = []

# Database configuration
config = {'user': settings.PANORAMA_DBUSER,
          'password': settings.PANORAMA_DBUSERPASS,
          'host': settings.PANORAMA_DBHOST,
          'port': settings.PANORAMA_DBPORT,
          'database': settings.PANORAMA_DATABASE,
          'raise_on_warnings': True}

skyline_app = 'luminosity'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

redis_conn = get_redis_conn(skyline_app)
redis_conn_decoded = get_redis_conn_decoded(skyline_app)


def classify_metrics(i, start_timestamp, classify_for):

    logger = logging.getLogger(skyline_app_logger)
    debug_algorithms = False
    logger.info('classify_metrics :: with start_timestamp - %s' % str(start_timestamp))
    start_classify_metrics = timer()

    # Handle luminosity running with multiple processes
    def manage_processing_key(current_pid, base_name, classify_for, action):
        result = False
        processing_key = 'luminosity.classify_metrics.processing.%s' % str(base_name)
        if action == 'add':
            key_exists = None
            try:
                key_exists = redis_conn_decoded.get(processing_key)
                if key_exists:
                    result = False
                    return result
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_metrics :: failed to set timestamp for %s in Redis hash key - luminosity.classify_metrics.level_shift' % (
                    base_name))
            try:
                data = {'pid': current_pid, 'timestamp': int(time())}
                redis_conn.setex(processing_key, classify_for, str(data))
                result = True
                logger.info('classify_metrics :: managing %s added %s with %s' % (
                    str(base_name), processing_key, str(data)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_metrics :: failed to create key %s' % (
                    processing_key))
        if action == 'remove':
            try:
                redis_conn.delete(processing_key)
                result = True
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_metrics :: failed to remove key %s' % (
                    processing_key))
        return result

    def check_significant_level_shift(check_timeseries, base_name, anomalies, significant_length, save_plots_to):
        # Determine resolution from the last 30 data points
        # INFO took 0.002060 seconds
        resolution_timestamps = []
        for metric_datapoint in check_timeseries[-30:]:
            timestamp = int(metric_datapoint[0])
            resolution_timestamps.append(timestamp)
        timestamp_resolutions = []
        if resolution_timestamps:
            last_timestamp = None
            for timestamp in resolution_timestamps:
                if last_timestamp:
                    resolution = timestamp - last_timestamp
                    timestamp_resolutions.append(resolution)
                    last_timestamp = timestamp
                else:
                    last_timestamp = timestamp
            try:
                del resolution_timestamps
            except:
                pass
        if timestamp_resolutions:
            try:
                timestamp_resolutions_count = Counter(timestamp_resolutions)
                ordered_timestamp_resolutions_count = timestamp_resolutions_count.most_common()
                metric_resolution = int(ordered_timestamp_resolutions_count[0][0])
            except:
                pass
            try:
                del timestamp_resolutions
            except:
                pass
        continuous_periods = {}
        last_timestamp = None
        for ts, value in anomalies:
            if not last_timestamp:
                continuous_periods[ts] = {}
                continuous_periods[ts]['timestamps'] = [ts]
                last_timestamp = ts
                last_period = ts
                continue
            if (ts - metric_resolution) == last_timestamp:
                continuous_periods_timestamps = continuous_periods[last_period]['timestamps']
                continuous_periods_timestamps.append(ts)
                last_timestamp = ts
                continuous_periods[last_period]['timestamps'] = continuous_periods_timestamps
            else:
                continuous_periods[ts] = {}
                continuous_periods[ts]['timestamps'] = [ts]
                last_timestamp = ts
                last_period = ts
        logger.info('classify_metrics :: %s level_shift_ad anomalies detected spanning %s continuous_periods on %s, result: %s, anomalyScore: %s' % (
            str(len(anomalies)), str(len(continuous_periods)), str(base_name),
            str(result), str(anomalyScore)))

        persist_ad_dict = {}
        try:
            metric_dir = base_name.replace('.', '/')
            timestamp_dir = str(int(check_timeseries[-1][0]))
            PersistAD_data_file = '%s/%s/%s/%s/PersistAD.data.txt' % (
                save_plots_to, custom_algorithm, metric_dir,
                timestamp_dir)
            if os.path.isfile(PersistAD_data_file):
                try:
                    with open(PersistAD_data_file) as f:
                        for line in f:
                            persist_ad_dict = literal_eval(line)
                            if persist_ad_dict:
                                break
                except Exception as e:
                    trace = traceback.format_exc()
                    logger.error(trace)
                    fail_msg = 'error :: get_classify_metrics :: failed to open PersistAD_data_file: %s - %s' % (PersistAD_data_file, e)
                    logger.error('%s' % fail_msg)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to check PersistAD_data_file for %s' % base_name)
        persist_ad_anomalies = []
        if persist_ad_dict:
            persist_ad_anomalies_dict = persist_ad_dict['anomalies']
            persist_ad_anomalies = list(persist_ad_anomalies_dict.keys())

        significant_level_shift = False

        if not persist_ad_anomalies:
            # A metric is deemed to have level shifted significantly if it only
            # does so occassionally and the shift lasts longer than 5 data
            # points
            # To identify metrics that are likely candidates to run the LevelShiftAD
            # algorithm against during realtime analysis, candidates should be
            # fairly stable and not trigger LevelShiftAD a lot.
            if len(continuous_periods) <= 2:
                for timestamp in continuous_periods:
                    length_of_level_shift = len(continuous_periods[timestamp]['timestamps'])
                    logger.debug('debug :: classify_metrics :: %s - continuous_periods[%s] - %s' % (
                        base_name, str(timestamp), str(continuous_periods[timestamp])))
                    logger.debug('debug :: classify_metrics :: %s - length_of_level_shift: %s, significant_length: %s' % (
                        base_name, str(length_of_level_shift), str(significant_length)))
                    if length_of_level_shift >= significant_length:
                        significant_level_shift = True
                        logger.info('classify_metrics :: level shift metric - %s' % base_name)
                        triggered_timestamp = int(timestamp)
                        logger.info('classify_metrics :: level shift metric - %s at %s' % (base_name, str(triggered_timestamp)))
                        if triggered_timestamp:
                            try:
                                redis_conn.hset('luminosity.classify_metrics.level_shift', base_name, triggered_timestamp)
                                break
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: classify_metrics :: failed to set timestamp for %s in Redis hash key - luminosity.classify_metrics.level_shift' % (
                                    base_name))
                if not significant_level_shift:
                    logger.info('classify_metrics :: no significant, sustained level shifts - %s' % base_name)
            else:
                logger.info('classify_metrics :: not a level shift candidate metric - %s' % base_name)

        if persist_ad_anomalies:
            level_shift_anomalies_timestamps = [ts for ts, value in anomalies]
            persist_anomalies_timestamps = list(persist_ad_anomalies)
            shared_timestamps = []
            for timestamp in level_shift_anomalies_timestamps:
                if timestamp in persist_anomalies_timestamps:
                    shared_timestamps.append(timestamp)
            logger.info('classify_metrics :: LevelShiftAD and PersistAD have %s shared anomaly timestamps - %s' % (
                str(len(shared_timestamps)), base_name))
            continuous_periods = {}
            if shared_timestamps:
                last_timestamp = None
                for ts in shared_timestamps:
                    if not last_timestamp:
                        continuous_periods[ts] = {}
                        continuous_periods[ts]['timestamps'] = [ts]
                        last_timestamp = ts
                        last_period = ts
                        continue
                    if (ts - metric_resolution) == last_timestamp:
                        continuous_periods_timestamps = continuous_periods[last_period]['timestamps']
                        continuous_periods_timestamps.append(ts)
                        last_timestamp = ts
                        continuous_periods[last_period]['timestamps'] = continuous_periods_timestamps
                    else:
                        continuous_periods[ts] = {}
                        continuous_periods[ts]['timestamps'] = [ts]
                        last_timestamp = ts
                        last_period = ts

                continuous_periods = {}
                last_timestamp = None
                last_p_timestamp = None
                for ts in level_shift_anomalies_timestamps:
                    if ts in persist_anomalies_timestamps:
                        if last_p_timestamp:
                            if ts < last_p_timestamp:
                                continue
                        continuous_periods[ts] = {}
                        continuous_periods[ts]['timestamps'] = [ts]
                        if not last_p_timestamp:
                            last_p_timestamp = ts
                        last_timestamp = ts
                        for p_ts in persist_anomalies_timestamps:
                            if p_ts < ts:
                                continue
                            if (p_ts - metric_resolution) == last_p_timestamp:
                                continuous_periods_timestamps = continuous_periods[ts]['timestamps']
                                continuous_periods_timestamps.append(p_ts)
                                last_p_timestamp = p_ts
                                continuous_periods[ts]['timestamps'] = continuous_periods_timestamps

            significant_level_shift_timestamps = []
            if continuous_periods:
                for timestamp in continuous_periods:
                    length_of_level_shift = len(continuous_periods[timestamp]['timestamps'])
                    if length_of_level_shift >= significant_length:
                        significant_level_shift_timestamps.append(timestamp)
                if len(significant_level_shift_timestamps) > 0:
                    significant_level_shift = True
                    try:
                        redis_conn.hset('luminosity.classify_metrics.level_shift', base_name, significant_level_shift_timestamps[0])
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: classify_metrics :: failed to set timestamp for %s in Redis hash key - luminosity.classify_metrics.level_shift' % (
                            base_name))

            if not significant_level_shift:
                logger.info('classify_metrics :: no significant, persisted level shifts - %s' % base_name)
            else:
                logger.info('classify_metrics :: %s significant, persisted level shifts detected - %s' % (
                    str(len(significant_level_shift_timestamps)), base_name))

        return significant_level_shift

    # Get all metrics
    unique_base_names = []
    try:
        unique_base_names = list(redis_conn_decoded.smembers('aet.analyzer.unique_base_names'))
    except:
        logger.info(traceback.format_exc())
        logger.error('error :: classify_metrics :: could not get the aet.analyzer.unique_base_names key from Redis')

    # Get last processed timestamps
    last_classified_hash_key = 'luminosity.last.classify_metrics'
    last_classified = []
    try:
        last_classified = redis_conn_decoded.hgetall(last_classified_hash_key)
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: classify_metrics :: failed to get Redis hash key - %s' % last_classified_hash_key)

    classified_metrics = []
    if last_classified:
        for metric in last_classified:
            classified_metrics.append(metric)

    sorted_last_classified = []
    if last_classified:
        try:
            sorted_last_classified = sorted(last_classified.items(), key=itemgetter(1), reverse=False)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: classify_metrics :: failed to sort last_classified by timestamp values')
            sorted_last_classified = []
    logger.info('classify_metrics :: %s metrics sorted by oldest analyzed timestamp first' % (
        str(len(sorted_last_classified))))

    metrics_to_analyse = []

    if not classified_metrics:
        metrics_to_analyse = list(unique_base_names)
        logger.info('classify_metrics :: no classified_metrics so unique_base_names has been added to metrics_to_analyse')

    if unique_base_names and classified_metrics:
        try:
            unique_base_names_set = set(list(unique_base_names))
            classified_metrics_set = set(list(classified_metrics))
            if unique_base_names_set == classified_metrics_set:
                logger.info('classify_metrics :: unique_base_names_set and classified_metrics_set are the same')
            else:
                set_difference = unique_base_names_set.difference(classified_metrics_set)
                for metric in set_difference:
                    metrics_to_analyse.append(metric)
                logger.info('classify_metrics :: there are %s metrics that have not been classified that have been added to metrics_to_analyse' % str(len(metrics_to_analyse)))
                del set_difference
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: classify_metrics :: failed to determine whether the unique_base_names_set and classified_metrics_set are different')

    last_classified_metrics = []
    if sorted_last_classified:
        try:
            last_classified_metrics = [item[0] for item in sorted_last_classified]
            logger.info('classify_metrics :: reordered last_classified_metrics by oldest analyzed timestamp from Redis hash key - %s' % (
                last_classified_hash_key))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to generate list of metric names from sorted_last_classified')

    continue_to_process_metrics = []
    continue_processing_metrics = []
    continue_processing_hash_key = 'luminosity.classify_metric.continue_processing_metrics'
    try:
        continue_to_process_metrics = redis_conn_decoded.hgetall(continue_processing_hash_key)
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: classify_metrics :: failed to get Redis hash key - %s' % continue_processing_hash_key)
    if continue_to_process_metrics:
        try:
            continue_processing_metrics_list = sorted(continue_to_process_metrics.items(), key=itemgetter(1), reverse=False)
            continue_processing_metrics = [item[0] for item in continue_processing_metrics_list]
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: classify_metrics :: failed to sort last_classified by timestamp values')
            continue_processing_metrics = []
    if continue_processing_metrics:
        logger.info('classify_metrics :: there are %s metrics to continue to process' % str(len(continue_processing_metrics)))
        metrics_to_analyse = continue_processing_metrics + metrics_to_analyse

    if last_classified_metrics:
        metrics_to_analyse = metrics_to_analyse + last_classified_metrics
        logger.info('classify_metrics :: there are %s metrics in metrics_to_analyse' % str(len(metrics_to_analyse)))

    # Get mirage resolutions
    mirage_metric_resolutions_dict = {}
    if metrics_to_analyse:
        try:
            mirage_metric_resolutions_dict = redis_conn_decoded.hgetall('mirage.hash_key.metrics_resolutions')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: classify_metrics :: failed to get Redis hash key - mirage.hash_key.metrics_resolutions')

    metrics_to_process_per_run = int(len(unique_base_names) / (14400 / 60))
    logger.info('classify_metrics :: metrics_to_process_per_run - %s' % (
        str(metrics_to_process_per_run)))

    current_pid = getpid()

    # Because this is periodic analysis only analyse data from before the last
    # half hour
    calculate_until = int(start_timestamp) - 1800
    metrics_proceessed = 0
    metrics_classified = 0
    metrics_in_ttl = 0

    classifications_to_do = False

    # use_ttl = False
    use_ttl = True

    check_long_term_for_level_shift = False

    level_shifted_skipped_metrics = 0

    # @added 20210315 - Feature #3978: luminosity - classify_metrics
    # When a new metric is added check the long term timeseries for level shifts
    if LUMINOSITY_CLASSIFY_METRICS_LEVEL_SHIFT:
        check_long_term_for_level_shift = True
        classifications_to_do = True

    if LUMINOSITY_CLASSIFY_METRICS_VOLATILITY_SHIFT:
        classifications_to_do = True

    # @added 20210316 - Feature #3978: luminosity - classify_metrics
    # Exclude metrics already classified
    known_level_shift_metrics = []
    if LUMINOSITY_CLASSIFY_METRICS_LEVEL_SHIFT:
        try:
            known_level_shift_metrics_dict = redis_conn_decoded.hgetall('luminosity.classify_metrics.level_shift')
            if known_level_shift_metrics_dict:
                known_level_shift_metrics = list(known_level_shift_metrics_dict.keys())
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: classify_metrics :: failed to get Redis hash key - luminosity.classify_metrics.level_shift')
        logger.info('classify_metrics :: to exclude - known_level_shift_metrics: %s' % (
            str(len(known_level_shift_metrics))))

    known_volatility_shift_metrics = []
    if LUMINOSITY_CLASSIFY_METRICS_VOLATILITY_SHIFT:
        try:
            known_volatility_shift_metrics_dict = redis_conn_decoded.hgetall('luminosity.classify_metrics.volatility_shift')
            if known_volatility_shift_metrics_dict:
                known_volatility_shift_metrics = list(known_volatility_shift_metrics_dict.keys())
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: classify_metrics :: failed to get Redis hash key - luminosity.classify_metrics.volatility_shift')
        logger.info('classify_metrics :: to exclude - known_volatility_shift_metrics: %s' % (
            str(len(known_volatility_shift_metrics))))

    base_names_processed = []
    for base_name in metrics_to_analyse:
        if not classifications_to_do:
            logger.info('classify_metrics :: no classifications to do, nothing enabled')
            break
        if metrics_proceessed == metrics_to_process_per_run:
            break
        if base_name in base_names_processed:
            continue
        base_names_processed.append(base_name)
        time_now = time()
        runtime = time_now - start_timestamp
        if runtime >= (classify_for - 0.3):
            logger.info('classify_metrics :: stopping before timeout is reached')
            break

        last_analyzed_timestamp = None
        if last_classified:
            try:
                last_analyzed_timestamp = int(last_classified[base_name])
                # logger.debug('debug :: classify_metrics :: last_analyzed_timestamp - %s - %s' % (base_name, str(last_analyzed_timestamp)))
                if use_ttl:
                    if (last_analyzed_timestamp + 14400) > time_now:
                        metrics_in_ttl += 1
                        continue
            except:
                last_analyzed_timestamp = None

        # Handle luminosity running with multiple processes
        manage_metric = False
        try:
            manage_metric = manage_processing_key(current_pid, base_name, classify_for, 'add')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: classify_metrics :: failed to run manage_processing_key')
        if not manage_metric:
            logger.info('classify_metrics :: skipping as processing_key exists for %s' % base_name)
            continue

        if last_analyzed_timestamp:
            processed_seconds_ago = int(time_now) - int(last_analyzed_timestamp)
            logger.info('classify_metrics :: last processed %s seconds ago, processing %s' % (
                str(processed_seconds_ago), str(base_name)))

        use_full_duration = int(settings.FULL_DURATION)
        try:
            metric_second_order_resolution_hours = mirage_metric_resolutions_dict[base_name]
            use_full_duration = int(metric_second_order_resolution_hours) * 3600
        except:
            use_full_duration = int(settings.FULL_DURATION)
        try:
            from_timestamp = int(calculate_until) - int(use_full_duration)
            until_timestamp = int(calculate_until)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: classify_metrics :: calculating from_timestamp and until_timestamp for %s' % base_name)

        skip_level_shift = False

        check_new_metric_for_long_term_for_level_shift = False

        # @added 20210315 -
        # Analyse a 6 month period to determine if any level shifts have occurred
        metric_already_classified = False
        if LUMINOSITY_CLASSIFY_METRICS_LEVEL_SHIFT:

            if LUMINOSITY_LEVEL_SHIFT_SKIP_NAMESPACES:
                for namespace in LUMINOSITY_LEVEL_SHIFT_SKIP_NAMESPACES:
                    if namespace in base_name:
                        skip_level_shift = True
                        last_analyzed_timestamp = time_now
                        level_shifted_skipped_metrics += 1

            checking_long_term_for_level_shift = False
            long_term_processing_key = 'luminosity.long_term_processing.%s' % str(base_name)
            if not last_analyzed_timestamp:
                checking_long_term_for_level_shift = True
                check_new_metric_for_long_term_for_level_shift = True
                try:
                    redis_conn.setex(long_term_processing_key, 600, int(time()))
                    logger.info('classify_metrics :: long_term_processing_key set - %s' % (
                        long_term_processing_key))
                except:
                    pass
            else:
                if last_analyzed_timestamp < (int(calculate_until) - (86400 * 7)):
                    try:
                        metric_being_processed = int(redis_conn_decoded.get(long_term_processing_key))
                        logger.info('classify_metrics :: a long_term_processing_key was found (%s) - last_analyzed_timestamp: %s is older than 2 weeks setting checking_long_term_for_level_shift on %s' % (
                            str(metric_being_processed), str(last_analyzed_timestamp), str(base_name)))
                        checking_long_term_for_level_shift = True
                    except:
                        pass

            if base_name in known_level_shift_metrics and checking_long_term_for_level_shift:
                logger.info('classify_metrics :: not processing known level shift metric - %s' % (
                    str(base_name)))
                checking_long_term_for_level_shift = False

            if skip_level_shift:
                checking_long_term_for_level_shift = False

            if base_name in continue_processing_metrics:
                logger.info('classify_metrics :: continuing long term processing from %s because in continue_processing_metrics - %s' % (
                    str(last_analyzed_timestamp), str(base_name)))
                checking_long_term_for_level_shift = True

            long_term_timeseries = []
            if check_long_term_for_level_shift and checking_long_term_for_level_shift:
                try:
                    long_term_calculate_until = calculate_until - (86400 * 7)
                    long_term_period = int(86400 * 90)
                    if base_name in continue_processing_metrics and last_analyzed_timestamp:
                        long_term_period = int(last_analyzed_timestamp)
                    long_term_from_timestamp = int(long_term_calculate_until) - long_term_period
                    # Continue from where it left off
                    if last_analyzed_timestamp:
                        if last_analyzed_timestamp > long_term_from_timestamp:
                            long_term_from_timestamp = last_analyzed_timestamp + 1
                            logger.info('classify_metrics :: check_long_term_for_level_shift - long_term_from_timestamp set to last_analyzed_timestamp value + 1 for %s' % (
                                base_name))
                    long_term_until_timestamp = int(long_term_calculate_until)
                    logger.info('classify_metrics :: check_long_term_for_level_shift - calculated long_term_from_timestamp: %s, long_term_until_timestamp: %s for %s' % (
                        str(long_term_from_timestamp), str(long_term_until_timestamp),
                        base_name))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: classify_metrics :: check_long_term_for_level_shift - calculating long_term_from_timestamp and until_timestamp for %s' % base_name)

                time_now = time()
                runtime = time_now - start_timestamp
                if runtime >= (classify_for - 1):
                    logger.info('classify_metrics :: stopping before timeout is reached')
                    # Handle luminosity running with multiple processes
                    manage_metric = False
                    try:
                        manage_metric = manage_processing_key(current_pid, base_name, classify_for, 'remove')
                        if manage_metric:
                            logger.info('classify_metrics :: removed manage_processing_key - %s' % base_name)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: classify_metrics :: failed to remove manage_processing_key')
                    break

                try:
                    long_term_timeseries = get_graphite_metric(
                        skyline_app, str(base_name), long_term_from_timestamp,
                        long_term_until_timestamp, 'list',
                        'object')
                    if len(long_term_timeseries) > 0:
                        if long_term_timeseries[0][0] < (time_now - (86400 * 7)):
                            checking_long_term_for_level_shift = True
                            logger.info('classify_metrics :: checking_long_term_for_level_shift - %s datapoints retrieved for processing %s' % (
                                str(len(long_term_timeseries)), str(base_name)))
                    else:
                        logger.info('classify_metrics :: checking_long_term_for_level_shift - %s datapoints retrieved for processing %s - nothing to do setting %s' % (
                            str(len(long_term_timeseries)), str(base_name),
                            last_classified_hash_key))
                        try:
                            redis_conn.hset(last_classified_hash_key, base_name, int(long_term_until_timestamp))
                            logger.info('classify_metrics :: checking_long_term_for_level_shift - %s set timestamp in %s to long_term_until_timestamp - %s' % (
                                str(base_name), str(last_classified_hash_key),
                                str(long_term_until_timestamp)))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: classify_metrics :: checking_long_term_for_level_shift - failed to set timestamp for %s in Redis hash key - %s' % (
                                base_name, last_classified_hash_key))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: classify_metrics :: checking_long_term_for_level_shift - getting Graphite data for %s with long_term_from_timestamp: %s, long_term_until_timestamp: %s' % (
                        base_name, str(long_term_from_timestamp), str(long_term_until_timestamp)))

            # level_shift_ad
            custom_algorithm = 'adtk_level_shift'
            custom_algorithm_dict = {}

            custom_algorithm_dict['debug_logging'] = False
            debug_algorithm_logging = False

            if debug_algorithms:
                custom_algorithm_dict['debug_logging'] = True
                debug_algorithm_logging = True

            custom_algorithm_dict['debug_logging'] = True
            debug_algorithm_logging = True

            custom_algorithm_dict['algorithm_source'] = '/opt/skyline/github/skyline/skyline/custom_algorithms/adtk_level_shift.py'
            custom_algorithm_dict['algorithm_parameters'] = {
                'window': 10, 'c': 9.9, 'return_anomalies': True,
                'realtime_analysis': False,
                'save_plots_to': save_plots_to,
                'debug_logging': debug_algorithm_logging,
                'run_PersistAD': True,
                'persist_ad_algorithm_parameters': {'c': 9.9, 'window': 60}
            }
            custom_algorithm_dict['max_execution_time'] = 5.0

            result = None
            anomalyScore = None
            anomalies = None
            timeseries_period_end = None

            if checking_long_term_for_level_shift and len(long_term_timeseries) > 0:
                timeseries_start_timestamp = long_term_timeseries[0][0]
                timeseries_end_timestamp = long_term_timeseries[-1][0]
                timeseries_period_start = timeseries_start_timestamp
                timeseries_period_end = timeseries_start_timestamp + use_full_duration
                periods_processed = 0
                periods_to_process = int((timeseries_end_timestamp - timeseries_start_timestamp) / (86400 * 7))
                if not periods_to_process:
                    logger.error('error :: classify_metrics :: checking_long_term_for_level_shift - no periods_to_process: %s' % str(periods_to_process))
                else:
                    logger.info('classify_metrics :: checking_long_term_for_level_shift - periods_to_process: %s - %s' % (
                        str(periods_to_process), base_name))

                significant_level_shifts_detected = 0

                time_now = int(time())
                runtime = time_now - start_timestamp
                if runtime >= (classify_for - 1):
                    logger.info('classify_metrics :: stopping before timeout is reached')
                    # Handle luminosity running with multiple processes
                    manage_metric = False
                    try:
                        manage_metric = manage_processing_key(current_pid, base_name, classify_for, 'remove')
                        if manage_metric:
                            logger.info('classify_metrics :: removed manage_processing_key - %s' % base_name)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: classify_metrics :: failed to remove manage_processing_key')
                    break

                start_long_term_timer = timer()
                while timeseries_period_end < timeseries_end_timestamp:
                    periods_processed += 1
                    working_timeseries = [item for item in long_term_timeseries if item[0] >= timeseries_period_start and item[0] <= timeseries_period_end]
                    if not working_timeseries:
                        timeseries_period_start = timeseries_period_end + 1
                        timeseries_period_end = timeseries_period_end + use_full_duration
                        continue

                    time_now = int(time())
                    runtime = time_now - start_timestamp
                    if runtime >= (classify_for - 1):
                        logger.info('classify_metrics :: stopping before timeout is reached')
                        # Handle luminosity running with multiple processes
                        manage_metric = False
                        try:
                            manage_metric = manage_processing_key(current_pid, base_name, classify_for, 'remove')
                            if manage_metric:
                                logger.info('classify_metrics :: removed manage_processing_key - %s' % base_name)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: classify_metrics :: failed to remove manage_processing_key')
                        break

                    # Add to continue_processing_hash_key
                    last_working_timestamp = int(working_timeseries[-1][0])
                    try:
                        redis_conn.hset(continue_processing_hash_key, base_name, last_working_timestamp)
                        logger.info('classify_metrics :: checking_long_term_for_level_shift - adding %s to %s with %s' % (
                            str(base_name), continue_processing_hash_key,
                            str(last_working_timestamp)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: classify_metrics :: failed to get Redis hash key - %s' % continue_processing_hash_key)

                    result = None
                    anomalyScore = None
                    anomalies = None
                    try:
                        result, anomalyScore, anomalies = run_custom_algorithm_on_timeseries(skyline_app, current_pid, base_name, working_timeseries, custom_algorithm, custom_algorithm_dict, debug_algorithms)
                        if debug_algorithms:
                            logger.debug('debug :: classify_metrics :: checking_long_term_for_level_shift - run_custom_algorithm_on_timeseries run %s with result - %s, anomalyScore - %s' % (
                                custom_algorithm, str(result), str(anomalyScore)))
                    except:
                        if debug_algorithms:
                            logger.error(traceback.format_exc())
                            logger.error('error :: algorithms :: checking_long_term_for_level_shift - failed to run custom_algorithm %s on %s' % (
                                custom_algorithm, base_name))
                    if not anomalies:
                        logger.info('classify_metrics :: checking_long_term_for_level_shift - %s level_shift_ad anomalies were detected on %s, result: %s, anomalyScore: %s' % (
                            str(anomalies), str(base_name), str(result), str(anomalyScore)))
                    timeseries_period_start = working_timeseries[-1][0] + 1
                    timeseries_period_end = working_timeseries[-1][0] + use_full_duration
                    if anomalies:
                        significant_level_shift = check_significant_level_shift(working_timeseries, base_name, anomalies, 3, save_plots_to)
                        if significant_level_shift:
                            significant_level_shifts_detected += 1
                            if not metric_already_classified:
                                metrics_classified += 1
                                metric_already_classified = True
                            logger.info('classify_metrics :: checking_long_term_for_level_shift - %s significant level shift detected' % (
                                str(base_name)))
                            try:
                                metric_dir = base_name.replace('.', '/')
                                timestamp_dir = str(last_working_timestamp)
                                significant_file = '%s/%s/%s/%s/significant.txt' % (
                                    save_plots_to, custom_algorithm, metric_dir,
                                    timestamp_dir)
                                write_data_to_file(skyline_app, significant_file, 'w', str(True))
                                logger.info('created significant file - %s' % significant_file)
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to save significant file')
                        logger.info('classify_metrics :: checking_long_term_for_level_shift - %s level_shift_ad anomalies were detected on %s, result: %s, anomalyScore: %s, significant: %s' % (
                            str(len(anomalies)), str(base_name), str(result), str(anomalyScore),
                            str(significant_level_shift)))
                    try:
                        redis_conn.hset(last_classified_hash_key, base_name, last_working_timestamp)
                        logger.info('classify_metrics :: checking_long_term_for_level_shift - %s set timestamp in %s to %s for period %s' % (
                            str(base_name), str(last_classified_hash_key),
                            str(last_working_timestamp), str(periods_processed)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: classify_metrics :: checking_long_term_for_level_shift - failed to set timestamp for %s in Redis hash key - %s' % (
                            base_name, last_classified_hash_key))
                    logger.info('classify_metrics :: checking_long_term_for_level_shift - processed %s of %s periods to process - %s' % (
                        str(periods_processed), str(periods_to_process), base_name))

                end_long_term_timer = timer()

                if periods_processed == periods_to_process:
                    try:
                        redis_conn.hdel(continue_processing_hash_key, base_name)
                        logger.info('classify_metrics :: checking_long_term_for_level_shift - removing %s from %s processed all %s of %s long term periods' % (
                            str(base_name), continue_processing_hash_key,
                            str(periods_processed), str(periods_to_process)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: classify_metrics :: failed to remove %s from Redis hash key - %s' % (
                            base_name, continue_processing_hash_key))
                    logger.info('classify_metrics :: checking_long_term_for_level_shift - COMPLETE - periods_processed: %s, for %s with %s significant level shifts detected in %.6f seconds' % (
                        str(periods_processed), base_name, str(significant_level_shifts_detected),
                        (end_long_term_timer - start_long_term_timer)))
                else:
                    logger.info('classify_metrics :: checking_long_term_for_level_shift - completed processing %s of %s periods_to_process, for %s with %s significant level shifts detected in %.6f seconds' % (
                        str(periods_processed), str(periods_to_process),
                        base_name, str(significant_level_shifts_detected),
                        (end_long_term_timer - start_long_term_timer)))

        time_now = time()
        runtime = time_now - start_timestamp
        if runtime >= (classify_for - 1):
            logger.info('classify_metrics :: stopping before timeout is reached')
            # Handle luminosity running with multiple processes
            manage_metric = False
            try:
                manage_metric = manage_processing_key(current_pid, base_name, classify_for, 'remove')
                if manage_metric:
                    logger.info('classify_metrics :: removed manage_processing_key - %s' % base_name)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_metrics :: failed to remove manage_processing_key')
            break

        get_timeseries_for_metric = False
        if base_name not in known_level_shift_metrics:
            get_timeseries_for_metric = True
        else:
            logger.info('classify_metrics :: known level shift metric - %s' % (
                str(base_name)))

        if LUMINOSITY_CLASSIFY_METRICS_LEVEL_SHIFT and skip_level_shift:
            get_timeseries_for_metric = False

        if LUMINOSITY_CLASSIFY_METRICS_VOLATILITY_SHIFT:
            if base_name not in known_volatility_shift_metrics:
                get_timeseries_for_metric = True
            else:
                logger.info('classify_metrics :: known volatility shift metric - %s' % (
                    str(base_name)))

        if not get_timeseries_for_metric:
            try:
                redis_conn.hset(last_classified_hash_key, base_name, int(time_now))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_metrics :: failed to set timestamp for %s in Redis hash key - %s' % (
                    base_name, last_classified_hash_key))
            # Handle luminosity running with multiple processes
            manage_metric = False
            try:
                manage_metric = manage_processing_key(current_pid, base_name, classify_for, 'remove')
                if manage_metric:
                    logger.info('classify_metrics :: removed manage_processing_key - %s' % base_name)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_metrics :: failed to remove manage_processing_key')
            continue

        time_now = time()
        runtime = time_now - start_timestamp
        if runtime >= (classify_for - 1):
            logger.info('classify_metrics :: stopping before timeout is reached')
            # Handle luminosity running with multiple processes
            manage_metric = False
            try:
                manage_metric = manage_processing_key(current_pid, base_name, classify_for, 'remove')
                if manage_metric:
                    logger.info('classify_metrics :: removed manage_processing_key - %s' % base_name)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_metrics :: failed to remove manage_processing_key')
            break

        timeseries = []
        if get_timeseries_for_metric:
            try:
                timeseries = get_graphite_metric(
                    skyline_app, base_name, from_timestamp, until_timestamp, 'list',
                    'object')
                logger.info('classify_metrics :: timeseries retieved for - %s' % base_name)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_metrics :: getting Graphite data for %s with use_full_duration: %s, from_timestamp: %s, until_timestamp: %s' % (
                    base_name, str(use_full_duration), str(from_timestamp), str(until_timestamp)))
        else:
            if debug_algorithms:
                logger.debug('classify_metrics :: not processing, known - %s' % (
                    str(base_name)))

        time_now = time()
        runtime = time_now - start_timestamp
        if runtime >= (classify_for - 1):
            logger.info('classify_metrics :: stopping before timeout is reached')
            # Handle luminosity running with multiple processes
            manage_metric = False
            try:
                manage_metric = manage_processing_key(current_pid, base_name, classify_for, 'remove')
                if manage_metric:
                    logger.info('classify_metrics :: removed manage_processing_key - %s' % base_name)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_metrics :: failed to remove manage_processing_key')
            break

        if not timeseries:
            try:
                redis_conn.hset(last_classified_hash_key, str(base_name), int(time_now))
                if not skip_level_shift:
                    logger.info('classify_metrics :: no timeseries data for %s, set timestamp to now %s in %s' % (
                        str(base_name), str(int(time_now)), str(last_classified_hash_key)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_metrics :: failed to set timestamp for %s in Redis hash key - %s' % (
                    base_name, last_classified_hash_key))
            # Handle luminosity running with multiple processes
            manage_metric = False
            try:
                manage_metric = manage_processing_key(current_pid, base_name, classify_for, 'remove')
                if manage_metric:
                    logger.info('classify_metrics :: removed manage_processing_key - %s' % base_name)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_metrics :: failed to remove manage_processing_key')
            continue
        logger.info('classify_metrics :: timeseries has %s datapoints for processing %s' % (
            str(len(timeseries)), str(base_name)))

        time_now = int(time())
        runtime = time_now - start_timestamp
        if runtime >= (classify_for - 1):
            logger.info('classify_metrics :: stopping before timeout is reached')
            # Handle luminosity running with multiple processes
            manage_metric = False
            try:
                manage_metric = manage_processing_key(current_pid, base_name, classify_for, 'remove')
                if manage_metric:
                    logger.info('classify_metrics :: removed manage_processing_key - %s' % base_name)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_metrics :: failed to remove manage_processing_key')
            break

        if LUMINOSITY_CLASSIFY_METRICS_LEVEL_SHIFT:
            if skip_level_shift:
                # Handle luminosity running with multiple processes
                manage_metric = False
                try:
                    manage_metric = manage_processing_key(current_pid, base_name, classify_for, 'remove')
                    if manage_metric:
                        logger.info('classify_metrics :: removed manage_processing_key - %s' % base_name)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: classify_metrics :: failed to remove manage_processing_key')
                continue
            try:
                result, anomalyScore, anomalies = run_custom_algorithm_on_timeseries(skyline_app, current_pid, base_name, timeseries, custom_algorithm, custom_algorithm_dict, debug_algorithms)
                if debug_algorithms:
                    logger.debug('debug :: classify_metrics :: run_custom_algorithm_on_timeseries run %s with result - %s, anomalyScore - %s' % (
                        custom_algorithm, str(result), str(anomalyScore)))
            except:
                if debug_algorithms:
                    logger.error(traceback.format_exc())
                    logger.error('error :: classify_metrics :: failed to run custom_algorithm %s on %s' % (
                        custom_algorithm, base_name))
            if not anomalies:
                logger.info('classify_metrics :: %s level_shift_ad anomalies were detected on %s, result: %s, anomalyScore: %s' % (
                    str(anomalies), str(base_name), str(result), str(anomalyScore)))

            try:
                redis_conn.hset(last_classified_hash_key, base_name, int(until_timestamp))
                logger.info('classify_metrics :: timeseries processed for %s, set timestamp to now %s in %s' % (
                    str(base_name), str(int(until_timestamp)), str(last_classified_hash_key)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_metrics :: failed to set timestamp for %s in Redis hash key - %s' % (
                    base_name, last_classified_hash_key))

            time_now = time()
            runtime = time_now - start_timestamp
            if runtime >= (classify_for - 1):
                logger.info('classify_metrics :: stopping before timeout is reached')
                # Handle luminosity running with multiple processes
                manage_metric = False
                try:
                    manage_metric = manage_processing_key(current_pid, base_name, classify_for, 'remove')
                    if manage_metric:
                        logger.info('classify_metrics :: removed manage_processing_key - %s' % base_name)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: classify_metrics :: failed to remove manage_processing_key')
                break

            metric_resolution = False
            if anomalies:
                significant_level_shift = check_significant_level_shift(timeseries, base_name, anomalies, custom_algorithm_dict['algorithm_parameters']['window'], save_plots_to)
                if significant_level_shift:
                    if not metric_already_classified:
                        metrics_classified += 1
                        metric_already_classified = True
                    try:
                        metric_dir = base_name.replace('.', '/')
                        timestamp_dir = str(int(timeseries[-1][0]))
                        significant_file = '%s/%s/%s/%s/significant.txt' % (
                            save_plots_to, custom_algorithm, metric_dir,
                            timestamp_dir)
                        write_data_to_file(skyline_app, significant_file, 'w', str(True))
                        logger.info('classify_metrics :: created significant file - %s' % significant_file)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: classify_metrics :: failed to save significant file')

        time_now = time()
        runtime = time_now - start_timestamp
        if runtime >= (classify_for - 1):
            logger.info('classify_metrics :: stopping before timeout is reached')
            # Handle luminosity running with multiple processes
            manage_metric = False
            try:
                manage_metric = manage_processing_key(current_pid, base_name, classify_for, 'remove')
                if manage_metric:
                    logger.info('classify_metrics :: removed manage_processing_key - %s' % base_name)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_metrics :: failed to remove manage_processing_key')
            break

        # volatility_shift_ad
        if LUMINOSITY_CLASSIFY_METRICS_VOLATILITY_SHIFT:
            custom_algorithm = 'adtk_volatility_shift'
            custom_algorithm_dict = {}

            custom_algorithm_dict['debug_logging'] = False
            debug_algorithm_logging = False

            if debug_algorithms:
                custom_algorithm_dict['debug_logging'] = True
                debug_algorithm_logging = True

            custom_algorithm_dict['debug_logging'] = True
            debug_algorithm_logging = True

            custom_algorithm_dict['algorithm_source'] = '/opt/skyline/github/skyline/skyline/custom_algorithms/adtk_volatility_shift.py'
            custom_algorithm_dict['algorithm_parameters'] = {'window': 5, 'c': 9.9, 'return_anomalies': True, 'realtime_analysis': False, 'debug_logging': debug_algorithm_logging}
            custom_algorithm_dict['max_execution_time'] = 0.5

            result = None
            anomalyScore = None
            anomalies = None
            try:
                result, anomalyScore, anomalies = run_custom_algorithm_on_timeseries(skyline_app, current_pid, base_name, timeseries, custom_algorithm, custom_algorithm_dict, debug_algorithms)
                if debug_algorithms:
                    logger.debug('debug :: classify_metrics :: run_custom_algorithm_on_timeseries run %s with result - %s, anomalyScore - %s' % (
                        custom_algorithm, str(result), str(anomalyScore)))
            except:
                if debug_algorithms:
                    logger.error(traceback.format_exc())
                    logger.error('error :: algorithms :: failed to run custom_algorithm %s on %s' % (
                        custom_algorithm, base_name))
            if not anomalies:
                logger.info('classify_metrics :: %s volatility_shift_ad anomalies were detected on %s, result: %s, anomalyScore: %s' % (
                    str(anomalies), str(base_name), str(result), str(anomalyScore)))

            if anomalies:
                # Determine resolution from the last 30 data points
                # INFO took 0.002060 seconds
                if not metric_resolution:
                    resolution_timestamps = []
                    metric_resolution = False
                    for metric_datapoint in timeseries[-30:]:
                        timestamp = int(metric_datapoint[0])
                        resolution_timestamps.append(timestamp)
                    timestamp_resolutions = []
                    if resolution_timestamps:
                        last_timestamp = None
                        for timestamp in resolution_timestamps:
                            if last_timestamp:
                                resolution = timestamp - last_timestamp
                                timestamp_resolutions.append(resolution)
                                last_timestamp = timestamp
                            else:
                                last_timestamp = timestamp
                        try:
                            del resolution_timestamps
                        except:
                            pass
                    if timestamp_resolutions:
                        try:
                            timestamp_resolutions_count = Counter(timestamp_resolutions)
                            ordered_timestamp_resolutions_count = timestamp_resolutions_count.most_common()
                            metric_resolution = int(ordered_timestamp_resolutions_count[0][0])
                        except:
                            pass
                        try:
                            del timestamp_resolutions
                        except:
                            pass

                continuous_periods = {}
                last_timestamp = None
                for ts, value in anomalies:
                    if not last_timestamp:
                        continuous_periods[ts] = {}
                        continuous_periods[ts]['timestamps'] = [ts]
                        last_timestamp = ts
                        last_period = ts
                        continue
                    if (ts - metric_resolution) == last_timestamp:
                        continuous_periods_timestamps = continuous_periods[last_period]['timestamps']
                        continuous_periods_timestamps.append(ts)
                        last_timestamp = ts
                        continuous_periods[last_period]['timestamps'] = continuous_periods_timestamps
                    else:
                        continuous_periods[ts] = {}
                        continuous_periods[ts]['timestamps'] = [ts]
                        last_timestamp = ts
                        last_period = ts
                logger.info('classify_metrics :: %s volatility_shift_ad anomalies detected spanning %s continuous_periods on %s, result: %s, anomalyScore: %s' % (
                    str(len(anomalies)), str(len(continuous_periods)), str(base_name),
                    str(result), str(anomalyScore)))

                try:
                    int_metric_timestamp = int(timeseries[-1][0])
                except:
                    int_metric_timestamp = 0
                if len(continuous_periods) <= 2:
                    logger.info('classify_metrics :: volatility shift metric - %s' % base_name)
                    triggered_timestamp = None
                    for timestamp in continuous_periods:
                        triggered_timestamp = int(timestamp)
                    logger.info('classify_metrics :: volatility shift metric - %s at %s' % (base_name, str(triggered_timestamp)))
                    if triggered_timestamp:
                        try:
                            redis_conn.hset('luminosity.classify_metrics.volatility_shift', base_name, triggered_timestamp)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: classify_metrics :: failed to set timestamp for %s in Redis hash key - luminosity.classify_metrics.volatility_shift' % (
                                base_name))
                        if not metric_already_classified:
                            metrics_classified += 1
                            metric_already_classified = True
                else:
                    logger.info('classify_metrics :: NOT volatility shift metric - %s' % base_name)

        try:
            int_metric_timestamp = int(timeseries[-1][0])
        except:
            int_metric_timestamp = int(time_now)
        if last_analyzed_timestamp:
            if int_metric_timestamp == last_analyzed_timestamp:
                int_metric_timestamp = int(time_now)
        if last_analyzed_timestamp:
            if last_analyzed_timestamp > int_metric_timestamp:
                int_metric_timestamp = int(time_now)

        if check_new_metric_for_long_term_for_level_shift and timeseries:
            if int_metric_timestamp < (time_now - 3600):
                int_metric_timestamp = int(time_now)
        if check_long_term_for_level_shift and checking_long_term_for_level_shift and get_timeseries_for_metric:
            if int_metric_timestamp < (time_now - 3600):
                int_metric_timestamp = int(time_now)

        # update_hash_key = False
        # if int_metric_timestamp and not checked_long_term_for_level_shift:
        #    update_hash_key = True
        # if update_hash_key:
        if int_metric_timestamp:
            try:
                redis_conn.hset(last_classified_hash_key, base_name, int_metric_timestamp)
                logger.info('classify_metrics :: analysis of complete, set timestamp to %s in %s for %s' % (
                    str(int_metric_timestamp), str(last_classified_hash_key),
                    str(base_name)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_metrics :: failed to set timestamp for %s in Redis hash key - %s' % (
                    base_name, last_classified_hash_key))
        metrics_proceessed += 1
        # Handle luminosity running with multiple processes
        manage_metric = False
        try:
            manage_metric = manage_processing_key(current_pid, base_name, classify_for, 'remove')
            if manage_metric:
                logger.info('classify_metrics :: removed manage_processing_key - %s' % base_name)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: classify_metrics :: failed to remove manage_processing_key')

    try:
        data = [int(start_timestamp), metrics_proceessed]
        redis_conn.sadd('luminosity.classify_metrics.proceessed', str(data))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: classify_metrics :: failed to entry to Redis set luminosity.classify_metrics.proceessed')
    try:
        data = [int(start_timestamp), metrics_classified]
        redis_conn.sadd('luminosity.classify_metrics.classified', str(data))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: classify_metrics :: failed to entry to Redis set luminosity.classify_metrics.classified')

    end_classify_metrics = timer()

    logger.info('classify_metrics :: %s metrics were classified' % str(metrics_classified))
    if metrics_in_ttl:
        logger.info('classify_metrics :: %s metrics are still in TTL' % str(metrics_in_ttl))

    if LUMINOSITY_LEVEL_SHIFT_SKIP_NAMESPACES:
        logger.info('classify_metrics :: %s metrics were skipped as in LUMINOSITY_LEVEL_SHIFT_SKIP_NAMESPACES' % str(level_shifted_skipped_metrics))

    logger.info('classify_metrics :: %s metrics were processed to classify of the total of %s metrics in the allowed time of %s seconds, took %.6f seconds' % (
        str(metrics_proceessed), str(len(metrics_to_analyse)), str(classify_for),
        (end_classify_metrics - start_classify_metrics)))

    return
