import logging
from redis import StrictRedis
from msgpack import Unpacker
import traceback
from math import ceil
from luminol.anomaly_detector import AnomalyDetector
from luminol.correlator import Correlator
from timeit import default_timer as timer
import settings
from skyline_functions import (mysql_select, is_derivative_metric,
                               nonNegativeDerivative)

# Database configuration
config = {'user': settings.PANORAMA_DBUSER,
          'password': settings.PANORAMA_DBUSERPASS,
          'host': settings.PANORAMA_DBHOST,
          'port': settings.PANORAMA_DBPORT,
          'database': settings.PANORAMA_DATABASE,
          'raise_on_warnings': True}

# @added 20180423 - Feature #2360: CORRELATE_ALERTS_ONLY
#                   Branch #2270: luminosity
# Only correlate metrics with an alert setting
try:
    correlate_alerts_only = settings.CORRELATE_ALERTS_ONLY
except:
    correlate_alerts_only = True

skyline_app = 'luminosity'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

# @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
if settings.REDIS_PASSWORD:
    redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
else:
    redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)


def get_anomaly(request_type):
    """
    Query the database for the anomaly details
    """

    logger = logging.getLogger(skyline_app_logger)

    if isinstance(request_type, int):
        latest = False
    else:
        latest = True

    if latest:
        query = 'SELECT * FROM anomalies ORDER BY id DESC LIMIT 1'
    else:
        query = 'SELECT * FROM anomalies WHERE id=%s' % str(request_type)

    try:
        results = mysql_select(skyline_app, query)
    except:
        logger.error(traceback.format_exc())
        logger.error('MySQL error')
        return (False, False, False, False)

    try:
        anomaly_id = int(results[0][0])
        metric_id = int(results[0][1])
        anomaly_timestamp = int(results[0][5])
        query = 'SELECT metric FROM metrics WHERE id=%s' % str(metric_id)
        results = mysql_select(skyline_app, query)
        base_name = str(results[0][0])
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: MySQL error - %s' % query)
        return (False, False, False, False)

    return (anomaly_id, metric_id, anomaly_timestamp, base_name)


def get_anomalous_ts(base_name, anomaly_timestamp):

    logger = logging.getLogger(skyline_app_logger)

    # @added 20180423 - Feature #2360: CORRELATE_ALERTS_ONLY
    #                   Branch #2270: luminosity
    # Only correlate metrics with an alert setting
    if correlate_alerts_only:
        try:
            smtp_alerter_metrics = list(redis_conn.smembers('analyzer.smtp_alerter_metrics'))
        except:
            smtp_alerter_metrics = []
        if base_name not in smtp_alerter_metrics:
            logger.error('%s has no alerter setting, not correlating' % base_name)
            return False

    if not base_name or not anomaly_timestamp:
        return False

    # from skyline_functions import nonNegativeDerivative
    anomalous_metric = '%s%s' % (settings.FULL_NAMESPACE, base_name)
    assigned_metrics = [anomalous_metric]
    # @modified 20180419 -
    raw_assigned = []
    try:
        raw_assigned = redis_conn.mget(assigned_metrics)
    except:
        raw_assigned = []
    if raw_assigned == [None]:
        logger.info('%s data not retrieved from local Redis' % (str(base_name)))
        raw_assigned = []

    if not raw_assigned and settings.OTHER_SKYLINE_REDIS_INSTANCES:
        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        # for redis_ip, redis_port in settings.OTHER_SKYLINE_REDIS_INSTANCES:
        for redis_ip, redis_port, redis_password in settings.OTHER_SKYLINE_REDIS_INSTANCES:
            if not raw_assigned:
                try:
                    if redis_password:
                        other_redis_conn = StrictRedis(host=str(redis_ip), port=int(redis_port), password=str(redis_password))
                    else:
                        other_redis_conn = StrictRedis(host=str(redis_ip), port=int(redis_port))
                    raw_assigned = other_redis_conn.mget(assigned_metrics)
                    if raw_assigned == [None]:
                        logger.info('%s data not retrieved from Redis at %s on port %s' % (str(base_name), str(redis_ip), str(redis_port)))
                        raw_assigned = []
                    if raw_assigned:
                        logger.info('%s data retrieved from Redis at %s on port %s' % (str(base_name), str(redis_ip), str(redis_port)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to connect to Redis at %s on port %s' % (str(redis_ip), str(redis_port)))
                    raw_assigned = []

    if not raw_assigned or raw_assigned == [None]:
        logger.info('%s data not retrieved' % (str(base_name)))
        return False

    for i, metric_name in enumerate(assigned_metrics):
        try:
            raw_series = raw_assigned[i]
            unpacker = Unpacker(use_list=False)
            unpacker.feed(raw_series)
            timeseries = list(unpacker)
        except:
            timeseries = []

    # Convert the time series if this is a known_derivative_metric
    known_derivative_metric = is_derivative_metric(skyline_app, base_name)
    if known_derivative_metric:
        derivative_timeseries = nonNegativeDerivative(timeseries)
        timeseries = derivative_timeseries

    # Sample the time series
    from_timestamp = anomaly_timestamp - 600
    anomaly_ts = []
    for ts, value in timeseries:
        if int(ts) < from_timestamp:
            continue
        if int(ts) <= anomaly_timestamp:
            anomaly_ts.append((int(ts), value))
        if int(ts) > anomaly_timestamp:
            break
    return anomaly_ts


def get_anoms(anomalous_ts):

    logger = logging.getLogger(skyline_app_logger)

    if not anomalous_ts:
        logger.error('error :: get_anoms :: no anomalous_ts')
        return []

    anomalies = []
    try:
        anomaly_ts_dict = dict(anomalous_ts)
        my_detector = AnomalyDetector(anomaly_ts_dict, score_threshold=1.5)
        anomalies = my_detector.get_anomalies()
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: get_anoms :: AnomalyDetector')
    return anomalies


def get_assigned_metrics(i):
    try:
        unique_metrics = list(redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: get_assigned_metrics :: no unique_metrics')
        return []
    # Discover assigned metrics
    keys_per_processor = int(ceil(float(len(unique_metrics)) / float(settings.LUMINOSITY_PROCESSES)))
    if i == settings.LUMINOSITY_PROCESSES:
        assigned_max = len(unique_metrics)
    else:
        assigned_max = min(len(unique_metrics), i * keys_per_processor)
    assigned_min = (i - 1) * keys_per_processor
    assigned_keys = range(assigned_min, assigned_max)
    # Compile assigned metrics
    assigned_metrics = [unique_metrics[index] for index in assigned_keys]
    return assigned_metrics


def get_correlations(base_name, anomaly_timestamp, anomalous_ts, assigned_metrics, raw_assigned, anomalies):

    logger = logging.getLogger(skyline_app_logger)

    # Distill timeseries strings into lists
    start = timer()
    count = 0
    # Sample the time series
    from_timestamp = anomaly_timestamp - 600
    correlated_metrics = []
    correlations = []
    no_data = False
    if not anomalous_ts:
        no_data = True
    if not assigned_metrics:
        no_data = True
    if not raw_assigned:
        no_data = True
    if not anomalies:
        no_data = True
    if no_data:
        logger.error('error :: get_correlations :: no data')
        return (correlated_metrics, correlations)

    for i, metric_name in enumerate(assigned_metrics):
        count += 1
        # print(metric_name)
        if count > 1000:
            break
        correlated = None
        metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
        if str(metric_base_name) == str(base_name):
            continue
        try:
            raw_series = raw_assigned[i]
            unpacker = Unpacker(use_list=False)
            unpacker.feed(raw_series)
            timeseries = list(unpacker)
        except:
            timeseries = []
        if not timeseries:
            # print('no time series data for %s' % base_name)
            continue

        # Convert the time series if this is a known_derivative_metric
        known_derivative_metric = is_derivative_metric(skyline_app, metric_base_name)
        if known_derivative_metric:
            try:
                derivative_timeseries = nonNegativeDerivative(timeseries)
                timeseries = derivative_timeseries
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: nonNegativeDerivative')

        correlate_ts = []
        for ts, value in timeseries:
            if int(ts) < from_timestamp:
                continue
            if int(ts) <= anomaly_timestamp:
                correlate_ts.append((int(ts), value))
            if int(ts) > (anomaly_timestamp + 61):
                break
        if not correlate_ts:
            continue

        anomaly_ts_dict = dict(anomalous_ts)
        correlate_ts_dict = dict(correlate_ts)

        for a in anomalies:
            try:
                if int(a.exact_timestamp) < int(anomaly_timestamp - 120):
                    continue
                if int(a.exact_timestamp) > int(anomaly_timestamp + 120):
                    continue
            except:
                continue
            try:
                time_period = (int(anomaly_timestamp - 120), int(anomaly_timestamp + 120))
                my_correlator = Correlator(anomaly_ts_dict, correlate_ts_dict, time_period)
                # For better correlation use 0.9 instead of 0.8 for the threshold
                # @modified 20180524 - Feature #2360: CORRELATE_ALERTS_ONLY
                #                      Branch #2270: luminosity
                #                      Feature #2378: Add redis auth to Skyline and rebrow
                # Added this to setting.py
                # if my_correlator.is_correlated(threshold=0.9):
                try:
                    cross_correlation_threshold = settings.LUMINOL_CROSS_CORRELATION_THRESHOLD
                except:
                    cross_correlation_threshold = 0.9
                if my_correlator.is_correlated(threshold=cross_correlation_threshold):
                    correlation = my_correlator.get_correlation_result()
                    correlated = True
                    correlations.append([metric_base_name, correlation.coefficient, correlation.shift, correlation.shifted_coefficient])
            except:
                pass
        if correlated:
            correlated_metrics.append(metric_base_name)

    end = timer()
    logger.info('correlated %s metrics to %s anomaly, processed in %.6f seconds' % (str(len(correlated_metrics)), base_name, (end - start)))
    return (correlated_metrics, correlations)


def process_correlations(i, anomaly_id):
    anomaly_id, metric_id, anomaly_timestamp, base_name = get_anomaly(anomaly_id)
    anomalous_ts = get_anomalous_ts(base_name, anomaly_timestamp)
    anomalies = get_anoms(anomalous_ts)
    assigned_metrics = get_assigned_metrics(i)
    raw_assigned = redis_conn.mget(assigned_metrics)
    correlated_metrics, correlations = get_correlations(base_name, anomaly_timestamp, anomalous_ts, assigned_metrics, raw_assigned, anomalies)
    sorted_correlations = sorted(correlations, key=lambda x: x[1], reverse=True)
    return (base_name, anomaly_timestamp, anomalies, correlated_metrics, correlations, sorted_correlations)
