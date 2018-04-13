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

skyline_app = 'luminosity'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

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
        return False

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
        return False

    return (anomaly_id, metric_id, anomaly_timestamp, base_name)


def get_anomalous_ts(base_name, anomaly_timestamp):

    # from skyline_functions import nonNegativeDerivative
    anomalous_metric = '%s%s' % (settings.FULL_NAMESPACE, base_name)
    assigned_metrics = [anomalous_metric]
    raw_assigned = redis_conn.mget(assigned_metrics)
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

    anomalies = []
    try:
        anomaly_ts_dict = dict(anomalous_ts)
        my_detector = AnomalyDetector(anomaly_ts_dict, score_threshold=1.5)
        anomalies = my_detector.get_anomalies()
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: AnomalyDetector')
    return anomalies


def get_assigned_metrics(i):
    unique_metrics = list(redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
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
                if my_correlator.is_correlated(threshold=0.9):
                    correlation = my_correlator.get_correlation_result()
                    correlated = True
                    correlations.append([metric_base_name, correlation.coefficient, correlation.shift, correlation.shifted_coefficient])
            except:
                pass
        if correlated:
            correlated_metrics.append(metric_base_name)

    end = timer()
    logger.info('correlated %s metrics calculated in %.6f seconds' % (str(len(correlated_metrics)), (end - start)))
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
