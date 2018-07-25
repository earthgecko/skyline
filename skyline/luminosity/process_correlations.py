import logging
from redis import StrictRedis
from msgpack import Unpacker
import traceback
from math import ceil
from luminol.anomaly_detector import AnomalyDetector
from luminol.correlator import Correlator
from timeit import default_timer as timer
# @added 20180720 - Feature #2464: luminosity_remote_data
# Added requests and ast
import requests
from ast import literal_eval

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
            return []

    if not base_name or not anomaly_timestamp:
        return []

    # from skyline_functions import nonNegativeDerivative
    anomalous_metric = '%s%s' % (settings.FULL_NAMESPACE, base_name)
    unique_metrics = []
    try:
        unique_metrics = list(redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: get_assigned_metrics :: no unique_metrics')
        return []
    # @added 20180720 - Feature #2464: luminosity_remote_data
    # Ensure that Luminosity only processes it's own Redis metrics so that if
    # multiple Skyline instances are running, Luminosity does not process an
    # anomaly_id for a metric that is not local to itself.  This will stop the
    # call to the remote Redis with other_redis_conn below.  With the
    # introduction of the preprocessing luminosity_remote_data API endpoint for
    # remote Skyline instances, there is no further requirement for Skyline
    # instances to have direct access to Redis on another Skyline instance.
    # A much better solution and means all data is preprocessed and encrypted,
    # there is no need for iptables other than 443 (or custom https port).
    #
    if anomalous_metric in unique_metrics:
        logger.info('%s is a metric in Redis, processing on this Skyline instance' % base_name)
    else:
        logger.info('%s is not a metric in Redis, not processing on this Skyline instance' % base_name)
        return []

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

    # @modified 20180721 - Feature #2464: luminosity_remote_data
    # TO BE DEPRECATED settings.OTHER_SKYLINE_REDIS_INSTANCES
    # with the addition of the luminosity_remote_data API call and the above
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
        return []

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
    # @modified 20180720 - Feature #2464: luminosity_remote_data
    # Added note here - if you modify the value of 600 here, it must be
    # modified in the luminosity_remote_data function in
    # skyline/webapp/backend.py as well
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
    # @modified 20180720 - Feature #2464: luminosity_remote_data
    # Use just 1 processor
    # keys_per_processor = int(ceil(float(len(unique_metrics)) / float(settings.LUMINOSITY_PROCESSES)))
    # if i == settings.LUMINOSITY_PROCESSES:
    #     assigned_max = len(unique_metrics)
    # else:
    #     assigned_max = min(len(unique_metrics), i * keys_per_processor)
    # assigned_min = (i - 1) * keys_per_processor
    assigned_min = 0
    assigned_max = len(unique_metrics)
    assigned_keys = range(assigned_min, assigned_max)
    # Compile assigned metrics
    assigned_metrics = [unique_metrics[index] for index in assigned_keys]
    logger.info('get_assigned_metrics :: %s metrics found in local Redis' % str(len(unique_metrics)))
    return assigned_metrics


# @added 20180720 - Feature #2464: luminosity_remote_data
def get_remote_assigned(anomaly_timestamp):
    remote_assigned = []
    for remote_url, remote_user, remote_password in settings.REMOTE_SKYLINE_INSTANCES:
        # @modified 20180722 - Feature #2464: luminosity_remote_data
        # Use a gzipped response - deprecated the raw unprocessed time series
        # method
        # response_ok = False
        # url = '%s/api?luminosity_remote_data=true&anomaly_timestamp=%s' % (remote_url, str(anomaly_timestamp))
        # try:
        #     r = requests.get(url, timeout=15, auth=(remote_user, remote_password))
        #     if int(r.status_code) == 200:
        #         logger.info('get_remote_assigned :: time series data retrieved from %s' % remote_url)
        #         response_ok = True
        #     else:
        #         logger.error('get_remote_assigned :: time series data not retrieved from %s, status code %s returned' % (remote_url, str(r.status_code)))
        # except:
        #     logger.error(traceback.format_exc())
        #     logger.error('error :: get_remote_assigned :: failed to get time series data from %s' % str(url))
        # if response_ok:
        #     data = literal_eval(r.text)
        #     ts_data = data['results']
        #     # remote_assigned.append(ts_data)
        #     remote_assigned = ts_data
        #     logger.info('get_remote_assigned :: %s metrics retrieved from %s' % (str(len(ts_data)), remote_url))

        # @added 20180721 - Feature #2464: luminosity_remote_data
        # Use a gzipped response as the response as raw unprocessed time series
        # can be mulitple megabytes
        url = '%s/luminosity_remote_data?anomaly_timestamp=%s' % (remote_url, str(anomaly_timestamp))
        response_ok = False
        try:
            r = requests.get(url, timeout=15, auth=(remote_user, remote_password))
            if int(r.status_code) == 200:
                logger.info('get_remote_assigned :: time series data retrieved from %s' % remote_url)
                response_ok = True
            else:
                logger.error('get_remote_assigned :: time series data not retrieved from %s, status code %s returned' % (remote_url, str(r.status_code)))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: get_remote_assigned :: failed to get time series data from %s' % str(url))
        if response_ok:
            # import gzip
            # import StringIO
            data = None
            try:
                # @modified 20180722 - Feature #2464: luminosity_remote_data
                # First method is failing.  Trying requests automatic gzip
                # decoding below.
                # Binary Response Content - You can also access the response body as bytes, for non-text requests:
                # >>> r.content
                # b'[{"repository":{"open_issues":0,"url":"https://github.com/...
                # The gzip and deflate transfer-encodings are automatically decoded for you.
                # fakefile = StringIO.StringIO(r.content)  # fakefile is now a file-like object thta can be passed to gzip.GzipFile:
                # try:
                #     uncompressed = gzip.GzipFile(fileobj=fakefile, mode='r')
                #    decompressed_data = uncompressed.read()
                decompressed_data = (r.content)
                data = literal_eval(decompressed_data)
                logger.info('get_remote_assigned :: response data decompressed with native requests gzip decoding')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: get_remote_assigned :: failed to decompress gzipped response with native requests gzip decoding')

            if data:
                ts_data = data['results']
                logger.info('get_remote_assigned :: %s metrics retrieved compressed from %s' % (str(len(ts_data)), remote_url))
                if remote_assigned:
                    remote_assigned.append(ts_data)
                else:
                    remote_assigned = ts_data

    logger.info('get_remote_assigned :: %s metrics retrieved from remote Skyline instances' % (str(len(remote_assigned))))
    return remote_assigned


# @modified 20180720 - Feature #2464: luminosity_remote_data
# def get_correlations(base_name, anomaly_timestamp, anomalous_ts, assigned_metrics, raw_assigned, anomalies):
def get_correlations(
    base_name, anomaly_timestamp, anomalous_ts, assigned_metrics, raw_assigned,
        remote_assigned, anomalies):

    logger = logging.getLogger(skyline_app_logger)

    # Distill timeseries strings into lists
    start = timer()
    count = 0
    metrics_checked_for_correlation = 0
    # Sample the time series
    # @modified 20180720 - Feature #2464: luminosity_remote_data
    # Added note here - if you modify the value of 600 here, it must be
    # modified in the luminosity_remote_data function in
    # skyline/webapp/backend.py as well
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

    start_local_correlations = timer()

    local_redis_metrics_checked_count = 0
    local_redis_metrics_correlations_count = 0

    logger.info('get_correlations :: the local Redis metric count is %s' % str(len(assigned_metrics)))
    for i, metric_name in enumerate(assigned_metrics):
        count += 1
        # print(metric_name)
        # @modified 20180719 - Branch #2270: luminosity
        # Removed test limiting that was errorneously left in
        # if count > 1000:
        #     break
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
            # @modified 20180720 - Feature #2464: luminosity_remote_data
            # Added note here - if you modify the value of 61 here, it must be
            # modified in the luminosity_remote_data function in
            # skyline/webapp/backend.py as well
            if int(ts) > (anomaly_timestamp + 61):
                break
        if not correlate_ts:
            continue

        local_redis_metrics_checked_count += 1
        anomaly_ts_dict = dict(anomalous_ts)
        correlate_ts_dict = dict(correlate_ts)

        for a in anomalies:
            try:
                # @modified 20180720 - Feature #2464: luminosity_remote_data
                # Added note here - if you modify the value of 120 here, it must be
                # modified in the luminosity_remote_data function in
                # skyline/webapp/backend.py as well
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
                    metrics_checked_for_correlation += 1
                except:
                    cross_correlation_threshold = 0.9
                if my_correlator.is_correlated(threshold=cross_correlation_threshold):
                    correlation = my_correlator.get_correlation_result()
                    correlated = True
                    correlations.append([metric_base_name, correlation.coefficient, correlation.shift, correlation.shifted_coefficient])
                    local_redis_metrics_correlations_count += 1
            except:
                pass
        if correlated:
            correlated_metrics.append(metric_base_name)

    # @added 20180720 - Feature #2464: luminosity_remote_data
    # Added the correlation of preprocessed remote data
    end_local_correlations = timer()
    logger.info('get_correlations :: checked - local_redis_metrics_checked_count is %s' % str(local_redis_metrics_checked_count))
    logger.info('get_correlations :: correlated - local_redis_metrics_correlations_count is %s' % str(local_redis_metrics_correlations_count))
    logger.info('get_correlations :: processed %s correlations on local_redis_metrics_checked_count %s local metrics in %.6f seconds' % (
        str(local_redis_metrics_correlations_count),
        str(local_redis_metrics_checked_count),
        (end_local_correlations - start_local_correlations)))

    remote_metrics_count = 0
    remote_correlations_check_count = 0
    remote_correlations_count = 0
    logger.info('get_correlations :: remote_assigned count %s' % str(len(remote_assigned)))
    start_remote_correlations = timer()
    for ts_data in remote_assigned:
        remote_metrics_count += 1
        correlated = None
        metric_name = str(ts_data[0])
        metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
        if str(metric_base_name) == str(base_name):
            continue
        timeseries = []
        try:
            timeseries = ts_data[1]
        except:
            timeseries = []
        if not timeseries:
            continue

        correlate_ts = []
        for ts, value in timeseries:
            if int(ts) < from_timestamp:
                continue
            if int(ts) <= anomaly_timestamp:
                correlate_ts.append((int(ts), value))
            # @modified 20180720 - Feature #2464: luminosity_remote_data
            # Added note here - if you modify the value of 61 here, it must be
            # modified in the luminosity_remote_data function in
            # skyline/webapp/backend.py as well
            if int(ts) > (anomaly_timestamp + 61):
                break
        if not correlate_ts:
            continue

        anomaly_ts_dict = dict(anomalous_ts)
        correlate_ts_dict = dict(correlate_ts)

        for a in anomalies:
            try:
                # @modified 20180720 - Feature #2464: luminosity_remote_data
                # Added note here - if you modify the value of 120 here, it must be
                # modified in the luminosity_remote_data function in
                # skyline/webapp/backend.py as well
                if int(a.exact_timestamp) < int(anomaly_timestamp - 120):
                    continue
                if int(a.exact_timestamp) > int(anomaly_timestamp + 120):
                    continue
            except:
                continue
            try:
                time_period = (int(anomaly_timestamp - 120), int(anomaly_timestamp + 120))
                my_correlator = Correlator(anomaly_ts_dict, correlate_ts_dict, time_period)
                metrics_checked_for_correlation += 1
                remote_correlations_check_count += 1
                try:
                    cross_correlation_threshold = settings.LUMINOL_CROSS_CORRELATION_THRESHOLD
                except:
                    cross_correlation_threshold = 0.9
                if my_correlator.is_correlated(threshold=cross_correlation_threshold):
                    correlation = my_correlator.get_correlation_result()
                    correlated = True
                    correlations.append([metric_base_name, correlation.coefficient, correlation.shift, correlation.shifted_coefficient])
                    remote_correlations_count += 1
            except:
                pass
        if correlated:
            correlated_metrics.append(metric_base_name)

    end_remote_correlations = timer()
    logger.info('get_correlations :: checked - remote_correlations_check_count is %s' % str(remote_correlations_check_count))
    logger.info('get_correlations :: correlated - remote_correlations_count is %s' % str(remote_correlations_count))
    logger.info('get_correlations :: processed remote correlations on remote_metrics_count %s local metric in %.6f seconds' % (
        str(remote_metrics_count),
        (end_remote_correlations - start_remote_correlations)))

    end = timer()
    logger.info('get_correlations :: checked a total of %s metrics and correlated %s metrics to %s anomaly, processed in %.6f seconds' % (
        str(metrics_checked_for_correlation), str(len(correlated_metrics)),
        base_name, (end - start)))
    # @added 20170720 - Task #2462: Implement useful metrics for Luminosity
    # Added runtime to calculate avg_runtime Graphite metric
    runtime = '%.6f' % (end - start)
    return (correlated_metrics, correlations, metrics_checked_for_correlation, runtime)


def process_correlations(i, anomaly_id):

    logger = logging.getLogger(skyline_app_logger)

    anomalies = []
    correlated_metrics = []
    correlations = []
    sorted_correlations = []

    start_process_correlations = timer()

    anomaly_id, metric_id, anomaly_timestamp, base_name = get_anomaly(anomaly_id)
    anomalous_ts = get_anomalous_ts(base_name, anomaly_timestamp)
    if not anomalous_ts:
        metrics_checked_for_correlation = 0
        runtime = 0
        return (base_name, anomaly_timestamp, anomalies, correlated_metrics, correlations, sorted_correlations, metrics_checked_for_correlation, runtime)
    anomalies = get_anoms(anomalous_ts)
    if not anomalies:
        metrics_checked_for_correlation = 0
        runtime = 0
        return (base_name, anomaly_timestamp, anomalies, correlated_metrics, correlations, sorted_correlations, metrics_checked_for_correlation, runtime)
    assigned_metrics = get_assigned_metrics(i)
    raw_assigned = redis_conn.mget(assigned_metrics)
    # @added 20180720 - Feature #2464: luminosity_remote_data
    remote_assigned = []
    if settings.REMOTE_SKYLINE_INSTANCES:
        remote_assigned = get_remote_assigned(anomaly_timestamp)
    # @modified 20180720 - Feature #2464: luminosity_remote_data
    # correlated_metrics, correlations = get_correlations(base_name, anomaly_timestamp, anomalous_ts, assigned_metrics, raw_assigned, anomalies)
    correlated_metrics, correlations, metrics_checked_for_correlation, runtime = get_correlations(base_name, anomaly_timestamp, anomalous_ts, assigned_metrics, raw_assigned, remote_assigned, anomalies)
    sorted_correlations = sorted(correlations, key=lambda x: x[1], reverse=True)
    end_process_correlations = timer()

    logger.info('process_correlations :: took %.6f seconds' % (
        (end_process_correlations - start_process_correlations)))

    return (base_name, anomaly_timestamp, anomalies, correlated_metrics, correlations, sorted_correlations, metrics_checked_for_correlation, runtime)
