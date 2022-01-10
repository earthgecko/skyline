import logging
from redis import StrictRedis
from msgpack import Unpacker
import traceback
# @modified 20191115 - Branch #3262: py3
# from math import ceil

from luminol.anomaly_detector import AnomalyDetector
from luminol.correlator import Correlator
from timeit import default_timer as timer
# @added 20180720 - Feature #2464: luminosity_remote_data
# Added requests and ast
import requests
from ast import literal_eval

# @added 20200428 - Feature #3510: Enable Luminosity to handle correlating namespaces only
#                   Feature #3500: webapp - crucible_process_metrics
#                   Feature #1448: Crucible web UI
from time import time

import settings
from skyline_functions import (
    mysql_select, is_derivative_metric, nonNegativeDerivative,
    # @added 20191030 - Bug #3266: py3 Redis binary objects not strings
    #                   Branch #3262: py3
    # Added a single functions to deal with Redis connection and the
    # charset='utf-8', decode_responses=True arguments required in py3
    get_redis_conn, get_redis_conn_decoded,
    # @added 20200506 - Feature #3532: Sort all time series
    sort_timeseries,
    # @added 20201207 - Feature #3858: skyline_functions - correlate_or_relate_with
    correlate_or_relate_with)

# @added 20210820 - Task #4030: refactoring
#                   Feature #4164: luminosity - cloudbursts
from functions.timeseries.determine_data_frequency import determine_data_frequency

# @added 20200428 - Feature #3510: Enable Luminosity to handle correlating namespaces only
#                   Feature #3512: matched_or_regexed_in_list function
from matched_or_regexed_in_list import matched_or_regexed_in_list
try:
    # @modified 20200606 - Bug #3572: Apply list to settings import
    correlate_namespaces_only = list(settings.LUMINOSITY_CORRELATE_NAMESPACES_ONLY)
except:
    correlate_namespaces_only = []

# @added 20200924 - Feature #3756: LUMINOSITY_CORRELATION_MAPS
try:
    LUMINOSITY_CORRELATION_MAPS = settings.LUMINOSITY_CORRELATION_MAPS.copy()
except:
    LUMINOSITY_CORRELATION_MAPS = {}

# @added 20210124 - Feature #3956: luminosity - motifs
try:
    LUMINOSITY_ANALYSE_MOTIFS = settings.LUMINOSITY_ANALYSE_MOTIFS
except:
    LUMINOSITY_ANALYSE_MOTIFS = True
if LUMINOSITY_ANALYSE_MOTIFS:
    from collections import Counter
    import matrixprofile as mp
    import numpy as np

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
# @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
#                      Branch #3262: py3
# Use get_redis_conn and get_redis_conn_decoded to use on Redis sets when the bytes
# types need to be decoded as utf-8 to str
# if settings.REDIS_PASSWORD:
#     redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
# else:
#     redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)

# @added 20191030 - Bug #3266: py3 Redis binary objects not strings
#                   Branch #3262: py3
# Added a single functions to deal with Redis connection and the
# charset='utf-8', decode_responses=True arguments required in py3
redis_conn = get_redis_conn(skyline_app)
redis_conn_decoded = get_redis_conn_decoded(skyline_app)


# @added 20201203 - Feature #3860: luminosity - handle low frequency data
# Handle varying metric resolutions
def determine_resolution(timeseries):
    """
    Determine the resolution of the timeseries data
    """
    resolution = 60
    try:
        ts_resolution = int(timeseries[-1][0]) - int(timeseries[-2][0])
        if ts_resolution != 60:
            resolution = ts_resolution
        # Handle data that has any big airgaps as best effort as possible by
        # checking the previous timestamps
        if ts_resolution > 3601:
            ts_resolution = int(timeseries[-3][0]) - int(timeseries[-4][0])
        if ts_resolution != 60:
            resolution = ts_resolution
    except:
        pass
    return resolution


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
            # @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            # smtp_alerter_metrics = list(redis_conn.smembers('analyzer.smtp_alerter_metrics'))
            # @modified 20200421 - Feature #3306: Record anomaly_end_timestamp
            #                      Branch #2270: luminosity
            #                      Branch #3262: py3
            # Changed to use the aet Redis set, used to determine and record the
            # anomaly_end_timestamp, some transient sets need to copied so that
            # the data always exists, even if it is sourced from a transient set.
            # smtp_alerter_metrics = list(redis_conn_decoded.smembers('analyzer.smtp_alerter_metrics'))
            smtp_alerter_metrics = list(redis_conn_decoded.smembers('aet.analyzer.smtp_alerter_metrics'))
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
        # @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # unique_metrics = list(redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
        unique_metrics = list(redis_conn_decoded.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
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

        # @added 20200507 - Feature #3532: Sort all time series
        # To ensure that there are no unordered timestamps in the time
        # series which are artefacts of the collector or carbon-relay, sort
        # all time series by timestamp before analysis.
        original_timeseries = timeseries
        if original_timeseries:
            timeseries = sort_timeseries(original_timeseries)
            del original_timeseries

    # Convert the time series if this is a known_derivative_metric
    known_derivative_metric = is_derivative_metric(skyline_app, base_name)
    if known_derivative_metric:
        derivative_timeseries = nonNegativeDerivative(timeseries)
        timeseries = derivative_timeseries

    # @added 20201203 - Feature #3860: luminosity - handle low frequency data
    # Determine data resolution
    # @modified 20210820 - Feature #3860: luminosity - handle low frequency data
    #                      Task #4030: refactoring
    #                      Feature #4164: luminosity - cloudbursts
    # Use common determine_data_frequency function
    # resolution = determine_resolution(timeseries)
    resolution = determine_data_frequency(skyline_app, timeseries, False)

    # Sample the time series
    # @modified 20180720 - Feature #2464: luminosity_remote_data
    # Added note here - if you modify the value of 600 here, it must be
    # modified in the luminosity_remote_data function in
    # skyline/webapp/backend.py as well
    # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
    # from_timestamp = anomaly_timestamp - 600
    from_timestamp = anomaly_timestamp - (resolution * 10)

    anomaly_ts = []
    for ts, value in timeseries:
        if int(ts) < from_timestamp:
            continue
        if int(ts) <= anomaly_timestamp:
            anomaly_ts.append((int(ts), value))
        if int(ts) > anomaly_timestamp:
            break

    # @added 20190515 - Bug #3008: luminosity - do not analyse short time series
    # Only return a time series sample if the sample has sufficient data points
    # otherwise get_anomalies() will throw and error
    len_anomaly_ts = len(anomaly_ts)
    if len_anomaly_ts <= 9:
        logger.info('%s insufficient data not retrieved, only %s data points surfaced, not correlating' % (
            str(base_name), str(len_anomaly_ts)))
        return []

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


# @modified 20200506 - Feature #3510: Enable Luminosity to handle correlating namespaces only
# def get_assigned_metrics(i,):
def get_assigned_metrics(i, base_name):
    try:
        # @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # unique_metrics = list(redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
        unique_metrics = list(redis_conn_decoded.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
        # @added 20200924 - Feature #3756: LUMINOSITY_CORRELATION_MAPS
        original_unique_metrics = list(unique_metrics)
        logger.info('get_asssigned_metrics :: created original_unique_metrics with %s metrics' % (
            str(len(original_unique_metrics))))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: get_assigned_metrics :: no unique_metrics')
        return []

    # @added 20200506 - Feature #3510: Enable Luminosity to handle correlating namespaces only
    correlate_namespace_to = None
    if correlate_namespaces_only:
        for correlate_namespace in correlate_namespaces_only:
            try:
                correlate_namespace_matched_by = None
                correlate_namespace_to, correlate_namespace_matched_by = matched_or_regexed_in_list(skyline_app, base_name, [correlate_namespace])
                if correlate_namespace_to:
                    break
            except:
                pass
    if correlate_namespaces_only:
        if not correlate_namespace_to:
            correlate_with_metrics = []
            for metric_name in unique_metrics:
                # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
                # metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                if metric_name.startswith(settings.FULL_NAMESPACE):
                    metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                else:
                    metric_base_name = metric_name

                add_metric, add_metric_matched_by = matched_or_regexed_in_list(skyline_app, metric_base_name, correlate_namespaces_only)
                if not add_metric:
                    correlate_with_metrics.append(metric_name)
            if correlate_with_metrics:
                logger.info('get_asssigned_metrics :: replaced %s unique_metrics with %s correlate_with_metrics, excluding metrics from LUMINOSITY_CORRELATE_NAMESPACES_ONLY' % (
                    str(len(unique_metrics)), str(len(correlate_with_metrics))))
                unique_metrics = correlate_with_metrics
    if correlate_namespace_to:
        correlate_with_metrics = []
        correlate_with_namespace = correlate_namespace_matched_by['matched_namespace']
        if correlate_with_namespace:
            for metric_name in unique_metrics:
                # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
                # metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                if metric_name.startswith(settings.FULL_NAMESPACE):
                    metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                else:
                    metric_base_name = metric_name

                add_metric, add_metric_matched_by = matched_or_regexed_in_list(skyline_app, metric_base_name, [correlate_with_namespace])
                if add_metric:
                    correlate_with_metrics.append(metric_name)
        if correlate_with_metrics:
            logger.info('get_correlations :: replaced %s unique_metrics with %s correlate_with_metrics' % (
                str(len(unique_metrics)), str(len(correlate_with_metrics))))
            unique_metrics = correlate_with_metrics

    # @added 20200924 - Feature #3756: LUMINOSITY_CORRELATION_MAPS
    # WIP not tested or fully POCed
    if LUMINOSITY_CORRELATION_MAPS:
        also_correlate = []
        if metric_name.startswith(settings.FULL_NAMESPACE):
            metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
        else:
            metric_base_name = metric_name
        for correlation_map in LUMINOSITY_CORRELATION_MAPS:
            metric_in_map = False
            for i_map in LUMINOSITY_CORRELATION_MAPS[correlation_map]:
                if i_map == metric_base_name:
                    metric_in_map = True
                    break
            if metric_in_map:
                for i_map in LUMINOSITY_CORRELATION_MAPS[correlation_map]:
                    if i_map != metric_base_name:
                        also_correlate.append(i_map)
        if also_correlate:
            for i_metric_base_name in also_correlate:
                i_metric_name = '%s%s' % (settings.FULL_NAMESPACE, i_metric_base_name)
                if i_metric_name not in unique_metrics:
                    unique_metrics.append(i_metric_name)
            logger.info('get_correlations :: appended %s metrics to unique_metrics to be correlated' % (
                str(len(also_correlate))))

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
# @modified 20201203 - Feature #3860: luminosity - handle low frequency data
# Added the metric resolution
# def get_remote_assigned(anomaly_timestamp):
def get_remote_assigned(anomaly_timestamp, resolution):
    remote_assigned = []
    # @modified 20201215 - Feature #3890: metrics_manager - sync_cluster_files
    # for remote_url, remote_user, remote_password in settings.REMOTE_SKYLINE_INSTANCES:
    for remote_url, remote_user, remote_password, hostname in settings.REMOTE_SKYLINE_INSTANCES:
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
        # @modified 20201117 - Feature #3824: get_cluster_data
        # Correct url with api for get_cluster_data
        # url = '%s/luminosity_remote_data?anomaly_timestamp=%s' % (remote_url, str(anomaly_timestamp))
        # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
        # Added the metric resolution
        # url = '%s/api?luminosity_remote_data&anomaly_timestamp=%s' % (remote_url, str(anomaly_timestamp))
        url = '%s/api?luminosity_remote_data&anomaly_timestamp=%s&resolution=%s' % (remote_url, str(anomaly_timestamp), str(resolution))

        response_ok = False

        # @added 20190519 - Branch #3002: docker
        # Handle self signed certificate on Docker
        verify_ssl = True
        try:
            running_on_docker = settings.DOCKER
        except:
            running_on_docker = False
        if running_on_docker:
            verify_ssl = False

        try:
            # @modified 20190519 - Branch #3002: docker
            # r = requests.get(url, timeout=15, auth=(remote_user, remote_password))
            # @modified 20201222 - Feature #3824: get_cluster_data
            # Allow for longer response
            # r = requests.get(url, timeout=15, auth=(remote_user, remote_password), verify=verify_ssl)
            # @modified 20201222 - Feature #3824: get_cluster_data
            # Allow for longer cluster responses with large metric populations
            # r = requests.get(url, timeout=15, auth=(remote_user, remote_password), verify=verify_ssl)
            r = requests.get(url, timeout=65, auth=(remote_user, remote_password), verify=verify_ssl)
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
                # @modified 20201204 - Feature #2464: luminosity_remote_data
                #                      Feature #3820: HORIZON_SHARDS
                # decompressed_data = (r.content)
                # data = literal_eval(decompressed_data)
                data = literal_eval(r.text)
                logger.info('get_remote_assigned :: response data decompressed with native requests gzip decoding')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: get_remote_assigned :: failed to decompress gzipped response with native requests gzip decoding')

            if data:
                ts_data = data['results']
                logger.info('get_remote_assigned :: %s metrics retrieved compressed from %s' % (str(len(ts_data)), remote_url))
                if remote_assigned:
                    # @modified 20201204 - Feature #2464: luminosity_remote_data
                    #                      Feature #3820: HORIZON_SHARDS
                    # remote_assigned.append(ts_data)
                    for item in ts_data:
                        remote_assigned.append(item)
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

    # @added 20201203 - Feature #3860: luminosity - handle low frequency data
    # Determine data resolution
    # @modified 20210820 - Feature #3860: luminosity - handle low frequency data
    #                      Task #4030: refactoring
    #                      Feature #4164: luminosity - cloudbursts
    # Use common determine_data_frequency function
    # resolution = determine_resolution(anomalous_ts)
    resolution = determine_data_frequency(skyline_app, anomalous_ts, False)

    # Sample the time series
    # @modified 20180720 - Feature #2464: luminosity_remote_data
    # Added note here - if you modify the value of 600 here, it must be
    # modified in the luminosity_remote_data function in
    # skyline/webapp/backend.py as well
    # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
    # from_timestamp = anomaly_timestamp - 600
    from_timestamp = anomaly_timestamp - (resolution * 10)

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

    # @added 20200428 - Feature #3510: Enable Luminosity to handle correlating namespaces only
    #                   Feature #3500: webapp - crucible_process_metrics
    #                   Feature #1448: Crucible web UI
    # Discard the check if the anomaly_timestamp is not in FULL_DURATION as it
    # will have been added via the Crucible or webapp/crucible route
    start_timestamp_of_full_duration_data = int(time() - settings.FULL_DURATION)
    if anomaly_timestamp < (start_timestamp_of_full_duration_data + 2000):
        logger.info('get_correlations :: the anomaly_timestamp is too old not correlating')
        return (correlated_metrics, correlations)

    start_local_correlations = timer()

    local_redis_metrics_checked_count = 0
    local_redis_metrics_correlations_count = 0

    logger.info('get_correlations :: the local Redis metric count is %s' % str(len(assigned_metrics)))

    # @added 20200428 - Feature #3510: Enable Luminosity to handle correlating namespaces only
    # Removed here and handled in get_assigned_metrics

    for i, metric_name in enumerate(assigned_metrics):
        count += 1
        # print(metric_name)
        # @modified 20180719 - Branch #2270: luminosity
        # Removed test limiting that was errorneously left in
        # if count > 1000:
        #     break
        correlated = None
        # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
        # metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
        if metric_name.startswith(settings.FULL_NAMESPACE):
            metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
        else:
            metric_base_name = metric_name

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

        # @added 20200507 - Feature #3532: Sort all time series
        # To ensure that there are no unordered timestamps in the time
        # series which are artefacts of the collector or carbon-relay, sort
        # all time series by timestamp before analysis.
        original_timeseries = timeseries
        if original_timeseries:
            timeseries = sort_timeseries(original_timeseries)
            del original_timeseries

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

        # @added 20201203 - Feature #3860: luminosity - handle low frequency data
        # Determine data resolution
        # @modified 20210820 - Feature #3860: luminosity - handle low frequency data
        #                      Task #4030: refactoring
        #                      Feature #4164: luminosity - cloudbursts
        # Use common determine_data_frequency function
        # resolution = determine_resolution(timeseries)
        resolution = determine_data_frequency(skyline_app, timeseries, False)

        for ts, value in timeseries:
            if int(ts) < from_timestamp:
                continue
            if int(ts) <= anomaly_timestamp:
                correlate_ts.append((int(ts), value))
            # @modified 20180720 - Feature #2464: luminosity_remote_data
            # Added note here - if you modify the value of 61 here, it must be
            # modified in the luminosity_remote_data function in
            # skyline/webapp/backend.py as well
            # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
            # Handle varying metric resolutions
            # if int(ts) > (anomaly_timestamp + 61):
            # @modified 20211226 - Feature #3860: luminosity - handle low frequency data
            # Only if resolution is known
            if resolution:
                if int(ts) > (anomaly_timestamp + (resolution + 1)):
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
                # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
                # Handle varying metric resolutions
                # if int(a.exact_timestamp) < int(anomaly_timestamp - 120):
                #     continue
                # if int(a.exact_timestamp) > int(anomaly_timestamp + 120):
                #     continue
                if int(a.exact_timestamp) < int(anomaly_timestamp - (resolution * 2)):
                    continue
                if int(a.exact_timestamp) > int(anomaly_timestamp + (resolution * 2)):
                    continue

            except:
                continue
            try:
                # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
                # Handle varying metric resolutions
                # time_period = (int(anomaly_timestamp - 120), int(anomaly_timestamp + 120))
                time_period = (int(anomaly_timestamp - (resolution * 2)), int(anomaly_timestamp + (resolution * 2)))

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

    # @added 20201207 - Feature #3858: skyline_functions - correlate_or_relate_with
    do_not_correlate_with = []

    remote_metrics_count = 0
    remote_correlations_check_count = 0
    remote_correlations_count = 0
    logger.info('get_correlations :: remote_assigned count %s' % str(len(remote_assigned)))
    start_remote_correlations = timer()
    for ts_data in remote_assigned:
        remote_metrics_count += 1
        correlated = None
        metric_name = str(ts_data[0])
        # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
        # metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
        if metric_name.startswith(settings.FULL_NAMESPACE):
            metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
        else:
            metric_base_name = metric_name

        if str(metric_base_name) == str(base_name):
            continue

        # @added 20201207 - Feature #3858: skyline_functions - correlate_or_relate_with
        try:
            correlate_or_relate = correlate_or_relate_with(skyline_app, base_name, metric_base_name)
            if not correlate_or_relate:
                do_not_correlate_with.append(metric_base_name)
                continue
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: get_remote_assigned :: failed to evaluate correlate_or_relate_with')

        timeseries = []
        try:
            timeseries = ts_data[1]
        except:
            timeseries = []
        if not timeseries:
            continue

        # @added 20201203 - Feature #3860: luminosity - handle low frequency data
        # Determine data resolution
        # @modified 20210820 - Feature #3860: luminosity - handle low frequency data
        #                      Task #4030: refactoring
        #                      Feature #4164: luminosity - cloudbursts
        # Use common determine_data_frequency function
        # resolution = determine_resolution(timeseries)
        resolution = determine_data_frequency(skyline_app, timeseries, False)

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
            # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
            # Handle varying metric resolutions
            # if int(ts) > (anomaly_timestamp + 61):
            if int(ts) > (anomaly_timestamp + (resolution + 1)):
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
                # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
                # Handle varying metric resolutions
                # if int(a.exact_timestamp) < int(anomaly_timestamp - 120):
                #     continue
                # if int(a.exact_timestamp) > int(anomaly_timestamp + 120):
                #     continue
                if int(a.exact_timestamp) < int(anomaly_timestamp - (resolution * 2)):
                    continue
                if int(a.exact_timestamp) > int(anomaly_timestamp + (resolution * 2)):
                    continue
            except:
                continue
            try:
                # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
                # Handle varying metric resolutions
                # time_period = (int(anomaly_timestamp - 120), int(anomaly_timestamp + 120))
                time_period = (int(anomaly_timestamp - (resolution * 2)), int(anomaly_timestamp + (resolution * 2)))

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

    # @added 20201207 - Feature #3858: skyline_functions - correlate_or_relate_with
    if len(do_not_correlate_with) > 0:
        logger.info('get_correlations :: discarded %s remote assigned metrics as not in a correlation group with %s' % (
            str(len(do_not_correlate_with)), base_name))

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

    # @added 20210124 - Feature #3956: luminosity - motifs
    if LUMINOSITY_ANALYSE_MOTIFS:
        # DO stuff...
        # def skyline_matrixprofile(current_skyline_app, parent_pid, timeseries, algorithm_parameters):
        todo = True

    return (correlated_metrics, correlations, metrics_checked_for_correlation, runtime)


def process_correlations(i, anomaly_id):

    logger = logging.getLogger(skyline_app_logger)

    anomalies = []
    correlated_metrics = []
    correlations = []
    sorted_correlations = []
    # @added 20210124 - Feature #3956: luminosity - motifs
    motifs = []

    start_process_correlations = timer()

    anomaly_id, metric_id, anomaly_timestamp, base_name = get_anomaly(anomaly_id)

    # @added 20200428 - Feature #3510: Enable Luminosity to handle correlating namespaces only
    #                   Feature #3500: webapp - crucible_process_metrics
    #                   Feature #1448: Crucible web UI
    # Discard the check if the anomaly_timestamp is not in FULL_DURATION as it
    # will have been added via the Crucible or webapp/crucible route
    start_timestamp_of_full_duration_data = int(time() - settings.FULL_DURATION)
    if anomaly_timestamp < (start_timestamp_of_full_duration_data + 2000):
        logger.info('process_correlations :: the anomaly_timestamp %s is too old not correlating' % str(anomaly_timestamp))
        metrics_checked_for_correlation = 0
        runtime = 0
        # @added 20210124 - Feature #3956: luminosity - motifs
        # Added motifs
        return (base_name, anomaly_timestamp, anomalies, correlated_metrics, correlations, sorted_correlations, metrics_checked_for_correlation, runtime, motifs)

    anomalous_ts = get_anomalous_ts(base_name, anomaly_timestamp)
    if not anomalous_ts:
        metrics_checked_for_correlation = 0
        runtime = 0
        logger.info('process_correlations :: no timeseries data available in Redis for %s, nothing to correlate' % str(base_name))
        # @added 20210124 - Feature #3956: luminosity - motifs
        # Added motifs
        return (base_name, anomaly_timestamp, anomalies, correlated_metrics, correlations, sorted_correlations, metrics_checked_for_correlation, runtime, motifs)
    anomalies = get_anoms(anomalous_ts)
    if not anomalies:
        metrics_checked_for_correlation = 0
        runtime = 0
        logger.info('process_correlations :: no anomalies found for %s, nothing to correlate' % str(base_name))
        # @added 20210124 - Feature #3956: luminosity - motifs
        # Added motifs
        return (base_name, anomaly_timestamp, anomalies, correlated_metrics, correlations, sorted_correlations, metrics_checked_for_correlation, runtime, motifs)

    # @modified 20200506 - Feature #3510: Enable Luminosity to handle correlating namespaces only
    # assigned_metrics = get_assigned_metrics(i)
    assigned_metrics = get_assigned_metrics(i, base_name)

    # @added 20201203 - Feature #3860: luminosity - handle low frequency data
    # Determine data resolution
    # @modified 20210820 - Feature #3860: luminosity - handle low frequency data
    #                      Task #4030: refactoring
    #                      Feature #4164: luminosity - cloudbursts
    # Use common determine_data_frequency function
    # resolution = determine_resolution(anomalous_ts)
    resolution = determine_data_frequency(skyline_app, anomalous_ts, False)

    raw_assigned = redis_conn.mget(assigned_metrics)
    # @added 20180720 - Feature #2464: luminosity_remote_data
    remote_assigned = []
    if settings.REMOTE_SKYLINE_INSTANCES:
        # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
        # Add the metric resolution
        # remote_assigned = get_remote_assigned(anomaly_timestamp)
        remote_assigned = get_remote_assigned(anomaly_timestamp, resolution)

    # @modified 20180720 - Feature #2464: luminosity_remote_data
    # correlated_metrics, correlations = get_correlations(base_name, anomaly_timestamp, anomalous_ts, assigned_metrics, raw_assigned, anomalies)
    correlated_metrics, correlations, metrics_checked_for_correlation, runtime = get_correlations(base_name, anomaly_timestamp, anomalous_ts, assigned_metrics, raw_assigned, remote_assigned, anomalies)
    sorted_correlations = sorted(correlations, key=lambda x: x[1], reverse=True)
    end_process_correlations = timer()

    logger.info('process_correlations :: took %.6f seconds' % (
        (end_process_correlations - start_process_correlations)))

    # @modified 20210124 - Feature #3956: luminosity - motifs
    # Added motifs
    return (base_name, anomaly_timestamp, anomalies, correlated_metrics, correlations, sorted_correlations, metrics_checked_for_correlation, runtime, motifs)
