"""
populate_redis_timeseries.py
"""
import logging
from time import time

from settings import FULL_DURATION
from skyline_functions import get_redis_conn_decoded
from functions.prometheus.metric_name_labels_parser import metric_name_labels_parser
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric

retention_msecs = int(FULL_DURATION) * 1000


# @added 20220714 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
def populate_redis_timeseries(current_skyline_app, metric, log=True):
    """
    Return a metric time series as a list e.g.
    [[ts, value], [ts, value], ..., [ts, value]]

    :param current_skyline_app: the app calling the function
    :param metric_name: the full Redis metric name
    :param log: whether to log or not, optional, defaults to True
    :type current_skyline_app: str
    :type metric_name: str
    :type log: boolean
    :return: timeseries
    :rtype: list

    """

    function_str = 'functions.redis.populate_redis_timeseries'
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    labels = {}
    try:
        metric_dict = metric_name_labels_parser(current_skyline_app, metric)
        if metric_dict:
            labels = metric_dict['labels']
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to determine labels for %s - %s' % (
            function_str, metric, err))

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to connect to Redis to fetch time series for %s - %s' % (
            function_str, metric, err))

    until_timestamp = int(time())
    from_timestamp = until_timestamp - FULL_DURATION
    timeseries = []
    try:
        timeseries = get_victoriametrics_metric(
            current_skyline_app, metric, from_timestamp, until_timestamp, 'list',
            'object')
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: get_victoriametrics_metric failed for %s - %s' % (
            function_str, metric, err))

    if not timeseries:
        return

    metric_id = None
    try:
        metric_id = get_metric_id_from_base_name(current_skyline_app, metric)
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: get_metric_id_from_base_name failed for %s - %s' % (
            function_str, metric, err))

    if not metric_id:
        return

    redis_ts_key = 'labelled_metrics.%s' % str(metric_id)
    redis_timeseries_errors = []
    redis_timeseries_failed_to_insert = []
    inserted_count = 0
    try:
        for item in timeseries:
            value = item[1]
            timestamp = item[0]
            timestamp_ms = int(timestamp) * 1000
            inserted = 0
            try:
                inserted = redis_conn_decoded.ts().add(redis_ts_key, timestamp_ms, value, retention_msecs=retention_msecs, labels=labels, chunk_size=128, duplicate_policy='first')
            except Exception as err:
                if "TSDB: Couldn't parse LABELS" in str(err):
                    redis_timeseries_errors.append([item, {'labels': labels}, err])
                else:
                    redis_timeseries_errors.append(['insert', item, err])
                continue
            if not inserted:
                redis_timeseries_failed_to_insert.append(item)
            else:
                inserted_count += 1
    except Exception as err:
        redis_timeseries_errors.append(['process error', item, err])

    return timeseries
