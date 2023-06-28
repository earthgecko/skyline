"""
get_metric_redistimeseries.py
"""
import logging
from time import time

from settings import FULL_DURATION
from skyline_functions import get_redis_conn_decoded, nonNegativeDerivative
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.metrics.get_labelled_metric_type import get_labelled_metric_type


# @added 20220808 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
# Handle labelled_metrics.
def get_metric_redistimeseries(current_skyline_app, metric, from_timestamp, until_timestamp, log=False):
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

    function_str = 'functions.redis.get_metric_redistimeseries'
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    timeseries = []
    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to connect to Redis to fetch time series for %s - %s' % (
            function_str, metric, e))

    use_metric = str(metric)
    metric_id = 0

    if metric.startswith('labelled_metrics.'):
        metric_id_str = metric.replace('labelled_metrics.', '', 1)
        try:
            metric_id = int(metric_id_str)
        except:
            pass

    if '_tenant_id="' in metric:
        try:
            metric_id = get_metric_id_from_base_name(current_skyline_app, metric)
            if metric_id:
                use_metric = 'labelled_metrics.%s' % str(metric_id)
        except Exception as err:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: get_metric_id_from_base_name failed for %s - %s' % (
                function_str, metric, err))
            return timeseries

    if not until_timestamp:
        until_timestamp = int(time())
    if not from_timestamp:
        from_timestamp = until_timestamp - int(FULL_DURATION)

    try:
        timeseries = redis_conn_decoded.ts().range(use_metric, (from_timestamp * 1000), (until_timestamp * 1000))
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to get %s timeseries from Redis - %s' % (
            function_str, use_metric, err))
        return timeseries
    if timeseries:
        # Convert Redis millisecond timestamps to second timestamps
        timeseries = [[int(mts / 1000), value] for mts, value in timeseries]
    metric_type = None
    if timeseries:
        try:
            metric_type = get_labelled_metric_type(current_skyline_app, use_metric)
        except Exception as err:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: get_labelled_metric_type failed for %s - %s' % (
                function_str, use_metric, err))
    if metric_type == 'COUNTER':
        try:
            derivative_timeseries = nonNegativeDerivative(timeseries)
            timeseries = derivative_timeseries
        except Exception as err:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: nonNegativeDerivative failed - %s' % (
                function_str, err))

    return timeseries
