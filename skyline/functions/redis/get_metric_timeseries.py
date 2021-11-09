import logging

from msgpack import Unpacker
from settings import FULL_NAMESPACE
from skyline_functions import (
    get_redis_conn, get_redis_conn_decoded, nonNegativeDerivative)


# @added 20210525 - Branch #1444: thunder
#                   Task #4030: refactoring
# Add a global method to get a metric timeseries from Redis
def get_metric_timeseries(current_skyline_app, metric_name, log=True):
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

    function_str = 'functions.redis.get_metric_timeseries'
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    timeseries = []
    try:
        redis_conn = get_redis_conn(current_skyline_app)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to connect to Redis to fetch time series for %s - %s' % (
            function_str, metric_name, e))

    if metric_name.startswith(FULL_NAMESPACE):
        metric_name = str(metric_name)
    else:
        metric_name = '%s%s' % (FULL_NAMESPACE, str(metric_name))

    raw_series = None
    try:
        raw_series = redis_conn.get(metric_name)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: failed to get %s from Redis - %s' % (metric_name, e))
        raw_series = None

    if not raw_series:
        return timeseries

    try:
        unpacker = Unpacker(use_list=False)
        unpacker.feed(raw_series)
        timeseries = list(unpacker)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: failed to unpack %s time series from Redis data - %s' % (metric_name, e))
        timeseries = []

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to connect to Redis to get derivative_metrics - %s' % (
            function_str, e))
    derivative_metrics = []
    try:
        # @modified 20211012 - Feature #4280: aet.metrics_manager.derivative_metrics Redis hash
        # derivative_metrics = list(redis_conn_decoded.smembers('derivative_metrics'))
        derivative_metrics = list(redis_conn_decoded.smembers('aet.metrics_manager.derivative_metrics'))
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to connect to Redis for smembers of derivative_metrics - %s' % (
            function_str, e))
        derivative_metrics = []
    if metric_name in derivative_metrics:
        if len(timeseries) > 3:
            try:
                derivative_timeseries = nonNegativeDerivative(timeseries)
                timeseries = derivative_timeseries
            except Exception as e:
                if not log:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error('error :: %s :: nonNegativeDerivative failed - %s' % (
                    function_str, e))

    return timeseries
