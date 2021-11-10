import logging
import traceback

from msgpack import Unpacker
from settings import FULL_NAMESPACE
from skyline_functions import (
    get_redis_conn, get_redis_conn_decoded, nonNegativeDerivative,
    sort_timeseries)


# @added 20211008 - Feature #4264: luminosity - cross_correlation_relationships
# Add a global method to get a metrics timeseries from Redis
def get_redis_metrics_timeseries(current_skyline_app, metrics, log=False):
    """
    Return a dict of metrics timeseries as lists e.g.
    {
        'base_name.1': [[ts, value], [ts, value], ..., [ts, value]],
        'base_name.2': [[ts, value], [ts, value], ..., [ts, value]]
    }

    :param current_skyline_app: the app calling the function
    :param metrics: a list of base_names or full Redis metric names
    :param log: whether to log or not, optional, defaults to False
    :type current_skyline_app: str
    :type metrics: list
    :type log: boolean
    :return: metrics_timeseries
    :rtype: dict

    """

    function_str = 'functions.redis.get_metrics_timeseries'
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    metrics_timeseries = {}
    try:
        redis_conn = get_redis_conn(current_skyline_app)
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: %s :: get_redis_conn failed - %s' % (
            current_skyline_app, function_str, str(err)))

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: %s :: get_redis_conn_decoded failed - %s' % (
            current_skyline_app, function_str, str(err)))

    assigned_metrics = []
    base_names = []
    for metric in metrics:
        if metric.startswith(FULL_NAMESPACE):
            metric_name = str(metric)
            base_name = metric.replace(FULL_NAMESPACE, '')
        else:
            metric_name = '%s%s' % (FULL_NAMESPACE, str(metric))
            base_name = str(metric)
        assigned_metrics.append(metric_name)
        base_names.append(base_name)
        metrics_timeseries[base_name] = {}

    derivative_metrics = []
    try:
        # @modified 20211012 - Feature #4280: aet.metrics_manager.derivative_metrics Redis hash
        # derivative_metrics = list(redis_conn_decoded.smembers('derivative_metrics'))
        derivative_metrics = list(redis_conn_decoded.smembers('aet.metrics_manager.derivative_metrics'))
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: failed to get derivative_metrics from Redis - %s' % (
            current_skyline_app, function_str, str(err)))

    raw_assigned = {}
    try:
        raw_assigned = redis_conn.mget(assigned_metrics)
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: failed to get raw_assigned from Redis - %s' % (
            current_skyline_app, function_str, str(err)))

    if raw_assigned:
        for index, metric_name in enumerate(assigned_metrics):
            timeseries = []
            try:
                raw_series = raw_assigned[index]
                if raw_series:
                    unpacker = Unpacker(use_list=False)
                    unpacker.feed(raw_series)
                    timeseries = list(unpacker)
            except Exception as err:
                if not log:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error('error :: %s :: %s :: failed to unpack %s timeseries - %s' % (
                    current_skyline_app, function_str, metric_name, str(err)))
                timeseries = []
            if timeseries:
                # Convert Redis ts floats to ints
                timeseries = [[int(ts), value] for ts, value in timeseries]
            if timeseries:
                # To ensure that there are no unordered timestamps in the time
                # series which are artefacts of the collector or carbon-relay, sort
                # all time series by timestamp before analysis.
                original_timeseries = timeseries
                if original_timeseries:
                    timeseries = sort_timeseries(original_timeseries)
                    del original_timeseries
                if metric_name in derivative_metrics:
                    if len(timeseries) > 3:
                        try:
                            derivative_timeseries = nonNegativeDerivative(timeseries)
                            timeseries = derivative_timeseries
                        except Exception as err:
                            if not log:
                                current_skyline_app_logger = current_skyline_app + 'Log'
                                current_logger = logging.getLogger(current_skyline_app_logger)
                            current_logger.error(traceback.format_exc())
                            current_logger.error('error :: %s :: %s :: nonNegativeDerivative failed on timeseries for %s - %s' % (
                                current_skyline_app, function_str, metric_name, str(err)))
            if timeseries:
                base_name = base_names[index]
                metrics_timeseries[base_name] = timeseries

    return metrics_timeseries
