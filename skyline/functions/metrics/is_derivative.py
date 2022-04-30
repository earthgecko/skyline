"""
is_derivative.py
"""
import logging
import traceback

from settings import FULL_NAMESPACE
from skyline_functions import get_redis_conn_decoded


# @added 20220406 - Feature #4520: settings - ZERO_FILL_NAMESPACES
#                   Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
def is_derivative(current_skyline_app, metric):
    """
    Returns a dict of base_names that are no longer active in the data pipeline.

    :param current_skyline_app: the app calling the function
    :param namespace: the namespace to access for inactive metrics
    :type current_skyline_app: str
    :type namespace: str
    :return: inactive_metrics
    :rtype: list

    """
    function_str = 'functions.metrics.is_derivative'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as e:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: get_redis_conn_decoded failed - %s' % (
            current_skyline_app, function_str, e))
        return False

    derivative_metrics = []
    try:
        derivative_metrics = list(redis_conn_decoded.smembers('aet.metrics_manager.derivative_metrics'))
    except Exception as e:
        current_logger.error('error :: %s :: failed to connect to Redis for smembers of derivative_metrics - %s' % (
            function_str, e))
        derivative_metrics = []

    if not metric.startswith(FULL_NAMESPACE):
        metric = '%s%s' % (FULL_NAMESPACE, metric)

    if metric in derivative_metrics:
        return True

    return False
