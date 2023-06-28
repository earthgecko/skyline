"""
non_derivative_metrics_list
"""
import logging
import traceback

from skyline_functions import get_redis_conn_decoded


# @added 20220406 - Feature #4520: settings - ZERO_FILL_NAMESPACES
def non_derivative_metrics_list(current_skyline_app):
    """
    Returns a list of base_names that are metrics that are user defined as non
    derivative metrics.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: non_derivative_metrics_list
    :rtype: list

    """
    function_str = 'functions.metrics.non_derivative_metrics_list'
    non_derivative_metrics = []

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_redis_conn_decoded failed - %s' % (
            function_str, err))
        return non_derivative_metrics_list

    try:
        non_derivative_metrics = list(redis_conn_decoded.smembers('metrics_manager.non_derivative_metrics'))
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to connect to Redis for smembers of metrics_manager.non_derivative_metrics - %s' % (
            function_str, err))

    return non_derivative_metrics
