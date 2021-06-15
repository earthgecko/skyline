import logging
import traceback

from skyline_functions import get_redis_conn_decoded


# @added 20210604 - Branch #1444: thunder
def get_top_level_namespaces(current_skyline_app, log=False):
    """
    Determine all top level parent namespaces and return the list.

    :param current_skyline_app: the app calling the function
    :param log: whether to log or not, optional, defaults to False
    :type current_skyline_app: str
    :type log: boolean
    :return: top_level_namespaces
    :rtype: list

    """

    top_level_namespaces = []
    function_str = 'functions.metrics.get_top_level_namespaces'
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.info('%s :: %s :: determining top level namespaces' % (
            current_skyline_app, function_str))
    else:
        current_logger = None

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: get_redis_conn_decoded failed - %s' % (
            current_skyline_app, function_str, e))
        return top_level_namespaces

    unique_base_names = []
    redis_key = 'aet.analyzer.unique_base_names'
    try:
        unique_base_names = list(redis_conn_decoded.smembers(redis_key))
        if unique_base_names:
            if log:
                current_logger.info('%s :: %s :: got %s unique_base_names' % (
                    current_skyline_app, function_str,
                    str(len(unique_base_names))))
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: failed to get Redis key %s - %s' % (
            current_skyline_app, function_str, redis_key, e))
    for base_name in unique_base_names:
        top_level_namespace = base_name.split('.')[0]
        if top_level_namespace:
            top_level_namespaces.append(top_level_namespace)
    if top_level_namespaces:
        top_level_namespaces = list(set(top_level_namespaces))
        if log:
            current_logger.info('%s :: %s :: returning %s top level namespaces' % (
                current_skyline_app, function_str,
                str(len(top_level_namespaces))))

    return top_level_namespaces
