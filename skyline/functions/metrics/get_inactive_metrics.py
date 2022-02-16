"""
get_inactive_metrics.py
"""
import logging
import traceback

from skyline_functions import get_redis_conn_decoded
from functions.metrics.get_base_names_and_metric_ids import get_base_names_and_metric_ids
from matched_or_regexed_in_list import matched_or_regexed_in_list


# @added 20220216 - Feature #4444: webapp - inactive_metrics
def get_inactive_metrics(current_skyline_app, namespace=None):
    """
    Returns a dict of base_names that are no longer active in the data pipeline.

    :param current_skyline_app: the app calling the function
    :param namespace: the namespace to access for inactive metrics
    :type current_skyline_app: str
    :type namespace: str
    :return: inactive_metrics
    :rtype: list

    """
    function_str = 'functions.metrics.get_inactive_metrics'
    inactive_metrics = []

    base_names_with_ids = {}
    try:
        base_names_with_ids = get_base_names_and_metric_ids(current_skyline_app)
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: get_base_names_and_metric_ids failed - %s' % (
            current_skyline_app, function_str, str(err)))
        return inactive_metrics

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as e:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: get_redis_conn_decoded failed - %s' % (
            current_skyline_app, function_str, e))
        return inactive_metrics

    unique_base_names = []
    try:
        unique_base_names = list(redis_conn_decoded.smembers('aet.analyzer.unique_base_names'))
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: get_redis_conn_decoded failed - %s' % (
            current_skyline_app, function_str, str(err)))
        return inactive_metrics

    pattern_match_errors = []
    for base_name in list(base_names_with_ids.keys()):
        if base_name in unique_base_names:
            continue
        if namespace:
            pattern_match = False
            try:
                pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, base_name, [namespace])
                del metric_matched_by
            except Exception as err:
                pattern_match_errors.append([base_name, namespace, err])
            if not pattern_match:
                continue
        inactive_metrics.append(base_name)

    return inactive_metrics
