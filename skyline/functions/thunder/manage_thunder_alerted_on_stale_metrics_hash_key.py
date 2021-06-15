import logging
import traceback
from time import time

from settings import FULL_DURATION
from skyline_functions import get_redis_conn_decoded


# @added 20210603 - Branch #1444: thunder
def manage_thunder_alerted_on_stale_metrics_hash_key(
        current_skyline_app, log=True):
    """
    Determine stale metrics in each top level namespace.

    :param current_skyline_app: the app calling the function
    :param log: whether to log or not, optional, defaults to True
    :type current_skyline_app: str
    :type log: boolean
    :return: (namespace_stale_metrics_dict, namespace_recovered_metrics_dict)
    :rtype: tuple

    """

    removed_from_hash = 0
    function_str = 'metrics_manager :: functions.thunder.manage_thunder_alerted_on_stale_metrics_hash_key'
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    hash_key = 'thunder.alerted_on.stale_metrics'

    if log:
        current_logger.info('%s :: pruning old entries that never recovered from Redis hash key %s' % (
            function_str, hash_key))

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get Redis connection - %s' % (
            function_str, e))
        return removed_from_hash
    thunder_alerted_on_stale_metrics_dict = {}
    try:
        thunder_alerted_on_stale_metrics_dict = redis_conn_decoded.hgetall(hash_key)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get Redis hash key %s - %s' % (
            function_str, hash_key, e))
    if not thunder_alerted_on_stale_metrics_dict:
        return removed_from_hash

    redis_set = 'aet.analyzer.unique_base_names'
    unique_base_names = []
    try:
        unique_base_names = list(redis_conn_decoded.smembers(redis_set))
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to generate a unique_base_names list from Redis set %s - %s' % (
            function_str, redis_set, e))
    if not unique_base_names:
        return removed_from_hash

    # Do not send stale alerts for any identified sparsely populated metrics
    full_duration_timestamp = int(time()) - FULL_DURATION
    alerted_on_metrics = list(thunder_alerted_on_stale_metrics_dict.keys())
    for base_name in alerted_on_metrics:
        try:
            timestamp = float(thunder_alerted_on_stale_metrics_dict[base_name])
            if timestamp < full_duration_timestamp:
                # If roomba has not pruned the metric yet, skip removing it
                if base_name in unique_base_names:
                    continue
                try:
                    redis_conn_decoded.hdel(hash_key, base_name)
                    removed_from_hash += 1
                except Exception as e:
                    if not log:
                        current_skyline_app_logger = current_skyline_app + 'Log'
                        current_logger = logging.getLogger(current_skyline_app_logger)
                    current_logger.error(traceback.format_exc())
                    current_logger.error('error :: %s :: failed to del %s from Redis hash key %s - %s' % (
                        function_str, base_name, hash_key, e))
        except Exception as e:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: manage %s from Redis hash key %s, breaking out of loop - %s' % (
                function_str, base_name, hash_key, e))
            break

    if log:
        current_logger.info('%s :: removed %s old entries from Redis hash key %s' % (
            function_str, str(removed_from_hash), hash_key))

    return removed_from_hash
