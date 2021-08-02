import logging
import traceback

from skyline_functions import get_redis_conn_decoded


# @added 20210603 - Branch #1444: thunder
def prune_metrics_timestamp_hash_key(
        current_skyline_app, hash_key, older_than_timestamp, log=True):
    """
    Remove any entries from a metrics timestamp hash key older than the
    timestamp passed.

    :param current_skyline_app: the app calling the function
    :param hash_key: the metric:timestamp style Redis hash key
    :param older_than_timestamp: the unix timestamp
    :param log: whether to log or not, optional, defaults to True
    :type current_skyline_app: str
    :type hash_key: str
    :type timestamp: int
    :type log: boolean
    :return: removed_count
    :rtype: int

    """

    removed_from_hash = 0
    function_str = 'metrics_manager :: functions.redis.prune_metrics_timestamp_hash_key'
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    if log:
        current_logger.info('%s :: pruning entries older than %s from Redis hash key %s' % (
            function_str, str(older_than_timestamp), hash_key))

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

    metrics_dict = {}
    try:
        metrics_dict = redis_conn_decoded.hgetall(hash_key)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get Redis hash key %s - %s' % (
            function_str, hash_key, e))

    if not metrics_dict:
        return removed_from_hash

    # Remove entries older_than_timestamp
    metrics = list(metrics_dict.keys())
    for metric in metrics:
        try:
            timestamp = float(metrics_dict[metric])
            if int(timestamp) < older_than_timestamp:
                try:
                    redis_conn_decoded.hdel(hash_key, metric)
                    removed_from_hash += 1
                except Exception as e:
                    if not log:
                        current_skyline_app_logger = current_skyline_app + 'Log'
                        current_logger = logging.getLogger(current_skyline_app_logger)
                    current_logger.error(traceback.format_exc())
                    current_logger.error('error :: %s :: failed to del %s from Redis hash key %s - %s' % (
                        function_str, metric, hash_key, e))
        except Exception as e:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: manage %s from Redis hash key %s, breaking out of loop - %s' % (
                function_str, metric, hash_key, e))
            break

    if log:
        current_logger.info('%s :: removed %s old entries from Redis hash key %s' % (
            function_str, str(removed_from_hash), hash_key))

    return removed_from_hash
