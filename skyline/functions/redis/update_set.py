import logging
import traceback

from skyline_functions import get_redis_conn

LOCAL_DEBUG = False


# @added 20210518 - Branch #1444: thunder
#                   Branch #3068: SNAB
#                   Task #3748: POC SNAB
#                   Task #4030: refactoring
# Add a global method to update or remove data from a Redis set
def update_redis_set(
        current_skyline_app, redis_set, original_data_str, update_data_str,
        log=True):
    """
    Manage data in a Redis set.

    :param current_skyline_app: the app calling the function
    :param redis_set: the Redis key name of the set
    :param original_data: the data in the set which to take action on
    :param update_data_str: the updated data or the string 'remove' to remove
        the data from the set.
    :param log: whether to log or not, optional, defaults to True
    :type current_skyline_app: str
    :type redis_set: str
    :type original_data: str
    :type update_data_str: str
    :type log: boolean

    """

    function_str = 'functions.redis.update_set'

    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    try:
        redis_conn = get_redis_conn(current_skyline_app)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to connect to Redis to manage data in Redis set %s - %s' % (
            function_str, redis_set, e))
    try:
        redis_conn.srem(redis_set, str(original_data_str))
        # @added 20220110 - Bug #4364: Prune old thunder.events
        #                   Branch #1444: thunder
        if log:
            current_logger.info('removed item from Redis set %s - %s' % (
                redis_set, str(original_data_str)))
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to remove item from Redis set %s - %s' % (
            function_str, redis_set, e))
    if update_data_str != 'remove':
        try:
            redis_conn.sadd(redis_set, str(update_data_str))
            # @added 20220110 - Bug #4364: Prune old thunder.events
            #                   Branch #1444: thunder
            if log:
                current_logger.info('added updated item to Redis set %s - %s' % (
                    redis_set, str(update_data_str)))
        except Exception as e:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to update item in Redis set %s - %s' % (
                function_str, redis_set, e))
    return
