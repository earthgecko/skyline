import logging
import traceback

from skyline_functions import get_redis_conn_decoded


# @added 20210525 - Branch #1444: thunder
#                   Task #4030: refactoring
# Add a global method to check a Redis key
def check_redis_key(current_skyline_app, redis_key, log=True):
    """
    Check a Redis key.

    :param current_skyline_app: the app calling the function
    :param redis_key: the Redis key name
    :param log: whether to log or not, optional, defaults to True
    :type current_skyline_app: str
    :type redis_key: str
    :type log: boolean
    :return: data
    :rtype: object

    """

    function_str = 'functions.redis.check_redis_key'
    data = None
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    redis_conn_decoded = None
    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as e:
        if log:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to connect to Redis to get %s - %s' % (
                function_str, redis_key, e))

    if not redis_conn_decoded:
        return data

    try:
        data = redis_conn_decoded.get(redis_key)
    except Exception as e:
        if log:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to remove item from Redis set %s - %s' % (
                function_str, redis_key, e))
        data = None

    return data
