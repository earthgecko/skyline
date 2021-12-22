import logging
import traceback


# @added 20210525 - Branch #1444: thunder
#                   Task #4030: refactoring
# Add a global method to check a Redis key
def check_redis_key(self, redis_key, log=True):
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

    if not self.redis_conn_decoded:
        if log:
            self.logger.error('error :: %s :: no self.redis_conn_decoded to get %s' % (
                function_str, redis_key))
        return data

    try:
        data = self.redis_conn_decoded.get(redis_key)
    except Exception as e:
        if log:
            self.logger.error(traceback.format_exc())
            self.logger.error('error :: %s :: failed to remove item from Redis set %s - %s' % (
                function_str, redis_key, e))
        data = None

    return data
