"""
incr_memcache_key.py
"""
import logging

from functions.memcache.get_memcache_client import get_memcache_client


# @added 20220426 - Feature #4536: Handle Redis failure
# Add a global method to set a memcache key
def incr_memcache_key(current_skyline_app, key, value):
    """
    Return success.

    :param current_skyline_app: the app calling the function
    :param key: the key to increment
    :param value: the amount by which to increment the value
    :type current_skyline_app: str
    :type key: str
    :type value: int
    :return: success
    :rtype: boolean

    """
    function_str = 'functions.memcache.incr_memcache_key'
    success = True
    memcache_client = None
    try:
        memcache_client = get_memcache_client(current_skyline_app)
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.info('%s :: get_memcache_client failed - %s' % (
            function_str, err))
        memcache_client = None
        success = False
    if memcache_client:
        try:
            memcache_client.incr(key, value)
        except Exception as err:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: failed to incr %s - %s' % (
                function_str, key, err))
        try:
            memcache_client.close()
        except Exception as err:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: failed to close memcache_client - %s' % (
                function_str, err))

    return success
