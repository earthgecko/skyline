"""
append_memcache_key.py
"""
import logging

from functions.memcache.get_memcache_client import get_memcache_client


# @added 20220429 - Feature #4536: Handle Redis failure
# Add a global method to set a memcache key
def append_memcache_key(current_skyline_app, key, data, expire=0):
    """
    Return success.

    :param current_skyline_app: the app calling the function
    :param key: the key to set
    :param data: the key data
    :param expire: the key expiry time
    :type current_skyline_app: str
    :type key: str
    :type data: object
    :type expire: int
    :return: success
    :rtype: boolean

    """
    function_str = 'functions.memcache.append_memcache_key'
    success = False
    memcache_client = None
    try:
        memcache_client = get_memcache_client(current_skyline_app)
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.info('%s :: get_memcache_client failed - %s' % (
            function_str, err))
        memcache_client = None
    if memcache_client:
        current_data = None
        try:
            current_data = memcache_client.get(key)
        except:
            current_data = None
        if not current_data:
            try:
                success = memcache_client.set(key, data, expire)
            except Exception as err:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error('error :: %s :: failed to set %s - %s' % (
                    function_str, key, err))
        else:
            try:
                del current_data
                success = memcache_client.append(key, data, expire)
            except Exception as err:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error('error :: %s :: failed to append %s - %s' % (
                    function_str, key, err))
        try:
            memcache_client.close()
        except Exception as err:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: failed to close memcache_client - %s' % (
                function_str, err))
    return success
