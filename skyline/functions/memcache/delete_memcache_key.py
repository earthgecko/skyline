"""
delete_memcache_key.py
"""
import logging

from functions.memcache.get_memcache_client import get_memcache_client


# @added 20220426 - Feature #4536: Handle Redis failure
# Add a global method to query mamcache for a key
def delete_memcache_key(current_skyline_app, key):
    """
    Delete the key from memcache.

    :param current_skyline_app: the app calling the function
    :param key: the memcache key
    :type current_skyline_app: str
    :type key: str
    :return: success
    :rtype: boolean

    """
    function_str = 'functions.memcache.delete_memcache_key'

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
        try:
            success = memcache_client.delete(key)
        except Exception as err:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.info('%s :: failed to delete %s from memcache - %s' % (
                function_str, key, err))
        try:
            memcache_client.close()
        except Exception as err:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: failed to close memcache_client - %s' % (function_str, err))
    return success
