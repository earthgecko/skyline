"""
get_memcache_key.py
"""
import logging
from ast import literal_eval

from functions.memcache.get_memcache_client import get_memcache_client


# @added 20220426 - Feature #4536: Handle Redis failure
# Add a global method to query mamcache for a key
def get_memcache_key(current_skyline_app, key):
    """
    Return the key data from memcache.

    :param current_skyline_app: the app calling the function
    :param key: the memcache key
    :type current_skyline_app: str
    :type key: str
    :return: data
    :rtype: object

    """
    function_str = 'functions.memcache.get_memcache_key'

    data = None

    memcache_client = None
    try:
        memcache_client = get_memcache_client(current_skyline_app)
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.info('%s :: get_memcache_client failed - %s' % (
            function_str, err))
        memcache_client = None

    raw_data = None
    if memcache_client:
        try:
            raw_data = memcache_client.get(key)
        except Exception as err:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.info('%s :: did not get %s from memcache - %s' % (
                function_str, key, err))
        try:
            memcache_client.close()
        except Exception as err:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: failed to close memcache_client - %s' % (function_str, err))
    if raw_data:
        try:
            data = literal_eval(raw_data.decode('utf-8'))
        except Exception as err:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            if 'malformed node or string' in str(err):
                current_logger.warning('warning :: %s :: failed to literal_eval the memcache key data for %s - %s' % (
                    function_str, key, err))
            else:
                current_logger.error('error :: %s :: failed to literal_eval the memcache key data for %s - %s' % (
                    function_str, key, err))
    return data
