"""
get_memcache_client.py
"""
import logging

from pymemcache.client.base import Client as pymemcache_Client

MEMCACHE_ENABLED = False
try:
    from settings import MEMCACHE_ENABLED
except:
    MEMCACHE_ENABLED = False

MEMCACHED_SERVER_IP = None
try:
    from settings import MEMCACHED_SERVER_IP
except:
    MEMCACHED_SERVER_IP = None
MEMCACHED_SERVER_PORT = 0
try:
    from settings import MEMCACHED_SERVER_PORT
except:
    MEMCACHED_SERVER_PORT = 0


# @added 20220426 - Feature #4536: Handle Redis failure
# Add a global method to get a memcache client
def get_memcache_client(current_skyline_app):
    """
    Return memcache client.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: memcache_client
    :rtype: object

    """
    function_str = 'functions.memcache.get_memcache_client'
    memcache_client = None
    if MEMCACHE_ENABLED and MEMCACHED_SERVER_IP and MEMCACHED_SERVER_PORT:
        try:
            memcache_client = pymemcache_Client((MEMCACHED_SERVER_IP, MEMCACHED_SERVER_PORT), connect_timeout=0.1, timeout=0.2)
        except Exception as err:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.info('%s :: pymemcache_Client failed - %s' % (
                function_str, err))
    else:
        memcache_client = None

    return memcache_client
