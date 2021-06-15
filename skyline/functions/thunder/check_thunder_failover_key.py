import logging
from os import uname
from os.path import isfile
from time import time
from ast import literal_eval

import settings

from functions.filesystem.remove_file import remove_file

skyline_app = 'thunder'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

# The failover THUNDER keys directory which is failed over to and
# used in the event that Redis is down
THUNDER_KEYS_DIR = '%s/thunder/keys' % settings.SKYLINE_TMP_DIR

this_host = str(uname()[1])


# @added 20210520 - Branch #1444: thunder
def check_thunder_failover_key(self, check_key):
    """
    Determine if there is a failover alert key for an alert if Redis is down

    :param self: the self object
    :param check_key: the alert cache key name
    :type self: object
    :type check_key: str
    :return: expiry
    :rtype: int

    """

    function_str = 'functions.thunder.checks.check_thunder_failover_key'
    expiry = 0
    thunder_key_file = '%s/%s' % (THUNDER_KEYS_DIR, check_key)
    key_dict = {}
    if isfile(thunder_key_file):
        try:
            with open(thunder_key_file, 'r') as f:
                key_dict_str = f.read()
            key_dict = literal_eval(key_dict_str)
        except Exception as e:
            logger.error('error :: %s :: failed to open thunder_key_file: %s - %s' % (
                function_str, thunder_key_file, e))
    timestamp = 0
    if key_dict:
        try:
            timestamp = int(key_dict['timestamp'])
            expiry = int(key_dict['expiry'])
        except Exception as e:
            logger.error('error :: %s :: failed to determine timestamp and expiry from key_dict created from thunder_key_file: %s - %s' % (
                function_str, thunder_key_file, e))
    if timestamp:
        now = int(time())
        if (timestamp + expiry) >= now:
            expiry = 0
            try:
                removed_file = remove_file(thunder_key_file)
                if removed_file:
                    logger.info('%s :: removed expired thunder_key_file: %s' % (
                        function_str, thunder_key_file))
            except Exception as e:
                logger.error('error :: %s ::  failed to remove %s, continuing - %s' % (
                    function_str, thunder_key_file, e))
        if (timestamp + expiry) <= now:
            expiry = now - (timestamp + expiry)

    # Try and set in Redis and remove failover key if successful
    if expiry and timestamp:
        try:
            set_alert_cache_key = self.redis_conn.setex(check_key, expiry, timestamp)
            if set_alert_cache_key:
                logger.info('%s :: set Redis key %s with %s TTL' % (
                    function_str, check_key, str(expiry)))
                try:
                    removed_file = remove_file(thunder_key_file)
                    if removed_file:
                        logger.info('%s :: added thunder alert key to Redis so removed thunder_key_file: %s' % (
                            function_str, thunder_key_file))
                except Exception as e:
                    logger.error('error :: %s ::  failed to remove %s, continuing - %s' % (
                        function_str, thunder_key_file, e))
        except Exception as e:
            logger.warn('warning :: %s :: failed to set_alert_cache_key in Redis, probably still down - %s - %s' % (
                function_str, check_key, e))

    return expiry
