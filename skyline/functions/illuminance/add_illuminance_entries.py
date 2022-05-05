"""
add_illuminance_entries.py
"""
import logging
from time import time, strftime, gmtime
from ast import literal_eval


# @added 20220504 - Feature #2580: illuminance
def add_illuminance_entries(self, current_skyline_app, timestamp, illuminance_dict):
    """
    Updates the key in the Redis app illuminance hash and returns the
    illuminance_dict for the current time period key

    :param self: the self object
    :param current_skyline_app: the app calling the function
    :param timestamp: the timestamp to use as the hash key
    :param illuminance_dict: the illuminance_dict, which is a dict like, example
        {'test.metric.1': {'timestamp': 1651645359, 'value': 14.89}, 'test.metric.2': {'timestamp': 1651645379, 'value': 3.2}}
    :type self: object
    :type current_skyline_app: str
    :type timestamp: int
    :type illuminance_dict: dict
    :return: current_illuminance_dict
    :rtype: dict

    """
    skyline_app_logger = '%sLog' % current_skyline_app
    logger = logging.getLogger(skyline_app_logger)
    function_str = 'functions.illuminance.add_illuminance_entries'

    date_string = str(strftime('%Y-%m-%d', gmtime()))
    illuminance_redis_hash = '%s.illuminance.%s' % (current_skyline_app, date_string)

    try:
        hash_key = int(int(timestamp) // 60 * 60)
    except:
        hash_key = int(int(time()) // 60 * 60)

    logger.info('%s :: adding %s entries to %s key in %s' % (
        function_str, str(len(illuminance_dict)), str(hash_key),
        illuminance_redis_hash))

    current_illuminance_dict = {}
    try:
        current_illuminance_dict = self.redis_conn_decoded.hget(illuminance_redis_hash, hash_key)
    except Exception as err:
        logger.error('error :: %s :: failed to hget %s from %s - %s' % (
            function_str, str(hash_key), illuminance_redis_hash, err))

    if current_illuminance_dict:
        try:
            current_illuminance_dict = literal_eval(current_illuminance_dict)
        except Exception as err:
            logger.error('error :: failed to literal_eval current_illuminance_dict - %s' % (
                err))
            current_illuminance_dict = {}
    else:
        current_illuminance_dict = {}

    logger.info('%s :: got %s entries from %s key in %s' % (
        function_str, str(len(current_illuminance_dict)), str(hash_key), illuminance_redis_hash))

    for base_name in list(illuminance_dict.keys()):
        try:
            current_illuminance_dict[base_name] = illuminance_dict[base_name]
        except Exception as err:
            logger.error('error :: failed to add %s to current_illuminance_dict - %s' % (
                base_name, err))

    if current_illuminance_dict:
        try:
            self.redis_conn_decoded.hset(illuminance_redis_hash, hash_key, str(current_illuminance_dict))
            logger.info('%s :: set %s key in %s with new data with %s entries' % (
                function_str, str(hash_key), illuminance_redis_hash,
                str(len(current_illuminance_dict))))
        except Exception as err:
            logger.error('error :: %s :: failed to hset %s in %s - %s' % (
                function_str, str(hash_key), illuminance_redis_hash, err))
        try:
            self.redis_conn_decoded.expire(illuminance_redis_hash, (86400 * 90))
        except Exception as err:
            logger.error('error :: %s :: failed set expire on %s - %s' % (
                function_str, illuminance_redis_hash, err))

    return current_illuminance_dict
