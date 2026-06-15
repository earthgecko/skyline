"""
redis_rename_key.py
"""
import logging
import traceback

from skyline_functions import get_redis_conn_decoded


# @added 20231223 - Task #5188: Optimise redis renames
#                   Task #5178: Build and test skyline v4.1.0
# Add a global method to rename a Redis key, if it exists.  Historically a
# rename would be wrapped in a try except and the key was renamed if it existed
# otherwise it would be handled by pass.  With the new Errorstats and
# cmdstat_method rejected_calls and falied_calls metrics being added to Redis,
# this try except pass methods results in Redis reporting failed_calls on many
# renames.  To mitigate this and make these new Redis metrics valuable, an
# exist is first called and only if it exists is the rename called
def redis_rename_key(
        current_skyline_app, current_redis_key, new_redis_key, log=False):
    """
    Rename a Redis key if it exists.

    :param current_skyline_app: the app calling the function
    :param current_redis_key: the Redis key name to rename
    :param new_redis_key: the new name of the Redis key
    :param log: whether to log or not, optional, defaults to False
    :type current_skyline_app: str
    :type current_redis_key: str
    :type new_redis_key: str
    :type log: boolean

    """

    function_str = 'functions.redis.redis_rename_key'
    renamed = False
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to connect to Redis to rename Redis set %s, err: %s' % (
            function_str, current_redis_key, err))
    exists = False
    try:
        exists = redis_conn_decoded.exists(current_redis_key)
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: exists failed on Redis key %s, err: %s' % (
            function_str, current_redis_key, err))
    if exists:
        if log:
            current_logger.info('%s :: Redis key %s exists, renaming' % (
                function_str, current_redis_key))
        try:
            renamed = redis_conn_decoded.rename(current_redis_key, new_redis_key)
            if log:
                current_logger.info('%s :: renamed Redis key %s to %s' % (
                    function_str, current_redis_key, new_redis_key))
        except Exception as err:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to renamed Redis key %s to %s, err: %s' % (
                function_str, current_redis_key, new_redis_key, err))
    else:
        if log:
            current_logger.info('%s :: Redis key %s does not exists, not renaming' % (
                function_str, current_redis_key))

    return renamed
