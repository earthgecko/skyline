import logging
import traceback
from ast import literal_eval

from skyline_functions import get_redis_conn_decoded

LOCAL_DEBUG = False


# @added 20210601 - Feature #4000: EXTERNAL_SETTINGS
#                   Branch #1444: thunder
def get_external_settings(current_skyline_app, namespace=None, log=False):
    """
    Determine get the skyline.external_settings key and literal_eval it and
    return the dict.

    :param current_skyline_app: the app calling the function
    :param base_name: the metric base_name
    :param log: whether to log or not, optional, defaults to False
    :type current_skyline_app: str
    :type base_name: str
    :type log: boolean
    :return: stale_period
    :rtype: int

    """

    external_settings = {}
    function_str = 'functions.settings.get_external_settings'
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
        if log:
            current_logger.info('get_external_settings :: got redis_conn_decoded')
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_redis_conn_decoded failed - %s' % (
            function_str, e))
        return external_settings

    redis_key = 'skyline.external_settings'
    try:
        external_settings_raw = redis_conn_decoded.get(redis_key)
        if external_settings_raw:
            external_settings = literal_eval(external_settings_raw)
            if log:
                current_logger.info('get_external_settings :: got %s external_settings from Redis key %s' % (
                    str(len(external_settings)), redis_key))
        else:
            if log:
                current_logger.info('get_external_settings :: did not get any external_settings from Redis key %s' % (
                    redis_key))
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_external_settings :: failed to get Redis key %s - %s' % (
            redis_key, e))

    if namespace and external_settings:
        namespace_external_settings = {}
        for config_id in list(external_settings.keys()):
            try:
                if external_settings[config_id]['namespace'] == namespace:
                    namespace_external_settings[config_id] = external_settings[config_id]
                    if log:
                        current_logger.info('get_external_settings :: got %s external settings for %s from Redis key %s' % (
                            str(len(namespace_external_settings)), config_id,
                            redis_key))
            except Exception as e:
                if not log:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_external_settings :: failed to check namespace for external_settings[\'%s\'] - %s' % (
                    config_id, e))
        external_settings = namespace_external_settings

    return external_settings
