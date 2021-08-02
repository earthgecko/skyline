"""
external_settings_configs
"""
import logging
import traceback
import requests
import simplejson as json
from ast import literal_eval
from skyline_functions import get_redis_conn_decoded
import settings


# @added 20210601 - Feature #4000: EXTERNAL_SETTINGS
def manage_external_settings(current_skyline_app):
    """
    Return a concatenated external settings from :mod:`settings.EXTERNAL_SETTINGS`
    of any fetched external settings.

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :type current_skyline_app: str
    :return: (external_settings, external_from_cache)
    :rtype: (dict, boolean)

    """
    # Get the logger
    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    function_str = 'metrics_manager :: functions.settings.manage_external_settings'

    debug_get_external_settings = None

    # Set the default dicts to return
    external_settings = {}
    external_from_cache = None

    last_known_redis_key = 'skyline.last_known.external_settings'

    # Define the items that are expected in the external settings json
    EXTERNAL_SETTINGS_JSON_ITEMS = (
        'id', 'namespace', 'full_duration',
        'second_order_resolution_seconds', 'learn_full_duration_seconds',
    )

    OPTIONAL_EXTERNAL_ALERTS_JSON_ITEMS = (
        'retention_1_resolution_seconds', 'retention_1_period_seconds',
        'retention_2_resolution_seconds', 'retention_2_period_seconds',
        'flux_token', 'thunder_alert_endpoint', 'thunder_alert_token',
        'alert_on_no_data', 'alert_on_stale_metrics',
        'do_not_alert_on_stale_metrics',
    )

    try:
        EXTERNAL_SETTINGS = settings.EXTERNAL_SETTINGS.copy()
    except Exception as e:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to determine EXTERNAL_SETTINGS - %s' % (
            function_str, e))
        return (external_settings, external_from_cache)

    for external_settings_item in list(EXTERNAL_SETTINGS.keys()):
        endpoint = None
        try:
            endpoint = EXTERNAL_SETTINGS[external_settings_item]['endpoint']
        except Exception as e:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to determine endpoint for EXTERNAL_SETTINGS[\'%s\'] - %s' % (
                function_str, str(external_settings_item), e))
        if not endpoint:
            continue
        post_data = None
        try:
            post_data = EXTERNAL_SETTINGS[external_settings_item]['post_data']
        except Exception as e:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to determine post_data for EXTERNAL_SETTINGS[\'%s\'] - %s' % (
                function_str, str(external_settings_item), e))
        if not post_data:
            continue
        external_settings_dict = {}
        current_logger.info('%s :: fetching external settings from %s' % (
            function_str, str(endpoint)))
        try:
            header = {"content-type": "application/json"}
            r = requests.post(endpoint, data=json.dumps(post_data),
                              headers=header, timeout=10)
            external_settings_dict = r.json()
        except Exception as e:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: could not retrieve json from the url - %s - %s' % (
                function_str, str(endpoint), e))
            continue
        if not external_settings_dict:
            current_logger.error('error :: %s :: failed to retrieve json from the url - %s' % (
                function_str, str(endpoint)))

        if external_settings_dict:
            namespaces_list = []
            try:
                namespaces_list = external_settings_dict['data']['namespaces']
            except Exception as e:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: could not parse [\'data\'][\'namespaces\'] from json from url - %s - %s' % (
                    function_str, str(endpoint), e))
                continue
            for item in namespaces_list:
                try:
                    required_elements = True
                    for element in EXTERNAL_SETTINGS_JSON_ITEMS:
                        valid_element = False
                        try:
                            valid_element = item[element]
                        except Exception as e:
                            current_logger.error('error :: %s :: could not validate %s from json from url - %s - %s' % (
                                function_str, element, str(endpoint), e))
                            required_elements = False
                        if not valid_element:
                            required_elements = False
                            continue
                    if required_elements:
                        config_id = 'external-%s' % str(item['id'])
                        external_settings[config_id] = item
                except Exception as e:
                    current_logger.error(traceback.format_exc())
                    current_logger.error('error :: get_external_settings :: could not parse namespace element from json from url - %s - %s - %s' % (
                        str(endpoint), str(item), e))

    redis_conn_decoded = None
    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as e:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_external_settings :: failed to get decoded Redis connection - %s' % e)
        return (external_settings, external_from_cache)

    if not external_settings:
        try:
            external_settings_raw = redis_conn_decoded.get(last_known_redis_key)
            if external_settings_raw:
                external_settings = literal_eval(external_settings_raw)
                external_from_cache = True
        except Exception as e:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_external_settings :: failed to query Redis for %s - %s' % (
                last_known_redis_key, e))
        return (external_settings, external_from_cache)

    redis_key = 'skyline.external_settings'
    try:
        redis_conn_decoded.set(redis_key, str(external_settings))
    except Exception as e:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_external_settings :: failed to set Redis key %s - %s' % (
            redis_key, e))
    try:
        redis_conn_decoded.set(last_known_redis_key, str(external_settings))
    except Exception as e:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_external_settings :: failed to set Redis key %s - %s' % (
            last_known_redis_key, e))

    return (external_settings, external_from_cache)
