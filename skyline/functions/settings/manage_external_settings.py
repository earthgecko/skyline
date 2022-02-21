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
        # @added 20220210 - Feature #4412: flux - quota - thunder alert
        #                   Feature #4404: flux - external_settings - aggregation
        'aggregate', 'alert_on_metric_quota_exceeded',
        # @added 20220215 - Feature #4442: settings - LOCAL_EXTERNAL_SETTINGS
        'skip_metrics', 'do_not_skip_metrics', 'override',
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

        # @added 20220210 - Feature #4404: flux - external_settings - aggregation
        # Added missing enabled check
        enabled = True
        try:
            enabled = EXTERNAL_SETTINGS[external_settings_item]['enabled']
        except Exception as e:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to determine enabled for EXTERNAL_SETTINGS[\'%s\'] - %s' % (
                function_str, str(external_settings_item), e))
        if not enabled:
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

    # @added 20220215 - Feature #4442: settings - LOCAL_EXTERNAL_SETTINGS
    LOCAL_EXTERNAL_SETTINGS = {}
    try:
        LOCAL_EXTERNAL_SETTINGS = settings.LOCAL_EXTERNAL_SETTINGS.copy()
    except AttributeError:
        LOCAL_EXTERNAL_SETTINGS = {}
    except Exception as e:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to determine LOCAL_EXTERNAL_SETTINGS - %s' % (
            function_str, e))
        LOCAL_EXTERNAL_SETTINGS = {}
    if LOCAL_EXTERNAL_SETTINGS:
        external_settings_config_ids = list(external_settings.keys())
        for config_id in list(LOCAL_EXTERNAL_SETTINGS.keys()):
            local_external_settings_used = False
            if config_id in external_settings_config_ids:
                external_settings_config_id_present = True
            else:
                external_settings_config_id_present = False
                current_logger.info('%s :: LOCAL_EXTERNAL_SETTINGS exist for %s but it is not present in EXTERNAL_SETTINGS' % (
                    function_str, str(config_id)))
            override = False
            try:
                override = LOCAL_EXTERNAL_SETTINGS[config_id]['override']
            except KeyError:
                override = False
            except:
                override = False
            if not external_settings_config_id_present and override:
                current_logger.info('%s :: adding %s to external settings from LOCAL_EXTERNAL_SETTINGS' % (
                    function_str, str(config_id)))
                external_settings[config_id] = LOCAL_EXTERNAL_SETTINGS[config_id]
                external_settings[config_id]['LOCAL_EXTERNAL_SETTINGS'] = LOCAL_EXTERNAL_SETTINGS[config_id].copy()
                # @added 20220221 - Feature #4442: settings - LOCAL_EXTERNAL_SETTINGS
                # Add what was overriden
                external_settings[config_id]['overridden'] = {}
                continue
            # @added 20220221 - Feature #4442: settings - LOCAL_EXTERNAL_SETTINGS
            # Add what was overriden
            if override:
                external_settings[config_id]['overridden'] = {}

            for key in list(LOCAL_EXTERNAL_SETTINGS[config_id].keys()):
                key_present = False
                try:
                    key_value = external_settings[config_id][key]
                    key_value_str = str(key_value)
                    if isinstance(key_value_str, str):
                        key_present = True
                except KeyError:
                    key_present = False
                if not key_present and external_settings_config_id_present:
                    current_logger.info('%s :: adding %s to external settings %s with value from LOCAL_EXTERNAL_SETTINGS' % (
                        function_str, str(key), config_id))
                    external_settings[config_id][key] = LOCAL_EXTERNAL_SETTINGS[config_id][key]
                    local_external_settings_used = True
                if key_present and override:
                    # @added 20220221 - Feature #4442: settings - LOCAL_EXTERNAL_SETTINGS
                    # Add what was overriden
                    if isinstance(external_settings[config_id][key], dict):
                        external_settings[config_id]['overridden'][key] = external_settings[config_id][key].copy()
                    else:
                        external_settings[config_id]['overridden'][key] = external_settings[config_id][key]

                    current_logger.info('%s :: overriding %s in external settings %s with value from LOCAL_EXTERNAL_SETTINGS' % (
                        function_str, str(key), config_id))
                    external_settings[config_id][key] = LOCAL_EXTERNAL_SETTINGS[config_id][key]
                    local_external_settings_used = True
                if local_external_settings_used:
                    external_settings[config_id]['LOCAL_EXTERNAL_SETTINGS'] = LOCAL_EXTERNAL_SETTINGS[config_id].copy()

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
