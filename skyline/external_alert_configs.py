"""
external_alert_configs
"""
import logging
import traceback
import requests
import simplejson as json
from ast import literal_eval
from skyline_functions import get_redis_conn_decoded
import settings


# @added 20200528 - Feature #3560: External alert config
def get_external_alert_configs(current_skyline_app):
    """
    Return concatenated alerts configs based on :mod:`settings.ALERTS` and any
    fetched external alert configs declared in :mod:`settings.ALERTS`

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :type current_skyline_app: str
    :return: [[alerter, namespace, ]]
    :rtype: list

    """
    debug_get_external_alert_configs = None
    external_alert_configs = {}

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        EXTERNAL_ALERTS = settings.EXTERNAL_ALERTS
        if debug_get_external_alert_configs:
            current_logger.debug('debug :: get_external_alert_configs settings.EXTERNAL_ALERTS is defined')
    except:
        return external_alert_configs

    redis_key = 'skyline.external_alert_configs'
    redis_conn_decoded = None
    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_external_alert_configs :: failed to get decoded Redis connection')
    raw_external_alert_configs = None
    if redis_conn_decoded:
        try:
            redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
            raw_external_alert_configs = redis_conn_decoded.get(redis_key)
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_external_alert_configs :: failed to query Redis for skyline.external_alert_configs')
    if raw_external_alert_configs:
        try:
            external_alert_configs = literal_eval(raw_external_alert_configs)
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_external_alert_configs :: failed to literal_eval skyline.external_alert_configs')
    if external_alert_configs:
        return external_alert_configs

    for external_alert_config in EXTERNAL_ALERTS:
        external_alert_config_url = None
        try:
            external_alert_config_url = EXTERNAL_ALERTS[external_alert_config]['url']
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_external_alert_configs :: could not determine url from EXTERNAL_ALERTS[\'%s\'][\'url\']' % (
                str(external_alert_config)))
            continue
        external_alert_config_method = None
        try:
            external_alert_config_method = EXTERNAL_ALERTS[external_alert_config]['method']
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_external_alert_configs :: could not determine url from EXTERNAL_ALERTS[\'%s\'][\'method\']' % (
                str(external_alert_config)))
            continue
        external_alert_config_post_data = None
        if external_alert_config_method == 'POST' or external_alert_config_method == 'post':
            try:
                external_alert_config_post_data = EXTERNAL_ALERTS[external_alert_config]['data']
            except:
                external_alert_config_post_data = None
        external_alert_json = None
        try:
            current_logger.info('get_external_alert_configs :: retrieving alert config json for %s from %s via %s' % (
                str(external_alert_config), str(external_alert_config_url),
                str(external_alert_config_method)))
            if external_alert_config_method == 'GET':
                r = requests.get(external_alert_config_url, timeout=2)
            if external_alert_config_method == 'POST':
                header = {"content-type": "application/json"}
                if external_alert_config_post_data:
                    r = requests.post(external_alert_config_url,
                                      data=json.dumps(external_alert_config_post_data),
                                      headers=header, timeout=2)
                else:
                    r = requests.post(external_alert_config_url, headers=header,
                                      timeout=2)
            external_alert_json = r.json()
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_external_alert_configs :: could not retrieve json from the url - %s' % str(external_alert_config_url))
            continue
        if not external_alert_json:
            current_logger.error('error :: get_external_alert_configs :: did not retrieve json from the url - %s' % str(external_alert_config_url))
            continue

        for alerter_id in external_alert_json['data']:
            try:
                alerter = external_alert_json['data'][alerter_id]['alerter']
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_external_alert_configs :: could not determine alerter from json - %s' % str(external_alert_json['data'][alerter_id]))
                continue
            try:
                namespace_prefix = external_alert_json['data'][alerter_id]['namespace_prefix']
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_external_alert_configs :: could not determine namespace_prefix from json - %s' % str(external_alert_json['data'][alerter_id]))
                continue
            try:
                namespace = external_alert_json['data'][alerter_id]['namespace']
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_external_alert_configs :: could not determine namespace from json - %s' % str(external_alert_json['data'][alerter_id]))
                continue
            try:
                if namespace_prefix == namespace:
                    full_namespace = namespace
                else:
                    full_namespace = '%s.%s' % (namespace_prefix, namespace)
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_external_alert_configs :: failed to interpolation full_namespace from namespace_prefix and namespace in the json - %s' % str(external_alert_json['data'][alerter_id]))
                continue
            try:
                expiration = int(external_alert_json['data'][alerter_id]['expiration'])
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_external_alert_configs :: could not determine expiration from json - %s' % str(external_alert_json['data'][alerter_id]))
                continue
            try:
                second_order_resolution = int(external_alert_json['data'][alerter_id]['second_order_resolution'])
                if second_order_resolution == settings.FULL_DURATION:
                    second_order_resolution_hours = None
                else:
                    second_order_resolution_hours = int(second_order_resolution / 3600)
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_external_alert_configs :: could not determine second_order_resolution from json - %s' % str(external_alert_json['data'][alerter_id]))
                continue
            try:
                learn_days = int(external_alert_json['data'][alerter_id]['learn_days'])
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_external_alert_configs :: could not determine learn_days from json - %s' % str(external_alert_json['data'][alerter_id]))
                continue
            alerter_config = {
                'alerter': alerter,
                'namespace': full_namespace,
                'expiration': expiration,
                'second_order_resolution_hours': second_order_resolution_hours,
                'learn_days': learn_days
            }
            external_alert_configs[alerter_id] = alerter_config

    if redis_conn_decoded and external_alert_configs:
        redis_conn_decoded.setex(redis_key, 900, str(external_alert_configs))

    return external_alert_configs
