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
    Return a concatenated alerts configs from :mod:`settings.EXTERNAL_ALERTS` of
    any fetched external alert configs, a all_alerts list which is a concentated
    and deduplicated list of the and whether it was retrieved from cache
    or fetched source.

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :type current_skyline_app: str
    :return: (external_alert_configs, all_alerts, external_from_cache)
    :rtype: (dict, list, boolean)

    """
    debug_get_external_alert_configs = None

    # Set the default dicts to return
    external_alert_configs = {}
    # Set the default dict to return
    internal_alert_configs = {}
    # Set the default all_alerts to return
    all_alerts = list(settings.ALERTS)
    all_alert_configs = None
    # Set the default external_from_cache to return
    external_from_cache = None
    # Set the default internal_from_cache to return
    internal_from_cache = None
    # Set the default all_from_cache to return
    all_from_cache = None

    last_known_redis_key = 'skyline.last_known.external_alert_configs'

    # Get the logger
    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # Define the items that are expected in the external alert config json
    EXTERNAL_ALERTS_JSON_ITEMS = (
        'alerter', 'expiration', 'namespace', 'namespace_prefix',
        'second_order_resolution', 'second_order_resolution_hours', 'learn_days',
        'inactive_after'
    )
    OPTIONAL_EXTERNAL_ALERTS_JSON_ITEMS = (
        'namespace_prefix', 'second_order_resolution_hours', 'learn_days',
        'inactive_after'
    )

    try:
        EXTERNAL_ALERTS = settings.EXTERNAL_ALERTS
        if debug_get_external_alert_configs:
            current_logger.debug('debug :: get_external_alert_configs settings.EXTERNAL_ALERTS is defined')
    except:
        return (external_alert_configs, external_from_cache, internal_alert_configs, internal_from_cache, tuple(all_alerts), all_from_cache)

    redis_conn_decoded = None
    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_external_alert_configs :: failed to get decoded Redis connection')

    # The all_alert_configs Redis key is cached for 60 seconds, if found return
    # as it is all that is needed
    redis_key = 'skyline.all_alert_configs'
    raw_all_alert_configs = None
    if redis_conn_decoded:
        try:
            raw_all_alert_configs = redis_conn_decoded.get(redis_key)
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_external_alert_configs :: failed to query Redis for skyline.all_alert_configs')
    if raw_all_alert_configs:
        try:
            all_alert_configs = literal_eval(raw_all_alert_configs)
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_external_alert_configs :: failed to literal_eval skyline.all_alert_configs')
    if all_alert_configs:
        # Set that the external_alert_config was fetched from cache
        all_from_cache = True
        return (external_alert_configs, external_from_cache, internal_alert_configs, internal_from_cache, all_alert_configs, all_from_cache)

    redis_key = 'skyline.external_alert_configs'
    raw_external_alert_configs = None
    if redis_conn_decoded:
        try:
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
        # Set that the external_alert_config was fetched from cache
        external_from_cache = True
        if redis_conn_decoded:
            try:
                redis_conn_decoded.set(last_known_redis_key, str(external_alert_configs))
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_external_alert_configs :: failed to set %s Redis key' % last_known_redis_key)

    redis_key = 'skyline.internal_alert_configs'
    raw_internal_alert_configs = None
    if redis_conn_decoded:
        try:
            raw_internal_alert_configs = redis_conn_decoded.get(redis_key)
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_external_alert_configs :: failed to query Redis for skyline.internal_alert_configs')
    if raw_internal_alert_configs:
        try:
            internal_alert_configs = literal_eval(raw_internal_alert_configs)
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_external_alert_configs :: failed to literal_eval skyline.internal_alert_configs')
    if internal_alert_configs:
        # Set that the external_alert_config was fetched from cache
        internal_from_cache = True

    # If the external_alert_config was not fectched from cache build it
    if not external_alert_configs:
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
                    r = requests.get(external_alert_config_url, timeout=10)
                if external_alert_config_method == 'POST':
                    header = {"content-type": "application/json"}
                    if external_alert_config_post_data:
                        r = requests.post(external_alert_config_url,
                                          data=json.dumps(external_alert_config_post_data),
                                          headers=header, timeout=10)
                    else:
                        r = requests.post(external_alert_config_url, headers=header,
                                          timeout=10)
                external_alert_json = r.json()
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_external_alert_configs :: could not retrieve json from the url - %s' % str(external_alert_config_url))
                continue
            if not external_alert_json:
                current_logger.error('error :: get_external_alert_configs :: did not retrieve json from the url - %s' % str(external_alert_config_url))
                continue

            for alerter_id in external_alert_json['data']:
                config_id = 'external-%s' % str(alerter_id)
                alerter_config = {'id': config_id}
                namespace_prefix = None
                namespace = None
                for key in EXTERNAL_ALERTS_JSON_ITEMS:
                    try:
                        if key == 'namespace_prefix':
                            try:
                                namespace_prefix = external_alert_json['data'][alerter_id][key]
                            except:
                                namespace_prefix = None
                        elif key == 'namespace':
                            namespace = external_alert_json['data'][alerter_id][key]
                        else:
                            alerter_config[key] = external_alert_json['data'][alerter_id][key]
                    except:
                        if key in OPTIONAL_EXTERNAL_ALERTS_JSON_ITEMS:
                            if key == 'inactive_after':
                                alerter_config[key] = 7200
                            continue
                        else:
                            current_logger.error(traceback.format_exc())
                            current_logger.error('error :: get_external_alert_configs :: could not determine %s from json - %s' % (
                                key, str(alerter_id)))
                            alerter_config = {}
                            break
                if alerter_config:
                    try:
                        if namespace_prefix == namespace:
                            full_namespace_str = namespace
                        else:
                            if namespace_prefix is None:
                                full_namespace_str = namespace
                            else:
                                full_namespace_str = '%s.%s' % (namespace_prefix, namespace)
                            full_namespace = full_namespace_str.replace(',', '.')
                        alerter_config['namespace'] = full_namespace
                    except:
                        current_logger.error(traceback.format_exc())
                        current_logger.error('error :: get_external_alert_configs :: failed to interpolation full_namespace from namespace_prefix and namespace in the json - %s' % str(external_alert_json['data'][alerter_id]))
                        continue
                    try:
                        alerter_config['type'] = 'external'
                    except:
                        current_logger.error(traceback.format_exc())
                        current_logger.error('error :: get_external_alert_configs :: failed to add type external to alerter_config')
                        continue
                    try:
                        external_alert_configs[alerter_id] = alerter_config
                    except:
                        current_logger.error(traceback.format_exc())
                        current_logger.error('error :: get_external_alert_configs :: could not add alert_config dict to external_alert_configs dict from json - %s' % str(external_alert_json['data'][alerter_id]))
                        continue

    # If the key expired and no alerter_configs were constructed from the
    # external source then use the last known good external_alert_configs
    last_good_external_alert_configs = None
    if not external_alert_configs:
        if redis_conn_decoded:
            last_good_raw_external_alert_configs = None
            try:
                last_good_raw_external_alert_configs = redis_conn_decoded.get(last_known_redis_key)
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_external_alert_configs :: failed to query Redis for %s' % last_known_redis_key)
            last_good_external_alert_configs = None
            if last_good_raw_external_alert_configs:
                try:
                    last_good_external_alert_configs = literal_eval(last_good_raw_external_alert_configs)
                except:
                    current_logger.error(traceback.format_exc())
                    current_logger.error('error :: get_external_alert_configs :: failed to literal_eval skyline.last_known.external_alert_configs')
            if last_good_external_alert_configs:
                current_logger.info('get_external_alert_configs :: failed to construct the external_alert_configs using skyline.last_known.external_alert_configs')
                external_alert_configs = last_good_external_alert_configs
                external_from_cache = True

    # Build the all_alerts list by contenating the external_alert_configs
    new_all_alerts = []

    if external_alert_configs:
        # external smtp alerts
        # All set to no_email in analyzer and mirage_alerters as every alert
        # must be routed through the smtp workflow, even if it does not send a
        # smtp alert, as the smtp alert route creates the the training data
        # resources.
        for external_alert_config in external_alert_configs:
            config_id = None
            namespace = None
            expiration = None
            second_order_resolution = None
            second_order_resolution_hours = None
            try:
                config_id = external_alert_configs[external_alert_config]['id']
            except:
                continue
            try:
                namespace = external_alert_configs[external_alert_config]['namespace']
            except:
                continue
            try:
                expiration = int(external_alert_configs[external_alert_config]['expiration'])
            except:
                continue
            try:
                second_order_resolution = int(external_alert_configs[external_alert_config]['second_order_resolution'])
                second_order_resolution_hours = int(second_order_resolution / 3600)
            except:
                continue

            # First add an smtp no_email alerter for the external_alert_config
            # this is required to route anomalies through the training_data
            # resources creation workflow
            # alert = ('metric5.thing.*.rpm', 'smtp', 900, 168),
            new_all_alerts.append([namespace, 'smtp', expiration, second_order_resolution_hours, external_alert_configs[external_alert_config]])

    # internal smtp alerts
    for index, alert in enumerate(settings.ALERTS):
        # alert = ('metric5.thing.*.rpm', 'smtp', 900, 168),
        if str(alert[1]) == 'smtp':
            try:
                second_order_resolution_hours = int(alert[3])
                second_order_resolution = second_order_resolution_hours * 3600
            except:
                second_order_resolution = 0
                # @added 20211128 - Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
                # Added missing second_order_resolution_hours
                second_order_resolution_hours = 0
            config_id = 'internal-%s' % str(index)
            internal_alert_config = {
                'id': config_id,
                'alerter': alert[1],
                'namespace': alert[0],
                'expiration': alert[2],
                'second_order_resolution': second_order_resolution,
                'inactive_after': 7200,
                'type': 'internal'
            }
            new_all_alerts.append([alert[0], alert[1], alert[2], second_order_resolution_hours, internal_alert_config])
            try:
                internal_alert_configs[index] = internal_alert_config
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_external_alert_configs :: could not add internal_alert_config dict to internal_alert_configs dict')
                continue

    # external alerts - non-smtp
    if external_alert_configs:
        for external_alert_config in external_alert_configs:
            config_id = None
            alerter = None
            namespace = None
            expiration = None
            second_order_resolution = None
            second_order_resolution_hours = 0
            try:
                config_id = external_alert_configs[external_alert_config]['id']
            except:
                continue
            try:
                alerter = external_alert_configs[external_alert_config]['alerter']
            except:
                continue
            try:
                namespace = external_alert_configs[external_alert_config]['namespace']
            except:
                continue
            try:
                expiration = int(external_alert_configs[external_alert_config]['expiration'])
            except:
                continue
            try:
                second_order_resolution = int(external_alert_configs[external_alert_config]['second_order_resolution'])
                second_order_resolution_hours = int(second_order_resolution / 3600)
            except:
                continue

            # First add an smtp no_email alerter for the external_alert_config
            # this is required to route anomalies through the training_data
            # resources creation workflow
            # alert = ('metric5.thing.*.rpm', 'smtp', 900, 168),
            new_all_alerts.append([namespace, alerter, expiration, second_order_resolution_hours, external_alert_configs[external_alert_config]])

    # internal non smtp alerts
    for index, alert in enumerate(settings.ALERTS):
        # alert = ('metric5.thing.*.rpm', 'smtp', 900, 168),
        if str(alert[1]) != 'smtp':
            try:
                second_order_resolution_hours = int(alert[3])
                second_order_resolution = second_order_resolution_hours * 3600
            except:
                second_order_resolution_hours = 0
            config_id = 'internal-%s' % str(index)
            internal_alert_config = {
                'id': config_id,
                'alerter': alert[1],
                'namespace': alert[0],
                'expiration': alert[2],
                'second_order_resolution': second_order_resolution,
                'inactive_after': 7200,
                'type': 'internal'
            }
            new_all_alerts.append([alert[0], alert[1], alert[2], second_order_resolution_hours, internal_alert_config])
            try:
                internal_alert_configs[index] = internal_alert_config
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_external_alert_configs :: could not add internal_alert_config dict to internal_alert_configs dict')
                continue

    if new_all_alerts:
        all_alerts = tuple(new_all_alerts)

    if redis_conn_decoded and external_alert_configs:
        if not external_from_cache:
            redis_key = 'skyline.external_alert_configs'
            try:
                redis_conn_decoded.setex(redis_key, 300, str(external_alert_configs))
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_external_alert_configs :: failed to set %s' % redis_key)
    if redis_conn_decoded and internal_alert_configs:
        if not internal_from_cache:
            redis_key = 'skyline.internal_alert_configs'
            try:
                redis_conn_decoded.setex(redis_key, 60, str(internal_alert_configs))
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_external_alert_configs :: failed to set %s' % redis_key)
    if redis_conn_decoded and all_alerts:
        if not all_from_cache:
            redis_key = 'skyline.all_alert_configs'
            try:
                redis_conn_decoded.setex(redis_key, 60, str(all_alerts))
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_external_alert_configs :: failed to set %s' % redis_key)

    return (external_alert_configs, external_from_cache, internal_alert_configs, internal_from_cache, all_alerts, all_from_cache)
