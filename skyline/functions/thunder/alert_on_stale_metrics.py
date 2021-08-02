import logging
import traceback
from time import time
from os import uname

import settings
from functions.settings.get_external_settings import get_external_settings
from thunder_alerters import thunder_alert

skyline_app = 'thunder'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
this_host = str(uname()[1])


# @added 20210601 - Branch #1444: thunder
def alert_on_stale_metrics(
        self, level, message, parent_namespace, stale_metrics, data):
    """
    Send a single thunder alert for all the stale metrics in the period for a
    top level namespace.

    :param self: the self object
    :param level: the alert level
    :param parent_namespace: the parent_namespace
    :param stale_metrics: list of metrics in the namespace that are stale
    :param data: the thunder event data dict
    :type self: object
    :type parent_namespace: str
    :type stale_metrics: list
    :type data: dict
    :return: success
    :rtype: boolean

    """

    function_str = 'functions.thunder.alert_on_stale_metrics'
    alerts_sent_dict = {}

    metrics_to_alert_on = []
    # Get all alerted on stale metrics
    alerted_on_stale_metrics_hash_key = 'thunder.alerted_on.stale_metrics'
    try:
        alerted_on_stale_metrics_dict = self.redis_conn_decoded.hgetall(alerted_on_stale_metrics_hash_key)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to get Redis hash key %s - %s' % (
            function_str, alerted_on_stale_metrics_hash_key, e))
    alerted_on_stale_metrics = []
    if alerted_on_stale_metrics_dict and level == 'alert':
        alerted_on_stale_metrics = list(alerted_on_stale_metrics_dict.keys())
    for metric in stale_metrics:
        if metric in alerted_on_stale_metrics:
            continue
        metrics_to_alert_on.append(metric)

    if not metrics_to_alert_on:
        logger.info('%s :: all %s stale metrics have already been alerted on' % (
            function_str, str(len(stale_metrics))))
        alerts_sent_dict['all_sent'] = True
        return alerts_sent_dict

    alert_vias = []
    alert_via_smtp = True
    alert_via_slack = False
    alert_via_pagerduty = False
    try:
        alert_via_smtp = settings.THUNDER_OPTS['alert_via_smtp']
    except KeyError:
        alert_via_smtp = True
    except Exception as e:
        logger.error('error :: failed to determine alert_via_smtp for stale_metrics alert - %s' % (
            e))
    if alert_via_smtp:
        alert_vias.append('alert_via_smtp')
    try:
        alert_via_slack = settings.THUNDER_OPTS['alert_via_slack']
    except KeyError:
        alert_via_slack = False
    except Exception as e:
        logger.error('error :: failed to determine alert_via_slack for stale_metrics alert - %s' % (
            e))
    if alert_via_slack:
        alert_vias.append('alert_via_slack')
    try:
        alert_via_pagerduty = settings.THUNDER_CHECKS['alert_via_pagerduty']
    except KeyError:
        alert_via_pagerduty = False
    except Exception as e:
        logger.error('error :: failed to determine alert_via_pagerduty for stale_metrics alert - %s' % (
            e))
    if alert_via_pagerduty:
        alert_vias.append('alert_via_pagerduty')

    external_settings = {}
    try:
        external_settings = get_external_settings(skyline_app, parent_namespace, True)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: get_external_settings failed - %s' % (
            function_str, e))

    thunder_alert_endpoint = None
    thunder_alert_token = None

    if external_settings:
        alert_on_stale_metrics = None
        config_id = list(external_settings.keys())[0]
        logger.info('%s :: checking external_settings %s for alert_on_stale_metrics' % (
            function_str, config_id))
        try:
            alert_on_stale_metrics = external_settings[config_id]['alert_on_stale_metrics']['enabled']
        except KeyError:
            alert_on_stale_metrics = False
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: error determining external_settings[%s][\'alert_on_stale_metrics\'][\'enabled\'] - %s' % (
                function_str, config_id, e))
        if alert_on_stale_metrics:
            try:
                thunder_alert_endpoint = external_settings[config_id]['thunder_alert_endpoint']
                thunder_alert_token = external_settings[config_id]['thunder_alert_token']
            except KeyError:
                alert_on_stale_metrics = False
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: %s :: error determining external_settings[%s][\'alert_on_stale_metrics\'][\'enabled\'] - %s' % (
                    function_str, config_id, e))
        if thunder_alert_endpoint and thunder_alert_token:
            alert_via_dict = {
                'alert_via_http': True,
                'thunder_alert_endpoint': thunder_alert_endpoint,
                'thunder_alert_token': thunder_alert_token,
            }
            alert_vias.append(alert_via_dict)

    subject = message
    alerts_sent = 0
    all_sent = False
    alert_sent_at = time()

    # Create a Redis key to send a link with the alert
    redis_key = 'thunder.stale_metrics.alert.%s' % str(alert_sent_at)
    data_dict = {
        'status': data['status'],
        'metrics': metrics_to_alert_on,
    }
    try:
        self.redis_conn_decoded.setex(redis_key, 604800, str(data_dict))
    except Exception as e:
        logger.error('error :: %s :: failed to set key %s - %s' % (
            function_str, redis_key, e))
    key_link = '%s/api?thunder_stale_metrics=true&timestamp=%s' % (
        settings.SKYLINE_URL, str(alert_sent_at))

    for alert_via in alert_vias:
        use_body = '%s stale metrics listed at %s (from %s)' % (
            str(len(metrics_to_alert_on)), key_link, this_host)
        if data['status'] == 'recovered' and level == 'notice':
            use_body = '%s recovered stale metrics listed at %s (from %s)' % (
                str(len(metrics_to_alert_on)), key_link, this_host)
        try:
            if alert_via == 'alert_via_slack':
                title = 'Skyline Thunder - %s - %s stale metrics' % (
                    level.upper(), str(len(stale_metrics)))
                if data['status'] == 'recovered' and level == 'notice':
                    title = 'Skyline Thunder - %s - %s stale metrics recovered' % (
                        level.upper(), str(len(stale_metrics)))
                with_subject = subject.replace(level, '')
                title = title + with_subject
                alert_sent = thunder_alert(alert_via, title, use_body)
                if alert_sent:
                    alerts_sent_dict[alert_via] = True
                    alerts_sent += 1
            if alert_via == 'alert_via_smtp':
                title = 'Skyline Thunder - %s' % level.upper()
                with_subject = subject.replace(level, '')
                final_subject = title + with_subject
                alert_sent = thunder_alert(alert_via, final_subject, str(use_body))
                if alert_sent:
                    alerts_sent_dict[alert_via] = True
                    alerts_sent += 1
            if alert_via == 'alert_via_pagerduty':
                alert_sent = thunder_alert(alert_via, subject, str(use_body))
                if alert_sent:
                    alerts_sent_dict[alert_via] = True
                    alerts_sent += 1
            if isinstance(alert_via, dict):
                alert_data_dict = {
                    'status': {},
                    'data': {
                        'token': alert_via_dict['thunder_alert_token'],
                        'level': level,
                        'alert': 'stale_metrics',
                        'alert_id': 2,
                        'namespace': parent_namespace,
                        'metrics': metrics_to_alert_on,
                        'status': data['status'],
                    },
                }
                alert_via['alert_data_dict'] = alert_data_dict
                http_alert_sent = thunder_alert(alert_via, level, alert_data_dict)
                if http_alert_sent:
                    alerts_sent_dict['alert_via_http'] = True
                    alerts_sent += 1
            if alert_sent:
                logger.info('sent thunder_alert(%s, %s' % (
                    str(alert_via), str(subject)))
        except Exception as e:
            logger.error('error :: %s :: failed to alert for stale_metrics for alert_via %s - %s' % (
                function_str, str(alert_via), e))

    alerts_sent_dict['to_send'] = len(alert_vias)
    alerts_sent_dict['sent'] = alerts_sent
    if alerts_sent == len(alert_vias):
        alerts_sent_dict['all_sent'] = True
        all_sent = True
    else:
        alerts_sent_dict['all_sent'] = False
        all_sent = False

    if all_sent and level == 'alert':
        for metric in metrics_to_alert_on:
            try:
                self.redis_conn_decoded.hset(alerted_on_stale_metrics_hash_key, metric, alert_sent_at)
            except Exception as e:
                logger.error('error :: %s :: failed to set key in hash %s - %s' % (
                    function_str, alerted_on_stale_metrics_hash_key, e))

    if all_sent and level == 'notice':
        for metric in metrics_to_alert_on:
            try:
                self.redis_conn_decoded.hdel(alerted_on_stale_metrics_hash_key, metric)
            except Exception as e:
                logger.error('error :: %s :: failed to hdel key in hash %s - %s' % (
                    function_str, alerted_on_stale_metrics_hash_key, e))

    return alerts_sent_dict
