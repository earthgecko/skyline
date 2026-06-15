import logging
import traceback
from time import time
from os import uname
from ast import literal_eval

import settings
from functions.settings.get_external_settings import get_external_settings
from thunder_alerters import thunder_alert

skyline_app = 'thunder'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
this_host = str(uname()[1])


# @added 20231121 - Feature #5104: boundary - external_settings
# Limit cardinality
def alert_on_boundary_cardinality_breach(
        self, level, message, parent_namespace, expiry, rejected_metrics, data):
    """
    Send a single thunder alert for metrics discarded from a boundary rule due
    to breaching cardinality.

    :param self: the self object
    :param level: the alert level
    :param parent_namespace: the parent_namespace
    :param expiry: the expiry time
    :param rejected_metrics: list of metrics in the namespace that were rejected
    :param data: the thunder event data dict
    :type self: object
    :type parent_namespace: str
    :type expiry: int
    :type rejected_metrics_over_quota: list
    :type data: dict
    :return: success
    :rtype: boolean

    """

    function_str = 'functions.thunder.alert_on_boundary_caridnality_breach'
    alerts_sent_dict = {}

    alert_vias = []
    alert_via_smtp = True
    alert_via_slack = False
    alert_via_pagerduty = False
    try:
        alert_via_smtp = settings.THUNDER_OPTS['alert_via_smtp']
    except KeyError:
        alert_via_smtp = True
    except Exception as err:
        logger.error('error :: failed to determine alert_via_smtp for alert_on_boundary_caridnality_breach - %s' % (
            err))
    if alert_via_smtp:
        alert_vias.append('alert_via_smtp')
    try:
        alert_via_slack = settings.THUNDER_OPTS['alert_via_slack']
    except KeyError:
        alert_via_slack = False
    except Exception as err:
        logger.error('error :: failed to determine alert_via_slack for alert_on_boundary_caridnality_breach - %s' % (
            err))
    if alert_via_slack:
        alert_vias.append('alert_via_slack')
    try:
        alert_via_pagerduty = settings.THUNDER_CHECKS['alert_via_pagerduty']
    except KeyError:
        alert_via_pagerduty = False
    except Exception as err:
        logger.error('error :: failed to determine alert_via_pagerduty for alert_on_boundary_caridnality_breach - %s' % (
            err))
    if alert_via_pagerduty:
        alert_vias.append('alert_via_pagerduty')

    external_settings = {}
    try:
        external_settings = get_external_settings(skyline_app, parent_namespace, True)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: get_external_settings failed - %s' % (
            function_str, err))

    thunder_alert_endpoint = None
    thunder_alert_token = None

    if external_settings:
        do_alert_on_boundary_caridnality_breach = False
        config_id = list(external_settings.keys())[0]
        logger.info('%s :: checking external_settings %s for alert_on_boundary_caridnality_breach' % (
            function_str, config_id))
        try:
            do_alert_on_boundary_caridnality_breach = external_settings[config_id]['alert_on_boundary_cardinality_breach']['enabled']
            alert_via_expiry = external_settings[config_id]['alert_on_boundary_cardinality_breach']['expiry']
            if alert_via_expiry:
                expiry = int(alert_via_expiry)
        except KeyError:
            do_alert_on_boundary_caridnality_breach = True
            expiry = 3600
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: error determining external_settings[%s][\'alert_on_boundary_cardinality_breach\'][\'enabled\'] - %s' % (
                function_str, config_id, err))
        if do_alert_on_boundary_caridnality_breach:
            try:
                thunder_alert_endpoint = external_settings[config_id]['thunder_alert_endpoint']
                thunder_alert_token = external_settings[config_id]['thunder_alert_token']
            except KeyError:
                do_alert_on_boundary_caridnality_breach = False
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: %s :: error determining external_settings[%s][\'thunder_alert_endpoint\'] - %s' % (
                    function_str, config_id, err))
        if thunder_alert_endpoint and thunder_alert_token:
            alert_via_dict = {
                'alert_via_http': True,
                'thunder_alert_endpoint': thunder_alert_endpoint,
                'thunder_alert_token': thunder_alert_token,
                'expiry': expiry,
            }
            alert_vias.append(alert_via_dict)

    subject = message
    alerts_sent = 0
    alert_sent_at = time()

    # Get all alerted on breached
    alerted_on_boundary_cardinality_breach_hash_key = 'thunder.alerted_on.boundary_cardinality_breach.%s.%s' % (
        parent_namespace, data['algorithm'])
    alerted_on_boundary_cardinality_breach_dict = {}
    try:
        alerted_on_boundary_cardinality_breach_dict = self.redis_conn_decoded.hgetall(alerted_on_boundary_cardinality_breach_hash_key)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to get Redis hash key %s - %s' % (
            function_str, alerted_on_boundary_cardinality_breach_hash_key, err))

    metrics_already_alerted_on = []
    for namespace_alert_timestamp in list(alerted_on_boundary_cardinality_breach_dict.keys()):
        if int(namespace_alert_timestamp) <= (alert_sent_at - expiry):
            try:
                self.redis_conn_decoded.hdel(alerted_on_boundary_cardinality_breach_hash_key, namespace_alert_timestamp)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: %s :: failed to hdel %s from Redis hash key %s - %s' % (
                    function_str, namespace_alert_timestamp, alerted_on_boundary_cardinality_breach_hash_key, err))
            continue
        metrics_alerted_on = []
        try:
            metrics_alerted_on = literal_eval(alerted_on_boundary_cardinality_breach_dict[namespace_alert_timestamp])
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: failed to literal_eval %s in Redis hash key %s - %s' % (
                function_str, namespace_alert_timestamp, alerted_on_boundary_cardinality_breach_hash_key, err))
        for metric in metrics_alerted_on:
            metrics_already_alerted_on.append(metric)
    metrics_already_alerted_on = list(set(metrics_already_alerted_on))

    metrics_to_alert_on = []
    if metrics_already_alerted_on:
        for metric in rejected_metrics:
            if metric not in metrics_already_alerted_on:
                metrics_to_alert_on.append(metric)
    else:
        metrics_to_alert_on = list(rejected_metrics)

    if not metrics_to_alert_on:
        logger.info('%s :: all metrics in %s namespace have already been alerted on' % (
            function_str, parent_namespace))
        alerts_sent_dict['all_sent'] = True
        return alerts_sent_dict

    try:
        self.redis_conn_decoded.hset(alerted_on_boundary_cardinality_breach_hash_key, int(alert_sent_at), str(metrics_to_alert_on))
        self.redis_conn_decoded.expire(alerted_on_boundary_cardinality_breach_hash_key, expiry)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to hset %s from Redis hash key %s - %s' % (
            function_str, str(int(alert_sent_at)), alerted_on_boundary_cardinality_breach_hash_key, err))

    # Create a Redis key to send a link with the alert
    redis_key = 'thunder.boundary_cardinality_breach.alert.%s' % str(alert_sent_at)
    try:
        del data['rejected_metrics']
    except:
        pass
    data_dict = {
        'status': data['status'],
        'metrics': metrics_to_alert_on,
        'data': data,
    }
    try:
        self.redis_conn_decoded.setex(redis_key, 3600, str(data_dict))
    except Exception as err:
        logger.error('error :: %s :: failed to set key %s - %s' % (
            function_str, redis_key, err))
    key_link = '%s/api?thunder_boundary_cardinality_breach=true&timestamp=%s' % (
        settings.SKYLINE_URL, str(alert_sent_at))

    for alert_via in alert_vias:
        use_body = '%s metrics breached alert rule cardinality, listed at %s (from %s)' % (
            str(len(metrics_to_alert_on)), key_link, this_host)
        try:
            if alert_via == 'alert_via_slack':
                title = 'Skyline Thunder - %s - %s rejected metrics - over quota' % (
                    level.upper(), str(len(metrics_to_alert_on)))
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
                        'alert': 'boundary_cardinality_breach',
                        'alert_id': 4,
                        'namespace': parent_namespace,
                        'alerter_id': data['id'],
                        'metrics': rejected_metrics,
                        'breached_metrics_count': len(rejected_metrics),
                        #'processed_metrics': data['processed_metrics'],
                        # 'processed_metrics_count': data['processed_metrics_count'],
                        # 'submitted_metrics_count': data['submitted_metrics_count'],
                        'status': data['status'],
                        'url': key_link,
                    },
                }
                alert_via['alert_data_dict'] = alert_data_dict
#                http_alert_sent = thunder_alert(alert_via, level, alert_data_dict)
                del alert_data_dict['data']['rejected_metrics']
                logger.info('%s :: would thunder_alert with alert_via: %s, level: %s, alert_dict_data: %s' % (
                    function_str, str(alert_via), str(level),
                        str(alert_data_dict)))
                http_alert_sent = True
                if http_alert_sent:
                    alerts_sent_dict['alert_via_http'] = True
                    alerts_sent += 1
            if alert_sent:
                logger.info('sent thunder_alert(%s, %s' % (
                    str(alert_via), str(subject)))
        except Exception as e:
            logger.error('error :: %s :: failed to alert for metric_quota_exceeded for alert_via %s - %s' % (
                function_str, str(alert_via), e))

    alerts_sent_dict['to_send'] = len(alert_vias)
    alerts_sent_dict['sent'] = alerts_sent
    if alerts_sent == len(alert_vias):
        alerts_sent_dict['all_sent'] = True
        all_sent = True
    else:
        alerts_sent_dict['all_sent'] = False

    return alerts_sent_dict
