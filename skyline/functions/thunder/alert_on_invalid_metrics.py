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


# @added 20240319 - Feature #5312: thunder - 207 alert
def alert_on_invalid_metrics(
        self, level, message, parent_namespace, timestamp, data):
    """
    Send a single thunder alert for all invalid metrics submitted to flux for a
    top level namespace.

    :param self: the self object
    :param level: the alert level
    :param parent_namespace: the parent_namespace
    :param timestamp: the timestamp
    :param data: the thunder event data dict
    :type self: object
    :type parent_namespace: str
    :type timestamp: int
    :type data: dict
    :return: success
    :rtype: boolean

    """

    function_str = 'functions.thunder.alert_on_invalid_metrics'
    alerts_sent_dict = {}

    # Check if already alerted
    alerted_on_invalid_metrics_key = 'thunder.alerted_on.invalid_metrics.%s' % parent_namespace
    alerted_on_invalid_metrics_key_ttl = 0
    try:
        alerted_on_invalid_metrics_key_ttl = self.redis_conn_decoded.ttl(alerted_on_invalid_metrics_key)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: exists failed on Redis key %s, err: %s' % (
            function_str, alerted_on_invalid_metrics_key, err))
    if alerted_on_invalid_metrics_key_ttl > 0:
        alerts_sent_dict['all_sent'] = True
        alerts_sent_dict['in_expiry'] = True
        alerts_sent_dict['ttl'] = alerted_on_invalid_metrics_key_ttl
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
        logger.error('error :: failed to determine alert_via_smtp for invalid_metrics alert - %s' % (
            e))
    if alert_via_smtp:
        alert_vias.append('alert_via_smtp')
    try:
        alert_via_slack = settings.THUNDER_OPTS['alert_via_slack']
    except KeyError:
        alert_via_slack = False
    except Exception as e:
        logger.error('error :: failed to determine alert_via_slack for invalid_metrics alert - %s' % (
            e))
    if alert_via_slack:
        alert_vias.append('alert_via_slack')
    try:
        alert_via_pagerduty = settings.THUNDER_CHECKS['alert_via_pagerduty']
    except KeyError:
        alert_via_pagerduty = False
    except Exception as e:
        logger.error('error :: failed to determine alert_via_pagerduty for invalid_metrics alert - %s' % (
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
        alert_on_invalid_metrics = None
        config_id = list(external_settings.keys())[0]
        logger.info('%s :: checking external_settings %s for alert_on_invalid_metrics' % (
            function_str, config_id))
        try:
            alert_on_invalid_metrics = external_settings[config_id]['alert_on_invalid_metrics']['enabled']
        except KeyError:
            alert_on_invalid_metrics = False
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: error determining external_settings[%s][\'alert_on_invalid_metrics\'][\'enabled\'] - %s' % (
                function_str, config_id, e))
        if alert_on_invalid_metrics:
            try:
                thunder_alert_endpoint = external_settings[config_id]['thunder_alert_endpoint']
                thunder_alert_token = external_settings[config_id]['thunder_alert_token']
            except KeyError:
                alert_on_invalid_metrics = False
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: %s :: error determining external_settings[%s][\'alert_on_invalid_metrics\'][\'enabled\'] - %s' % (
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

    # Create a Redis key to send a link with the alert
    invalid_metrics = data['invalid_metrics']
    redis_key = 'thunder.invalid_metrics.alert.%s.%s' % (
        parent_namespace, str(timestamp))
    data_dict = {
        'status': data['status'],
        'invalid_metrics': invalid_metrics,
    }
    try:
        self.redis_conn_decoded.setex(redis_key, 7200, str(data_dict))
    except Exception as err:
        logger.error('error :: %s :: failed to set key %s, err: %s' % (
            function_str, redis_key, err))
    key_link = '%s/api?thunder_invalid_metrics=true&parent_namespace=%s&timestamp=%s' % (
        settings.SKYLINE_URL, parent_namespace, str(timestamp))

    for alert_via in alert_vias:
        use_body = '%s invalid metrics listed at %s (from %s)' % (
            str(len(invalid_metrics)), key_link, this_host)
        try:
            if alert_via == 'alert_via_slack':
                title = 'Skyline Thunder - %s - %s invalid metrics' % (
                    level.upper(), str(len(invalid_metrics)))
                with_subject = subject.replace(level, '')
                title = title + ' ' + with_subject
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
                        'alert': 'invalid_metrics',
                        'alert_id': 5,
                        'namespace': parent_namespace,
                        'metrics': invalid_metrics,
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
        except Exception as err:
            logger.error('error :: %s :: failed to alert for invalid_metrics for alert_via %s, err: %s' % (
                function_str, str(alert_via), err))

    alerts_sent_dict['to_send'] = len(alert_vias)
    alerts_sent_dict['sent'] = alerts_sent
    if alerts_sent == len(alert_vias):
        alerts_sent_dict['all_sent'] = True
        all_sent = True
        try:
            alerted_on_invalid_metrics_key_ttl = self.redis_conn_decoded.setex(alerted_on_invalid_metrics_key, 3600, timestamp)
        except Exception as err:
            logger.error('error :: %s :: setex failed on Redis key %s, err: %s' % (
                function_str, alerted_on_invalid_metrics_key, err))
    else:
        alerts_sent_dict['all_sent'] = False
        all_sent = False

    # @added 20220207 - Task #4430: Limit thunder alert retries
    # Only try and alert 5 times
    if not all_sent:
        times_alerted_key = 'thunder.invalid_metrics.alert.times_attempted.%s.%s' % (
            parent_namespace, data['status'])
        number_of_times_alerted = 0
        try:
            number_of_times_alerted = self.redis_conn_decoded.get(times_alerted_key)
            if not number_of_times_alerted:
                number_of_times_alerted = 0
        except Exception as err:
            logger.error('error :: %s :: failed to set key %s, err: %s' % (
                function_str, times_alerted_key, err))
        number_of_times_alerted = int(float(number_of_times_alerted)) + 1
        if number_of_times_alerted == 5:
            logger.warning('warning :: %s :: attempted to alert 5 times and an alert_via has failed for %s on %s, setting all_sent to True to delete hash key' % (
                function_str, data['status'], parent_namespace))
            all_sent = True
        else:
            try:
                self.redis_conn_decoded.setex(times_alerted_key, 1600, number_of_times_alerted)
                logger.info('%s :: attempted to alert %s times for %s on %s' % (
                    function_str, str(number_of_times_alerted), data['status'],
                    parent_namespace))
            except Exception as err:
                logger.error('error :: %s :: failed to set key %s, err: %s' % (
                    function_str, times_alerted_key, err))

    return alerts_sent_dict
