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


# @added 20210604 - Branch #1444: thunder
def alert_on_no_data(self, level, message, parent_namespace, data):
    """
    Send a single thunder alert for to alert no_data or recovered from no_data
    for a top level namespace.

    :param self: the self object
    :param level: the alert level
    :param message: the message
    :param parent_namespace: the parent_namespace
    :param data: the thunder event data dict
    :type self: object
    :type level: str
    :type message: str
    :type parent_namespace: str
    :type data: dict
    :return: success
    :rtype: boolean

    """

    function_str = 'functions.thunder.alert_on_no_data'

    thunder_alert_key = 'thunder.alert.no_data.%s' % parent_namespace
    alerts_sent_dict = {}

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

    expiry = 1800
    if external_settings:
        alert_on_no_data = None
        config_id = list(external_settings.keys())[0]
        logger.info('%s :: checking external_settings %s for alert_on_no_data' % (
            function_str, config_id))
        try:
            alert_on_no_data = external_settings[config_id]['alert_on_no_data']['enabled']
        except KeyError:
            alert_on_no_data = False
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: error determining external_settings[%s][\'alert_on_no_data\'][\'enabled\'] - %s' % (
                function_str, config_id, e))
        if alert_on_no_data:
            try:
                thunder_alert_endpoint = external_settings[config_id]['thunder_alert_endpoint']
                thunder_alert_token = external_settings[config_id]['thunder_alert_token']
            except KeyError:
                alert_on_no_data = False
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: %s :: error determining external_settings[%s][\'alert_on_stale_metrics\'][\'enabled\'] - %s' % (
                    function_str, config_id, e))
            try:
                expiry = external_settings[config_id]['alert_on_no_data']['expiry']
            except KeyError:
                expiry = 1800
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
            logger.info('%s :: added alert_via_http to alert_vias for external settings %s - %s' % (
                function_str, config_id, thunder_alert_endpoint))

    subject = message
    alerts_sent = 0
    all_sent = False
    alert_sent_at = time()

    for alert_via in alert_vias:
        use_body = '%s of %s %s namespace metrics not receiving data (from %s)' % (
            str(data['total_stale_metrics']), str(data['total_metrics']),
            parent_namespace, this_host)
        if data['status'] == 'recovered' and level == 'notice':
            total_recent_metrics = str(data['total_recent_metrics'])
            total_stale_metrics = str(data['total_stale_metrics'])
            total_metrics = str(data['total_metrics'])
            use_body = '%s of %s %s namespace metrics recovered, receiving data again after no data for %s seconds (from %s) - {"total_recent_metrics": %s, "total_stale_metrics": %s, "total_metrics": %s}' % (
                total_recent_metrics, total_metrics, parent_namespace,
                str(data['recovered_after_seconds']), this_host,
                total_recent_metrics, total_stale_metrics, total_metrics)

        # @added 20210720 - Branch #1444: thunder
        # Added remove
        if data['status'] == 'removed' and level == 'notice':
            total_recent_metrics = str(data['total_recent_metrics'])
            total_stale_metrics = str(data['total_stale_metrics'])
            total_metrics = str(data['total_metrics'])
            use_body = '%s %s namespace metrics were removed from the no data check (from %s)' % (
                total_metrics, parent_namespace, this_host)
        try:
            if alert_via == 'alert_via_slack':
                title = 'Skyline Thunder - %s - URGENT - %s no data' % (
                    level.upper(), parent_namespace)
                if data['status'] == 'recovered' and level == 'notice':
                    title = 'Skyline Thunder - %s - %s receiving data (RECOVERED)' % (
                        level.upper(), parent_namespace)
                # @added 20210720 - Branch #1444: thunder
                # Added remove
                if data['status'] == 'removed' and level == 'notice':
                    title = 'Skyline Thunder - %s - %s - %s metrics removed from no data check' % (
                        level.upper(), parent_namespace, total_metrics)

                with_subject = subject.replace(level, '')
                title = title + with_subject
                alert_sent = thunder_alert(alert_via, title, use_body)
                if alert_sent:
                    alerts_sent_dict[alert_via] = True
                    alerts_sent += 1
            if alert_via == 'alert_via_smtp':
                title = 'Skyline Thunder - %s - URGENT' % level.upper()
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
                try:
                    recovered_after_seconds = data['recovered']
                except KeyError:
                    recovered_after_seconds = 0
                alert_data_dict = {
                    'status': {},
                    'data': {
                        'token': alert_via['thunder_alert_token'],
                        'level': level,
                        'alert': 'no_data',
                        'alert_id': 1,
                        'namespace': parent_namespace,
                        'last_timestamp': data['last_timestamp'],
                        'recovered_after_seconds': recovered_after_seconds,
                        'total_metrics': data['total_metrics'],
                        'total_recent_metrics': data['total_recent_metrics'],
                        'total_stale_metrics': data['total_stale_metrics'],
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
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: failed to alert for no_data for alert_via %s - %s' % (
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
        try:
            self.redis_conn_decoded.setex(thunder_alert_key, expiry, alert_sent_at)
            logger.info('set %s Redis key with expiry of %s' % (
                thunder_alert_key, str(expiry)))
        except Exception as e:
            logger.error('error :: %s :: failed to set key in hash %s - %s' % (
                function_str, thunder_alert_key, e))

    if all_sent and level == 'notice':
        try:
            self.redis_conn_decoded.delete(thunder_alert_key)
            logger.info('removed %s Redis key' % (thunder_alert_key))
        except Exception as e:
            logger.error('error :: %s :: failed to delete key in hash %s - %s' % (
                function_str, thunder_alert_key, e))

    return alerts_sent_dict
