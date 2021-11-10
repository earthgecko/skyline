import logging
import traceback
import requests

import settings
from functions.smtp.send_email import send_email
from slack_functions import slack_post_message
from functions.pagerduty.send_pagerduty import send_pagerduty

skyline_app = 'thunder'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)


def thunder_alert(alert_via, subject, body):
    """

    """
    logger = logging.getLogger(skyline_app_logger)

    message_sent = False

    if alert_via == 'alert_via_smtp':
        send_smtp_alert = False
        try:
            if settings.THUNDER_OPTS['alert_via_smtp']:
                send_smtp_alert = True
                to = settings.THUNDER_OPTS['smtp_recipients'][0]
                cc = []
                for i in settings.THUNDER_OPTS['smtp_recipients']:
                    if i != to:
                        cc.append(i)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: thunder_alert :: failed to determine alert_via_smtp settings - %s' % e)
        if send_smtp_alert:
            try:
                message_sent = send_email(skyline_app, to, cc, subject, str(body))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: thunder_alert :: failed to send smtp messages - %s' % e)
        if message_sent:
            logger.info('thunder_alert :: smtp message sent - %s' % subject)

    if alert_via == 'alert_via_slack':
        try:
            if settings.THUNDER_OPTS['alert_via_slack']:
                message = '*%s*\n%s' % (subject, body)
                message_sent = slack_post_message(skyline_app,
                                                  settings.THUNDER_OPTS['slack_channel'],
                                                  'None', message)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: thunder_alert :: failed to send slack message - %s' % e)
        if message_sent:
            logger.info('thunder_alert :: slack message sent - %s' % subject)

    if alert_via == 'alert_via_pagerduty':
        try:
            if settings.THUNDER_OPTS['alert_via_pagerduty']:
                message = '%s %s' % (subject, body)
                message_sent = send_pagerduty(skyline_app, message, log=True)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: thunder_alert :: failed to determine alert_via_smtp settings - %s' % e)
        if message_sent:
            logger.info('thunder_alert :: slack message sent - %s' % subject)

    alert_via_http = False
    alerter_endpoint = None
    thunder_alert_token = None
    if isinstance(alert_via, dict):
        try:
            alert_via_http = alert_via['alert_via_http']
            if alert_via_http:
                alerter_endpoint = alert_via['thunder_alert_endpoint']
                thunder_alert_token = alert_via['thunder_alert_token']
                alert_data_dict = alert_via['alert_data_dict']
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: thunder_alert :: failed to determine alert_via settings - %s - %s' % (
                str(alert_via), e))
    if alerter_endpoint and thunder_alert_token and alert_data_dict:
        connect_timeout = 5
        read_timeout = 20
        use_timeout = (int(connect_timeout), int(read_timeout))
        response = None
        try:
            # response = requests.post(alerter_endpoint, data=alert_data, headers=headers, timeout=use_timeout)
            response = requests.post(alerter_endpoint, json=alert_data_dict, timeout=use_timeout)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: thunder_alert :: failed to post alert to %s - %s' % (
                str(alerter_endpoint), e))
        try:
            if response.status_code != 200:
                logger.warning('warning :: thunder_alert :: %s responded with status code %s and reason %s' % (
                    str(alerter_endpoint), str(response.status_code),
                    str(response.reason)))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: thunder_alert :: failed determine response.status_code - %s' % e)
        try:
            if response.status_code == 400:
                response_str = None
                try:
                    response_str = str(response.json())
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: thunder_alert :: failed determine response.status_code - %s' % e)
                logger.info('thunder_alert :: 400 response - %s' % (
                    str(response_str)))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: thunder_alert :: failed determine response.status_code - %s' % e)
        if response:
            if response.status_code == 200:
                logger.info('thunder_alert :: alert sent to %s - %s' % (
                    # str(alerter_endpoint), str(alert_data_dict['status'])))
                    str(alerter_endpoint), str(response.status_code)))
                message_sent = True
        if message_sent:
            logger.info('thunder_alert :: external thunder message sent')

    return message_sent
