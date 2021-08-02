import logging
import pygerduty

import settings


# @added 20210524 - Branch #1444: thunder
def send_pagerduty(current_skyline_app, message, log=True):
    """
    Send a pagerduty event.

    :param current_skyline_app: the app calling the function
    :param message: the message
    :type current_skyline_app: str
    :type message: str
    :return: page_sent
    :rtype: boolean

    """

    function_str = 'functions.pagerduty.send_pagerduty'
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    page_sent = False
    pager = None
    try:
        pager = pygerduty.PagerDuty(settings.PAGERDUTY_OPTS['subdomain'], settings.PAGERDUTY_OPTS['auth_token'])
    except Exception as e:
        current_logger.error('error :: %s :: failed to Initialize pager - %s' % (
            function_str, e))
        pager = None
    if pager:
        try:
            page_sent = pager.trigger_incident(settings.PAGERDUTY_OPTS['key'], '%s - %s' % (
                message))
        except Exception as e:
            current_logger.error('error :: %s :: failed to Initialize pager - %s' % (
                function_str, e))
    return page_sent
