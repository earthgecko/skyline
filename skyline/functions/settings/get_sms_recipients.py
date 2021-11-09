import logging
import traceback

import settings
from matched_or_regexed_in_list import matched_or_regexed_in_list


# @added 20210724 - Feature #4196: functions.aws.send_sms
def get_sms_recipients(current_skyline_app, metric):
    """
    Determine SMS numbers for a metric.

    :param current_skyline_app: the app calling the function
    :param metric: the metric name
    :type current_skyline_app: str
    :type metric: str
    :return: sms_recipients
    :rtype: list

    """

    sms_recipients = []
    function_str = '%s :: functions.settings.get_sms_recipients' % current_skyline_app
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if metric.startswith(settings.FULL_NAMESPACE):
        base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
    else:
        base_name = metric

    for namespace in list(settings.SMS_ALERT_OPTS['namespaces']):
        pattern_match = False
        try:
            pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, base_name, [namespace])
        except Exception as e:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: matched_or_regexed_in_list failed checking %s in [%s] - %s' % (
                function_str, base_name, namespace, e))
            pattern_match = False
        if pattern_match:
            matched_namespace = metric_matched_by['matched_namespace']
            for recipient in settings.SMS_ALERT_OPTS['namespaces'][matched_namespace]:
                sms_recipients.append(recipient)
    sms_numbers = []
    for recipient in sms_recipients:
        sms_number = None
        try:
            sms_number = settings.SMS_ALERT_OPTS['recipients'][recipient]
            sms_numbers.append(sms_number)
        except Exception as e:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to determine number for SMS recipient %s - %s' % (
                function_str, recipient, e))
    if sms_numbers:
        sms_recipients = list(set(sms_numbers))

    return sms_recipients
