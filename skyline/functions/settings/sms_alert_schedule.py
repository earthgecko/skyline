import datetime
import pytz
import logging
import traceback
from time import time, strftime

import settings
from matched_or_regexed_in_list import matched_or_regexed_in_list


# @added 20240407 - Feature #4214: alert.paused
def sms_alert_schedule(current_skyline_app, metric):
    """
    Determine if metric is enabled in the SMS_ALERT_SCHEDULE.

    :param current_skyline_app: the app calling the function
    :param metric: the metric name
    :type current_skyline_app: str
    :type metric: str
    :return: enabled
    :rtype: bool

    """

    sms_alert_enabled = True
    try:
        sms_alert_schedule = settings.SMS_ALERTS_SCHEDULE
    except:
        sms_alert_schedule = {}
    if not sms_alert_schedule:
        return sms_alert_enabled

    def is_time_enabled(enabled, disabled, current):
        if enabled < disabled:
            # Period does not cross midnight
            return enabled <= current <= disabled
        else:
            # Period crosses midnight
            return current >= enabled or current <= disabled

    function_str = '%s :: functions.settings.sms_alert_schedule' % current_skyline_app
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    enabled = None
    disabled = None
    schedule = None
    for metric_namespace in sms_alert_schedule.keys():
        pattern_match = False
        try:
            pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, metric, [metric_namespace])
        except Exception as err:
            current_logger.error('error :: %s :: matched_or_regexed_in_list failed checking %s in [%s], err: %s' % (
                function_str, metric, metric_namespace, err))
        if pattern_match:
            schedule = dict(sms_alert_schedule[metric_namespace])
            break
    if not schedule:
        return sms_alert_enabled

    local_tz = pytz.timezone(settings.SERVER_PYTZ_TIMEZONE)
    if 'timezone' in schedule.keys():
        try:
            local_tz = pytz.timezone(schedule['timezone'])
        except Exception as err:
            current_logger.error('error :: %s :: failed to determine local_tz from %s, err: %s' % (
                function_str, str(schedule['timezone']), err))

    days = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
    if 'days' in schedule.keys():
        try:
            defined_days = schedule['days']
            if isinstance(defined_days, list):
                days = list(defined_days)
        except Exception as err:
            current_logger.error('error :: %s :: failed to determine days from %s, err: %s' % (
                function_str, str(schedule['days']), err))

    current_timestamp = int(time())
    current_abbreviated_weekday = datetime.datetime.fromtimestamp(current_timestamp, local_tz).strftime('%a')
    if current_abbreviated_weekday not in days:
        return sms_alert_enabled

    try:
        enabled = schedule['enabled']
    except Exception as err:
        current_logger.error('error :: %s :: failed to determine enabled for %s, err: %s' % (
            function_str, metric_namespace, err))
        return sms_alert_enabled
    try:
        disabled = schedule['disabled']
    except Exception as err:
        current_logger.error('error :: %s :: failed to determine disabled for %s, err: %s' % (
            function_str, metric_namespace, err))
        return sms_alert_enabled
    if not enabled and not disabled:
        return sms_alert_enabled

    current_time = datetime.datetime.fromtimestamp(current_timestamp, local_tz).strftime('%H:%M:%S')
    time_format = '%H:%M:%S'
    enabled_time = datetime.datetime.strptime(enabled, time_format).time()
    disabled_time = datetime.datetime.strptime(disabled, time_format).time()
    current_time_obj = datetime.datetime.strptime(current_time, time_format).time()

    current_logger.info('%s :: checking if %s SMS alert is currently in the enabled period, enabled: %s, disabled: %s' % (
        function_str, metric, enabled, disabled))

    # Check if current time is enabled or disabled
    try:
        sms_alert_enabled = is_time_enabled(enabled_time, disabled_time, current_time_obj)
    except Exception as err:
        current_logger.error('error :: %s :: is_time_enabled failed, err: %s' % (
            function_str, err))
        return sms_alert_enabled
    current_logger.info('%s :: %s SMS alert is currently enabled: %s' % (
        function_str, metric, str(sms_alert_enabled)))

    return sms_alert_enabled
