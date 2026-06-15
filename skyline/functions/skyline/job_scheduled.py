"""
job_scheduled.py
"""
import logging
from datetime import datetime

from croniter import croniter

# @added 20240514 - Feature #5350: functions.skyline.job_scheduled
def job_scheduled(current_skyline_app, cron_expression=None, schedule_dict={}, interval=60):
    """

    Determine if a cron_expression or schedule_dict falls within the interval to
    run.  Both the cron_expression and the schedule_dict are based on the same
    syntax as cron:
    # ┌───────────── minute (0–59)
    # │ ┌───────────── hour (0–23)
    # │ │ ┌───────────── day of the month (1–31)
    # │ │ │ ┌───────────── month (1–12)
    # │ │ │ │ ┌───────────── day of the week (0–6) (Sunday to Saturday; 7 is also Sunday on some systems)
    # │ │ │ │ │
    # │ │ │ │ │
    # │ │ │ │ │
    # 5 7 * * *
    An example ``cron_expression='5 7 * * *'``
    An example schedule_dict: ``{'minute': 5, 'hour': 7, 'day_of_month': '*', 'month': '*', 'day_of_week': '*'}``
    A schedule is considered disabled if all the values are set to 0
    
    :param current_skyline_app: the current_skyline_app
    :param cron_expression: a string of in cron syntax
    :param schedule_dict: a dict of a cron schedule
    :param interval: the interval of the period covered
    :type current_skyline_app: str
    :type cron_expression: str
    :type schedule_dict: dict
    :type interval: int
    :return: scheduled
    :rtype: bool

    """
    function_str = 'job_scheduled'
    scheduled = False
    if not cron_expression:
        try:
            cron_expression = ''
            for key, item in schedule_dict.items():
                item_str = str(item)
                if isinstance(item, list):
                    item_str = ''
                    for i in item:
                        if item_str == '':
                            item_str = '%s' % str(i)
                        else:
                            item_str = '%s,%s' % (item_str, str(i))
                cron_expression = cron_expression + str(item_str) + ' '
        except Exception as err:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: failed to build cron_expression from schedule_dict: %s, err: %s' % (
                function_str, str(schedule_dict), err))
    # A schedule is considered disabled if the values are set to 0 
    if cron_expression == '0 0 0 0 0 ':
        # Disabled
        return scheduled

    current_time = datetime.now()
    if cron_expression:
        try:
            scheduled = croniter.match(cron_expression, current_time)
        except Exception as err:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: croniter.match on cron_expression: %s, err: %s' % (
                function_str, str(cron_expression), err))
    return scheduled
