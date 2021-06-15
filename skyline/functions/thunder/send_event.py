import logging
import traceback
from os import path
from time import time

import settings
from skyline_functions import get_redis_conn, write_data_to_file, mkdir_p

# The required THUNDER directories which are failed over to and
# used in the event that Redis is down
THUNDER_EVENTS_DIR = '%s/thunder/events' % settings.SKYLINE_TMP_DIR


# @added 20210518 - Branch #1444: thunder
def thunder_send_event(current_skyline_app, event, log=True):
    """
    Add an event to the thunder.events Redis set or the thunder check dir if
    Redis is not available.

    :param current_skyline_app: the app calling the function
    :param event: the event data
    :param log: whether to log or not, optional, defaults to False
    :type current_skyline_app: str
    :type event: dict
    :type log: boolean
    :return: submitted
    :rtype: boolean

    """

    function_str = 'functions.thunder.thunder_sent_event'
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    submitted = 0
    try:
        redis_conn = get_redis_conn(current_skyline_app)
        submitted = redis_conn.sadd('thunder.events', str(event))
        if submitted:
            return True
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to add %s to thunder.events Redis set - %s' % (
            function_str, str(event), e))

    # If the thunder event was not added to Redis set, create the event_file
    if not path.exists(THUNDER_EVENTS_DIR):
        mkdir_p(THUNDER_EVENTS_DIR)
        current_logger.info('created dir - %s' % THUNDER_EVENTS_DIR)
    event_file = '%s/%s.thunder.event.dict' % (THUNDER_EVENTS_DIR, str(time()))
    try:
        write_data_to_file(current_skyline_app, event_file, 'w', str(event))
        current_logger.info('added thunder event file - %s' % event_file)
        submitted = True
    except Exception as e:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: failed to add thunder event file - %s - %s' % (event_file, e))
        submitted = False

    return submitted
