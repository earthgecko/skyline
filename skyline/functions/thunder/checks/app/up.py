import logging
import traceback
from os import uname
from time import time
import requests

import settings
from functions.thunder.send_event import thunder_send_event

skyline_app = 'thunder'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

# The required THUNDER directories which are failed over to and
# used in the event that Redis is down
THUNDER_EVENTS_DIR = '%s/thunder/events' % settings.SKYLINE_TMP_DIR
THUNDER_KEYS_DIR = '%s/thunder/keys' % settings.SKYLINE_TMP_DIR

this_host = str(uname()[1])


# @added 20210520 - Branch #1444: thunder
def thunder_check_app(self, check_app):
    """
    Determine the state of a Skyline app and send thunder events when necessary.

    :param self: the self object
    :param check_app: the Skyline app to check
    :param expiry: the Skyline app check alert expiry time
    :type self: object
    :type check_app: str
    :type expiry: int
    :return: success
    :rtype: boolean

    """

    function_str = 'functions.thunder.checks.app.thunder_check_app'
    success = True
    event_type = 'up'

    # Hit the webapp_up endpoint to update the webapp Redis key
    if check_app == 'webapp':
        try:
            webapp_up_url = 'http://%s:%s/webapp_up' % (
                settings.WEBAPP_IP, str(settings.WEBAPP_PORT))
            response = requests.get(webapp_up_url)
            if not response:
                logger.error('error :: no response from %s' % str(webapp_up_url))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: worker :: failed to request %s' % str(webapp_up_url))

    # Check if the app has a Redis key
    cache_key = check_app
    app_last_timestamp = 0
    send_event = False
    try:
        app_last_timestamp = self.redis_conn_decoded.get(cache_key)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to get %s Redis key - %s' % (
            function_str, cache_key, e))
        success = False
        send_event = True
    if not app_last_timestamp:
        success = False
        send_event = True
        logger.warn('warning :: %s :: failed to get app_last_timestamp from %s Redis key' % (
            function_str, cache_key))

    # Determine what to last known app run timestamp was
    thunder_app_last_timestamp_cache_key = 'thunder.last.%s' % check_app
    thunder_app_last_timestamp = 0
    try:
        thunder_app_last_timestamp = self.redis_conn_decoded.get(thunder_app_last_timestamp_cache_key)
        logger.info('%s :: thunder_app_last_timestamp: %s, from %s Redis key' % (
            function_str, str(thunder_app_last_timestamp), thunder_app_last_timestamp_cache_key))
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to get %s Redis key - %s' % (
            function_str, cache_key, e))

    # Determine if a DOWN thunder alert has been sent for the app
    check_down_alert = None
    cache_key = 'thunder.alert.%s.%s' % (check_app, event_type)
    try:
        check_down_alert = self.redis_conn_decoded.get(cache_key)
        if check_down_alert:
            send_event = False
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to get %s Redis key - %s' % (
            function_str, cache_key, e))

    # The app has not recovered yet still down, return
    if not app_last_timestamp and check_down_alert:
        logger.warn('warning :: %s :: %s still down but alert key exists so not alerting' % (
            function_str, check_app))
        return False

    thunder_event = {}
    metric = 'skyline.%s.%s.up' % (check_app, this_host)
    try:
        expiry = int(float(settings.THUNDER_CHECKS[check_app]['up']['expiry']))
    except Exception as e:
        logger.error('error :: %s :: failed to determine the expiry for %s up check - %s' % (
            function_str, check_app, e))
        expiry = 900

    # If a down alert was sent out and the app is back online, send a recovered
    # alert
    if app_last_timestamp and check_down_alert:
        # Remove the alert key
        try:
            self.redis_conn_decoded.delete(cache_key)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: failed to delete %s Redis key - %s' % (
                function_str, cache_key, e))
        down_time = 'unknown'
        if thunder_app_last_timestamp:
            seconds_down = int(float(app_last_timestamp)) - int(float(thunder_app_last_timestamp))
            down_time = '%s' % str(seconds_down)
        status = '%s has recovered after being down for %s seconds on %s' % (
            check_app, down_time, this_host)
        logger.info('%s :: %s removing %s Redis key' % (
            function_str, status, cache_key))
        # Remove the alert key
        try:
            self.redis_conn_decoded.delete(cache_key)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: failed to delete %s Redis key - %s' % (
                function_str, cache_key, e))
        level = 'notice'
        message = '%s - %s is UP (recovered) on %s' % (
            level, check_app, this_host)
        # Send app UP thunder event
        thunder_event = {
            'level': level,
            'event_type': event_type,
            'message': message,
            'app': check_app,
            'metric': metric,
            'source': 'thunder',
            'timestamp': app_last_timestamp,
            'expiry': 59,
            'data': {'status': status}
        }
        try:
            submitted = thunder_send_event(skyline_app, thunder_event, log=True)
        except Exception as e:
            logger.error('error :: %s :: thunder_send_event failed - %s' % (
                function_str, e))
        if submitted:
            send_event = False

    if send_event:
        last_reported_up_seconds_ago = 'unknown'
        if thunder_app_last_timestamp:
            last_reported_up_seconds = int(time()) - int(float(thunder_app_last_timestamp))
            last_reported_up_seconds_ago = '%s' % str(last_reported_up_seconds)
        status = '%s last reported up %s seconds ago on %s' % (
            check_app, last_reported_up_seconds_ago, this_host)
        level = 'alert'
        message = '%s - %s is DOWN on %s' % (level, check_app, this_host)
        thunder_event = {
            'level': level,
            'event_type': event_type,
            'message': message,
            'app': check_app,
            'metric': metric,
            'source': 'thunder',
            'timestamp': time(),
            'expiry': expiry,
            'data': {'status': status}
        }
        logger.info('%s :: sending thunder event from %s down' % (
            function_str, check_app))
        try:
            submitted = thunder_send_event(skyline_app, thunder_event, log=True)
            logger.info('%s :: thunder event submitted: %s' % (
                function_str, str(submitted)))
        except Exception as e:
            logger.error('error :: %s :: thunder_send_event failed - %s' % (
                function_str, e))
        success = False

    # Set to new thunder_app_last_timestamp
    if app_last_timestamp:
        try:
            self.redis_conn_decoded.set(thunder_app_last_timestamp_cache_key, app_last_timestamp)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: failed to get %s Redis key - %s' % (
                function_str, thunder_app_last_timestamp_cache_key, e))

    return success
