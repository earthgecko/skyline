import logging
import traceback
from os import uname
from os import getpid
from time import time

import settings
from functions.redis.get_metric_timeseries import get_metric_timeseries
from functions.thunder.send_event import thunder_send_event
from functions.thunder.check_thunder_failover_key import check_thunder_failover_key
from custom_algorithms import run_custom_algorithm_on_timeseries

skyline_app = 'thunder'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

# The required THUNDER directories which are failed over to and
# used in the event that Redis is down
THUNDER_EVENTS_DIR = '%s/thunder/events' % settings.SKYLINE_TMP_DIR
THUNDER_KEYS_DIR = '%s/thunder/keys' % settings.SKYLINE_TMP_DIR

this_host = str(uname()[1])


# @added 20210520 - Branch #1444: thunder
def thunder_check_horizon_metrics_received(self):
    """
    Determine any significant change in the number of metrics recieved by the
    workers and sends a thunder event when necessary.

    :param self: the self object
    :type self: object
    :return: success
    :rtype: boolean

    """

    function_str = 'functions.thunder.thunder_check_horizon_metrics_recieved'

    check_app = 'horizon'
    event_type = 'worker.metrics_received'
    base_name = 'skyline.%s.%s.%s' % (check_app, this_host, event_type)

    success = True
    now = int(time())
    try:
        expiry = int(settings.THUNDER_CHECKS[check_app][event_type]['expiry'])
    except Exception as e:
        logger.error('error :: %s :: failed to determine the expiry for %s %s check - %s' % (
            function_str, check_app, event_type, e))
        expiry = 900

    check_dict = {}
    cache_key = 'thunder.%s.%s' % (check_app, event_type)
    try:
        check_dict = self.redis_conn_decoded.hgetall(cache_key)
    except Exception as e:
        logger.error('error :: %s :: could not get the Redis %s key - %s' % (
            function_str, cache_key, e))
    value = None
    timestamp = None
    if check_dict:
        try:
            value = float(check_dict['value'])
            timestamp = int(float(check_dict['timestamp']))
            logger.info('thunder/rolling :: %s :: %s.%s value from dict: %s' % (
                function_str, check_app, event_type, str(value)))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: thunder/rolling :: %s :: could not determine %s.%s value from dict - %s' % (
                function_str, check_app, event_type, e))

    # Determine if a thunder alert has been sent for this check
    check_thunder_alert = None
    cache_key = 'thunder.alert.%s.%s' % (check_app, event_type)
    try:
        check_thunder_alert = self.redis_conn_decoded.get(cache_key)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error('error :: thunder/rolling :: %s :: failed to get %s Redis key - %s' % (
            function_str, cache_key, e))
    if not check_thunder_alert:
        check_thunder_alert = check_thunder_failover_key(self, cache_key)

    if not check_thunder_alert:
        # If worker has not updated the thunder.horizon.worker.metrics_received
        # Redis in 5 minutes, alert
        alert_no_metrics_recieved_reported = False
        if timestamp:
            if timestamp < (int(time()) - 299):
                alert_no_metrics_recieved_reported = True
        if alert_no_metrics_recieved_reported:
            level = 'alert'
            message = '%s - Horizon worker not reporting metrics_received' % level
            status = '%s no count reported for 5 minutes' % base_name
            thunder_event = {
                'level': level,
                'event_type': event_type,
                'message': message,
                'app': check_app,
                'metric': base_name,
                'source': 'thunder',
                'timestamp': time(),
                'expiry': expiry,
                'data': {'status': status}
            }
            submitted = None
            try:
                submitted = thunder_send_event(skyline_app, thunder_event, log=True)
            except Exception as e:
                logger.error('error :: thunder/rolling :: %s :: thunder_send_event failed - %s' % (
                    function_str, e))
            if submitted:
                logger.info('thunder/rolling :: %s :: %s %s not reported in 5 minutes thunder_send_event submitted' % (
                    function_str, check_app, event_type))
                return False
            else:
                logger.error('error :: thunder/rolling :: %s :: %s %s not reported in 5 minutes thunder_send_event failed' % (
                    function_str, check_app, event_type))

    # Get the timeseries
    metric_name = '%s%s' % (settings.FULL_NAMESPACE, base_name)
    timeseries = []
    try:
        timeseries = get_metric_timeseries(skyline_app, metric_name)
    except Exception as e:
        logger.error('error :: %s :: get_metric_timeseries failed for %s - %s' % (
            function_str, metric_name, e))

    send_alert = False
    anomalous = None
    anomalyScore = None
    if timeseries:
        percent = 20
        try:
            percent = float(settings.THUNDER_CHECKS[check_app][event_type]['significant_change_percentage'])
        except Exception as e:
            logger.error('error :: %s :: failed to determine significant_change_percentage for %s %s check - %s' % (
                function_str, check_app, event_type, e))
            percent = 20
        window = 600
        try:
            window = float(settings.THUNDER_CHECKS[check_app][event_type]['significant_change_window'])
        except Exception as e:
            logger.error('error :: %s :: failed to determine significant_change_window for %s %s check - %s' % (
                function_str, check_app, event_type, e))
            window = 600
        period = 3600
        try:
            period = float(settings.THUNDER_CHECKS[check_app][event_type]['significant_change_over'])
        except Exception as e:
            logger.error('error :: %s :: failed to determine significant_change_over for %s %s check - %s' % (
                function_str, check_app, event_type, e))
            period = 3600
        times_in_a_row = 5
        try:
            times_in_a_row = int(float(settings.THUNDER_CHECKS[check_app][event_type]['times_in_a_row']))
        except Exception as e:
            logger.error('error :: %s :: failed to determine times_in_a_row for %s %s check - %s' % (
                function_str, check_app, event_type, e))
            times_in_a_row = 5

        custom_algorithm_dict = {}
        algorithm_source = '/opt/skyline/github/skyline/skyline/custom_algorithms/significant_change_window_percent_sustained.py'
        try:
            algorithm_source = settings.THUNDER_CHECKS[check_app][event_type]['algorithm_source']
        except Exception as e:
            logger.error('error :: %s :: failed to determine algorithm_source for %s %s check - %s' % (
                function_str, check_app, event_type, e))
            algorithm_source = '/opt/skyline/github/skyline/skyline/custom_algorithms/significant_change_window_percent_sustained.py'
        custom_algorithm_dict['algorithm_source'] = algorithm_source
        custom_algorithm_dict['max_execution_time'] = 10.0
        algorithm_parameters = {
            'percent': percent, 'window': window, 'period': period,
            'times_in_a_row': times_in_a_row,
            'return_percent_as_anomalyScore': True,
            'debug_logging': True,
        }
        custom_algorithm_dict['algorithm_parameters'] = algorithm_parameters
        custom_algorithm = 'significant_change_window_percent_sustained'
        anomalous = None
        anomalyScore = None
        try:
            anomalous, anomalyScore = run_custom_algorithm_on_timeseries(skyline_app, getpid(), base_name, timeseries, custom_algorithm, custom_algorithm_dict, False)
        except Exception as e:
            logger.error('error :: %s :: failed to determine the expiry for %s %s check - %s' % (
                function_str, check_app, event_type, e))
        if anomalous:
            send_alert = True
            success = False
            logger.warn('warning :: thunder/rolling :: %s :: %s.%s is anomalous latest value is %s %% different from median of windows in period' % (
                function_str, check_app, event_type, str(anomalyScore)))
        else:
            logger.info('thunder/rolling :: %s :: %s.%s not anomalous latest value only %s %% different from median of windows in period' % (
                function_str, check_app, event_type, str(anomalyScore)))

    # If not anomalous and an alert has been sent, send a recovery notice
    if check_thunder_alert and anomalous is False:
        level = 'notice'
        message = '%s - Horizon worker.metrics_received has recovered' % level
        status = '%s is now %.2f %% different from the median windows (%s seconds) of the last period (%s seconds)' % (
            base_name, anomalyScore, str(window), str(period))
        thunder_event = {
            'level': level,
            'event_type': event_type,
            'message': message,
            'app': check_app,
            'metric': base_name,
            'source': 'thunder',
            'timestamp': now,
            'expiry': 59,
            'data': {'status': status}
        }
        try:
            submitted = thunder_send_event(skyline_app, thunder_event, log=True)
        except Exception as e:
            logger.error('error :: %s :: thunder_send_event failed - %s' % (
                function_str, e))
        if submitted:
            logger.info('%s :: %s %s thunder_send_event submitted' % (
                function_str, check_app, event_type))
            # Remove alert key
            cache_key = 'thunder.alert.%s.%s' % (check_app, event_type)
            try:
                self.redis_conn.delete(cache_key)
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: thunder/rolling :: %s :: failed to delete %s Redis key - %s' % (
                    function_str, cache_key, e))
        else:
            logger.error('error :: %s :: %s %s thunder_send_event failed' % (
                function_str, check_app, event_type))
        return success

    # If anomalous
    if send_alert and anomalous and not check_thunder_alert:
        level = 'alert'
        message = '%s - Horizon worker.metrics_received has changed significantly' % level
        status = '%s is %.2f %% different from the median windows (%s seconds) of the last period (%s seconds)' % (
            base_name, anomalyScore, str(window), str(period))
        try:
            expiry = int(settings.THUNDER_CHECKS[check_app][event_type]['expiry'])
        except Exception as e:
            logger.error('error :: %s :: failed to determine the expiry for %s %s check - %s' % (
                function_str, check_app, event_type, e))
            expiry = 900
        thunder_event = {
            'level': level,
            'event_type': event_type,
            'message': message,
            'app': check_app,
            'metric': base_name,
            'source': 'thunder',
            'timestamp': time(),
            'expiry': expiry,
            'data': {'status': status}
        }
        try:
            submitted = thunder_send_event(skyline_app, thunder_event, log=True)
        except Exception as e:
            logger.error('error :: %s :: thunder_send_event failed - %s' % (
                function_str, e))
        if submitted:
            logger.info('%s :: Horizon worker.metrics_received thunder_send_event submitted' % (
                function_str))
        else:
            logger.error('error :: %s :: Horizon worker.metrics_received thunder_send_event failed' % (
                function_str))
    return success
