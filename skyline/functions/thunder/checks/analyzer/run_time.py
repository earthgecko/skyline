import logging
import traceback
from os import uname
from time import time

import settings
from functions.redis.get_metric_timeseries import get_metric_timeseries
from functions.thunder.send_event import thunder_send_event
from functions.thunder.check_thunder_failover_key import check_thunder_failover_key

skyline_app = 'thunder'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

# The required THUNDER directories which are failed over to and
# used in the event that Redis is down
THUNDER_EVENTS_DIR = '%s/thunder/events' % settings.SKYLINE_TMP_DIR
THUNDER_KEYS_DIR = '%s/thunder/keys' % settings.SKYLINE_TMP_DIR

ANALYZER_OPTIMUM_RUN_DURATION = settings.ANALYZER_OPTIMUM_RUN_DURATION
# ANALYZER_OPTIMUM_RUN_DURATION = 5

this_host = str(uname()[1])


# @added 20210520 - Branch #1444: thunder
def thunder_check_analyzer_run_time(self):
    """
    Determine the state of Analyzer run_time and send thunder events when
    necessary.

    :param self: the self object
    :type self: object
    :return: success
    :rtype: boolean

    """

    function_str = 'functions.thunder.thunder_check_analyzer_run_time'

    check_app = 'analyzer'
    event_type = 'run_time'
    metric = 'skyline.%s.%s.%s' % (check_app, this_host, event_type)

    success = True
    now = int(time())

    analyzer_run_time_dict = {}
    cache_key = 'analyzer.run_time'
    try:
        analyzer_run_time_dict = self.redis_conn_decoded.hgetall(cache_key)
    except Exception as e:
        logger.error('error :: %s :: could not get the Redis %s key - %s' % (
            function_str, cache_key, e))
    analyzer_run_time = None
    analyzer_run_time_timestamp = None
    if analyzer_run_time_dict:
        try:
            analyzer_run_time = float(analyzer_run_time_dict['value'])
            analyzer_run_time_timestamp = int(analyzer_run_time_dict['timestamp'])
            logger.info('thunder/rolling :: %s :: analyzer_run_time from dict: %s' % (
                function_str, str(analyzer_run_time)))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: thunder/rolling :: %s :: could not determine analyzer_run_time from dict - %s' % (
                function_str, e))

    # Determine if a analyzer_run_time thunder alert has been sent
    analyzer_run_time_thunder_alert = None
    cache_key = 'thunder.alert.%s.%s.alert' % (check_app, event_type)
    try:
        analyzer_run_time_thunder_alert = self.redis_conn_decoded.get(cache_key)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error('error :: thunder/rolling :: %s :: failed to get %s Redis key - %s' % (
            function_str, cache_key, e))
    if not analyzer_run_time_thunder_alert:
        analyzer_run_time_thunder_alert = check_thunder_failover_key(self, cache_key)

    # If normal send a recovered alert
    if analyzer_run_time and analyzer_run_time_thunder_alert:
        recovered = False
        if analyzer_run_time < ANALYZER_OPTIMUM_RUN_DURATION:
            if analyzer_run_time_timestamp:
                if analyzer_run_time_timestamp > (now - 60):
                    recovered = True
        if recovered:
            # Remove the alert Redis key
            try:
                self.redis_conn_decoded.delete(cache_key)
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: thunder/rolling :: %s :: failed to delete %s Redis key - %s' % (
                    function_str, cache_key, e))
            # Send thunder recovered event
            recovered_after = analyzer_run_time_timestamp - int(analyzer_run_time_thunder_alert)
            level = 'notice'
            message = '%s - Analyzer run_time has recovered' % level
            status = '%s.run_time has recovered to %s seconds after over running for %s seconds' % (
                metric, str(int(analyzer_run_time)), str(recovered_after))
            thunder_event = {
                'level': level,
                'event_type': event_type,
                'message': message,
                'app': check_app,
                'metric': metric,
                'source': 'thunder',
                'timestamp': now,
                'expiry': 59,
                'data': {'status': status}
            }
            submitted = None
            try:
                submitted = thunder_send_event(skyline_app, thunder_event, log=True)
            except Exception as e:
                logger.error('error :: thunder/rolling :: %s :: thunder_send_event failed - %s' % (
                    function_str, e))
            if submitted:
                logger.info('thunder/rolling :: %s :: Analyzer run_time recovered thunder_send_event submitted' % (
                    function_str))
            else:
                logger.error('error :: thunder/rolling :: %s :: Analyzer run_time recovered thunder_send_event failed' % (
                    function_str))

    # If over running send an alert
    if analyzer_run_time:
        send_overrun_alert = False
        if analyzer_run_time > ANALYZER_OPTIMUM_RUN_DURATION:
            success = False
            if not analyzer_run_time_thunder_alert:
                send_overrun_alert = True
        if send_overrun_alert:
            after_overruns = 5
            try:
                after_overruns = int(settings.THUNDER_CHECKS['analyzer']['run_time']['after_overruns'])
            except KeyError:
                after_overruns = 5
            except Exception as e:
                logger.error('error :: %s :: after_overruns was not found in THUNDER_CHECKS[\'analyzer\'][\'run_time\'] - %s' % (
                    function_str, e))
                after_overruns = 5
            metric_name = '%s%s' % (settings.FULL_NAMESPACE, metric)
            timeseries = []
            try:
                timeseries = get_metric_timeseries(skyline_app, metric_name)
            except Exception as e:
                logger.error('error :: %s :: get_metric_timeseries failed for %s - %s' % (
                    function_str, metric_name, e))
            overruns = []
            if timeseries:
                for ts, value in timeseries[-after_overruns:]:
                    if value > ANALYZER_OPTIMUM_RUN_DURATION:
                        overruns.append(ts)
                if len(overruns) < after_overruns:
                    send_overrun_alert = False

        if send_overrun_alert:
            level = 'alert'
            message = '%s - Analyzer run_time has overrun' % level
            status = '%s.run_time %s seconds - over running ANALYZER_OPTIMUM_RUN_DURATION of %s' % (
                metric, str(int(analyzer_run_time)), str(ANALYZER_OPTIMUM_RUN_DURATION))
            try:
                expiry = int(settings.THUNDER_CHECKS[check_app]['run_time']['expiry'])
            except Exception as e:
                logger.error('error :: %s :: failed to determine the expiry for %s up check - %s' % (
                    function_str, check_app, e))
                expiry = 900
            thunder_event = {
                'level': 'alert',
                'event_type': event_type,
                'message': message,
                'app': check_app,
                'metric': metric,
                'source': 'thunder',
                'timestamp': now,
                'expiry': expiry,
                'data': {'status': status}
            }
            try:
                submitted = thunder_send_event(skyline_app, thunder_event, log=True)
            except Exception as e:
                logger.error('error :: %s :: thunder_send_event failed - %s' % (
                    function_str, e))
            if submitted:
                logger.info('%s :: Analyzer run_time overrun thunder_send_event submitted' % (
                    function_str))
            else:
                logger.error('error :: %s :: Analyzer run_time overrun thunder_send_event failed' % (
                    function_str))
    return success
