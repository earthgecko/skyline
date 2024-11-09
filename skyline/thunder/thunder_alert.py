import logging
import traceback
from time import time, sleep
from threading import Thread
from multiprocessing import Process

import os
from os import kill, getpid
import os.path
from ast import literal_eval

import settings
from skyline_functions import get_redis_conn_decoded

from analyzer.alerters import trigger_alert as analyzer_trigger_alert
from mirage.mirage_alerters import trigger_alert as mirage_trigger_alert

from functions.graphite.send_graphite_metric import send_graphite_metric

skyline_app = 'thunder'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

this_host = str(os.uname()[1])

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)

LOCAL_DEBUG = False

# @added 20240304 - Feature #5302: thunder.thunder_alert
#                   Task #5300: Decouple alerting from analysis
class ThunderAlert(Thread):
    """
    The ThunderAlert class which controls the thunder_alert thread and spawned
    processes. ThunderAlert is alerting for analyzer and mirage.
    """

    def __init__(self, parent_pid):
        """
        Initialize the ThunderAlert

        Create the :obj:`self.redis_conn_decoded` connection

        """
        super(ThunderAlert, self).__init__()
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            logger.warning('warning :: parent or current process dead')
            exit(0)


    def remove_thunder_alert_key(self, redis_thunder_alert_key):
        deleted_key = 0
        try:
            deleted_key = self.redis_conn_decoded.hdel('thunder.thunder_alert.alerts', redis_thunder_alert_key)
        except Exception as err:
            logger.error('error :: thunder_alert :: remove_thunder_alert_key failed to remove %s, err: %s' % (
                redis_thunder_alert_key, err))
        if deleted_key:
            logger.info('thunder_alert :: remove_thunder_alert_key removed %s' % (
                redis_thunder_alert_key))
        return deleted_key


    def spin_thunder_alert(self, i, redis_thunder_alert_key):
        """
        Send an alert.

        :param i: python process id

        :return: anomalous
        :rtype: boolean
        """

        spin_start = time()

        logger.info('spin_thunder_alert :: process %s - alerting for %s' % (str(i), redis_thunder_alert_key))
  
        alert_data_str = None
        try:
            alert_data_str = self.redis_conn_decoded.hget('thunder.thunder_alert.alerts', redis_thunder_alert_key)
        except Exception as err:
            logger.error('error :: spin_thunder_alert :: hget failed on %s, err: %s' % (
                redis_thunder_alert_key, err))
            self.remove_thunder_alert_key(redis_thunder_alert_key)
            return False

        alert_dict = {}
        try:
            alert_dict = literal_eval(alert_data_str)
        except Exception as err:
            logger.error('error :: spin_thunder_alert :: literal_eval failed on data from %s, alert_data_str: %s, err: %s' % (
                redis_thunder_alert_key, str(alert_data_str), err))
            self.remove_thunder_alert_key(redis_thunder_alert_key)
            return False

        logger.info('spin_thunder_alert :: alert_dict: %s' % str(alert_dict))

        for key in ['app', 'alert', 'metric', 'context']:
            try:
                if key == 'app':
                    alert_app = alert_dict[key]
                if key == 'alert':
                    alert = alert_dict[key]
                if key == 'metric':
                    metric = alert_dict[key]
                if key == 'context':
                    context = alert_dict[key]
            except Exception as err:
                logger.error('error :: spin_thunder_alert :: failed to determine %s, err: %s' % (
                    key, err))
                self.remove_thunder_alert_key(redis_thunder_alert_key)
                logger.error('error :: spin_thunder_alert :: failed on %s' % (
                    redis_thunder_alert_key))
                return False
        if alert_app == 'mirage':
            try:
                second_order_resolution_seconds = alert_dict['second_order_resolution_seconds']
            except Exception as err:
                logger.error('error :: spin_thunder_alert :: failed to determine second_order_resolution_seconds, err: %s' % (
                    err))
                self.remove_thunder_alert_key(redis_thunder_alert_key)
                logger.error('error :: spin_thunder_alert :: failed on %s' % (
                    redis_thunder_alert_key))
                return False
            try:
                triggered_algorithms = alert_dict['triggered_algorithms']
            except Exception as err:
                logger.error('error :: spin_thunder_alert :: failed to determine triggered_algorithms, err: %s' % (
                    err))
                self.remove_thunder_alert_key(redis_thunder_alert_key)
                logger.error('error :: spin_thunder_alert :: failed on %s' % (
                    redis_thunder_alert_key))
                return False

        mock_alert = False
        if 'mock_alert' in alert_dict:
            try:
                mock_alert = alert_dict['mock_alert']
            except KeyError:
                mock_alert = False
        if mock_alert:
            logger.info('spin_thunder_alert :: mock_alert, doing nothing')
            #alert_app = 'mock_alert'

        if alert_app == 'analyzer':
            try:
                analyzer_trigger_alert(alert, metric, context)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: spin_thunder_alert :: analyzer_trigger_alert failed, err: %s' % (
                    err))
                self.remove_thunder_alert_key(redis_thunder_alert_key)
                logger.error('error :: spin_thunder_alert :: failed on %s' % (
                    redis_thunder_alert_key))
                return False
        if alert_app == 'mirage':
            try:
                mirage_trigger_alert(alert, metric, second_order_resolution_seconds, context, triggered_algorithms)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: spin_thunder_alert :: mirage_trigger_alert failed, err: %s' % (
                    err))
                self.remove_thunder_alert_key(redis_thunder_alert_key)
                logger.error('error :: spin_thunder_alert :: failed on %s' % (
                    redis_thunder_alert_key))
                return False

        self.remove_thunder_alert_key(redis_thunder_alert_key)
        spin_end = time() - spin_start
        logger.info('spin_thunder_alert took %.2f seconds' % spin_end)
        return

    def run(self):
        """
        - Called when the process intializes.

        - Determine if Redis is up and discover checks to run.

        - If Redis is down and discover checks to run from the filesystem.

        - Process event.

        - Wait for the processes to finish.

        - Repeat.

        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        now = time()
        log_wait_for = now + 5
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time()
            else:
                now = log_wait_for + 1

        logger.info('starting %s thunder_alert' % skyline_app)
        if os.path.isfile(skyline_app_loglock):
            logger.error('error - bin/%s.d log management seems to have failed, continuing' % skyline_app)
            try:
                os.remove(skyline_app_loglock)
                logger.info('log lock file removed')
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_loglock)
        else:
            logger.info('bin/%s.d log management done' % skyline_app)

        try:
            SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
            if SERVER_METRIC_PATH == '.':
                SERVER_METRIC_PATH = ''
            logger.info('SERVER_METRIC_PATH is set from settings.py to %s' % str(SERVER_METRIC_PATH))
        except:
            SERVER_METRIC_PATH = ''
            logger.info('warning :: SERVER_METRIC_PATH is not declared in settings.py, defaults to \'\'')
        logger.info('skyline_app_graphite_namespace is set to %s' % str(skyline_app_graphite_namespace))

        last_sent_to_graphite = int(time())
        thunder_alerts_sent = 0

        while True:
            now = time()
            # Make sure Redis is up
            try:
                self.redis_conn_decoded.ping()
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: thunder_alert :: not connected via get_redis_conn_decoded - %s' % err)
                sleep(10)
                try:
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                    logger.info('thunder_alert :: connected via get_redis_conn_decoded')
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: thunder_alert :: cannot connect via get_redis_conn_decoded - %s' % err)

            # Determine if any alerts have been added to process
            thunder_last_run = int(time())
            while True:

                current_timestamp = int(time())

                if current_timestamp >= (last_sent_to_graphite + 60):
                    logger.info('thunder_alert :: sending Graphite metrics')
                    logger.info('thunder_alert :: alerts.sent          :: %s' % str(thunder_alerts_sent))
                    send_metric_name = '%s.thunder_alert.alerts.sent' % skyline_app_graphite_namespace
                    send_graphite_metric(self, skyline_app, send_metric_name, str(thunder_alerts_sent))

                    last_sent_to_graphite = int(time())
                    thunder_alerts_sent = 0

                if (current_timestamp - thunder_last_run) < 3:
                    sleep(3)
                thunder_last_run = int(time())

                # Report app AND Redis as up
                redis_is_up = False
                try:
                    redis_is_up = self.redis_conn_decoded.setex('thunder.thunder_alert', 120, current_timestamp)
                    if redis_is_up:
                        try:
                            logger.info('thunder_alert :: set thunder Redis key')
                            self.redis_conn_decoded.setex('redis', 120, current_timestamp)
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: thunder_alert :: could not update the Redis redis key - %s' % (
                                err))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: thunder_alert :: could not update the Redis %s key - %s' % (
                        skyline_app, err))

                if not settings.THUNDER_ENABLED:
                    logger.info('thunder_alert :: sleeping 59 seconds settings.THUNDER_ENABLED not set')
                    sleep(59)
                    logger.info('thunder_alert :: breaking after 59 seconds settings.THUNDER_ENABLED not set')
                    break

                # Determine events to process from the Redis set
                thunder_alerts = {}
                thunder_alerts_exists = False
                try:
                    thunder_alerts_exists = self.redis_conn_decoded.exists('thunder.thunder_alert.alerts')
                except Exception as err:
                    logger.error('error :: thunder_alert :: hgetall thunder.thunder_alert.alerts failed, err: %s' % err)
                if thunder_alerts_exists:
                    try:
                        thunder_alerts = self.redis_conn_decoded.hkeys('thunder.thunder_alert.alerts')
                    except Exception as err:
                        logger.error('error :: thunder_alert :: hkeys thunder.thunder_alert.alerts failed, err: %s' % err)

                # If no data was returned from Redis ensure thunder_events is
                # a set so that any event_files can be added to the set
                if thunder_alerts:
                    logger.info('%s entries in thunder.events Redis set' % str(len(thunder_alerts)))
                else:
                    logger.info('no entries in thunder.thunder_alert.alerts')
                    break

                redis_thunder_alert_keys = []
                redis_thunder_alert_keys = sorted(thunder_alerts)
                logger.info('%s alerts to send' % str(len(redis_thunder_alert_keys)))
                while len(redis_thunder_alert_keys) > 0:

                    if int(time()) >= (last_sent_to_graphite + 60):
                        break

                    # Spawn processes
                    pids = []
                    spawned_pids = []
                    pid_count = 0

                    THUNDER_PROCESSES = 2
                    for i in range(1, THUNDER_PROCESSES + 1):
                        if len(redis_thunder_alert_keys) == 0:
                            break
                        redis_thunder_alert_key = None
                        try:
                            redis_thunder_alert_key = redis_thunder_alert_keys[0]
                        except:
                            redis_thunder_alert_key = None
                        if redis_thunder_alert_key:
                            p = Process(target=self.spin_thunder_alert, args=(i, redis_thunder_alert_key))
                            pids.append(p)
                            pid_count += 1
                            logger.info('starting spin_thunder_process')
                            p.start()
                            spawned_pids.append(p.pid)
                            thunder_alerts_sent += 1
                            del redis_thunder_alert_keys[0]
                    # Self monitor processes and terminate if any spin_thunder_process
                    # that has run for longer than 30 seconds
                    p_starts = time()
                    while time() - p_starts <= 30:
                        if any(p.is_alive() for p in pids):
                            # Just to avoid hogging the CPU
                            sleep(.1)
                        else:
                            # All the processes are done, break now.
                            time_to_run = time() - p_starts
                            logger.info('%s spin_thunder_process process completed in %.2f seconds' % (
                                str(len(spawned_pids)), time_to_run))
                            break
                    else:
                        # We only enter this if we didn't 'break' above.
                        logger.info('timed out, killing all spin_thunder_process processes')
                        for p in pids:
                            p.terminate()
                            # p.join()

                    for p in pids:
                        if p.is_alive():
                            logger.info('stopping spin_thunder_process - %s' % (str(p.is_alive())))
                            killing_pid = p.pid
                            logger.info('kill spin_thunder_process with pid: %s' % (str(killing_pid)))
                            p.terminate()
                            logger.info('killed spin_thunder_process process with pid: %s' % (str(killing_pid)))

            if int(time()) >= (last_sent_to_graphite + 60):
                logger.info('sending Graphite metrics')

                logger.info('alerts.sent          :: %s' % str(thunder_alerts_sent))
                send_metric_name = '%s.thunder_alert.alerts.sent' % skyline_app_graphite_namespace
                send_graphite_metric(self, skyline_app, send_metric_name, str(thunder_alerts_sent))

                last_sent_to_graphite = int(time())
                thunder_alerts_sent = 0
