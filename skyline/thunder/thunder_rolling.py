from __future__ import division
import logging
from time import time, sleep
from threading import Thread
from multiprocessing import Process
import os
from os import kill, getpid
import traceback

import settings
from skyline_functions import (
    get_redis_conn, get_redis_conn_decoded)

# from functions.thunder.stale_metrics import thunder_stale_metrics

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

full_uniques = '%sunique_metrics' % settings.FULL_NAMESPACE

LOCAL_DEBUG = False


class RollingThunder(Thread):
    """
    The RollingThunder class which controls the thunder/rolling thread and
    spawned processes. thunder/rolling carries out internal and external checks
    and sends any events to thunder for various Skyline app operations and
    dependencies, such as Redis, mariadb, memcache and Graphite.
    In a distributed Skyline set up, thunder/rolling can alert on other Skyline
    apps in the cluster (TBD)

    """

    def __init__(self, parent_pid):
        """
        Initialize Rolling
        """
        super(RollingThunder, self).__init__()
        self.redis_conn = get_redis_conn(skyline_app)
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
            exit(0)

    def rolling_process(self, i):
        """
        Create and manage the required lists and Redis sets
        """
        spin_start = time()
        logger.info('thunder/rolling :: rolling_process started')

        last_run_timestamp = 0
        thunder_rolling_last_timestamp_key = 'thunder.rolling.last_run_timestamp'
        try:
            last_run_timestamp = self.redis_conn_decoded.get(thunder_rolling_last_timestamp_key)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: thunder/rolling :: failed to timestamp from %s Redis key - %s' % (
                thunder_rolling_last_timestamp_key, e))
            last_run_timestamp = 0
        if last_run_timestamp:
            logger.info('thunder/rolling :: %s Redis key has not expired, not running' % (
                thunder_rolling_last_timestamp_key))
            return

        # Check apps
        thunder_apps = list(settings.THUNDER_CHECKS.keys())
        check_apps = []
        for thunder_app in thunder_apps:
            if settings.THUNDER_CHECKS[thunder_app]['up']['run']:
                check_apps.append(thunder_app)
        apps_up = 0
        thunder_check_app = None
        if len(check_apps) > 0:
            try:
                from functions.thunder.checks.app.up import thunder_check_app
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: thunder/rolling :: failed to import thunder_check_app - %s' % (
                    e))

        for check_app in check_apps:
            try:
                if thunder_check_app:
                    success = thunder_check_app(self, check_app)
                    if success:
                        apps_up += 1
                        logger.info('thunder/rolling :: %s is reporting UP' % (
                            check_app))
                    else:
                        logger.warning('warning :: thunder/rolling :: %s is NOT reporting UP' % (
                            check_app))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: thunder/rolling :: thunder_check_app errored for %s - %s' % (
                    check_app, e))
        if check_apps:
            logger.info('thunder/rolling :: %s Skyline apps checked and %s apps reporting UP' % (
                str(len(check_apps)), str(apps_up)))

        # Analyzer checks
        if 'analyzer' in thunder_apps:
            # run_time check
            run_time_check = False
            try:
                run_time_check = settings.THUNDER_CHECKS['analyzer']['run_time']['run']
            except Exception as e:
                logger.error('error :: thunder/rolling :: failed to determine if analyzer run_time check should be run - %s' % (
                    e))
            if run_time_check:
                thunder_check_analyzer_run_time = None
                try:
                    from functions.thunder.checks.analyzer.run_time import thunder_check_analyzer_run_time
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: thunder/rolling :: failed to import thunder_check_analyzer_run_time - %s' % (
                        e))
                if thunder_check_analyzer_run_time:
                    try:
                        success = thunder_check_analyzer_run_time(self)
                        if success:
                            logger.info('thunder/rolling :: analyzer run_time OK')
                        else:
                            logger.warning('warning :: thunder/rolling :: analyzer run_time overruning')
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: thunder/rolling :: thunder_check_analyzer_run_time errored - %s' % (
                            e))
                else:
                    logger.warning('warning :: thunder/rolling :: did not check Analyzer run_time as thunder_check_analyzer_run_time failed to import')

        # Horizon checks
        if 'horizon' in thunder_apps:
            # metrics_received check
            metrics_received_check = False
            try:
                metrics_received_check = settings.THUNDER_CHECKS['horizon']['worker.metrics_received']['run']
            except Exception as e:
                logger.error('error :: thunder/rolling :: failed to determine if horizon metrics_received check should be run - %s' % (
                    e))
            if metrics_received_check:
                thunder_check_horizon_metrics_received = None
                try:
                    from functions.thunder.checks.horizon.metrics_received import thunder_check_horizon_metrics_received
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: thunder/rolling :: failed to import thunder_check_analyzer_run_time - %s' % (
                        e))
                if thunder_check_horizon_metrics_received:
                    try:
                        success = thunder_check_horizon_metrics_received(self)
                        if success:
                            logger.info('thunder/rolling :: horizon metrics_received OK')
                        else:
                            logger.warning('warning :: thunder/rolling :: horizon metrics_received has significantly changed')
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: thunder/rolling :: tthunder_check_horizon_metrics_recieved errored - %s' % (
                            e))
                else:
                    logger.warning('warning :: thunder/rolling :: did not check horizon metrics_recieved as thunder_check_horizon_metrics_received failed to import')

        spin_end = time() - spin_start
        logger.info('thunder/rolling :: checks took %.2f seconds' % spin_end)
        return

    def run(self):
        """
        - Called when the process intializes.

        - Determine if Redis is up

        - Spawn a rolling process to do checks

        - Wait for the process to finish.

        - run_every 60 seconds
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

        logger.info('thunder/rolling :: starting %s/rolling' % skyline_app)

        try:
            SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
            if SERVER_METRIC_PATH == '.':
                SERVER_METRIC_PATH = ''
        except Exception as e:
            SERVER_METRIC_PATH = ''
            logger.warning('warning :: thunder/rolling :: settings.SERVER_METRICS_NAME is not declared in settings.py, defaults to \'\' - %s' % e)

        run_every = 60

        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: thunder/rolling cannot connect to redis at socket path %s - %s' % (
                    settings.REDIS_SOCKET_PATH, e))
                sleep(10)
                try:
                    self.redis_conn = get_redis_conn(skyline_app)
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                except Exception as e:
                    logger.info(traceback.format_exc())
                    logger.error('error :: thunder/rolling cannot connect to get_redis_conn - %s' % e)
                continue

            # Report app up
            try:
                self.redis_conn.setex('thunder.rolling', 120, now)
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: thunder/rolling :: could not update the Redis analyzer.thunder/rolling key - %s' % e)

            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            try:
                p = Process(target=self.rolling_process, args=(0,))
                pids.append(p)
                pid_count += 1
                logger.info('thunder/rolling :: starting rolling_process')
                p.start()
                spawned_pids.append(p.pid)
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: thunder/rolling :: failed to spawn process - %s' % e)

            # Self monitor processes and terminate if any rolling_process that
            # has run for longer than 180 seconds
            p_starts = time()
            while time() - p_starts <= run_every:
                if any(p.is_alive() for p in pids):
                    # Just to avoid hogging the CPU
                    sleep(.1)
                else:
                    # All the processes are done, break now.
                    time_to_run = time() - p_starts
                    logger.info('thunder/rolling :: rolling_process completed in %.2f seconds' % (time_to_run))
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('thunder/rolling :: timed out, killing rolling_process process')
                for p in pids:
                    logger.info('thunder/rolling :: killing rolling_process process')
                    p.terminate()
                    logger.info('thunder/rolling :: killed rolling_process process')

            for p in pids:
                if p.is_alive():
                    try:
                        logger.info('thunder/rolling :: stopping rolling_process - %s' % (str(p.is_alive())))
                        p.terminate()
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: thunder/rolling :: failed to stop rolling_process - %s' % e)

            process_runtime = time() - now
            if process_runtime < run_every:
                sleep_for = (run_every - process_runtime)

                process_runtime_now = time() - now
                sleep_for = (run_every - process_runtime_now)

                logger.info('thunder/rolling :: sleeping for %.2f seconds due to low run time...' % sleep_for)
                sleep(sleep_for)
                try:
                    del sleep_for
                except Exception as e:
                    logger.error('error :: thunder/rolling :: failed to del sleep_for - %s' % e)
            try:
                del process_runtime
            except Exception as e:
                logger.error('error :: thunder/rolling :: failed to del process_runtime - %s' % e)
