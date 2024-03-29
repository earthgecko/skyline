from __future__ import division
import logging
from time import time, sleep
from threading import Thread
from multiprocessing import Process
import os
from sys import exit
from os import kill, getpid
import traceback

import settings
from skyline_functions import (
    # @modified 20220726 - Task #2732: Prometheus to Skyline
    #                      Branch #4300: prometheus
    # Moved send_graphite_metric
    # get_redis_conn, get_redis_conn_decoded, send_graphite_metric)
    get_redis_conn, get_redis_conn_decoded)
from functions.thunder.checks.app.up import thunder_check_app
from functions.thunder.checks.analyzer.run_time import thunder_check_analyzer_run_time
from functions.thunder.checks.horizon.metrics_received import thunder_check_horizon_metrics_received

# @added 20220726 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.graphite.send_graphite_metric import send_graphite_metric

# from functions.thunder.stale_metrics import thunder_stale_metrics

# @added 20230622 - Feature #4958: webapp_features_profile - status
from functions.thunder.checks.webapp.webapp_features_profile import thunder_check_webapp_features_profile

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

        redis_available = False
        last_run_timestamp = 0
        thunder_rolling_last_timestamp_key = 'thunder.rolling.last_run_timestamp'
        try:
            last_run_timestamp = self.redis_conn_decoded.get(thunder_rolling_last_timestamp_key)
            redis_available = True
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: thunder/rolling :: failed to timestamp from %s Redis key - %s' % (
                thunder_rolling_last_timestamp_key, err))
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

        check_apps_up = []
        for check_app in check_apps:
            if not redis_available:
                logger.warning('warning :: thunder/rolling :: redis not available not check %s' % (
                    check_app))
                continue
            try:
                success = thunder_check_app(self, check_app)
                if success:
                    apps_up += 1
                    logger.info('thunder/rolling :: %s is reporting UP' % (
                        check_app))
                    check_apps_up.append(check_app)
                else:
                    logger.warning('warning :: thunder/rolling :: %s is NOT reporting UP' % (
                        check_app))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: thunder/rolling :: thunder_check_app errored for %s - %s' % (
                    check_app, err))
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

            if not redis_available:
                logger.warning('warning :: thunder/rolling :: redis not available not checking analyzer run_time')
                run_time_check = False

            if run_time_check:
                try:
                    success = thunder_check_analyzer_run_time(self)
                    if success:
                        logger.info('thunder/rolling :: analyzer run_time OK')
                    else:
                        logger.warning('warning :: thunder/rolling :: analyzer run_time overruning')
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: thunder/rolling :: thunder_check_analyzer_run_time errored - %s' % (
                        err))

        # Horizon checks
        if 'horizon' in thunder_apps:
            # metrics_received check
            metrics_received_check = False
            try:
                metrics_received_check = settings.THUNDER_CHECKS['horizon']['worker.metrics_received']['run']
            except Exception as e:
                logger.error('error :: thunder/rolling :: failed to determine if horizon metrics_received check should be run - %s' % (
                    e))
            if not redis_available:
                logger.warning('warning :: thunder/rolling :: redis not available not checking horizon metrics_received')
                metrics_received_check = False

            if metrics_received_check:
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

        if not redis_available:
            for check_app in check_apps:
                check_apps_up.append(check_app)

        # @added 20230622 - Feature #4958: webapp_features_profile - status
        # Check that webapp_features_profile gunicorn workers are responsive
        # if not the main webapp_features_profile gunicorn process is issued
        # -HUP to reload the workers
        if 'webapp' in thunder_apps:
            webapp_features_profile_check = False
            try:
                webapp_features_profile_check = settings.THUNDER_CHECKS['webapp']['webapp_features_profile']['run']
            except Exception as err:
                logger.error('error :: thunder/rolling :: failed to determine if horizon metrics_received check should be run - %s' % (
                    err))
            if webapp_features_profile_check:
                try:
                    success = thunder_check_webapp_features_profile(self)
                    if success:
                        logger.info('thunder/rolling :: webapp_features_profile OK')
                    else:
                        logger.warning('warning :: thunder/rolling :: webapp_features_profile unknown')
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: thunder/rolling :: tthunder_check_horizon_metrics_recieved errored - %s' % (
                        err))

        # @added 20220328 - Feature #4018: thunder - skyline.errors
        # Consume the app RedisErrorLogHandler Redis key which is a count for
        # every error logged and create the
        # skyline.<hostname>.<skyline_app>.logged_errors metrics
        if 'thunder' not in check_apps_up:
            check_apps.append('thunder')
        flux_done = False
        vista_done = False
        for check_app in check_apps_up:
            # Do not check Redis
            if check_app == 'redis':
                continue
            check_app_name = str(check_app)
            error_count = 0
            error_count_key = '%s.log.errors.per_minute' % check_app
            # Only check flux once, although there are flux.listen and
            # flux.worker check app Redis keys, both submit errors to the same
            # Redis error key
            if check_app.startswith('flux'):
                check_app_name = 'flux'
                error_count_key = 'flux.log.errors.per_minute'
                if flux_done:
                    continue
                flux_done = True
                check_app = 'flux'
            # Only check vista once, although there are vista and vista.worker
            # check app Redis keys, both submit errors to the same Redis error
            # key
            if check_app.startswith('vista'):
                check_app_name = 'vista'
                error_count_key = 'vista.log.errors.per_minute'
                if vista_done:
                    continue
                vista_done = True
                check_app = 'vista'
            check_log_errors_file = False
            try:
                error_count_str = None
                # @modified 20230205 - Task #4844: Replace Redis getset with set with get
                # As of Redis version 6.2.0, this command is regarded as deprecated.
                # It can be replaced by SET with the GET argument when migrating or writing new code.
                # error_count_str = self.redis_conn_decoded.getset(error_count_key, 0)
                error_count_str = self.redis_conn_decoded.set(error_count_key, 0, get=True)

                if error_count_str:
                    error_count = int(error_count_str)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: thunder/rolling :: failed to getset Redis key %s - %s' % (
                    error_count_key, err))
                check_log_errors_file = True
            if check_log_errors_file:
                error_count = 0
                log_errors_file = '%s/%s.log_errors.txt' % (
                    settings.SKYLINE_TMP_DIR, check_app_name)
                error_count_array = []
                if os.path.isfile(log_errors_file):
                    logger.info('thunder/rolling :: using %s' % (
                        log_errors_file))
                    try:
                        with open(log_errors_file, 'r') as f:
                            for line in f:
                                value_string = line.replace('\n', '')
                                unquoted_value_string = value_string.replace("'", '')
                                float_value = float(unquoted_value_string)
                                error_count_array.append(float_value)
                    except:
                        error_count_array = []
                    try:
                        os.remove(log_errors_file)
                    except:
                        pass
                if error_count_array:
                    error_count = len(error_count_array)

            check_app_graphite_namespace = 'skyline.%s%s' % (check_app, SERVER_METRIC_PATH)
            send_metric_name = '%s.logged_errors' % check_app_graphite_namespace
            logger.info('thunder/rolling :: %s :: %s' % (send_metric_name, str(error_count)))
            send_graphite_metric(self, skyline_app, send_metric_name, error_count)

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
                # continue

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
