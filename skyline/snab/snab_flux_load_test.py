from __future__ import division
import logging
import uuid
import random

import requests

from time import time, sleep
from threading import Thread
from multiprocessing import Process
import os
from os import kill, getpid
import traceback
from sys import version_info
import os.path
import datetime

import settings
from skyline_functions import get_redis_conn, get_redis_conn_decoded

skyline_app = 'snab_flux_load_test'
skyline_app_logger = 'snab_flux_load_testLog'
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

python_version = int(version_info[0])
this_host = str(os.uname()[1])

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

try:
    SNAB_FLUX_LOAD_TEST_ENABLED = settings.SNAB_FLUX_LOAD_TEST_ENABLED
except:
    SNAB_FLUX_LOAD_TEST_ENABLED = True
try:
    SNAB_FLUX_LOAD_TEST_METRICS = settings.SNAB_FLUX_LOAD_TEST_METRICS
except:
    SNAB_FLUX_LOAD_TEST_METRICS = 0
try:
    SNAB_FLUX_LOAD_TEST_METRICS_PER_POST = settings.SNAB_FLUX_LOAD_TEST_METRICS_PER_POST
except:
    SNAB_FLUX_LOAD_TEST_METRICS_PER_POST = 100
try:
    SNAB_FLUX_LOAD_TEST_NAMESPACE_PREFIX = settings.SNAB_FLUX_LOAD_TEST_NAMESPACE_PREFIX
except:
    SNAB_FLUX_LOAD_TEST_NAMESPACE_PREFIX = 'skyline.snab.%s.flux_load_test' % this_host

FLUX_POST_URL = '%s/flux/metric_data_post' % settings.SKYLINE_URL

LOCAL_DEBUG = False

snab_flux_load_test_metrics_set = 'snab.flux_load_test.metrics'
snab_flux_load_test_metrics_all_set = 'snab.flux_load_test.metrics.all'


class SNAB_flux_load_test(Thread):
    """
    The SNAB class which controls the snab thread and spawned
    processes.

    """

    def __init__(self, parent_pid):
        """
        Initialize the SNAB_flux_load_test
        """
        super(SNAB_flux_load_test, self).__init__()
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
            # @added 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
            # Log warning
            logger.warn('warning :: parent or current process dead')
            exit(0)

    def spin_snab_flux_load_test_process(self, current_timestamp):
        """
        Push metrics to flux.

        :param i: python process id

        :return: True
        :rtype: boolean
        """

        spin_start = time()
        snab_flux_load_test_metrics_set = 'snab.flux_load_test.metrics'
        snab_flux_load_test_metrics_all_set = 'snab.flux_load_test.metrics.all'

        spin_snab_flux_load_test_process_pid = os.getpid()
        logger.info('spin_snab_flux_load_test_process - pid %s, sending %s metrics to flux at %s metrics per POST' % (
            str(spin_snab_flux_load_test_process_pid),
            str(SNAB_FLUX_LOAD_TEST_METRICS),
            str(SNAB_FLUX_LOAD_TEST_METRICS_PER_POST)))
        if not SNAB_FLUX_LOAD_TEST_METRICS:
            logger.info('nothing to do')
            return

        snab_flux_load_test_metrics = []
        try:
            snab_flux_load_test_metrics = sorted(list(self.redis_conn_decoded.smembers(snab_flux_load_test_metrics_set)))
        except Exception as e:
            logger.error('error :: could not query Redis for set %s - %s' % (snab_flux_load_test_metrics_set, e))
        logger.info('snab_flux_load_test_metrics determined %s test metrics from Redis' % (
            str(len(snab_flux_load_test_metrics))))

        if snab_flux_load_test_metrics:
            try:
                self.redis_conn.sadd(snab_flux_load_test_metrics_all_set, *set(snab_flux_load_test_metrics))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to add multiple members to the %s Redis set' % snab_flux_load_test_metrics_all_set)

        snab_flux_load_test_metrics_all = []
        try:
            snab_flux_load_test_metrics_all = sorted(list(self.redis_conn_decoded.smembers(snab_flux_load_test_metrics_all_set)))
        except Exception as e:
            logger.error('error :: could not query Redis for set %s - %s' % (snab_flux_load_test_metrics_all_set, e))
        logger.info('snab_flux_load_test_metrics_all determined %s known test metrics from Redis' % (
            str(len(snab_flux_load_test_metrics_all))))

        check_for_removals = True
        if len(snab_flux_load_test_metrics) != len(snab_flux_load_test_metrics_all):
            check_for_removals = False
        if len(snab_flux_load_test_metrics) > SNAB_FLUX_LOAD_TEST_METRICS:
            check_for_removals = True
        if check_for_removals:
            logger.info('checking what snab test metrics need to be removed')
            remove_from_snab_set = False
            if len(snab_flux_load_test_metrics) > SNAB_FLUX_LOAD_TEST_METRICS:
                remove_from_snab_set = True
            metrics_to_remove = []
            if remove_from_snab_set:
                metrics_to_remove = snab_flux_load_test_metrics_all[SNAB_FLUX_LOAD_TEST_METRICS:]
                snab_flux_load_test_metrics = snab_flux_load_test_metrics[0:SNAB_FLUX_LOAD_TEST_METRICS]
            if remove_from_snab_set:
                if metrics_to_remove:
                    logger.info('removing %s metrics from %s Redis set' % (
                        str(len(metrics_to_remove)), snab_flux_load_test_metrics_set))
                    try:
                        self.redis_conn.srem(snab_flux_load_test_metrics_set, *set(metrics_to_remove))
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to remove multiple members from %s Redis set' % snab_flux_load_test_metrics_set)

            logger.info('getting list of metrics.unique_metrics from Redis set')
            full_uniques = '%sunique_metrics' % settings.FULL_NAMESPACE
            try:
                unique_metrics = list(self.redis_conn_decoded.smembers(full_uniques))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to generate a list from %s Redis set' % full_uniques)
            logger.info('checking if any snab test metrics need to be removed from metrics.unique_metrics')
            for metric in unique_metrics:
                if SNAB_FLUX_LOAD_TEST_NAMESPACE_PREFIX in metric:
                    if metric.startswith(settings.FULL_NAMESPACE):
                        base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
                    else:
                        base_name = metric
                    if base_name not in snab_flux_load_test_metrics:
                        try:
                            self.redis_conn.srem(full_uniques, str(metric))
                        except:
                            pass
            del unique_metrics
            del metrics_to_remove

            flux_last_key_prefix = 'flux.last.%s.*' % SNAB_FLUX_LOAD_TEST_NAMESPACE_PREFIX
            logger.info('generating list of all possible %s Redis keys' % flux_last_key_prefix)
            snab_remove_flux_last_keys = []
            for base_name in snab_flux_load_test_metrics_all:
                if base_name not in snab_flux_load_test_metrics:
                    try:
                        flux_last_key = 'flux.last.%s' % base_name
                        snab_remove_flux_last_keys.append(flux_last_key)
                    except:
                        pass
            del snab_flux_load_test_metrics_all
            logger.info('getting list of flux.last Redis keys')
            flux_last_keys = list(self.redis_conn_decoded.scan_iter(match=flux_last_key_prefix))
            logger.info('there are potentially %s flux.last keys that need to be removed from Redis for not in use snab test metrics' % str(len(snab_remove_flux_last_keys)))
            logger.info('checking if any of the %s flux.last keys need to be removed from Redis' % str(len(flux_last_keys)))
            snab_flux_last_keys_to_remove = []
            for flux_last_key in snab_remove_flux_last_keys:
                if flux_last_key in flux_last_keys:
                    snab_flux_last_keys_to_remove.append(flux_last_key)
            del flux_last_keys
            del snab_remove_flux_last_keys
            if snab_flux_last_keys_to_remove:
                for flux_last_key in snab_flux_last_keys_to_remove:
                    try:
                        self.redis_conn.delete(flux_last_key)
                    except:
                        continue
                logger.info('deleted %s flux.last keys for not in use snab test metrics' % str(len(snab_flux_last_keys_to_remove)))
            else:
                logger.info('there are no flux.last keys for not in use snab test metrics to delete')
            del snab_flux_last_keys_to_remove

        adding_metrics = 0
        if len(snab_flux_load_test_metrics) < SNAB_FLUX_LOAD_TEST_METRICS:
            adding_metrics = SNAB_FLUX_LOAD_TEST_METRICS - len(snab_flux_load_test_metrics)
            logger.info('adding %s metrics to snab_flux_load_test_metrics and %s Redis set' % (
                str(adding_metrics), snab_flux_load_test_metrics_set))

        if len(snab_flux_load_test_metrics) < SNAB_FLUX_LOAD_TEST_METRICS:
            snab_flux_load_test_metrics_all = []
            known_snab_metrics_to_added = 0
            try:
                snab_flux_load_test_metrics_all = list(self.redis_conn_decoded.smembers(snab_flux_load_test_metrics_all_set))
            except Exception as e:
                logger.error('error :: could not query Redis for set %s - %s' % (snab_flux_load_test_metrics_all_set, e))
                snab_flux_load_test_metrics_all = []
            logger.info('snab_flux_load_test_metrics_all determined %s test metrics from Redis' % (
                str(len(snab_flux_load_test_metrics_all))))
            if snab_flux_load_test_metrics_all:
                unique_snab_flux_load_test_metrics_set = set(snab_flux_load_test_metrics)
                unique_snab_flux_load_test_metrics_all_set = set(snab_flux_load_test_metrics_all)
                known_snab_metrics_to_add = []
                set_difference = unique_snab_flux_load_test_metrics_all_set.difference(unique_snab_flux_load_test_metrics_set)
                for metric in set_difference:
                    known_snab_metrics_to_add.append(metric)
                if known_snab_metrics_to_add:
                    known_snab_metrics_to_add = list(set(known_snab_metrics_to_add))
                for metric in known_snab_metrics_to_add:
                    if len(snab_flux_load_test_metrics) < SNAB_FLUX_LOAD_TEST_METRICS:
                        snab_flux_load_test_metrics.append(metric)
                        known_snab_metrics_to_added += 1
            logger.info('%s known_snab_metrics added snab_flux_load_test_metrics' % (
                str(known_snab_metrics_to_added)))
            del snab_flux_load_test_metrics_all

        while len(snab_flux_load_test_metrics) < SNAB_FLUX_LOAD_TEST_METRICS:
            new_uuid = str(uuid.uuid4())
            new_metric_uuid = new_uuid.replace('-', '.')
            slot = str(round(random.random(), 2))  # nosec
            new_metric = '%s.%s.%s' % (SNAB_FLUX_LOAD_TEST_NAMESPACE_PREFIX, slot, new_metric_uuid)
            snab_flux_load_test_metrics.append(new_metric)
            # Add to the snab_flux_load_test_metrics_set Redis set
            try:
                self.redis_conn.sadd(snab_flux_load_test_metrics_set, new_metric)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to add item to Redis set %s' % (
                    snab_flux_load_test_metrics_set))

        if adding_metrics:
            logger.info('snab_flux_load_test_metrics now has %s metrics' % (
                str(len(snab_flux_load_test_metrics))))

        logger.info('snab_flux_load_test_metrics has %s metrics' % (
            str(len(snab_flux_load_test_metrics))))
        logger.info('snab_flux_load_test_metrics has %s unique metrics' % (
            str(len(set(snab_flux_load_test_metrics)))))
        try:
            self.redis_conn.sadd(snab_flux_load_test_metrics_set, *set(snab_flux_load_test_metrics))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to add multiple members to the %s Redis set' % snab_flux_load_test_metrics)

        epoch_Y = datetime.datetime.today().year
        epoch_m = datetime.datetime.today().month
        epoch_d = datetime.datetime.today().day
        epoch_H = datetime.datetime.today().hour
        epoch_M = datetime.datetime.today().minute
        epoch_S = 0
        initial_datetime = datetime.datetime(epoch_Y, epoch_m, epoch_d, epoch_H, epoch_M, epoch_S)
        one_minute = datetime.timedelta(minutes=1)
        epoch_datetime = initial_datetime - one_minute
        epoch_timestamp = int(epoch_datetime.strftime('%s'))

        connect_timeout = 5
        read_timeout = 5
        use_timeout = (int(connect_timeout), int(read_timeout))

        if settings.WEBAPP_AUTH_ENABLED:
            user = str(settings.WEBAPP_AUTH_USER)
            password = str(settings.WEBAPP_AUTH_USER_PASSWORD)

        post_count = 0
        posted_count = 0
        for metric in snab_flux_load_test_metrics:
            if not post_count:
                post_data_dict = {
                    'key': settings.FLUX_SELF_API_KEY,
                    'metrics': []
                }
            if post_count < SNAB_FLUX_LOAD_TEST_METRICS_PER_POST:
                post_data_dict['metrics'].append({'metric': metric, 'timestamp': str(epoch_timestamp), 'value': str(round(random.random(), 2))})  # nosec
                post_count += 1
            if post_count == SNAB_FLUX_LOAD_TEST_METRICS_PER_POST:
                response = None
                try:
                    response = requests.post(FLUX_POST_URL, auth=(user, password), json=post_data_dict, timeout=use_timeout, verify=settings.VERIFY_SSL)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to post %s metrics, sleeping for 1 second' % (
                        str(post_count)))
                    response = None
                    sleep(1)
                if response:
                    logger.info('posted %s metrics to flux with status code %s returned' % (str(post_count), str(response.status_code)))
                    posted_count += post_count
                    post_count = 0
            running_for = int(time()) - current_timestamp
            if running_for > 55:
                logger.info('load test has run for longer than 55 seconds, stopping')
                post_count = 0
                break

        if post_count:
            response = None
            try:
                response = requests.post(FLUX_POST_URL, auth=(user, password), json=post_data_dict, timeout=use_timeout, verify=settings.VERIFY_SSL)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to post %s metrics' % (
                    str(post_count)))
                response = None
            if response:
                posted_count += post_count

        spin_end = time() - spin_start
        logger.info('spin_snab_flux_load_test_process posted %s metrics to flux in %.2f seconds' % (str(posted_count), spin_end))

        return

    def run(self):
        """
        - Called when the process intializes.

        - Determine if Redis is up and discover checks to run.

        - Divide and assign each process a metric check to analyse and add
          results to source Redis set.

        - Wait for the processes to finish.

        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        if os.path.isfile(skyline_app_logwait):
            try:
                os.remove(skyline_app_logwait)
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_logwait)
                pass

        now = time()
        log_wait_for = now + 5
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time()
            else:
                now = log_wait_for + 1

        logger.info('starting %s run' % skyline_app)
        if os.path.isfile(skyline_app_loglock):
            logger.error('error - bin/%s.d log management seems to have failed, continuing' % skyline_app)
            try:
                os.remove(skyline_app_loglock)
                logger.info('log lock file removed')
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_loglock)
                pass
        else:
            logger.info('bin/%s.d log management done' % skyline_app)

        logger.info('starting SNAB_flux_load_test')

        while 1:
            now = time()
            # Make sure Redis is up
            try:
                self.redis_conn.ping()
                logger.info('pinged Redis via get_redis_conn')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                sleep(10)
                try:
                    self.redis_conn = get_redis_conn(skyline_app)
                    logger.info('connected via get_redis_conn')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: not connected via get_redis_conn')
                continue
            try:
                self.redis_conn_decoded.ping()
                logger.info('pinged Redis via get_redis_conn_decoded')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: not connected via get_redis_conn_decoded')
                sleep(10)
                try:
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                    logger.info('connected via get_redis_conn_decoded')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cannot connect to get_redis_conn_decoded')
                continue

            """
            Run load test
            """
            while True:

                current_timestamp = int(time())

                logger.info('snab_flux_load_test - running load test')

                # Spawn processes
                pids = []
                spawned_pids = []
                pid_count = 0
                p = Process(target=self.spin_snab_flux_load_test_process, args=(current_timestamp,))
                pids.append(p)
                pid_count += 1
                logger.info('starting 1 of %s spin_snab_process' % (str(pid_count)))
                p.start()
                spawned_pids.append(p.pid)

                # Send wait signal to zombie processes
                # for p in pids:
                #     p.join()
                # Self monitor processes and terminate if any spin_snab_process
                # that has run for longer than 58 seconds
                p_starts = time()
                while time() - p_starts <= 58:
                    if any(p.is_alive() for p in pids):
                        # Just to avoid hogging the CPU
                        sleep(.1)
                    else:
                        # All the processes are done, break now.
                        time_to_run = time() - p_starts
                        logger.info('1 spin_snab_flux_load_test_process completed in %.2f seconds' % (time_to_run))
                        break
                else:
                    # We only enter this if we didn't 'break' above.
                    logger.info('timed out, killing spin_snab_flux_load_test_process process')
                    for p in pids:
                        p.terminate()
                        # p.join()

                for p in pids:
                    if p.is_alive():
                        logger.info('stopping spin_snab_flux_load_test_process - %s' % (str(p.is_alive())))
                        p.join()

                process_runtime = time() - current_timestamp
                if process_runtime < 60:
                    sleep_for = (60 - process_runtime)
                    logger.info('sleeping for %.2f seconds' % sleep_for)
                    sleep(sleep_for)
                    try:
                        del sleep_for
                    except:
                        pass
