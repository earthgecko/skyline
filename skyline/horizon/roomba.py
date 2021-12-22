from __future__ import division
# @modified 20191115 - Branch #3262: py3
# from os import kill, getpid
from os import kill

from redis import StrictRedis, WatchError
from multiprocessing import Process
from threading import Thread
from msgpack import Unpacker, packb
try:
    from types import TupleType
except ImportError:
    eliminated_in_python3 = True
from time import time, sleep
from math import ceil
# import traceback
import logging

import sys
import os.path
from os import remove as os_remove
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# @modified 20191115 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
if True:
    import settings
    # @added 20191030 - Bug #3266: py3 Redis binary objects not strings
    #                   Branch #3262: py3
    # Added a single functions to deal with Redis connection and the
    # charset='utf-8', decode_responses=True arguments required in py3
    from skyline_functions import get_redis_conn, get_redis_conn_decoded

parent_skyline_app = 'horizon'
child_skyline_app = 'roomba'
skyline_app_logger = '%sLog' % parent_skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app = '%s.%s' % (parent_skyline_app, child_skyline_app)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, parent_skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

python_version = int(sys.version_info[0])

# @added 20200727 - Feature #3650: ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
#                   Feature #3480: batch_processing
#                   Feature #3486: analyzer_batch
try:
    from settings import ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
except:
    ROOMBA_DO_NOT_PROCESS_BATCH_METRICS = False
try:
    from settings import BATCH_PROCESSING
except:
    BATCH_PROCESSING = None
try:
    BATCH_PROCESSING_NAMESPACES = list(settings.BATCH_PROCESSING_NAMESPACES)
except:
    BATCH_PROCESSING_NAMESPACES = []
try:
    from settings import BATCH_PROCESSING_DEBUG
except:
    BATCH_PROCESSING_DEBUG = None


class Roomba(Thread):
    """
    The Roomba is responsible for deleting keys older than DURATION.
    """
    def __init__(self, parent_pid, skip_mini):
        super(Roomba, self).__init__()
        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        # @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # if settings.REDIS_PASSWORD:
        #     self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        # else:
        #     self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)

        # @added 20191030 - Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
        # Added a single functions to deal with Redis connection and the
        # charset='utf-8', decode_responses=True arguments required in py3
        self.redis_conn = get_redis_conn(skyline_app)
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

        self.daemon = True
        self.parent_pid = parent_pid
        self.skip_mini = skip_mini

    def check_if_parent_is_alive(self):
        """
        Self explanatory.
        """
        try:
            kill(self.parent_pid, 0)
        except:
            # @added 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
            # Log warning
            logger.warning('warning :: parent process is dead')
            exit(0)

    def vacuum(self, i, namespace, duration):
        """
        Trim metrics that are older than settings.FULL_DURATION and purge old
        metrics.
        """
        begin = time()
        logger.info('%s :: started vacuum' % (skyline_app))

        # Discover assigned metrics
        namespace_unique_metrics = '%sunique_metrics' % str(namespace)
        # @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # unique_metrics = list(self.redis_conn.smembers(namespace_unique_metrics))
        unique_metrics = list(self.redis_conn_decoded.smembers(namespace_unique_metrics))

        # @added 20200727 - Feature #3650: ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
        #                   Feature #3480: batch_processing
        #                   Feature #3486: analyzer_batch
        if ROOMBA_DO_NOT_PROCESS_BATCH_METRICS and BATCH_PROCESSING and BATCH_PROCESSING_NAMESPACES:
            try:
                # @modified 20211127 - Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
                # Ensure that known and new batch_processing_metrics are
                # accounted for
                # batch_metrics = list(self.redis_conn_decoded.smembers('aet.analyzer.batch_processing_metrics'))
                batch_metrics1 = list(self.redis_conn_decoded.smembers('aet.analyzer.batch_processing_metrics'))
                batch_metrics2 = list(self.redis_conn_decoded.smembers('analyzer.batch_processing_metrics'))
                all_batch_metrics = batch_metrics1 + batch_metrics2
                batch_metrics = list(set(all_batch_metrics))
            except:
                logger.error('error - failed to get Redis set aet.analyzer.batch_processing_metrics')
                batch_metrics = []
            if batch_metrics:
                full_namespace_batch_metrics = []
                for base_name in batch_metrics:
                    metric = ''.join((settings.FULL_NAMESPACE, base_name))
                    full_namespace_batch_metrics.append(metric)
                del batch_metrics
                non_batch_unique_metrics = []
                for metric in unique_metrics:
                    if metric not in full_namespace_batch_metrics:
                        non_batch_unique_metrics.append(metric)
                # @modified 20200815 - Feature #3650: ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
                # del after log
                # UnboundLocalError: local variable 'full_namespace_batch_metrics' referenced before assignment
                # del full_namespace_batch_metrics
                if non_batch_unique_metrics:
                    logger.info('roomba :: batch_processing :: removing %s batch metrics from unique_metrics' % str(len(full_namespace_batch_metrics)))
                    unique_metrics = non_batch_unique_metrics
                del non_batch_unique_metrics
                # @added 20200815 - Feature #3650: ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
                del full_namespace_batch_metrics

        keys_per_processor = int(ceil(float(len(unique_metrics)) / float(settings.ROOMBA_PROCESSES)))
        if i == settings.ROOMBA_PROCESSES:
            assigned_max = len(unique_metrics)
        else:
            assigned_max = min(len(unique_metrics), i * keys_per_processor)
        assigned_min = (i - 1) * keys_per_processor
        assigned_keys = range(assigned_min, assigned_max)

        # Compile assigned metrics
        assigned_metrics = [unique_metrics[index] for index in assigned_keys]

        euthanized = 0
        blocked = 0
        trimmed_keys = 0
        active_keys = 0

        # @modified 20191016 - Task #3280: Handle py2 xange and py3 range
        #                      Branch #3262: py3
        # for i in xrange(len(assigned_metrics)):
        range_list = []
        if python_version == 2:
            for i in xrange(len(assigned_metrics)):
                range_list.append(i)
        if python_version == 3:
            for i in range(len(assigned_metrics)):
                range_list.append(i)
        for i in range_list:
            self.check_if_parent_is_alive()

            pipe = self.redis_conn.pipeline()
            now = time()
            key = assigned_metrics[i]

            try:
                # WATCH the key
                pipe.watch(key)

                # Everything below NEEDS to happen before another datapoint
                # comes in. If your data has a very small resolution (<.1s),
                # this technique may not suit you.
                raw_series = pipe.get(key)
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = sorted([unpacked for unpacked in unpacker])

                # Put pipe back in multi mode
                pipe.multi()

                # There's one value. Purge if it's too old
                try:
                    if python_version == 2:
                        if not isinstance(timeseries[0], TupleType):
                            if timeseries[0] < now - duration:
                                pipe.delete(key)
                                pipe.srem(namespace_unique_metrics, key)
                                pipe.execute()
                                euthanized += 1
                            continue
                    if python_version == 3:
                        if not isinstance(timeseries[0], tuple):
                            if timeseries[0] < now - duration:
                                pipe.delete(key)
                                pipe.srem(namespace_unique_metrics, key)
                                pipe.execute()
                                euthanized += 1
                            continue
                except IndexError:
                    continue

                # Check if the last value is too old and purge
                if timeseries[-1][0] < now - duration:
                    pipe.delete(key)
                    pipe.srem(namespace_unique_metrics, key)
                    pipe.execute()
                    euthanized += 1
                    continue

                # Remove old datapoints and duplicates from timeseries
                temp = set()
                temp_add = temp.add
                delta = now - duration
                trimmed = [
                    tuple for tuple in timeseries
                    if tuple[0] > delta and
                    tuple[0] not in temp and not
                    temp_add(tuple[0])
                ]

                # Purge if everything was deleted, set key otherwise
                if len(trimmed) > 0:
                    # Serialize and turn key back into not-an-array
                    btrimmed = packb(trimmed)
                    if len(trimmed) <= 15:
                        value = btrimmed[1:]
                    elif len(trimmed) <= 65535:
                        value = btrimmed[3:]
                        trimmed_keys += 1
                    else:
                        value = btrimmed[5:]
                        trimmed_keys += 1
                    pipe.set(key, value)
                    active_keys += 1
                else:
                    pipe.delete(key)
                    pipe.srem(namespace_unique_metrics, key)
                    euthanized += 1

                pipe.execute()

            except WatchError:
                blocked += 1
                assigned_metrics.append(key)
            except Exception as e:
                # If something bad happens, zap the key and hope it goes away
                pipe.delete(key)
                pipe.srem(namespace_unique_metrics, key)
                pipe.execute()
                euthanized += 1
                logger.info(e)
                logger.info('%s :: vacuum Euthanizing %s' % (skyline_app, key))
            finally:
                pipe.reset()

        logger.info(
            '%s :: vacuum operated on %s %d keys in %f seconds' %
            (skyline_app, namespace, len(assigned_metrics), time() - begin))
        logger.info('%s :: vaccum %s keyspace is now %d keys' % (skyline_app, namespace, (len(assigned_metrics) - euthanized)))
        logger.info('%s :: vaccum blocked %d times' % (skyline_app, blocked))
        logger.info('%s :: vacuum euthanized %d geriatric keys' % (skyline_app, euthanized))
        logger.info('%s :: vacuum processed %d active keys' % (skyline_app, active_keys))
        logger.info('%s :: vacuum potentially trimmed %d keys' % (skyline_app, trimmed_keys))

        # sleeping in the main process is more CPU efficient than sleeping
        # in the vacuum def
        # if (time() - begin < 30):
        #     logger.info(skyline_app + ' :: sleeping due to low run time...')
        #     sleep(10)

    def run(self):
        """
        Called when process initializes.
        """
        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        if os.path.isfile(skyline_app_logwait):
            try:
                os_remove(skyline_app_logwait)
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
                os_remove(skyline_app_loglock)
                logger.info('log lock file removed')
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_loglock)
                pass
        else:
            logger.info('bin/%s.d log management done' % skyline_app)

        logger.info('%s :: started roomba' % skyline_app)

        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except:
                logger.error(
                    '%s :: roomba can\'t connect to redis at socket path %s' %
                    (skyline_app, settings.REDIS_SOCKET_PATH))
                sleep(10)
                # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                # @modified 20191115 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                if settings.REDIS_PASSWORD:
                    self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                else:
                    self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                # @added 20191115 - Bug #3266: py3 Redis binary objects not strings
                #                   Branch #3262: py3
                self.redis_conn = get_redis_conn(skyline_app)
                self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

                continue

            # Spawn processes
            pids = []
            for i in range(1, settings.ROOMBA_PROCESSES + 1):
                if not self.skip_mini:
                    logger.info('%s :: starting vacuum process on mini namespace' % skyline_app)
                    p = Process(target=self.vacuum, args=(i, settings.MINI_NAMESPACE, settings.MINI_DURATION + settings.ROOMBA_GRACE_TIME))
                    pids.append(p)
                    p.start()

                logger.info('%s :: starting vacuum process' % skyline_app)
                p = Process(target=self.vacuum, args=(i, settings.FULL_NAMESPACE, settings.FULL_DURATION + settings.ROOMBA_GRACE_TIME))
                pids.append(p)
                p.start()

            # Send wait signal to zombie processes
            # for p in pids:
            #     p.join()
            # deroomba - kill any lingering vacuum processes
            # Changed to manage Roomba processes as edge cases related to I/O
            # wait have been experienced that resulted in Roomba stalling so a
            # ROOMBA_TIMEOUT setting was added and here we use the pattern
            # described by http://stackoverflow.com/users/2073595/dano at
            # http://stackoverflow.com/a/26064238 to monitor and kill any
            # stalled processes rather than using p.join(TIMEOUT) - 20160505
            # @earthgecko ref 1342
            logger.info('%s :: allowing vacuum process/es %s seconds to run' % (
                skyline_app, str(settings.ROOMBA_TIMEOUT)))
            start = time()
            while time() - start <= settings.ROOMBA_TIMEOUT:
                if any(p.is_alive() for p in pids):
                    # Just to avoid hogging the CPU
                    sleep(.1)
                else:
                    # All the processes are done, break now.
                    time_to_run = time() - start
                    logger.info('%s :: vacuum processes completed in %.2f' % (skyline_app, time_to_run))
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('%s :: timed out, killing all Roomba processes' % (skyline_app))
                for p in pids:
                    p.terminate()
                    p.join()

            # sleeping in the main process is more CPU efficient than sleeping
            # in the vacuum def also roomba is quite CPU intensive so we only
            # what to run roomba once every minute
            process_runtime = time() - now

            # @added 20210513 - Feature #4066: ROOMBA_OPTIMUM_RUN_DURATION
            try:
                ROOMBA_OPTIMUM_RUN_DURATION = int(settings.ROOMBA_OPTIMUM_RUN_DURATION)
            except Exception as e:
                logger.warn('%s :: roomba failed to determine ROOMBA_OPTIMUM_RUN_DURATION from settings, defaulting to 60 - %s' % (
                    skyline_app, e))
                ROOMBA_OPTIMUM_RUN_DURATION = 60

            # @modified 20210513 - Feature #4066: ROOMBA_OPTIMUM_RUN_DURATION
            # roomba_optimum_run_duration = 60
            roomba_optimum_run_duration = ROOMBA_OPTIMUM_RUN_DURATION
            if process_runtime < roomba_optimum_run_duration:
                sleep_for = (roomba_optimum_run_duration - process_runtime)
                logger.info('%s :: sleeping %.2f for due to low run time' % (skyline_app, sleep_for))
                sleep(sleep_for)
