from __future__ import division
# from os import kill, system
from os import kill
from redis import StrictRedis, WatchError
from multiprocessing import Process
try:
    from Queue import Empty
except ImportError:
    from queue import Empty
from msgpack import packb
from time import time, sleep

import traceback
import logging
# import socket

# import sys
import os.path
from os import remove as os_remove

# @added 20190130 - Task #2690: Test Skyline on Python-3.6.7
#                   Branch #3262: py3
from sys import version_info

import settings
from skyline_functions import send_graphite_metric

parent_skyline_app = 'horizon'
child_skyline_app = 'worker'
skyline_app_logger = '%sLog' % parent_skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app = '%s.%s' % (parent_skyline_app, child_skyline_app)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, parent_skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

skyline_app_graphite_namespace = 'skyline.%s%s.%s' % (
    parent_skyline_app, SERVER_METRIC_PATH, child_skyline_app)

LOCAL_DEBUG = False

# @added 20170319 - Feature #1978: worker - DO_NOT_SKIP_LIST
try:
    # @modified 20200606 - Bug #3572: Apply list to settings import
    DO_NOT_SKIP_LIST = list(settings.DO_NOT_SKIP_LIST)
except:
    DO_NOT_SKIP_LIST = []

# @added 20201103 - Feature #3820: HORIZON_SHARDS
try:
    HORIZON_SHARDS = settings.HORIZON_SHARDS.copy()
except:
    HORIZON_SHARDS = {}
try:
    HORIZON_SHARD_DEBUG = settings.HORIZON_SHARD_DEBUG
except:
    HORIZON_SHARD_DEBUG = True

number_of_horizon_shards = 0
this_host = str(os.uname()[1])
HORIZON_SHARD = 0
if HORIZON_SHARDS:
    import zlib
    number_of_horizon_shards = len(HORIZON_SHARDS)
    HORIZON_SHARD = HORIZON_SHARDS[this_host]

# @added 20190130 - Task #2690: Test Skyline on Python-3.6.7
#                   Branch #3262: py3
python_version = int(version_info[0])


class Worker(Process):
    """
    The worker processes chunks from the queue and appends
    the latest datapoints to their respective timesteps in Redis.
    """
    # @modified 20201017 - Feature #3788: snab_flux_load_test
    #                      Feature #3680: horizon.worker.datapoints_sent_to_redis
    # Added worker_number
    def __init__(self, queue, parent_pid, skip_mini, worker_number, canary=False):
        super(Worker, self).__init__()
        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        if settings.REDIS_PASSWORD:
            # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            # self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
            self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
        else:
            # self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
            self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
        self.q = queue
        self.parent_pid = parent_pid
        self.daemon = True
        self.canary = canary
        self.skip_mini = skip_mini
        # @added 20201017 - Feature #3788: snab_flux_load_test
        #                   Feature #3680: horizon.worker.datapoints_sent_to_redis
        # Added worker_number
        self.worker_number = worker_number

    def check_if_parent_is_alive(self):
        """
        Self explanatory.
        """
        try:
            kill(self.parent_pid, 0)
        except:
            exit(0)

    def in_skip_list(self, metric_name):
        """
        Check if the metric is in SKIP_LIST.

        # @added 20170319 - Feature #1978: worker - DO_NOT_SKIP_LIST
        The SKIP_LIST allows for a string match or a match on dotted elements
        within the metric namespace.

        """

        # @modified 20170319 - Feature #1978: worker - DO_NOT_SKIP_LIST
        # Allow for dotted element matches and DO_NOT_SKIP_LIST
        # for to_skip in settings.SKIP_LIST:
        #    if to_skip in metric_name:
        #        return True
        # return False

        # @added 20190130 - Task #2690: Test Skyline on Python-3.6.7
        #                   Branch #3262: py3
        #                   Bug #3266: py3 Redis binary objects not strings
        if python_version == 3:
            str_metric_name = str(metric_name)
            metric_name = str_metric_name

        metric_namespace_elements = metric_name.split('.')
        process_metric = True

        for to_skip in settings.SKIP_LIST:
            if to_skip in metric_name:
                process_metric = False
                break
            to_skip_namespace_elements = to_skip.split('.')
            elements_matched = set(metric_namespace_elements) & set(to_skip_namespace_elements)
            if len(elements_matched) == len(to_skip_namespace_elements):
                process_metric = False
                break

        if not process_metric:
            for do_not_skip in DO_NOT_SKIP_LIST:
                if do_not_skip in metric_name:
                    process_metric = True
                    break
                do_not_skip_namespace_elements = do_not_skip.split('.')
                elements_matched = set(metric_namespace_elements) & set(do_not_skip_namespace_elements)
                if len(elements_matched) == len(do_not_skip_namespace_elements):
                    process_metric = True
                    break

        if not process_metric:
            # skip
            return True

        return False

# @added 20201103 - Feature #3820: HORIZON_SHARDS
    def in_shard(self, metric_name):
        """
        Check if the metric belongs to the Horizon instance shard.
        """
        if not HORIZON_SHARDS:
            return True
        metric_as_bytes = str(metric_name).encode()
        value = zlib.adler32(metric_as_bytes)
        modulo_result = value % number_of_horizon_shards
        if modulo_result == HORIZON_SHARD:
            return True
        else:
            return False
        return True

    def run(self):
        """
        Called when the process intializes.
        """
        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        # @modified 20201017 - Feature #3788: snab_flux_load_test
        #                      Feature #3680: horizon.worker.datapoints_sent_to_redis
        # Only do log management with the canary worker
        if self.canary:
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

        logger.info('starting %s worker number %s run' % (skyline_app, self.worker_number))
        # @modified 20201017 - Feature #3788: snab_flux_load_test
        #                      Feature #3680: horizon.worker.datapoints_sent_to_redis
        if self.canary:
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

        logger.info('%s :: started worker %s' % (skyline_app, str(self.worker_number)))

        # @added 20201103 - Feature #3820: HORIZON_SHARDS
        if HORIZON_SHARDS:
            logger.info('%s :: HORIZON_SHARDS declared, this horizon instance is assigned shard %s' % (skyline_app, str(HORIZON_SHARD)))

        FULL_NAMESPACE = settings.FULL_NAMESPACE
        MINI_NAMESPACE = settings.MINI_NAMESPACE
        MAX_RESOLUTION = settings.MAX_RESOLUTION
        full_uniques = '%sunique_metrics' % FULL_NAMESPACE
        mini_uniques = '%sunique_metrics' % MINI_NAMESPACE
        pipe = self.redis_conn.pipeline()

        last_send_to_graphite = time()
        queue_sizes = []

        # @added 20200815 - Feature #3680: horizon.worker.datapoints_sent_to_redis
        datapoints_sent_to_redis = 0
        last_datapoints_to_redis = int(time())

        # @added 20201103 - Feature #3820: HORIZON_SHARDS
        if HORIZON_SHARDS:
            horizon_shard_assigned_metrics = []
            horizon_shard_dropped_metrics = []
        # @added 20201120 - Feature #3820: HORIZON_SHARDS
        metrics_received = []

        # python-2.x and python3.x handle while 1 and while True differently
        # while 1:
        running = True
        while running:

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except:
                logger.error('%s :: cannot connect to Redis at socket path %s' % (skyline_app, settings.REDIS_SOCKET_PATH))
                sleep(10)
                # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                if settings.REDIS_PASSWORD:
                    # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                    self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
                else:
                    # self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                    self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
                pipe = self.redis_conn.pipeline()
                continue

            try:
                # Get a chunk from the queue with a 15 second timeout
                chunk = self.q.get(True, 15)
                # @modified 20170317 - Feature #1978: worker - DO_NOT_SKIP_LIST
                # now = time()
                now = int(time())

                for metric in chunk:

                    # @added 20201120 - Feature #3820: HORIZON_SHARDS
                    try:
                        metrics_received.append(metric[0])
                    except:
                        pass

                    # @added 20201103 - Feature #3820: HORIZON_SHARDS
                    # If a metric does not map to the HORIZON_SHARD, drop it
                    # and continue
                    if HORIZON_SHARDS and number_of_horizon_shards:
                        if not self.in_shard(metric[0]):
                            horizon_shard_dropped_metrics.append(metric[0])
                            continue
                        else:
                            horizon_shard_assigned_metrics.append(metric[0])

                    # Check if we should skip it
                    if self.in_skip_list(metric[0]):
                        continue

                    # Bad data coming in
                    # @modified 20190130 - Task #2690: Test Skyline on Python-3.6.7
                    #                      Branch #3262: py3
                    #                      Bug #3266: py3 Redis binary objects not strings
                    # if metric[1][0] < now - MAX_RESOLUTION:
                    #     continue
                    try:
                        if int(metric[1][0]) < now - MAX_RESOLUTION:
                            continue
                    except:
                        pass

                    # Append to messagepack main namespace
                    # @modified 20190130 - Task #2690: Test Skyline on Python-3.6.7
                    #                      Branch #3262: py3
                    #                      Bug #3266: py3 Redis binary objects not strings
                    # key = ''.join((FULL_NAMESPACE, metric[0]))
                    key = ''.join((FULL_NAMESPACE, str(metric[0])))
                    if LOCAL_DEBUG:
                        logger.info('debug :: worker :: adding metric Redis key - %s' % str(key))

                    # @modified 20190130 - Task #2690: Test Skyline on Python-3.6.7
                    #                      Branch #3262: py3
                    #                      Bug #3266: py3 Redis binary objects not strings
                    # pipe.append(key, packb(metric[1]))
                    # pipe.sadd(full_uniques, key)
                    try:
                        pipe.append(str(key), packb(metric[1]))
                        # @added 20200815 - Feature #3680: horizon.worker.datapoints_sent_to_redis
                        datapoints_sent_to_redis += 1
                    except Exception as e:
                        logger.error('%s :: error on pipe.append: %s' % (skyline_app, str(e)))
                    try:
                        # pipe.sadd(full_uniques, key)
                        pipe.sadd(full_uniques, str(key))
                    except Exception as e:
                        logger.error('%s :: error on pipe.sadd: %s' % (skyline_app, str(e)))

                    if not self.skip_mini:
                        # Append to mini namespace
                        # @modified 20190130 - Task #2690: Test Skyline on Python-3.6.7
                        #                      Branch #3262: py3
                        #                      Bug #3266: py3 Redis binary objects not strings
                        # mini_key = ''.join((MINI_NAMESPACE, metric[0]))
                        mini_key = ''.join((MINI_NAMESPACE, str(metric[0])))
                        pipe.append(mini_key, packb(metric[1]))
                        pipe.sadd(mini_uniques, mini_key)

                    # @modified 20190130 - Task #2690: Test Skyline on Python-3.6.7
                    #                      Branch #3262: py3
                    #                      Bug #3266: py3 Redis binary objects not strings
                    # pipe.execute()
                    try:
                        pipe.execute()
                    except Exception as e:
                        logger.error('%s :: error on pipe.execute: %s' % (skyline_app, str(e)))
            except Empty:
                logger.info('%s :: worker queue is empty and timed out' % skyline_app)
            except WatchError:
                logger.error('%s :: WatchError - %s' % (skyline_app, str(key)))
            except NotImplementedError:
                pass
            except Exception as e:
                # @added 20190130 - Task #2690: Test Skyline on Python-3.6.7
                #                   Branch #3262: py3
                #                   Bug #3266: py3 Redis binary objects not strings
                # Added traceback
                logger.error(traceback.format_exc())
                logger.error('%s :: error: %s' % (skyline_app, str(e)))

            # Log progress
            if self.canary:
                logger.info('%s :: queue size at %d' % (skyline_app, self.q.qsize()))
                queue_sizes.append(self.q.qsize())
                # Only send average queue mertics to graphite once per 10 seconds
                now = time()
                last_sent_graphite = now - last_send_to_graphite
                if last_sent_graphite > 10:
                    number_queue_sizes = len(queue_sizes)
                    total_of_queue_sizes = sum(queue_sizes)
                    if total_of_queue_sizes > 0:
                        average_queue_size = total_of_queue_sizes / number_queue_sizes
                    else:
                        average_queue_size = 0
                    logger.info('%s :: total queue size for the last 10 seconds - %s' % (skyline_app, str(total_of_queue_sizes)))
                    logger.info('%s :: total queue values known for the last 10 seconds - %s' % (skyline_app, str(number_queue_sizes)))
                    logger.info('%s :: average queue size for the last 10 seconds - %s' % (skyline_app, str(average_queue_size)))
                    # self.send_graphite_metric('skyline.horizon.' + SERVER_METRIC_PATH + 'queue_size', self.q.qsize())
                    # self.send_graphite_metric('queue_size', average_queue_size)
                    send_metric_name = '%s.queue_size' % skyline_app_graphite_namespace
                    send_graphite_metric(skyline_app, send_metric_name, average_queue_size)

                    # reset queue_sizes and last_sent_graphite
                    queue_sizes = []
                    last_send_to_graphite = time()

            # @added 20200815 - Feature #3680: horizon.worker.datapoints_sent_to_redis
            # @modified 20201017 - Feature #3788: snab_flux_load_test
            #                      Feature #3680: horizon.worker.datapoints_sent_to_redis
            # Send for each worker
            last_datapoints_count_to_redis = now - last_datapoints_to_redis
            if last_datapoints_count_to_redis >= 60:
                logger.info('%s :: datapoints_sent_to_redis in last 60 seconds - %s' % (skyline_app, str(datapoints_sent_to_redis)))
                if self.canary:
                    send_metric_name = '%s.datapoints_sent_to_redis' % (
                        skyline_app_graphite_namespace)
                else:
                    send_metric_name = '%s.datapoints_sent_to_redis_%s' % (
                        skyline_app_graphite_namespace, str(self.worker_number))
                send_graphite_metric(skyline_app, send_metric_name, datapoints_sent_to_redis)
                datapoints_sent_to_redis = 0
                last_datapoints_to_redis = int(time())

                # @added 20201120 - Feature #3820: HORIZON_SHARDS
                if HORIZON_SHARDS:
                    try:
                        self.redis_conn.sadd('horizon.shards.metrics_assigned', *set(horizon_shard_assigned_metrics))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('%s :: error adding horizon.shard.metrics_assigned: %s' % (skyline_app, str(e)))
                    try:
                        self.redis_conn.sadd('horizon.shard.metrics_dropped', *set(horizon_shard_dropped_metrics))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('%s :: error adding horizon.shard.metrics_dropped: %s' % (skyline_app, str(e)))

                # @added 20201103 - Feature #3820: HORIZON_SHARDS
                if HORIZON_SHARDS and HORIZON_SHARD_DEBUG:
                    horizon_shard_assigned_metrics_count = len(horizon_shard_assigned_metrics)
                    horizon_shard_dropped_metrics_count = len(horizon_shard_dropped_metrics)
                    logger.info('%s :: %s assigned metrics to HORIZON_SHARD %s and dropped %s metrics in last 60 seconds' % (
                        skyline_app, str(horizon_shard_assigned_metrics_count),
                        str(HORIZON_SHARD),
                        str(horizon_shard_dropped_metrics_count)))
                    if LOCAL_DEBUG:
                        logger.info('%s :: horizon_shard_assigned_metrics - %s' % (
                            skyline_app, str(horizon_shard_assigned_metrics)))
                        logger.info('%s :: horizon_shard_dropped_metrics - %s' % (
                            skyline_app, str(horizon_shard_dropped_metrics)))
                    horizon_shard_assigned_metrics = []
                    horizon_shard_dropped_metrics = []

                # @added 20201120 - Feature #3820: HORIZON_SHARDS
                try:
                    self.redis_conn.sadd('horizon.metrics_received', *set(metrics_received))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('%s :: error adding horizon.metrics_received: %s' % (skyline_app, str(e)))
                if self.canary:
                    try:
                        logger.info('renaming key horizon.metrics_received to aet.horizon.metrics_received')
                        self.redis_conn.rename('horizon.metrics_received', 'aet.horizon.metrics_received')
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to rename Redis key horizon.metrics_received to aet.horizon.metrics_received')
                metrics_received = []
