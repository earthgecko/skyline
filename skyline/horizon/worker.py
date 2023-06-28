"""
worker.py
"""
from __future__ import division
# from os import kill, system
from os import kill
from multiprocessing import Process
try:
    from Queue import Empty
except ImportError:
    from queue import Empty
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
from sys import exit as sys_exit

# @added 20220722 - Task #4624: Change all dict copy to deepcopy
import copy

from msgpack import packb
from redis import StrictRedis, WatchError

import settings
# @modified 20220216 - Feature #4446: Optimise horizon worker in_skip_list
# Added get_redis_conn_decoded
# @modified 20220726 - Task #2732: Prometheus to Skyline
#                      Branch #4300: prometheus
# Moved send_graphite_metric
# from skyline_functions import send_graphite_metric, get_redis_conn_decoded
from skyline_functions import get_redis_conn_decoded
# @added 20220726 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.graphite.send_graphite_metric import send_graphite_metric

# @added 20230512 - Feature #4904: Handle send_graphite_metric failure
from functions.horizon.process_graphite_fail_queue import process_graphite_fail_queue

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
    # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
    # HORIZON_SHARDS = settings.HORIZON_SHARDS.copy()
    HORIZON_SHARDS = copy.deepcopy(settings.HORIZON_SHARDS)
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
        # @added 20220216 - Feature #4446: Optimise horizon worker in_skip_list
        # Added get_redis_conn_decoded
        self.redis_conn_decoded = get_redis_conn_decoded(parent_skyline_app)

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
            sys_exit(0)

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
            else:
                logger.info('bin/%s.d log management done' % skyline_app)

        logger.info('%s :: started worker %s' % (skyline_app, str(self.worker_number)))

        # @added 20220216 - Feature #4446: Optimise horizon worker in_skip_list
        skip_metrics_dict = {}
        do_not_skip_metrics_dict = {}
        skip_metrics_list = []
        do_not_skip_metrics_list = []
        checked_for_skip_count = 0
        checked_through_skip_list = []

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
        # @added 20210416 - Task #4020: Add horizon.queue_size_60s_avg metric
        last_log_queue_size = time()
        queue_sizes_60_seconds = []

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

            # @added 20220216 - Feature #4446: Optimise horizon worker in_skip_list
            if not skip_metrics_dict:
                try:
                    skip_metrics_dict = self.redis_conn_decoded.hgetall('horizon.skip_metrics')
                    logger.info('%s :: got %s metrics from horizon.skip_metrics from Redis hash as not set' % (
                        skyline_app, str(len(skip_metrics_dict))))
                except Exception as err:
                    logger.error('%s :: error on hgetall horizon.skip_metrics: %s' % (skyline_app, str(err)))
            if not do_not_skip_metrics_dict:
                try:
                    do_not_skip_metrics_dict = self.redis_conn_decoded.hgetall('horizon.do_not_skip_metrics')
                    logger.info('%s :: got %s metrics from horizon.do_not_skip_metrics from Redis hash as not set' % (
                        skyline_app, str(len(do_not_skip_metrics_dict))))
                except Exception as err:
                    logger.error('%s :: error on hgetall horizon.do_not_skip_metrics: %s' % (skyline_app, str(err)))

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
                        horizon_shard_assigned_metrics.append(metric[0])

                    # @added 20220216 - Feature #4446: Optimise horizon worker in_skip_list
                    found_in_do_not_skip_dict = False
                    if do_not_skip_metrics_dict:
                        try:
                            found_in_do_not_skip_dict = do_not_skip_metrics_dict[metric[0]]
                        except KeyError:
                            found_in_do_not_skip_dict = False
                        if found_in_do_not_skip_dict:
                            do_not_skip_metrics_list.append(metric[0])
                    found_in_skip_dict = False
                    if not found_in_do_not_skip_dict and skip_metrics_dict:
                        try:
                            found_in_skip_dict = skip_metrics_dict[metric[0]]
                        except KeyError:
                            found_in_skip_dict = False
                        if found_in_skip_dict:
                            skip_metrics_list.append(metric[0])
                            continue

                    # @modified 20220216 - Feature #4446: Optimise horizon worker in_skip_list
                    if not found_in_do_not_skip_dict:
                        checked_for_skip_count += 1
                        checked_through_skip_list.append(metric[0])

                        # Check if we should skip it
                        if self.in_skip_list(metric[0]):

                            # @added 20220216 - Feature #4446: Optimise horizon worker in_skip_list
                            skip_metrics_list.append(metric[0])

                            continue

                    # @added 20220216 - Feature #4446: Optimise horizon worker in_skip_list
                    if not found_in_do_not_skip_dict:
                        do_not_skip_metrics_list.append(metric[0])

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
                    except Exception as err:
                        logger.error('error :: worker :: error on pipe.append to %s: %s' % (
                            str(key), str(err)))
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
                # @modified 20210511
                # Do not logg queue size for every chunk
                # logger.info('%s :: queue size at %d' % (skyline_app, self.q.qsize()))

                queue_sizes.append(self.q.qsize())
                # Only send average queue mertics to graphite once per 10 seconds
                now = time()

                # @modified 20210416 - Task #4020: Add horizon.queue_size_60s_avg metric
                # last_sent_graphite = now - last_send_to_graphite
                # if last_sent_graphite > 10:
                last_logged_queue_size = now - last_log_queue_size
                if last_logged_queue_size > 10:
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
                    send_graphite_metric(self, skyline_app, send_metric_name, average_queue_size)
                    # @added 20210416 - Task #4020: Add horizon.queue_size_60s_avg metric
                    for size_value in queue_sizes:
                        queue_sizes_60_seconds.append(size_value)
                    # reset queue_sizes
                    queue_sizes = []
                    # last_send_to_graphite = time()
                    last_log_queue_size = time()

                # @added 20210416 - Task #4020: Add horizon.queue_size_60s_avg metric
                # Moved from the 10 second block above to its own 60 seconds block
                last_sent_graphite = now - last_send_to_graphite
                if last_sent_graphite >= 60:
                    try:
                        number_queue_sizes = len(queue_sizes_60_seconds)
                        total_of_queue_sizes = sum(queue_sizes_60_seconds)
                        if total_of_queue_sizes > 0:
                            average_queue_size = total_of_queue_sizes / number_queue_sizes
                        else:
                            average_queue_size = 0
                        logger.info('%s :: total queue size for the last 60 seconds - %s' % (skyline_app, str(total_of_queue_sizes)))
                        logger.info('%s :: total queue values known for the last 60 seconds - %s' % (skyline_app, str(number_queue_sizes)))
                        logger.info('%s :: average queue size for the last 60 seconds - %s' % (skyline_app, str(average_queue_size)))
                        send_metric_name = '%s.queue_size_60s_avg' % skyline_app_graphite_namespace
                        send_graphite_metric(self, skyline_app, send_metric_name, average_queue_size)
                        # reset queue_sizes_60_seconds and last_sent_graphite
                        queue_sizes_60_seconds = []
                        last_send_to_graphite = time()
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('%s :: error: %s' % (skyline_app, str(e)))

                    # @added 20210520 - Branch #1444: thunder
                    # Added Redis hash keys for thunder to monitor the horizon
                    # listen state, inferred via the
                    # horizon.worker.average_queue_size, without incurring the
                    # addition of Redis to horizon/listen.  This is used by
                    # Thunder notify on degradation of service and operational
                    # changes.
                    cache_key = 'horizon.worker.average_queue_size'
                    try:
                        self.redis_conn.hset(cache_key, 'value', int(average_queue_size))
                        self.redis_conn.hset(cache_key, 'timestamp', int(now))
                    except Exception as e:
                        logger.error('error :: horizon :: worker :: could not update the Redis %s key - %s' % (
                            cache_key, e))

                    # @added 20230512 - Feature #4904: Handle send_graphite_metric failure
                    # Process any metrics to be submitted in the fail queue
                    graphite_fail_queue_exists = 0
                    try:
                        graphite_fail_queue_exists = self.redis_conn_decoded.exists('skyline.graphite_fail_queue')
                    except Exception as err:
                        logger.error('%s :: error: exists failed on Redis set skyline.graphite_fail_queue - %s' % (skyline_app, err))
                    if graphite_fail_queue_exists:
                        try:
                            sent_count = process_graphite_fail_queue(self)
                        except Exception as err:
                            logger.error('error :: horizon :: worker :: process_graphite_fail_queue failed - %s' % (
                                err))

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
                send_graphite_metric(self, skyline_app, send_metric_name, datapoints_sent_to_redis)
                datapoints_sent_to_redis = 0
                last_datapoints_to_redis = int(time())

                # @added 20201120 - Feature #3820: HORIZON_SHARDS
                if HORIZON_SHARDS:
                    try:
                        self.redis_conn.sadd('horizon.shards.metrics_assigned', *set(horizon_shard_assigned_metrics))
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('%s :: error adding horizon.shard.metrics_assigned: %s' % (skyline_app, str(e)))
                    try:
                        self.redis_conn.sadd('horizon.shard.metrics_dropped', *set(horizon_shard_dropped_metrics))
                    except Exception as e:
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
                if metrics_received:
                    try:
                        self.redis_conn.sadd('horizon.metrics_received', *set(metrics_received))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('%s :: error adding horizon.metrics_received' % (skyline_app))
                    if self.canary:
                        try:
                            logger.info('%s :: renaming key horizon.metrics_received to aet.horizon.metrics_received' % skyline_app)
                            self.redis_conn.rename('horizon.metrics_received', 'aet.horizon.metrics_received')
                        except Exception as e:
                            logger.info(traceback.format_exc())
                            logger.error('error :: %s failed to rename Redis key horizon.metrics_received to aet.horizon.metrics_received - %s' % (
                                skyline_app, e))

                        metrics_received_count = 0
                        try:
                            metrics_received_count = self.redis_conn.scard('aet.horizon.metrics_received')
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: %s :: running redis_conn.scard(\'aet.horizon.metrics_received\') - %s' % (
                                skyline_app, e))
                        cache_key = 'thunder.horizon.worker.metrics_received'
                        try:
                            self.redis_conn.hset(cache_key, 'value', int(metrics_received_count))
                            self.redis_conn.hset(cache_key, 'timestamp', int(now))
                        except Exception as e:
                            logger.error('error :: horizon :: worker :: could not update the Redis %s key - %s' % (
                                cache_key, e))
                        send_metric_name = '%s.metrics_received' % (
                            skyline_app_graphite_namespace)
                        send_graphite_metric(self, skyline_app, send_metric_name, metrics_received_count)

                    # @added 20210520 - Branch #1444: thunder
                    # Added Redis hash keys for thunder to monitor the horizon
                    # listen state, inferred via the
                    # horizon.worker.average_queue_size, without incurring the
                    # addition of Redis to horizon/listen.  This is used by
                    # Thunder notify on degradation of service and operational
                    # changes.
                    cache_key = 'horizon'
                    try:
                        self.redis_conn.setex(cache_key, 120, int(now))
                    except Exception as e:
                        logger.error('error :: horizon :: worker :: could not update the Redis %s key - %s' % (
                            cache_key, e))

                metrics_received = []

                # @added 20220216 - Feature #4446: Optimise horizon worker in_skip_list
                if skip_metrics_list:
                    update_skip_metrics = {}
                    for metric in list(set(skip_metrics_list)):
                        update_entry = False
                        if skip_metrics_dict:
                            try:
                                # Only update skip metrics every hour to save on
                                # Redis overhead
                                last_update_timestamp_str = skip_metrics_dict[metric]
                                if last_update_timestamp_str:
                                    last_update_timestamp = int(float(last_update_timestamp_str))
                                    if (now - last_update_timestamp) >= 3600:
                                        update_entry = True
                            except KeyError:
                                update_entry = True
                            except:
                                update_entry = True
                        else:
                            update_entry = True
                        if update_entry:
                            update_skip_metrics[metric] = int(now)
                    if update_skip_metrics:
                        try:
                            self.redis_conn_decoded.hset('horizon.skip_metrics', mapping=update_skip_metrics)
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: listen :: failed to update horizon.skip_metrics Redis hash - %s' % (
                                err))
                if do_not_skip_metrics_list:
                    update_metrics = {}
                    for metric in list(set(do_not_skip_metrics_list)):
                        update_entry = False
                        if do_not_skip_metrics_dict:
                            try:
                                # Only update skip metrics every hour to save on
                                # Redis overhead
                                last_update_timestamp_str = skip_metrics_dict[metric]
                                if last_update_timestamp_str:
                                    last_update_timestamp = int(float(last_update_timestamp_str))
                                    if (now - last_update_timestamp) >= 3600:
                                        update_entry = True
                            except KeyError:
                                update_entry = True
                            except:
                                update_entry = True
                        else:
                            update_entry = True
                        if update_entry:
                            update_metrics[metric] = int(now)
                    if update_metrics:
                        try:
                            self.redis_conn_decoded.hset('horizon.do_not_skip_metrics', mapping=update_metrics)
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: listen :: failed to update horizon.do_not_skip_metrics Redis hash - %s' % (
                                err))
                try:
                    skip_metrics_dict = self.redis_conn_decoded.hgetall('horizon.skip_metrics')
                    logger.info('%s :: got %s metrics from horizon.skip_metrics from Redis hash on minutely run' % (
                        skyline_app, str(len(skip_metrics_dict))))
                except Exception as err:
                    logger.error('%s :: error on hgetall horizon.skip_metrics: %s' % (skyline_app, str(err)))
                try:
                    do_not_skip_metrics_dict = self.redis_conn_decoded.hgetall('horizon.do_not_skip_metrics')
                    logger.info('%s :: got %s metrics from horizon.do_not_skip_metrics from Redis hash on minutely run' % (
                        skyline_app, str(len(do_not_skip_metrics_dict))))
                except Exception as err:
                    logger.error('%s :: error on hgetall horizon.do_not_skip_metrics: %s' % (skyline_app, str(err)))
                logger.info('%s :: checked %s metrics through in_skip_list' % (
                    skyline_app, str(checked_for_skip_count)))
                if checked_through_skip_list:
                    try:
                        self.redis_conn_decoded.sadd('horizon.checked_through_skip_list', *set(checked_through_skip_list))
                        self.redis_conn_decoded.expire('horizon.checked_through_skip_list', 86400)
                        logger.info('%s :: updated horizon.checked_through_skip_list Redis set on minutely run' % (
                            skyline_app))
                    except Exception as err:
                        logger.error('%s :: sadd failed on horizon.checked_through_skip_list - %s' % (skyline_app, str(err)))
                    checked_through_skip_list = []
                skip_metrics_list = []
                do_not_skip_metrics_list = []
