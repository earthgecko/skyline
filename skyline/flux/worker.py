"""
worker.py
"""
import sys
import os.path
from os import kill
from os import getpid
import traceback
# @modified 20191115 - Branch #3262: py3
# from multiprocessing import Queue, Process
from multiprocessing import Process

try:
    from Queue import Empty  # Python 2.7
except ImportError:
    from queue import Empty  # Python 3
from time import sleep, time
from ast import literal_eval

# @added 20201019 - Feature #3790: flux - pickle to Graphite
# bandit [B403:blacklist] Consider possible security implications associated
# with pickle module.  These have been considered.
import pickle  # nosec
import socket
import struct

# @added 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
# Better handle multiple workers
import random

# from redis import StrictRedis
import graphyte
import statsd

from logger import set_up_logging
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# @modified 20191115 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
load_settings = True
if load_settings:
    import settings
    from skyline_functions import (
        send_graphite_metric,
        # @added 20191111 - Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
        get_redis_conn, get_redis_conn_decoded)
    # @added 20220429 - Feature #4536: Handle Redis failure
    from functions.flux.get_last_metric_data import get_last_metric_data

# @added 20220428 - Feature #4536: Handle Redis failure
if settings.MEMCACHE_ENABLED:
    from functions.memcache.get_memcache_key import get_memcache_key
    from functions.memcache.set_memcache_key import set_memcache_key
    from functions.memcache.delete_memcache_key import delete_memcache_key
else:
    get_memcache_key = None
    set_memcache_key = None
    delete_memcache_key = None

# @modified 20191129 - Branch #3262: py3
# Consolidate flux logging
# logger = set_up_logging('worker')
logger = set_up_logging(None)

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

# @added 20200827 - Feature #3708: FLUX_ZERO_FILL_NAMESPACES
try:
    FLUX_ZERO_FILL_NAMESPACES = settings.FLUX_ZERO_FILL_NAMESPACES
except:
    FLUX_ZERO_FILL_NAMESPACES = []

# added 20201016 - Feature #3788: snab_flux_load_test
# Wrap per metric logging in if FLUX_VERBOSE_LOGGING
try:
    FLUX_VERBOSE_LOGGING = settings.FLUX_VERBOSE_LOGGING
except:
    FLUX_VERBOSE_LOGGING = True

# @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
try:
    FLUX_PERSIST_QUEUE = settings.FLUX_PERSIST_QUEUE
except:
    FLUX_PERSIST_QUEUE = False

# @added 20201020 - Feature #3796: FLUX_CHECK_LAST_TIMESTAMP
try:
    FLUX_CHECK_LAST_TIMESTAMP = settings.FLUX_CHECK_LAST_TIMESTAMP
except:
    FLUX_CHECK_LAST_TIMESTAMP = True
try:
    VISTA_ENABLED = settings.VISTA_ENABLED
except:
    VISTA_ENABLED = False

# @added 20201120 - Feature #3796: FLUX_CHECK_LAST_TIMESTAMP
#                   Feature #3400: Identify air gaps in the metric data
try:
    IDENTIFY_AIRGAPS = settings.IDENTIFY_AIRGAPS
except:
    IDENTIFY_AIRGAPS = False

parent_skyline_app = 'flux'

# @added 20191010 - Feature #3250: Allow Skyline to send metrics to another Carbon host
# Added missing skyline_app required for send_graphite_metric
skyline_app = 'flux'

skyline_app_graphite_namespace = 'skyline.%s%s.worker' % (parent_skyline_app, SERVER_METRIC_PATH)

# @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
listen_graphite_namespace = 'skyline.%s%s.listen' % (parent_skyline_app, SERVER_METRIC_PATH)

LOCAL_DEBUG = False

if settings.FLUX_SEND_TO_CARBON:
    GRAPHITE_METRICS_PREFIX = None
    CARBON_HOST = settings.FLUX_CARBON_HOST
    CARBON_PORT = settings.FLUX_CARBON_PORT
    try:
        graphyte.init(CARBON_HOST, port=CARBON_PORT, prefix=None, timeout=5)
        logger.info('worker :: succeeded to graphyte.init with host: %s, port: %s, prefix: %s' % (
            str(CARBON_HOST), str(CARBON_PORT),
            str(GRAPHITE_METRICS_PREFIX)))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: worker :: failed to run graphyte.init with host: %s, port: %s, prefix: %s' % (
            str(CARBON_HOST), str(CARBON_PORT),
            str(GRAPHITE_METRICS_PREFIX)))
if settings.FLUX_SEND_TO_STATSD:
    STATSD_HOST = settings.FLUX_STATSD_HOST
    STATSD_PORT = settings.FLUX_STATSD_PORT
    try:
        statsd_conn = statsd.StatsClient(STATSD_HOST, STATSD_PORT)
        logger.info('worker :: initialized statsd.StatsClient with STATSD_HOST: %s, STATSD_PORT: %s' % (
            str(STATSD_HOST), str(STATSD_PORT)))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: worker :: failed to initialize statsd.StatsClient with STATSD_HOST: %s, STATSD_PORT: %s' % (
            str(STATSD_HOST), str(STATSD_PORT)))


class Worker(Process):
    """
    The worker processes metric from the queue and sends them to Graphite.
    """
    def __init__(self, queue, parent_pid):
        super(Worker, self).__init__()
        # @modified 20191115 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # if settings.REDIS_PASSWORD:
        #     self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        # else:
        #     self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
        # @added 20191115 - Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
        try:
            self.redis_conn = get_redis_conn(skyline_app)
        except Exception as err:
            logger.error('error :: worker :: get_redis_conn failed - %s' % err)
            self.redis_conn = None
        try:
            self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
        except Exception as err:
            logger.error('error :: worker :: get_redis_conn_decoded failed - %s' % err)
            self.redis_conn_decoded = None

        self.q = queue
        self.parent_pid = parent_pid
        self.daemon = True
        self.current_pid = getpid()

    def check_if_parent_is_alive(self):
        """
        Self explanatory.
        """
        try:
            kill(self.parent_pid, 0)
        except:
            # @added 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
            # Log warning
            logger.warn('warning :: parent process is dead')
            sys.exit(0)

    def run(self):
        """
        Called when the process intializes.
        """

        def pickle_data_to_graphite(data):

            message = None
            try:
                payload = pickle.dumps(data, protocol=2)
                header = struct.pack("!L", len(payload))
                message = header + payload
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: worker :: failed to pickle to send to Graphite')
                return False
            if message:
                try:
                    sock = socket.socket()
                    sock.connect((CARBON_HOST, settings.FLUX_CARBON_PICKLE_PORT))
                    sock.sendall(message)
                    sock.close()
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: worker :: failed to send pickle data to Graphite')
                    return False
            else:
                logger.error(traceback.format_exc())
                logger.error('error :: worker :: failed to pickle metric data into message')
                return False
            return True

        def submit_pickle_data_to_graphite(pickle_data):

            # @modified 20201207 - Task #3864: flux - try except everything
            try:
                number_of_datapoints = len(pickle_data)
            except Exception as err:
                logger.error('error :: worker :: could not determine number_of_datapoints from len(pickle_data) - %s' % str(err))
                return False

            data_points_sent = 0
            smallListOfMetricTuples = []
            tuples_added = 0

            for data in pickle_data:
                # @modified 20201207 - Task #3864: flux - try except everything
                try:
                    smallListOfMetricTuples.append(data)
                    tuples_added += 1
                    if tuples_added >= 480:
                        # @modified 20201207 - Task #3864: flux - try except everything
                        try:
                            pickle_data_sent = pickle_data_to_graphite(smallListOfMetricTuples)
                        except Exception as e:
                            logger.error('error :: worker :: pickle_data_to_graphite error - %s' % str(e))
                            pickle_data_sent = False

                        # Reduce the speed of submissions to Graphite
                        # if there are lots of data points
                        if number_of_datapoints > 4000:
                            sleep(0.3)
                        if pickle_data_sent:
                            data_points_sent += tuples_added
                            logger.info('worker :: sent %s/%s of %s data points to Graphite via pickle' % (
                                str(tuples_added), str(data_points_sent),
                                str(number_of_datapoints)))
                            smallListOfMetricTuples = []
                            tuples_added = 0
                        else:
                            logger.error('error :: worker :: failed to send %s data points to Graphite via pickle' % (
                                str(tuples_added)))
                            return False
                except Exception as e:
                    logger.error('error :: worker :: error handling data in pickle_data - %s' % str(e))
                    return False

            if smallListOfMetricTuples:
                # @modified 20201207 - Task #3864: flux - try except everything
                try:
                    tuples_to_send = len(smallListOfMetricTuples)
                    pickle_data_sent = pickle_data_to_graphite(smallListOfMetricTuples)
                    if pickle_data_sent:
                        data_points_sent += tuples_to_send
                        if FLUX_VERBOSE_LOGGING:
                            logger.info('worker :: sent the last %s/%s of %s data points to Graphite via pickle' % (
                                str(tuples_to_send), str(data_points_sent),
                                str(number_of_datapoints)))
                    else:
                        logger.error('error :: failed to send the last %s data points to Graphite via pickle' % (
                            str(tuples_to_send)))
                        return False
                except Exception as e:
                    logger.error('error :: worker :: error in smallListOfMetricTuples pickle_data_to_graphite - %s' % str(e))
                    return False
            return True

        # @added 20220428 - Feature #4536: Handle Redis failure
        def add_to_memcache_flux_workers_metrics_sent(metrics_to_add):
            current_all_metrics_sent = []
            try:
                current_all_metrics_sent = get_memcache_key('flux', 'flux.workers.metrics_sent')
                if not current_all_metrics_sent:
                    current_all_metrics_sent = []
            except Exception as err:
                logger.error('error :: add_to_memcache_flux_workers_metrics_sent failed - %s' % (
                    err))
                current_all_metrics_sent = []
            current_all_metrics_sent = list(set(current_all_metrics_sent + metrics_to_add))
            success = False
            try:
                success = set_memcache_key('flux', 'flux.workers.metrics_sent', current_all_metrics_sent)
            except:
                logger.error('error :: worker :: failed to add %s metrics to memcache flux.workers.metrics_sent while Redis unavailable' % str(len(metrics_to_add)))
            return success

        logger.info('worker :: starting worker')

        # Determine a master worker that zerofills and last_known_value
        worker_pid = getpid()
        main_process_pid = 0
        try:
            main_process_pid = int(self.redis_conn_decoded.get('flux.main_process_pid'))
            if main_process_pid:
                logger.info('worker :: main_process_pid found in Redis key - %s' % str(main_process_pid))
        except:
            main_process_pid = 0
        # @added 20220428 - Feature #4536: Handle Redis failure
        main_process_pid_from_memcache = False
        failed_over_to_memcache = False
        if not main_process_pid:
            if settings.MEMCACHE_ENABLED:
                try:
                    main_process_pid = get_memcache_key('flux', 'flux.main_process_pid')
                    if main_process_pid:
                        logger.info('worker :: main_process_pid found in memcache key - %s' % str(main_process_pid))
                        main_process_pid_from_memcache = True
                        failed_over_to_memcache = True
                    else:
                        main_process_pid = 0
                except Exception as err:
                    logger.error('error :: worker :: could get flux.main_process_pid from memcache - %s' % str(err))
                    main_process_pid = 0
        if not main_process_pid:
            logger.error('error :: worker :: no main_process_pid known, exiting')
            sys.exit(1)

        primary_worker_key = 'flux.primary_worker_pid.%s' % str(main_process_pid)
        logger.info('worker :: starting primary_worker election using primary_worker_key: %s' % primary_worker_key)
        sleep_for = random.uniform(0.1, 1.5)
        logger.info('worker :: starting primary_worker election - sleeping for %s' % str(sleep_for))
        sleep(sleep_for)
        primary_worker_pid = 0
        try:
            primary_worker_pid = int(self.redis_conn_decoded.get(primary_worker_key))
            if primary_worker_pid:
                logger.info('worker :: primary_worker_pid found in Redis key - %s' % str(primary_worker_pid))
        except:
            primary_worker_pid = 0
            # @added 20220428 - Feature #4536: Handle Redis failure
            if settings.MEMCACHE_ENABLED:
                try:
                    primary_worker_pid = get_memcache_key('flux', 'flux.worker.primary_worker_key')
                    if primary_worker_pid:
                        logger.info('worker :: primary_worker_key found in memcache key - %s' % str(primary_worker_pid))
                        failed_over_to_memcache = True
                    else:
                        primary_worker_pid = 0
                except:
                    primary_worker_pid = 0

        if not primary_worker_pid:
            logger.info('worker :: no primary_worker found, becoming primary_worker')
            try:
                self.redis_conn.setex(primary_worker_key, 75, worker_pid)
                primary_worker_pid = int(self.redis_conn_decoded.get(primary_worker_key))
                logger.info('worker :: set self pid to primary_worker - %s' % str(primary_worker_pid))
            except:
                primary_worker_pid = 0
                # @added 20220428 - Feature #4536: Handle Redis failure
                if settings.MEMCACHE_ENABLED:
                    success = False
                    try:
                        success = set_memcache_key('flux', 'flux.worker.primary_worker_key', worker_pid, 75)
                    except:
                        primary_worker_pid = 0
                    if success:
                        try:
                            primary_worker_pid = get_memcache_key('flux', 'flux.worker.primary_worker_key')
                            if primary_worker_pid:
                                logger.info('worker :: primary_worker_key found in memcache key - %s' % str(primary_worker_pid))
                                failed_over_to_memcache = True
                            else:
                                primary_worker_pid = 0
                        except:
                            primary_worker_pid = 0

        primary_worker = False
        if primary_worker_pid == worker_pid:
            primary_worker = True
        logger.info('worker :: primary_worker_pid is set to %s, primary_worker: %s' % (
            str(primary_worker_pid), str(primary_worker)))

        last_sent_to_graphite = int(time())
        metrics_sent_to_graphite = 0

        # @added 20200827 - Feature #3708: FLUX_ZERO_FILL_NAMESPACES
        last_zero_fill_to_graphite = 0
        metrics_sent = []

        remove_from_flux_queue_redis_set = []

        # @added 20201019 - Feature #3790: flux - pickle to Graphite
        pickle_data = []
        # send_to_reciever = 'line'
        send_to_reciever = 'pickle'

        # @modified 20201207 - Task #3864: flux - try except everything
        try:
            metric_data_queue_size = self.q.qsize()
        except Exception as e:
            logger.error('error :: worker :: could not determine metric_data_queue_size - %s' % str(e))
            metric_data_queue_size = 0

        if metric_data_queue_size > 10:
            send_to_reciever = 'pickle'

        # @added 202011120 - Feature #3790: flux - pickle to Graphite
        # Debug Redis set
        metrics_data_sent = []

        # @added 20201020 - Feature #3796: FLUX_CHECK_LAST_TIMESTAMP
        # Even if flux.last Redis keys are disabled in flux they are used in
        # Vista
        vista_metrics = []
        if not FLUX_CHECK_LAST_TIMESTAMP and VISTA_ENABLED:
            try:
                vista_metrics = list(self.redis_conn_decoded.sscan_iter('vista.metrics', match='*'))
            except:
                vista_metrics = []

        # @added 20210407 - Bug #4002: Change flux FLUX_ZERO_FILL_NAMESPACES to pickle
        # Set the variable to default
        last_zero_fill_to_graphite = []

        # @added 20220428 - Feature #4536: Handle Redis failure
        sent_add_to_memcache = []
        last_timed_out_empty_log = int(time())

        # Populate API keys and tokens in memcache
        # python-2.x and python3.x handle while 1 and while True differently
        # while 1:
        running = True
        while running:
            # Make sure Redis is up
            redis_up = False
            while not redis_up:
                try:
                    redis_up = self.redis_conn.ping()
                    if redis_up and failed_over_to_memcache:
                        logger.info('worker :: Redis has RECOVERED')
                    if redis_up:
                        failed_over_to_memcache = False
                        continue
                except:
                    if not failed_over_to_memcache:
                        logger.error('worker :: cannot connect to redis at socket path %s' % (settings.REDIS_SOCKET_PATH))
                        sleep(2)
                # @modified 20191115 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # Use get_redis_conn and get_redis_conn_decoded
                # if settings.REDIS_PASSWORD:
                #     self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                # else:
                #     self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                # @modified 20201207 - Task #3864: flux - try except everything
                try:
                    self.redis_conn = get_redis_conn(skyline_app)
                    if self.redis_conn:
                        try:
                            redis_up = self.redis_conn.ping()
                            if redis_up:
                                failed_over_to_memcache = False
                                logger.info('worker :: Redis connection established - RECOVERED')
                        except:
                            redis_up = False
                            logger.warning('worker :: Redis connection not established')
                            failed_over_to_memcache = True
                except Exception as err:
                    if not failed_over_to_memcache:
                        logger.error('error :: worker :: could not get_redis_conn - %s' % str(err))
                    if settings.MEMCACHE_ENABLED:
                        failed_over_to_memcache = True
                try:
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                except Exception as err:
                    if not failed_over_to_memcache:
                        logger.error('error :: worker :: could not get_redis_conn_decoded - %s' % str(err))
                    if settings.MEMCACHE_ENABLED:
                        failed_over_to_memcache = True
                # @added 20220428 - Feature #4536: Handle Redis failure
                if not settings.MEMCACHE_ENABLED:
                    failed_over_to_memcache = False
                if failed_over_to_memcache:
                    redis_up = True
                    logger.warning('warning :: worker :: Redis is unavailable, no further Redis errors will be logged in this run, failed_over_to_memcache: %s' % str(failed_over_to_memcache))

            if LOCAL_DEBUG:
                try:
                    metric_data_queue_size = self.q.qsize()
                    logger.info('worker :: debug :: flux.httpMetricDataQueue queue size - %s' % str(metric_data_queue_size))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: worker :: failed to determine size of queue flux.httpMetricDataQueue')

            # @added 20220428 - Feature #4536: Handle Redis failure
            # Set this dictionary variable so it can be surfaced from memcache
            # and used in the event of a Redis failure
            memcache_flux_last_metric_data = {}
            new_memcache_flux_last_metric_data = {}

            metric_data = None
            try:
                # Get a metric from the queue with a 1 second timeout, each
                # metric item on the queue is a list e.g.
                # metric_data = [metricName, metricValue, metricTimestamp]
                metric_data = self.q.get(True, 1)

            except Empty:
                if pickle_data:
                    # @modified 20201207 - Task #3864: flux - try except everything
                    try:
                        pickle_data_submitted = submit_pickle_data_to_graphite(pickle_data)
                    except Exception as e:
                        logger.error('error :: worker :: queue Empty failed to submit_pickle_data_to_graphite - %s' % str(e))
                        pickle_data_submitted = False

                    if pickle_data_submitted:
                        pickle_data = []

                    # @added 20220428 - Feature #4536: Handle Redis failure
                    if sent_add_to_memcache:
                        success = False
                        try:
                            success = add_to_memcache_flux_workers_metrics_sent(sent_add_to_memcache)
                        except:
                            pass
                        if success:
                            failed_over_to_memcache = True
                            logger.info('worker :: added %s sent metrics to flux.workers.metrics_sent memcache key' % str(len(sent_add_to_memcache)))
                    sent_add_to_memcache = []

                if (int(time()) - last_timed_out_empty_log) >= 10:
                    logger.info('worker :: queue is empty and timed out, %s - primary_worker: %s' % (
                        str(self.q.qsize()), str(primary_worker)))
                    last_timed_out_empty_log = int(time())

                sleep(1)
                # @added 20201017 - Feature #3788: snab_flux_load_test
                # Send to Graphite even if worker gets no metrics
                current_time = int(time())
                if (current_time - last_sent_to_graphite) >= 60:

                    # @added 20210407 - Bug #4002: Change flux FLUX_ZERO_FILL_NAMESPACES to pickle
                    # Added to FLUX_ZERO_FILL_NAMESPACES to the empty queue
                    # block as well as it was only execute in the final block
                    # which resulted in missing data if the loop exited on the
                    # empty queue.
                    # Send 0 for any metric in the flux.zero_fill_metrics Redis set that
                    # has not submitted data in the last 60 seconds.  The flux.last
                    # Redis key is not updated for these sent 0 values so if the source
                    # sends data for a timestamp in the period later (due to a lag, etc),
                    # it will be valid and sent to Graphite.
                    # @modified 20210406 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                    # Now that the zero_fill setting can be passed in the
                    # FLUX_AGGREGATE_NAMESPACES settings check the Redis set
                    # if FLUX_ZERO_FILL_NAMESPACES:
                    # if not last_zero_fill_to_graphite:
                    #     last_zero_fill_to_graphite = current_time - 60
                    if primary_worker:
                        flux_zero_fill_metrics = []
                        try:
                            flux_zero_fill_metrics = list(self.redis_conn_decoded.smembers('flux.zero_fill_metrics'))
                        except:
                            if not failed_over_to_memcache:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to generate a list from flux.zero_fill_metrics Redis set')
                            # @added 20220428 - Feature #4536: Handle Redis failure
                            if settings.MEMCACHE_ENABLED:
                                try:
                                    flux_zero_fill_metrics = get_memcache_key('flux', 'flux.zero_fill_metrics')
                                    if flux_zero_fill_metrics:
                                        logger.info('worker :: flux_zero_fill_metrics found in memcache')
                                        failed_over_to_memcache = True
                                    else:
                                        flux_zero_fill_metrics = []
                                except:
                                    flux_zero_fill_metrics = []

                        # @modified 20210408 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                        # Use Redis set
                        try:
                            all_metrics_sent = list(self.redis_conn_decoded.smembers('flux.workers.metrics_sent'))
                        except:
                            if not failed_over_to_memcache:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to generate a list from flux.workers.metrics_sent Redis set')
                            # @added 20220428 - Feature #4536: Handle Redis failure
                            if settings.MEMCACHE_ENABLED:
                                try:
                                    all_metrics_sent = get_memcache_key('flux', 'flux.workers.metrics_sent')
                                    if all_metrics_sent:
                                        logger.info('worker :: all_metrics_sent found in memcache')
                                        failed_over_to_memcache = True
                                    else:
                                        all_metrics_sent = []
                                except:
                                    all_metrics_sent = []

                        # @added 20220428 - Feature #4536: Handle Redis failure
                        metrics_which_were_zero_filled = []

                        for flux_zero_fill_metric in flux_zero_fill_metrics:
                            # if flux_zero_fill_metric not in metrics_sent:
                            if flux_zero_fill_metric not in all_metrics_sent:
                                try:
                                    tuple_data = (flux_zero_fill_metric, (last_sent_to_graphite, 0.0))
                                    pickle_data.append(tuple_data)
                                    if FLUX_VERBOSE_LOGGING:
                                        logger.info('worker :: zero fill - added %s to pickle_data' % (str(tuple_data)))
                                    metrics_sent_to_graphite += 1
                                    metrics_sent.append(flux_zero_fill_metric)
                                    # @added 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                                    # Better handle multiple workers
                                    # @modified 20220428 - Feature #4536: Handle Redis failure
                                    # Do in 1 requests and use memcache if fail
                                    # try:
                                    #     self.redis_conn.sadd('flux.workers.metrics_sent', flux_zero_fill_metric)
                                    # except:
                                    #     pass
                                    metrics_which_were_zero_filled.append(flux_zero_fill_metric)
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: worker :: zero fill - failed to add metric data to pickle for %s' % str(flux_zero_fill_metric))
                                    metric = None

                        # @added 20220428 - Feature #4536: Handle Redis failure
                        if metrics_which_were_zero_filled:
                            try:
                                self.redis_conn.sadd('flux.workers.metrics_sent', *set(metrics_which_were_zero_filled))
                            except:
                                if settings.MEMCACHE_ENABLED:
                                    try:
                                        success = add_to_memcache_flux_workers_metrics_sent(metrics_which_were_zero_filled)
                                    except Exception as err:
                                        logger.error('error :: worker :: failed to add %s zero filled metrics to memcache flux.workers.metrics_sent as Redis unavailable - %s' % (
                                            str(len(metrics_which_were_zero_filled)), err))
                                    if success:
                                        logger.info('worker :: added %s zero filled metrics to memcache flux.workers.metrics_sent as Redis unavailable' % str(len(metrics_which_were_zero_filled)))
                                        failed_over_to_memcache = True

                        last_zero_fill_to_graphite = current_time

                        flux_last_known_value_metrics = []
                        try:
                            flux_last_known_value_metrics = list(self.redis_conn_decoded.smembers('flux.last_known_value_metrics'))
                        except:
                            if not failed_over_to_memcache:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to generate a list from flux.last_known_value_metrics Redis set')
                            flux_last_known_value_metrics = []
                            # @added 20220428 - Feature #4536: Handle Redis failure
                            if settings.MEMCACHE_ENABLED:
                                try:
                                    flux_last_known_value_metrics = get_memcache_key('flux', 'flux.last_known_value_metrics')
                                    if flux_last_known_value_metrics:
                                        logger.info('worker :: lux_last_known_value_metrics found in memcache')
                                        failed_over_to_memcache = True
                                    else:
                                        flux_last_known_value_metrics = []
                                except:
                                    flux_last_known_value_metrics = []

                        # @added 20220428 - Feature #4536: Handle Redis failure
                        metrics_which_were_last_known_value_filled = []

                        for flux_last_known_value_metric in flux_last_known_value_metrics:
                            if flux_last_known_value_metric not in metrics_sent:
                                last_known_value = None
                                try:
                                    last_known_value = float(self.redis_conn_decoded.hget('flux.last_known_value', flux_last_known_value_metric))
                                except Exception as err:
                                    if not failed_over_to_memcache:
                                        # logger.error(traceback.format_exc())
                                        logger.error('error :: worker :: last_known_value - failed to get last known value for %s - %s' % (
                                            str(flux_last_known_value_metric), err))
                                    last_known_value = None
                                    if settings.MEMCACHE_ENABLED:
                                        if not memcache_flux_last_metric_data:
                                            # Only check once and fail once
                                            if str(memcache_flux_last_metric_data) == '{}':
                                                try:
                                                    memcache_flux_last_metric_data = get_memcache_key('flux', 'flux.last.metric_data')
                                                    if not memcache_flux_last_metric_data:
                                                        # Only check once and fail once
                                                        memcache_flux_last_metric_data = False
                                                        logger.warning('warning :: worker :: get_memcache_key flux.last.metric_data no data found')
                                                    else:
                                                        logger.info('worker :: get_memcache_key flux.last.metric_data data loaded')
                                                except Exception as err:
                                                    logger.error('error :: worker :: get_memcache_key failed for flux.last.metric_data - %s' % (
                                                        err))
                                        if memcache_flux_last_metric_data:
                                            try:
                                                last_known_value = memcache_flux_last_metric_data[flux_last_known_value_metric]['value']
                                            except KeyError:
                                                last_known_value = None

                                if last_known_value is not None:
                                    try:
                                        tuple_data = (flux_last_known_value_metric, (last_sent_to_graphite, last_known_value))
                                        pickle_data.append(tuple_data)
                                        if FLUX_VERBOSE_LOGGING:
                                            logger.info('worker :: last_known_value - added %s to pickle_data' % (str(tuple_data)))
                                        metrics_sent_to_graphite += 1
                                        metrics_sent.append(flux_last_known_value_metric)
                                        # @added 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                                        # Better handle multiple workers
                                        # @modified 20220428 - Feature #4536: Handle Redis failure
                                        # Do in 1 requests and use memcache if fail
                                        # try:
                                        #     self.redis_conn.sadd('flux.workers.metrics_sent', flux_last_known_value_metric)
                                        # except:
                                        #     pass
                                        metrics_which_were_last_known_value_filled.append(flux_last_known_value_metric)
                                    except Exception as e:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: worker :: last_known_value - failed to add metric data to pickle for %s - %s' % (
                                            str(flux_last_known_value_metric), e))

                        # @added 20220428 - Feature #4536: Handle Redis failure
                        if metrics_which_were_last_known_value_filled:
                            try:
                                self.redis_conn.sadd('flux.workers.metrics_sent', *set(metrics_which_were_last_known_value_filled))
                            except:
                                if not failed_over_to_memcache:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to generate a list from flux.last_known_value_metrics Redis set')
                                if settings.MEMCACHE_ENABLED:
                                    try:
                                        success = add_to_memcache_flux_workers_metrics_sent(metrics_which_were_last_known_value_filled)
                                    except Exception as err:
                                        logger.error('error :: worker :: failed to add %s last known value filled metrics to memcache flux.workers.metrics_sent as Redis unavailable - %s' % (
                                            str(len(metrics_which_were_zero_filled)), err))
                                    if success:
                                        logger.info('worker :: added %s last known value filled metrics to memcache flux.workers.metrics_sent as Redis unavailable' % str(len(metrics_which_were_zero_filled)))
                                        failed_over_to_memcache = True

                    logger.info('worker :: metrics_sent_to_graphite in last 60 seconds - %s' % str(metrics_sent_to_graphite))
                    skyline_metric = '%s.metrics_sent_to_graphite' % skyline_app_graphite_namespace
                    if primary_worker:
                        try:
                            # @modified 20191008 - Feature #3250: Allow Skyline to send metrics to another Carbon host
                            # graphyte.send(skyline_metric, metrics_sent_to_graphite, time_now)
                            # @modified 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                            # Better handle multiple workers get count from the key
                            # send_graphite_metric(skyline_app, skyline_metric, metrics_sent_to_graphite)
                            all_metrics_sent_to_graphite = int(metrics_sent_to_graphite)
                            try:
                                all_metrics_sent_to_graphite = len(list(self.redis_conn_decoded.smembers('flux.workers.metrics_sent')))
                                # @modified 20220427 - Feature #4536: Handle Redis failure
                                # Add flux required data to memcache as well
                                # self.redis_conn.delete('flux.workers.metrics_sent')
                                # @modified 20220510 - Feature #3824: get_cluster_data
                                #                      Release #4562 - v3.0.4
                                # Only rename if exists
                                if all_metrics_sent_to_graphite:
                                    self.redis_conn.rename('flux.workers.metrics_sent', 'aet.flux.workers.metrics_sent')
                            except:
                                # pass
                                # @added 20220428 - Feature #4536: Handle Redis failure
                                if not failed_over_to_memcache:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to generate a list from flux.workers.metrics_sent Redis set')
                                if settings.MEMCACHE_ENABLED:
                                    try:
                                        all_metrics_sent_to_graphite = get_memcache_key('flux', 'flux.workers.metrics_sent')
                                        if all_metrics_sent_to_graphite:
                                            all_metrics_sent_to_graphite = len(all_metrics_sent_to_graphite)
                                            logger.info('worker :: all_metrics_sent_to_graphite found in flux.workers.metrics_sent memcache key')
                                            failed_over_to_memcache = True
                                            try:
                                                success = delete_memcache_key(skyline_app, 'flux.workers.metrics_sent')
                                            except Exception as err:
                                                logger.error('error :: worker :: failed to delete memcache key flux.workers.metrics_sent - %s' % (
                                                    err))
                                        else:
                                            all_metrics_sent_to_graphite = []
                                    except Exception as err:
                                        logger.error('error :: worker :: failed to get memcache key flux.workers.metrics_sent - %s' % (
                                            err))
                                        all_metrics_sent_to_graphite = int(metrics_sent_to_graphite)
                            send_graphite_metric(skyline_app, skyline_metric, all_metrics_sent_to_graphite)
                            logger.info('worker :: all_metrics_sent_to_graphite in last 60 seconds - %s' % str(all_metrics_sent_to_graphite))
                            last_sent_to_graphite = int(time())
                            metrics_sent_to_graphite = 0
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to send_graphite_metric %s with %s' % (
                                skyline_metric, str(metrics_sent_to_graphite)))

                        # @added 20220329 - Feature #4018: thunder - skyline.errors
                        # Report app up
                        try:
                            self.redis_conn.setex('flux.worker', 120, int(time()))
                        except Exception as err:
                            if not failed_over_to_memcache:
                                logger.error(traceback.format_exc())
                                logger.error('error :: worker :: failed to set flux.worker Redis key - %s' % (
                                    err))

                        # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                        # @modified 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                        # Send to the Graphite that collects skyline metrics not
                        # use_backfill = False
                        # timestamp_for_metrics = int(time())
                        discarded_already_received = 0
                        skyline_metric = '%s.discarded.already_received' % skyline_app_graphite_namespace
                        try:
                            discarded_already_received = len(list(self.redis_conn_decoded.smembers('flux.workers.discarded.already_received')))
                            self.redis_conn.delete('flux.workers.discarded.already_received')
                        except Exception as e:
                            if not failed_over_to_memcache:
                                logger.error('error :: worker :: failed to get Redis set flux.workers.discarded.already_received - %s' % e)
                        logger.info('worker :: discarded_already_received in last 60 seconds (in empty queue block) - %s' % str(discarded_already_received))
                        # @modified 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                        # Send to the Graphite that collects skyline metrics not
                        # via flux queue which could send to a different Graphite
                        # skyline_metric_data = [skyline_metric, discarded_already_received, timestamp_for_metrics, use_backfill]
                        # try:
                        #     self.q.put(skyline_metric_data, block=False)
                        # except Exception as e:
                        #     logger.error('error :: worker :: failed to add data %s to send to Graphite to flux.httpMetricDataQueue - %s' % (
                        #         str(metric_data), e))
                        # @added 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                        # Send to the Graphite that collects skyline metrics not
                        # via flux queue which could send to a different Graphite
                        try:
                            send_graphite_metric(skyline_app, skyline_metric, discarded_already_received)
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                                skyline_metric, str(discarded_already_received), e))

                        listen_discarded_invalid_timestamp = 0
                        skyline_metric = '%s.discarded.invalid_timestamp' % listen_graphite_namespace
                        try:
                            listen_discarded_invalid_timestamp = len(list(self.redis_conn_decoded.smembers('flux.listen.discarded.invalid_timestamp')))
                            self.redis_conn.delete('flux.listen.discarded.invalid_timestamp')
                        except Exception as e:
                            if not failed_over_to_memcache:
                                logger.error('error :: worker :: failed to get Redis set flux.listen.discarded.invalid_timestamp - %s' % e)
                        logger.info('worker :: listen_discarded_invalid_timestamp in last 60 seconds (in empty queue block) - %s' % str(listen_discarded_invalid_timestamp))
                        # @modified 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                        # Send to the Graphite that collects skyline metrics not
                        # via flux queue which could send to a different Graphite
                        # skyline_metric_data = [skyline_metric, listen_discarded_invalid_timestamp, timestamp_for_metrics, use_backfill]
                        # try:
                        #     self.q.put(skyline_metric_data, block=False)
                        # except Exception as e:
                        #     logger.error('error :: worker :: failed to add data %s to send to Graphite to flux.httpMetricDataQueue - %s' % (
                        #         str(metric_data), e))
                        # @added 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                        # Send to the Graphite that collects skyline metrics not
                        # via flux queue which could send to a different Graphite
                        try:
                            send_graphite_metric(skyline_app, skyline_metric, listen_discarded_invalid_timestamp)
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                                skyline_metric, str(listen_discarded_invalid_timestamp), e))

                        listen_discarded_metric_name = 0
                        skyline_metric = '%s.discarded.metric_name' % listen_graphite_namespace
                        try:
                            listen_discarded_metric_name = len(list(self.redis_conn_decoded.smembers('flux.listen.discarded.metric_name')))
                            self.redis_conn.delete('flux.listen.discarded.metric_name')
                        except Exception as e:
                            if not failed_over_to_memcache:
                                logger.error('error :: worker :: failed to get Redis set flux.listen.discarded.metric_name - %s' % e)
                        logger.info('worker :: listen_discarded_metric_name in last 60 seconds (in empty queue block) - %s' % str(listen_discarded_metric_name))
                        # @modified 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                        # Send to the Graphite that collects skyline metrics not
                        # via flux queue which could send to a different Graphite
                        # skyline_metric_data = [skyline_metric, listen_discarded_metric_name, timestamp_for_metrics, use_backfill]
                        # try:
                        #     self.q.put(skyline_metric_data, block=False)
                        # except Exception as e:
                        #     logger.error('error :: worker :: failed to add data %s to send to Graphite to flux.httpMetricDataQueue - %s' % (
                        #         str(metric_data), e))
                        # @added 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                        # Send to the Graphite that collects skyline metrics not
                        # via flux queue which could send to a different Graphite
                        try:
                            send_graphite_metric(skyline_app, skyline_metric, listen_discarded_metric_name)
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                                skyline_metric, str(listen_discarded_metric_name), e))

                        listen_discarded_invalid_value = 0
                        skyline_metric = '%s.discarded.invalid_value' % listen_graphite_namespace
                        try:
                            listen_discarded_invalid_value = len(list(self.redis_conn_decoded.smembers('flux.listen.discarded.invalid_value')))
                            self.redis_conn.delete('flux.listen.discarded.invalid_value')
                        except Exception as e:
                            if not failed_over_to_memcache:
                                logger.error('error :: worker :: failed to get Redis set flux.listen.discarded.invalid_value - %s' % e)
                        logger.info('worker :: listen_discarded_invalid_value in last 60 seconds (in empty queue block) - %s' % str(listen_discarded_invalid_value))
                        # @modified 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                        # Send to the Graphite that collects skyline metrics not
                        # via flux queue which could send to a different Graphite
                        # skyline_metric_data = [skyline_metric, listen_discarded_metric_name, timestamp_for_metrics, use_backfill]
                        # skyline_metric_data = [skyline_metric, listen_discarded_invalid_value, timestamp_for_metrics, use_backfill]
                        # try:
                        #     self.q.put(skyline_metric_data, block=False)
                        # except Exception as e:
                        #     logger.error('error :: worker :: failed to add data %s to send to Graphite to flux.httpMetricDataQueue - %s' % (
                        #         str(metric_data), e))
                        # @added 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                        # Send to the Graphite that collects skyline metrics not
                        # via flux queue which could send to a different Graphite
                        try:
                            send_graphite_metric(skyline_app, skyline_metric, listen_discarded_invalid_value)
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                                skyline_metric, str(listen_discarded_invalid_value), e))

                        listen_discarded_invalid_key = 0
                        skyline_metric = '%s.discarded.invalid_key' % listen_graphite_namespace
                        try:
                            listen_discarded_invalid_key = len(list(self.redis_conn_decoded.smembers('flux.listen.discarded.invalid_key')))
                            self.redis_conn.delete('flux.listen.discarded.invalid_key')
                        except Exception as e:
                            if not failed_over_to_memcache:
                                logger.error('error :: worker :: failed to get Redis set flux.listen.discarded.invalid_key - %s' % e)
                        logger.info('worker :: listen_discarded_invalid_key in last 60 seconds (in empty queue block) - %s' % str(listen_discarded_invalid_key))
                        # @modified 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                        # Send to the Graphite that collects skyline metrics not
                        # via flux queue which could send to a different Graphite
                        # skyline_metric_data = [skyline_metric, listen_discarded_invalid_key, timestamp_for_metrics, use_backfill]
                        # try:
                        #     self.q.put(skyline_metric_data, block=False)
                        # except Exception as e:
                        #     logger.error('error :: worker :: failed to add data %s to send to Graphite to flux.httpMetricDataQueue - %s' % (
                        #         str(metric_data), e))
                        # @added 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                        # Send to the Graphite that collects skyline metrics not
                        # via flux queue which could send to a different Graphite
                        try:
                            send_graphite_metric(skyline_app, skyline_metric, listen_discarded_invalid_key)
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                                skyline_metric, str(listen_discarded_invalid_key), e))

                        listen_discarded_invalid_parameters = 0
                        skyline_metric = '%s.discarded.invalid_parameters' % listen_graphite_namespace
                        try:
                            listen_discarded_invalid_parameters = len(list(self.redis_conn_decoded.smembers('flux.listen.discarded.invalid_parameters')))
                            self.redis_conn.delete('flux.listen.discarded.invalid_parameters')
                        except Exception as e:
                            if not failed_over_to_memcache:
                                logger.error('error :: worker :: failed to get Redis set flux.listen.discarded.invalid_parameters - %s' % e)
                        logger.info('worker :: listen_discarded_invalid_parameters in last 60 seconds (in empty queue block) - %s' % str(listen_discarded_invalid_parameters))
                        # @modified 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                        # Send to the Graphite that collects skyline metrics not
                        # via flux queue which could send to a different Graphite
                        # skyline_metric_data = [skyline_metric, listen_discarded_invalid_parameters, timestamp_for_metrics, use_backfill]
                        # try:
                        #     self.q.put(skyline_metric_data, block=False)
                        # except Exception as e:
                        #     logger.error('error :: worker :: failed to add data %s to send to Graphite to flux.httpMetricDataQueue - %s' % (
                        #         str(metric_data), e))
                        # @added 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                        # Send to the Graphite that collects skyline metrics not
                        # via flux queue which could send to a different Graphite
                        try:
                            send_graphite_metric(skyline_app, skyline_metric, listen_discarded_invalid_parameters)
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                                skyline_metric, str(listen_discarded_invalid_parameters), e))

                        listen_added_to_queue = 0
                        skyline_metric = '%s.added_to_queue' % listen_graphite_namespace
                        try:
                            listen_added_to_queue_str = None
                            listen_added_to_queue_str = self.redis_conn_decoded.getset('flux.listen.added_to_queue', 0)
                            if listen_added_to_queue_str:
                                listen_added_to_queue = int(listen_added_to_queue_str)
                        except Exception as e:
                            if not failed_over_to_memcache:
                                logger.error('error :: worker :: failed to get Redis set flux.listen.added_to_queue - %s' % e)
                            # @added 20220428 - Feature #4536: Handle Redis failure
                            if settings.MEMCACHE_ENABLED:
                                try:
                                    listen_added_to_queue = get_memcache_key('flux', 'flux.listen.added_to_queue')
                                    if not listen_added_to_queue:
                                        listen_added_to_queue = 0
                                except:
                                    listen_added_to_queue = 0
                                if listen_added_to_queue:
                                    success = False
                                    try:
                                        success = delete_memcache_key(skyline_app, 'flux.listen.added_to_queue')
                                    except Exception as err:
                                        logger.error('error :: worker :: failed to delete memcache flux.listen.added_to_queue - %s' % (str(err)))
                                    if success:
                                        logger.info('worker :: deleted flux.listen.added_to_queue memcache key as Redis available')

                        logger.info('worker :: listen_added_to_queue in last 60 seconds (in empty queue block) - %s' % str(listen_added_to_queue))
                        # @modified 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                        # Send to the Graphite that collects skyline metrics not
                        # via flux queue which could send to a different Graphite
                        # skyline_metric_data = [skyline_metric, listen_added_to_queue, timestamp_for_metrics, use_backfill]
                        # try:
                        #     self.q.put(skyline_metric_data, block=False)
                        # except Exception as e:
                        #     logger.error('error :: worker :: failed to add data %s to send to Graphite to flux.httpMetricDataQueue - %s' % (
                        #         str(metric_data), e))
                        # @added 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                        # Send to the Graphite that collects skyline metrics not
                        # via flux queue which could send to a different Graphite
                        try:
                            send_graphite_metric(skyline_app, skyline_metric, listen_added_to_queue)
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                                skyline_metric, str(listen_added_to_queue), e))

                        # @added 20220302 - Feature #4400: flux - quota
                        # If there are flux namespace quotas add a metric to track
                        # over quota count and dropped_non_numeric_metrics
                        flux_namespace_quotas = {}
                        try:
                            flux_namespace_quotas = self.redis_conn_decoded.hgetall('metrics_manager.flux.namespace_quotas')
                        except Exception as err:
                            if not failed_over_to_memcache:
                                logger.error(traceback.format_exc())
                                logger.error('error :: worker :: failed to hgetall metrics_manager.flux.namespace_quotas - %s' % err)
                            # @added 20220428 - Feature #4536: Handle Redis failure
                            if settings.MEMCACHE_ENABLED:
                                try:
                                    flux_namespace_quotas = get_memcache_key('flux', 'metrics_manager.flux.namespace_quotas')
                                    if not flux_namespace_quotas:
                                        flux_namespace_quotas = {}
                                except:
                                    flux_namespace_quotas = {}
                        if flux_namespace_quotas:
                            over_quota_count = 0
                            skyline_metric = '%s.over_quota_metrics_count' % listen_graphite_namespace
                            try:
                                over_quota_count = self.redis_conn_decoded.scard('flux.listen.over_quota.rejected_metrics')
                                if over_quota_count:
                                    self.redis_conn_decoded.rename('flux.listen.over_quota.rejected_metrics', 'aet.flux.listen.over_quota.rejected_metrics')
                            except Exception as err:
                                if not failed_over_to_memcache:
                                    logger.error('error :: worker :: failed to get Redis set flux.listen.over_quota.rejected_metrics - %s' % err)
                                # @added 20220428 - Feature #4536: Handle Redis failure
                                if settings.MEMCACHE_ENABLED:
                                    try:
                                        over_quota_count_list = get_memcache_key('flux', 'flux.listen.over_quota.rejected_metrics')
                                        if over_quota_count_list:
                                            over_quota_count = len(over_quota_count)
                                            success = False
                                            try:
                                                success = delete_memcache_key(skyline_app, 'flux.listen.over_quota.rejected_metrics')
                                            except Exception as err:
                                                logger.error('error :: worker :: failed to delete memcache flux.listen.over_quota.rejected_metrics key - %s' % (str(err)))
                                            if success:
                                                logger.info('worker :: deleted flux.listen.over_quota.rejected_metrics memcache key')
                                        else:
                                            over_quota_count = 0
                                    except:
                                        over_quota_count = 0

                            logger.info('worker :: listen namespaces over_quota metric count in last 60 seconds - %s' % str(over_quota_count))
                            try:
                                send_graphite_metric(skyline_app, skyline_metric, over_quota_count)
                            except Exception as err:
                                logger.error(traceback.format_exc())
                                logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                                    skyline_metric, str(over_quota_count), err))
                        dropped_non_numeric_metrics_count = 0
                        skyline_metric = '%s.dropped_non_numeric_metrics' % listen_graphite_namespace
                        try:
                            dropped_non_numeric_metrics_count = self.redis_conn_decoded.scard('flux.listen.dropped_non_numeric_metrics')
                            if dropped_non_numeric_metrics_count:
                                self.redis_conn_decoded.rename('flux.listen.dropped_non_numeric_metrics', 'aet.flux.listen.dropped_non_numeric_metrics')
                        except Exception as err:
                            if not failed_over_to_memcache:
                                logger.error('error :: worker :: failed to get Redis set flux.listen.dropped_non_numeric_metrics - %s' % err)
                        logger.info('worker :: listen dropped_non_numeric_metrics count in last 60 seconds - %s' % str(dropped_non_numeric_metrics_count))
                        try:
                            send_graphite_metric(skyline_app, skyline_metric, dropped_non_numeric_metrics_count)
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                                skyline_metric, str(dropped_non_numeric_metrics_count), err))

                        # @added 20220303 - Feature #4400: flux - quota
                        listen_added_to_aggregation_queue = 0
                        skyline_metric = '%s.added_to_aggregation_queue' % listen_graphite_namespace
                        try:
                            listen_added_to_aggregation_queue = None
                            listen_added_to_aggregation_queue_str = self.redis_conn_decoded.getset('flux.listen.added_to_aggregation_queue', 0)
                            if listen_added_to_aggregation_queue_str:
                                listen_added_to_aggregation_queue = int(listen_added_to_aggregation_queue_str)
                        except Exception as err:
                            if not failed_over_to_memcache:
                                logger.error('error :: worker :: failed to get Redis set flux.listen.added_to_aggregation_queue - %s' % err)
                        logger.info('worker :: listen added_to_aggregation_queue in last 60 seconds - %s' % str(listen_added_to_aggregation_queue))
                        try:
                            send_graphite_metric(skyline_app, skyline_metric, listen_added_to_aggregation_queue)
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                                skyline_metric, str(listen_added_to_aggregation_queue), err))

                    metric_data_queue_size = 0
                    try:
                        metric_data_queue_size = self.q.qsize()
                        logger.info('worker :: flux.httpMetricDataQueue queue size - %s' % str(metric_data_queue_size))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to determine size of queue flux.httpMetricDataQueue')
                    skyline_metric = '%s.httpMetricDataQueue.size' % skyline_app_graphite_namespace
                    if primary_worker:
                        try:
                            send_graphite_metric(skyline_app, skyline_metric, metric_data_queue_size)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to send_graphite_metric %s with %s' % (
                                skyline_metric, str(metrics_sent_to_graphite)))
                    # @added 20201019 - Feature #3790: flux - pickle to Graphite
                    if metric_data_queue_size > 10:
                        send_to_reciever = 'pickle'
                    else:
                        send_to_reciever = 'line'
                    send_to_reciever = 'pickle'

                    # @added 202011120 - Feature #3790: flux - pickle to Graphite
                    # Debug Redis set
                    metrics_data_sent_strs = []
                    for item in metrics_data_sent:
                        metrics_data_sent_strs.append(str(item))
                    if metrics_data_sent_strs:
                        try:
                            self.redis_conn.sadd('flux.metrics_data_sent', *set(metrics_data_sent_strs))
                            logger.info('worker :: added %s items to the flux.metrics_data_sent Redis set' % str(len(metrics_data_sent)))
                        except:
                            if not failed_over_to_memcache:
                                logger.error(traceback.format_exc())
                                logger.error('error :: worker :: failed to determine size of flux.queue Redis set')
                        metrics_data_sent = []
                        try:
                            new_set = 'aet.flux.metrics_data_sent.%s' % str(self.current_pid)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to current_pid for aet.flux.metrics_data_sent Redis set name')
                            new_set = 'aet.flux.metrics_data_sent'
                        if primary_worker:
                            try:
                                self.redis_conn.rename('flux.metrics_data_sent', new_set)
                                logger.info('worker :: renamed flux.metrics_data_sent Redis set to %s' % new_set)
                            # @added 20201128 - Feature #3820: HORIZON_SHARDS
                            # With metrics that come in at a frequency of less
                            # than 60 seconds, it is possible that this key will
                            # not exist as flux has not been sent metric data
                            # so this operation will error with no such key
                            except Exception as err:
                                traceback_str = traceback.format_exc()
                                if not failed_over_to_memcache:
                                    if 'no such key' in str(err):
                                        logger.warn('warning :: worker :: failed to rename flux.metrics_data_sent to %s Redis set - flux has not recieved data in 60 seconds - %s' % (new_set, err))
                                    else:
                                        logger.error(traceback_str)
                                        logger.error('error :: worker :: failed to rename flux.metrics_data_sent to %s Redis set' % new_set)
                            try:
                                self.redis_conn.expire(new_set, 600)
                            except Exception as err:
                                if not failed_over_to_memcache:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: worker :: failed to set 600 seconds TTL on %s Redis set - %s' % (new_set, err))

                    # @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
                    if FLUX_PERSIST_QUEUE:
                        redis_set_size = 0
                        try:
                            redis_set_size = self.redis_conn.scard('flux.queue')
                        except:
                            if not failed_over_to_memcache:
                                logger.error(traceback.format_exc())
                                logger.error('error :: worker :: failed to determine size of flux.queue Redis set')
                        logger.info('worker - flux.queue Redis set size of %s before removal of %s items' % (
                            str(redis_set_size), str(len(remove_from_flux_queue_redis_set))))
                        if remove_from_flux_queue_redis_set:
                            try:
                                self.redis_conn.srem('flux.queue', *set(remove_from_flux_queue_redis_set))
                                remove_from_flux_queue_redis_set = []
                            except:
                                if not failed_over_to_memcache:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: worker :: failed to remove multiple items from flux.queue Redis set')
                            try:
                                redis_set_size = self.redis_conn.scard('flux.queue')
                            except:
                                if not failed_over_to_memcache:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: worker :: failed to determine size of flux.queue Redis set')
                            logger.info('worker - flux.queue Redis set size of %s after the removal of items' % (
                                str(redis_set_size)))
                            remove_from_flux_queue_redis_set = []
                    # @added 20201020 - Feature #3796: FLUX_CHECK_LAST_TIMESTAMP
                    # Even if flux.last Redis keys are disabled in flux they are used in
                    # Vista
                    vista_metrics = []
                    if not FLUX_CHECK_LAST_TIMESTAMP and VISTA_ENABLED:
                        try:
                            vista_metrics = list(self.redis_conn_decoded.sscan_iter('vista.metrics', match='*'))
                        except:
                            vista_metrics = []

                    # @added 20210407 - Bug #4002: Change flux FLUX_ZERO_FILL_NAMESPACES to pickle
                    # Reset metrics_sent
                    metrics_sent = []

                    primary_worker_pid = 0
                    try:
                        primary_worker_pid = int(self.redis_conn_decoded.get(primary_worker_key))
                        if primary_worker_pid:
                            logger.info('worker :: primary_worker_pid found in Redis key - %s' % str(primary_worker_pid))
                    except:
                        primary_worker_pid = 0
                        # @added 20220428 - Feature #4536: Handle Redis failure
                        if settings.MEMCACHE_ENABLED:
                            try:
                                primary_worker_pid = get_memcache_key('flux', 'flux.worker.primary_worker_key')
                                if primary_worker_pid:
                                    logger.info('worker :: primary_worker_key found in memcache key - %s' % str(primary_worker_pid))
                                    failed_over_to_memcache = True
                                else:
                                    primary_worker_pid = 0
                            except:
                                primary_worker_pid = 0

                    if not primary_worker_pid:
                        logger.info('worker :: no primary_worker found, taking role')
                        try:
                            self.redis_conn.setex(primary_worker_key, 75, worker_pid)
                            primary_worker_pid = int(self.redis_conn_decoded.get(primary_worker_key))
                            logger.info('worker :: set self pid to primary_worker - %s' % str(primary_worker_pid))
                        except:
                            primary_worker_pid = 0
                            # @added 20220428 - Feature #4536: Handle Redis failure
                            if settings.MEMCACHE_ENABLED:
                                success = False
                                try:
                                    success = set_memcache_key('flux', 'flux.worker.primary_worker_key', worker_pid, 75)
                                except Exception as err:
                                    logger.error('error :: worker :: failed to set memcache primary_worker_key - %s' % (str(err)))
                                if success:
                                    primary_worker_pid = worker_pid
                                    logger.info('worker :: primary_worker_key set in memcache primary_worker_key - %s' % str(primary_worker_pid))

                    if primary_worker_pid and primary_worker:
                        if primary_worker_pid != worker_pid:
                            logger.info('worker :: primary_worker role has been taken over by %s' % str(primary_worker_pid))
                            primary_worker = False
                    if primary_worker_pid == worker_pid:
                        if not primary_worker:
                            logger.info('worker :: taking over primary_worker role')
                        primary_worker = True

                    # @added 20220428 - Feature #4536: Handle Redis failure
                    # Swap to using a Redis hash instead of the
                    # flux.last.<metric> keys
                    if settings.MEMCACHE_ENABLED and primary_worker:
                        redis_last_metrics_data_dict = {}
                        try:
                            redis_last_metrics_data_dict = self.redis_conn_decoded.hgetall('flux.last.metric_data')
                        except Exception as err:
                            if not failed_over_to_memcache:
                                logger.error('error :: worker :: failed to hgetall flux.last.metric_data - %s' % (
                                    err))
                        # Use the local data to update the memcache data, each
                        # worker will update the data
                        if not redis_last_metrics_data_dict:
                            memcache_flux_last_metric_data_dict = {}
                            try:
                                memcache_flux_last_metric_data_dict = get_memcache_key('flux', 'flux.last.metric_data')
                                if not memcache_flux_last_metric_data_dict:
                                    memcache_flux_last_metric_data_dict = {}
                            except Exception as err:
                                logger.error('error :: worker :: failed get_memcache_key flux.last.metric_data - %s' % (
                                    err))
                            for metric in list(new_memcache_flux_last_metric_data.keys()):
                                memcache_flux_last_metric_data_dict[metric] = new_memcache_flux_last_metric_data[metric]
                            redis_last_metrics_data_dict = memcache_flux_last_metric_data_dict
                        if redis_last_metrics_data_dict:
                            success = False
                            try:
                                success = set_memcache_key('flux', 'flux.last.metric_data', redis_last_metrics_data_dict)
                            except Exception as err:
                                logger.error('error :: worker :: failed to set memcache key flux.last.metric_data - %s' % (
                                    err))
                            if success:
                                logger.info('worker :: set memcache key flux.last.metric_data')

                    # @added 20220428 - Feature #4536: Handle Redis failure
                    set_in_redis = False

                    if primary_worker:
                        try:
                            self.redis_conn.setex(primary_worker_key, 75, worker_pid)
                            primary_worker_pid = int(self.redis_conn_decoded.get(primary_worker_key))
                            logger.info('worker :: set Redis primary_worker_key key to self pid to primary_worker - %s' % str(primary_worker_pid))
                            # @added 20220428 - Feature #4536: Handle Redis failure
                            set_in_redis = True
                        except Exception as e:
                            if not failed_over_to_memcache:
                                logger.error('error :: worker :: failed to set Redis primary_worker_key key to self pid - %s' % (str(e)))
                            # @added 20220428 - Feature #4536: Handle Redis failure
                            if settings.MEMCACHE_ENABLED:
                                success = False
                                try:
                                    success = set_memcache_key('flux', 'flux.worker.primary_worker_key', worker_pid, 75)
                                except Exception as err:
                                    logger.error('error :: worker :: failed to set memcache primary_worker_key - %s' % (str(err)))
                                if success:
                                    primary_worker_pid = worker_pid
                                    logger.info('worker :: primary_worker_key set in memcache primary_worker_key - %s' % str(primary_worker_pid))
                        # @added 20220428 - Feature #4536: Handle Redis failure
                        if set_in_redis and main_process_pid_from_memcache:
                            success = False
                            logger.info('worker :: trying to set flux.main_process_pid in Redis as retrieved from memcache')
                            try:
                                success = self.redis_conn_decoded.set('flux.main_process_pid', main_process_pid)
                            except Exception as err:
                                if not failed_over_to_memcache:
                                    logger.error('error :: worker :: failed to set Redis flux.main_process_pid key - %s' % (str(err)))
                            if success:
                                main_process_pid_from_memcache = False
                                logger.info('worker :: set flux.main_process_pid in Redis, deleting memcache key as Redis available')
                                success = False
                                try:
                                    success = delete_memcache_key(skyline_app, 'flux.main_process_pid')
                                except Exception as err:
                                    logger.error('error :: worker :: failed to delete memcache primary_worker_key - %s' % (str(err)))
                                if success:
                                    logger.info('worker :: deleted flux.main_process_pid memcache key as Redis available')

                    else:
                        last_sent_to_graphite = int(time())

            # except NotImplementedError:
            #    pass
            except KeyboardInterrupt:
                logger.info('worker :: server has been issued a user signal to terminate - KeyboardInterrupt')
            except SystemExit:
                logger.info('worker :: server was interrupted - SystemExit')
            except Exception as e:
                logger.error('error :: worker :: %s' % (str(e)))

            # @added 20200206 - Feature #3444: Allow flux to backfill
            # Added backfill
            backfill = False

            # @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
            if metric_data and FLUX_PERSIST_QUEUE:
                try:
                    # Do not remove each individual metrics from the flux.queue
                    # Redis set, add to a list that is removed in one srem *set
                    # operation each 60 seconds.  This is a more perfomant
                    # method and requires a single blocking call for a batch of
                    # metrics, rather than a blocking call for every metric.
                    # self.redis_conn.srem('flux.queue', str(metric_data))
                    remove_from_flux_queue_redis_set.append(str(metric_data))
                except:
                    pass

            if metric_data:
                try:
                    metric = str(metric_data[0])
                    value = float(metric_data[1])
                    timestamp = int(metric_data[2])
                    # @added 20200206 - Feature #3444: Allow flux to backfill
                    # Added backfill, convert the boolean to an int
                    backfill = int(metric_data[3])
                    if LOCAL_DEBUG:
                        logger.info('worker :: debug :: queue item found - %s' % str(metric_data))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: worker :: failed to interpolate metric, value, timestamp from metric_data - %s' % str(metric_data))
                    continue

                # @added 20201020 - Feature #3796: FLUX_CHECK_LAST_TIMESTAMP
                # Only check flux.last key if this is not backfill and
                # FLUX_CHECK_LAST_TIMESTAMP is enable or it is in VISTA_ENABLED
                cache_key = None
                # if FLUX_CHECK_LAST_TIMESTAMP:
                cache_key = 'flux.last.%s' % metric
                check_flux_last_key = False
                if not backfill and FLUX_CHECK_LAST_TIMESTAMP:
                    check_flux_last_key = True
                if VISTA_ENABLED:
                    if metric in vista_metrics:
                        check_flux_last_key = True

                if settings.FLUX_SEND_TO_CARBON:
                    # Best effort de-duplicate the data
                    valid_data = True

                    # @added 20200818 - Feature #3694: flux - POST multiple metrics
                    # Handle Redis and literal_eval separately
                    redis_last_metric_data = None

                    # @modified 20200206 - Feature #3444: Allow flux to backfill
                    # Only check flux.last key if this is not backfill
                    # @modified 20201020 - Feature #3796: FLUX_CHECK_LAST_TIMESTAMP
                    # Use the check_flux_last_key value determined above
                    # if not backfill:
                    if check_flux_last_key:
                        # @modified 20201020 - Feature #3796: FLUX_CHECK_LAST_TIMESTAMP
                        # Set cache_key outside the conditional block
                        # cache_key = 'flux.last.%s' % metric
                        last_metric_timestamp = None

                        # @added 20220428 - Feature #4536: Handle Redis failure
                        # Swap to using a Redis hash instead of the
                        # flux.last.<metric> keys
                        redis_last_metric_data_dict = {}
                        try:
                            redis_last_metric_data_dict = get_last_metric_data(skyline_app, metric)
                        except Exception as err:
                            if not failed_over_to_memcache:
                                logger.error('error :: worker :: get_last_metric_data failed - %s' % (
                                    err))

                        use_old_timestamp_keys = True
                        if redis_last_metric_data_dict:
                            try:
                                last_metric_timestamp = redis_last_metric_data_dict['timestamp']
                                use_old_timestamp_keys = False
                            except KeyError:
                                last_metric_timestamp = None
                            except Exception as err:
                                logger.error('error :: worker :: failed to get timestamp from - %s - %s' % (
                                    str(redis_last_metric_data_dict), err))
                                last_metric_timestamp = None

                        redis_last_metric_data = None
                        if not last_metric_timestamp:
                            try:
                                # @modified 20191128 - Bug #3266: py3 Redis binary objects not strings
                                #                      Branch #3262: py3
                                # redis_last_metric_data = self.redis_conn.get(cache_key)
                                redis_last_metric_data = self.redis_conn_decoded.get(cache_key)
                            except Exception as e:
                                if not failed_over_to_memcache:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: worker :: failed to determine last_metric_timestamp from Redis key %s - %s' % (
                                        str(cache_key), e))
                                redis_last_metric_data = None

                        # @modified 20200818 - Feature #3694: flux - POST multiple metrics
                        # Handle Redis and literal_eval separately, only
                        # literal_eval if Redis had data for the key
                        if redis_last_metric_data:
                            try:
                                last_metric_data = literal_eval(redis_last_metric_data)
                                last_metric_timestamp = int(last_metric_data[0])
                                if LOCAL_DEBUG:
                                    logger.info('worker :: debug :: last_metric_timestamp for %s from %s is %s' % (metric, str(cache_key), str(last_metric_timestamp)))
                            except Exception as e:
                                logger.error(traceback.format_exc())
                                logger.error('error :: worker :: failed to determine last_metric_timestamp from Redis key %s - %s' % (
                                    str(cache_key), e))
                                last_metric_timestamp = False

                        if last_metric_timestamp:
                            if timestamp <= last_metric_timestamp:
                                valid_data = False
                                logger.info('worker :: debug :: not valid data - the queue data timestamp %s is <= to the last_metric_timestamp %s for %s' % (
                                    str(timestamp), str(last_metric_timestamp), metric))
                                if LOCAL_DEBUG:
                                    logger.info('worker :: debug :: not valid data - the queue data timestamp %s is <= to the last_metric_timestamp %s for %s' % (
                                        str(timestamp), str(last_metric_timestamp), metric))

                    if valid_data:
                        submittedToGraphite = False
                        if send_to_reciever == 'line':
                            try:
                                graphyte.send(metric, value, timestamp)
                                submittedToGraphite = True
                                # modified 20201016 - Feature #3788: snab_flux_load_test
                                if FLUX_VERBOSE_LOGGING:
                                    logger.info('worker :: sent %s, %s, %s to Graphite - via graphyte' % (str(metric), str(value), str(timestamp)))
                                metrics_sent_to_graphite += 1
                                # @added 20200827 - Feature #3708: FLUX_ZERO_FILL_NAMESPACES
                                metrics_sent.append(metric)
                                # @added 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                                # Better handle multiple workers
                                try:
                                    self.redis_conn.sadd('flux.workers.metrics_sent', metric)
                                except:
                                    # pass
                                    # @added 20220428 - Feature #4536: Handle Redis failure
                                    if settings.MEMCACHE_ENABLED:
                                        sent_add_to_memcache.append(metric)

                                # @added 202011120 - Feature #3790: flux - pickle to Graphite
                                # Debug Redis set
                                metrics_data_sent.append([metric, value, timestamp])
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: worker :: failed to send metric data to Graphite for %s' % str(metric))
                                metric = None

                        if send_to_reciever == 'pickle':
                            # @modified 20201212 - Task #3864: flux - try except everything
                            try:
                                tuple_data = (metric, (int(timestamp), float(value)))
                                pickle_data.append(tuple_data)
                                if FLUX_VERBOSE_LOGGING:
                                    logger.info('worker :: sending %s, %s, %s to Graphite - via pickle' % (str(metric), str(value), str(timestamp)))
                                submittedToGraphite = True
                                metrics_sent_to_graphite += 1
                                metrics_sent.append(metric)
                                # @added 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                                # Better handle multiple workers
                                try:
                                    self.redis_conn.sadd('flux.workers.metrics_sent', metric)
                                except Exception as e:
                                    if not failed_over_to_memcache:
                                        logger.error('error :: worker :: failed to add metric to flux.workers.metrics_sent Redis set - %s' % str(e))
                                    # @added 20220428 - Feature #4536: Handle Redis failure
                                    if settings.MEMCACHE_ENABLED:
                                        sent_add_to_memcache.append(metric)

                                # @added 202011120 - Feature #3790: flux - pickle to Graphite
                                # Debug Redis set
                                metrics_data_sent.append([metric, value, timestamp])
                            except Exception as err:
                                logger.error('error :: worker :: failed to append to pickle_data - %s' % str(err))

                        if submittedToGraphite:
                            # Update the metric Redis flux key
                            # @modified 20200206 - Feature #3444: Allow flux to backfill
                            # Only update the flux.last key if this is not backfill
                            # @modified 20201020 - Feature #3796: FLUX_CHECK_LAST_TIMESTAMP
                            # Use the check_flux_last_key value determined above
                            # if not backfill:
                            if check_flux_last_key:
                                metric_data = [timestamp, value]

                                # @modified 20201207 - Task #3864: flux - try except everything
                                if use_old_timestamp_keys:
                                    try:
                                        # @modified 20220428 - Feature #4536: Handle Redis failure
                                        # Swap to using a Redis hash instead of the
                                        # flux.last.<metric> keys make existing keys
                                        # expire
                                        # self.redis_conn.set(cache_key, str(metric_data))
                                        self.redis_conn.setex(cache_key, 3600, str(metric_data))
                                    except Exception as err:
                                        if not failed_over_to_memcache:
                                            logger.error('error :: worker :: failed to set check_flux_last_key Redis key - %s' % str(err))
                                # @added 20220428 - Feature #4536: Handle Redis failure
                                # Swap to using a Redis hash instead of the
                                # flux.last.<metric> keys
                                metric_data_dict = {'timestamp': timestamp, 'value': value}
                                new_memcache_flux_last_metric_data[metric] = metric_data_dict
                                try:
                                    self.redis_conn.hset('flux.last.metric_data', metric, str(metric_data_dict))
                                except Exception as err:
                                    if not failed_over_to_memcache:
                                        logger.error('error :: worker :: failed to set flux.last.metric_data Redis key - %s' % str(err))

                            # @added 20200213 - Bug #3448: Repeated airgapped_metrics
                            else:
                                # @added 20201120 - Feature #3796: FLUX_CHECK_LAST_TIMESTAMP
                                #                   Feature #3400: Identify air gaps in the metric data
                                # Only execute if IDENTIFY_AIRGAPS is enabled
                                if IDENTIFY_AIRGAPS:
                                    # @added 20200213 - Bug #3448: Repeated airgapped_metrics
                                    # Add a flux.filled key to Redis with a expiry
                                    # set to FULL_DURATION so that Analyzer knows to
                                    # sort and deduplicate the Redis time series
                                    # data as carbon-relay will send it to Horizon
                                    # and the datapoints will be out of order in the
                                    # Redis key
                                    try:
                                        flux_filled_key = 'flux.filled.%s' % str(metric)
                                        self.redis_conn.setex(
                                            flux_filled_key, settings.FULL_DURATION,
                                            int(time()))
                                        logger.info('worker :: set Redis key %s' % (str(flux_filled_key)))
                                    except Exception as e:
                                        if not failed_over_to_memcache:
                                            logger.error('error :: failed to could not set Redis flux.filled key: %s' % e)
                    else:
                        # modified 20201016 - Feature #3788: snab_flux_load_test
                        if FLUX_VERBOSE_LOGGING:
                            logger.info('worker :: discarded %s, %s, %s as a data point for %s has already been submitted to Graphite' % (
                                str(metric), str(value), str(timestamp), str(timestamp)))

                        # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                        try:
                            self.redis_conn_decoded.sadd('flux.workers.discarded.already_received', str(metric))
                        except Exception as e:
                            if not failed_over_to_memcache:
                                logger.error('error :: worker :: failed to add metric to Redis set flux.workers.discarded.already_received - %s' % e)
                else:
                    logger.info('worker :: settings.FLUX_SEND_TO_CARBON is set to %s, discarded %s, %s, %s' % (
                        str(settings.FLUX_SEND_TO_CARBON), str(metric), str(value), str(timestamp)))

                if settings.FLUX_SEND_TO_STATSD:
                    statsd_conn.incr(metric, value, timestamp)
                    # modified 20201016 - Feature #3788: snab_flux_load_test
                    if FLUX_VERBOSE_LOGGING:
                        logger.info('worker sent %s, %s, %s to statsd' % (metric, str(value), str(timestamp)))
                    # @added 20200827 - Feature #3708: FLUX_ZERO_FILL_NAMESPACES
                    metrics_sent.append(metric)
                    # @added 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                    # Better handle multiple workers
                    try:
                        self.redis_conn.sadd('flux.workers.metrics_sent', metric)
                    except:
                        pass

                submit_pickle_data = False
                if pickle_data:
                    number_of_datapoints = len(pickle_data)
                    if number_of_datapoints >= 1000:
                        submit_pickle_data = True
                    else:
                        try:
                            metric_data_queue_size = self.q.qsize()
                        except:
                            metric_data_queue_size = 0
                        if metric_data_queue_size == 0:
                            submit_pickle_data = True
                if submit_pickle_data:
                    # @modified 20201207 - Task #3864: flux - try except everything
                    try:
                        pickle_data_submitted = submit_pickle_data_to_graphite(pickle_data)
                    except Exception as e:
                        logger.error('error :: worker :: submit_pickle_data_to_graphite failed - %s' % str(e))
                        pickle_data_submitted = False

                    if pickle_data_submitted:
                        pickle_data = []

                    # @added 20220428 - Feature #4536: Handle Redis failure
                    if sent_add_to_memcache:
                        success = False
                        try:
                            success = add_to_memcache_flux_workers_metrics_sent(sent_add_to_memcache)
                        except:
                            pass
                        if success:
                            failed_over_to_memcache = True
                            logger.info('worker :: added %s sent metrics to flux.workers.metrics_sent memcache key' % str(len(sent_add_to_memcache)))
                    sent_add_to_memcache = []

            time_now = int(time())
            if (time_now - last_sent_to_graphite) >= 60:

                # @added 20200827 - Feature #3708: FLUX_ZERO_FILL_NAMESPACES
                # Send 0 for any metric in the flux.zero_fill_metrics Redis set that
                # has not submitted data in the last 60 seconds.  The flux.last
                # Redis key is not updated for these sent 0 values so if the source
                # sends data for a timestamp in the period later (due to a lag, etc),
                # it will be valid and sent to Graphite.

                # @modified 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                # Better handle multiple workers, only do on the primary
                # worker and now that the zero_fill setting can be passed in the
                # FLUX_AGGREGATE_NAMESPACES settings check the Redis set
                # if FLUX_ZERO_FILL_NAMESPACES:
                if primary_worker:
                    if not last_zero_fill_to_graphite:
                        last_zero_fill_to_graphite = time_now - 60
                    run_fill = True
                    # Check that it was not run in the empty exception
                    # immediately before
                    if last_zero_fill_to_graphite in list(range((time_now - 5), time_now)):
                        run_fill = False
                    if run_fill:
                        flux_zero_fill_metrics = []
                        try:
                            flux_zero_fill_metrics = list(self.redis_conn_decoded.smembers('flux.zero_fill_metrics'))
                        except:
                            if not failed_over_to_memcache:
                                logger.info(traceback.format_exc())
                                logger.error('error :: failed to generate a list from flux.zero_fill_metrics Redis set')
                            # @added 20220428 - Feature #4536: Handle Redis failure
                            if settings.MEMCACHE_ENABLED:
                                try:
                                    flux_zero_fill_metrics = get_memcache_key('flux', 'flux.zero_fill_metrics')
                                    if flux_zero_fill_metrics:
                                        logger.info('worker :: flux_zero_fill_metrics found in flux.zero_fill_metrics memcache key')
                                        failed_over_to_memcache = True
                                    else:
                                        flux_zero_fill_metrics = []
                                except Exception as err:
                                    logger.error('error :: worker :: failed to get memcache key flux.zero_fill_metrics - %s' % (
                                        err))
                                    flux_zero_fill_metrics = []

                        # @added 20210408 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                        # Use Redis set
                        try:
                            all_metrics_sent = list(self.redis_conn_decoded.smembers('flux.workers.metrics_sent'))
                        except:
                            if not failed_over_to_memcache:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to generate a list from flux.workers.metrics_sent Redis set')
                            # @added 20220428 - Feature #4536: Handle Redis failure
                            if settings.MEMCACHE_ENABLED:
                                try:
                                    all_metrics_sent = get_memcache_key('flux', 'flux.workers.metrics_sent')
                                    if all_metrics_sent:
                                        logger.info('worker :: all_metrics_sent found in flux.workers.metrics_sent memcache key')
                                        failed_over_to_memcache = True
                                    else:
                                        all_metrics_sent = []
                                except Exception as err:
                                    logger.error('error :: worker :: failed to get memcache key flux.workers.metrics_sent - %s' % (
                                        err))
                                    all_metrics_sent = int(metrics_sent_to_graphite)

                        for flux_zero_fill_metric in flux_zero_fill_metrics:
                            # if flux_zero_fill_metric not in metrics_sent:
                            if flux_zero_fill_metric not in all_metrics_sent:
                                try:
                                    # @modified 20210406 - Bug #4002: Change flux FLUX_ZERO_FILL_NAMESPACES to pickle
                                    # Do not use graphyte to send zeros as it can
                                    # result in missing data, add date to the pickle
                                    # graphyte.send(flux_zero_fill_metric, 0.0, time_now)
                                    tuple_data = (flux_zero_fill_metric, (last_zero_fill_to_graphite, 0.0))
                                    pickle_data.append(tuple_data)

                                    # modified 20201016 - Feature #3788: snab_flux_load_test
                                    if FLUX_VERBOSE_LOGGING:
                                        # @modified 20210406 - Bug #4002: Change flux FLUX_ZERO_FILL_NAMESPACES to pickle
                                        # logger.info('worker :: zero fill - sent %s, %s, %s to Graphite' % (str(flux_zero_fill_metric), str(0.0), str(time_now)))
                                        logger.info('worker :: zero fill - added %s to pickle_data' % (str(tuple_data)))
                                    metrics_sent_to_graphite += 1
                                    metrics_sent.append(flux_zero_fill_metric)
                                    # @added 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                                    # Better handle multiple workers
                                    try:
                                        self.redis_conn.sadd('flux.workers.metrics_sent', flux_zero_fill_metric)
                                    except:
                                        # pass
                                        # @added 20220428 - Feature #4536: Handle Redis failure
                                        if settings.MEMCACHE_ENABLED:
                                            success = False
                                            try:
                                                success = add_to_memcache_flux_workers_metrics_sent([flux_zero_fill_metric])
                                            except:
                                                pass
                                            if success:
                                                failed_over_to_memcache = True
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: worker :: zero fill - failed to add metric data to pickle for %s' % str(flux_zero_fill_metric))
                                    metric = None
                        last_zero_fill_to_graphite = time_now

                # @added 20210406 - Bug #4002: Change flux FLUX_ZERO_FILL_NAMESPACES to pickle
                # Reset metrics_sent
                metrics_sent = []

                if pickle_data:
                    # @modified 20201207 - Task #3864: flux - try except everything
                    try:
                        pickle_data_submitted = submit_pickle_data_to_graphite(pickle_data)
                    except Exception as e:
                        logger.error('error :: worker :: submit_pickle_data_to_graphite failed last_sent_to_graphite >= 60 - %s' % str(e))
                        pickle_data_submitted = False

                    if pickle_data_submitted:
                        pickle_data = []
                logger.info('worker :: metrics_sent_to_graphite in last 60 seconds - %s' % str(metrics_sent_to_graphite))
                skyline_metric = '%s.metrics_sent_to_graphite' % skyline_app_graphite_namespace
                if primary_worker:
                    try:
                        # @modified 20191008 - Feature #3250: Allow Skyline to send metrics to another Carbon host
                        # graphyte.send(skyline_metric, metrics_sent_to_graphite, time_now)
                        # @modified 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                        # Better handle multiple workers get count from the key
                        # send_graphite_metric(skyline_app, skyline_metric, metrics_sent_to_graphite)
                        all_metrics_sent_to_graphite = int(metrics_sent_to_graphite)
                        try:
                            all_metrics_sent_to_graphite = len(list(self.redis_conn_decoded.smembers('flux.workers.metrics_sent')))
                            self.redis_conn.delete('flux.workers.metrics_sent')
                        except Exception as e:
                            if not failed_over_to_memcache:
                                logger.error('error :: worker :: failed to get Redis set all_metrics_sent_to_graphite in last 60 seconds - %s' % e)
                            # @added 20220428 - Feature #4536: Handle Redis failure
                            if settings.MEMCACHE_ENABLED:
                                try:
                                    all_metrics_sent_to_graphite = get_memcache_key('flux', 'flux.workers.metrics_sent')
                                    if all_metrics_sent_to_graphite:
                                        all_metrics_sent_to_graphite = len(all_metrics_sent_to_graphite)
                                        logger.info('worker :: all_metrics_sent_to_graphite found in flux.workers.metrics_sent memcache key')
                                        failed_over_to_memcache = True
                                        try:
                                            success = delete_memcache_key('flux', 'flux.workers.metrics_sent')
                                        except Exception as err:
                                            logger.error('error :: worker :: failed to delete memcache key flux.workers.metrics_sent - %s' % (
                                                err))
                                    else:
                                        all_metrics_sent_to_graphite = []
                                except Exception as err:
                                    logger.error('error :: worker :: failed to get memcache key flux.workers.metrics_sent - %s' % (
                                        err))
                                    all_metrics_sent_to_graphite = int(metrics_sent_to_graphite)

                        send_graphite_metric(skyline_app, skyline_metric, all_metrics_sent_to_graphite)
                        logger.info('worker :: all_metrics_sent_to_graphite in last 60 seconds - %s' % str(all_metrics_sent_to_graphite))
                        last_sent_to_graphite = int(time())
                        metrics_sent_to_graphite = 0
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to send_graphite_metric %s with %s' % (
                            skyline_metric, str(metrics_sent_to_graphite)))

                    # @added 20220329 - Feature #4018: thunder - skyline.errors
                    # Report app up
                    try:
                        self.redis_conn.setex('flux.worker', 120, int(time()))
                    except Exception as err:
                        if not failed_over_to_memcache:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to set flux.worker Redis key - %s' % (
                                err))

                    # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                    # @modified 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                    # Send to the Graphite that collects skyline metrics not
                    # use_backfill = False
                    # timestamp_for_metrics = int(time())

                    discarded_already_received = 0
                    skyline_metric = '%s.discarded.already_received' % skyline_app_graphite_namespace
                    try:
                        discarded_already_received = len(list(self.redis_conn_decoded.smembers('flux.workers.discarded.already_received')))
                        self.redis_conn.delete('flux.workers.discarded.already_received')
                    except Exception as e:
                        if not failed_over_to_memcache:
                            logger.error('error :: worker :: failed to get Redis set flux.workers.discarded.already_received - %s' % e)
                    logger.info('worker :: discarded_already_received in last 60 seconds (in empty queue block) - %s' % str(discarded_already_received))
                    # @modified 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                    # Send to the Graphite that collects skyline metrics not
                    # via flux queue which could send to a different Graphite
                    # skyline_metric_data = [skyline_metric, discarded_already_received, timestamp_for_metrics, use_backfill]
                    # try:
                    #     self.q.put(skyline_metric_data, block=False)
                    # except Exception as e:
                    #     logger.error('error :: worker :: failed to add data %s to send to Graphite to flux.httpMetricDataQueue - %s' % (
                    #         str(metric_data), e))
                    # @added 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                    # Send to the Graphite that collects skyline metrics not
                    # via flux queue which could send to a different Graphite
                    try:
                        send_graphite_metric(skyline_app, skyline_metric, discarded_already_received)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                            skyline_metric, str(discarded_already_received), e))

                    listen_discarded_invalid_timestamp = 0
                    skyline_metric = '%s.discarded.invalid_timestamp' % listen_graphite_namespace
                    try:
                        listen_discarded_invalid_timestamp = len(list(self.redis_conn_decoded.smembers('flux.listen.discarded.invalid_timestamp')))
                        self.redis_conn.delete('flux.listen.discarded.invalid_timestamp')
                    except Exception as e:
                        if not failed_over_to_memcache:
                            logger.error('error :: worker :: failed to get Redis set flux.listen.discarded.invalid_timestamp - %s' % e)
                    logger.info('worker :: listen_discarded_invalid_timestamp in last 60 seconds (in empty queue block) - %s' % str(listen_discarded_invalid_timestamp))
                    # @modified 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                    # Send to the Graphite that collects skyline metrics not
                    # via flux queue which could send to a different Graphite
                    # skyline_metric_data = [skyline_metric, listen_discarded_invalid_timestamp, timestamp_for_metrics, use_backfill]
                    # try:
                    #     self.q.put(skyline_metric_data, block=False)
                    # except Exception as e:
                    #     logger.error('error :: worker :: failed to add data %s to send to Graphite to flux.httpMetricDataQueue - %s' % (
                    #         str(metric_data), e))
                    # @added 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                    # Send to the Graphite that collects skyline metrics not
                    # via flux queue which could send to a different Graphite
                    try:
                        send_graphite_metric(skyline_app, skyline_metric, listen_discarded_invalid_timestamp)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                            skyline_metric, str(listen_discarded_invalid_timestamp), e))

                    listen_discarded_metric_name = 0
                    skyline_metric = '%s.discarded.metric_name' % listen_graphite_namespace
                    try:
                        listen_discarded_metric_name = len(list(self.redis_conn_decoded.smembers('flux.listen.discarded.metric_name')))
                        self.redis_conn.delete('flux.listen.discarded.metric_name')
                    except Exception as e:
                        if not failed_over_to_memcache:
                            logger.error('error :: worker :: failed to get Redis set flux.listen.discarded.metric_name - %s' % e)
                    logger.info('worker :: listen_discarded_metric_name in last 60 seconds (in empty queue block) - %s' % str(listen_discarded_metric_name))
                    # @modified 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                    # Send to the Graphite that collects skyline metrics not
                    # via flux queue which could send to a different Graphite
                    # skyline_metric_data = [skyline_metric, listen_discarded_metric_name, timestamp_for_metrics, use_backfill]
                    # try:
                    #     self.q.put(skyline_metric_data, block=False)
                    # except Exception as e:
                    #     logger.error('error :: worker :: failed to add data %s to send to Graphite to flux.httpMetricDataQueue - %s' % (
                    #         str(metric_data), e))
                    # @added 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                    # Send to the Graphite that collects skyline metrics not
                    # via flux queue which could send to a different Graphite
                    try:
                        send_graphite_metric(skyline_app, skyline_metric, listen_discarded_metric_name)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                            skyline_metric, str(listen_discarded_metric_name), e))

                    listen_discarded_invalid_value = 0
                    skyline_metric = '%s.discarded.invalid_value' % listen_graphite_namespace
                    try:
                        listen_discarded_invalid_value = len(list(self.redis_conn_decoded.smembers('flux.listen.discarded.invalid_value')))
                        self.redis_conn.delete('flux.listen.discarded.invalid_value')
                    except Exception as e:
                        if not failed_over_to_memcache:
                            logger.error('error :: worker :: failed to get Redis set flux.listen.discarded.invalid_value - %s' % e)
                    logger.info('worker :: listen_discarded_invalid_value in last 60 seconds (in empty queue block) - %s' % str(listen_discarded_invalid_value))
                    # @modified 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                    # Send to the Graphite that collects skyline metrics not
                    # via flux queue which could send to a different Graphite
                    # skyline_metric_data = [skyline_metric, listen_discarded_metric_name, timestamp_for_metrics, use_backfill]
                    # skyline_metric_data = [skyline_metric, listen_discarded_invalid_value, timestamp_for_metrics, use_backfill]
                    # try:
                    #     self.q.put(skyline_metric_data, block=False)
                    # except Exception as e:
                    #     logger.error('error :: worker :: failed to add data %s to send to Graphite to flux.httpMetricDataQueue - %s' % (
                    #         str(metric_data), e))
                    # @added 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                    # Send to the Graphite that collects skyline metrics not
                    # via flux queue which could send to a different Graphite
                    try:
                        send_graphite_metric(skyline_app, skyline_metric, listen_discarded_invalid_value)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                            skyline_metric, str(listen_discarded_invalid_value), e))

                    listen_discarded_invalid_key = 0
                    skyline_metric = '%s.discarded.invalid_key' % listen_graphite_namespace
                    try:
                        listen_discarded_invalid_key = len(list(self.redis_conn_decoded.smembers('flux.listen.discarded.invalid_key')))
                        self.redis_conn.delete('flux.listen.discarded.invalid_key')
                    except Exception as e:
                        if not failed_over_to_memcache:
                            logger.error('error :: worker :: failed to get Redis set flux.listen.discarded.invalid_key - %s' % e)
                    logger.info('worker :: listen_discarded_invalid_key in last 60 seconds (in empty queue block) - %s' % str(listen_discarded_invalid_key))
                    # @modified 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                    # Send to the Graphite that collects skyline metrics not
                    # via flux queue which could send to a different Graphite
                    # skyline_metric_data = [skyline_metric, listen_discarded_invalid_key, timestamp_for_metrics, use_backfill]
                    # try:
                    #     self.q.put(skyline_metric_data, block=False)
                    # except Exception as e:
                    #     logger.error('error :: worker :: failed to add data %s to send to Graphite to flux.httpMetricDataQueue - %s' % (
                    #         str(metric_data), e))
                    # @added 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                    # Send to the Graphite that collects skyline metrics not
                    # via flux queue which could send to a different Graphite
                    try:
                        send_graphite_metric(skyline_app, skyline_metric, listen_discarded_invalid_key)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                            skyline_metric, str(listen_discarded_invalid_key), e))

                    listen_discarded_invalid_parameters = 0
                    skyline_metric = '%s.discarded.invalid_parameters' % listen_graphite_namespace
                    try:
                        listen_discarded_invalid_parameters = len(list(self.redis_conn_decoded.smembers('flux.listen.discarded.invalid_parameters')))
                        self.redis_conn.delete('flux.listen.discarded.invalid_parameters')
                    except Exception as e:
                        if not failed_over_to_memcache:
                            logger.error('error :: worker :: failed to get Redis set flux.listen.discarded.invalid_parameters - %s' % e)
                    logger.info('worker :: listen_discarded_invalid_parameters in last 60 seconds (in empty queue block) - %s' % str(listen_discarded_invalid_parameters))
                    # @modified 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                    # Send to the Graphite that collects skyline metrics not
                    # via flux queue which could send to a different Graphite
                    # skyline_metric_data = [skyline_metric, listen_discarded_invalid_parameters, timestamp_for_metrics, use_backfill]
                    # try:
                    #     self.q.put(skyline_metric_data, block=False)
                    # except Exception as e:
                    #     logger.error('error :: worker :: failed to add data %s to send to Graphite to flux.httpMetricDataQueue - %s' % (
                    #         str(metric_data), e))
                    # @added 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                    # Send to the Graphite that collects skyline metrics not
                    # via flux queue which could send to a different Graphite
                    try:
                        send_graphite_metric(skyline_app, skyline_metric, listen_discarded_invalid_parameters)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                            skyline_metric, str(listen_discarded_invalid_parameters), e))

                    listen_added_to_queue = 0
                    skyline_metric = '%s.added_to_queue' % listen_graphite_namespace
                    try:
                        listen_added_to_queue_str = None
                        listen_added_to_queue_str = self.redis_conn_decoded.getset('flux.listen.added_to_queue', 0)
                        if listen_added_to_queue_str:
                            listen_added_to_queue = int(listen_added_to_queue_str)
                    except Exception as e:
                        if not failed_over_to_memcache:
                            logger.error('error :: worker :: failed to get Redis set flux.listen.added_to_queue - %s' % e)
                        # @added 20220428 - Feature #4536: Handle Redis failure
                        if settings.MEMCACHE_ENABLED:
                            try:
                                listen_added_to_queue = get_memcache_key('flux', 'flux.listen.added_to_queue')
                                if not listen_added_to_queue:
                                    listen_added_to_queue = 0
                            except:
                                listen_added_to_queue = 0
                            if listen_added_to_queue:
                                success = False
                                try:
                                    success = delete_memcache_key(skyline_app, 'flux.listen.added_to_queue')
                                except Exception as err:
                                    logger.error('error :: worker :: failed to delete memcache flux.listen.added_to_queue - %s' % (str(err)))
                                if success:
                                    logger.info('worker :: deleted flux.listen.added_to_queue memcache key as Redis available')

                    logger.info('worker :: listen_added_to_queue in last 60 seconds (in empty queue block) - %s' % str(listen_added_to_queue))
                    # @modified 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                    # Send to the Graphite that collects skyline metrics not
                    # via flux queue which could send to a different Graphite
                    # skyline_metric_data = [skyline_metric, listen_added_to_queue, timestamp_for_metrics, use_backfill]
                    # try:
                    #     self.q.put(skyline_metric_data, block=False)
                    # except Exception as e:
                    #     logger.error('error :: worker :: failed to add data %s to send to Graphite to flux.httpMetricDataQueue - %s' % (
                    #         str(metric_data), e))
                    # @added 20210729 - Feature #4060: skyline.flux.worker.discarded metrics
                    # Send to the Graphite that collects skyline metrics not
                    # via flux queue which could send to a different Graphite
                    try:
                        send_graphite_metric(skyline_app, skyline_metric, listen_added_to_queue)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                            skyline_metric, str(listen_added_to_queue), e))

                    # @added 20220302 - Feature #4400: flux - quota
                    # If there are flux namespace quotas add a metric to track
                    # over quota count and dropped_non_numeric_metrics
                    flux_namespace_quotas = {}
                    try:
                        flux_namespace_quotas = self.redis_conn_decoded.hgetall('metrics_manager.flux.namespace_quotas')
                    except Exception as err:
                        if not failed_over_to_memcache:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to hgetall metrics_manager.flux.namespace_quotas - %s' % err)
                        last_known_value = None
                    if flux_namespace_quotas:
                        over_quota_count = 0
                        skyline_metric = '%s.over_quota_metrics_count' % listen_graphite_namespace
                        try:
                            over_quota_count = self.redis_conn_decoded.scard('flux.listen.over_quota.rejected_metrics')
                            if over_quota_count:
                                self.redis_conn_decoded.rename('flux.listen.over_quota.rejected_metrics', 'aet.flux.listen.over_quota.rejected_metrics')
                        except Exception as err:
                            if not failed_over_to_memcache:
                                logger.error('error :: worker :: failed to get Redis set flux.listen.over_quota.rejected_metrics - %s' % err)
                            # @added 20220428 - Feature #4536: Handle Redis failure
                            if settings.MEMCACHE_ENABLED:
                                try:
                                    over_quota_count_list = get_memcache_key('flux', 'flux.listen.over_quota.rejected_metrics')
                                    if over_quota_count_list:
                                        over_quota_count = len(over_quota_count)
                                        success = False
                                        try:
                                            success = delete_memcache_key(skyline_app, 'flux.listen.over_quota.rejected_metrics')
                                        except Exception as err:
                                            logger.error('error :: worker :: failed to delete memcache flux.listen.over_quota.rejected_metrics key - %s' % (str(err)))
                                        if success:
                                            logger.info('worker :: deleted flux.listen.over_quota.rejected_metrics memcache key')
                                    else:
                                        over_quota_count = 0
                                except:
                                    over_quota_count = 0

                        logger.info('worker :: listen namespaces over_quota metric count in last 60 seconds - %s' % str(over_quota_count))
                        try:
                            send_graphite_metric(skyline_app, skyline_metric, over_quota_count)
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                                skyline_metric, str(over_quota_count), err))
                    dropped_non_numeric_metrics_count = 0
                    skyline_metric = '%s.dropped_non_numeric_metrics' % listen_graphite_namespace
                    try:
                        dropped_non_numeric_metrics_count = self.redis_conn_decoded.scard('flux.listen.dropped_non_numeric_metrics')
                        if dropped_non_numeric_metrics_count:
                            self.redis_conn_decoded.rename('flux.listen.dropped_non_numeric_metrics', 'aet.flux.listen.dropped_non_numeric_metrics')
                    except Exception as err:
                        if not failed_over_to_memcache:
                            logger.error('error :: worker :: failed to get Redis set flux.listen.dropped_non_numeric_metrics - %s' % err)
                    logger.info('worker :: listen dropped_non_numeric_metrics count in last 60 seconds - %s' % str(dropped_non_numeric_metrics_count))
                    try:
                        send_graphite_metric(skyline_app, skyline_metric, dropped_non_numeric_metrics_count)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                            skyline_metric, str(dropped_non_numeric_metrics_count), err))

                    # @added 20220303 - Feature #4400: flux - quota
                    listen_added_to_aggregation_queue = 0
                    skyline_metric = '%s.added_to_aggregation_queue' % listen_graphite_namespace
                    try:
                        listen_added_to_aggregation_queue = None
                        listen_added_to_aggregation_queue_str = self.redis_conn_decoded.getset('flux.listen.added_to_aggregation_queue', 0)
                        if listen_added_to_aggregation_queue_str:
                            listen_added_to_aggregation_queue = int(listen_added_to_aggregation_queue_str)
                    except Exception as err:
                        if not failed_over_to_memcache:
                            logger.error('error :: worker :: failed to get Redis set flux.listen.added_to_aggregation_queue - %s' % err)
                    logger.info('worker :: listen added_to_aggregation_queue in last 60 seconds - %s' % str(listen_added_to_aggregation_queue))
                    try:
                        send_graphite_metric(skyline_app, skyline_metric, listen_added_to_aggregation_queue)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to send_graphite_metric %s with %s - %s' % (
                            skyline_metric, str(listen_added_to_aggregation_queue), err))

                metric_data_queue_size = 0
                try:
                    metric_data_queue_size = self.q.qsize()
                    logger.info('worker :: flux.httpMetricDataQueue queue size - %s' % str(metric_data_queue_size))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: worker :: failed to determine size of queue flux.httpMetricDataQueue')
                skyline_metric = '%s.httpMetricDataQueue.size' % skyline_app_graphite_namespace
                if primary_worker:
                    try:
                        send_graphite_metric(skyline_app, skyline_metric, metric_data_queue_size)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to send_graphite_metric %s with %s' % (
                            skyline_metric, str(metrics_sent_to_graphite)))
                # @added 20201019 - Feature #3790: flux - pickle to Graphite
                if metric_data_queue_size > 10:
                    send_to_reciever = 'pickle'
                else:
                    send_to_reciever = 'line'

                # @added 20210406 - Bug #4002: Change flux FLUX_ZERO_FILL_NAMESPACES to pickle
                # Only use pickle
                send_to_reciever = 'pickle'

                # @added 202011120 - Feature #3790: flux - pickle to Graphite
                # Debug Redis set
                metrics_data_sent_strs = []
                for item in metrics_data_sent:
                    metrics_data_sent_strs.append(str(item))
                if metrics_data_sent_strs:
                    try:
                        self.redis_conn.sadd('flux.metrics_data_sent', *set(metrics_data_sent_strs))
                        logger.info('worker :: added %s items to the flux.metrics_data_sent Redis set' % str(len(metrics_data_sent)))
                    except:
                        if not failed_over_to_memcache:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to determine size of flux.queue Redis set')
                    metrics_data_sent = []
                    try:
                        new_set = 'aet.flux.metrics_data_sent.%s' % str(self.current_pid)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to current_pid for aet.flux.metrics_data_sent Redis set name')
                        new_set = 'aet.flux.metrics_data_sent'

                    if primary_worker:
                        try:
                            self.redis_conn.rename('flux.metrics_data_sent', new_set)
                            logger.info('worker :: renamed flux.metrics_data_sent Redis set to %s' % new_set)
                        # @modified 20201128 - Feature #3820: HORIZON_SHARDS
                        # With metrics that come in at a frequency of less
                        # than 60 seconds, it is possible that this key will
                        # not exist as flux has not been sent metric data
                        # so this operation will error with no such key
                        except Exception as err:
                            traceback_str = traceback.format_exc()
                            if not failed_over_to_memcache:
                                if 'no such key' in str(err):
                                    logger.warn('warning :: worker :: failed to rename flux.metrics_data_sent to %s Redis set - flux has not recieved data in 60 seconds - %s' % (new_set, err))
                                else:
                                    logger.error(traceback_str)
                                    logger.error('error :: worker :: failed to rename flux.metrics_data_sent to %s Redis set' % new_set)
                        try:
                            self.redis_conn.expire(new_set, 600)
                        except:
                            if not failed_over_to_memcache:
                                logger.error(traceback.format_exc())
                                logger.error('error :: worker :: failed to set 600 seconds TTL on %s Redis set' % new_set)

                # @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
                if FLUX_PERSIST_QUEUE:
                    redis_set_size = 0
                    try:
                        redis_set_size = self.redis_conn.scard('flux.queue')
                    except:
                        if not failed_over_to_memcache:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to determine size of flux.queue Redis set')
                    logger.info('worker - flux.queue Redis set size %s before removal of %s items' % (
                        str(redis_set_size), str(len(remove_from_flux_queue_redis_set))))
                    if remove_from_flux_queue_redis_set:
                        try:
                            self.redis_conn.srem('flux.queue', *set(remove_from_flux_queue_redis_set))
                            remove_from_flux_queue_redis_set = []
                        except:
                            if not failed_over_to_memcache:
                                logger.error(traceback.format_exc())
                                logger.error('error :: worker :: failed to remove multiple items from flux.queue Redis set')
                        try:
                            redis_set_size = self.redis_conn.scard('flux.queue')
                        except:
                            if not failed_over_to_memcache:
                                logger.error(traceback.format_exc())
                                logger.error('error :: worker :: failed to determine size of flux.queue Redis set')
                        logger.info('worker - flux.queue Redis set size of %s after the removal of items' % (
                            str(redis_set_size)))
                        remove_from_flux_queue_redis_set = []
                # @added 20201020 - Feature #3796: FLUX_CHECK_LAST_TIMESTAMP
                # Even if flux.last Redis keys are disabled in flux they are used in
                # Vista
                vista_metrics = []
                if not FLUX_CHECK_LAST_TIMESTAMP and VISTA_ENABLED:
                    try:
                        vista_metrics = list(self.redis_conn_decoded.sscan_iter('vista.metrics', match='*'))
                    except:
                        vista_metrics = []

                # @added 20220428 - Feature #4536: Handle Redis failure
                # Swap to using a Redis hash instead of the
                # flux.last.<metric> keys
                if settings.MEMCACHE_ENABLED and primary_worker:
                    redis_last_metrics_data_dict = {}
                    try:
                        redis_last_metrics_data_dict = self.redis_conn_decoded.hgetall('flux.last.metric_data')
                    except Exception as err:
                        if not failed_over_to_memcache:
                            logger.error('error :: worker :: failed to hgetall flux.last.metric_data - %s' % (
                                err))
                    # Use the local data to update the memcache data, each
                    # worker will update the data
                    if not redis_last_metrics_data_dict:
                        memcache_flux_last_metric_data_dict = {}
                        try:
                            memcache_flux_last_metric_data_dict = get_memcache_key('flux', 'flux.last.metric_data')
                            if not memcache_flux_last_metric_data_dict:
                                memcache_flux_last_metric_data_dict = {}
                        except Exception as err:
                            logger.error('error :: worker :: failed get_memcache_key flux.last.metric_data - %s' % (
                                err))
                        for metric in list(new_memcache_flux_last_metric_data.keys()):
                            memcache_flux_last_metric_data_dict[metric] = new_memcache_flux_last_metric_data[metric]
                        redis_last_metrics_data_dict = memcache_flux_last_metric_data_dict
                    if redis_last_metrics_data_dict:
                        success = False
                        try:
                            success = set_memcache_key('flux', 'flux.last.metric_data', redis_last_metrics_data_dict)
                        except Exception as err:
                            logger.error('error :: worker :: failed to set memcache key flux.last.metric_data - %s' % (
                                err))
                        if success:
                            logger.info('worker :: set memcache key flux.last.metric_data')

                # @added 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                # Better handle multiple workers
                primary_worker_pid = 0
                try:
                    primary_worker_pid = int(self.redis_conn_decoded.get(primary_worker_key))
                    if primary_worker_pid:
                        logger.info('worker :: primary_worker_pid found in Redis key - %s' % str(primary_worker_pid))
                except:
                    primary_worker_pid = 0
                    # @added 20220428 - Feature #4536: Handle Redis failure
                    if settings.MEMCACHE_ENABLED:
                        try:
                            primary_worker_pid = get_memcache_key('flux', 'flux.worker.primary_worker_key')
                            if primary_worker_pid:
                                logger.info('worker :: primary_worker_key found in memcache key - %s' % str(primary_worker_pid))
                                failed_over_to_memcache = True
                            else:
                                primary_worker_pid = 0
                        except:
                            primary_worker_pid = 0

                if not primary_worker_pid:
                    logger.info('worker :: no primary_worker found, taking role')
                    try:
                        self.redis_conn.setex(primary_worker_key, 75, worker_pid)
                        primary_worker_pid = int(self.redis_conn_decoded.get(primary_worker_key))
                        logger.info('worker :: set self pid to primary_worker - %s' % str(primary_worker_pid))
                    except:
                        primary_worker_pid = 0
                        # @added 20220428 - Feature #4536: Handle Redis failure
                        if settings.MEMCACHE_ENABLED:
                            success = False
                            try:
                                success = set_memcache_key('flux', 'flux.worker.primary_worker_key', worker_pid, 75)
                            except Exception as err:
                                logger.error('error :: worker :: failed to set memcache primary_worker_key - %s' % (str(err)))
                            if success:
                                primary_worker_pid = worker_pid
                                logger.info('worker :: primary_worker_key set in memcache primary_worker_key - %s' % str(primary_worker_pid))

                if primary_worker_pid and primary_worker:
                    if primary_worker_pid != worker_pid:
                        logger.info('worker :: primary_worker role has been taken over by %s' % str(primary_worker_pid))
                        primary_worker = False
                if primary_worker_pid == worker_pid:
                    if not primary_worker:
                        logger.info('worker :: taking over primary_worker role')
                    primary_worker = True

                if primary_worker:
                    try:
                        self.redis_conn.setex(primary_worker_key, 75, worker_pid)
                        primary_worker_pid = int(self.redis_conn_decoded.get(primary_worker_key))
                        logger.info('worker :: set Redis primary_worker_key key to self pid to primary_worker - %s' % str(primary_worker_pid))
                    except Exception as e:
                        if not failed_over_to_memcache:
                            logger.error('error :: worker :: failed to set Redis primary_worker_key key to self pid - %s' % (str(e)))
                        # @added 20220428 - Feature #4536: Handle Redis failure
                        if settings.MEMCACHE_ENABLED:
                            success = False
                            try:
                                success = set_memcache_key('flux', 'flux.worker.primary_worker_key', worker_pid, 75)
                            except Exception as err:
                                logger.error('error :: worker :: failed to set memcache primary_worker_key - %s' % (str(err)))
                            if success:
                                logger.info('worker :: primary_worker_key set in memcache primary_worker_key - %s' % str(primary_worker_pid))
                    if main_process_pid_from_memcache:
                        success = False
                        logger.info('worker :: trying to set flux.main_process_pid in Redis as retrieved from memcache')
                        try:
                            success = self.redis_conn_decoded.set('flux.main_process_pid', main_process_pid)
                        except Exception as err:
                            if not failed_over_to_memcache:
                                logger.error('error :: worker :: failed to set Redis flux.main_process_pid key - %s' % (str(err)))
                        if success:
                            main_process_pid_from_memcache = False
                            logger.info('worker :: set flux.main_process_pid in Redis, deleting memcache key as Redis available')
                            success = False
                            try:
                                success = delete_memcache_key(skyline_app, 'flux.main_process_pid')
                            except Exception as err:
                                logger.error('error :: worker :: failed to delete memcache primary_worker_key - %s' % (str(err)))
                            if success:
                                logger.info('worker :: deleted flux.main_process_pid memcache key as Redis available')
                else:
                    last_sent_to_graphite = int(time())
                metrics_sent_to_graphite = 0
