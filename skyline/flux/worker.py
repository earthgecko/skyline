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

# from redis import StrictRedis
import graphyte
import statsd

from logger import set_up_logging
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# @modified 20191115 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
if True:
    import settings
    from skyline_functions import (
        send_graphite_metric,
        # @added 20191111 - Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
        get_redis_conn, get_redis_conn_decoded)

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
        self.redis_conn = get_redis_conn(skyline_app)
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

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
            exit(0)

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
            number_of_datapoints = len(pickle_data)
            data_points_sent = 0
            smallListOfMetricTuples = []
            tuples_added = 0
            for data in pickle_data:
                smallListOfMetricTuples.append(data)
                tuples_added += 1
                if tuples_added >= 480:
                    pickle_data_sent = pickle_data_to_graphite(smallListOfMetricTuples)
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
            if smallListOfMetricTuples:
                tuples_to_send = len(smallListOfMetricTuples)
                pickle_data_sent = pickle_data_to_graphite(smallListOfMetricTuples)
                if pickle_data_sent:
                    data_points_sent += tuples_to_send
                    logger.info('worker :: sent the last %s/%s of %s data points to Graphite via pickle' % (
                        str(tuples_to_send), str(data_points_sent),
                        str(number_of_datapoints)))
                else:
                    logger.error('error :: failed to send the last %s data points to Graphite via pickle' % (
                        str(tuples_to_send)))
                    return False
            return True

        logger.info('worker :: starting worker')

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
        metric_data_queue_size = self.q.qsize()
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
                except:
                    logger.error('worker :: cannot connect to redis at socket path %s' % (settings.REDIS_SOCKET_PATH))
                    sleep(2)
                    # @modified 20191115 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # Use get_redis_conn and get_redis_conn_decoded
                    # if settings.REDIS_PASSWORD:
                    #     self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                    # else:
                    #     self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                    self.redis_conn = get_redis_conn(skyline_app)
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

            if LOCAL_DEBUG:
                try:
                    metric_data_queue_size = self.q.qsize()
                    logger.info('worker :: debug :: flux.httpMetricDataQueue queue size - %s' % str(metric_data_queue_size))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: worker :: failed to determine size of queue flux.httpMetricDataQueue')

            metric_data = None
            try:
                # Get a metric from the queue with a 1 second timeout, each
                # metric item on the queue is a list e.g.
                # metric_data = [metricName, metricValue, metricTimestamp]
                metric_data = self.q.get(True, 1)

            except Empty:
                if pickle_data:
                    pickle_data_submitted = submit_pickle_data_to_graphite(pickle_data)
                    if pickle_data_submitted:
                        pickle_data = []
                logger.info('worker :: queue is empty and timed out')
                sleep(1)
                # @added 20201017 - Feature #3788: snab_flux_load_test
                # Send to Graphite even if worker gets no metrics
                if (int(time()) - last_sent_to_graphite) >= 60:
                    logger.info('worker :: metrics_sent_to_graphite in last 60 seconds - %s' % str(metrics_sent_to_graphite))
                    skyline_metric = '%s.metrics_sent_to_graphite' % skyline_app_graphite_namespace
                    try:
                        # @modified 20191008 - Feature #3250: Allow Skyline to send metrics to another Carbon host
                        # graphyte.send(skyline_metric, metrics_sent_to_graphite, time_now)
                        send_graphite_metric(skyline_app, skyline_metric, metrics_sent_to_graphite)
                        last_sent_to_graphite = int(time())
                        metrics_sent_to_graphite = 0
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to send_graphite_metric %s with %s' % (
                            skyline_metric, str(metrics_sent_to_graphite)))
                    metric_data_queue_size = 0
                    try:
                        metric_data_queue_size = self.q.qsize()
                        logger.info('worker :: flux.httpMetricDataQueue queue size - %s' % str(metric_data_queue_size))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to determine size of queue flux.httpMetricDataQueue')
                    skyline_metric = '%s.httpMetricDataQueue.size' % skyline_app_graphite_namespace
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
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to determine size of flux.queue Redis set')
                        metrics_data_sent = []
                        try:
                            new_set = 'aet.flux.metrics_data_sent.%s' % str(self.current_pid)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to current_pid for aet.flux.metrics_data_sent Redis set name')
                            new_set = 'aet.flux.metrics_data_sent'
                        try:
                            self.redis_conn.rename('flux.metrics_data_sent', new_set)
                            logger.info('worker :: renamed flux.metrics_data_sent Redis set to %s' % new_set)
                        # @added 20201128 - Feature #3820: HORIZON_SHARDS
                        # With metrics that come in at a frequency of less
                        # than 60 seconds, it is possible that this key will
                        # not exist as flux has not been sent metric data
                        # so this operation will error with no such key
                        except Exception as e:
                            traceback_str = traceback.format_exc()
                            if 'no such key' in e:
                                logger.warn('warning :: worker :: failed to rename flux.metrics_data_sent to %s Redis set - flux has not recieved data in 60 seconds - %s' % (new_set, e))
                            else:
                                logger.error(traceback_str)
                                logger.error('error :: worker :: failed to rename flux.metrics_data_sent to %s Redis set' % new_set)
                        try:
                            self.redis_conn.expire(new_set, 600)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to set 600 seconds TTL on %s Redis set' % new_set)

                    # @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
                    if FLUX_PERSIST_QUEUE:
                        redis_set_size = 0
                        try:
                            redis_set_size = self.redis_conn.scard('flux.queue')
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to determine size of flux.queue Redis set')
                        logger.info('worker - flux.queue Redis set size of %s before removal of %s items' % (
                            str(redis_set_size), str(len(remove_from_flux_queue_redis_set))))
                        if remove_from_flux_queue_redis_set:
                            try:
                                self.redis_conn.srem('flux.queue', *set(remove_from_flux_queue_redis_set))
                                remove_from_flux_queue_redis_set = []
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: worker :: failed to remove multiple items from flux.queue Redis set')
                            try:
                                redis_set_size = self.redis_conn.scard('flux.queue')
                            except:
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
            except NotImplementedError:
                pass
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
                    # Added backfill
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
                        try:
                            # @modified 20191128 - Bug #3266: py3 Redis binary objects not strings
                            #                      Branch #3262: py3
                            # redis_last_metric_data = self.redis_conn.get(cache_key)
                            redis_last_metric_data = self.redis_conn_decoded.get(cache_key)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to determine last_metric_timestamp from Redis key %s' % str(cache_key))
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
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: worker :: failed to determine last_metric_timestamp from Redis key %s' % str(cache_key))
                                last_metric_timestamp = False

                        if last_metric_timestamp:
                            if timestamp <= last_metric_timestamp:
                                valid_data = False
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
                                    logger.info('worker :: sent %s, %s, %s to Graphite' % (str(metric), str(value), str(timestamp)))
                                metrics_sent_to_graphite += 1
                                # @added 20200827 - Feature #3708: FLUX_ZERO_FILL_NAMESPACES
                                metrics_sent.append(metric)
                                # @added 202011120 - Feature #3790: flux - pickle to Graphite
                                # Debug Redis set
                                metrics_data_sent.append([metric, value, timestamp])
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: worker :: failed to send metric data to Graphite for %s' % str(metric))
                                metric = None
                        if send_to_reciever == 'pickle':
                            tuple_data = (metric, (int(timestamp), float(value)))
                            pickle_data.append(tuple_data)
                            submittedToGraphite = True
                            metrics_sent_to_graphite += 1
                            metrics_sent.append(metric)
                            # @added 202011120 - Feature #3790: flux - pickle to Graphite
                            # Debug Redis set
                            metrics_data_sent.append([metric, value, timestamp])

                        if submittedToGraphite:
                            # Update the metric Redis flux key
                            # @modified 20200206 - Feature #3444: Allow flux to backfill
                            # Only update the flux.last key if this is not backfill
                            # @modified 20201020 - Feature #3796: FLUX_CHECK_LAST_TIMESTAMP
                            # Use the check_flux_last_key value determined above
                            # if not backfill:
                            if check_flux_last_key:
                                metric_data = [timestamp, value]
                                self.redis_conn.set(cache_key, str(metric_data))
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
                                        logger.error('error :: failed to could not set Redis flux.filled key: %s' % e)
                    else:
                        # modified 20201016 - Feature #3788: snab_flux_load_test
                        if FLUX_VERBOSE_LOGGING:
                            logger.info('worker :: discarded %s, %s, %s as a data point for %s has already been submitted to Graphite' % (
                                str(metric), str(value), str(timestamp), str(timestamp)))
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
                    pickle_data_submitted = submit_pickle_data_to_graphite(pickle_data)
                    if pickle_data_submitted:
                        pickle_data = []

            time_now = int(time())

            # @added 20200827 - Feature #3708: FLUX_ZERO_FILL_NAMESPACES
            # Send 0 for any metric in the flux.zero_fill_metrics Redis set that
            # has not submitted data in the last 60 seconds.  The flux.last
            # Redis key is not updated for these sent 0 values so if the source
            # sends data for a timestamp in the period later (due to a lag, etc),
            # it will be valid and sent to Graphite.
            if FLUX_ZERO_FILL_NAMESPACES:
                if not last_zero_fill_to_graphite:
                    last_zero_fill_to_graphite = time_now - 60
                if (time_now - last_sent_to_graphite) >= 60:
                    try:
                        flux_zero_fill_metrics = list(self.redis_conn_decoded.smembers('flux.zero_fill_metrics'))
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to generate a list from flux.zero_fill_metrics Redis set')
                    for flux_zero_fill_metric in flux_zero_fill_metrics:
                        if flux_zero_fill_metric not in metrics_sent:
                            try:
                                graphyte.send(flux_zero_fill_metric, 0.0, time_now)
                                # modified 20201016 - Feature #3788: snab_flux_load_test
                                if FLUX_VERBOSE_LOGGING:
                                    logger.info('worker :: zero fill - sent %s, %s, %s to Graphite' % (str(flux_zero_fill_metric), str(0.0), str(time_now)))
                                metrics_sent_to_graphite += 1
                                metrics_sent.append(metric)
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: worker :: zero fill - failed to send metric data to Graphite for %s' % str(flux_zero_fill_metric))
                                metric = None
                    last_zero_fill_to_graphite = time_now
                    metrics_sent = []

            if (time_now - last_sent_to_graphite) >= 60:
                if pickle_data:
                    pickle_data_submitted = submit_pickle_data_to_graphite(pickle_data)
                    if pickle_data_submitted:
                        pickle_data = []
                logger.info('worker :: metrics_sent_to_graphite in last 60 seconds - %s' % str(metrics_sent_to_graphite))
                skyline_metric = '%s.metrics_sent_to_graphite' % skyline_app_graphite_namespace
                try:
                    # @modified 20191008 - Feature #3250: Allow Skyline to send metrics to another Carbon host
                    # graphyte.send(skyline_metric, metrics_sent_to_graphite, time_now)
                    send_graphite_metric(skyline_app, skyline_metric, metrics_sent_to_graphite)
                    last_sent_to_graphite = int(time())
                    metrics_sent_to_graphite = 0
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: worker :: failed to send_graphite_metric %s with %s' % (
                        skyline_metric, str(metrics_sent_to_graphite)))
                metric_data_queue_size = 0
                try:
                    metric_data_queue_size = self.q.qsize()
                    logger.info('worker :: flux.httpMetricDataQueue queue size - %s' % str(metric_data_queue_size))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: worker :: failed to determine size of queue flux.httpMetricDataQueue')
                skyline_metric = '%s.httpMetricDataQueue.size' % skyline_app_graphite_namespace
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
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to determine size of flux.queue Redis set')
                    metrics_data_sent = []
                    try:
                        new_set = 'aet.flux.metrics_data_sent.%s' % str(self.current_pid)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to current_pid for aet.flux.metrics_data_sent Redis set name')
                        new_set = 'aet.flux.metrics_data_sent'
                    try:
                        self.redis_conn.rename('flux.metrics_data_sent', new_set)
                        logger.info('worker :: renamed flux.metrics_data_sent Redis set to %s' % new_set)
                    # @modified 20201128 - Feature #3820: HORIZON_SHARDS
                    # With metrics that come in at a frequency of less
                    # than 60 seconds, it is possible that this key will
                    # not exist as flux has not been sent metric data
                    # so this operation will error with no such key
                    except Exception as e:
                        traceback_str = traceback.format_exc()
                        if 'no such key' in e:
                            logger.warn('warning :: worker :: failed to rename flux.metrics_data_sent to %s Redis set - flux has not recieved data in 60 seconds - %s' % (new_set, e))
                        else:
                            logger.error(traceback_str)
                            logger.error('error :: worker :: failed to rename flux.metrics_data_sent to %s Redis set' % new_set)

                    try:
                        self.redis_conn.expire(new_set, 600)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to set 600 seconds TTL on %s Redis set' % new_set)

                # @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
                if FLUX_PERSIST_QUEUE:
                    redis_set_size = 0
                    try:
                        redis_set_size = self.redis_conn.scard('flux.queue')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to determine size of flux.queue Redis set')
                    logger.info('worker - flux.queue Redis set size %s before removal of %s items' % (
                        str(redis_set_size), str(len(remove_from_flux_queue_redis_set))))
                    if remove_from_flux_queue_redis_set:
                        try:
                            self.redis_conn.srem('flux.queue', *set(remove_from_flux_queue_redis_set))
                            remove_from_flux_queue_redis_set = []
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to remove multiple items from flux.queue Redis set')
                        try:
                            redis_set_size = self.redis_conn.scard('flux.queue')
                        except:
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
