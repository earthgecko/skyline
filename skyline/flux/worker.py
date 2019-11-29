import sys
import os.path
from os import kill
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

        logger.info('worker :: starting worker')

        last_sent_to_graphite = int(time())
        metrics_sent_to_graphite = 0

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
                logger.info('worker :: queue is empty and timed out')
                sleep(1)
            except NotImplementedError:
                pass
            except KeyboardInterrupt:
                logger.info('worker :: server has been issued a user signal to terminate - KeyboardInterrupt')
            except SystemExit:
                logger.info('worker :: server was interrupted - SystemExit')
            except Exception as e:
                logger.error('error :: worker :: %s' % (str(e)))

            if metric_data:
                try:
                    metric = str(metric_data[0])
                    value = float(metric_data[1])
                    timestamp = int(metric_data[2])
                    if LOCAL_DEBUG:
                        logger.info('worker :: debug :: queue item found - %s' % str(metric_data))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: worker :: failed to interpolate metric, value, timestamp from metric_data - %s' % str(metric_data))
                    continue

                if settings.FLUX_SEND_TO_CARBON:
                    # Best effort de-duplicate the data
                    valid_data = True
                    cache_key = 'flux.last.%s' % metric
                    last_metric_timestamp = None
                    try:
                        # @modified 20191128 - Bug #3266: py3 Redis binary objects not strings
                        #                      Branch #3262: py3
                        # redis_last_metric_data = self.redis_conn.get(cache_key)
                        redis_last_metric_data = self.redis_conn_decoded.get(cache_key)
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
                        try:
                            graphyte.send(metric, value, timestamp)
                            submittedToGraphite = True
                            logger.info('worker :: sent %s, %s, %s to Graphite' % (str(metric), str(value), str(timestamp)))
                            metrics_sent_to_graphite += 1
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to send metric data to Graphite for %s' % str(metric))
                            metric = None
                        if submittedToGraphite:
                            # Update the metric Redis flux key
                            metric_data = [timestamp, value]
                            self.redis_conn.set(cache_key, str(metric_data))
                    else:
                        logger.info('worker :: discarded %s, %s, %s as a data point for %s has already been submitted to Graphite' % (
                            str(metric), str(value), str(timestamp), str(timestamp)))
                else:
                    logger.info('worker :: settings.FLUX_SEND_TO_CARBON is set to %s, discarded %s, %s, %s' % (
                        str(settings.FLUX_SEND_TO_CARBON), str(metric), str(value), str(timestamp)))

                if settings.FLUX_SEND_TO_STATSD:
                    statsd_conn.incr(metric, value, timestamp)
                    logger.info('worker sent %s, %s, %s to statsd' % (metric, str(value), str(timestamp)))

            time_now = int(time())
            if (time_now - last_sent_to_graphite) >= 60:
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
