import sys
import os.path
from os import kill
from os import getpid
import traceback
from multiprocessing import Process
from time import sleep, time
from ast import literal_eval
import random

from logger import set_up_logging
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# @modified 20191115 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
if True:
    import settings
    import flux
    from skyline_functions import (
        get_redis_conn, get_redis_conn_decoded)
    # @added 20210718 - skyline-syslog
    from matched_or_regexed_in_list import matched_or_regexed_in_list

logger = set_up_logging(None)

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

# Wrap per metric logging in if FLUX_VERBOSE_LOGGING
try:
    FLUX_VERBOSE_LOGGING = settings.FLUX_VERBOSE_LOGGING
except:
    FLUX_VERBOSE_LOGGING = True

try:
    FLUX_PERSIST_QUEUE = settings.FLUX_PERSIST_QUEUE
except:
    FLUX_PERSIST_QUEUE = False

parent_skyline_app = 'flux'
skyline_app = 'flux'
skyline_app_graphite_namespace = 'skyline.%s%s.aggregator' % (parent_skyline_app, SERVER_METRIC_PATH)

LOCAL_DEBUG = False


class Aggregator(Process):
    """
    The aggregator processes metrics from the queue, aggregates them and sends
    them to the httpMetricDataQueue queue for worker to submit to Graphite.
    """
    def __init__(self, parent_pid):
        super(Aggregator, self).__init__()
        self.redis_conn = get_redis_conn(skyline_app)
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

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
            logger.warn('warning :: parent process is dead')
            exit(0)

    def run(self):
        """
        Called when the process intializes.
        """

        logger.info('aggregator :: starting aggregator')

        # Determine a primary aggregator
        aggregator_pid = getpid()
        main_process_pid = 0
        try:
            main_process_pid = int(self.redis_conn_decoded.get('flux.main_process_pid'))
            if main_process_pid:
                logger.info('aggregator :: main_process_pid found in Redis key - %s' % str(main_process_pid))
        except:
            main_process_pid = 0
        if not main_process_pid:
            logger.error('error :: aggregator :: no main_process_pid known, exiting')
            sys.exit(1)

        primary_aggregator_key = 'flux.primary_aggregator_pid.%s' % str(main_process_pid)
        logger.info('aggregator :: starting primary_aggregator election using primary_aggregator_key: %s' % primary_aggregator_key)
        sleep_for = random.uniform(0.1, 1.5)
        logger.info('aggregator :: starting primary_aggregator election - sleeping for %s' % str(sleep_for))
        sleep(sleep_for)
        primary_aggregator_pid = 0
        try:
            primary_aggregator_pid = int(self.redis_conn_decoded.get(primary_aggregator_key))
            if primary_aggregator_pid:
                logger.info('aggregator :: primary_aggregator_pid found in Redis key - %s' % str(primary_aggregator_pid))
        except:
            primary_aggregator_pid = 0
        if not primary_aggregator_pid:
            try:
                self.redis_conn.setex(primary_aggregator_key, 300, aggregator_pid)
                primary_aggregator_pid = int(self.redis_conn_decoded.get(primary_aggregator_key))
                logger.info('aggregator :: set self pid to primary_aggregator - %s' % str(primary_aggregator_pid))
            except:
                primary_aggregator_pid = 0
        primary_aggregator = False
        if primary_aggregator_pid == aggregator_pid:
            primary_aggregator = True
        logger.info('aggregator :: primary_aggregator_pid is set to %s, primary_aggregator: %s' % (
            str(primary_aggregator_pid), str(primary_aggregator)))

        last_flush = int(time()) - 59
        remove_from_flux_queue_redis_set = []

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
                    logger.error('aggregator :: cannot connect to redis at socket path %s' % (settings.REDIS_SOCKET_PATH))
                    sleep(2)
                    try:
                        self.redis_conn = get_redis_conn(skyline_app)
                    except Exception as e:
                        logger.error('error :: aggregator :: could not get_redis_conn - %s' % str(e))
                    try:
                        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                    except Exception as e:
                        logger.error('error :: aggregator :: could not get_redis_conn_decoded - %s' % str(e))

            try:
                time_now = int(time())
                while (time_now - last_flush) <= 59:
                    sleep(1)
                    remove_from_flux_queue_redis_set = []
                    time_now = int(time())

                primary_aggregator_pid = 0
                try:
                    primary_aggregator_pid = int(self.redis_conn_decoded.get(primary_aggregator_key))
                    if primary_aggregator_pid:
                        logger.info('aggregator :: primary_aggregator_pid found in Redis key - %s' % str(primary_aggregator_pid))
                except:
                    primary_aggregator_pid = 0
                if not primary_aggregator_pid:
                    try:
                        self.redis_conn.setex(primary_aggregator_key, 300, aggregator_pid)
                        primary_aggregator_pid = int(self.redis_conn_decoded.get(primary_aggregator_key))
                        logger.info('aggregator :: set self pid to primary_aggregator - %s' % str(primary_aggregator_pid))
                    except:
                        primary_aggregator_pid = 0
                primary_aggregator = False
                if primary_aggregator_pid == aggregator_pid:
                    primary_aggregator = True
                logger.info('aggregator :: primary_aggregator_pid is set to %s, primary_aggregator: %s' % (
                    str(primary_aggregator_pid), str(primary_aggregator)))

                flux_aggregator_queue = []
                if primary_aggregator:
                    logger.info('aggregator :: checking for data to aggregate')
                    try:
                        flux_aggregator_queue = self.redis_conn_decoded.smembers('flux.aggregator.queue')
                        logger.info('aggregator :: %s entries in flux.aggregator.queue to process' % str(len(flux_aggregator_queue)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: could not get the flux.aggregator.queue set from Redis')
                else:
                    logger.info('aggregator :: not primary, in standby to take over should the primary_aggregator fail')

                flux_aggregator_queue_items = []
                all_metrics = []
                if flux_aggregator_queue:
                    for flux_aggregator_queue_item_str in flux_aggregator_queue:
                        try:
                            flux_aggregator_queue_item = literal_eval(flux_aggregator_queue_item_str)
                            all_metrics.append(flux_aggregator_queue_item[0])
                            flux_aggregator_queue_items.append([flux_aggregator_queue_item, flux_aggregator_queue_item_str])
                            # self.redis_conn.srem('flux.aggregator.queue', flux_aggregator_queue_item_str)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to evaluate item from flux.aggregator.queue Redis set')
                metrics = list(set(all_metrics))
                for metric in metrics:
                    last_metric_flush = last_flush
                    last_metric_flush_str = None
                    try:
                        last_metric_flush_str = self.redis_conn_decoded.hget('flux.aggregate_metrics.last_flush', metric)
                        # Handle new metric without throwing an error if they do
                        # not have an entry in the hash
                        if last_metric_flush_str:
                            last_metric_flush = int(last_metric_flush_str)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed convert last_metric_flush_str value to an int from flux.aggregate_metrics.last_flush Redis hash for %s' % metric)
                    if not last_metric_flush:
                        # Handle new metric without throwing an error if they do
                        # not have an entry in the hash
                        logger.info('aggregator :: probable new metric - no last_metric_flush found in flux.aggregate_metrics.last_flush Redis hash for %s using last_flush' % metric)
                        last_metric_flush = last_flush
                    metric_aggregation_settings = {}
                    try:
                        metric_aggregation_settings_str = self.redis_conn_decoded.hget('metrics_manager.flux.aggregate_namespaces.settings', metric)
                        # @modified 20210718
                        if metric_aggregation_settings_str:
                            metric_aggregation_settings = literal_eval(metric_aggregation_settings_str)
                        else:
                            metric_aggregation_settings = {}
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to determine aggregation_settings from metrics_manager.flux.aggregate_namespaces.settings Redis hash for %s' % metric)

                    # @added 20210718
                    # Handle newly added metrics that have not been added to
                    # metrics_manager.flux.aggregate_namespaces.settings due to
                    # to the chicken or the egg problem
                    if not metric_aggregation_settings:
                        logger.info('aggregator :: probable new metric - %s not found in metrics_manager.flux.aggregate_namespaces.settings Redis hash' % metric)
                        aggregate_namespaces = list(settings.FLUX_AGGREGATE_NAMESPACES.keys())
                        pattern_match, metric_matched_by = matched_or_regexed_in_list('flux', metric, aggregate_namespaces)
                        if pattern_match:
                            matched_namespace = metric_matched_by['matched_namespace']
                            metric_aggregation_settings = settings.FLUX_AGGREGATE_NAMESPACES[matched_namespace]
                            logger.info('aggregator :: new metric - %s detemined metric_aggregation_settings from FLUX_AGGREGATE_NAMESPACES - %s' % (
                                metric, str(metric_aggregation_settings)))
                        else:
                            logger.error('error :: aggregator :: new metric - %s could not detemine metric_aggregation_settings from FLUX_AGGREGATE_NAMESPACES' % (
                                metric))

                    interval = 60
                    try:
                        interval = int(metric_aggregation_settings['interval'])
                    except:
                        # logger.error(traceback.format_exc())
                        logger.error('error :: failed to get interval from metric_aggregation_settings for %s, setting to default 60' % metric)
                        interval = 60
                    if (time_now - last_metric_flush) < interval:
                        continue
                    metric_values = []
                    for flux_aggregator_queue_item in flux_aggregator_queue_items:
                        if flux_aggregator_queue_item[0][0] != metric:
                            continue
                        # Discard any values older than the last metric flush
                        if int(flux_aggregator_queue_item[0][2]) > last_metric_flush:
                            metric_values.append(flux_aggregator_queue_item[0][1])
                        try:
                            self.redis_conn.srem('flux.aggregator.queue', flux_aggregator_queue_item[1])
                            remove_from_flux_queue_redis_set.append(flux_aggregator_queue_item[1])
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to remove item from flux.aggregator.queue Redis set - %s' % str(flux_aggregator_queue_item[1]))
                    if not metric_aggregation_settings:
                        logger.error('error :: no aggregation settings known for %s, discarding data' % metric)
                        continue
                    if metric_values:
                        methods = []
                        try:
                            methods = metric_aggregation_settings['method']
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to determine aggregation methods from metric_aggregation_settings - %s' % str(metric_aggregation_settings))
                            methods = []
                        for method in methods:
                            try:
                                metric_namespace = metric
                                if metric_aggregation_settings['method_suffix']:
                                    metric_namespace = '%s.%s' % (metric, method)
                                aggregate_value = None
                                if method == 'avg':
                                    if len(metric_values) > 1:
                                        aggregate_value = sum(metric_values) / len(metric_values)
                                    else:
                                        aggregate_value = metric_values[0]
                                if method == 'sum':
                                    aggregate_value = sum(metric_values)
                                if method == 'max':
                                    aggregate_value = max(metric_values)
                                if method == 'min':
                                    aggregate_value = min(metric_values)
                                if aggregate_value is not None:
                                    try:
                                        backfill = False
                                        metric_data = [metric_namespace, aggregate_value, (time_now - interval), backfill]
                                        flux.httpMetricDataQueue.put(metric_data, block=False)
                                        logger.info('aggregator :: added %s' % (str(metric_data)))
                                        try:
                                            self.redis_conn.hset('flux.aggregate_metrics.last_flush', metric, time_now)
                                        except:
                                            logger.error(traceback.format_exc())
                                            logger.error('error :: aggregator :: failed to set last metric flush time in Redis hash flux.aggregate_metrics.last_flush')
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: aggregator :: failed to add aggregator data to flux.httpMetricDataQueue - %s' % str(metric_data))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: aggregator :: failed to aggregate metric_values by a method for %s' % str(metric))

                last_flush = time_now

                # flux_zero_fill_metrics = list(self.redis_conn_decoded.smembers('flux.zero_fill_metrics'))

                if FLUX_PERSIST_QUEUE:
                    redis_set_size = 0
                    try:
                        redis_set_size = self.redis_conn.scard('flux.queue')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: aggregator :: failed to determine size of flux.queue Redis set')
                    logger.info('aggregator :: flux.queue Redis set size of %s before removal of %s items' % (
                        str(redis_set_size), str(len(remove_from_flux_queue_redis_set))))
                    if remove_from_flux_queue_redis_set:
                        try:
                            self.redis_conn.srem('flux.queue', *set(remove_from_flux_queue_redis_set))
                            remove_from_flux_queue_redis_set = []
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: aggregator :: failed to remove multiple items from flux.queue Redis set')
                        try:
                            redis_set_size = self.redis_conn.scard('flux.queue')
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: aggregator :: failed to determine size of flux.queue Redis set')
                        logger.info('aggregator :: flux.queue Redis set size of %s after the removal of items' % (
                            str(redis_set_size)))
                        remove_from_flux_queue_redis_set = []

                if primary_aggregator:
                    try:
                        self.redis_conn.setex(primary_aggregator_key, 300, aggregator_pid)
                        primary_aggregator_pid = int(self.redis_conn_decoded.get(primary_aggregator_key))
                        logger.info('aggregator :: set self pid to primary_aggregator - %s' % str(primary_aggregator_pid))
                        logger.info('aggregator :: set Redis primary_aggregator_key key to self pid to primary_aggregator - %s' % str(primary_aggregator_pid))
                    except Exception as e:
                        logger.error('error :: aggregator :: failed to set Redis primary_aggregator_key key to self pid - %s' % (str(e)))

            except NotImplementedError:
                pass
            except KeyboardInterrupt:
                logger.info('aggregator :: server has been issued a user signal to terminate - KeyboardInterrupt')
            except SystemExit:
                logger.info('aggregator :: server was interrupted - SystemExit')
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: aggregator :: %s' % (str(e)))
