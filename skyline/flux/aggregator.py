"""
aggregator.py
"""
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
load_settings = True
if load_settings:
    import settings
    import flux
    from skyline_functions import (
        get_redis_conn, get_redis_conn_decoded,
        # @added 20220210 - Feature #4284: flux - telegraf
        # Added flux.aggregator.queue metric
        send_graphite_metric)
    # @added 20210718 - skyline-syslog
    from matched_or_regexed_in_list import matched_or_regexed_in_list
    # @added 20220429 - Feature #4536: Handle Redis failure
    if settings.MEMCACHE_ENABLED:
        from functions.memcache.get_memcache_key import get_memcache_key
        from functions.memcache.set_memcache_key import set_memcache_key
        from functions.memcache.delete_memcache_key import delete_memcache_key
        from functions.memcache.incr_memcache_key import incr_memcache_key
        from functions.memcache.append_memcache_key import append_memcache_key
    else:
        get_memcache_key = None
        set_memcache_key = None
        delete_memcache_key = None
        incr_memcache_key = None
        append_memcache_key = None


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
            sys.exit(0)

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

        # @added 20220429 - Feature #4536: Handle Redis failure
        main_process_pid_from_memcache = False
        failed_over_to_memcache = False
        if not main_process_pid:
            if settings.MEMCACHE_ENABLED:
                try:
                    main_process_pid = get_memcache_key('flux', 'flux.main_process_pid')
                    if main_process_pid:
                        logger.info('aggregator :: main_process_pid found in memcache key - %s' % str(main_process_pid))
                        main_process_pid_from_memcache = True
                        failed_over_to_memcache = True
                    else:
                        main_process_pid = 0
                except Exception as err:
                    logger.error('error :: aggregator :: could get flux.main_process_pid from memcache - %s' % str(err))
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
            # @added 20220429 - Feature #4536: Handle Redis failure
            if settings.MEMCACHE_ENABLED:
                try:
                    primary_aggregator_pid = get_memcache_key('flux', primary_aggregator_key)
                    if primary_aggregator_pid:
                        logger.info('aggregator :: primary_aggregator_pid found in memcache key - %s' % str(primary_aggregator_key))
                        failed_over_to_memcache = True
                    else:
                        primary_aggregator_pid = 0
                except:
                    primary_aggregator_pid = 0

        if not primary_aggregator_pid:
            try:
                self.redis_conn.setex(primary_aggregator_key, 75, aggregator_pid)
                primary_aggregator_pid = int(self.redis_conn_decoded.get(primary_aggregator_key))
                logger.info('aggregator :: set self pid to primary_aggregator - %s' % str(primary_aggregator_pid))
            except:
                primary_aggregator_pid = 0
                # @added 20220429 - Feature #4536: Handle Redis failure
                if settings.MEMCACHE_ENABLED:
                    success = False
                    try:
                        success = set_memcache_key('flux', primary_aggregator_key, aggregator_pid, 75)
                    except:
                        primary_aggregator_pid = 0
                    if success:
                        try:
                            primary_aggregator_pid = get_memcache_key('flux', primary_aggregator_key)
                            if primary_aggregator_pid:
                                logger.info('aggregator :: primary_aggregator_pid found in memcache key - %s' % str(primary_aggregator_pid))
                                failed_over_to_memcache = True
                            else:
                                primary_aggregator_pid = 0
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
                    # @added 20220429 - Feature #4536: Handle Redis failure
                    if redis_up and failed_over_to_memcache:
                        logger.info('aggregator :: Redis has RECOVERED')
                    if redis_up:
                        failed_over_to_memcache = False
                except Exception as err:
                    logger.error('aggregator :: cannot connect to redis - %s' % err)
                    failed_over_to_memcache = True
                    sleep(2)
                    try:
                        self.redis_conn = get_redis_conn(skyline_app)
                    except Exception as e:
                        logger.error('error :: aggregator :: could not get_redis_conn - %s' % str(e))
                    try:
                        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                    except Exception as e:
                        logger.error('error :: aggregator :: could not get_redis_conn_decoded - %s' % str(e))
                # @added 20220429 - Feature #4536: Handle Redis failure
                try:
                    redis_up = self.redis_conn.ping()
                    if redis_up and failed_over_to_memcache:
                        logger.info('aggregator :: Redis has RECOVERED')
                    if redis_up:
                        failed_over_to_memcache = False
                except:
                    failed_over_to_memcache = True
                if not settings.MEMCACHE_ENABLED:
                    failed_over_to_memcache = False
                if failed_over_to_memcache:
                    redis_up = True
                    logger.warning('warning :: aggregation :: Redis is unavailable, no further Redis errors will be logged in this run, failed_over_to_memcache: %s' % str(failed_over_to_memcache))

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
                    # @added 20220429 - Feature #4536: Handle Redis failure
                    if settings.MEMCACHE_ENABLED:
                        try:
                            primary_aggregator_pid = get_memcache_key('flux', primary_aggregator_key)
                            if primary_aggregator_pid:
                                logger.info('aggregator :: primary_aggregator_pid found in memcache key - %s' % str(primary_aggregator_key))
                                failed_over_to_memcache = True
                            else:
                                primary_aggregator_pid = 0
                        except:
                            primary_aggregator_pid = 0

                if not primary_aggregator_pid:
                    try:
                        self.redis_conn.setex(primary_aggregator_key, 75, aggregator_pid)
                        primary_aggregator_pid = int(self.redis_conn_decoded.get(primary_aggregator_key))
                        logger.info('aggregator :: set self pid to primary_aggregator - %s' % str(primary_aggregator_pid))
                    except:
                        primary_aggregator_pid = 0
                        # @added 20220429 - Feature #4536: Handle Redis failure
                        if settings.MEMCACHE_ENABLED:
                            success = False
                            try:
                                success = set_memcache_key('flux', primary_aggregator_key, aggregator_pid, 75)
                            except:
                                primary_aggregator_pid = 0
                            if success:
                                try:
                                    primary_aggregator_pid = get_memcache_key('flux', primary_aggregator_key)
                                    if primary_aggregator_pid:
                                        logger.info('aggregator :: primary_aggregator_pid found in memcache key - %s' % str(primary_aggregator_pid))
                                        failed_over_to_memcache = True
                                    else:
                                        primary_aggregator_pid = 0
                                except:
                                    primary_aggregator_pid = 0

                primary_aggregator = False
                if primary_aggregator_pid == aggregator_pid:
                    primary_aggregator = True
                logger.info('aggregator :: primary_aggregator_pid is set to %s, primary_aggregator: %s' % (
                    str(primary_aggregator_pid), str(primary_aggregator)))

                flux_aggregator_queue = []

                # @added 20220429 - Feature #4536: Handle Redis failure
                flux_aggregator_queue_from_memcache = False
                flux_aggregate_metrics_last_flush_memcache_dict = {}
                metrics_manager_flux_aggregate_namespaces_settings_memcache_dict = {}
                new_memcache_last_flush_dict = {}
                added_to_queue_count = 0
                flux_aggregator_queue_length = 0

                if primary_aggregator:
                    logger.info('aggregator :: checking for data to aggregate')
                    try:
                        flux_aggregator_queue = self.redis_conn_decoded.smembers('flux.aggregator.queue')
                        # @added 20220210 - Feature #4284: flux - telegraf
                        # Added flux.aggregator.queue metric
                        flux_aggregator_queue_length = len(flux_aggregator_queue)
                        logger.info('aggregator :: %s entries in flux.aggregator.queue to process' % str(flux_aggregator_queue_length))
                    except Exception as err:
                        # logger.error(traceback.format_exc())
                        logger.error('error :: aggregator :: could not get the flux.aggregator.queue set from Redis - %s' % err)
                        # @added 20220429 - Feature #4536: Handle Redis failure
                        if settings.MEMCACHE_ENABLED:
                            try:
                                flux_aggregator_queue = get_memcache_key('flux', 'flux.aggregator.queue')
                                if flux_aggregator_queue:
                                    failed_over_to_memcache = True
                                    try:
                                        success = delete_memcache_key('flux', 'flux.aggregator.queue')
                                    except Exception as err:
                                        logger.error('error :: aggregator :: failed to delete memcache key flux.aggregator.queue - %s' % (
                                            err))
                                    if success:
                                        logger.info('aggregator :: deleted flux.aggregator.queue memcache key')
                                    flux_aggregator_queue_length = len(flux_aggregator_queue)
                                    logger.info('aggregator :: %s entries in flux.aggregator.queue memcache key to process' % str(flux_aggregator_queue_length))
                                    failed_over_to_memcache = True
                                    flux_aggregator_queue_from_memcache = True
                                else:
                                    flux_aggregator_queue = []
                            except Exception as err:
                                logger.error('error :: aggregator :: could not get the flux.aggregator.queue memcache key - %s' % err)
                                flux_aggregator_queue = []
                else:
                    logger.info('aggregator :: not primary, in standby to take over should the primary_aggregator fail')

                flux_aggregator_queue_items = []
                all_metrics = []
                if flux_aggregator_queue:
                    for flux_aggregator_queue_item_str in flux_aggregator_queue:
                        try:
                            # @modified 20220429 - Feature #4536: Handle Redis failure
                            # The memcache data is already a list item
                            if flux_aggregator_queue_from_memcache:
                                flux_aggregator_queue_item = list(flux_aggregator_queue_item_str)
                            else:
                                flux_aggregator_queue_item = literal_eval(flux_aggregator_queue_item_str)
                            all_metrics.append(flux_aggregator_queue_item[0])
                            flux_aggregator_queue_items.append([flux_aggregator_queue_item, flux_aggregator_queue_item_str])
                            # self.redis_conn.srem('flux.aggregator.queue', flux_aggregator_queue_item_str)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: aggregator :: failed to evaluate item from flux.aggregator.queue Redis set')
                metrics = list(set(all_metrics))

                # @modified 20220222 - Feature #4284: flux - telegraf
                #                      Feature #4404: flux - external_settings - aggregation
                metrics_manager_flux_aggregate_namespaces = {}
                try:
                    metrics_manager_flux_aggregate_namespaces = self.redis_conn_decoded.hgetall('metrics_manager.flux.aggregate_namespaces')
                except Exception as err:
                    # logger.error(traceback.format_exc())
                    logger.error('error :: aggregator :: could not get hgetall metrics_manager.flux.aggregate_namespaces hash from Redis - %s' % err)
                    # @modified 20220429 - Feature #4536: Handle Redis failure
                    if settings.MEMCACHE_ENABLED:
                        try:
                            metrics_manager_flux_aggregate_namespaces = get_memcache_key('flux', 'metrics_manager.flux.aggregate_namespaces')
                            if metrics_manager_flux_aggregate_namespaces:
                                failed_over_to_memcache = True
                                logger.info('aggregator :: there are %s items in the namespace_quotas_dict memcache dict' % (
                                    str(len(metrics_manager_flux_aggregate_namespaces))))
                            else:
                                metrics_manager_flux_aggregate_namespaces = {}
                                logger.warning('warning :: aggregator :: failed to get metrics_manager.flux.aggregate_namespaces memcache dict')
                        except Exception as err:
                            logger.error('error :: aggregator :: could not get memcache dict metrics_manager.flux.aggregate_namespaces - %s' % err)
                            metrics_manager_flux_aggregate_namespaces = {}

                if metrics_manager_flux_aggregate_namespaces:
                    for flux_aggregate_namespace in list(metrics_manager_flux_aggregate_namespaces.keys()):
                        flux_aggregate_namespace_setting = {}
                        try:
                            flux_aggregate_namespace_setting = literal_eval(metrics_manager_flux_aggregate_namespaces[flux_aggregate_namespace])
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: aggregator :: failed to evaluate item for %s from metrics_manager.flux.aggregate_namespaces Redis hash - %s' % (
                                str(flux_aggregate_namespace), err))
                        if flux_aggregate_namespace_setting:
                            metrics_manager_flux_aggregate_namespaces[flux_aggregate_namespace] = flux_aggregate_namespace_setting.copy()

                # @added 20220429 - Feature #4536: Handle Redis failure
                # If the aggregator has failed over to memcache get the memcache
                # flux.aggregate_metrics.last_flush data
                if settings.MEMCACHE_ENABLED and failed_over_to_memcache:
                    try:
                        flux_aggregate_metrics_last_flush_memcache_dict = get_memcache_key('flux', 'flux.aggregate_metrics.last_flush')
                        if flux_aggregate_metrics_last_flush_memcache_dict:
                            logger.info('aggregator :: got flux.aggregate_metrics.last_flush memcache key data with %s entries' % (
                                str(len(flux_aggregate_metrics_last_flush_memcache_dict))))
                        else:
                            flux_aggregate_metrics_last_flush_memcache_dict = False
                    except:
                        flux_aggregate_metrics_last_flush_memcache_dict = False
                    try:
                        metrics_manager_flux_aggregate_namespaces_settings_memcache_dict = get_memcache_key(skyline_app, 'metrics_manager.flux.aggregate_namespaces.settings')
                        if metrics_manager_flux_aggregate_namespaces_settings_memcache_dict:
                            logger.info('aggregator :: got metrics_manager.flux.aggregate_namespaces.settings memcache key data with %s entries' % (
                                str(len(metrics_manager_flux_aggregate_namespaces_settings_memcache_dict))))
                        else:
                            metrics_manager_flux_aggregate_namespaces_settings_memcache_dict = {}
                    except:
                        metrics_manager_flux_aggregate_namespaces_settings_memcache_dict = {}

                for metric in metrics:
                    last_metric_flush = int(last_flush)
                    last_metric_flush_str = None

                    # @added 20220429 - Feature #4536: Handle Redis failure
                    # If the aggregator has failed over to memcache check the
                    # memcache data
                    if flux_aggregate_metrics_last_flush_memcache_dict:
                        try:
                            last_metric_flush = int(float(flux_aggregate_metrics_last_flush_memcache_dict[metric]))
                        except:
                            last_metric_flush = int(last_flush)
                    else:
                        last_metric_flush = None

                    # @modified 20220429 - Feature #4536: Handle Redis failure
                    # Only check is not failed over to memcache
                    if not last_metric_flush:
                        try:
                            last_metric_flush_str = self.redis_conn_decoded.hget('flux.aggregate_metrics.last_flush', metric)
                            # Handle new metric without throwing an error if they do
                            # not have an entry in the hash
                            if last_metric_flush_str:
                                last_metric_flush = int(last_metric_flush_str)
                            else:
                                last_metric_flush = None
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: aggregator :: failed convert last_metric_flush_str value to an int from flux.aggregate_metrics.last_flush Redis hash for %s' % metric)

                    if not last_metric_flush:
                        # Handle new metric without throwing an error if they do
                        # not have an entry in the hash
                        logger.info('aggregator :: probable new metric - no last_metric_flush found in flux.aggregate_metrics.last_flush Redis hash for %s using last_flush' % metric)
                        last_metric_flush = (last_flush - 900)

                    metric_aggregation_settings = {}

                    # @added 20220429 - Feature #4536: Handle Redis failure
                    # If the aggregator has failed over to memcache check the
                    # memcache data
                    if metrics_manager_flux_aggregate_namespaces_settings_memcache_dict:
                        try:
                            metric_aggregation_settings_str = metrics_manager_flux_aggregate_namespaces_settings_memcache_dict[metric]
                            if metric_aggregation_settings_str:
                                metric_aggregation_settings = literal_eval(metric_aggregation_settings_str)
                            else:
                                metric_aggregation_settings = {}
                        except Exception as err:
                            logger.error('error :: aggregator :: failed to determine aggregation_settings from the memcache metrics_manager_flux_aggregate_namespaces_settings_memcache_dict for %s - %s' % (
                                metric, err))

                    # @modified 20220429 - Feature #4536: Handle Redis failure
                    # Only check is not failed over to memcache
                    if not metric_aggregation_settings:
                        try:
                            metric_aggregation_settings_str = self.redis_conn_decoded.hget('metrics_manager.flux.aggregate_namespaces.settings', metric)
                            # @modified 20210718
                            if metric_aggregation_settings_str:
                                metric_aggregation_settings = literal_eval(metric_aggregation_settings_str)
                            else:
                                metric_aggregation_settings = {}
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: aggregator :: failed to determine aggregation_settings from metrics_manager.flux.aggregate_namespaces.settings Redis hash for %s' % metric)

                    # @added 20210718
                    # Handle newly added metrics that have not been added to
                    # metrics_manager.flux.aggregate_namespaces.settings due to
                    # to the chicken or the egg problem
                    if not metric_aggregation_settings:
                        logger.info('aggregator :: probable new metric - %s not found in metrics_manager.flux.aggregate_namespaces.settings Redis hash' % metric)

                        # @modified 20220222 - Feature #4284: flux - telegraf
                        #                      Feature #4404: flux - external_settings - aggregation
                        # Update to use the Redis hash data
                        # aggregate_namespaces = list(settings.FLUX_AGGREGATE_NAMESPACES.keys())
                        aggregate_namespaces = list(metrics_manager_flux_aggregate_namespaces.keys())

                        pattern_match, metric_matched_by = matched_or_regexed_in_list('flux', metric, aggregate_namespaces)
                        if pattern_match:
                            matched_namespace = metric_matched_by['matched_namespace']
                            # @modified 20220222 - Feature #4284: flux - telegraf
                            #                      Feature #4404: flux - external_settings - aggregation
                            # Update to use the Redis hash data
                            # metric_aggregation_settings = settings.FLUX_AGGREGATE_NAMESPACES[matched_namespace]
                            # logger.info('aggregator :: new metric - %s detemined metric_aggregation_settings from FLUX_AGGREGATE_NAMESPACES - %s' % (
                            #     metric, str(metric_aggregation_settings)))
                            metric_aggregation_settings = metrics_manager_flux_aggregate_namespaces[matched_namespace]
                            logger.info('aggregator :: new metric - %s detemined metric_aggregation_settings from metrics_manager.flux.aggregate_namespaces data - %s' % (
                                metric, str(metric_aggregation_settings)))
                            # @modified 20220429 - Feature #4536: Handle Redis failure
                            # Only update if not failed over to memcache
                            # if metric_aggregation_settings:
                            if metric_aggregation_settings and not failed_over_to_memcache:
                                try:
                                    self.redis_conn_decoded.hset('metrics_manager.flux.aggregate_namespaces.settings', metric, str(metric_aggregation_settings))
                                except Exception as err:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: aggregator :: failed to hset %s in metrics_manager.flux.aggregate_namespaces.settings Redis hash - %s' % (
                                        metric, err))
                        else:
                            # logger.warning('warning :: aggregator :: new metric - %s could not detemine metric_aggregation_settings from FLUX_AGGREGATE_NAMESPACES' % (
                            #     metric))
                            logger.warning('warning :: aggregator :: new metric - %s could not detemine metric_aggregation_settings from metrics_manager.flux.aggregate_namespaces data' % (
                                metric))

                    interval = 60
                    try:
                        interval = int(metric_aggregation_settings['interval'])
                    except:
                        # logger.error(traceback.format_exc())
                        logger.warning('warning :: aggregator :: failed to get interval from metric_aggregation_settings for %s, setting to default 60' % metric)
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

                        # @modified 20220429 - Feature #4536: Handle Redis failure
                        # Only remove if not failed over to memcache
                        if not failed_over_to_memcache:
                            try:
                                self.redis_conn.srem('flux.aggregator.queue', flux_aggregator_queue_item[1])
                                remove_from_flux_queue_redis_set.append(flux_aggregator_queue_item[1])
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: aggregator :: failed to remove item from flux.aggregator.queue Redis set - %s' % str(flux_aggregator_queue_item[1]))

                    if not metric_aggregation_settings:
                        # logger.error('error :: no aggregation settings known for %s, discarding data' % metric)
                        logger.warning('warning :: aggregator :: no aggregation settings known for %s, discarding data' % metric)
                        continue

                    if metric_values:
                        methods = []
                        try:
                            methods = metric_aggregation_settings['method']
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: aggregator :: failed to determine aggregation methods from metric_aggregation_settings - %s' % str(metric_aggregation_settings))
                            methods = []
                        for method in methods:
                            try:
                                metric_namespace = metric
                                if metric_aggregation_settings['method_suffix']:
                                    metric_namespace = '%s.%s' % (metric, method)
                                else:
                                    # @added 20220126 - Feature #4400: flux - quota
                                    # If method_suffix is not set but multiple
                                    # methods are being used, method_suffix
                                    # must be applied, otherwise the metric will
                                    # have all the method values submitted to a
                                    # single metric name.
                                    if len(methods) > 1:
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
                                        # @modified 20220429 - Feature #4536: Handle Redis failure
                                        # Align timestamp to the current minute
                                        # metric_data = [metric_namespace, aggregate_value, (time_now - interval), backfill]
                                        metric_data = [metric_namespace, aggregate_value, int(time_now // 60 * 60), backfill]
                                        flux.httpMetricDataQueue.put(metric_data, block=False)
                                        added_to_queue_count += 1
                                        if FLUX_VERBOSE_LOGGING:
                                            logger.info('aggregator :: added %s' % (str(metric_data)))
                                        # @modified 20220429 - Feature #4536: Handle Redis failure
                                        # Only set if not failed over to memcache
                                        if not failed_over_to_memcache:
                                            try:
                                                self.redis_conn.hset('flux.aggregate_metrics.last_flush', metric, time_now)
                                            except:
                                                logger.error(traceback.format_exc())
                                                logger.error('error :: aggregator :: failed to set last metric flush time in Redis hash flux.aggregate_metrics.last_flush')
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: aggregator :: failed to add aggregator data to flux.httpMetricDataQueue - %s' % str(metric_data))

                                    # @added 20220429 - Feature #4536: Handle Redis failure
                                    new_memcache_last_flush_dict[metric] = time_now
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

                # @added 20220429 - Feature #4536: Handle Redis failure
                if settings.MEMCACHE_ENABLED and new_memcache_last_flush_dict:
                    for metric in list(new_memcache_last_flush_dict.keys()):
                        flux_aggregate_metrics_last_flush_memcache_dict[metric] = new_memcache_last_flush_dict[metric]
                if flux_aggregate_metrics_last_flush_memcache_dict:
                    success = False
                    try:
                        success = set_memcache_key('flux', 'flux.aggregate_metrics.last_flush', flux_aggregate_metrics_last_flush_memcache_dict, 3600)
                    except Exception as err:
                        logger.error('error :: aggregator :: failed to set memcache flux.aggregate_metrics.last_flush - %s' % (str(err)))
                    if success:
                        logger.info('aggregator :: set flux.aggregate_metrics.last_flush memcache key')

                if primary_aggregator:
                    try:
                        self.redis_conn.setex(primary_aggregator_key, 75, aggregator_pid)
                        primary_aggregator_pid = int(self.redis_conn_decoded.get(primary_aggregator_key))
                        logger.info('aggregator :: set self pid to primary_aggregator - %s' % str(primary_aggregator_pid))
                        logger.info('aggregator :: set Redis primary_aggregator_key key to self pid to primary_aggregator - %s' % str(primary_aggregator_pid))
                    except Exception as e:
                        logger.error('error :: aggregator :: failed to set Redis primary_aggregator_key key to self pid - %s' % (str(e)))
                        # @added 20220429 - Feature #4536: Handle Redis failure
                        if settings.MEMCACHE_ENABLED:
                            success = False
                            try:
                                success = set_memcache_key('flux', primary_aggregator_key, aggregator_pid, 75)
                            except Exception as err:
                                logger.error('error :: aggregator :: failed to set memcache %s key to self pid - %s' % (
                                    primary_aggregator_key, str(err)))
                            if success:
                                logger.info('aggregator :: set memcache %s key' % primary_aggregator_key)

                    # @added 20220210 - Feature #4284: flux - telegraf
                    # Added flux.aggregator.queue metric
                    skyline_metric = '%s.queue.size' % skyline_app_graphite_namespace
                    try:
                        send_graphite_metric(skyline_app, skyline_metric, flux_aggregator_queue_length)
                        logger.info('aggregator :: %s - %s' % (skyline_metric, str(flux_aggregator_queue_length)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: aggregator :: failed to send_graphite_metric %s with %s' % (
                            skyline_metric, str(flux_aggregator_queue_length)))

                    logger.info('aggregator :: added %s metrics to worker queue' % str(added_to_queue_count))

            except NotImplementedError:
                pass
            except KeyboardInterrupt:
                logger.info('aggregator :: server has been issued a user signal to terminate - KeyboardInterrupt')
            except SystemExit:
                logger.info('aggregator :: server was interrupted - SystemExit')
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: aggregator :: %s' % (str(e)))
