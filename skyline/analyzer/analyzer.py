from __future__ import division
import logging
try:
    from Queue import Empty
except:
    from queue import Empty
# from redis import StrictRedis
from time import time, sleep, strftime, gmtime
from threading import Thread
from collections import defaultdict
# @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
# Use Redis sets in place of Manager().list to reduce memory and number of
# processes
# from multiprocessing import Process, Manager, Queue
from multiprocessing import Process, Queue
from msgpack import Unpacker, packb
import os
from os import path, kill, getpid
from math import ceil
import traceback
import operator
import re
from sys import version_info
import os.path
import resource
from ast import literal_eval

import settings
from skyline_functions import (
    send_graphite_metric, write_data_to_file, send_anomalous_metric_to, mkdir_p,
    filesafe_metricname,
    # @added 20170602 - Feature #2034: analyse_derivatives
    nonNegativeDerivative, strictly_increasing_monotonicity, in_list,
    # @added 20191030 - Bug #3266: py3 Redis binary objects not strings
    #                   Branch #3262: py3
    # Added a single functions to deal with Redis connection and the
    # charset='utf-8', decode_responses=True arguments required in py3
    get_redis_conn, get_redis_conn_decoded,
    # @added 20200413 - Feature #3486: analyzer_batch
    #                   Feature #3480: batch_processing
    is_batch_metric,
    # @added 20200506 - Feature #3532: Sort all time series
    sort_timeseries)

# @added 20200425 - Feature #3512: matched_or_regexed_in_list function
#                   Feature #3508: ionosphere.untrainable_metrics
#                   Feature #3486: analyzer_batch
from matched_or_regexed_in_list import matched_or_regexed_in_list

from alerters import trigger_alert
from algorithms import run_selected_algorithm
from algorithm_exceptions import TooShort, Stale, Boring

try:
    send_algorithm_run_metrics = settings.ENABLE_ALGORITHM_RUN_METRICS
except:
    send_algorithm_run_metrics = False
if send_algorithm_run_metrics:
    from algorithms import determine_array_median

# TODO if settings.ENABLE_CRUCIBLE: and ENABLE_PANORAMA
#    from spectrum import push_to_crucible

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
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
    DO_NOT_ALERT_ON_STALE_METRICS = settings.DO_NOT_ALERT_ON_STALE_METRICS
except:
    DO_NOT_ALERT_ON_STALE_METRICS = []

# @modified 20190524 - Feature #2882: Mirage - periodic_check
#                      Branch #3002: docker
# Moved the interpolation of the MIRAGE_PERIODIC_ variables to the top
# of the file out of spawn_process so they can be accessed in run too
# @added 20190408 - Feature #2882: Mirage - periodic_check
# Add Mirage periodic checks so that Mirage is analysing each metric at
# least once per hour.
try:
    MIRAGE_PERIODIC_CHECK = settings.MIRAGE_PERIODIC_CHECK
except:
    MIRAGE_PERIODIC_CHECK = False
try:
    MIRAGE_PERIODIC_CHECK_INTERVAL = settings.MIRAGE_PERIODIC_CHECK_INTERVAL
except:
    MIRAGE_PERIODIC_CHECK_INTERVAL = 3600
# @added 20200505 - Feature #2882: Mirage - periodic_check
# Surface this once
try:
    mirage_periodic_check_namespaces = settings.MIRAGE_PERIODIC_CHECK_NAMESPACES
except:
    mirage_periodic_check_namespaces = []


# @added 20190410 - Feature #2916: ANALYZER_ENABLED setting
try:
    ANALYZER_ENABLED = settings.ANALYZER_ENABLED
    logger.info('ANALYZER_ENABLED is set to %s' % str(ANALYZER_ENABLED))
except:
    ANALYZER_ENABLED = True
    logger.info('warning :: ANALYZER_ENABLED is not declared in settings.py, defaults to True')

try:
    ALERT_ON_STALE_METRICS = settings.ALERT_ON_STALE_METRICS
except:
    ALERT_ON_STALE_METRICS = False

# @added 20200117 - Feature #3400: Identify air gaps in the metric data
try:
    from settings import IDENTIFY_AIRGAPS
except:
    IDENTIFY_AIRGAPS = False
try:
    from settings import MAX_AIRGAP_PERIOD
except:
    MAX_AIRGAP_PERIOD = int(3600 * 6)

# @added 20200214 - Bug #3448: Repeated airgapped_metrics
#                   Feature #3400: Identify air gaps in the metric data
try:
    from settings import IDENTIFY_UNORDERED_TIMESERIES
except:
    IDENTIFY_UNORDERED_TIMESERIES = False

# @added 20200411 - Feature #3480: batch_processing
try:
    from settings import BATCH_PROCESSING
except:
    BATCH_PROCESSING = None
try:
    from settings import BATCH_PROCESSING_NAMESPACES
except:
    BATCH_PROCESSING_NAMESPACES = []
# @added 20200414 - Feature #3486: analyzer_batch
#                   Feature #3480: batch_processing
try:
    from settings import BATCH_PROCESSING_DEBUG
except:
    BATCH_PROCESSING_DEBUG = None

# @added 20200423 - Feature #3504: Handle airgaps in batch metrics
#                   Feature #3400: Identify air gaps in the metric data
CHECK_AIRGAPS = []
if IDENTIFY_AIRGAPS:
    try:
        from settings import CHECK_AIRGAPS
    except:
        CHECK_AIRGAPS = []
    if CHECK_AIRGAPS:
        from skyline_functions import is_check_airgap_metric

# @added 20200423 - Feature #3508: ionosphere.untrainable_metrics
# Determine if any metrcs have negatives values some they can be
# added to the ionosphere.untrainable_metrics Redis set
try:
    from settings import KNOWN_NEGATIVE_METRICS
except:
    KNOWN_NEGATIVE_METRICS = []

# @added 20200427 - Feature #3514: Identify inactive metrics
# Added metrics that have become inactive to the Redis set to be flagged
# as inactive
try:
    inactive_after = settings.FULL_DURATION - settings.METRICS_INACTIVE_AFTER
except:
    inactive_after = settings.FULL_DURATION - 3600

# @added 20190522 - Feature #2580: illuminance
# Disabled for now as in concept phase.  This would work better if
# the illuminance_datapoint was determined from the time series
# after min max scaling was applied to it.  That way each metric
# would have the same weight.  txBytes for instance would not have
calculate_illuminance = False

skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)

LOCAL_DEBUG = False

# @added 20191107 - Branch #3262: py3
alert_test_file = '%s/%s_alert_test.txt' % (settings.SKYLINE_TMP_DIR, skyline_app)


class Analyzer(Thread):
    """
    The Analyzer class which controls the analyzer thread and spawned processes.
    """

    def __init__(self, parent_pid):
        """
        Initialize the Analyzer

        Create the :obj:`self.exceptions_q` queue
        Create the :obj:`self.anomaly_breakdown_q` queue

        """
        super(Analyzer, self).__init__()
        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        # @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # Use get_redis_conn and get_redis_conn_decoded to use on Redis sets when the bytes
        # types need to be decoded as utf-8 to str
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
        self.current_pid = getpid()
        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
        #                      Task #3032: Debug number of Python processes and memory use
        #                      Branch #3002: docker
        # Reduce amount of Manager instances that are used as each requires a
        # copy of entire memory to be copied into each subprocess so this
        # results in a python process per Manager instance, using as much
        # memory as the parent.  OK on a server, not so much in a container.
        # Disabled all the Manager() lists below and replaced with Redis sets
        # self.anomalous_metrics = Manager().list()
        self.exceptions_q = Queue()
        self.anomaly_breakdown_q = Queue()
        # @modified 20160813 - Bug #1558: Memory leak in Analyzer
        # Not used
        # self.mirage_metrics = Manager().list()
        # @added 20160923 - Branch #922: Ionosphere
        # self.mirage_metrics = Manager().list()
        # self.ionosphere_metrics = Manager().list()
        # @added 20161119 - Branch #922: ionosphere
        #                   Task #1718: review.tsfresh
        # Send a breakdown of what metrics were sent to other apps
        # self.sent_to_mirage = Manager().list()
        # self.sent_to_crucible = Manager().list()
        # self.sent_to_panorama = Manager().list()
        # self.sent_to_ionosphere = Manager().list()
        # @added 20161229 - Feature #1830: Ionosphere alerts
        # self.all_anomalous_metrics = Manager().list()
        # @added 20170108 - Feature #1830: Ionosphere alerts
        # Adding lists of smtp_alerter_metrics and non_smtp_alerter_metrics
        # self.smtp_alerter_metrics = Manager().list()
        # self.non_smtp_alerter_metrics = Manager().list()
        # @added 20180903 - Feature #2580: illuminance
        #                   Feature #1986: flux
        # self.illuminance_datapoints = Manager().list()
        # @added 20190408 - Feature #2882: Mirage - periodic_check
        # self.mirage_periodic_check_metrics = Manager().list()
        # self.real_anomalous_metrics = Manager().list()

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            exit(0)

    # @added 20200213 - Bug #3448: Repeated airgapped_metrics
    #                   Feature #3400: Identify air gaps in the metric data
    def uniq_datapoints(self, timeseries):
        last = object()
        for item in timeseries:
            if item == last:
                continue
            yield item
            last = item

    def spawn_alerter_process(self, alert, metric, context):
        """
        Spawn a process to trigger an alert.

        This is used by smtp alerters so that matplotlib objects are cleared
        down and the alerter cannot create a memory leak in this manner and
        plt.savefig keeps the object in memory until the process terminates.
        Seeing as data is being surfaced and processed in the alert_smtp
        context, multiprocessing the alert creation and handling prevents any
        memory leaks in the parent.

        Added 20160814 relating to:

        * Bug #1558: Memory leak in Analyzer
        * Issue #21 Memory leak in Analyzer see https://github.com/earthgecko/skyline/issues/21

        Parameters as per :py:func:`skyline.analyzer.alerters.trigger_alert
        <analyzer.alerters.trigger_alert>`

        """

        trigger_alert(alert, metric, context)

    def spin_process(self, i, unique_metrics):
        """
        Assign a bunch of metrics for a process to analyze.

        Multiple get the assigned_metrics to the process from Redis.

        For each metric:

        - unpack the `raw_timeseries` for the metric.
        - Analyse each timeseries against `ALGORITHMS` to determine if it is
          anomalous.
        - If anomalous add it to the Redis set analyzer.anomalous_metrics
        - Add what algorithms triggered to the :obj:`self.anomaly_breakdown_q`
          queue
        - If :mod:`settings.ENABLE_CRUCIBLE` is ``True``:

          - Add a crucible data file with the details about the timeseries and
            anomaly.
          - Write the timeseries to a json file for crucible.

        Add keys and values to the queue so the parent process can collate for:\n
        * :py:obj:`self.anomaly_breakdown_q`
        * :py:obj:`self.exceptions_q`
        """

        spin_start = time()
        logger.info('spin_process started')
        if LOCAL_DEBUG:
            logger.info('debug :: Memory usage spin_process start: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

        # TESTING removal of p.join() from p.terminate()
        # sleep(4)

        # @modified 20160801 - Adding additional exception handling to Analyzer
        # Check the unique_metrics list is valid
        try:
            len(unique_metrics)
            # @added 20191016 - Branch #3262: py3
            if LOCAL_DEBUG:
                logger.info('debug :: unique_metrics count: %s' % str(len(unique_metrics)))
        except:
            logger.error('error :: the unique_metrics list is not valid')
            logger.info(traceback.format_exc())
            logger.info('nothing to do, no unique_metrics')
            return

        # Discover assigned metrics
        keys_per_processor = int(ceil(float(len(unique_metrics)) / float(settings.ANALYZER_PROCESSES)))
        if i == settings.ANALYZER_PROCESSES:
            assigned_max = len(unique_metrics)
        else:
            assigned_max = min(len(unique_metrics), i * keys_per_processor)
        # Fix analyzer worker metric assignment #94
        # https://github.com/etsy/skyline/pull/94 @languitar:worker-fix
        assigned_min = (i - 1) * keys_per_processor
        assigned_keys = range(assigned_min, assigned_max)

        # Compile assigned metrics
        assigned_metrics = [unique_metrics[index] for index in assigned_keys]
        if LOCAL_DEBUG:
            logger.info('debug :: Memory usage spin_process after assigned_metrics: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

        # @added 20190410 - Feature #2916: ANALYZER_ENABLED setting
        if not ANALYZER_ENABLED:
            len_assigned_metrics = len(assigned_metrics)
            logger.info('ANALYZER_ENABLED is set to %s removing the %s assigned_metrics' % (
                str(ANALYZER_ENABLED), str(len_assigned_metrics)))
            assigned_metrics = []
            del unique_metrics

        # Check if this process is unnecessary
        if len(assigned_metrics) == 0:
            return

        # Multi get series
        # @modified 20160801 - Adding additional exception handling to Analyzer
        raw_assigned_failed = True
        try:
            raw_assigned = self.redis_conn.mget(assigned_metrics)
            raw_assigned_failed = False
            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage spin_process after raw_assigned: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        except:
            logger.info(traceback.format_exc())
            logger.error('error :: failed to get assigned_metrics from Redis')

        # Make process-specific dicts
        exceptions = defaultdict(int)
        anomaly_breakdown = defaultdict(int)

        # @added 20160803 - Adding additional exception handling to Analyzer
        if raw_assigned_failed:
            logger.info('No raw_assigned set, returning')
            return

        # @added 20161119 - Branch #922: ionosphere
        #                   Task #1718: review.tsfresh
        # Determine the unique Mirage and Ionosphere metrics once, which are
        # used later to determine how Analyzer should handle/route anomalies
        try:
            # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            # mirage_unique_metrics = list(self.redis_conn.smembers('mirage.unique_metrics'))
            mirage_unique_metrics = list(self.redis_conn_decoded.smembers('mirage.unique_metrics'))
        except:
            mirage_unique_metrics = []

        # @added 20190408 - Feature #2882: Mirage - periodic_check
        # Add Mirage periodic checks so that Mirage is analysing each metric at
        # least once per hour.
        mirage_periodic_check_metric_list = []
        # @modified 20190524 - Feature #2882: Mirage - periodic_check
        #                      Branch #3002: docker
        # Moved the interpolation of the MIRAGE_PERIODIC_ variables to the top
        # of the file out of spawn_process so they can be accessed in run too
        mirage_periodic_check_interval_minutes = int(int(MIRAGE_PERIODIC_CHECK_INTERVAL) / 60)
        if mirage_unique_metrics and MIRAGE_PERIODIC_CHECK:
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # mirage_unique_metrics_count is unused so commented out
            # mirage_unique_metrics_count = len(mirage_unique_metrics)

            # Mirage periodic checks are only done on declared namespaces as to
            # process all Mirage metrics periodically would probably create a
            # substantial load on Graphite and is probably not required only key
            # metrics should be analysed by Mirage periodically.
            periodic_check_mirage_metrics = []

            if mirage_periodic_check_namespaces:
                for metric_name in mirage_unique_metrics:
                    mirage_periodic_metric = False
                    periodic_mirage_metric_matched_by = None
                    mirage_periodic_metric, periodic_mirage_metric_matched_by = matched_or_regexed_in_list(skyline_app, metric_name, mirage_periodic_check_namespaces)
                    if mirage_periodic_metric:
                        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        # test for membership should be 'not in'
                        # if not metric_name in periodic_check_mirage_metrics:
                        # if metric_name not in periodic_check_mirage_metrics:
                        periodic_check_mirage_metrics.append(metric_name)
                    del periodic_mirage_metric_matched_by

            periodic_check_mirage_metrics_count = len(periodic_check_mirage_metrics)
            logger.info(
                'there are %s known Mirage periodic metrics' % (
                    str(periodic_check_mirage_metrics_count)))
            for metric_name in periodic_check_mirage_metrics:
                try:
                    self.redis_conn.sadd('new.mirage.periodic_check.metrics.all', metric_name)
                except Exception as e:
                    logger.error('error :: could not add %s to Redis set new.mirage.periodic_check.metrics.all: %s' % (
                        metric_name, e))

            if periodic_check_mirage_metrics_count > mirage_periodic_check_interval_minutes:
                mirage_periodic_checks_per_minute = periodic_check_mirage_metrics_count / mirage_periodic_check_interval_minutes
            else:
                mirage_periodic_checks_per_minute = 1
            logger.info(
                '%s Mirage periodic checks can be added' % (
                    str(int(mirage_periodic_checks_per_minute))))

            # @added 20190524 - Feature #2882: Mirage - periodic_check
            #                   Branch #3002: docker
            # Fetch all the mirage.periodic_check. keys into a list to check
            # against rather than checking each metric name for a key below
            mirage_periodic_check_keys = []
            try:
                # @modified 20200430 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # mirage_periodic_check_keys = list(self.redis_conn.scan_iter(match='mirage.periodic_check.*'))
                mirage_periodic_check_keys = list(self.redis_conn_decoded.scan_iter(match='mirage.periodic_check.*'))
                logger.info('detemined %s mirage.periodic_check. keys from Redis scan_iter' % (
                    str(len(mirage_periodic_check_keys))))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to scan mirage.periodic_check.* from Redis')

            for metric_name in periodic_check_mirage_metrics:
                if len(mirage_periodic_check_metric_list) == int(mirage_periodic_checks_per_minute):
                    break
                base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                mirage_periodic_check_cache_key = 'mirage.periodic_check.%s' % base_name
                mirage_periodic_check_key = False
                # @modified 20190524 - Feature #2882: Mirage - periodic_check
                #                      Branch #3002: docker
                # Check the added mirage_periodic_check_keys list rather than
                # checking each and every metric name key
                # try:
                #     mirage_periodic_check_key = self.redis_conn.get(mirage_periodic_check_cache_key)
                # except Exception as e:
                #    logger.error('error :: could not query Redis for cache_key: %s' % e)
                if mirage_periodic_check_cache_key in mirage_periodic_check_keys:
                    mirage_periodic_check_key = True

                if not mirage_periodic_check_key:
                    try:
                        key_created_at = int(time())
                        self.redis_conn.setex(
                            mirage_periodic_check_cache_key,
                            MIRAGE_PERIODIC_CHECK_INTERVAL, key_created_at)
                        logger.info(
                            'created Mirage periodic_check Redis key - %s' % (mirage_periodic_check_cache_key))
                        mirage_periodic_check_metric_list.append(metric_name)
                        try:
                            self.redis_conn.sadd('new.mirage.periodic_check.metrics', metric_name)
                        except Exception as e:
                            logger.error('error :: could not add %s to Redis set new.mirage.periodic_check.metrics: %s' % (
                                metric_name, e))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error(
                            'error :: failed to create Mirage periodic_check Redis key - %s' % (mirage_periodic_check_cache_key))

            mirage_periodic_check_metric_list_count = len(mirage_periodic_check_metric_list)
            logger.info(
                '%s Mirage periodic checks were added' % (
                    str(mirage_periodic_check_metric_list_count)))

        try:
            # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            # ionosphere_unique_metrics = list(self.redis_conn.smembers('ionosphere.unique_metrics'))
            ionosphere_unique_metrics = list(self.redis_conn_decoded.smembers('ionosphere.unique_metrics'))
        except:
            ionosphere_unique_metrics = []

        # @added 20170602 - Feature #2034: analyse_derivatives
        # In order to convert monotonic, incrementing metrics to a deriative
        # metric
        try:
            # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            # derivative_metrics = list(self.redis_conn.smembers('derivative_metrics'))
            derivative_metrics = list(self.redis_conn_decoded.smembers('derivative_metrics'))
        except:
            derivative_metrics = []
        try:
            # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            # non_derivative_metrics = list(self.redis_conn.smembers('non_derivative_metrics'))
            non_derivative_metrics = list(self.redis_conn_decoded.smembers('non_derivative_metrics'))
        except:
            non_derivative_metrics = []
        # This is here to refresh the sets
        try:
            manage_derivative_metrics = self.redis_conn.get('analyzer.derivative_metrics_expiry')
        except Exception as e:
            if LOCAL_DEBUG:
                logger.error('error :: could not query Redis for analyzer.derivative_metrics_expiry key: %s' % str(e))
            manage_derivative_metrics = False

        # @added 20170901 - Bug #2154: Infrequent missing new_ Redis keys
        # If the analyzer.derivative_metrics_expiry is going to expire in the
        # next 60 seconds, just manage the derivative_metrics in the run as
        # there is an overlap some times where the key existed at the start of
        # the run but has expired by the end of the run.
        derivative_metrics_expiry_ttl = False
        if manage_derivative_metrics:
            try:
                derivative_metrics_expiry_ttl = self.redis_conn.ttl('analyzer.derivative_metrics_expiry')
                logger.info('the analyzer.derivative_metrics_expiry key ttl is %s' % str(derivative_metrics_expiry_ttl))
            except:
                logger.error('error :: could not query Redis for analyzer.derivative_metrics_expiry key: %s' % str(e))
            if derivative_metrics_expiry_ttl:
                if int(derivative_metrics_expiry_ttl) < 60:
                    logger.info('managing derivative_metrics as the analyzer.derivative_metrics_expiry key ttl is less than 60 with %s' % str(derivative_metrics_expiry_ttl))
                    manage_derivative_metrics = False
                    try:
                        self.redis_conn.delete('analyzer.derivative_metrics_expiry')
                        logger.info('deleted the Redis key analyzer.derivative_metrics_expiry')
                    except:
                        logger.error('error :: failed to delete Redis key :: analyzer.derivative_metrics_expiry')

        try:
            non_derivative_monotonic_metrics = settings.NON_DERIVATIVE_MONOTONIC_METRICS
        except:
            non_derivative_monotonic_metrics = []

        # @added 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        # Added Redis sets for Boring, TooShort and Stale
        redis_set_errors = 0

        # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
        # Use Redis set analyzer.non_smtp_alerter_metrics in place of Manager.list
        non_smtp_alerter_metrics = []
        try:
            # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            # non_smtp_alerter_metrics = list(self.redis_conn.smembers('analyzer.non_smtp_alerter_metrics'))
            non_smtp_alerter_metrics = list(self.redis_conn_decoded.smembers('analyzer.non_smtp_alerter_metrics'))
        except:
            logger.info(traceback.format_exc())
            logger.error('error :: failed to generate a list from analyzer.non_smtp_alerter_metrics Redis set')
            non_smtp_alerter_metrics = []

        # @added 20200213 - Bug #3448: Repeated airgapped_metrics
        #                   Feature #3400: Identify air gaps in the metric data
        # Backfill airgaps in Redis time series with flux back filled data
        flux_filled_keys = []

        # @added 20200214 - Bug #3448: Repeated airgapped_metrics
        #                   Feature #3400: Identify air gaps in the metric data
        # Also sort and deduplicate any metrics that were identified as being
        # unordered in the last run through algorithms.
        analyzer_unordered_timeseries = []

        # @added 20200117 - Feature #3400: Identify air gaps in the metric data
        # Get the airgapped_metrics list so that it is only got once per run and
        # passed to run_selected_algorithm if enabled and exists.
        # The implementation of this feature bumped up analyzer.run_time from:
        # from ~2.5 to 3 seconds up to between 3.0 and 4.0 seconds on 608 metrics
        # from ~5.5 to 10 seconds up to between 7.5 and 11.5 seconds on 1441 metrics
        airgapped_metrics = []

        # @added 20200501 - Feature #3400: Identify air gaps in the metric data
        # Handle airgaps filled so that once they have been submitted as filled
        # Analyzer will not identify them as airgapped again, even if there is
        # a airgap in the original airgap period
        airgapped_metrics_filled = []

        if IDENTIFY_AIRGAPS:
            try:
                airgapped_metrics = list(self.redis_conn_decoded.smembers('analyzer.airgapped_metrics'))
            except Exception as e:
                logger.error('error :: could not query Redis for analyzer.airgapped_metrics - %s' % str(e))
                airgapped_metrics = []
            logger.info('determined %s airgapped metrics' % str(len(airgapped_metrics)))

            # @added 20200501 - Feature #3400: Identify air gaps in the metric data
            # Handle airgaps filled so that once they have been submitted as filled
            # Analyzer will not identify them as airgapped again
            try:
                airgapped_metrics_filled = list(self.redis_conn_decoded.smembers('analyzer.airgapped_metrics.filled'))
            except Exception as e:
                logger.error('error :: could not remove item from analyzer.airgapped_metrics.filled Redis set - %s' % str(e))

            # @added 20200213 - Bug #3448: Repeated airgapped_metrics
            #                   Feature #3400: Identify air gaps in the metric data
            # Backfill airgaps in Redis time series with flux back filled data
            try:
                # @modified 20200430 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # flux_filled_keys = list(self.redis_conn.scan_iter(match='flux.filled.*'))
                flux_filled_keys = list(self.redis_conn_decoded.scan_iter(match='flux.filled.*'))
                logger.info('detemined %s flux.filled keys from Redis scan_iter' % (
                    str(len(flux_filled_keys))))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to scan flux.filled.* from Redis')

            # @added 20200214 - Bug #3448: Repeated airgapped_metrics
            #                   Feature #3400: Identify air gaps in the metric data
            # Also sort and deduplicate any metrics that were identified as being
            # unordered in the last run through algorithms.
            redis_set = 'analyzer.unordered_timeseries'
            try:
                analyzer_unordered_timeseries = list(self.redis_conn_decoded.smembers(redis_set))
            except Exception as e:
                logger.error('error :: could not query Redis for %s - %s' % (redis_set, str(e)))
                analyzer_unordered_timeseries = []
            logger.info('determined %s unordered metrics' % str(len(analyzer_unordered_timeseries)))
            try:
                # Delete the analyzer.unordered_timeseries Redis set so it can
                # be recreated in the next run_selected_algorithms
                self.redis_conn.delete(redis_set)
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to delete Redis key %s' % (
                    redis_set))

        # @added 20200411 - Feature #3480: batch_processing
        # This variable is for debug testing only
        enable_analyzer_batch_processing = True
        # Only send batches if analyzer_batch is reporting up
        analyzer_batch_up = None
        if BATCH_PROCESSING:
            try:
                analyzer_batch_up = int(self.redis_conn.get('analyzer_batch'))
            except:
                analyzer_batch_up = None
            if analyzer_batch_up:
                try:
                    analyzer_batch_up_check_timestamp = int(time())
                    analyzer_batch_up_for = analyzer_batch_up_check_timestamp - analyzer_batch_up
                    if analyzer_batch_up_for < 128:
                        analyzer_batch_up = True
                except:
                    analyzer_batch_up = None

        # @added 20200430 - Feature #3480: batch_processing
        # Tidy up and reduce logging, consolidate logging with counts
        batch_metric_count = 0
        batch_processing_down_processing_normally = 0
        last_timestamps_match_not_batch_processing = 0
        last_timestamps_match_no_stale_or_airgap_check_not_processing = 0
        no_new_data_but_sent_to_normal_processing_to_check_for_stale_or_airaps = 0
        no_new_data_and_not_sent_to_check_for_stale_or_airaps = 0
        sent_to_analyzer_batch_proccessing_metrics = 0
        sent_to_analyzer_batch_proccessing_metrics_but_also_processing_normally_to_identify_airgaps = 0
        batch_metric_but_processing_normally_as_penultimate_timeseries_timestamp_equals_second_last_timestamp_from_redis = 0

        # @added 20200427 - Feature #3514: Identify inactive metrics
        # Added metrics that have become inactive to the Redis set to be flagged
        # as inactive
        inactive_after = settings.FULL_DURATION - 3600
        try:
            inactive_metrics = list(self.redis_conn_decoded.smembers('analyzer.inactive_metrics'))
        except:
            inactive_metrics = []

        # Distill timeseries strings into lists
        for i, metric_name in enumerate(assigned_metrics):
            self.check_if_parent_is_alive()

            # @added 20191016 - Branch #3262: py3
            if LOCAL_DEBUG:
                logger.info('debug :: checking %s' % str(metric_name))

            try:
                raw_series = raw_assigned[i]
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)
            except:
                timeseries = []

            # @added 20200506 - Feature #3532: Sort all time series
            # To ensure that there are no unordered timestamps in the time
            # series which are artefacts of the collector or carbon-relay, sort
            # all time series by timestamp before analysis.
            original_timeseries = timeseries
            if original_timeseries:
                timeseries = sort_timeseries(original_timeseries)
                del original_timeseries

            base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)

            # @added 20200427 - Feature #3514: Identify inactive metrics
            # Added metrics that have become inactive to the Redis set to be flagged
            # as inactive
            inactive_metric = False
            if timeseries:
                try:
                    last_timeseries_timestamp = int(timeseries[-1][0])
                    spin_start
                    if last_timeseries_timestamp < int(spin_start - inactive_after):
                        inactive_metric = True
                except:
                    inactive_metric = False
            add_inactive_metric = False
            if inactive_metric:
                if base_name not in inactive_metrics:
                    add_inactive_metric = True
            if add_inactive_metric:
                # HERE if it is added the set, after Panorama has processed and
                # removed from the set, how does analyzer identify to not send
                # again?  Set a key that expires one hour later?
                # HOW does the metric become identified as active again?
                inactive_metrics = list(self.redis_conn_decoded.smembers('analyzer.inactive_metrics'))

            # @added 20170602 - Feature #2034: analyse_derivatives
            # In order to convert monotonic, incrementing metrics to a deriative
            # metric
            known_derivative_metric = False
            unknown_deriv_status = True
            if metric_name in non_derivative_metrics:
                unknown_deriv_status = False
            if unknown_deriv_status:
                if metric_name in derivative_metrics:
                    known_derivative_metric = True
                    unknown_deriv_status = False
            # This is here to refresh the sets
            if not manage_derivative_metrics:
                unknown_deriv_status = True

            # @added 20200213 - Bug #3448: Repeated airgapped_metrics
            #                   Feature #3400: Identify air gaps in the metric data
            # When flux backfills a metric airgap and sends it to Graphite,
            # carbon-relay sends it to Horizon which results in it being written
            # to the Redis metric key time series out of order.  For metrics
            # that have been backfilled by Flux in the last FULL_DURATION
            # period, Analyzer needs to sort and deduplicate the Redis time
            # series data for that metric as long as a flux.filled key exists
            # for that metric and replace the metric key data with sorted data.
            sort_data = False
            update_new_data = False
            # @added 20200501 -
            # Added get_updated_redis_timeseries
            get_updated_redis_timeseries = False
            if IDENTIFY_AIRGAPS and flux_filled_keys:
                metric_flux_filled_key = 'flux.filled.%s' % str(base_name)
                sorted_and_deduplicated_timeseries = None
                populated_redis_key = False
                new_metric_name_key = 'analyzer.sorted.deduped.%s' % str(metric_name)
                sort_data = False
                if metric_flux_filled_key in flux_filled_keys:
                    sort_data = True
                    logger.info('sorting and deduplicating data because a Flux Redis key exists - %s' % str(metric_flux_filled_key))
            if IDENTIFY_UNORDERED_TIMESERIES and analyzer_unordered_timeseries:
                if metric_name in analyzer_unordered_timeseries:
                    sort_data = True
                    logger.info('sorting and deduplicating data because %s is in the analyzer.unordered_timeseries' % str(metric_name))
            sorted_and_deduplicated_timeseries = None
            if sort_data:
                try:
                    unsorted_length = len(timeseries)
                    sorted_timeseries = sorted(timeseries, key=lambda x: x[0])
                    sorted_and_deduplicated_timeseries = list(self.uniq_datapoints(sorted(sorted_timeseries, reverse=False)))
                    sorted_and_deduped_length = len(sorted_and_deduplicated_timeseries)
                    logger.info('timeseries length is %s, sorted and deduplicated timeseries length is %s' % (
                        str(unsorted_length), str(sorted_and_deduped_length)))
                    timeseries = sorted_and_deduplicated_timeseries
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: failed to sort and deduplicate flux filled timeseries for %s' % str(metric_name))
                if not sorted_and_deduplicated_timeseries:
                    logger.error('error :: failed to sort and deduplicate flux filled timeseries for %s' % str(metric_name))
            # Recreate the Redis key sorted and deduplicated and feed to
            # Redis ala Horizon worker method more or less
            if sorted_and_deduplicated_timeseries:
                logger.info('populating Redis key %s with sorted and deduplicated data' % str(new_metric_name_key))
                try:
                    new_metric_name_key = 'analyzer.sorted.deduped.%s' % str(metric_name)
                    for datapoint in sorted_and_deduplicated_timeseries:
                        metric = (new_metric_name_key, (datapoint[0], datapoint[1]))
                        self.redis_conn.append(str(new_metric_name_key), packb(metric[1]))
                    populated_redis_key = True
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: failed to populate Redis key %s with sorted and deduplicated data' % str(metric_name))
                if not populated_redis_key:
                    try:
                        self.redis_conn.delete(new_metric_name_key)
                    except:
                        pass
                test_timeseries = None
                if populated_redis_key:
                    logger.info('getting current Redis key %s data to compare with sorted and deduplicated data' % str(metric_name))
                    try:
                        test_raw_series = self.redis_conn.get(metric_name)
                        unpacker = Unpacker(use_list=False)
                        unpacker.feed(test_raw_series)
                        test_timeseries = list(unpacker)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to get Redis key %s to test against sorted and deduplicated data' % str(metric_name))
                    # @added 20200506 - Feature #3532: Sort all time series
                    # To ensure that there are no unordered timestamps in the time
                    # series which are artefacts of the collector or carbon-relay, sort
                    # all time series by timestamp before analysis.
                    original_test_timeseries = test_timeseries
                    if original_test_timeseries:
                        test_timeseries = sort_timeseries(original_test_timeseries)
                        del original_test_timeseries

                verified_existing_key_data = False
                if test_timeseries:
                    logger.info('comparing most recent current timestamp with sorted and deduplicated data')
                    try:
                        logger.info('sorting and deduplicating current data')
                        try:
                            sorted_test_timeseries = sorted(test_timeseries, key=lambda x: x[0])
                            sorted_and_deduplicated_test_timeseries = list(self.uniq_datapoints(sorted(sorted_test_timeseries, reverse=False)))
                            test_timeseries = sorted_and_deduplicated_test_timeseries
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: failed to sort and deduplicate current data for %s' % str(metric_name))
                        if test_timeseries:
                            try:
                                last_current_ts = int(test_timeseries[-1][0])
                                last_new_sort_ts = int(sorted_and_deduplicated_timeseries[-1][0])
                                if last_current_ts == last_new_sort_ts:
                                    verified_existing_key_data = True
                                    logger.info('most recent current timestamp with sorted and deduplicated data match')
                                else:
                                    logger.info('the sorted and deduplicated last timestamp (%s) does not match the current data last timestamp (%s) not replaced Redis key %s' % (
                                        str(last_new_sort_ts), str(last_current_ts), str(metric_name)))
                            except:
                                logger.info(traceback.format_exc())
                                logger.error('error :: failed to determine for timestamps match')
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to comparing most recent current timestamp with sorted and deduplicated data for %s' % str(metric_name))
                if not verified_existing_key_data:
                    try:
                        self.redis_conn.delete(new_metric_name_key)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to delete Redis key %s' % (
                            new_metric_name_key))
                original_key_renamed = False
                metric_key_to_delete = 'analyzer.flux.rename.%s' % str(metric_name)
                if verified_existing_key_data:
                    try:
                        logger.info('renaming key %s to %s' % (metric_name, metric_key_to_delete))
                        self.redis_conn.rename(metric_name, metric_key_to_delete)
                        original_key_renamed = True
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to rename Redis key %s to %s' % (
                            str(metric_name), metric_key_to_delete))
                        try:
                            logger.info('deleting key %s' % (new_metric_name_key))
                            self.redis_conn.delete(new_metric_name_key)
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: failed to delete Redis key %s' % (
                                new_metric_name_key))
                new_key_renamed = False
                if original_key_renamed:
                    try:
                        logger.info('renaming key %s to %s' % (new_metric_name_key, metric_name))
                        self.redis_conn.rename(new_metric_name_key, metric_name)
                        new_key_renamed = True
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to rename Redis key %s to %s' % (
                            str(new_metric_name_key), metric_name))
                        try:
                            logger.info('reverting by renaming key %s to %s' % (metric_key_to_delete, metric_name))
                            self.redis_conn.rename(metric_key_to_delete, metric_name)
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: failed to rename Redis key %s to %s' % (
                                str(metric_key_to_delete), metric_name))
                        try:
                            self.redis_conn.delete(new_metric_name_key)
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: failed to delete Redis key %s' % (
                                new_metric_name_key))
                if new_key_renamed:
                    test_timeseries = None
                    logger.info('determining if any new data was added to the metric Redis key during the rename')
                    try:
                        test_raw_series = self.redis_conn.get(metric_key_to_delete)
                        unpacker = Unpacker(use_list=False)
                        unpacker.feed(test_raw_series)
                        test_timeseries = list(unpacker)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to get Redis key %s to test against sorted and deduplicated data' % str(metric_key_to_delete))
                    update_new_data = False
                    if test_timeseries:

                        # @added 20200506 - Feature #3532: Sort all time series
                        # To ensure that there are no unordered timestamps in the time
                        # series which are artefacts of the collector or carbon-relay, sort
                        # all time series by timestamp before analysis.
                        original_test_timeseries = test_timeseries
                        if original_test_timeseries:
                            test_timeseries = sort_timeseries(original_test_timeseries)
                            del original_test_timeseries

                        # @added 20200501 -
                        get_updated_redis_timeseries = True

                        logger.info('comparing most recent current timestamp from the renamed key with new sorted and deduplicated data')
                        try:
                            last_current_ts = int(test_timeseries[-1][0])
                            if last_current_ts == last_new_sort_ts:
                                logger.info('the timestamps match, OK')
                            else:
                                logger.info('the sorted and deduplicated last timestamp (%s) does not match the last timestamp (%s) from the renamed Redis key' % (
                                    str(last_new_sort_ts), str(last_current_ts)))
                                update_new_data = True
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: failed to determine if timestamps match with the renamed key')
                    if update_new_data:
                        # Add any data that was added to the renamed key to the
                        # new sorted and deduplicated key, even if this unorders
                        # it again.  Better to reorder on the next run than lose
                        # data points
                        new_datapoints = []
                        try:
                            # slice reverse the list with [::-1]
                            for datapoint in test_timeseries[::-1]:
                                if int(datapoint[0]) != last_new_sort_ts:
                                    new_datapoints.append(datapoint)
                                else:
                                    break
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: failed to determine datapoints to append to the new key')
                        if new_datapoints:
                            try:
                                for datapoint in new_datapoints:
                                    metric = (metric_name, (datapoint[0], datapoint[1]))
                                    self.redis_conn.append(metric_name, packb(metric[1]))
                                # @added 20200501 -
                                get_updated_redis_timeseries = True
                            except:
                                logger.info(traceback.format_exc())
                                logger.error('error :: failed to populate Redis key %s with new data' % str(metric_name))
                    try:
                        logger.info('deleting key %s' % (metric_key_to_delete))
                        self.redis_conn.delete(metric_key_to_delete)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to delete Redis key %s' % (
                            metric_key_to_delete))
                    try:
                        logger.info('Redis time series key data sorted and ordered with Flux additions, deleting key %s' % (metric_flux_filled_key))
                        self.redis_conn.delete(metric_flux_filled_key)
                        get_updated_redis_timeseries = True
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to delete Redis key %s' % (
                            metric_key_to_delete))

            if get_updated_redis_timeseries:
                updated_timeseries = []
                try:
                    raw_series = self.redis_conn.get(metric_name)
                    unpacker = Unpacker(use_list=False)
                    unpacker.feed(raw_series)
                    updated_timeseries = list(unpacker)
                except:
                    updated_timeseries = []
                if updated_timeseries:
                    logger.info('Using updated Redis time series for %s' % (metric_name))

                    # @added 20200506 - Feature #3532: Sort all time series
                    # To ensure that there are no unordered timestamps in the time
                    # series which are artefacts of the collector or carbon-relay, sort
                    # all time series by timestamp before analysis.
                    original_updated_timeseries = updated_timeseries
                    if original_updated_timeseries:
                        updated_timeseries = sort_timeseries(original_updated_timeseries)
                        del original_updated_timeseries

                    timeseries = updated_timeseries

            # @added 20200411 - Feature #3480: batch_processing
            batch_metric = False
            check_for_airgaps_only = False
            if BATCH_PROCESSING:
                # @modified 20200423 - Feature #3504: Handle airgaps in batch metrics
                #                      Feature #3480: batch_processing
                #                      Feature #3486: analyzer_batch
                # Rather set the default to False which is the default
                # configuration for all metrics
                # batch_metric = True
                batch_metric = False
                try:
                    batch_metric = is_batch_metric(skyline_app, base_name)
                except:
                    # batch_metric = True
                    batch_metric = False

                if batch_metric:
                    batch_metric_count += 1
                    # @added 20200423 - Feature #3504: Handle airgaps in batch metrics
                    #                   Feature #3480: batch_processing
                    #                   Feature #3486: analyzer_batch
                    #                   Feature #3400: Identify air gaps in the metric data
                    # Check to see if this is a batch processing metric that has been sent to
                    # analyzer_batch for processing but needs to be sent through Analyzer to
                    # check for airgaps only and not be run through algorithms.  If so set a
                    # Redis key for the metric (rather than add to set), so the
                    # analyzer/algorithms.py does not do not have to add get_redis_conn_decoded
                    # and do not issue the normal "continue"
                    check_for_airgap_only = False
                    check_metric_for_airgaps = False
                    if IDENTIFY_AIRGAPS:
                        check_metric_for_airgaps = True
                        if CHECK_AIRGAPS:
                            check_metric_for_airgaps = False
                            try:
                                check_metric_for_airgaps = is_check_airgap_metric(metric_name)
                            except:
                                check_metric_for_airgaps = True
                                try:
                                    logger.error('failed to determine if %s is an airgap metric' % (
                                        str(metric_name), traceback.format_exc()))
                                except:
                                    logger.error('failed to failure regarding deleting the check_airgap_only_key Redis key')
                        if check_metric_for_airgaps:
                            check_airgap_only_key = 'analyzer.check_airgap_only.%s' % metric_name
                            try:
                                key_value = int(time())
                                self.redis_conn.setex(
                                    check_airgap_only_key, settings.FULL_DURATION, key_value)
                                check_for_airgap_only = True
                            except Exception as e:
                                logger.error('error :: could not set Redis analyzer.check_airgap_only key: %s' % e)

                if batch_metric:
                    last_metric_timestamp = None
                    if batch_metric:
                        redis_set = 'analyzer.batch_processing_metrics_current'
                        try:
                            self.redis_conn.sadd(redis_set, base_name)
                            if BATCH_PROCESSING_DEBUG:
                                logger.info('batch processing - added metric %s to Redis set %s' % (
                                    base_name, redis_set))
                        except:
                            if BATCH_PROCESSING_DEBUG:
                                logger.error('error :: batch processing - failed to add metric %s to Redis set %s' % (
                                    base_name, redis_set))
                            pass
                        last_metric_timestamp_key = 'last_timestamp.%s' % base_name
                        try:
                            last_metric_timestamp = int(self.redis_conn.get(last_metric_timestamp_key))
                            if BATCH_PROCESSING_DEBUG:
                                logger.info('batch processing - Redis key %s - %s' % (
                                    last_metric_timestamp_key, str(last_metric_timestamp)))
                        except:
                            last_metric_timestamp = None
                            if BATCH_PROCESSING_DEBUG:
                                logger.info('batch processing - no last_metric_timestamp for %s was found' % (
                                    base_name))

                    if not analyzer_batch_up:
                        batch_processing_down_processing_normally += 1
                        if BATCH_PROCESSING_DEBUG:
                            logger.error('error :: batch processing - analyzer_batch is not reporting up, not batch processing, processing normally')

                    # Only run batch processing if analyzer_batch is reporting up
                    if analyzer_batch_up:
                        # If there is no known last_timestamp, this is the first
                        # processing of a metric, just begin processing as normal
                        # and do not send to analyzer_batch.
                        last_timeseries_timestamp = None
                        penultimate_timeseries_timestamp = None
                        if last_metric_timestamp:
                            try:
                                last_timeseries_timestamp = int(timeseries[-1][0])
                                penultimate_timeseries_timestamp = int(timeseries[-2][0])
                                if BATCH_PROCESSING_DEBUG:
                                    logger.info('batch processing - last_timeseries_timestamp from current Redis time series data for %s was %s' % (
                                        base_name, str(last_timeseries_timestamp)))
                                    logger.info('batch processing - penultimate_timeseries_timestamp from current Redis time series data for %s was %s' % (
                                        base_name, str(penultimate_timeseries_timestamp)))
                            except:
                                last_timeseries_timestamp = None
                                penultimate_timeseries_timestamp = None
                                if BATCH_PROCESSING_DEBUG:
                                    logger.error('error :: batch processing - penultimate_timeseries_timestamp from current Redis time series data for metric %s was not determined' % (
                                        base_name))
                        if last_timeseries_timestamp:
                            if last_timeseries_timestamp == last_metric_timestamp:
                                # If the last processed timestamp is the same as the
                                # the last timestamp in the current Redis time
                                # series data, there is no need to check the
                                # penultimate_timeseries_timestamp
                                penultimate_timeseries_timestamp = None
                                last_timestamps_match_not_batch_processing += 1
                                if BATCH_PROCESSING_DEBUG:
                                    logger.info('batch processing - the last_timeseries_timestamp and last timestamp from current Redis time series data for %s match, no need to batch process, OK' % (
                                        base_name))
                                # added 20200414 - Feature #3486: analyzer_batch
                                #                  Feature #3480: batch_processing
                                # Only continue if Analyzer is NOT alerting
                                # on stale metrics, otherwise Analyzer will
                                # not identify batch metrics as stale if
                                # they go stale.  However if they are in
                                # DO_NOT_ALERT_ON_STALE_METRICS then it
                                # should continue so as to save the overhead
                                # of reprocessing a metric until it goes
                                # stale.
                                # @modified 20200423 - Feature #3504: Handle airgaps in batch metrics
                                # if not ALERT_ON_STALE_METRICS:
                                if not ALERT_ON_STALE_METRICS and not check_for_airgap_only:
                                    # @modified 20200430 - Feature #3480: batch_processing
                                    # Tidy up and reduce logging
                                    last_timestamps_match_no_stale_or_airgap_check_not_processing += 1
                                    if BATCH_PROCESSING_DEBUG:
                                        logger.info('batch processing - the last_timeseries_timestamp and last timestamp from current Redis time series data for %s match, no need to process at all skipping, OK' % (
                                            base_name))
                                    continue
                                else:
                                    if DO_NOT_ALERT_ON_STALE_METRICS:
                                        metric_namespace_elements = metric_name.split('.')
                                        process_metric = True
                                        for do_not_alert_namespace in DO_NOT_ALERT_ON_STALE_METRICS:
                                            if do_not_alert_namespace in metric_name:
                                                process_metric = False
                                                break
                                            do_not_alert_namespace_namespace_elements = do_not_alert_namespace.split('.')
                                            elements_matched = set(metric_namespace_elements) & set(do_not_alert_namespace_namespace_elements)
                                            if len(elements_matched) == len(do_not_alert_namespace_namespace_elements):
                                                process_metric = False
                                                break
                                        # @modified 20200423 - Feature #3504: Handle airgaps in batch metrics
                                        # if not process_metric:
                                        if not process_metric and not check_for_airgap_only:
                                            # @modified 20200430 - Feature #3480: batch_processing
                                            # Tidy up and reduce logging
                                            no_new_data_and_not_sent_to_check_for_stale_or_airaps += 1
                                            if BATCH_PROCESSING_DEBUG:
                                                logger.info('batch processing - %s is a DO_NOT_ALERT_ON_STALE_METRICS namespace and is not set to check for airgaps, no need to process at all as does not need to be identified as stale, skipping normal processing.' % (
                                                    base_name))
                                            continue
                                    # @modified 20200430 - Feature #3480: batch_processing
                                    # Tidy up and reduce logging
                                    no_new_data_but_sent_to_normal_processing_to_check_for_stale_or_airaps += 1
                                    check_for_airgaps_only = True
                                    if BATCH_PROCESSING_DEBUG:
                                        logger.info('batch processing - processing %s normally through Analyzer, even though this data has already been analyzed however Analyzer can then identify as stale, if it goes stale or airgaps.' % (
                                            base_name))

                        if penultimate_timeseries_timestamp:
                            if penultimate_timeseries_timestamp != last_metric_timestamp:
                                # Add to analyzer.batch to check
                                added_to_analyzer_batch_proccessing_metrics = None
                                data = [metric_name, last_metric_timestamp]
                                # redis_set = 'crucible.firings.batch_processing_metrics'
                                redis_set = 'analyzer.batch'
                                try:
                                    self.redis_conn.sadd(redis_set, str(data))
                                    added_to_analyzer_batch_proccessing_metrics = True
                                    sent_to_analyzer_batch_proccessing_metrics += 1
                                    if BATCH_PROCESSING_DEBUG:
                                        logger.info('batch processing - the penultimate_timeseries_timestamp is not the same as the last_metric_timestamp added to Redis set %s - %s' % (
                                            redis_set, str(data)))
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: batch processing - failed to add %s to %s Redis set' % (
                                        str(data), redis_set))
                                    # @modified 20200423 - Feature #3504: Handle airgaps in batch metrics
                                    #                      Feature #3480: batch_processing
                                    #                      Feature #3486: analyzer_batch
                                    # If this fails it should not set batch_metric
                                    # batch_metric = False
                                    added_to_analyzer_batch_proccessing_metrics = False
                                if added_to_analyzer_batch_proccessing_metrics:
                                    if BATCH_PROCESSING_DEBUG:
                                        logger.info('batch processing - add %s to %s Redis set to be batch processed' % (
                                            str(data), redis_set))

                                    # @modified 20200423 - Feature #3504: Handle airgaps in batch metrics
                                    # Wrapped in if not check_metric_for_airgaps
                                    if not check_metric_for_airgaps:
                                        if enable_analyzer_batch_processing:
                                            continue
                                            if enable_analyzer_batch_processing:
                                                if BATCH_PROCESSING_DEBUG:
                                                    logger.info('batch processing - this log line should not have been reached, a continue was issued')
                                    else:
                                        # @added 20200423 - Feature #3504: Handle airgaps in batch metrics
                                        # @modified 20200430 - Feature #3480: batch_processing
                                        # Tidy up and reduce logging
                                        sent_to_analyzer_batch_proccessing_metrics_but_also_processing_normally_to_identify_airgaps += 1
                                        check_for_airgaps_only = True
                                        if BATCH_PROCESSING_DEBUG:
                                            logger.info('batch processing - %s sent to analyzer_batch but also sending through aanalyzer to check_metric_for_airgaps' % (
                                                base_name))
                            else:
                                # @modified 20200430 - Feature #3480: batch_processing
                                # Tidy up and reduce logging
                                batch_metric_but_processing_normally_as_penultimate_timeseries_timestamp_equals_second_last_timestamp_from_redis += 1
                                if BATCH_PROCESSING_DEBUG:
                                    logger.info('batch processing - the penultimate_timeseries_timestamp is the same as second last timestamp from current Redis time series data for %s, not batch processing, continuing as normal' % (
                                        base_name))

            # @added 20170617 - Bug #2050: analyse_derivatives - change in monotonicity
            # First check if it has its own Redis z.derivative_metric key
            # that has not expired
            derivative_metric_key = 'z.derivative_metric.%s' % str(base_name)

            if unknown_deriv_status:
                # @added 20170617 - Bug #2050: analyse_derivatives - change in monotonicity
                last_derivative_metric_key = False
                try:
                    last_derivative_metric_key = self.redis_conn.get(derivative_metric_key)
                except Exception as e:
                    logger.error('error :: could not query Redis for last_derivative_metric_key: %s' % e)

                # Determine if it is a strictly increasing monotonically metric
                # or has been in last FULL_DURATION via its z.derivative_metric
                # key
                if not last_derivative_metric_key:
                    is_strictly_increasing_monotonically = strictly_increasing_monotonicity(timeseries)
                    if is_strictly_increasing_monotonically:
                        try:
                            last_expire_set = int(time())
                            self.redis_conn.setex(
                                derivative_metric_key, settings.FULL_DURATION, last_expire_set)
                        except Exception as e:
                            logger.error('error :: could not set Redis derivative_metric key: %s' % e)
                else:
                    # Until the z.derivative_metric key expires, it is classed
                    # as such
                    is_strictly_increasing_monotonically = True

                skip_derivative = in_list(base_name, non_derivative_monotonic_metrics)
                if skip_derivative:
                    is_strictly_increasing_monotonically = False
                if is_strictly_increasing_monotonically:
                    known_derivative_metric = True
                    try:
                        self.redis_conn.sadd('derivative_metrics', metric_name)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis derivative_metrics set')
                    try:
                        self.redis_conn.sadd('new_derivative_metrics', metric_name)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis new_derivative_metrics set')
                else:
                    try:
                        self.redis_conn.sadd('non_derivative_metrics', metric_name)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis non_derivative_metrics set')
                    try:
                        self.redis_conn.sadd('new_non_derivative_metrics', metric_name)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis new_non_derivative_metrics set')
            if known_derivative_metric:
                try:
                    derivative_timeseries = nonNegativeDerivative(timeseries)
                    timeseries = derivative_timeseries
                except:
                    logger.error('error :: nonNegativeDerivative failed')

            # @added 20180903 - Feature #2580: illuminance
            #                   Feature #1986: flux
            # Disabled for now as in concept phase.  This would work better if
            # the illuminance_datapoint was determined from the time series
            # after min max scaling wass applied to it.  That way each metric
            # would have the same weight.  txBytes for instance would not have
            # a far greater weight than a low range metric.
            if calculate_illuminance:
                try:
                    illuminance_datapoint = timeseries[-1][1]
                    if '.illuminance' not in metric_name:
                        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        # Use Redis set instead of Manager() list to reduce memory
                        # and number of Python processes
                        # self.illuminance_datapoints.append(illuminance_datapoint)
                        redis_set = 'analyzer.illuminance_datapoints'
                        data = illuminance_datapoint
                        try:
                            self.redis_conn.sadd(redis_set, data)
                        except Exception as e:
                            logger.info(traceback.format_exc())
                            logger.error('error :: failed to add %s to Redis set %s: %s' % (
                                str(data), str(redis_set), str(e)))
                except:
                    pass

            try:

                # @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
                # Determine if any metrcs have negatives values some they can be
                # added to the ionosphere.untrainable_metrics Redis set
                run_negatives_present = False
                enabled_check_for_negatives = False
                if settings.IONOSPHERE_ENABLED and enabled_check_for_negatives:
                    run_negatives_present = True
                    known_negative_metric_matched_by = None
                    known_negative_metric, known_negative_metric_matched_by = matched_or_regexed_in_list(skyline_app, metric_name, KNOWN_NEGATIVE_METRICS)
                    if known_negative_metric:
                        run_negatives_present = False

                # @modified 20200117 - Feature #3400: Identify air gaps in the metric data
                # Also pass the airgapped_metrics list so that it is only got
                # once per run and does not have to be queried in algorithms
                # anomalous, ensemble, datapoint = run_selected_algorithm(timeseries, metric_name)
                # @modified 20200213 - Bug #3448: Repeated airgapped_metrics
                # Filter out airgaps for the metric from airgapped_metrics and
                # pass just those
                # anomalous, ensemble, datapoint = run_selected_algorithm(timeseries, metric_name, airgapped_metrics)
                metric_airgaps = []
                for i in airgapped_metrics:
                    if base_name in i:
                        metric_airgaps.append(i)
                # @added 20200501 - Feature #3400: Identify air gaps in the metric data
                # Handle airgaps filled so that once they have been submitted as filled
                # Analyzer will not identify them as airgapped again, even if there is
                # a airgap in the original airgap period
                metric_airgaps_filled = []
                for i in airgapped_metrics_filled:
                    if base_name in i:
                        metric_airgaps_filled.append(i)

                # @modified 20200424 - Feature #3508: ionosphere.untrainable_metrics
                # Added negatives_found and run_negatives_present
                # anomalous, ensemble, datapoint = run_selected_algorithm(timeseries, metric_name, metric_airgaps)
                # @modified 20200501 - Feature #3400: Identify air gaps in the metric data
                # Added metric_airgaps_filled and check_for_airgaps_only
                # anomalous, ensemble, datapoint, negatives_found = run_selected_algorithm(timeseries, metric_name, metric_airgaps, run_negatives_present)
                anomalous, ensemble, datapoint, negatives_found = run_selected_algorithm(timeseries, metric_name, metric_airgaps, metric_airgaps_filled, run_negatives_present, check_for_airgaps_only)
                del metric_airgaps
                del metric_airgaps_filled

                # @added 20191016 - Branch #3262: py3
                if LOCAL_DEBUG:
                    logger.info('debug :: metric %s - anomalous - %s' % (str(metric_name), str(anomalous)))

                # @added 20200214 - Bug #3448: Repeated airgapped_metrics
                #                   Feature #3400: Identify air gaps in the metric data
                # If the metric Redis key was updated with new data added during
                # the key rename after sorting and deduplicating the time series
                # data, it is possible the add timestamps in the appended data
                # are unordered.  In these cases do not honor a anomalous
                # result.
                if update_new_data and anomalous:
                    logger.info('metric %s - anomalous - %s, however not honoring this result as the sorted, deduplicated key data was appended with new data which may have been unordered' % (str(metric_name), str(anomalous)))
                    anomalous = False

                # @added 20191107 - Feature #3306: Record anomaly_end_timestamp
                #                   Branch #3262: py3
                # Created an Analyzer Redis set to keep track of not anomalous
                # metrics so that the anomaly_end_timestamp can be recorded
                if not anomalous:
                    try:
                        metric_timestamp = timeseries[-1][0]
                    except:
                        metric_timestamp = None
                    recent = False
                    if metric_timestamp:
                        # Only update if the timestamp is recent
                        deemed_recent = int(spin_start) - settings.STALE_PERIOD
                        if int(metric_timestamp) > deemed_recent:
                            recent = True
                    if recent:
                        try:
                            redis_set = 'analyzer.not_anomalous_metrics'
                            try:
                                base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                                data = [base_name, int(metric_timestamp)]
                                self.redis_conn.sadd(redis_set, str(data))
                            except Exception as e:
                                logger.info(traceback.format_exc())
                                logger.error('error :: failed to add %s to Redis set %s: %s' % (
                                    str(data), str(redis_set), str(e)))
                        except:
                            pass

                # @added 20190408 - Feature #2882: Mirage - periodic_check
                # Add for Mirage periodic - is really anomalous add to
                # real_anomalous_metrics and if in mirage_periodic_check_metric_list
                # add as anomalous
                if anomalous:
                    # @modified 20190412 - Bug #2932: self.real_anomalous_metrics not being populated correctly
                    #                      Feature #2882: Mirage - periodic_check
                    # self.real_anomalous_metrics.append(base_name)
                    base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    metric_timestamp = timeseries[-1][0]
                    metric = [datapoint, base_name, metric_timestamp]
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # Use Redis set instead of Manager() list to reduce memory
                    # and number of Python processes
                    # self.real_anomalous_metrics.append(metric)
                    redis_set = 'analyzer.real_anomalous_metrics'
                    data = str(metric)
                    try:
                        self.redis_conn.sadd(redis_set, data)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to add %s to Redis set %s' % (
                            str(data), str(redis_set)))

                if metric_name in mirage_periodic_check_metric_list:
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # Use Redis set instead of Manager() list to reduce memory
                    # and number of Python processes
                    # self.mirage_periodic_check_metrics.append(base_name)
                    redis_set = 'analyzer.mirage_periodic_check_metrics'
                    data = str(base_name)
                    try:
                        self.redis_conn.sadd(redis_set, data)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to add %s to Redis set %s' % (
                            str(data), str(redis_set)))

                    anomalous = True

                # @added 20200411 - Feature #3480: batch_processing
                if BATCH_PROCESSING:
                    if batch_metric:
                        # @modified 20200423 - Feature #3504: Handle airgaps in batch metrics
                        # Only update the last_timestamp key if this is NOT a
                        # check_for_airgap_only run
                        if not check_for_airgap_only:
                            last_metric_timestamp_key = 'last_timestamp.%s' % base_name
                            try:
                                int_metric_timestamp = int(metric_timestamp)
                                # @modified 20200503 - Feature #3504: Handle airgaps in batch metrics
                                #                      Feature #3480: batch_processing
                                #                      Feature #3486: analyzer_batch
                                # Set the last_timestamp expiry time to 1 month rather than
                                # settings.FULL_DURATION
                                self.redis_conn.setex(
                                    last_metric_timestamp_key, 2592000,
                                    int_metric_timestamp)
                                if BATCH_PROCESSING_DEBUG:
                                    logger.info('batch processing - normal analyzer analysis set Redis key %s to %s' % (
                                        last_metric_timestamp_key, str(int_metric_timestamp)))
                            except:
                                logger.error('error :: batch processing - failed to set Redis key %s' % last_metric_timestamp_key)

                # If it's anomalous, add it to list
                if anomalous:
                    base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    metric_timestamp = timeseries[-1][0]
                    metric = [datapoint, base_name, metric_timestamp]
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # self.anomalous_metrics.append(metric)
                    redis_set = 'analyzer.anomalous_metrics'
                    data = str(metric)
                    try:
                        self.redis_conn.sadd(redis_set, data)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to add %s to Redis set %s' % (
                            str(data), str(redis_set)))

                    # Get the anomaly breakdown - who returned True?
                    triggered_algorithms = []
                    for index, value in enumerate(ensemble):
                        if value:
                            algorithm = settings.ALGORITHMS[index]
                            anomaly_breakdown[algorithm] += 1
                            triggered_algorithms.append(algorithm)

                    # @added 20170206 - Bug #1904: Handle non filesystem friendly metric names in check files
                    sane_metricname = filesafe_metricname(str(base_name))

                    # @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
                    # Determine if any metrcs have negatives values some they can be
                    # added to the ionosphere.untrainable_metrics Redis set
                    if run_negatives_present and negatives_found:
                        redis_set = 'ionosphere.untrainable_metrics'
                        try:
                            last_negative_timestamp = int(negatives_found[-1][0])
                            last_negative_value = negatives_found[-1][1]
                            remove_after_timestamp = int(last_negative_timestamp + settings.FULL_DURATION)
                            data = str([metric_name, metric_timestamp, datapoint, last_negative_timestamp, last_negative_value, settings.FULL_DURATION, remove_after_timestamp])
                            self.redis_conn.sadd(redis_set, data)
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: failed to add %s to Redis set %s' % (
                                str(data), str(redis_set)))

                    # If Crucible or Panorama are enabled determine details
                    determine_anomaly_details = False
                    if settings.ENABLE_CRUCIBLE and settings.ANALYZER_CRUCIBLE_ENABLED:
                        determine_anomaly_details = True
                    if settings.PANORAMA_ENABLED:
                        determine_anomaly_details = True

                    # If Ionosphere is enabled determine details
                    try:
                        ionosphere_enabled = settings.IONOSPHERE_ENABLED
                        if settings.IONOSPHERE_ENABLED:
                            determine_anomaly_details = True
                    except:
                        ionosphere_enabled = False

                    if determine_anomaly_details:
                        metric_timestamp = str(int(timeseries[-1][0]))
                        from_timestamp = str(int(timeseries[1][0]))
                        timeseries_dir = base_name.replace('.', '/')

                    # @added 20161119 - Branch #922: ionosphere
                    #                   Task #1718: review.tsfresh
                    # Set defaults which can be used later to determine how
                    # Analyzer should handle/route anomalies
                    analyzer_metric = True
                    mirage_metric = False
                    # crucible_metric = False
                    # panorama_metric = False
                    ionosphere_metric = False
                    # send_to_panaroma = False
                    send_to_ionosphere = False

                    if metric_name in ionosphere_unique_metrics:
                        # @modified 20170101 - Feature #1830: Ionosphere alerts
                        # Correct rate limitining Ionosphere on last_alert cache key
                        # Do not set analyzer_metric to False until after last_alert
                        # key has been checked otherwise Ionosphere will get every
                        # anomaly and as it will not be rate limited by the alert
                        # cache key
                        # analyzer_metric = False
                        ionosphere_metric = True
                        send_to_ionosphere = True

                    if metric_name in mirage_unique_metrics:
                        analyzer_metric = False
                        ionosphere_metric = False
                        mirage_metric = True
                        send_to_ionosphere = False

                    # @added 20170108 - Feature #1830: Ionosphere alerts
                    # Only send smtp_alerter_metrics to Ionosphere
                    smtp_alert_enabled_metric = True
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # Use Redis set analyzer.non_smtp_alerter_metrics in place of
                    # self.non_smtp_alerter_metrics Manager.list
                    # if base_name in self.non_smtp_alerter_metrics:
                    if base_name in non_smtp_alerter_metrics:
                        smtp_alert_enabled_metric = False

                    if ionosphere_enabled:
                        if analyzer_metric:
                            # We do not want send all anomalous metrics to
                            # Ionosphere if they are not being alerted on as
                            # they will be pointless they will have no alert if
                            # it is within the EXPIRATION_TIME and there will be
                            # no reference graphs from an alert for the user to
                            # action.
                            cache_key = 'last_alert.smtp.%s' % (base_name)
                            last_alert = False
                            try:
                                last_alert = self.redis_conn.get(cache_key)
                            except Exception as e:
                                logger.error('error :: could not query Redis for cache_key: %s' % e)

                            if not last_alert:
                                send_to_ionosphere = True
                            else:
                                send_to_ionosphere = False
                                if ionosphere_metric:
                                    logger.info('not sending to Ionosphere - alert key exists - %s' % (base_name))
                        else:
                            if mirage_metric:
                                logger.info('not sending to Ionosphere - Mirage metric - %s' % (base_name))
                                send_to_ionosphere = False
                                # @added 20170306 - Feature #1960: ionosphere_layers
                                # Ionosphere layers require the timeseries at
                                # FULL_DURATION so if this is a Mirage and
                                # Ionosphere metric, Analyzer needs to provide
                                # the timeseries file for later (within 60
                                # seconds) analysis, however we want the data
                                # that triggered the anomaly, as before this was
                                # only created by Mirage if an alert was
                                # triggered, but Ionosphere layers now require
                                # this file before an alert is triggered
                                timeseries_dir = base_name.replace('.', '/')
                                training_dir = '%s/%s/%s' % (
                                    settings.IONOSPHERE_DATA_FOLDER, str(metric_timestamp),
                                    str(timeseries_dir))
                                if not os.path.exists(training_dir):
                                    mkdir_p(training_dir)
                                full_duration_in_hours = int(settings.FULL_DURATION) / 3600
                                ionosphere_json_file = '%s/%s.mirage.redis.%sh.json' % (
                                    training_dir, base_name,
                                    str(int(full_duration_in_hours)))
                                if not os.path.isfile(ionosphere_json_file):
                                    timeseries_json = str(timeseries).replace('[', '(').replace(']', ')')
                                    try:
                                        write_data_to_file(skyline_app, ionosphere_json_file, 'w', timeseries_json)
                                        logger.info('%s added Ionosphere Mirage %sh Redis data timeseries json file :: %s' % (
                                            skyline_app, str(int(full_duration_in_hours)), ionosphere_json_file))
                                    except:
                                        logger.info(traceback.format_exc())
                                        logger.error('error :: failed to add %s Ionosphere Mirage Redis data timeseries json file - %s' % (skyline_app, ionosphere_json_file))

                    # @modified 20170108 - Feature #1830: Ionosphere alerts
                    # Only send smtp_alerter_metrics to Ionosphere
                    # if send_to_ionosphere:
                    if send_to_ionosphere and smtp_alert_enabled_metric:
                        if metric_name in ionosphere_unique_metrics:
                            logger.info('sending an ionosphere metric to Ionosphere - %s' % (base_name))
                        else:
                            logger.info('sending an analyzer metric to Ionosphere for training - %s' % (base_name))
                        try:
                            # @modified 20161228 Feature #1828: ionosphere - mirage Redis data features
                            # Added full_duration
                            # @modified 20170127 - Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
                            # Added ionosphere_parent_id, always zero from Analyzer and Mirage
                            ionosphere_parent_id = 0
                            send_anomalous_metric_to(
                                skyline_app, 'ionosphere', timeseries_dir,
                                metric_timestamp, base_name, str(datapoint),
                                from_timestamp, triggered_algorithms,
                                timeseries, str(settings.FULL_DURATION),
                                str(ionosphere_parent_id))
                            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                            # Moved to Redis key block below
                            # self.sent_to_ionosphere.append(base_name)
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: failed to send_anomalous_metric_to to ionosphere')

                        # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        redis_set = 'analyzer.sent_to_ionosphere'
                        data = str(base_name)
                        try:
                            self.redis_conn.sadd(redis_set, data)
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: failed to add %s to Redis set %s' % (
                                str(data), str(redis_set)))

                        # @added 20170403 - Feature #1994: Ionosphere training_dir keys
                        #                   Feature #2000: Ionosphere - validated
                        #                   Feature #1996: Ionosphere - matches page
                        # The addition of this key data could be done in
                        # skyline_function.py, however that would introduce
                        # Redis requirements in the send_anomalous_metric_to
                        # function, which is not desirable I think. So this is
                        # a non-KISS pattern that is replicated in mirage.py as
                        # well.
                        # Each training_dir and data set is now Redis keyed to increase efficiency
                        # in terms of disk I/O for ionosphere.py and making keyed data
                        # available for each training_dir data set so that transient matched data
                        # can be surfaced for the webapp along with directory paths, etc
                        ionosphere_training_data_key = 'ionosphere.training_data.%s.%s' % (str(metric_timestamp), base_name)
                        ionosphere_training_data_key_data = [
                            ['metric_timestamp', int(metric_timestamp)],
                            ['base_name', str(base_name)],
                            ['timeseries_dir', str(timeseries_dir)],
                            ['added_by', str(skyline_app)]
                        ]
                        try:
                            self.redis_conn.setex(
                                ionosphere_training_data_key,
                                settings.IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR,
                                # @modified 20190413 - Task #2824: Test redis-py upgrade
                                #                      Task #2926: Update dependencies
                                # redis-py 3.x only accepts user data as bytes, strings or
                                # numbers (ints, longs and floats).  All 2.X users should
                                # make sure that the keys and values they pass into redis-py
                                # are either bytes, strings or numbers. Use str
                                str(ionosphere_training_data_key_data))
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: failed to set Redis key %s' % ionosphere_training_data_key)

                    if ionosphere_metric:
                        analyzer_metric = False

                    # Only send Analyzer metrics
                    if analyzer_metric and settings.PANORAMA_ENABLED:
                        if not os.path.exists(settings.PANORAMA_CHECK_PATH):
                            mkdir_p(settings.PANORAMA_CHECK_PATH)

                        # Note:
                        # The values are enclosed is single quoted intentionally
                        # as the imp.load_source used results in a shift in the
                        # decimal position when double quoted, e.g.
                        # value = "5622.0" gets imported as
                        # 2016-03-02 12:53:26 :: 28569 :: metric variable - value - 562.2
                        # single quoting results in the desired,
                        # 2016-03-02 13:16:17 :: 1515 :: metric variable - value - 5622.0
                        added_at = str(int(time()))
                        source = 'graphite'
                        panaroma_anomaly_data = 'metric = \'%s\'\n' \
                                                'value = \'%s\'\n' \
                                                'from_timestamp = \'%s\'\n' \
                                                'metric_timestamp = \'%s\'\n' \
                                                'algorithms = %s\n' \
                                                'triggered_algorithms = %s\n' \
                                                'app = \'%s\'\n' \
                                                'source = \'%s\'\n' \
                                                'added_by = \'%s\'\n' \
                                                'added_at = \'%s\'\n' \
                            % (base_name, str(datapoint), from_timestamp,
                               metric_timestamp, str(settings.ALGORITHMS),
                               triggered_algorithms, skyline_app, source,
                               this_host, added_at)

                        # Create an anomaly file with details about the anomaly
                        panaroma_anomaly_file = '%s/%s.%s.txt' % (
                            settings.PANORAMA_CHECK_PATH, added_at,
                            sane_metricname)
                        try:
                            write_data_to_file(
                                skyline_app, panaroma_anomaly_file, 'w',
                                panaroma_anomaly_data)
                            logger.info('added panorama anomaly file :: %s' % (panaroma_anomaly_file))
                            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                            # Moved to Redis set block below
                            # self.sent_to_panorama.append(base_name)
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: failed to add panorama anomaly file :: %s' % (panaroma_anomaly_file))

                        # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        redis_set = 'analyzer.sent_to_panorama'
                        data = str(base_name)
                        try:
                            self.redis_conn.sadd(redis_set, data)
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: failed to add %s to Redis set %s' % (
                                str(data), str(redis_set)))
                    else:
                        # @modified 20160207 - Branch #922: Ionosphere
                        # Handle if all other apps are not enabled
                        other_app = 'none'
                        if mirage_metric:
                            other_app = 'Mirage'
                        if ionosphere_metric:
                            other_app = 'Ionosphere'
                        logger.info('not adding panorama anomaly file for %s - %s' % (other_app, metric))

                    # If Crucible is enabled - save timeseries and create a
                    # Crucible check
                    if settings.ENABLE_CRUCIBLE and settings.ANALYZER_CRUCIBLE_ENABLED:
                        crucible_anomaly_dir = settings.CRUCIBLE_DATA_FOLDER + '/' + timeseries_dir + '/' + metric_timestamp
                        if not os.path.exists(crucible_anomaly_dir):
                            mkdir_p(crucible_anomaly_dir)

                        # Note:
                        # The values are enclosed is single quoted intentionally
                        # as the imp.load_source used in crucible results in a
                        # shift in the decimal position when double quoted, e.g.
                        # value = "5622.0" gets imported as
                        # 2016-03-02 12:53:26 :: 28569 :: metric variable - value - 562.2
                        # single quoting results in the desired,
                        # 2016-03-02 13:16:17 :: 1515 :: metric variable - value - 5622.0

                        crucible_anomaly_data = 'metric = \'%s\'\n' \
                                                'value = \'%s\'\n' \
                                                'from_timestamp = \'%s\'\n' \
                                                'metric_timestamp = \'%s\'\n' \
                                                'algorithms = %s\n' \
                                                'triggered_algorithms = %s\n' \
                                                'anomaly_dir = \'%s\'\n' \
                                                'graphite_metric = True\n' \
                                                'run_crucible_tests = False\n' \
                                                'added_by = \'%s\'\n' \
                                                'added_at = \'%s\'\n' \
                            % (base_name, str(datapoint), from_timestamp,
                               metric_timestamp, str(settings.ALGORITHMS),
                               triggered_algorithms, crucible_anomaly_dir,
                               skyline_app, metric_timestamp)

                        # @added 20170206 - Bug #1904: Handle non filesystem friendly metric names in check files
                        sane_metricname = filesafe_metricname(str(base_name))

                        # Create an anomaly file with details about the anomaly
                        crucible_anomaly_file = '%s/%s.txt' % (crucible_anomaly_dir, sane_metricname)
                        try:
                            write_data_to_file(
                                skyline_app, crucible_anomaly_file, 'w',
                                crucible_anomaly_data)
                            logger.info('added crucible anomaly file :: %s' % (crucible_anomaly_file))
                        except:
                            logger.error('error :: failed to add crucible anomaly file :: %s' % (crucible_anomaly_file))
                            logger.info(traceback.format_exc())

                        # Create timeseries json file with the timeseries
                        json_file = '%s/%s.json' % (crucible_anomaly_dir, base_name)
                        timeseries_json = str(timeseries).replace('[', '(').replace(']', ')')
                        try:
                            write_data_to_file(skyline_app, json_file, 'w', timeseries_json)
                            logger.info('added crucible timeseries file :: %s' % (json_file))
                        except:
                            logger.error('error :: failed to add crucible timeseries file :: %s' % (json_file))
                            logger.info(traceback.format_exc())

                        # Create a crucible check file
                        crucible_check_file = '%s/%s.%s.txt' % (settings.CRUCIBLE_CHECK_PATH, metric_timestamp, sane_metricname)
                        try:
                            write_data_to_file(
                                skyline_app, crucible_check_file, 'w',
                                crucible_anomaly_data)
                            logger.info('added crucible check :: %s,%s' % (base_name, metric_timestamp))
                            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                            # Move to Redis set block below
                            # self.sent_to_crucible.append(base_name)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to add crucible check file :: %s' % (crucible_check_file))

                        # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        redis_set = 'analyzer.sent_to_crucible'
                        data = str(base_name)
                        try:
                            self.redis_conn.sadd(redis_set, data)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to add %s to Redis set %s' % (
                                str(data), str(redis_set)))

            # It could have been deleted by the Roomba
            except TypeError:
                # @added 20200430 - Feature #3480: batch_processing
                # Added logging here as the DeletedByRoomba exception is
                # generally not related to that but related to some other fail
                # in the processing of the run algorithms phase.
                # It could have been deleted by the Roomba, but probably was not
                logger.error(traceback.format_exc())
                logger.error('error :: added as DeletedByRoomba but possibly not see traceback above')

                exceptions['DeletedByRoomba'] += 1
            # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
            # Added Redis sets for Boring, TooShort and Stale
            except TooShort:
                exceptions['TooShort'] += 1
                try:
                    self.redis_conn.sadd('analyzer.too_short', base_name)
                except:
                    redis_set_errors += 1
                # @added 20200423 - Feature #3504: Handle airgaps in batch metrics
                #                   Feature #3480: batch_processing
                #                   Feature #3486: analyzer_batch
                # If a batch_metric is added and analyzer also sees it as stale,
                # too short or boring it never get processed normally and has no
                # 'last_timestamp.' Redis key set
                if batch_metric:
                    if not last_metric_timestamp:
                        # If there is no known last_timestamp, this is the first
                        # processing of a metric and it is stale add the key
                        # anyway.
                        last_metric_timestamp_key = 'last_timestamp.%s' % base_name
                        try:
                            metric_timestamp = timeseries[-1][0]
                            int_metric_timestamp = int(metric_timestamp)
                            self.redis_conn.setex(
                                last_metric_timestamp_key, 2592000,
                                int_metric_timestamp)
                            if BATCH_PROCESSING_DEBUG:
                                logger.info('batch processing - normal analyzer analysis set Redis key %s to %s, even though it is too short' % (
                                    last_metric_timestamp_key, str(int_metric_timestamp)))
                        except:
                            logger.error('error :: batch processing - failed to set Redis key %s, even though it is too short' % last_metric_timestamp_key)
            except Stale:
                exceptions['Stale'] += 1
                try:
                    self.redis_conn.sadd('analyzer.stale', base_name)
                except:
                    redis_set_errors += 1
                # @added 20200423 - Feature #3504: Handle airgaps in batch metrics
                #                   Feature #3480: batch_processing
                #                   Feature #3486: analyzer_batch
                if batch_metric:
                    if not last_metric_timestamp:
                        last_metric_timestamp_key = 'last_timestamp.%s' % base_name
                        try:
                            metric_timestamp = timeseries[-1][0]
                            int_metric_timestamp = int(metric_timestamp)
                            self.redis_conn.setex(
                                last_metric_timestamp_key,
                                2592000, int_metric_timestamp)
                            if BATCH_PROCESSING_DEBUG:
                                logger.info('batch processing - normal analyzer analysis set Redis key %s to %s, even though it is stale' % (
                                    last_metric_timestamp_key, str(int_metric_timestamp)))
                        except:
                            logger.error('error :: batch processing - failed to set Redis key %s, even though it is stale' % last_metric_timestamp_key)
            except Boring:
                exceptions['Boring'] += 1
                try:
                    self.redis_conn.sadd('analyzer.boring', base_name)
                except:
                    redis_set_errors += 1
                # @added 20200423 - Feature #3504: Handle airgaps in batch metrics
                #                   Feature #3480: batch_processing
                #                   Feature #3486: analyzer_batch
                if batch_metric:
                    if not last_metric_timestamp:
                        last_metric_timestamp_key = 'last_timestamp.%s' % base_name
                        try:
                            metric_timestamp = timeseries[-1][0]
                            int_metric_timestamp = int(metric_timestamp)
                            self.redis_conn.setex(
                                last_metric_timestamp_key,
                                2592000, int_metric_timestamp)
                            if BATCH_PROCESSING_DEBUG:
                                logger.info('batch processing - normal analyzer analysis set Redis key %s to %s, even though it is boring' % (
                                    last_metric_timestamp_key, str(int_metric_timestamp)))
                        except:
                            logger.error('error :: batch processing - failed to set Redis key %s, even though it is boring' % last_metric_timestamp_key)
            except:
                exceptions['Other'] += 1
                logger.info(traceback.format_exc())

        # @added 20200430 - Feature #3480: batch_processing
        # Tidy up and reduce logging, consolidate logging with counts
        if BATCH_PROCESSING:
            logger.info('batch processing - identified %s batch metrics' % (
                str(batch_metric_count)))
            if batch_processing_down_processing_normally > 0:
                logger.error('error :: batch processing - analyzer_batch not reporting up, %s batch metrics were processed via the normal analyzer analysis' % (
                    str(batch_processing_down_processing_normally)))
            logger.info('batch processing - %s of the batch metrics have their last_timestamp match the last timestamp in Redis and were not sent to analyzer_batch' % (
                str(last_timestamps_match_not_batch_processing)))
            logger.info('batch processing - %s of the batch metrics have their last_timestamp match the last timestamp in Redis and were not sent through analyzer to be identified as stale or airgapped only, not processed' % (
                str(last_timestamps_match_no_stale_or_airgap_check_not_processing)))
            logger.info('batch processing - %s of the batch metrics have no new data but were sent through analyzer to be identified as stale or airgapped only' % (
                str(no_new_data_but_sent_to_normal_processing_to_check_for_stale_or_airaps)))
            logger.info('batch processing - %s of the batch metrics have no new data and not identified as stale or airgapped, not processed' % (
                str(no_new_data_and_not_sent_to_check_for_stale_or_airaps)))
            logger.info('batch processing - %s batch metrics were sent to analyzer_batch to be processed' % (
                str(sent_to_analyzer_batch_proccessing_metrics)))
            logger.info('batch processing - %s of the batch metrics were sent to analyzer_batch to be processed and also sent through analyzer to to be identified as stale or airgapped only' % (
                str(sent_to_analyzer_batch_proccessing_metrics_but_also_processing_normally_to_identify_airgaps)))
            logger.info('batch processing - %s of the batch metrics not batch processed but sent through analyzer to be processed because the current penultimate timeseries timestamp was equal to the second last timestamp in Redis' % (
                str(batch_metric_but_processing_normally_as_penultimate_timeseries_timestamp_equals_second_last_timestamp_from_redis)))

        # @added 20200117 - Feature #3400: Identify air gaps in the metric data
        del airgapped_metrics

        # @added 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        # Added Redis sets for Boring, TooShort and Stale
        if redis_set_errors > 0:
            logger.error('error :: failed to add some metric/s to a Redis analyzer.{boring,stale,too_short} set')
        else:
            # @added 20180523 - Feature #2378: Add redis auth to Skyline and rebrow
            # Added update time to Redis sets for Boring, TooShort and Stale
            try:
                updated_analyzer_sets_at = '_updated_at_%s' % str(strftime('%Y%m%d%H%M%S', gmtime()))
                self.redis_conn.sadd('analyzer.boring', updated_analyzer_sets_at)
                self.redis_conn.sadd('analyzer.stale', updated_analyzer_sets_at)
                self.redis_conn.sadd('analyzer.too_short', updated_analyzer_sets_at)
            except:
                redis_set_errors += 1

        # Add values to the queue so the parent process can collate
        for key, value in anomaly_breakdown.items():
            self.anomaly_breakdown_q.put((key, value))

        for key, value in exceptions.items():
            self.exceptions_q.put((key, value))

        if LOCAL_DEBUG:
            logger.info('debug :: Memory usage spin_process end: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

        spin_end = time() - spin_start
        logger.info('spin_process took %.2f seconds' % spin_end)
        return

    def run(self):
        """
        - Called when the process intializes.

        - Determine if Redis is up and discover the number of `unique metrics`.

        - Divide the `unique_metrics` between the number of `ANALYZER_PROCESSES`
          and assign each process a set of metrics to analyse for anomalies.

        - Wait for the processes to finish.

        - Determine whether if any anomalous metrics require:

            - Alerting on (and set `EXPIRATION_TIME` key in Redis for alert).
            - Feed to another module e.g. mirage.
            - Alert to syslog.

        - Populate the webapp json with the anomalous_metrics details.

        - Log the details about the run to the skyline analyzer log.

        - Send skyline.analyzer metrics to `GRAPHITE_HOST`
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

        # @added 20190417 - Feature #2950: Report defaulted settings to log
        # Added all the globally declared settings to enable reporting in the
        # log the state of each setting.
        try:
            SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
            if SERVER_METRIC_PATH == '.':
                SERVER_METRIC_PATH = ''
            logger.info('SERVER_METRIC_PATH is set from settings.py to %s' % str(SERVER_METRIC_PATH))
        except:
            SERVER_METRIC_PATH = ''
            logger.info('warning :: SERVER_METRIC_PATH is not declared in settings.py, defaults to \'\'')
        logger.info('skyline_app_graphite_namespace is set to %s' % str(skyline_app_graphite_namespace))
        try:
            ANALYZER_ENABLED = settings.ANALYZER_ENABLED
            logger.info('ANALYZER_ENABLED is set to %s' % str(ANALYZER_ENABLED))
        except:
            ANALYZER_ENABLED = True
            logger.info('warning :: ANALYZER_ENABLED is not declared in settings.py, defaults to True')
        try:
            DO_NOT_ALERT_ON_STALE_METRICS = settings.DO_NOT_ALERT_ON_STALE_METRICS
            do_not_alert_count = len(DO_NOT_ALERT_ON_STALE_METRICS)
            logger.info('DO_NOT_ALERT_ON_STALE_METRICS is set from settings.py to not alert on %s namespaces' % str(do_not_alert_count))
        except:
            DO_NOT_ALERT_ON_STALE_METRICS = []
            logger.info('warning :: DO_NOT_ALERT_ON_STALE_METRICS is not declared in settings.py, defaults to []')
        try:
            ALERT_ON_STALE_METRICS = settings.ALERT_ON_STALE_METRICS
            logger.info('ALERT_ON_STALE_METRICS is set from settings.py to %s' % str(ALERT_ON_STALE_METRICS))
        except:
            ALERT_ON_STALE_METRICS = False
            logger.info('warning :: ALERT_ON_STALE_METRICS is not declared in settings.py, defaults to %s' % str(ALERT_ON_STALE_METRICS))

        if not os.path.exists(settings.SKYLINE_TMP_DIR):
            # @modified 20160803 - Adding additional exception handling to Analyzer
            try:
                mkdir_p(settings.SKYLINE_TMP_DIR)
            except:
                logger.error('error :: failed to create %s' % settings.SKYLINE_TMP_DIR)
                logger.info(traceback.format_exc())

        def smtp_trigger_alert(alert, metric, context):
            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            try:
                p = Process(target=self.spawn_alerter_process, args=(alert, metric, context))
                pids.append(p)
                pid_count += 1
                p.start()
                spawned_pids.append(p.pid)
            except:
                logger.error('error :: failed to spawn_alerter_process')
                logger.info(traceback.format_exc())
            p_starts = time()
            while time() - p_starts <= 15:
                if any(p.is_alive() for p in pids):
                    # Just to avoid hogging the CPU
                    sleep(.1)
                else:
                    # All the processes are done, break now.
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('%s :: timed out, killing the spawn_trigger_alert process' % (skyline_app))
                for p in pids:
                    p.terminate()
                    # p.join()

            for p in pids:
                if p.is_alive():
                    logger.info('%s :: stopping spawn_trigger_alert - %s' % (skyline_app, str(p.is_alive())))
                    p.join()

        # Initiate the algorithm timings if Analyzer is configured to send the
        # algorithm_breakdown metrics with ENABLE_ALGORITHM_RUN_METRICS
        algorithm_tmp_file_prefix = settings.SKYLINE_TMP_DIR + '/' + skyline_app + '.'
        algorithms_to_time = []
        if send_algorithm_run_metrics:
            algorithms_to_time = settings.ALGORITHMS

        if LOCAL_DEBUG:
            logger.info('debug :: Memory usage in run after algorithms_to_time: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

        while 1:
            now = time()

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage before unique_metrics lookup: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except:
                logger.error('error :: Analyzer cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                logger.info(traceback.format_exc())
                sleep(10)
                try:
                    # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                    # @added 20191030 - Bug #3266: py3 Redis binary objects not strings
                    #                   Branch #3262: py3
                    # if settings.REDIS_PASSWORD:
                    #     self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                    # else:
                    #     self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                    self.redis_conn = get_redis_conn(skyline_app)
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                except:
                    logger.info(traceback.format_exc())
                    # logger.error('error :: Analyzer cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                    logger.error('error :: Analyzer cannot connect to get_redis_conn')
                continue

            # Report app up
            try:
                self.redis_conn.setex(skyline_app, 120, now)
            except:
                logger.error('error :: Analyzer could not update the Redis %s key' % skyline_app)
                logger.info(traceback.format_exc())

            # Discover unique metrics
            # @modified 20160803 - Adding additional exception handling to Analyzer
            try:
                # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                #                   Branch #3262: py3
                # unique_metrics = list(self.redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
                unique_metrics = list(self.redis_conn_decoded.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
            except:
                logger.error('error :: Analyzer could not get the unique_metrics list from Redis')
                logger.info(traceback.format_exc())
                sleep(10)
                continue

            if len(unique_metrics) == 0:
                logger.info('no metrics in redis. try adding some - see README')
                sleep(10)
                continue

            # @modified 20190518 - Bug #3020: Newly installed analyzer on docker not flushing and recreating analyzer algorithm exception Redis sets
            # The management of the analyzer Redis algorithm exception sets
            # was previously handled in the if settings.ENABLE_MIRAGE
            # block below.  This meant that if there are no mirage metrics, the
            # sets were not managed.
            manage_analyzer_algorithm_exception_sets = False
            try:
                manage_analyzer_algorithm_exception_sets_in_seconds = self.redis_conn.get('analyzer.algorithm.exception.metric.sets')
                if str(manage_analyzer_algorithm_exception_sets_in_seconds) == 'None':
                    manage_analyzer_algorithm_exception_sets = True
            except Exception as e:
                logger.error('error :: could not query Redis for analyzer.algorithm.exception.metric.sets: %s' % e)
                manage_analyzer_algorithm_exception_sets = True
            if manage_analyzer_algorithm_exception_sets:
                try:
                    key_timestamp = int(time())
                    self.redis_conn.setex('analyzer.algorithm.exception.metric.sets', 300, key_timestamp)
                except:
                    logger.error('error :: failed to set key :: analyzer.algorithm.exception.metric.sets')
                # @added 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                # Added Redis sets for Boring, TooShort and Stale
                try:
                    self.redis_conn.delete('analyzer.boring')
                    logger.info('deleted Redis analyzer.boring set to refresh')
                except:
                    logger.info('no Redis set to delete - analyzer.boring')
                try:
                    self.redis_conn.delete('analyzer.too_short')
                    logger.info('deleted Redis analyzer.too_short set to refresh')
                except:
                    logger.info('no Redis set to delete - analyzer.too_short')
                try:
                    # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                    #                   Branch #3262: py3
                    # stale_metrics = list(self.redis_conn.smembers('analyzer.stale'))
                    stale_metrics = list(self.redis_conn_decoded.smembers('analyzer.stale'))
                except:
                    stale_metrics = []
                if stale_metrics:
                    try:
                        self.redis_conn.delete('analyzer.stale')
                        logger.info('deleted Redis analyzer.stale set to refresh')
                    except:
                        logger.info('no Redis set to delete - analyzer.stale')
                # @added 20180807 - Feature #2492: alert on stale metrics
                try:
                    self.redis_conn.delete('analyzer.alert_on_stale_metrics')
                    logger.info('deleted Redis analyzer.alert_on_stale_metrics set to refresh')
                except:
                    logger.info('no Redis set to delete - analyzer.alert_on_stale_metrics')

            # @added 20160922 - Branch #922: Ionosphere
            # Add a Redis set of mirage.unique_metrics
            if settings.ENABLE_MIRAGE:
                # @added 20161229 - Feature #1830: Ionosphere alerts
                # Self manage the Mirage metrics set
                manage_mirage_unique_metrics = True
                manage_mirage_unique_metrics_key = []
                try:
                    manage_mirage_unique_metrics_key = list(self.redis_conn.get('analyzer.manage_mirage_unique_metrics'))
                except Exception as e:
                    if LOCAL_DEBUG:
                        logger.error('error :: could not query Redis for analyzer.manage_mirage_unique_metrics key: %s' % str(e))

                if not manage_mirage_unique_metrics_key:
                    manage_mirage_unique_metrics_key = []

                if len(manage_mirage_unique_metrics_key) > 0:
                    manage_mirage_unique_metrics = False

                mirage_unique_metrics = []
                try:
                    # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                    #                   Branch #3262: py3
                    # mirage_unique_metrics = list(self.redis_conn.smembers('mirage.unique_metrics'))
                    mirage_unique_metrics = list(self.redis_conn_decoded.smembers('mirage.unique_metrics'))
                    if LOCAL_DEBUG:
                        logger.info('debug :: fetched the mirage.unique_metrics Redis set')
                        logger.info('debug :: %s' % str(mirage_unique_metrics))
                except:
                    logger.info('failed to fetch the mirage.unique_metrics Redis set')
                    mirage_unique_metrics = []

                mirage_unique_metrics_count = len(mirage_unique_metrics)
                logger.info('mirage.unique_metrics Redis set count - %s' % str(mirage_unique_metrics_count))

                if manage_mirage_unique_metrics:
                    try:
                        logger.info('deleting Redis mirage.unique_metrics set to refresh')
                        self.redis_conn.delete('mirage.unique_metrics')
                        mirage_unique_metrics = []
                    except:
                        logger.error('error :: could not query Redis to delete mirage.unique_metric set')

                for alert in settings.ALERTS:
                    for metric in unique_metrics:
                        ALERT_MATCH_PATTERN = alert[0]
                        base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
                        METRIC_PATTERN = base_name
                        pattern_match = False
                        matched_by = 'not matched'
                        try:
                            alert_match_pattern = re.compile(ALERT_MATCH_PATTERN)
                            pattern_match = alert_match_pattern.match(METRIC_PATTERN)
                            if pattern_match:
                                matched_by = 'regex'
                                pattern_match = True
                        except:
                            pattern_match = False

                        if not pattern_match:
                            if alert[0] in base_name:
                                pattern_match = True
                                matched_by = 'substring'

                        if not pattern_match:
                            continue

                        mirage_metric = False
                        try:
                            SECOND_ORDER_RESOLUTION_FULL_DURATION = alert[3]
                            mirage_metric = True
                        except:
                            mirage_metric = False

                        if mirage_metric:
                            if metric not in mirage_unique_metrics:
                                try:
                                    self.redis_conn.sadd('mirage.unique_metrics', metric)
                                    if LOCAL_DEBUG:
                                        logger.info('debug :: added %s to mirage.unique_metrics' % metric)
                                except:
                                    if LOCAL_DEBUG:
                                        logger.error('error :: failed to add %s to mirage.unique_metrics set' % metric)

                                try:
                                    key_timestamp = int(time())
                                    self.redis_conn.setex('analyzer.manage_mirage_unique_metrics', 300, key_timestamp)
                                except:
                                    logger.error('error :: failed to set key :: analyzer.manage_mirage_unique_metrics')

                        # Do we use list or dict, which is better performance?
                        # With dict - Use EAFP (easier to ask forgiveness than
                        # permission)
                        try:
                            blah = dict["mykey"]
                            # key exists in dict
                        except:
                            # key doesn't exist in dict
                            blah = False

                # If they were refresh set them again
                if mirage_unique_metrics == []:
                    try:
                        # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                        #                   Branch #3262: py3
                        # mirage_unique_metrics = list(self.redis_conn.smembers('mirage.unique_metrics'))
                        mirage_unique_metrics = list(self.redis_conn_decoded.smembers('mirage.unique_metrics'))
                        mirage_unique_metrics_count = len(mirage_unique_metrics)
                        logger.info('mirage.unique_metrics Redis set count - %s' % str(mirage_unique_metrics_count))
                        if LOCAL_DEBUG:
                            logger.info('debug :: fetched the mirage.unique_metrics Redis set')
                            logger.info('debug :: %s' % str(mirage_unique_metrics))
                    except:
                        logger.info('failed to fetch the mirage.unique_metrics Redis set')
                        mirage_unique_metrics == []
                        mirage_unique_metrics_count = len(mirage_unique_metrics)
            # @modified 20160207 - Branch #922: Ionosphere
            # Handle if Mirage is not enabled
            else:
                mirage_unique_metrics = []
            # END Redis mirage.unique_metrics_set

            if LOCAL_DEBUG:
                try:
                    blah = dict["mykey"]
                    # key exists in dict
                except:
                    # key doesn't exist in dict
                    blah = False
                logger.info('debug :: Memory usage in run after unique_metrics: %s (kb), using blah %s' % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, str(blah)))

            # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # The Redis analyzer.smtp_alerter_metrics list is created here to
            # replace the self.smtp_alerter_metrics Manager.list in the below
            # section
            smtp_alerter_metrics = []
            try:
                # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                #                   Branch #3262: py3
                # smtp_alerter_metrics = list(self.redis_conn.smembers('analyzer.smtp_alerter_metrics'))
                smtp_alerter_metrics = list(self.redis_conn_decoded.smembers('analyzer.smtp_alerter_metrics'))
                if LOCAL_DEBUG:
                    logger.info('debug :: smtp_alerter_metrics :: %s' % (str(smtp_alerter_metrics)))
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to generate a list from the analyzer.smtp_alerter_metrics Redis set')
                smtp_alerter_metrics = []

            if LOCAL_DEBUG:
                logger.info('debug :: unique_metrics :: %s' % (str(unique_metrics)))

            # @added 20170108 - Feature #1830: Ionosphere alerts
            # Adding lists of smtp_alerter_metrics and non_smtp_alerter_metrics
            # Timed this takes 0.013319 seconds on 689 unique_metrics
            for metric_name in unique_metrics:
                # @added 20191014 - Bug #3266: py3 Redis binary objects not strings
                #                   Branch #3262: py3
                if python_version == 3:
                    metric_name = str(metric_name)

                base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                for alert in settings.ALERTS:
                    pattern_match = False
                    if str(alert[1]) == 'smtp':
                        ALERT_MATCH_PATTERN = alert[0]
                        METRIC_PATTERN = base_name
                        pattern_match = False
                        try:
                            # Match by regex
                            alert_match_pattern = re.compile(ALERT_MATCH_PATTERN)
                            pattern_match = alert_match_pattern.match(METRIC_PATTERN)
                            if pattern_match:
                                pattern_match = True
                                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                                # if base_name not in self.smtp_alerter_metrics:
                                #     self.smtp_alerter_metrics.append(base_name)
                                if base_name not in smtp_alerter_metrics:
                                    redis_set = 'analyzer.smtp_alerter_metrics'
                                    data = str(base_name)
                                    try:
                                        self.redis_conn.sadd(redis_set, data)
                                    except:
                                        logger.info(traceback.format_exc())
                                        logger.error('error :: failed to add %s to Redis set %s' % (
                                            str(data), str(redis_set)))
                        except:
                            pattern_match = False

                        if not pattern_match:
                            # Match by substring
                            if alert[0] in base_name:
                                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                                # if base_name not in self.smtp_alerter_metrics:
                                #     self.smtp_alerter_metrics.append(base_name)
                                if base_name not in smtp_alerter_metrics:
                                    redis_set = 'analyzer.smtp_alerter_metrics'
                                    data = str(base_name)
                                    try:
                                        self.redis_conn.sadd(redis_set, data)
                                    except:
                                        logger.info(traceback.format_exc())
                                        logger.error('error :: failed to add %s to Redis set %s' % (
                                            str(data), str(redis_set)))

                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # Use Redis set analyzer.smtp_alerter_metrics in place of
                # self.smtp_alerter_metrics Manager.list
                # if base_name not in self.smtp_alerter_metrics:
                #     if base_name not in self.smtp_alerter_metrics:
                #         self.non_smtp_alerter_metrics.append(base_name)
                present_in_smtp_alerter_metrics = False
                try:
                    present_in_smtp_alerter_metrics = self.redis_conn.sismember('analyzer.smtp_alerter_metrics', base_name)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to determine if %s is in analyzer.smtp_alerter_metrics Redis set' % base_name)
                    present_in_smtp_alerter_metrics = False
                if not present_in_smtp_alerter_metrics:
                    redis_set = 'analyzer.non_smtp_alerter_metrics'
                    data = str(base_name)
                    try:
                        self.redis_conn.sadd(redis_set, data)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to add %s to Redis set %s' % (
                            str(data), str(redis_set)))

            # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # Define variables from the Redis set which has replaced the Manager
            # lists
            smtp_alerter_metrics = []
            try:
                # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                #                   Branch #3262: py3
                # smtp_alerter_metrics = list(self.redis_conn.smembers('analyzer.smtp_alerter_metrics'))
                smtp_alerter_metrics = list(self.redis_conn_decoded.smembers('analyzer.smtp_alerter_metrics'))
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to generate a list from analyzer.smtp_alerter_metrics Redis set')
                smtp_alerter_metrics = []
            try:
                # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                #                   Branch #3262: py3
                # non_smtp_alerter_metrics = list(self.redis_conn.smembers('analyzer.non_smtp_alerter_metrics'))
                non_smtp_alerter_metrics = list(self.redis_conn_decoded.smembers('analyzer.non_smtp_alerter_metrics'))
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to generate a list from analyzer.non_smtp_alerter_metrics Redis set')
                non_smtp_alerter_metrics = []

            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # logger.info('smtp_alerter_metrics     :: %s' % str(len(self.smtp_alerter_metrics)))
            # logger.info('non_smtp_alerter_metrics :: %s' % str(len(self.non_smtp_alerter_metrics)))
            logger.info('smtp_alerter_metrics     :: %s' % str(len(smtp_alerter_metrics)))
            logger.info('non_smtp_alerter_metrics :: %s' % str(len(non_smtp_alerter_metrics)))

            # @added 20180423 - Feature #2360: CORRELATE_ALERTS_ONLY
            #                   Branch #2270: luminosity
            # Add a Redis set of smtp_alerter_metrics for Luminosity to only
            # cross correlate on metrics with an alert setting
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # Use variable from the Redis set which replaced Manager.list
            # for metric in self.smtp_alerter_metrics:
            for metric in smtp_alerter_metrics:
                self.redis_conn.sadd('new_analyzer.smtp_alerter_metrics', metric)
            try:
                self.redis_conn.rename('analyzer.smtp_alerter_metrics', 'analyzer.smtp_alerter_metrics.old')
            except:
                pass
            try:
                self.redis_conn.rename('new_analyzer.smtp_alerter_metrics', 'analyzer.smtp_alerter_metrics')
            except:
                pass
            try:
                self.redis_conn.delete('analyzer.smtp_alerter_metrics.old')
            except:
                pass

            # Using count files rather that multiprocessing.Value to enable metrics for
            # metrics for algorithm run times, etc
            for algorithm in algorithms_to_time:
                algorithm_count_file = algorithm_tmp_file_prefix + algorithm + '.count'
                algorithm_timings_file = algorithm_tmp_file_prefix + algorithm + '.timings'
                # with open(algorithm_count_file, 'a') as f:
                # @modified 20160803 - Adding additional exception handling to Analyzer
                try:
                    with open(algorithm_count_file, 'w') as f:
                        pass
                except:
                    logger.error('error :: could not create file %s' % algorithm_count_file)
                    logger.info(traceback.format_exc())

                try:
                    with open(algorithm_timings_file, 'w') as f:
                        pass
                except:
                    logger.error('error :: could not create file %s' % algorithm_timings_file)
                    logger.info(traceback.format_exc())

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run before removing algorithm_count_files and algorithm_timings_files: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Remove any existing algorithm.error files from any previous runs
            # that did not cleanup for any reason
            pattern = '%s.*.algorithm.error' % skyline_app
            try:
                for f in os.listdir(settings.SKYLINE_TMP_DIR):
                    if re.search(pattern, f):
                        try:
                            os.remove(os.path.join(settings.SKYLINE_TMP_DIR, f))
                            logger.info('cleaning up old error file - %s' % (str(f)))
                        except OSError:
                            pass
            except:
                logger.error('error :: failed to cleanup algorithm.error files')
                logger.info(traceback.format_exc())

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run before spawning processes: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # @added 20191107 - Feature #3306: Record anomaly_end_timestamp
            #                   Branch #3262: py3
            # Created an Analyzer Redis set to keep track of not anomalous
            # metrics so that the anomaly_end_timestamp can be recorded
            try:
                self.redis_conn.delete('aet.analyzer.not_anomalous_metrics')
            except:
                pass
            try:
                self.redis_conn.rename('analyzer.not_anomalous_metrics', 'aet.analyzer.not_anomalous_metrics')
            except:
                pass

            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            for i in range(1, settings.ANALYZER_PROCESSES + 1):
                if i > len(unique_metrics):
                    logger.info('WARNING: skyline is set for more cores than needed.')
                    break

                try:
                    p = Process(target=self.spin_process, args=(i, unique_metrics))
                    pids.append(p)
                    pid_count += 1
                    logger.info('starting %s of %s spin_process/es' % (str(pid_count), str(settings.ANALYZER_PROCESSES)))
                    p.start()
                    spawned_pids.append(p.pid)
                except:
                    logger.error('error :: failed to spawn process')
                    logger.info(traceback.format_exc())

            # Send wait signal to zombie processes
            # for p in pids:
            #     p.join()
            # Self monitor processes and terminate if any spin_process has run
            # for longer than 180 seconds - 20160512 @earthgecko
            p_starts = time()
            # TESTING p.join removal
            # while time() - p_starts <= 1:
            while time() - p_starts <= settings.MAX_ANALYZER_PROCESS_RUNTIME:
                if any(p.is_alive() for p in pids):
                    # Just to avoid hogging the CPU
                    sleep(.1)
                else:
                    # All the processes are done, break now.
                    time_to_run = time() - p_starts
                    logger.info('%s :: %s spin_process/es completed in %.2f seconds' % (skyline_app, str(settings.ANALYZER_PROCESSES), time_to_run))
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('%s :: timed out, killing all spin_process processes' % (skyline_app))
                for p in pids:
                    logger.info('%s :: killing spin_process process' % (skyline_app))
                    p.terminate()
                    # p.join()
                    logger.info('%s :: killed spin_process process' % (skyline_app))

            for p in pids:
                if p.is_alive():
                    logger.info('%s :: stopping spin_process - %s' % (skyline_app, str(p.is_alive())))
                    p.join()

            # Log the last reported error by any algorithms that errored in the
            # spawned processes from algorithms.py
            for completed_pid in spawned_pids:
                logger.info('spin_process with pid %s completed' % (str(completed_pid)))
                for algorithm in settings.ALGORITHMS:
                    algorithm_error_file = '%s/%s.%s.%s.algorithm.error' % (
                        settings.SKYLINE_TMP_DIR, skyline_app,
                        str(completed_pid), algorithm)
                    if os.path.isfile(algorithm_error_file):
                        logger.info(
                            'error :: spin_process with pid %s has reported an error with the %s algorithm' % (
                                str(completed_pid), algorithm))
                        try:
                            with open(algorithm_error_file, 'r') as f:
                                error_string = f.read()
                            logger.error('%s' % str(error_string))
                        except:
                            logger.error('error :: failed to read %s error file' % algorithm)
                        try:
                            os.remove(algorithm_error_file)
                        except OSError:
                            pass

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage after spin_process spawned processes finish: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Grab data from the queue and populate dictionaries
            exceptions = dict()
            anomaly_breakdown = dict()
            while 1:
                try:
                    key, value = self.anomaly_breakdown_q.get_nowait()
                    if key not in anomaly_breakdown.keys():
                        anomaly_breakdown[key] = value
                    else:
                        anomaly_breakdown[key] += value
                except Empty:
                    break

            while 1:
                try:
                    key, value = self.exceptions_q.get_nowait()
                    if key not in exceptions.keys():
                        exceptions[key] = value
                    else:
                        exceptions[key] += value
                except Empty:
                    break

            # @added 20191021 - Bug #3288: Always send anomaly_breakdown and exception metrics
            #                   Branch #3262: py3
            exceptions_metrics = ['Boring', 'Stale', 'TooShort', 'Other']
            for i_exception in exceptions_metrics:
                if i_exception not in exceptions.keys():
                    exceptions[i_exception] = 0
            for i_anomaly_breakdown in settings.ALGORITHMS:
                if i_anomaly_breakdown not in anomaly_breakdown.keys():
                    anomaly_breakdown[i_anomaly_breakdown] = 0

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run before alerts: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # @added 20161228 - Feature #1830: Ionosphere alerts
            #                   Branch #922: Ionosphere
            # Bringing Ionosphere online - do not alert on Ionosphere metrics
            try:
                # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                #                   Branch #3262: py3
                # ionosphere_unique_metrics = list(self.redis_conn.smembers('ionosphere.unique_metrics'))
                ionosphere_unique_metrics = list(self.redis_conn_decoded.smembers('ionosphere.unique_metrics'))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get ionosphere.unique_metrics from Redis')
                ionosphere_unique_metrics = []

            ionosphere_unique_metrics_count = len(ionosphere_unique_metrics)
            logger.info('ionosphere.unique_metrics Redis set count - %s' % str(ionosphere_unique_metrics_count))

            # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # Use Redis set instead of Manager() list
            analyzer_anomalous_metrics = []
            try:
                # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                #                   Branch #3262: py3
                # literal_analyzer_anomalous_metrics = list(self.redis_conn.smembers('analyzer.anomalous_metrics'))
                literal_analyzer_anomalous_metrics = list(self.redis_conn_decoded.smembers('analyzer.anomalous_metrics'))
                if LOCAL_DEBUG:
                    if literal_analyzer_anomalous_metrics:
                        logger.info('debug :: analyzer.anomalous_metrics Redis set surfaced - %s' % str(literal_analyzer_anomalous_metrics))
                    else:
                        logger.info('debug :: analyzer.anomalous_metrics no Redis set data')
                for metric_list_string in literal_analyzer_anomalous_metrics:
                    metric = literal_eval(metric_list_string)
                    analyzer_anomalous_metrics.append(metric)
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to generate list from Redis set analyzer.anomalous_metrics')

            # @added 20161229 - Feature #1830: Ionosphere alerts
            # Determine if Ionosphere added any alerts to be sent
            try:
                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # for metric in self.anomalous_metrics:
                for metric in analyzer_anomalous_metrics:
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # self.all_anomalous_metrics.append(metric)
                    redis_set = 'analyzer.all_anomalous_metrics'
                    data = str(metric)
                    try:
                        self.redis_conn.sadd(redis_set, data)
                        if LOCAL_DEBUG:
                            logger.info('debug :: added %s to %s Redis set' % (
                                str(data), redis_set))
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to add %s to Redis set %s' % (
                            str(data), str(redis_set)))
            except:
                logger.error(traceback.format_exc())
                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # logger.error('error :: failed to add to self.all_anomalous_metrics')
                logger.error('error :: failed to add to analyzer.all_anomalous_metrics Redis set')
            ionosphere_alerts = []
            context = 'Analyzer'
            try:
                # @modified 20200430 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # ionosphere_alerts = list(self.redis_conn.scan_iter(match='ionosphere.analyzer.alert.*'))
                ionosphere_alerts = list(self.redis_conn_decoded.scan_iter(match='ionosphere.analyzer.alert.*'))
                if LOCAL_DEBUG:
                    logger.info('debug :: ionosphere.analyzer.alert.* Redis keys - %s' % (
                        str(ionosphere_alerts)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to scan ionosphere.analyzer.alert.* from Redis')

            if not ionosphere_alerts:
                ionosphere_alerts = []

            # @added 20180914 - Bug #2594: Analyzer Ionosphere alert on Analyzer data point
            ionosphere_anomalous_metrics = []

            ionosphere_metric_alerts = []
            if len(ionosphere_alerts) != 0:
                logger.info('Ionosphere alert/s requested :: %s' % str(ionosphere_alerts))
                for cache_key in ionosphere_alerts:
                    try:
                        alert_on = self.redis_conn.get(cache_key)
                        send_alert_for = literal_eval(alert_on)
                        value = float(send_alert_for[0])
                        base_name = str(send_alert_for[1])
                        metric_timestamp = int(float(send_alert_for[2]))
                        triggered_algorithms = send_alert_for[3]
                        anomalous_metric = [value, base_name, metric_timestamp]
                        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        # self.all_anomalous_metrics.append(anomalous_metric)
                        redis_set = 'analyzer.all_anomalous_metrics'
                        data = str(anomalous_metric)
                        try:
                            self.redis_conn.sadd(redis_set, data)
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: failed to add %s to Redis set %s' % (
                                str(data), str(redis_set)))

                        # @added 20180914 - Bug #2594: Analyzer Ionosphere alert on Analyzer data point
                        ionosphere_anomalous_metrics.append(anomalous_metric)

                        for algorithm in triggered_algorithms:
                            key = algorithm
                            if key not in anomaly_breakdown.keys():
                                anomaly_breakdown[key] = 1
                            else:
                                anomaly_breakdown[key] += 1
                        self.redis_conn.delete(cache_key)
                        logger.info('alerting for Ionosphere on %s' % base_name)
                        ionosphere_metric_alerts.append(base_name)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add an Ionosphere anomalous_metric for %s' % cache_key)

            # @added 20200412 - Feature #3486: analyzer_batch
            #                   Feature #3480: batch_processing
            analyzer_batch_alerts = []
            if BATCH_PROCESSING:
                context = 'Analyzer'
                try:
                    # @modified 20200415 - Feature #3486: analyzer_batch
                    # analyzer_batch_alerts = list(self.redis_conn.scan_iter(match='analyzer_batch.alert.*'))
                    analyzer_batch_alerts = list(self.redis_conn_decoded.scan_iter(match='analyzer_batch.alert.*'))
                    if LOCAL_DEBUG:
                        logger.info('debug :: analyzer_batch.alert.* Redis keys - %s' % (
                            str(analyzer_batch_alerts)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to scan analyzer_batch.alert.* from Redis')
                if not analyzer_batch_alerts:
                    analyzer_batch_alerts = []
                if len(analyzer_batch_alerts) != 0:
                    logger.info('analyzer_batch alert/s requested :: %s' % str(analyzer_batch_alerts))
                    for cache_key in analyzer_batch_alerts:
                        try:
                            # @modified 20200415 - Feature #3486: analyzer_batch
                            # alert_on = self.redis_conn.get(cache_key)
                            alert_on = self.redis_conn_decoded.get(cache_key)
                            send_alert_for = literal_eval(alert_on)
                            value = float(send_alert_for[0])
                            base_name = str(send_alert_for[1])
                            metric_timestamp = int(float(send_alert_for[2]))
                            triggered_algorithms = send_alert_for[3]
                            anomalous_metric = [value, base_name, metric_timestamp]
                            redis_set = 'analyzer.all_anomalous_metrics'
                            data = str(anomalous_metric)
                            try:
                                self.redis_conn.sadd(redis_set, data)
                            except:
                                logger.info(traceback.format_exc())
                                logger.error('error :: failed to add %s to Redis set %s for analyzer_batch' % (
                                    str(data), str(redis_set)))
                            for algorithm in triggered_algorithms:
                                key = algorithm
                                if key not in anomaly_breakdown.keys():
                                    anomaly_breakdown[key] = 1
                                else:
                                    anomaly_breakdown[key] += 1
                            self.redis_conn.delete(cache_key)
                            logger.info('alerting for analyzer_batch on %s' % base_name)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to add an analyzer_batch anomalous_metric for %s' % str(cache_key))

            # Send alerts
            if settings.ENABLE_ALERTS:
                for alert in settings.ALERTS:
                    # @modified 20161229 - Feature #1830: Ionosphere alerts
                    # Handle alerting for Ionosphere
                    # for metric in self.anomalous_metrics:
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # for metric in self.all_anomalous_metrics:
                    # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                    #                   Branch #3262: py3
                    # all_anomalous_metrics = list(self.redis_conn.smembers('analyzer.all_anomalous_metrics'))
                    all_anomalous_metrics = list(self.redis_conn_decoded.smembers('analyzer.all_anomalous_metrics'))
                    if LOCAL_DEBUG:
                        logger.info('debug :: for alert in settings.ALERTS analyzer.all_anomalous_metrics - %s' % (
                            str(all_anomalous_metrics)))

                    for metric_list_string in all_anomalous_metrics:
                        metric = literal_eval(metric_list_string)
                        if LOCAL_DEBUG:
                            logger.info('debug :: metric in all_anomalous_metrics - %s' % (
                                str(metric)))

                        # @added 20180914 - Bug #2594: Analyzer Ionosphere alert on Analyzer data point
                        # Set each metric to the default Analyzer context
                        context = 'Analyzer'

                        pattern_match = False
                        # Absolute match
                        if str(metric[1]) == str(alert[0]):
                            pattern_match = True
                            if LOCAL_DEBUG:
                                logger.debug('debug :: metric absolutely pattern matched - %s %s' % (
                                    str(metric[1]), str(alert[0])))
                            # @added 20191021 - Bug #3266: py3 Redis binary objects not strings
                            #                   Branch #3262: py3
                            matched_by = 'absolutely'

                        if not pattern_match:
                            if LOCAL_DEBUG:
                                logger.debug('debug :: metric not absolutely pattern matched - %s' % (
                                    str(metric[1])))

                            ALERT_MATCH_PATTERN = alert[0]
                            METRIC_PATTERN = metric[1]
                            matched_by = 'not matched'
                            try:
                                alert_match_pattern = re.compile(ALERT_MATCH_PATTERN)
                                pattern_match = alert_match_pattern.match(METRIC_PATTERN)
                                # if LOCAL_DEBUG:
                                #     logger.info(
                                #         'debug :: regex alert pattern matched :: %s %s' %
                                #         (alert[0], metric[1]))
                                matched_by = 'regex'
                            except:
                                pattern_match = False
                        # @modified 20160806 - Reintroduced the original
                        # substring matching after wildcard matching, to allow
                        # more flexibility
                        if not pattern_match:
                            if alert[0] in metric[1]:
                                pattern_match = True
                                # if LOCAL_DEBUG:
                                #     logger.info(
                                #         'debug :: substring alert pattern matched :: %s %s' %
                                #         (alert[0], metric[1]))
                                matched_by = 'substring'

                        if not pattern_match:
                            if LOCAL_DEBUG:
                                logger.debug('debug :: metric not pattern matched, continuing - %s' % (
                                    str(metric[1])))
                            continue

                        mirage_metric = False
                        analyzer_metric = True
                        cache_key = 'last_alert.%s.%s' % (alert[1], metric[1])
                        if LOCAL_DEBUG:
                            logger.debug('debug :: last_alert cache key %s' % (
                                str(cache_key)))

                        if settings.ENABLE_MIRAGE:
                            try:
                                # @modified 20181023 - Feature #2618: alert_slack
                                # SECOND_ORDER_RESOLUTION_FULL_DURATION = alert[3]
                                SECOND_ORDER_RESOLUTION_FULL_DURATION = int(alert[3])
                                mirage_metric = True
                                analyzer_metric = False
                                logger.info(
                                    'mirage check :: %s at %s hours - matched by %s' %
                                    (metric[1],
                                        str(SECOND_ORDER_RESOLUTION_FULL_DURATION),
                                        matched_by))
                                context = 'Mirage'
                            except:
                                mirage_metric = False
                                if LOCAL_DEBUG:
                                    logger.debug('debug :: not Mirage metric - %s' % metric[1])

                        if mirage_metric:
                            # Write anomalous metric to test at second
                            # order resolution by Mirage to the check
                            # file
                            if LOCAL_DEBUG:
                                logger.info(
                                    'debug :: Memory usage in run before writing mirage check file: %s (kb)' %
                                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                            # @added 20160815 - Analyzer also alerting on Mirage metrics now #22
                            # With the reintroduction of the original substring
                            # matching it become evident that Analyzer could
                            # alert on a Mirage metric via a match on a parent
                            # namespace pattern later in the ALERTS tuples.
                            # If it is a Mirage metric, we add a mirage.metrics
                            # key for Analyzer to check in the analyzer_metric
                            # step below.  This allows us to wildcard and
                            # substring match, but the mirage.metrics keys act
                            # as a dynamic SKIP_LIST for Analyzer
                            mirage_metric_cache_key = 'mirage.metrics.%s' % metric[1]
                            try:
                                self.redis_conn.setex(mirage_metric_cache_key, alert[2], packb(metric[0]))
                            except:
                                logger.info(traceback.format_exc())
                                logger.error('error :: failed to add mirage.metrics Redis key - %s' % str(mirage_metric_cache_key))

                            # metric_timestamp = int(time())
                            # anomaly_check_file = '%s/%s.%s.txt' % (settings.MIRAGE_CHECK_PATH, metric_timestamp, metric[1])
                            # @added 20170206 - Bug #1904: Handle non filesystem friendly metric names in check files
                            anomaly_check_file = None
                            try:
                                sane_metricname = filesafe_metricname(str(metric[1]))
                                anomaly_check_file = '%s/%s.%s.txt' % (settings.MIRAGE_CHECK_PATH, str(int(metric[2])), sane_metricname)
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to determine anomaly_check_file')

                            if anomaly_check_file:
                                anomaly_check_file_created = False
                                try:
                                    # @added 20190410 - Feature #2882: Mirage - periodic_check
                                    # Allow for even non Mirage metrics to have
                                    # periodic checks done.  If the Mirage
                                    # SECOND_ORDER_RESOLUTION_HOURS are not set
                                    # in the alert tuple default to 7 days, 168
                                    # hours
                                    try:
                                        use_hours_to_resolve = int(alert[3])
                                    except:
                                        use_hours_to_resolve = 168

                                    with open(anomaly_check_file, 'w') as fh:
                                        # metric_name, anomalous datapoint, hours to resolve, timestamp
                                        # @modified 20190410 - Feature #2882: Mirage - periodic_check
                                        # fh.write('metric = "%s"\nvalue = "%s"\nhours_to_resolve = "%s"\nmetric_timestamp = "%s"\n' % (metric[1], metric[0], alert[3], str(metric[2])))
                                        fh.write('metric = "%s"\nvalue = "%s"\nhours_to_resolve = "%s"\nmetric_timestamp = "%s"\n' % (metric[1], metric[0], str(use_hours_to_resolve), str(metric[2])))
                                    anomaly_check_file_created = True
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to write anomaly_check_file')
                                if LOCAL_DEBUG:
                                    logger.info(
                                        'debug :: Memory usage in run after writing mirage check file: %s (kb)' %
                                        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

                                if anomaly_check_file_created:
                                    if python_version == 2:
                                        # @modified 20190130 - Task #2690: Test Skyline on Python-3.6.7
                                        #                      Task #2828: Skyline - Python 3.7
                                        #                      Branch #3262: py3
                                        # os.chmod(anomaly_check_file, 0644)
                                        os.chmod(anomaly_check_file, 0o644)
                                    if python_version == 3:
                                        os.chmod(anomaly_check_file, mode=0o644)
                                    if LOCAL_DEBUG:
                                        logger.info(
                                            'debug :: Memory usage in run after chmod mirage check file: %s (kb)' %
                                            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

                            logger.info('added mirage check :: %s,%s,%s' % (metric[1], metric[0], alert[3]))
                            try:
                                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                                #                      Task #3032: Debug number of Python processes and memory use
                                #                      Branch #3002: docker
                                # Replace Manager.list instances with Redis sets
                                # self.sent_to_mirage.append(metric[1])
                                redis_set = 'analyzer.sent_to_mirage'
                                data = str(metric[1])
                                try:
                                    self.redis_conn.sadd(redis_set, data)
                                except:
                                    logger.info(traceback.format_exc())
                                    logger.error('error :: failed to add %s to Redis set %s' % (
                                        str(data), str(redis_set)))
                            except:
                                logger.info(traceback.format_exc())
                                # logger.error(
                                #     'error :: failed add to self.sent_to_mirage.append with %s' %
                                #     metric[1])
                                logger.error(
                                    'error :: failed add %s to analyzer.sent_to_mirage Redis set' %
                                    metric[1])

                            # Alert for analyzer if enabled
                            # This sends an Analyzer ALERT for every check that
                            # is sent to Mirage.
                            if settings.ENABLE_FULL_DURATION_ALERTS:

                                # @modified 20200413 - Feature #3486: analyzer_batch
                                #                      Feature #3480: batch_processing
                                # Change the last_alert cache key to hold the
                                # the anomaly timestamp which which the alert
                                # was sent, not the packb anomaly value, which
                                # having reviewed the code is not used or called
                                # anywhere.  The packb of the last_alert cache
                                # key is from the original Etsy code, using the
                                # timestamp of the anomaly allows it to be used
                                # to determine if a batch anomaly should be
                                # alerted on based on the comparison of the
                                # timestamps rather than just the last_alert
                                # key as analyzer_batch could send multiple
                                # anomalies in one batch that might be
                                # EXPIRATION_TIME apart.
                                # self.redis_conn.setex(cache_key, alert[2], packb(metric[0]))
                                self.redis_conn.setex(cache_key, alert[2], int(metric[2]))
                                logger.info('triggering alert ENABLE_FULL_DURATION_ALERTS :: %s %s via %s' % (metric[1], metric[0], alert[1]))
                                try:
                                    if alert[1] != 'smtp':
                                        trigger_alert(alert, metric, context)
                                    else:
                                        smtp_trigger_alert(alert, metric, context)
                                except:
                                    logger.error(
                                        'error :: failed to trigger_alert ENABLE_FULL_DURATION_ALERTS :: %s %s via %s' %
                                        (metric[1], metric[0], alert[1]))
                            else:
                                if LOCAL_DEBUG:
                                    logger.info('debug :: ENABLE_FULL_DURATION_ALERTS not enabled')
                            continue

                        # @added 20161228 - Feature #1830: Ionosphere alerts
                        #                   Branch #922: Ionosphere
                        # Bringing Ionosphere online - do alert on Ionosphere
                        # metrics if Ionosphere is up
                        metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(metric[1]))
                        if LOCAL_DEBUG:
                            logger.debug('debug :: metric_name - %s' % str(metric_name))

                        if metric_name in ionosphere_unique_metrics:
                            ionosphere_up = False
                            try:
                                ionosphere_up = self.redis_conn.get('ionosphere')
                            except Exception as e:
                                logger.error('error :: could not query Redis for ionosphere key: %s' % str(e))
                            if ionosphere_up:
                                # @modified 20161229 - Feature #1830: Ionosphere alerts
                                # Do alert if Ionosphere created a Redis
                                # ionosphere.analyzer.alert key
                                if str(metric[1]) in ionosphere_metric_alerts:
                                    logger.info('alerting as in ionosphere_alerts - Ionosphere metric - %s' % str(metric[1]))
                                    context = 'Ionosphere'
                                else:
                                    logger.info('not alerting - Ionosphere metric - %s' % str(metric[1]))
                                    continue
                            else:
                                logger.error('error :: Ionosphere not report up')
                                logger.info('taking over alerting from Ionosphere if alert is matched on - %s' % str(metric[1]))
                        else:
                            if LOCAL_DEBUG:
                                logger.debug('debug :: metric_name not in ionosphere_unique_metrics - %s' % str(metric_name))

                        # @added 20161231 - Feature #1830: Ionosphere alerts
                        #                   Analyzer also alerting on Mirage metrics now #22
                        # Do not alert on Mirage metrics
                        if metric_name in mirage_unique_metrics:
                            logger.info('not alerting - skipping Mirage metric - %s' % str(metric[1]))
                            continue

                        if analyzer_metric:
                            if LOCAL_DEBUG:
                                logger.debug('debug :: metric_name is analyzer_metric - %s' % str(analyzer_metric))
                            # @added 20160815 - Analyzer also alerting on Mirage metrics now #22
                            # If the metric has a dynamic mirage.metrics key,
                            # skip it
                            mirage_metric_cache_key = 'mirage.metrics.%s' % metric[1]
                            mirage_metric_key = False
                            if settings.ENABLE_MIRAGE:
                                try:
                                    mirage_metric_key = self.redis_conn.get(mirage_metric_cache_key)
                                    if LOCAL_DEBUG:
                                        logger.debug('debug :: metric_name analyzer_metric mirage_metric_key - %s' % str(mirage_metric_key))

                                except Exception as e:
                                    logger.error('error :: could not query Redis for mirage_metric_cache_key: %s' % e)

                            if mirage_metric_key:
                                if LOCAL_DEBUG:
                                    logger.info('debug :: mirage metric key exists, skipping %s' % metric[1])
                                continue

                            try:
                                last_alert = self.redis_conn.get(cache_key)
                                if LOCAL_DEBUG:
                                    logger.debug('debug :: metric_name analyzer_metric last_alert - %s' % str(last_alert))
                            except Exception as e:
                                logger.error('error :: could not query Redis for cache_key: %s' % e)
                                continue

                            # @added 20200413 - Feature #3486: analyzer_batch
                            #                   Feature #3480: batch_processing
                            # Do not evaluate batch metrics against the presence
                            # of the last_alert cache key, but against the
                            # timestamp of the anomaly as reported by the key
                            if BATCH_PROCESSING and last_alert:
                                # Is this a analyzer_batch related anomaly
                                analyzer_batch_anomaly = None
                                analyzer_batch_metric_anomaly_key = 'analyzer_batch.anomaly.%s.%s' % (
                                    str(metric_timestamp), metric)
                                try:
                                    analyzer_batch_anomaly = self.redis_conn.get(analyzer_batch_metric_anomaly_key)
                                except Exception as e:
                                    logger.error(
                                        'error :: could not query cache_key - %s - %s' % (
                                            analyzer_batch_metric_anomaly_key, e))
                                    analyzer_batch_anomaly = None
                                if analyzer_batch_anomaly:
                                    logger.info('batch processing - identified as an analyzer_batch triggered anomaly from the presence of the Redis key %s' % analyzer_batch_metric_anomaly_key)
                                else:
                                    logger.info('batch processing - not identified as an analyzer_batch triggered anomaly as no Redis key found - %s' % analyzer_batch_metric_anomaly_key)
                                if analyzer_batch_anomaly:
                                    seconds_between_batch_anomalies = None
                                    try:
                                        seconds_between_batch_anomalies = int(metric[2]) - int(last_alert)
                                    except:
                                        logger.info(traceback.format_exc())
                                        logger.error('error :: failed to calculate number of seconds between last_alert anomalyy timestamp and current batch anomaly timestamp')
                                    if seconds_between_batch_anomalies == 0:
                                        seconds_between_batch_anomalies = 1
                                    if seconds_between_batch_anomalies > int(alert[2]):
                                        logger.info('batch processing - setting last_alert to None for %s, so Analyzer will alert on this anomaly which occurred %s seconds after the last anomaly alerted on' % (
                                            metric, str(seconds_between_batch_anomalies)))
                                        last_alert = None

                            if last_alert:
                                if str(metric[1]) in ionosphere_metric_alerts:
                                    logger.info('not alerting on ionosphere_alerts Ionosphere metric - last_alert key exists - %s' % str(metric[1]))
                                    logger.info('so alert resources will not be created for this ionosphere_alerts Ionosphere metric - %s' % str(metric[1]))
                                continue

                            # @added 20180914 - Bug #2594: Analyzer Ionosphere alert on Analyzer data point
                            # Due to ionosphere_alerts being added to the
                            # self.all_anomalous_metrics after Analyzer metrics
                            # here we need to ensure that we only alert on the
                            # last item for the metric in the list so that the
                            # alert is not sent out with any current
                            # anomaly data from the current Analyzer run, but
                            # with the data from the ionosphere_alerts item.
                            if context == 'Ionosphere':
                                for check_metric in ionosphere_anomalous_metrics:
                                    if metric[1] == check_metric[1]:
                                        if metric[2] != check_metric[2]:
                                            # If the timestamps do not match
                                            # then it is the list item from
                                            # the Analyzer anomaly, not the
                                            # Ionosphere list item
                                            continue

                            try:
                                # @modified 20191021 - Bug #3266: py3 Redis binary objects not strings
                                #                      Branch #3262: py3
                                # self.redis_conn.setex(cache_key, alert[2], packb(metric[0]))
                                if python_version == 2:
                                    # @modified 20200413 - Feature #3486: analyzer_batch
                                    #                      Feature #3480: batch_processing
                                    # Change the last_alert cache key to hold the
                                    # the anomaly timestamp for which the alert
                                    # was sent, not the packb anomaly value, which
                                    # having reviewed the code is not used or called
                                    # anywhere.  The packb of the last_alert cache
                                    # key is from the original Etsy code, using the
                                    # timestamp of the anomaly allows it to be used
                                    # to determine if a batch anomaly should be
                                    # alerted on based on the comparison of the
                                    # timestamps rather than just the last_alert
                                    # key as analyzer_batch could send multiple
                                    # anomalies in one batch that might be
                                    # EXPIRATION_TIME apart.
                                    # key_set = self.redis_conn.setex(cache_key, alert[2], packb(metric[0]))
                                    key_set = self.redis_conn.setex(cache_key, alert[2], int(metric[2]))
                                if python_version == 3:
                                    # key_set = self.redis_conn.setex(cache_key, str(alert[2]), packb(str(metric[0])))
                                    key_set = self.redis_conn.setex(cache_key, str(alert[2]), int(metric[2]))
                                if LOCAL_DEBUG:
                                    logger.debug('debug :: metric_name analyzer_metric cache_key setex - %s, %s, %s, %s' % (
                                        str(cache_key), str(alert[2]),
                                        str(metric[0]), str(key_set)))
                            except:
                                # logger.error('error :: failed to set alert cache key :: %s' % (cache_key))
                                logger.error(traceback.format_exc())
                                logger.error('error :: metric_name analyzer_metric cache_key setex - %s, %s, %s, %s' % (
                                    str(cache_key), str(alert[2]),
                                    str(metric[0]), str(key_set)))
                            try:
                                logger.info(
                                    'triggering alert :: %s %s via %s - matched by %s' %
                                    # @modified 20191021 - Bug #3266: py3 Redis binary objects not strings
                                    #                      Branch #3262: py3
                                    # (metric[1], metric[0], alert[1], matched_by))
                                    (str(metric[1]), str(metric[0]), str(alert[1]),
                                        str(matched_by)))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metric_name analyzer_metric triggering alert')
                            if LOCAL_DEBUG:
                                logger.info(
                                    'debug :: Memory usage in run before triggering alert: %s (kb)' %
                                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                            try:
                                if alert[1] != 'smtp':
                                    trigger_alert(alert, metric, context)
                                    logger.info('trigger_alert :: alert: %s, metric: %s, context: %s' % (
                                        str(alert), str(metric), str(context)))
                                else:
                                    smtp_trigger_alert(alert, metric, context)
                                    logger.info('smtp_trigger_alert :: alert: %s, metric: %s, context: %s' % (
                                        str(alert), str(metric), str(context)))
                                    if LOCAL_DEBUG:
                                        logger.info('debug :: smtp_trigger_alert spawned')
                                if LOCAL_DEBUG:
                                    logger.info(
                                        'debug :: Memory usage in run after triggering alert: %s (kb)' %
                                        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                            except:
                                logger.error('error :: trigger_alert - %s' % traceback.format_exc())
                                logger.error(
                                    'error :: failed to trigger_alert :: %s %s via %s' %
                                    (metric[1], metric[0], alert[1]))
            # @modified 20160207 - Branch #922: Ionosphere
            # Handle if alerts are not enabled
            else:
                logger.info('alerts not enabled')

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run after alerts: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Write anomalous_metrics to static webapp directory
            # @modified 20160818 - Issue #19: Add additional exception handling to Analyzer
            anomalous_metrics_list_len = False
            real_anomalous_metrics_count = 0
            try:
                # @modified 20190408 - Feature #2882: Mirage - periodic_check
                # Do not count Mirage periodic checks
                # len(self.anomalous_metrics)
                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # len(self.real_anomalous_metrics)
                # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # real_anomalous_metrics_count = len(self.redis_conn.smembers('analyzer.real_anomalous_metrics'))
                real_anomalous_metrics_count = len(self.redis_conn_decoded.smembers('analyzer.real_anomalous_metrics'))
                anomalous_metrics_list_len = True
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to determine length of real_anomalous_metrics list')

            if anomalous_metrics_list_len:
                # @modified 20190408 - Feature #2882: Mirage - periodic_check
                # Do not count Mirage periodic checks
                # if len(self.anomalous_metrics) > 0:
                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # if len(self.real_anomalous_metrics) > 0:
                if real_anomalous_metrics_count > 0:
                    filename = path.abspath(path.join(path.dirname(__file__), '..', settings.ANOMALY_DUMP))
                    try:
                        with open(filename, 'w') as fh:
                            # Make it JSONP with a handle_data() function
                            # @modified 20190408 - Feature #2882: Mirage - periodic_check
                            # Do not count Mirage periodic checks
                            # anomalous_metrics = list(self.anomalous_metrics)
                            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                            # anomalous_metrics = list(self.real_anomalous_metrics)
                            real_anomalous_metrics = []
                            try:
                                # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                                #                      Branch #3262: py3
                                # literal_real_anomalous_metrics = list(self.redis_conn.smembers('analyzer.real_anomalous_metrics'))
                                literal_real_anomalous_metrics = list(self.redis_conn_decoded.smembers('analyzer.real_anomalous_metrics'))
                                for metric_list_string in literal_real_anomalous_metrics:
                                    metric = literal_eval(metric_list_string)
                                    real_anomalous_metrics.append(metric)
                            except:
                                logger.info(traceback.format_exc())
                                logger.error('error :: failed to generate list from Redis set analyzer.real_anomalous_metrics')
                            real_anomalous_metrics.sort(key=operator.itemgetter(1))
                            fh.write('handle_data(%s)' % real_anomalous_metrics)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error(
                            'error :: failed to write anomalies to %s' %
                            str(filename))

            # @added 20191105 - Branch #3262: py3
            if os.path.isfile(alert_test_file):
                test_alert = None
                try:
                    with open((alert_test_file), 'r') as f:
                        raw_test_alert = f.read()
                    test_alert = literal_eval(raw_test_alert)
                    # [metric, alerter]
                    # e.g. ['server-1.cpu.user', 'smtp']
                    # e.g. ['server-1.cpu.user', 'slack']
                    # e.g. ['skyline_test.alerters.test', 'smtp']
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: could not evaluate test_alert from %s' % alert_test_file)
                if test_alert:
                    try:
                        logger.info('test alert metric found - alerting on %s' % str((test_alert)))
                        metric_name = str(test_alert[0])
                        test_alerter = str(test_alert[1])
                        metric = (1, metric_name, int(time()))
                        alert = (metric_name, test_alerter, 10)
                        if settings.SLACK_ENABLED and test_alerter == 'slack':
                            logger.info('test alert to slack for %s' % (metric_name))
                            trigger_alert(alert, metric, 'analyzer')
                        if test_alerter == 'smtp':
                            smtp_trigger_alert(alert, metric, 'analyzer')
                    except:
                        logger.error('error :: test trigger_alert - %s' % traceback.format_exc())
                        logger.error('error :: failed to test trigger_alert :: %s' % metric_name)
                try:
                    os.remove(alert_test_file)
                except OSError:
                    logger.error('error - failed to remove %s, continuing' % alert_test_file)
                    pass

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run before algorithm test run times: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            for algorithm in algorithms_to_time:
                algorithm_count_file = algorithm_tmp_file_prefix + algorithm + '.count'
                algorithm_timings_file = algorithm_tmp_file_prefix + algorithm + '.timings'

                try:
                    algorithm_count_array = []
                    with open(algorithm_count_file, 'r') as f:
                        for line in f:
                            value_string = line.replace('\n', '')
                            unquoted_value_string = value_string.replace("'", '')
                            float_value = float(unquoted_value_string)
                            algorithm_count_array.append(float_value)
                except:
                    algorithm_count_array = False

                if not algorithm_count_array:
                    continue

                number_of_times_algorithm_run = len(algorithm_count_array)
                logger.info(
                    'algorithm run count - %s run %s times' % (
                        algorithm, str(number_of_times_algorithm_run)))
                if number_of_times_algorithm_run == 0:
                    continue

                try:
                    algorithm_timings_array = []
                    with open(algorithm_timings_file, 'r') as f:
                        for line in f:
                            value_string = line.replace('\n', '')
                            unquoted_value_string = value_string.replace("'", '')
                            float_value = float(unquoted_value_string)
                            algorithm_timings_array.append(float_value)
                except:
                    algorithm_timings_array = False

                if not algorithm_timings_array:
                    continue

                number_of_algorithm_timings = len(algorithm_timings_array)
                logger.info(
                    'algorithm timings count - %s has %s timings' % (
                        algorithm, str(number_of_algorithm_timings)))

                if number_of_algorithm_timings == 0:
                    continue

                try:
                    _sum_of_algorithm_timings = sum(algorithm_timings_array)
                except:
                    logger.error("sum error: " + traceback.format_exc())
                    _sum_of_algorithm_timings = round(0.0, 6)
                    logger.error('error - sum_of_algorithm_timings - %s' % (algorithm))
                    continue

                sum_of_algorithm_timings = round(_sum_of_algorithm_timings, 6)
                # logger.info('sum_of_algorithm_timings - %s - %.16f seconds' % (algorithm, sum_of_algorithm_timings))

                try:
                    _median_algorithm_timing = determine_array_median(algorithm_timings_array)
                except:
                    _median_algorithm_timing = round(0.0, 6)
                    logger.error('error - _median_algorithm_timing - %s' % (algorithm))
                    continue
                median_algorithm_timing = round(_median_algorithm_timing, 6)
                # logger.info('median_algorithm_timing - %s - %.16f seconds' % (algorithm, median_algorithm_timing))

                logger.info(
                    'algorithm timing - %s - total: %.6f - median: %.6f' % (
                        algorithm, sum_of_algorithm_timings,
                        median_algorithm_timing))
                use_namespace = skyline_app_graphite_namespace + '.algorithm_breakdown.' + algorithm
                send_metric_name = use_namespace + '.timing.times_run'
                send_graphite_metric(skyline_app, send_metric_name, str(number_of_algorithm_timings))
                send_metric_name = use_namespace + '.timing.total_time'
                send_graphite_metric(skyline_app, send_metric_name, str(sum_of_algorithm_timings))
                send_metric_name = use_namespace + '.timing.median_time'
                send_graphite_metric(skyline_app, send_metric_name, str(median_algorithm_timing))

                # We del all variables that are floats as they become unique objects and
                # can result in what appears to be a memory leak, but is not, just the
                # way Python handles floats
                try:
                    del _sum_of_algorithm_timings
                except:
                    logger.error('error :: failed to del _sum_of_algorithm_timings')
                try:
                    del _median_algorithm_timing
                except:
                    logger.error('error :: failed to del _median_algorithm_timing')
                try:
                    del sum_of_algorithm_timings
                except:
                    logger.error('error :: failed to del sum_of_algorithm_timings')
                try:
                    del median_algorithm_timing
                except:
                    logger.error('error :: failed to del median_algorithm_timing')

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run after algorithm run times: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # @added 20180807 - Feature #2492: alert on stale metrics
            # @modified 20190410 - Feature #2916: ANALYZER_ENABLED setting
            # If Analyzer is not enabled do not alert on stale metrics
            # if settings.ALERT_ON_STALE_METRICS:
            if ALERT_ON_STALE_METRICS and ANALYZER_ENABLED:
                stale_metrics_to_alert_on = []
                alert_on_stale_metrics = []
                try:
                    # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # alert_on_stale_metrics = list(self.redis_conn.smembers('analyzer.alert_on_stale_metrics'))
                    alert_on_stale_metrics = list(self.redis_conn_decoded.smembers('analyzer.alert_on_stale_metrics'))
                    logger.info('alert_on_stale_metrics :: %s' % str(alert_on_stale_metrics))
                except:
                    alert_on_stale_metrics = []
                for metric_name in alert_on_stale_metrics:
                    metric_namespace_elements = metric_name.split('.')
                    process_metric = True
                    for do_not_alert_namespace in DO_NOT_ALERT_ON_STALE_METRICS:
                        if do_not_alert_namespace in metric_name:
                            process_metric = False
                            break
                        do_not_alert_namespace_namespace_elements = do_not_alert_namespace.split('.')
                        elements_matched = set(metric_namespace_elements) & set(do_not_alert_namespace_namespace_elements)
                        if len(elements_matched) == len(do_not_alert_namespace_namespace_elements):
                            process_metric = False
                            break

                    if not process_metric:
                        continue
                    base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    cache_key = 'last_alert.stale.%s' % (base_name)
                    last_alert = False
                    try:
                        last_alert = self.redis_conn.get(cache_key)
                    except Exception as e:
                        logger.error('error :: could not query Redis for cache_key: %s' % e)
                    if not last_alert:
                        stale_metrics_to_alert_on.append(base_name)
                        try:
                            # @modified 20180828 - Bug #2568: alert on stale metrics not firing
                            # Those expired in the year 2067
                            # self.redis_conn.setex(cache_key, int(time()), int(settings.STALE_PERIOD))
                            self.redis_conn.setex(cache_key, int(settings.STALE_PERIOD), int(time()))
                        except:
                            logger.error('error :: failed to add %s Redis key' % cache_key)
                if stale_metrics_to_alert_on:
                    alert = ('stale_metrics', 'stale_digest', settings.STALE_PERIOD)
                    metric = (len(stale_metrics_to_alert_on), stale_metrics_to_alert_on, int(time()))
                    context = 'Analyzer'
                    try:
                        trigger_alert(alert, metric, context)
                        logger.info('digest alerted on %s stale metrics' % str(len(stale_metrics_to_alert_on)))
                        logger.info('stale metrics :: %s' % str(stale_metrics_to_alert_on))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: could not send alert on stale digest email')
                    # @modified 20181117 - Feature #2492: alert on stale metrics
                    # Stop analyzer stalling if there is no
                    # stale_metrics_to_alert_on list to delete
                    try:
                        del stale_metrics_to_alert_on
                        del alert
                        del metric
                    except:
                        pass
                else:
                    logger.info('there are no stale metrics to alert on')

            # @added 20200121 - Feature #3396: http_alerter
            full_resend_queue = []
            resend_queues = ['analyzer', 'mirage', 'ionosphere']
            for source in resend_queues:
                resend_queue = []
                try:
                    redis_set = '%s.http_alerter.queue' % source
                    try:
                        resend_queue = list(self.redis_conn_decoded.smembers(redis_set))
                        if resend_queue:
                            logger.info('%s items in the %s Redis set' % (str(len(resend_queue)), redis_set))
                            resend_items = []
                            for index, resend_item in enumerate(resend_queue):
                                resend_item_list = literal_eval(resend_item)
                                resend_alert = literal_eval(resend_item_list[0])
                                resend_metric = literal_eval(resend_item_list[1])
                                resend_metric_alert_dict = literal_eval(resend_item_list[2])
                                resend_items.append([resend_alert, resend_metric, resend_metric_alert_dict])
                            resend_queue = resend_items
                            new_full_resend_queue = full_resend_queue + resend_queue
                            full_resend_queue = new_full_resend_queue
                        else:
                            logger.info('0 items in the %s Redis set' % (redis_set))
                    except Exception as e:
                        logger.error('error :: could not determine http_alerter item from Redis set %s - %s' % (redis_set, e))
                        resend_queue = []
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to get %s.http_alerter.queue from Redis' % source)
                    resend_queue = []
            if full_resend_queue:
                try:
                    http_alerters_down = []
                    for index, resend_item in enumerate(full_resend_queue):
                        resend_alert = resend_item[0]
                        resend_metric = resend_item[1]
                        resend_metric_alert_dict = resend_item[2]
                        # To ensure that Analyzer does not loop through every alert in the queue
                        # for an alerter_endpoint, if the alerter_endpoint is down and wait for
                        # the connect timeout on each one, if an alerter_endpoint fails a Redis
                        # key is created to check against to see if the alerter_endpoint is down
                        alerter_name = str(resend_alert[1])
                        alerter_endpoint_cache_key = 'http_alerter.down.%s' % str(alerter_name)
                        if alerter_endpoint_cache_key in http_alerters_down:
                            continue
                        alerter_endpoint_failed = False
                        try:
                            alerter_endpoint_failed = self.redis_conn.get(alerter_endpoint_cache_key)
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: alert_http could not query Redis for cache_key %s: %s' % (str(alerter_endpoint_cache_key), e))
                        if alerter_endpoint_failed:
                            if LOCAL_DEBUG:
                                logger.debug('alert_http :: debug :: %s exists not alerting' % str(alerter_endpoint_cache_key))
                            if alerter_endpoint_cache_key not in http_alerters_down:
                                http_alerters_down.append(alerter_endpoint_cache_key)
                                logger.info('%s Redis exists not alerting for any alerts for this endpoint' % str(alerter_endpoint_cache_key))
                            continue
                        logger.info('resend_queue item :: alert: %s, metric: %s, metric_alert_dict: %s' % (
                            str(resend_alert), str(resend_metric),
                            str(resend_metric_alert_dict)))
                        resend_context = resend_metric_alert_dict['source']
                        try:
                            trigger_alert(resend_alert, resend_metric, resend_context)
                            logger.info('trigger_alert :: alert: %s, metric: %s, context: %s' % (
                                str(resend_alert), str(resend_metric), str(resend_context)))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to trigger_alert for resend queue item')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to parse resend_queue')

            run_time = time() - now
            total_metrics = str(len(unique_metrics))

            # @modified 20190410 - Feature #2916: ANALYZER_ENABLED setting
            # total_analyzed = str(len(unique_metrics) - sum(exceptions.values()))
            if ANALYZER_ENABLED:
                total_analyzed = str(len(unique_metrics) - sum(exceptions.values()))
            else:
                total_analyzed = '0'

            # @modified 20190408 - Feature #2882: Mirage - periodic_check
            # Do not count Mirage periodic checks
            # total_anomalies = str(len(self.anomalous_metrics))
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # Use Redis set instead of Manager() list
            # total_anomalies = str(len(self.real_anomalous_metrics))
            real_anomalous_metrics = []
            try:
                # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # literal_real_anomalous_metrics = list(self.redis_conn.smembers('analyzer.real_anomalous_metrics'))
                literal_real_anomalous_metrics = list(self.redis_conn_decoded.smembers('analyzer.real_anomalous_metrics'))
                for metric_list_string in literal_real_anomalous_metrics:
                    metric = literal_eval(metric_list_string)
                    real_anomalous_metrics.append(metric)
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to generate list from Redis set analyzer.real_anomalous_metrics to calculate total_anomalies')
            total_anomalies = str(len(real_anomalous_metrics))

            # @added 20190408 - Feature #2882: Mirage - periodic_check
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # mirage_periodic_checks = str(len(self.mirage_periodic_check_metrics))
            mirage_periodic_checks = '0'
            # @modified 20190524 - Feature #2882: Mirage - periodic_check
            #                      Branch #3002: docker
            # Only check Redis set if MIRAGE_PERIODIC_CHECK is True
            if MIRAGE_PERIODIC_CHECK:
                try:
                    # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # mirage_periodic_checks = str(len(list(self.redis_conn.smembers('analyzer.mirage_periodic_check_metrics'))))
                    mirage_periodic_checks = str(len(list(self.redis_conn_decoded.smembers('analyzer.mirage_periodic_check_metrics'))))
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: failed to generate a list from analyzer.mirage_periodic_check_metrics Redis set')
                    mirage_periodic_checks = '0'

            # Log progress
            logger.info('seconds to run     :: %.2f' % run_time)
            logger.info('total metrics      :: %s' % total_metrics)
            logger.info('total analyzed     :: %s' % total_analyzed)
            logger.info('total anomalies    :: %s' % total_anomalies)
            logger.info('exception stats    :: %s' % exceptions)
            logger.info('anomaly breakdown  :: %s' % anomaly_breakdown)
            # @added 20190408 - Feature #2882: Mirage - periodic_check
            logger.info('mirage_periodic_checks  :: %s' % mirage_periodic_checks)

            # Log to Graphite
            graphite_run_time = '%.2f' % run_time
            send_metric_name = skyline_app_graphite_namespace + '.run_time'
            send_graphite_metric(skyline_app, send_metric_name, graphite_run_time)

            send_metric_name = skyline_app_graphite_namespace + '.total_analyzed'
            send_graphite_metric(skyline_app, send_metric_name, total_analyzed)

            send_metric_name = skyline_app_graphite_namespace + '.total_anomalies'
            send_graphite_metric(skyline_app, send_metric_name, total_anomalies)

            send_metric_name = skyline_app_graphite_namespace + '.total_metrics'
            send_graphite_metric(skyline_app, send_metric_name, total_metrics)

            # @added 20191021 - Bug #3288: Always send anomaly_breakdown and exception metrics

            for key, value in exceptions.items():
                send_metric_name = '%s.exceptions.%s' % (skyline_app_graphite_namespace, key)
                send_graphite_metric(skyline_app, send_metric_name, str(value))
            for key, value in anomaly_breakdown.items():
                send_metric_name = '%s.anomaly_breakdown.%s' % (skyline_app_graphite_namespace, key)
                send_graphite_metric(skyline_app, send_metric_name, str(value))

            # @added 20161119 - Branch #922: ionosphere
            #                   Task #1718: review.tsfresh
            # Send a breakdown of what metrics were sent to other apps
            # @added 20161119 - Branch #922: ionosphere
            #                   Task #1718: review.tsfresh
            # Send a breakdown of what metrics were sent to other apps
            sent_to_mirage = 0
            if settings.ENABLE_MIRAGE:
                try:
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # Replace Manager.list instances with Redis sets
                    # sent_to_mirage = str(len(self.sent_to_mirage))
                    # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # sent_to_mirage = str(len(list(self.redis_conn.smembers('analyzer.sent_to_mirage'))))
                    sent_to_mirage = str(len(list(self.redis_conn_decoded.smembers('analyzer.sent_to_mirage'))))
                except:
                    sent_to_mirage = '0'
                logger.info('sent_to_mirage     :: %s' % sent_to_mirage)
                send_metric_name = '%s.sent_to_mirage' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, sent_to_mirage)
                try:
                    mirage_unique_metrics_count_str = str(mirage_unique_metrics_count)
                except:
                    mirage_unique_metrics_count_str = '0'
                logger.info('Mirage metrics     :: %s' % mirage_unique_metrics_count_str)
                send_metric_name = '%s.mirage_metrics' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, mirage_unique_metrics_count_str)

                # @added 20190408 - Feature #2882: Mirage - periodic_check
                logger.info('mirage_periodic_checks  :: %s' % mirage_periodic_checks)
                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # Already defined above
                # mirage_periodic_checks = str(len(self.mirage_periodic_check_metrics))
                # logger.info('mirage_periodic_checks     :: %s' % mirage_periodic_checks)
                send_metric_name = '%s.mirage_periodic_checks' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, mirage_periodic_checks)

            sent_to_crucible = 0
            if settings.ENABLE_CRUCIBLE and settings.ANALYZER_CRUCIBLE_ENABLED:
                try:
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # sent_to_crucible = str(len(self.sent_to_crucible))
                    # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # sent_to_crucible = str(len(list(self.redis_conn.smembers('analyzer.sent_to_crucible'))))
                    sent_to_crucible = str(len(list(self.redis_conn_decoded.smembers('analyzer.sent_to_crucible'))))
                except:
                    sent_to_crucible = '0'
                logger.info('sent_to_crucible   :: %s' % sent_to_crucible)
                send_metric_name = '%s.sent_to_crucible' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, sent_to_crucible)

            sent_to_panorama = 0
            if settings.PANORAMA_ENABLED:
                try:
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # sent_to_panorama = str(len(self.sent_to_panorama))
                    # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # sent_to_panorama = str(len(list(self.redis_conn.smembers('analyzer.sent_to_panorama'))))
                    sent_to_panorama = str(len(list(self.redis_conn_decoded.smembers('analyzer.sent_to_panorama'))))
                except:
                    sent_to_panorama = '0'
                logger.info('sent_to_panorama   :: %s' % sent_to_panorama)
                send_metric_name = '%s.sent_to_panorama' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, sent_to_panorama)

            sent_to_ionosphere = 0
            if settings.IONOSPHERE_ENABLED:
                try:
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # sent_to_ionosphere = str(len(self.sent_to_ionosphere))
                    # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # sent_to_ionosphere = str(len(list(self.redis_conn.smembers('analyzer.sent_to_ionosphere'))))
                    sent_to_ionosphere = str(len(list(self.redis_conn_decoded.smembers('analyzer.sent_to_ionosphere'))))
                except:
                    sent_to_ionosphere = '0'
                logger.info('sent_to_ionosphere :: %s' % sent_to_ionosphere)
                send_metric_name = '%s.sent_to_ionosphere' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, sent_to_ionosphere)
                try:
                    ionosphere_unique_metrics_count_str = str(ionosphere_unique_metrics_count)
                except:
                    ionosphere_unique_metrics_count_str = '0'
                logger.info('Ionosphere metrics :: %s' % ionosphere_unique_metrics_count_str)
                send_metric_name = '%s.ionosphere_metrics' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, ionosphere_unique_metrics_count_str)

            # Check canary metric
            try:
                raw_series = self.redis_conn.get(settings.FULL_NAMESPACE + settings.CANARY_METRIC)
            except:
                logger.error('error :: failed to get CANARY_METRIC from Redis')
                raw_series = None

            projected = None

            # @added 20190517 - Branch #3002: docker
            # Declare these variables so that errors are not thrown when they
            # are deleted later
            timeseries = []
            time_human = 0

            if raw_series is not None:
                try:
                    unpacker = Unpacker(use_list=False)
                    unpacker.feed(raw_series)
                    timeseries = list(unpacker)

                    # @added 20200506 - Feature #3532: Sort all time series
                    # To ensure that there are no unordered timestamps in the time
                    # series which are artefacts of the collector or carbon-relay, sort
                    # all time series by timestamp before analysis.
                    original_timeseries = timeseries
                    if original_timeseries:
                        timeseries = sort_timeseries(original_timeseries)
                        del original_timeseries

                    time_human = (timeseries[-1][0] - timeseries[0][0]) / 3600
                    projected = 24 * (time() - now) / time_human
                    logger.info('canary duration    :: %.2f' % time_human)
                except:
                    logger.error(
                        'error :: failed to unpack/calculate canary duration')

                send_metric_name = skyline_app_graphite_namespace + '.duration'
                send_graphite_metric(skyline_app, send_metric_name, str(time_human))

                send_metric_name = skyline_app_graphite_namespace + '.projected'
                send_graphite_metric(skyline_app, send_metric_name, str(projected))

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage before reset counters: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # @added 20191107 - Feature #3306: Record anomaly_end_timestamp
            #                   Branch #3262: py3
            # Set the anomaly_end_timestamp for any metrics no longer anomalous
            not_anomalous_metrics = []
            not_anomalous_metrics_data = []
            redis_set = 'current.anomalies'
            try:
                current_anomalies = self.redis_conn_decoded.smembers(redis_set)
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to get Redis set %s' % redis_set)
                current_anomalies = []
            if current_anomalies:
                try:
                    redis_set = 'analyzer.not_anomalous_metrics'
                    dict_not_anomalous_metrics = self.redis_conn_decoded.smembers(redis_set)
                except:
                    dict_not_anomalous_metrics = []
                for item in dict_not_anomalous_metrics:
                    try:
                        list_data = literal_eval(item)
                        not_anomalous_metrics_data.append(list_data)
                        not_anomalous_metrics.append(str(list_data[0]))
                    except:
                        pass
                for item in current_anomalies:
                    try:
                        list_data = literal_eval(item)
                        anomalous_metric = str(list_data[0])
                        anomaly_timestamp = int(list_data[1])
                        try:
                            anomaly_id = int(list_data[2])
                        except:
                            anomaly_id = None
                        try:
                            anomaly_end_timestamp = int(list_data[3])
                        except:
                            anomaly_end_timestamp = None
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to generate a list from analyzer.illuminance_datapoints Redis set')
                    if anomaly_end_timestamp:
                        continue
                    if not anomaly_id:
                        continue
                    if anomalous_metric in not_anomalous_metrics:
                        for metric, anomaly_end_timestamp in not_anomalous_metrics_data:
                            if anomalous_metric == metric:
                                update_item = False
                                try:
                                    redis_set = 'current.anomalies'
                                    self.redis_conn.srem(redis_set, str(list_data))
                                    update_item = True
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to remove %s for Redis set %s' % (str(list_data), redis_set))
                                if update_item:
                                    new_list_data = [metric, anomaly_timestamp, anomaly_id, anomaly_end_timestamp]
                                    try:
                                        redis_set = 'current.anomalies'
                                        self.redis_conn.sadd(redis_set, str(new_list_data))
                                        logger.info('set anomaly_end_timestamp to %s for %s in Redis set %s' % (
                                            str(anomaly_end_timestamp), metric, redis_set))
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: failed to add %s to Redis set %s' % (str(new_list_data), redis_set))

            # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # Use Redis key set instead of self.illuminance_datapoints Manager()
            # list
            # Disabled for now as in concept phase.
            if calculate_illuminance:
                illuminance_datapoints = []
                try:
                    # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # illuminance_datapoints_strings = list(self.redis_conn.smembers('analyzer.illuminance_datapoints'))
                    illuminance_datapoints_strings = list(self.redis_conn_decoded.smembers('analyzer.illuminance_datapoints'))
                    for datapoint in illuminance_datapoints_strings:
                        illuminance_datapoints.append(float(datapoint))
                    logger.info('%s datapoints in list from analyzer.illuminance_datapoints Redis set' % str(len(illuminance_datapoints)))
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: failed to generate a list from analyzer.illuminance_datapoints Redis set')
                    illuminance_datapoints = []

                # @added 20180903 - Feature #2580: illuminance
                #                   Feature #1986: flux
                illuminance_sum = 0
                try:
                    # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # illuminance_sum = sum(self.illuminance_datapoints)
                    illuminance_sum = sum(illuminance_datapoints)
                    illuminance = str(illuminance_sum)
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: failed to sum list from analyzer.illuminance_datapoints Redis set')
                    illuminance = '0'
                logger.info('illuminance        :: %s' % str(illuminance))
                send_metric_name = '%s.illuminance' % skyline_app_graphite_namespace
                # @modified 20181017 - Feature #2580: illuminance
                # Disabled for now as in concept phase.  This would work better if
                # the illuminance_datapoint was determined from the time series
                # after min max scaling wass applied to it.  That way each metric
                # would have the same weight.  txBytes for instance would not have
                # a far greater weight than a low range metric.
                # send_graphite_metric(skyline_app, send_metric_name, illuminance)

                # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                try:
                    self.redis_conn.delete('analyzer.illuminance_datapoints')
                except:
                    logger.info('failed to delete analyzer.illuminance_datapoints Redis set')
                    pass

            # Reset counters
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # self.anomalous_metrics[:] = []
            try:
                self.redis_conn.delete('analyzer.anomalous_metrics')
            except:
                logger.info('failed to delete analyzer.anomalous_metrics Redis set')
                pass

            # @modified 20190522 - Branch #3002: docker
            #                      Task #3032: Debug number of Python processes and memory use
            # Reduce amount of Manager instances that are used as ech requires an
            # a copy of entire memory to be copied into each subprocess so this
            # results in a python process per Manager instance
            # self.mirage_metrics[:] = []
            # self.ionosphere_metrics[:] = []

            # @added 20161119 - Branch #922: ionosphere
            #                   Task #1718: review.tsfresh
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # Replace Manager.list instances with Redis sets
            # self.sent_to_mirage[:] = []
            if int(sent_to_mirage) > 0:
                try:
                    self.redis_conn.delete('analyzer.sent_to_mirage')
                except:
                    logger.info('failed to delete analyzer.sent_to_mirage Redis set')
                    pass
            # self.sent_to_crucible[:] = []
            if int(sent_to_crucible) > 0:
                try:
                    self.redis_conn.delete('analyzer.sent_to_crucible')
                except:
                    logger.info('failed to delete analyzer.sent_to_crucible Redis set')
                    pass
            # self.sent_to_panorama[:] = []
            if int(sent_to_panorama) > 0:
                try:
                    self.redis_conn.delete('analyzer.sent_to_panorama')
                except:
                    logger.info('failed to delete analyzer.sent_to_panorama Redis set')
                    pass
            # self.sent_to_ionosphere[:] = []
            if int(sent_to_ionosphere) > 0:
                try:
                    self.redis_conn.delete('analyzer.sent_to_ionosphere')
                except:
                    logger.info('failed to delete analyzer.sent_to_ionosphere Redis set')
                    pass

            # @added 20161229 - Feature #1830: Ionosphere alerts
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # Replace Manager.list instances with Redis sets
            # self.all_anomalous_metrics[:] = []
            all_anomalous_metrics_count = 0
            try:
                # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # all_anomalous_metrics_count = len(list(self.redis_conn.smembers('analyzer.all_anomalous_metrics')))
                all_anomalous_metrics_count = len(list(self.redis_conn_decoded.smembers('analyzer.all_anomalous_metrics')))
            except:
                pass
            if all_anomalous_metrics_count > 0:
                try:
                    self.redis_conn.delete('analyzer.all_anomalous_metrics')
                except:
                    logger.info('failed to delete analyzer.all_anomalous_metrics Redis set')
                    pass

            # @added 20191107 - Feature #3306: Record anomaly_end_timestamp
            #                   Branch #3262: py3
            # In order for Analyzer, Mirage and Panorama to all iterate the same
            # sets of data to determine and record the anomaly_end_timestamp,
            # some below transient sets need to copied so that the data always
            # exists, even if it is sourced from a transient set.
            try:
                self.redis_conn.delete('aet.analyzer.smtp_alerter_metrics')
            except:
                logger.info('failed to delete aet.analyzer.smtp_alerter_metrics Redis set')
                pass
            try:
                self.redis_conn.delete('aet.analyzer.non_smtp_alerter_metrics')
            except:
                logger.info('failed to delete aet.analyzer.non_smtp_alerter_metrics Redis set')
                pass

            # @added 20170108 - Feature #1830: Ionosphere alerts
            # Adding lists of smtp_alerter_metrics and non_smtp_alerter_metrics
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # Replace Manager.list instances with Redis sets
            # self.smtp_alerter_metrics[:] = []
            try:
                # @modified 20191107 - Feature #3306: Record anomaly_end_timestamp
                # The data in this set is required to determine the app the is
                # responsible for determining the anomaly_end_timestamp for a
                # metric.  Therefore these sets on now renamed to the aet. key
                # namespace to be used by the apps rather than being deleted.
                # self.redis_conn.delete('analyzer.smtp_alerter_metrics')
                self.redis_conn.rename('analyzer.smtp_alerter_metrics', 'aet.analyzer.smtp_alerter_metrics')
            except:
                # logger.info('failed to delete analyzer.smtp_alerter_metrics Redis set')
                logger.info('failed to rename Redis set analyzer.smtp_alerter_metrics to aet.analyzer.smtp_alerter_metrics')
                pass
            # self.non_smtp_alerter_metrics[:] = []
            try:
                # self.redis_conn.delete('analyzer.non_smtp_alerter_metrics')
                self.redis_conn.rename('analyzer.non_smtp_alerter_metrics', 'aet.analyzer.non_smtp_alerter_metrics')
            except:
                # logger.info('failed to delete analyzer.non_smtp_alerter_metrics Redis set')
                logger.info('failed to rename Redis set analyzer.non_smtp_alerter_metrics to aet.analyzer.non_smtp_alerter_metrics')
                pass

            # @added 20180903 - Feature #1986: illuminance
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # Replace Manager.list instances with Redis sets
            # self.illuminance_datapoints[:] = []
            # @modified 20190522 - Feature #2580: illuminance
            #                      Feature #1986: flux
            # Disabled for now as in concept phase.
            if calculate_illuminance:
                try:
                    self.redis_conn.delete('analyzer.illuminance_datapoints')
                except:
                    logger.info('failed to delete analyzer.illuminance_datapoints Redis set')
                    pass

            # @added 20190408 - Feature #2882: Mirage - periodic_check
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # self.mirage_periodic_check_metrics[:] = []
            # @modified 20190524 - Feature #2882: Mirage - periodic_check
            #                      Branch #3002: docker
            # Only check Redis set if MIRAGE_PERIODIC_CHECK is True
            if MIRAGE_PERIODIC_CHECK:
                try:
                    self.redis_conn.delete('analyzer.mirage_periodic_check_metrics')
                except:
                    logger.info('failed to delete analyzer.mirage_periodic_check_metrics Redis set')
                    pass

            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # self.real_anomalous_metrics[:] = []
            try:
                self.redis_conn.delete('analyzer.real_anomalous_metrics')
            except:
                logger.info('failed to delete analyzer.real_anomalous_metrics Redis set')
                pass

            unique_metrics = []
            raw_series = None
            unpacker = None
            # We del all variables that are floats as they become unique objects and
            # can result in what appears to be a memory leak, but it is not, it
            # is just the way that Python handles floats
            # @modified 20170809 - Bug #2136: Analyzer stalling on no metrics
            #                      Task #1544: Add additional except handling to Analyzer
            #                      Task #1372: algorithm performance tuning
            # Added except to all del methods to prevent Analyzer stalling if
            # any of the float variables are not set, like projected if no
            # metrics were being set to Redis
            try:
                del timeseries[:]
            except:
                logger.error('error :: failed to del timeseries')
            try:
                del time_human
            except:
                logger.error('error :: failed to del time_human')
            try:
                del projected
            except:
                logger.error('error :: failed to del projected - Redis probably has no metrics')
            try:
                del run_time
            except:
                logger.error('error :: failed to del run_time')

            # @added 20180903 - Feature #2580: illuminance
            #                   Feature #1986: flux
            # @modified 20190522 - Feature #2580: illuminance
            # Disabled for now as in concept phase.
            if calculate_illuminance:
                try:
                    del illuminance_sum
                except:
                    logger.error('error :: failed to del illuminance_sum')
                try:
                    del illuminance
                except:
                    logger.error('error :: failed to del illuminance')

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage after reset counters: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                logger.info('debug :: Memory usage before sleep: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # @added 20170602 - Feature #2034: analyse_derivatives
            # This is here to replace the sets
            key_timestamp = int(time())
            try:
                manage_derivative_metrics = self.redis_conn.get('analyzer.derivative_metrics_expiry')
            except Exception as e:
                if LOCAL_DEBUG:
                    logger.error('error :: could not query Redis for analyzer.derivative_metrics_expiry key: %s' % str(e))

            if not manage_derivative_metrics:
                try:
                    # @modified 20170901 - Bug #2154: Infrequent missing new_ Redis keys
                    # self.redis_conn.setex('analyzer.derivative_metrics_expiry', 120, key_timestamp)
                    self.redis_conn.setex('analyzer.derivative_metrics_expiry', 300, key_timestamp)
                    logger.info('set analyzer.derivative_metrics_expiry Redis key')
                except:
                    logger.error('error :: failed to set key :: analyzer.analyzer.derivative_metrics_expiry')

                # @added 20190517 - Branch #3002: docker
                # Declare and test if derivative_metrics exists
                derivative_metrics = None

                try:
                    # @modified 20190517 - Branch #3002: docker
                    # self.redis_conn.rename('new_derivative_metrics', 'derivative_metrics')
                    # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # derivative_metrics = list(self.redis_conn.smembers('derivative_metrics'))
                    derivative_metrics = list(self.redis_conn_decoded.smembers('derivative_metrics'))
                except Exception as e:
                    # @modified 20190417 - Bug #2946: ANALYZER_ENABLED False - rename Redis keys error
                    #                      Feature #2916: ANALYZER_ENABLED setting
                    #                      ANALYZER_ENABLED False - rename Redis keys error #103
                    # If Analyzer is not enabled the keys will not exist this is
                    # expected and not an error
                    # logger.error('error :: could not rename Redis set new_derivative_metrics: %s' % str(e))
                    if ANALYZER_ENABLED:
                        logger.error('error :: could not rename Redis set new_derivative_metrics: %s' % str(e))
                    else:
                        logger.info(
                            'there is no Redis set new_derivative_metrics to rename, expected as ANALYZER_ENABLED is set to %s - %s' % (
                                str(ANALYZER_ENABLED), str(e)))

                # @added 20190922 - Branch #3002: docker
                # Declare and test if new_derivative_metrics exists, if it is a
                # new instance they will not, so create the set and handle no
                # new_derivative_metrics set
                test_new_derivative_metrics = None
                try:
                    # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # test_new_derivative_metrics = list(self.redis_conn.smembers('new_derivative_metrics'))
                    test_new_derivative_metrics = list(self.redis_conn_decoded.smembers('new_derivative_metrics'))
                except Exception as e:
                    try:
                        key_data = 'skyline_set_as_of_%s' % str(int(time()))
                        self.redis_conn.sadd('new_derivative_metrics', key_data)
                        # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                        #                      Branch #3262: py3
                        # test_new_derivative_metrics = list(self.redis_conn.smembers('new_derivative_metrics'))
                        test_new_derivative_metrics = list(self.redis_conn_decoded.smembers('new_derivative_metrics'))
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to add default data to Redis new_derivative_metrics set')

                # @modified 20190517 - Branch #3002: docker
                # Wrapped in if derivative_metrics
                # @modified 20190922 - Branch #3002: docker
                # Handle no new_derivative_metrics
                # if derivative_metrics:
                if derivative_metrics and test_new_derivative_metrics:
                    try:
                        self.redis_conn.rename('new_derivative_metrics', 'derivative_metrics')
                    except Exception as e:
                        # @modified 20190417 - Bug #2946: ANALYZER_ENABLED False - rename Redis keys error
                        #                      Feature #2916: ANALYZER_ENABLED setting
                        #                      ANALYZER_ENABLED False - rename Redis keys error #103
                        # If Analyzer is not enabled the keys will not exist this is
                        # expected and not an error
                        # logger.error('error :: could not rename Redis set new_derivative_metrics: %s' % str(e))
                        if ANALYZER_ENABLED:
                            logger.error('error :: could not rename Redis set new_derivative_metrics: %s' % str(e))
                        else:
                            logger.info(
                                'there is no Redis set new_derivative_metrics to rename, expected as ANALYZER_ENABLED is set to %s - %s' % (
                                    str(ANALYZER_ENABLED), str(e)))
                    try:
                        self.redis_conn.rename('new_non_derivative_metrics', 'non_derivative_metrics')
                    except Exception as e:
                        # @modified 20190417 - Bug #2946: ANALYZER_ENABLED False - rename Redis keys error
                        #                      Feature #2916: ANALYZER_ENABLED setting
                        #                      ANALYZER_ENABLED False - rename Redis keys error #103
                        # If Analyzer is not enabled the keys will not exist this is
                        # expected and not an error
                        # logger.error('error :: could not rename Redis set new_non_derivative_metrics: %s' % str(e))
                        if ANALYZER_ENABLED:
                            logger.error('error :: could not rename Redis set new_non_derivative_metrics: %s' % str(e))
                        else:
                            logger.info(
                                'there is no Redis set new_non_derivative_metrics to rename, expected as ANALYZER_ENABLED is set to %s - %s' % (
                                    str(ANALYZER_ENABLED), str(e)))

                live_at = 'skyline_set_as_of_%s' % str(key_timestamp)
                try:
                    self.redis_conn.sadd('derivative_metrics', live_at)
                except Exception as e:
                    logger.error('error :: could not add live_at to Redis set derivative_metrics: %s' % str(e))
                try:
                    non_derivative_monotonic_metrics = settings.NON_DERIVATIVE_MONOTONIC_METRICS
                except:
                    non_derivative_monotonic_metrics = []
                try:
                    for non_derivative_monotonic_metric in non_derivative_monotonic_metrics:
                        self.redis_conn.sadd('non_derivative_metrics', non_derivative_monotonic_metric)
                    self.redis_conn.sadd('non_derivative_metrics', live_at)
                except Exception as e:
                    logger.error('error :: could not add live at to Redis set non_derivative_metrics: %s' % str(e))

            # @added 20200117 - Feature #3400: Identify air gaps in the metric data
            # Manage airgapped_metrics set and remove old entires
            if IDENTIFY_AIRGAPS:
                airgapped_metrics = []
                try:
                    airgapped_metrics = list(self.redis_conn_decoded.smembers('analyzer.airgapped_metrics'))
                except:
                    airgapped_metrics = []
                if airgapped_metrics:
                    try:
                        logger.info('managing analyzer.airgapped_metrics Redis set')
                        current_timestamp = int(time())
                        max_airgap_age = current_timestamp - MAX_AIRGAP_PERIOD
                        removed_airgap_items = 0
                        for i in airgapped_metrics:
                            airgap = literal_eval(i)
                            airgap_metric_start_timestamp = int(airgap[2])
                            if max_airgap_age > airgap_metric_start_timestamp:
                                logger.info('removing expired airgapped metric item - %s' % str(airgap))
                                try:
                                    self.redis_conn.srem('analyzer.airgapped_metrics', str(airgap))
                                    removed_airgap_items += 1
                                    # TODO: learn_airgapped_metrics
                                except Exception as e:
                                    logger.error('error :: could not remove item from analyzer.airgapped_metrics Redis set - %s' % str(e))
                        logger.info('removed %s old items from analyzer.airgapped_metrics Redis set' % str(removed_airgap_items))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to manage analyzer.airgapped_metrics Redis set Redis set')

                # @added 20200501 - Feature #3400: Identify air gaps in the metric data
                # Handle airgaps filled so that once they have been submitted as filled
                # Analyzer will not identify them as airgapped again
                airgapped_metrics_filled = []
                try:
                    airgapped_metrics_filled = list(self.redis_conn_decoded.smembers('analyzer.airgapped_metrics.filled'))
                except:
                    airgapped_metrics_filled = []
                if airgapped_metrics_filled:
                    try:
                        logger.info('managing analyzer.airgapped_metrics.filled Redis set')
                        current_timestamp = int(time())
                        max_airgap_age = current_timestamp - MAX_AIRGAP_PERIOD
                        removed_airgap_items = 0
                        for i in airgapped_metrics_filled:
                            airgap = literal_eval(i)
                            airgap_metric_start_timestamp = int(airgap[2])
                            if max_airgap_age > airgap_metric_start_timestamp:
                                logger.info('removing expired airgapped filled metric item - %s' % str(airgap))
                                try:
                                    self.redis_conn.srem('analyzer.airgapped_metrics.filled', str(airgap))
                                    removed_airgap_items += 1
                                except Exception as e:
                                    logger.error('error :: could not remove item from analyzer.airgapped_metrics.filled Redis set - %s' % str(e))
                        logger.info('removed %s old items from analyzer.airgapped_metrics.filled Redis set' % str(removed_airgap_items))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to manage analyzer.airgapped_metrics.filled Redis set Redis set')

            # @added 20200411 - Feature #3480: batch_processing
            try:
                self.redis_conn.delete('analyzer.batch_processing_metrics')
            except:
                pass
            try:
                self.redis_conn.rename('analyzer.batch_processing_metrics_current', 'analyzer.batch_processing_metrics')
            except:
                pass

            # @added 20200505 - Feature #2882: Mirage - periodic_check
            # Manage the mirage.periodic_check sets in the parent process
            try:
                self.redis_conn.rename('mirage.periodic_check.metrics.all', 'mirage.periodic_check.metrics.all.old')
            except:
                pass
            try:
                self.redis_conn.rename('new.mirage.periodic_check.metrics.all', 'mirage.periodic_check.metrics.all')
            except:
                pass
            try:
                self.redis_conn.delete('mirage.periodic_check.metrics.all.old')
            except:
                pass
            try:
                self.redis_conn.rename('mirage.periodic_check.metrics', 'mirage.periodic_check.metrics.old')
            except:
                pass
            try:
                self.redis_conn.rename('new.mirage.periodic_check.metrics', 'mirage.periodic_check.metrics')
            except:
                pass
            try:
                self.redis_conn.delete('mirage.periodic_check.metrics.old')
            except:
                pass

            # Sleep if it went too fast
            # if time() - now < 5:
            #    logger.info('sleeping due to low run time...')
            #    sleep(10)
            # @modified 20160504 - @earthgecko - development internal ref #1338, #1340)
            # Etsy's original for this was a value of 5 seconds which does
            # not make skyline Analyzer very efficient in terms of installations
            # where 100s of 1000s of metrics are not being analyzed.  This lead to
            # Analyzer running over several metrics multiple times in a minute
            # and always working.  Therefore this was changed from if you took
            # less than 5 seconds to run only then sleep.  This behaviour
            # resulted in Analyzer analysing a few 1000 metrics in 9 seconds and
            # then doing it again and again in a single minute.  Therefore the
            # ANALYZER_OPTIMUM_RUN_DURATION setting was added to allow this to
            # self optimise in cases where skyline is NOT deployed to analyze
            # 100s of 1000s of metrics.  This relates to optimising performance
            # for any deployments with a few 1000 metrics and 60 second resolution
            # area, e.g. smaller and local deployments.
            process_runtime = time() - now
            analyzer_optimum_run_duration = settings.ANALYZER_OPTIMUM_RUN_DURATION
            if process_runtime < analyzer_optimum_run_duration:
                sleep_for = (analyzer_optimum_run_duration - process_runtime)
                logger.info('sleeping for %.2f seconds due to low run time...' % sleep_for)
                sleep(sleep_for)
                try:
                    del sleep_for
                except:
                    logger.error('error :: failed to del sleep_for')
            try:
                del process_runtime
            except:
                logger.error('error :: failed to del process_runtime')
