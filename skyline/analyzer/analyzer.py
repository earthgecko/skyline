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

# @added 20201014 - Feature #3734: waterfall alerts
from shutil import rmtree

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
# modified 20201020 - Feature #3792: algorithm_exceptions - EmptyTimeseries
from algorithm_exceptions import TooShort, Stale, Boring, EmptyTimeseries

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
    # @modified 20200606 - Bug #3572: Apply list to settings imports
    DO_NOT_ALERT_ON_STALE_METRICS = list(settings.DO_NOT_ALERT_ON_STALE_METRICS)
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
    # @modified 20200606 - Bug #3572: Apply list to settings imports
    MIRAGE_PERIODIC_CHECK = list(settings.MIRAGE_PERIODIC_CHECK)
except:
    MIRAGE_PERIODIC_CHECK = False
try:
    MIRAGE_PERIODIC_CHECK_INTERVAL = settings.MIRAGE_PERIODIC_CHECK_INTERVAL
except:
    MIRAGE_PERIODIC_CHECK_INTERVAL = 3600
# @added 20200505 - Feature #2882: Mirage - periodic_check
# Surface this once
try:
    # @modified 20200606 - Bug #3572: Apply list to settings imports
    mirage_periodic_check_namespaces = list(settings.MIRAGE_PERIODIC_CHECK_NAMESPACES)
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
    # @modified 20200606 - Bug #3572: Apply list to settings imports
    # from settings import BATCH_PROCESSING_NAMESPACES
    BATCH_PROCESSING_NAMESPACES = list(settings.BATCH_PROCESSING_NAMESPACES)
except:
    BATCH_PROCESSING_NAMESPACES = []
# @added 20200414 - Feature #3486: analyzer_batch
#                   Feature #3480: batch_processing
try:
    from settings import BATCH_PROCESSING_DEBUG
except:
    BATCH_PROCESSING_DEBUG = None

# @added 20201017 - Feature #3818: ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED
# This was implemented to allow a busy analyzer to offload low priority metrics
# to analyzer_batch, unsuccessfully.  It works, but takes loanger and ages
# actually.  Being left in as may be workable with a different logic.
try:
    ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED = settings.ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED
except:
    ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED = False
# Always disable until refactored to work more efficiently if possible
ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED = False

# @added 20200423 - Feature #3504: Handle airgaps in batch metrics
#                   Feature #3400: Identify air gaps in the metric data
CHECK_AIRGAPS = []
if IDENTIFY_AIRGAPS:
    try:
        # @modified 20200606 - Bug #3572: Apply list to settings imports
        # from settings import CHECK_AIRGAPS
        CHECK_AIRGAPS = list(settings.CHECK_AIRGAPS)
    except:
        CHECK_AIRGAPS = []
    if CHECK_AIRGAPS:
        from skyline_functions import is_check_airgap_metric

# @added 20200423 - Feature #3508: ionosphere.untrainable_metrics
# Determine if any metrcs have negatives values some they can be
# added to the ionosphere.untrainable_metrics Redis set
try:
    # @modified 20200606 - Bug #3572: Apply list to settings imports
    # from settings import KNOWN_NEGATIVE_METRICS
    KNOWN_NEGATIVE_METRICS = list(settings.KNOWN_NEGATIVE_METRICS)
except:
    KNOWN_NEGATIVE_METRICS = []

# @added 20200427 - Feature #3514: Identify inactive metrics
# Added metrics that have become inactive to the Redis set to be flagged
# as inactive
try:
    inactive_after = settings.METRICS_INACTIVE_AFTER
except:
    inactive_after = settings.FULL_DURATION - 3600

# @added 20200528 - Feature #3560: External alert config
try:
    EXTERNAL_ALERTS = settings.EXTERNAL_ALERTS.copy()
except:
    EXTERNAL_ALERTS = {}
# @added 20200602 - Feature #3560: External alert config
if EXTERNAL_ALERTS:
    from external_alert_configs import get_external_alert_configs

# @added 20200603 - Feature #3566: custom_algorithms
try:
    CUSTOM_ALGORITHMS = settings.CUSTOM_ALGORITHMS.copy()
except:
    CUSTOM_ALGORITHMS = None
try:
    DEBUG_CUSTOM_ALGORITHMS = settings.DEBUG_CUSTOM_ALGORITHMS
except:
    DEBUG_CUSTOM_ALGORITHMS = False
# @added 20200607 - Feature #3566: custom_algorithms
try:
    MIRAGE_ALWAYS_METRICS = list(settings.MIRAGE_ALWAYS_METRICS)
except:
    MIRAGE_ALWAYS_METRICS = []

# @added 20200623 - Feature #3588: Metric http_alerter queues
#                   Feature #3396: http_alerter
try:
    HTTP_ALERTERS_ENABLED = settings.HTTP_ALERTERS_ENABLED
except:
    HTTP_ALERTERS_ENABLED = False

# @added 20200827 - Feature #3708: FLUX_ZERO_FILL_NAMESPACES
try:
    FLUX_ZERO_FILL_NAMESPACES = settings.FLUX_ZERO_FILL_NAMESPACES
except:
    FLUX_ZERO_FILL_NAMESPACES = []

# @added 20200916 - Branch #3068: SNAB
#                   Task #3744: POC matrixprofile
#                   Info #1792: Shapelet extraction
try:
    SNAB_ENABLED = settings.SNAB_ENABLED
except:
    # SNAB_ENABLED = False
    SNAB_ENABLED = True
try:
    SNAB_CHECKS = settings.SNAB_CHECKS.copy()
except:
    SNAB_CHECKS = {}

# @added 20201007 - Feature #3774: SNAB_LOAD_TEST_ANALYZER
# The number of metrics to load test Analyzer with
try:
    SNAB_LOAD_TEST_ANALYZER = int(settings.SNAB_LOAD_TEST_ANALYZER)
except:
    SNAB_LOAD_TEST_ANALYZER = 0

# @added 20201030 - Feature #3812: ANALYZER_ANALYZE_LOW_PRIORITY_METRICS
try:
    ANALYZER_ANALYZE_LOW_PRIORITY_METRICS = settings.ANALYZER_ANALYZE_LOW_PRIORITY_METRICS
except:
    ANALYZER_ANALYZE_LOW_PRIORITY_METRICS = True
# @added 20201030 - Feature #3808: ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS
try:
    ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS = settings.ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS
except:
    ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS = False
# @added 20201018 - Feature #3810: ANALYZER_MAD_LOW_PRIORITY_METRICS
try:
    ANALYZER_MAD_LOW_PRIORITY_METRICS = settings.ANALYZER_MAD_LOW_PRIORITY_METRICS
except:
    ANALYZER_MAD_LOW_PRIORITY_METRICS = 0
# @added 20201030 - Feature #3808: ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS
# Set the default ANALYZER_MAD_LOW_PRIORITY_METRICS to 10 if not set and
# ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS is set.
if ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS:
    if not ANALYZER_MAD_LOW_PRIORITY_METRICS:
        ANALYZER_MAD_LOW_PRIORITY_METRICS = 10
    # Import itemgetter to implement a dictionary sort by value as proposed in
    # PEP 265
    from operator import itemgetter
if ANALYZER_MAD_LOW_PRIORITY_METRICS:
    # Import mean and absolute from numpy to calculate MAD
    from numpy import mean, absolute
# Determine all the settings that place Analyzer in a mode to handle low
# priority metrics differently
ANALYZER_MANAGE_LOW_PRIORITY_METRICS = False
if ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED:
    ANALYZER_MANAGE_LOW_PRIORITY_METRICS = True
if not ANALYZER_ANALYZE_LOW_PRIORITY_METRICS:
    ANALYZER_MANAGE_LOW_PRIORITY_METRICS = True
if ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS:
    ANALYZER_MANAGE_LOW_PRIORITY_METRICS = True
if ANALYZER_MAD_LOW_PRIORITY_METRICS:
    ANALYZER_MANAGE_LOW_PRIORITY_METRICS = True
low_priority_metrics_hash_key = 'analyzer.low_priority_metrics.last_analyzed_timestamp'
metrics_last_timestamp_hash_key = 'analyzer.metrics.last_analyzed_timestamp'

# @added 20201107 - Feature #3830: metrics_manager
ANALYZER_USE_METRICS_MANAGER = True

# @added 20201209 - Feature #3870: metrics_manager - check_data_sparsity
try:
    CHECK_DATA_SPARSITY = settings.CHECK_DATA_SPARSITY
except:
    CHECK_DATA_SPARSITY = True

# @added 20201212 - Feature #3884: ANALYZER_CHECK_LAST_TIMESTAMP
try:
    ANALYZER_CHECK_LAST_TIMESTAMP = settings.ANALYZER_CHECK_LAST_TIMESTAMP
except:
    ANALYZER_CHECK_LAST_TIMESTAMP = False

# @added 20210512 - Feature #4064: VERBOSE_LOGGING
try:
    VERBOSE_LOGGING = settings.ANALYZER_VERBOSE_LOGGING
except:
    VERBOSE_LOGGING = False

# @added 20210513 - Feature #4068: ANALYZER_SKIP
try:
    ANALYZER_SKIP = list(settings.ANALYZER_SKIP)
except:
    ANALYZER_SKIP = []

# @added 20210519 - Feature #4076: CUSTOM_STALE_PERIOD
custom_stale_metrics_hash_key = 'analyzer.metrics_manager.custom_stale_periods'
try:
    CUSTOM_STALE_PERIOD = settings.CUSTOM_STALE_PERIOD.copy()
except:
    CUSTOM_STALE_PERIOD = {}

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
            # @added 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
            # Log warning
            logger.warn('warning :: parent or current process dead')
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
            logger.debug('debug :: Memory usage spin_process start: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

        # @added 20200724 - Feature #3532: Sort all time series
        # Reduce the time use Redis pipe
        pipe = None

        # TESTING removal of p.join() from p.terminate()
        # sleep(4)

        # @added 20210520 - Branch #1444: thunder
        # Added to supplement the ran Report app up if analyzer is just running
        # long and over running.
        last_reported_up = 0
        try:
            last_reported_up = self.redis_conn_decoded.get(skyline_app)
            if not last_reported_up:
                last_reported_up = 0
            else:
                last_reported_up = int(float(last_reported_up))

        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: Analyzer could not update the Redis %s key - %s' % (
                skyline_app, e))

        # @modified 20160801 - Adding additional exception handling to Analyzer
        # Check the unique_metrics list is valid
        try:
            len(unique_metrics)
            # @added 20191016 - Branch #3262: py3
            if LOCAL_DEBUG:
                logger.debug('debug :: unique_metrics count: %s' % str(len(unique_metrics)))
        except Exception as e:
            logger.error('error :: the unique_metrics list is not valid - %s' % e)
            logger.info('nothing to do, no unique_metrics')
            return

        # @added 20201214 - Feature #3892: global_anomalies
        # TODO
        # If an event occurs that cause a near global anomalous event, handle the swamp of work that will result on the systems.
        # In terms of analysis, creating alert resources, sending alerts, Graphite requests, etc, etc
        # Add a global_anomalies feature in Analyzer and a table in the DB.
        # If analyzer encounters x anomalies in a run - start a global_anomalies event:
        # Set a global anomaly key to put the system into a global anomaly state
        # Do not send to Mirage
        # Do not send alerts
        # Do send to panorama
        # Do send global_anomalies alert
        # When analyzer encounters less than x anomalies per run for y runs:
        # Set the global anomaly end_timestamp
        # Remove the global anomaly key so the system reverts to normal
        # global_anomalies can occur per shard in a cluster
        global_anomaly_in_progress = False
        if global_anomaly_in_progress:
            logger.warning('warning :: a global anomaly event is in progress')

        # @added 20201007 - Feature #3774: SNAB_LOAD_TEST_ANALYZER
        # The number of metrics to load test Analyzer with, testing is only done
        # after the normal analysis run.
        if SNAB_LOAD_TEST_ANALYZER:
            logger.info('SNAB_LOAD_TEST_ANALYZER will run after normal analysis')
            default_snab_analyzer_load_test_start = int(time())
            snab_load_test_start_redis_key = 'snab.analyzer_load_test.start'
            try:
                snab_analyzer_load_test_start = self.redis_conn_decoded.get(snab_load_test_start_redis_key)
            except Exception as e:
                logger.error('error :: could not get Redis snab.analyzer_load_test.start key: %s' % e)
            if snab_analyzer_load_test_start:
                try:
                    snab_analyzer_load_test_start_time = int(snab_analyzer_load_test_start)
                except:
                    snab_analyzer_load_test_start_time = default_snab_analyzer_load_test_start
            snab_analyzer_load_test_unique_metrics = list(unique_metrics)

        # @added 20210513 - Feature #4068: ANALYZER_SKIP
        analyzer_skip_metrics_skipped = 0
        analyzer_skip_metrics = []
        if ANALYZER_SKIP:
            logger.info('determining ANALYZER_SKIP metrics from analyzer.metrics_manager.analyzer_skip Redis set')
            try:
                analyzer_skip_metrics = list(self.redis_conn_decoded.smembers('analyzer.metrics_manager.analyzer_skip'))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to generate a list from analyzer.metrics_manager.analyzer_skip Redis set - %s' % e)
                analyzer_skip_metrics = []
            if analyzer_skip_metrics:
                logger.info('removing %s ANALYZER_SKIP metrics from the %s unique_metrics' % (
                    str(len(analyzer_skip_metrics)), str(len(unique_metrics))))
                unique_metrics = list(set(unique_metrics) - set(analyzer_skip_metrics))
                analyzer_skip_metrics_skipped = len(set(analyzer_skip_metrics))
            else:
                logger.info('did not determine any ANALYZER_SKIP metrics from from analyzer.metrics_manager.analyzer_skip Redis set, will check dynamically')

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
            logger.debug('debug :: Memory usage spin_process after assigned_metrics: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

        # @added 20190410 - Feature #2916: ANALYZER_ENABLED setting
        if not ANALYZER_ENABLED:
            # len_assigned_metrics = len(assigned_metrics)
            logger.info('ANALYZER_ENABLED is set to %s removing the %s assigned_metrics' % (
                str(ANALYZER_ENABLED), str(len(assigned_metrics))))
            assigned_metrics = []
            del unique_metrics

        # Check if this process is unnecessary
        if len(assigned_metrics) == 0:
            logger.info('0 assigned metrics, nothing to do')
            return

        run_selected_algorithm_count = 0

        # @added 20201017 - Feature #3818: ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED
        # Enable Analyzer to sort the assigned_metrics and order them by
        # smtp_alerter_metrics first and non_smtp_alerter_metrics second, so
        # that is the MAX_ANALYZER_PROCESS_RUNTIME is reached Analyzer will
        # send any non_smtp_alerter_metrics that have not been analyzed to
        # analyzer batch
        smtp_alerter_metrics = []
        high_priority_assigned_metrics = []
        low_priority_assigned_metrics = []

        # @added 20201212 - Feature #3884: ANALYZER_CHECK_LAST_TIMESTAMP
        # all_stale_metrics is required in ANALYZER_ANALYZE_LOW_PRIORITY_METRICS and
        # ANALYZER_CHECK_LAST_TIMESTAMP so set default
        all_stale_metrics = []

        # @added 20201030 - Feature #3812: ANALYZER_ANALYZE_LOW_PRIORITY_METRICS
        determine_low_priority_metrics = False

        # @modified 20201018 - Feature #3810: ANALYZER_MAD_LOW_PRIORITY_METRICS
        # Added ANALYZER_MAD_LOW_PRIORITY_METRICS
        if ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED or ANALYZER_MAD_LOW_PRIORITY_METRICS:
            determine_low_priority_metrics = True

        # @added 20201030 - Feature #3808: ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS
        if ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS and ANALYZER_ANALYZE_LOW_PRIORITY_METRICS:
            determine_low_priority_metrics = True

        # @added 20201030 - Feature #3812: ANALYZER_ANALYZE_LOW_PRIORITY_METRICS
        if not ANALYZER_ANALYZE_LOW_PRIORITY_METRICS:
            determine_low_priority_metrics = True

        if determine_low_priority_metrics:
            try:
                try:
                    smtp_alerter_metrics = list(self.redis_conn_decoded.smembers('aet.analyzer.smtp_alerter_metrics'))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to generate a list from aet.analyzer.smtp_alerter_metrics Redis set for priority based assigned_metrics')
                    smtp_alerter_metrics = []
                unique_smtp_alerter_metrics = []
                for metric in smtp_alerter_metrics:
                    metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(metric))
                    unique_smtp_alerter_metrics.append(metric_name)
                unique_smtp_alerter_metrics_set = set(unique_smtp_alerter_metrics)
                del unique_smtp_alerter_metrics

                assigned_metrics_set = set(assigned_metrics)
                high_priority_assigned_metrics_set = assigned_metrics_set.intersection(unique_smtp_alerter_metrics_set)
                low_priority_assigned_metrics_set = assigned_metrics_set.difference(high_priority_assigned_metrics_set)
                del assigned_metrics_set
                del unique_smtp_alerter_metrics_set

                high_priority_assigned_metrics = list(high_priority_assigned_metrics_set)
                del high_priority_assigned_metrics_set
                logger.info('discovered %s high_priority_assigned_metrics in assigned_metrics' % (
                    str(len(high_priority_assigned_metrics))))

                low_priority_assigned_metrics = list(low_priority_assigned_metrics_set)
                del low_priority_assigned_metrics_set
                logger.info('discovered %s low_priority_assigned_metrics in assigned_metrics' % (
                    str(len(low_priority_assigned_metrics))))

                # @added 20201030 - Feature #3808: ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS
                # If ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS is enabled
                # the last timestamp that was analyzed for each low priority
                # metric is stored in the analyzer.low_priority_metrics.last_analyzed_timestamp
                # Redis hash set.  The low priority metrics are then sorted by
                # the oldest tmestamps so that oldest analyzed metrics are first
                # in the list.  When a low priority metric is analyzed its
                # timestamp is updated in the hash set.  This method is fast on
                # 16000 low priority metrics taking 0.017699 seconds to HGETALL
                # the set and taking 0.008169 seconds to sort the data by
                # timestamp using the PEP 256 method and taking 0.002575 seconds
                # to generate the metric_name list of 16000 metrics.
                # @modified 20210429 - Bug #4042: Handle the removal of all low priority metrics
                #                      Feature #3808: ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS
                # Added low_priority_assigned_metrics to handle if all low priority
                # metrics are removed.
                # if ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS and ANALYZER_ANALYZE_LOW_PRIORITY_METRICS:
                if ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS and ANALYZER_ANALYZE_LOW_PRIORITY_METRICS and low_priority_assigned_metrics:
                    logger.info('reordering low priority metrics by oldest analyzed timestamp from Redis hash key - %s' % (
                        low_priority_metrics_hash_key))
                    low_priority_metrics_last_analyzed = []
                    try:
                        low_priority_metrics_last_analyzed = self.redis_conn_decoded.hgetall(low_priority_metrics_hash_key)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to get Redis hash key %s to sort low priority metrics on last analyzed timestamp' % (
                            low_priority_metrics_hash_key))
                        low_priority_metrics_last_analyzed = []
                    sorted_low_priority_metrics_last_analyzed = []
                    if low_priority_metrics_last_analyzed:
                        try:
                            sorted_low_priority_metrics_last_analyzed = sorted(low_priority_metrics_last_analyzed.items(), key=itemgetter(1), reverse=False)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to sort low_priority_metrics_last_analyzed by timestamp values')
                            sorted_low_priority_metrics_last_analyzed = []
                    low_priority_analyzed_metrics = []
                    if sorted_low_priority_metrics_last_analyzed:
                        logger.info('%s metrics by oldest analyzed timestamp' % (
                            str(len(sorted_low_priority_metrics_last_analyzed))))
                        try:
                            low_priority_analyzed_metrics = [item[0] for item in sorted_low_priority_metrics_last_analyzed]
                            logger.info('reordered low priority metrics by oldest analyzed timestamp from Redis hash key - %s' % (
                                low_priority_metrics_hash_key))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to generate list of metric names from sorted_low_priority_metrics_last_analyzed')
                        if low_priority_analyzed_metrics:
                            logger.info('removing reordered low priority metrics that are not assigned to this process')
                            low_priority_analyzed_metrics_count = len(low_priority_analyzed_metrics)
                            low_priority_assigned_metrics_count = 0
                            assigned_low_priority_analyzed_metrics = []
                            for base_name in low_priority_analyzed_metrics:
                                if base_name in low_priority_assigned_metrics:
                                    assigned_low_priority_analyzed_metrics.append(base_name)
                                    low_priority_assigned_metrics_count += 1
                            if assigned_low_priority_analyzed_metrics:
                                removed_metrics_count = low_priority_analyzed_metrics_count - low_priority_assigned_metrics_count
                                low_priority_analyzed_metrics = assigned_low_priority_analyzed_metrics
                                logger.info('removed %s low priority metrics that are not assigned to this process' % str(removed_metrics_count))

                    # Remove Boring, Stale and TooShort metrics
                    # The method in use here is fast enough, on 16000 metrics it
                    # reorders the list by current metrics and the exception
                    # metrics in 0.842042 seconds.  Because Analyzer refreshes
                    # the Redis exception sets frequently, these metrics will
                    # be added back to be reassessed frequently as well, but
                    # only once in every 300 seconds, this should result in no
                    # low priority metrics being left in a perpetual exception
                    # state, unless they are in that state.
                    if low_priority_analyzed_metrics:
                        boring_metrics = []
                        try:
                            all_boring_metrics = list(self.redis_conn_decoded.smembers('aet.analyzer.boring'))
                            for base_name in all_boring_metrics:
                                if base_name in low_priority_assigned_metrics:
                                    boring_metrics.append(base_name)
                        except:
                            boring_metrics = []
                        stale_metrics = []
                        try:
                            all_stale_metrics = list(self.redis_conn_decoded.smembers('aet.analyzer.stale'))
                            for base_name in all_stale_metrics:
                                if base_name in low_priority_assigned_metrics:
                                    stale_metrics.append(base_name)
                        except:
                            stale_metrics = []
                        tooshort_metrics = []
                        try:
                            all_tooshort_metrics = list(self.redis_conn_decoded.smembers('aet.analyzer.tooshort'))
                            for base_name in all_tooshort_metrics:
                                if base_name in low_priority_assigned_metrics:
                                    tooshort_metrics.append(base_name)
                        except:
                            tooshort_metrics = []
                        low_priority_metric_exception_base_names = boring_metrics + stale_metrics + tooshort_metrics
                        low_priority_metric_exception_metrics_names_to_remove = []
                        if low_priority_metric_exception_base_names:
                            low_priority_metric_exception_base_names_set = set(low_priority_metric_exception_base_names)
                            for base_name in low_priority_metric_exception_base_names_set:
                                metric_name = '%s%s' % (settings.FULL_NAMESPACE, base_name)
                                low_priority_metric_exception_metrics_names_to_remove.append(metric_name)
                            try:
                                del low_priority_metric_exception_metrics_to_remove_set
                            except:
                                pass
                            try:
                                del low_priority_metric_exception_base_names
                            except:
                                pass
                        if low_priority_metric_exception_metrics_names_to_remove:
                            logger.info('%s boring, slate or tooshort low priority metrics being removed and added to the end of low_priority_analyzed_metrics' % (
                                str(len(low_priority_metric_exception_metrics_names_to_remove))))
                            current_low_priority_analyzed_metrics = []
                            exception_metrics = []
                            for metric in low_priority_analyzed_metrics:
                                if metric not in low_priority_metric_exception_metrics_names_to_remove:
                                    current_low_priority_analyzed_metrics.append(metric)
                                else:
                                    exception_metrics.append(metric)
                            if current_low_priority_analyzed_metrics:
                                low_priority_analyzed_metrics = current_low_priority_analyzed_metrics + exception_metrics
                    # Determine and add any missing metrics adding them first
                    # so they are analyzed
                    if low_priority_analyzed_metrics:
                        missing_low_priority_metrics = []
                        for metric in low_priority_assigned_metrics:
                            if metric not in low_priority_analyzed_metrics:
                                missing_low_priority_metrics.append(metric)
                        if missing_low_priority_metrics:
                            try:
                                self.redis_conn.sadd('analyzer.missing_low_priority_metrics', *set(missing_low_priority_metrics))
                            except:
                                pass
                            low_priority_analyzed_metrics = missing_low_priority_metrics + low_priority_analyzed_metrics
                            logger.info('added %s unknown low priority metrics to low_priority_analyzed_metrics to be analyzed first' % (
                                str(len(missing_low_priority_metrics))))
                        low_priority_assigned_metrics = low_priority_analyzed_metrics
                        try:
                            self.redis_conn.sadd('analyzer.low_priority_metrics.count', len(low_priority_assigned_metrics))
                        except:
                            pass
                if high_priority_assigned_metrics or low_priority_assigned_metrics:
                    assigned_metrics = high_priority_assigned_metrics + low_priority_assigned_metrics
                    logger.info('reordered assigned_metrics by priority, %s high priority alerting metrics and %s low priority non alerting metrics' % (
                        str(len(high_priority_assigned_metrics)),
                        str(len(low_priority_assigned_metrics))))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to generate a priority based assigned_metrics list')

        # @added 20201030 - Feature #3812: ANALYZER_ANALYZE_LOW_PRIORITY_METRICS
        # If analysis has been disabled on low priority metrics via
        # ANALYZER_ANALYZE_LOW_PRIORITY_METRICS = False, remove all low priority
        # metrics created the asssigned_metrics from high priority metrics only.
        if not ANALYZER_ANALYZE_LOW_PRIORITY_METRICS:
            try:
                if high_priority_assigned_metrics:
                    assigned_metrics = high_priority_assigned_metrics
                    low_priority_metrics_count = 0
                    if low_priority_assigned_metrics:
                        low_priority_metrics_count = len(low_priority_assigned_metrics)
                    logger.info('assigned_metrics composed of %s high priority alerting metrics only, discarded %s low priority non alerting metrics discarded as ANALYZER_ANALYZE_LOW_PRIORITY_METRICS is False' % (
                        str(len(high_priority_assigned_metrics)),
                        str(low_priority_metrics_count)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to generate a high priority based assigned_metrics list only')

        # Multi get series
        # @modified 20160801 - Adding additional exception handling to Analyzer
        raw_assigned_failed = True
        try:
            raw_assigned = self.redis_conn.mget(assigned_metrics)
            raw_assigned_failed = False
            if LOCAL_DEBUG:
                logger.debug('debug :: Memory usage spin_process after raw_assigned: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to get raw_assigned from Redis - %s' % e)

        # Make process-specific dicts
        exceptions = defaultdict(int)
        anomaly_breakdown = defaultdict(int)

        # @added 20160803 - Adding additional exception handling to Analyzer
        if raw_assigned_failed:
            logger.info('No raw_assigned set, returning')
            return

        logger.info('got raw_assigned metric data from Redis for %s assigned metrics' % str(len(assigned_metrics)))

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

            logger.info('checking for mirage periodic metrics to add')

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

                # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
                # base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                if metric_name.startswith(settings.FULL_NAMESPACE):
                    base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                else:
                    base_name = metric_name

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

        # @added 20201111 - Feature #3480: batch_processing
        #                   Bug #2050: analyse_derivatives - change in monotonicity
        derivative_z_metric_keys_existing = []
        derivative_z_metric_keys_removed = []
        derivative_z_metric_keys_added = []
        metrics_added_to_derivative_metrics = []
        metrics_removed_from_derivative_metrics = []
        metrics_added_to_non_derivative_metrics = []

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
            except Exception as e:
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

        # @added 20201111 - Feature #3480: batch_processing
        #                   Bug #2050: analyse_derivatives - change in monotonicity
        # Added sets to track additions and removes
        if not manage_derivative_metrics and i == 1:
            logger.info('derivative metrics are being managed - spin_process %s is deleting analyzer.derivative metrics. Redis sets' % str(i))
            delete_redis_sets = [
                'analyzer.derivative_metrics_removed',
                'analyzer.z_derivative_metrics_added',
                'analyzer.z_derivative_metrics_removed',
            ]
            for i_redis_set in delete_redis_sets:
                redis_set_to_delete = i_redis_set
                try:
                    self.redis_conn.delete(redis_set_to_delete)
                    logger.info('deleted Redis set - %s' % redis_set_to_delete)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to delete Redis set - %s' % redis_set_to_delete)
        try:
            # @modified 20200606 - Bug #3572: Apply list to settings import
            # non_derivative_monotonic_metrics = settings.NON_DERIVATIVE_MONOTONIC_METRICS
            non_derivative_monotonic_metrics = list(settings.NON_DERIVATIVE_MONOTONIC_METRICS)
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
            non_smtp_alerter_metrics = list(self.redis_conn_decoded.smembers('aet.analyzer.non_smtp_alerter_metrics'))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to generate a list from aet.analyzer.non_smtp_alerter_metrics Redis set')
            non_smtp_alerter_metrics = []

        # @added 20200527 - Feature #3550: flux.uploaded_data_worker
        # If data has been uploaded ignoring submitted timestamps then the Redis
        # time series data needs to be sorted and deduplicated
        flux_upload_metrics_to_sort_and_deduplicate = []
        try:
            flux_upload_metrics_to_sort_and_deduplicate = list(self.redis_conn_decoded.smembers('flux.sort_and_dedup.metrics'))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to generate a list from flux.sort_and_dedup.metrics Redis set')
            flux_upload_metrics_to_sort_and_deduplicate = []

        # @added 20200604 - Feature #3570: Mirage - populate_redis
        mirage_filled_metrics_to_sort_and_deduplicate = []
        try:
            mirage_filled_metrics_to_sort_and_deduplicate = list(self.redis_conn_decoded.smembers('mirage.filled'))
            if mirage_filled_metrics_to_sort_and_deduplicate:
                logger.info('determined %s metrics from mirage.filled Redis set' % str(len(mirage_filled_metrics_to_sort_and_deduplicate)))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to generate a list from mirage.filled Redis set')
            mirage_filled_metrics_to_sort_and_deduplicate = []

        # @added 20200611 - Feature #3578: Test alerts
        test_alerts = []
        test_alerts_redis_set = '%s.test_alerts' % skyline_app
        try:
            test_alerts = list(self.redis_conn_decoded.smembers(test_alerts_redis_set))
            if test_alerts:
                logger.info('determined %s test alerts to send from %s Redis set - %s' % (
                    str(len(test_alerts)), test_alerts_redis_set, str(test_alerts)))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to generate a list from %s Redis set' % test_alerts_redis_set)
            test_alerts = []

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
                logger.error(traceback.format_exc())
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

        # @added 20201017 - Feature #3818: ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED
        low_priority_time_elasped = False
        low_priority_metrics_sent_to_analyzer_batch = 0
        skipped_low_priority_metrics_in_analyzer_batch_queue = 0
        logged_high_priority_run_time = False
        analyzer_batch_work = None
        analyzer_batch_queued_metrics = []
        if ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED:
            redis_set = 'analyzer.batch'
            try:
                analyzer_batch_work = self.redis_conn_decoded.smembers(redis_set)
            except Exception as e:
                logger.error('error :: could not query Redis for set %s - %s' % (redis_set, e))
                analyzer_batch_work = None
            if analyzer_batch_work:
                for index, analyzer_batch in enumerate(analyzer_batch_work):
                    try:
                        batch_processing_metric = literal_eval(analyzer_batch)
                        analyzer_batch_queued_metrics.append(str(batch_processing_metric[0]))
                    except:
                        pass
            if analyzer_batch_queued_metrics:
                analyzer_batch_queued_metrics = list(set(analyzer_batch_queued_metrics))
        # @added 20201018 - Feature #3810: ANALYZER_MAD_LOW_PRIORITY_METRICS
        low_priority_metrics_analysed_due_to_mad_trigger = 0
        mad_error_logged = False

        # @added 20201127 - Feature #3848: custom_algorithms - run_before_3sigma parameter
        # Determine if any custom_algorithms are to be run by analyzer so that
        # DEBUG_CUSTOM_ALGORITHMS only runs if a custom_algorithm is assigned to
        # analyzer
        run_custom_algorithm_enabled_on_app = None
        if CUSTOM_ALGORITHMS and DEBUG_CUSTOM_ALGORITHMS:
            for custom_algorithm in CUSTOM_ALGORITHMS:
                try:
                    use_with = CUSTOM_ALGORITHMS[custom_algorithm]['use_with']
                    if skyline_app in use_with:
                        run_custom_algorithm_enabled_on_app = True
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to determine if custom_algorithms are to be run with %s' % (
                        skyline_app))

        # @added 20201212 - Feature #3884: ANALYZER_CHECK_LAST_TIMESTAMP
        metrics_last_timestamp_dict = {}
        metrics_without_new_data = []
        metrics_with_new_data = []
        metrics_added_to_last_timestamp_hash_key = []
        if ANALYZER_CHECK_LAST_TIMESTAMP:
            try:
                metrics_last_timestamp_dict = self.redis_conn_decoded.hgetall(metrics_last_timestamp_hash_key)
                if metrics_last_timestamp_dict:
                    logger.info('ANALYZER_CHECK_LAST_TIMESTAMP - got %s metrics and last analysed timestamps from %s Redis hash key' % (
                        str(len(metrics_last_timestamp_dict)),
                        metrics_last_timestamp_hash_key))
                else:
                    logger.warn('warning :: ANALYZER_CHECK_LAST_TIMESTAMP enabled but got no data from the %s Redis hash key' % (
                        metrics_last_timestamp_hash_key))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get Redis hash key %s' % (
                    metrics_last_timestamp_hash_key))
                metrics_last_timestamp_dict = {}
            if not all_stale_metrics:
                # If all_stale_metrics were not determined for ANALYZER_ANALYZE_LOW_PRIORITY_METRICS
                # get them
                try:
                    all_stale_metrics = list(self.redis_conn_decoded.smembers('aet.analyzer.stale'))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to get Redis key aet.analyzer.stale for ANALYZER_CHECK_LAST_TIMESTAMP')
                    all_stale_metrics = []

        # @added 20210519 - Feature #4076: CUSTOM_STALE_PERIOD
        # In Analyzer the metrics_manager analyzer.metrics_manager.custom_stale_periods
        # Redis hash key is used to determine metrics that have a custom stale
        # period defined, looked up once and passed to run_selected_algorithms.
        # In Boundary the direct settings CUSTOM_STALE_PERIOD dict is checked
        # for each metric a custom stale period.
        metrics_updated_in_last_timeseries_timestamp_hash_key_count = 0
        custom_stale_metrics_dict = {}
        try:
            custom_stale_metrics_dict = self.redis_conn_decoded.hgetall(custom_stale_metrics_hash_key)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to create custom_stale_metrics_dict from Redis hash key %s - %s' % (
                custom_stale_metrics_hash_key, e))
        # @added 20210616 - Branch #1444: thunder
        #                   Feature #4076: CUSTOM_STALE_PERIOD
        # Surface the last_timeseries_timestamp Redis hash key so that the
        # timestamps can be compared as Thunder stale_metrics requires all
        # timestamps for all metrics
        metrics_last_timeseries_timestamp_dict = {}
        metrics_last_timeseries_timestamp_hash_key = 'analyzer.metrics.last_timeseries_timestamp'
        try:
            metrics_last_timeseries_timestamp_dict = self.redis_conn_decoded.hgetall(metrics_last_timeseries_timestamp_hash_key)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to create metrics_last_timeseries_timestamp_dict from Redis hash key %s - %s' % (
                metrics_last_timeseries_timestamp_hash_key, e))

        logger.info('checking %s assigned_metrics' % str(len(assigned_metrics)))

        # Distill timeseries strings into lists
        for i, metric_name in enumerate(assigned_metrics):
            self.check_if_parent_is_alive()

            # @added 20210513 - Feature #4068: ANALYZER_SKIP
            if ANALYZER_SKIP and not analyzer_skip_metrics:
                pattern_match, metric_matched_by = matched_or_regexed_in_list('analyzer', metric_name, ANALYZER_SKIP)
                if pattern_match:
                    analyzer_skip_metrics_skipped += 1
                    continue

            # @added 20191016 - Branch #3262: py3
            if LOCAL_DEBUG:
                logger.debug('debug :: checking %s' % str(metric_name))

            # @added 20201030 - Feature #3808: ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS
            #                   ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED
            low_priority_metric = False
            if low_priority_assigned_metrics and ANALYZER_MANAGE_LOW_PRIORITY_METRICS:
                if metric_name not in high_priority_assigned_metrics:
                    low_priority_metric = True
                    if not logged_high_priority_run_time:
                        logged_high_priority_run_time = (time() - spin_start)
                        logger.info('all high priority alerting metrics analysed in %.2f seconds' % logged_high_priority_run_time)
                if not low_priority_time_elasped:
                    time_elasped = int(time()) - spin_start
                    if time_elasped > (settings.MAX_ANALYZER_PROCESS_RUNTIME - 15):
                        low_priority_time_elasped = True
            # @modified 20210430 - Bug #4042: Handle the removal of all low priority metrics
            #                      Feature #3808: ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS
            # Added low_priority_metric to the condition
            if low_priority_time_elasped and ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS and low_priority_metric:
                logger.info('ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS - low_priority_time_elasped has been reached, not analysing any more metrics')
                break

            # @added 20210520 - Branch #1444: thunder
            # Added to supplement the ran Report app up if analyzer is just running
            # long and over running.
            update_analyzer_up_key = False
            right_now = int(time())
            if not last_reported_up:
                update_analyzer_up_key = True
            else:
                try:
                    if right_now > (last_reported_up + 60):
                        update_analyzer_up_key = True
                except Exception as e:
                    logger.error('error :: Analyzer could not determine if last_reported_up time is exceeded - %s' % (
                        e))
            if update_analyzer_up_key:
                # Report app up
                try:
                    self.redis_conn.setex(skyline_app, 120, right_now)
                    last_reported_up = right_now
                except Exception as e:
                    logger.error('error :: Analyzer could not update the Redis %s key - %s' % (
                        skyline_app, e))

            # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
            # base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
            if metric_name.startswith(settings.FULL_NAMESPACE):
                base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
            else:
                base_name = metric_name

            # @added 20201017 - Feature #3818: ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED
            if ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED:
                if metric_name in analyzer_batch_queued_metrics:
                    skipped_low_priority_metrics_in_analyzer_batch_queue += 1
                    continue
                if low_priority_time_elasped:
                    last_metric_timestamp_key = 'last_timestamp.%s' % base_name
                    try:
                        last_metric_timestamp = int(self.redis_conn.get(last_metric_timestamp_key))
                    except:
                        last_metric_timestamp = None
                    if last_metric_timestamp:
                        data = [metric_name, last_metric_timestamp]
                        redis_set = 'analyzer.batch'
                        try:
                            self.redis_conn.sadd(redis_set, str(data))
                            sent_to_analyzer_batch_proccessing_metrics += 1
                            low_priority_metrics_sent_to_analyzer_batch += 1
                            continue
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: batch processing - failed to add %s to %s Redis set' % (
                                str(data), redis_set))

            try:
                raw_series = raw_assigned[i]
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)
            except Exception as e:
                logger.error('error :: failed to unpack %s timeseries - %s' % (
                    str(base_name), e))
                timeseries = []

            # @added 20200506 - Feature #3532: Sort all time series
            # To ensure that there are no unordered timestamps in the time
            # series which are artefacts of the collector or carbon-relay, sort
            # all time series by timestamp before analysis.
            original_timeseries = timeseries
            if original_timeseries:
                timeseries = sort_timeseries(original_timeseries)
                del original_timeseries

            last_timeseries_timestamp = 0

            # @added 20200427 - Feature #3514: Identify inactive metrics
            # Added metrics that have become inactive to the Redis set to be flagged
            # as inactive
            inactive_metric = False
            if timeseries:
                try:
                    last_timeseries_timestamp = int(timeseries[-1][0])
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

            # @added 20210519 - Feature #4076: CUSTOM_STALE_PERIOD
            #                   Branch #1444: thunder
            # Maintain a Redis hash key of the last timestamp of all metrics and
            # only update the hash key if the timestamp is recent.  This Redis
            # hash key is pruned in metrics_manager when an entry has a
            # timestamp older that now - FULL_DURATION
            if last_timeseries_timestamp:
                # @modified 20210616 - Branch #1444: thunder
                #                      Feature #4076: CUSTOM_STALE_PERIOD
                # Thunder stale_metrics check requires all metrics to have their
                # timestamps recorded in the hash key
                update_last_timestamp_hash_key = False
                if metrics_last_timeseries_timestamp_dict:
                    last_hash_key_timestamp = None
                    try:
                        last_hash_key_timestamp = int(float(metrics_last_timeseries_timestamp_dict[base_name]))
                    except KeyError:
                        update_last_timestamp_hash_key = True
                    except:
                        update_last_timestamp_hash_key = True
                    if last_hash_key_timestamp:
                        if last_hash_key_timestamp < last_timeseries_timestamp:
                            update_last_timestamp_hash_key = True
                # if last_timeseries_timestamp > (int(spin_start) - 260):
                if update_last_timestamp_hash_key:
                    try:
                        self.redis_conn.hset('analyzer.metrics.last_timeseries_timestamp', base_name, last_timeseries_timestamp)
                        metrics_updated_in_last_timeseries_timestamp_hash_key_count += 1
                    except Exception as e:
                        logger.error('error :: failed to update metric timestamp in Redis analyzer.metrics.last_timeseries_timestamp hash key - %s' % e)

            # @added 20170602 - Feature #2034: analyse_derivatives
            # In order to convert monotonic, incrementing metrics to a deriative
            # metric
            known_derivative_metric = False
            unknown_deriv_status = True

            # @modified 20200529 - Feature #3480: batch_processing
            #                      Bug #2050: analyse_derivatives - change in monotonicity
            # Switch the order in which they are checked and do not check if
            # not manage_derivative_metrics as will only be set to True anyway
            # if metric_name in non_derivative_metrics:
            #     unknown_deriv_status = False
            # if unknown_deriv_status:
            #     if metric_name in derivative_metrics:
            #         known_derivative_metric = True
            #         unknown_deriv_status = False
            if metric_name in derivative_metrics:
                known_derivative_metric = True
                unknown_deriv_status = False
            if unknown_deriv_status:
                if metric_name in non_derivative_metrics:
                    unknown_deriv_status = False

            # This is here to refresh the sets
            if not manage_derivative_metrics:
                unknown_deriv_status = True

            # @added 20200609 - Feature #3570: Mirage - populate_redis
            #                   Bug #3448: Repeated airgapped_metrics
            #                   Feature #3400: Identify air gaps in the metric data
            # Defined this outside the if IDENTIFY_AIRGAPS and flux_filled_keys
            # scope below as is checked it the
            # if IDENTIFY_UNORDERED_TIMESERIES and analyzer_unordered_timeseries
            # block too which can also set sort = True, but now so can the
            # mirage_filled variable and the
            # mirage_filled_metrics_to_sort_and_deduplicate, so there is 3
            # conditions now and in the if not mirage_filled check, it fails to
            # interpolate this variable.
            metric_flux_filled_key = 'flux.filled.%s' % str(base_name)

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
                # @modified 20200609 - Feature #3570: Mirage - populate_redis
                # Define outside this conditional scope above.
                # metric_flux_filled_key = 'flux.filled.%s' % str(base_name)
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

            # @added 20200527 - Feature #3550: flux.uploaded_data_worker
            # If data has been uploaded ignoring submitted timestamps then the
            # Redis time series data needs to be sorted and deduplicated
            if base_name in flux_upload_metrics_to_sort_and_deduplicate:
                sort_data = True
                try:
                    self.redis_conn.srem('flux.sort_and_dedup.metrics', base_name)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to remove %s from flux.sort_and_dedup.metrics Redis set' % base_name)

            # @added 20200604 - Feature #3570: Mirage - populate_redis
            mirage_filled = False
            if base_name in mirage_filled_metrics_to_sort_and_deduplicate:
                sort_data = True
                mirage_filled = True
                try:
                    self.redis_conn.srem('mirage.filled', base_name)
                    logger.info('removed %s from mirage.filled Redis set' % base_name)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to remove %s from mirage.filled Redis set' % base_name)

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
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to sort and deduplicate flux/mirage filled timeseries for %s' % str(metric_name))
                if not sorted_and_deduplicated_timeseries:
                    logger.error('error :: failed to sort and deduplicate flux/mirage filled timeseries for %s' % str(metric_name))

                # @added 20200804 - Bug #3660: Handle flux and flux.filled lags
                #                   Feature #3400: Identify air gaps in the metric data
                # In cases where the Skyline instance is not processing all the
                # metrics within ANALYZER_OPTIMUM_RUN_DURATION and flux is
                # constantly filling or populating metrics and IDENTIFY_AIRGAPS
                # is enabled, analyzer can get into a state where:
                # - Sorting and de-duplicating flux metrics is never achieved on
                #   a number of metrics as there are new data points added during
                #   the processing period.
                # - Analyzer never gets through the processing in ANALYZER_OPTIMUM_RUN_DURATION
                #   as it is sorting and de-duplicating every flux metric. This
                #   results in all the sort and de-duplication processing being
                #   ineffective and unless.
                # Solution - Add any missing data points and timestamps to the
                # sorted and de-duplicated time series and use it, rather than
                # discarding it. This can result in dropped data points. However
                # if INDENTIFY_AIRGAPS is enabled, this will be mitigated as
                # metric will probably be reconciled in a subsequent run to fill
                # any airgap created by possibly dropping any unordered data
                # points/timestamps.

                # @added 20200723 - Feature #3532: Sort all time series
                # Reduce the time. Check if the last timestamps are equal before
                # submitting to Redis as if they are not at this point then they
                # will not be after
                try:
                    last_new_sort_ts = int(sorted_and_deduplicated_timeseries[-1][0])
                    if last_timeseries_timestamp != last_new_sort_ts:
                        # @modified 20200804 - Bug #3660: Handle flux and flux.filled lags
                        #                      Feature #3400: Identify air gaps in the metric data
                        # Do not discard the sorted_and_deduplicated_timeseries
                        # just append missing data points to it.
                        # logger.info('the sorted and deduplicated last timestamp (%s) does not match the current data last timestamp (%s) before populating Redis, skipping %s' % (
                        #     str(last_new_sort_ts), str(last_timeseries_timestamp), str(metric_name)))
                        # sorted_and_deduplicated_timeseries = []
                        logger.info('the sorted and deduplicated last timestamp (%s) does not match the current data last timestamp (%s) before populating Redis, appending to %s' % (
                            str(last_new_sort_ts), str(last_timeseries_timestamp), str(metric_name)))
                        # @added 20200804 - Bug #3660: Handle flux and flux.filled lags
                        #                   Feature #3400: Identify air gaps in the metric data
                        # Just append
                        # In cases where the Skyline instance is not processing all the
                        # metrics within ANALYZER_OPTIMUM_RUN_DURATION and flux is
                        # constantly filling or populating metrics and IDENTIFY_AIRGAPS
                        # is enabled, analyzer can get into a state where:
                        # - Sorting and de-duplicating flux metrics is never achieved on
                        #   a number of metrics as there are new data points added during
                        #   the processing period.
                        # - Analyzer never gets through the processing in ANALYZER_OPTIMUM_RUN_DURATION
                        #   as it is sorting and de-duplicating every flux metric. This
                        #   results in all the sort and de-duplication processing being
                        #   ineffective and unless.
                        # Solution - Add any missing data points and timestamps to the
                        # sorted and de-duplicated time series and use it, rather than
                        # discarding it. This can result in dropped data points. However
                        # if INDENTIFY_AIRGAPS is enabled, this will be mitigated as
                        # metric will probably be reconciled in a subsequent run to fill
                        # any airgap created by possibly dropping any unordered data
                        # points/timestamps.
                        try:
                            last_ten_datapoints_in_current_redis_ts = timeseries[-60:]
                            last_ten_sorted_current_redis_ts = sorted(last_ten_datapoints_in_current_redis_ts, key=lambda x: x[0])
                            last_ten_datapoints_in_sorted_and_deduplicated_timeseries = sorted_and_deduplicated_timeseries[-60:]
                            last_ten_ts_in_sorted_and_deduplicated_timeseries = [x[0] for x in last_ten_datapoints_in_sorted_and_deduplicated_timeseries]
                            appended_count = 0
                            for ts, value in last_ten_sorted_current_redis_ts:
                                if ts not in last_ten_ts_in_sorted_and_deduplicated_timeseries:
                                    sorted_and_deduplicated_timeseries.append((ts, value))
                                    appended_count += 1
                            logger.info('appended %s new data points to the sorted_and_deduplicated_timeseries for %s' % (
                                str(appended_count), str(metric_name)))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to append new data points to the sorted_and_deduplicated_timeseries')

                    else:
                        logger.info('the sorted and deduplicated last timestamp (%s) matches the current data last timestamp (%s) before populating Redis on %s' % (
                            str(last_new_sort_ts), str(last_timeseries_timestamp), str(metric_name)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to determine for timestamps match before populating Redis')

            # Recreate the Redis key sorted and deduplicated and feed to
            # Redis ala Horizon worker method more or less
            if sorted_and_deduplicated_timeseries:
                new_metric_name_key = 'analyzer.sorted.deduped.%s' % str(metric_name)
                logger.info('populating Redis key %s with sorted and deduplicated data' % str(new_metric_name_key))
                try:
                    # @modified 20200723 - Feature #3532: Sort all time series
                    # Reduce the time.  Using pipe is 10x faster and keeps the
                    # shape of the time series
                    if not pipe:
                        pipe = self.redis_conn.pipeline()
                    # metric_ts_data = []
                    for datapoint in sorted_and_deduplicated_timeseries:
                        metric = (new_metric_name_key, (datapoint[0], datapoint[1]))
                        # self.redis_conn.append(str(new_metric_name_key), packb(metric[1]))
                        # metric_ts_data.append((datapoint[0], datapoint[1]))
                        pipe.append(str(new_metric_name_key), packb(metric[1]))
                    # self.redis_conn.set(str(new_metric_name_key), packb(metric_ts_data))
                    pipe.execute()
                    # del metric_ts_data
                    populated_redis_key = True
                except:
                    logger.error(traceback.format_exc())
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
                        logger.error(traceback.format_exc())
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
                            logger.error(traceback.format_exc())
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
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to determine for timestamps match')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to comparing most recent current timestamp with sorted and deduplicated data for %s' % str(metric_name))
                if not verified_existing_key_data:
                    try:
                        self.redis_conn.delete(new_metric_name_key)
                    except:
                        logger.error(traceback.format_exc())
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
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to rename Redis key %s to %s' % (
                            str(metric_name), metric_key_to_delete))
                        try:
                            logger.info('deleting key %s' % (new_metric_name_key))
                            self.redis_conn.delete(new_metric_name_key)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to delete Redis key %s' % (
                                new_metric_name_key))
                new_key_renamed = False
                if original_key_renamed:
                    try:
                        logger.info('renaming key %s to %s' % (new_metric_name_key, metric_name))
                        self.redis_conn.rename(new_metric_name_key, metric_name)
                        new_key_renamed = True
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to rename Redis key %s to %s' % (
                            str(new_metric_name_key), metric_name))
                        try:
                            logger.info('reverting by renaming key %s to %s' % (metric_key_to_delete, metric_name))
                            self.redis_conn.rename(metric_key_to_delete, metric_name)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to rename Redis key %s to %s' % (
                                str(metric_key_to_delete), metric_name))
                        try:
                            self.redis_conn.delete(new_metric_name_key)
                        except:
                            logger.error(traceback.format_exc())
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
                        logger.error(traceback.format_exc())
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

                        # @added 20200501 - Feature #3532: Sort all time series
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
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to determine if timestamps match with the renamed key - %s' % e)
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
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to determine datapoints to append to the new key - %s' % e)
                        if new_datapoints:
                            try:
                                for datapoint in new_datapoints:
                                    metric = (metric_name, (datapoint[0], datapoint[1]))
                                    self.redis_conn.append(metric_name, packb(metric[1]))
                                # @added 20200501 - Feature #3532: Sort all time series
                                get_updated_redis_timeseries = True
                            except Exception as e:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to populate Redis key %s with new data - %s' % (
                                    str(metric_name), e))
                    try:
                        logger.info('deleting key %s' % (metric_key_to_delete))
                        self.redis_conn.delete(metric_key_to_delete)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to delete Redis key %s - %s' % (
                            metric_key_to_delete, e))

                    # @modified 20200604 - Feature #3570: Mirage - populate_redis
                    if not mirage_filled:
                        try:
                            logger.info('Redis time series key data sorted and ordered with Flux additions, deleting key %s' % (metric_flux_filled_key))
                            self.redis_conn.delete(metric_flux_filled_key)
                            get_updated_redis_timeseries = True
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to delete Redis key %s - %s' % (
                                metric_key_to_delete, e))

            if get_updated_redis_timeseries:
                updated_timeseries = []
                try:
                    raw_series = self.redis_conn.get(metric_name)
                    unpacker = Unpacker(use_list=False)
                    unpacker.feed(raw_series)
                    updated_timeseries = list(unpacker)
                except Exception as e:
                    logger.error('error :: failed to unpack timeseries for %s - %s' % (
                        metric_name, e))
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

            # @added 20170617 - Bug #2050: analyse_derivatives - change in monotonicity
            # First check if it has its own Redis z.derivative_metric key
            # that has not expired
            derivative_metric_key = 'z.derivative_metric.%s' % str(base_name)

            # @added 20200529 - Feature #3480: batch_processing
            #                   Bug #2050: analyse_derivatives - change in monotonicity
            # When a monotonic metric changes in the last run before a
            # manage_derivative_metrics run, when manage_derivative_metrics runs
            # it classifies it and adds it to non_derivative_metrics, the only
            # way to stop this is check the key for each metric.  Further if the
            # metric is a batch processing metric, then this is required before
            # batch_processing so this was moved out of the below
            # if unknown_deriv_status block
            last_derivative_metric_key = None
            try:
                last_derivative_metric_key = self.redis_conn_decoded.get(derivative_metric_key)
            except Exception as e:
                logger.error('error :: could not query Redis for last_derivative_metric_key: %s' % e)
            if last_derivative_metric_key:
                known_derivative_metric = True
                # @added 20201111 - Feature #3480: batch_processing
                #                   Bug #2050: analyse_derivatives - change in monotonicity
                derivative_z_metric_keys_existing.append(base_name)

            # @added 20201111 - Feature #3480: batch_processing
            #                   Bug #2050: analyse_derivatives - change in monotonicity
            # Unset the unknown_deriv_status if the derivate metrics are being
            # refreshed
            if not manage_derivative_metrics:
                unknown_deriv_status = True

            if unknown_deriv_status:
                # @added 20170617 - Bug #2050: analyse_derivatives - change in monotonicity
                # @modified 20200529 - Feature #3480: batch_processing
                #                      Bug #2050: analyse_derivatives - change in monotonicity
                # Always check, moved to above outside this condition
                # last_derivative_metric_key = False
                # try:
                #     last_derivative_metric_key = self.redis_conn.get(derivative_metric_key)
                # except Exception as e:
                #     logger.error('error :: could not query Redis for last_derivative_metric_key: %s' % e)

                # @modified 20200529 - Feature #3480: batch_processing
                #                      Bug #2050: analyse_derivatives - change in monotonicity
                # Apply skip_derivative
                skip_derivative = in_list(base_name, non_derivative_monotonic_metrics)
                is_strictly_increasing_monotonically = False
                if not skip_derivative:
                    is_strictly_increasing_monotonically = strictly_increasing_monotonicity(timeseries)

                    # @added 20201111 - Feature #3480: batch_processing
                    #                   Bug #2050: analyse_derivatives - change in monotonicity
                    # Only assign as derivative metric if it evaluates as
                    # strictly_increasing_monotonicity more than once.  This
                    # prevents metrics which are for example at 0 all day then
                    # record a 1 from being classified as monotonic if the
                    # classification happens to occur when the metric is at 1,
                    # on the next classification runs if the metric is still at
                    # 1 or increasing then it will be classified as monotonic.
                    # However if the metric returns to 0, this requirement to
                    # be classified multiple times ensures it is not incorrectly
                    # classified as monotonic.
                    if not last_derivative_metric_key:
                        running_derivative_metric_key = 'zz.derivative_metric.%s' % str(base_name)

                        # @added 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
                        # Handle key not existing and exception on last_key_value
                        # if None
                        last_key_value_data = None

                        last_key_value = 0
                        monotonic_count = 0
                        try:
                            # @modified 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
                            # Handle key not existing and exception on last_key_value
                            # if None
                            # last_key_value = self.redis_conn_decoded.get(running_derivative_metric_key)
                            last_key_value_data = self.redis_conn_decoded.get(running_derivative_metric_key)
                        except Exception as e:
                            logger.error('error :: could not query Redis for running_derivative_metric_key - %s: %s' % (
                                running_derivative_metric_key, e))

                        # @added 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
                        # Handle key not existing and exception on last_key_value
                        # if None
                        if last_key_value_data:
                            try:
                                last_key_value = int(last_key_value_data)
                            except:
                                last_key_value = 0

                        # @added 20201130 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
                        # Do not reclassify until a new data point and timestamp
                        # are available to prevent a boring sparsely populated
                        # from being classified on the same subsequence 3 times
                        # in a row if no new data points are submitted
                        last_monotonic_timestamp = 0
                        running_derivative_metric_last_timestamp_key = 'zz.derivative_metric.last_timestamp.%s' % str(base_name)
                        if last_key_value:
                            try:
                                # @modified 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
                                # Only set to int if the response in not None
                                # otherwise it will log an error and if this
                                # data does not exist it is OK it is idempotent
                                # last_monotonic_timestamp_data = int(self.redis_conn_decoded.get(running_derivative_metric_last_timestamp_key))
                                last_monotonic_timestamp_data = self.redis_conn_decoded.get(running_derivative_metric_last_timestamp_key)
                                if last_monotonic_timestamp_data:
                                    last_monotonic_timestamp = int(last_monotonic_timestamp_data)
                            except Exception as e:
                                logger.error('error :: could not query Redis for running_derivative_metric_last_timestamp_key - %s: %s' % (
                                    running_derivative_metric_last_timestamp_key, e))

                            # @modified 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
                            if last_monotonic_timestamp == int(timeseries[-1][0]):
                                last_key_value = last_key_value - 1

                        # @added 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
                        # Handle key not existing and exception on last_key_value
                        # if None
                        if last_monotonic_timestamp:
                            try:
                                if last_monotonic_timestamp == int(timeseries[-1][0]):
                                    last_key_value = last_key_value - 1
                            except:
                                pass

                        try:
                            monotonic_count = int(last_key_value) + 1
                        except:
                            pass

                        if is_strictly_increasing_monotonically:
                            try:
                                self.redis_conn.setex(
                                    running_derivative_metric_key, 960, monotonic_count)
                            except Exception as e:
                                logger.error('error :: could not set Redis running_derivative_metric key: %s' % e)

                            # @added 20201130 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
                            # Set a key with the last timestamp incremented and
                            # classified on
                            try:
                                self.redis_conn.setex(
                                    running_derivative_metric_last_timestamp_key,
                                    960, int(timeseries[-1][0]))
                            except Exception as e:
                                logger.error('error :: could not set Redis running_derivative_metric_last_timestamp_key: %s' % e)

                        # With manage_derivative_metrics running every 300
                        # seconds the metric has to be monotonic in the last
                        # 15 data points.
                        if monotonic_count < 3:
                            is_strictly_increasing_monotonically = False
                else:
                    is_strictly_increasing_monotonically = False

                # Determine if it is a strictly increasing monotonically metric
                # or has been in last FULL_DURATION via its z.derivative_metric
                # key
                if last_derivative_metric_key:
                    # Until the z.derivative_metric key expires, it is classed
                    # as such
                    if not is_strictly_increasing_monotonically:
                        is_strictly_increasing_monotonically = True

                if skip_derivative:
                    is_strictly_increasing_monotonically = False
                if is_strictly_increasing_monotonically:
                    known_derivative_metric = True
                    try:
                        self.redis_conn.sadd('derivative_metrics', metric_name)
                        # @added 20201111 - Feature #3480: batch_processing
                        #                   Bug #2050: analyse_derivatives - change in monotonicity
                        metrics_added_to_derivative_metrics.append(metric_name)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis derivative_metrics set')

                    # @added 20201111 - Feature #3480: batch_processing
                    #                   Bug #2050: analyse_derivatives - change in monotonicity
                    # Always remove the metric from the non_derivative_metrics
                    # Redis set
                    try:
                        self.redis_conn.srem('non_derivative_metrics', metric_name)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis derivative_metrics set - %s' % e)

                    try:
                        self.redis_conn.sadd('new_derivative_metrics', metric_name)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis new_derivative_metrics set - %s' % e)
                    # @added 20200529 - Feature #3480: batch_processing
                    #                   Bug #2050: analyse_derivatives - change in monotonicity
                    # Alway set the derivative_metric_key
                    # @modified 20201111 - Feature #3480: batch_processing
                    #                   Bug #2050: analyse_derivatives - change in monotonicity
                    # Only set the key once
                    if not last_derivative_metric_key:
                        try:
                            last_expire_set = int(time())
                            self.redis_conn.setex(
                                derivative_metric_key, settings.FULL_DURATION, last_expire_set)
                            # @added 20201111 - Feature #3480: batch_processing
                            #                   Bug #2050: analyse_derivatives - change in monotonicity
                            derivative_z_metric_keys_added.append(base_name)
                        except Exception as e:
                            logger.error('error :: could not set Redis derivative_metric key: %s' % e)
                else:
                    try:
                        self.redis_conn.sadd('non_derivative_metrics', metric_name)
                        # @added 20201111 - Feature #3480: batch_processing
                        #                   Bug #2050: analyse_derivatives - change in monotonicity
                        metrics_added_to_non_derivative_metrics.append(metric_name)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis non_derivative_metrics set - %s' % e)
                    try:
                        self.redis_conn.sadd('new_non_derivative_metrics', metric_name)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis new_non_derivative_metrics set - %s' % e)

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
                                    logger.error('failed to determine if %s is an airgap metric - %s' % (
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

            # @modified 20200529 - Feature #3480: batch_processing
            #                      Bug #2050: analyse_derivatives - change in monotonicity
            # Only test nonNegativeDerivative if the timeseries has more than 3
            # data points
            # if known_derivative_metric:
            if known_derivative_metric and timeseries:
                if len(timeseries) > 3:
                    try:
                        derivative_timeseries = nonNegativeDerivative(timeseries)
                        timeseries = derivative_timeseries
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: nonNegativeDerivative failed')
                else:
                    logger.info('cannot run nonNegativeDerivative on %s timeseries as it has less than 4 data points' % base_name)

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
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to add %s to Redis set %s: %s' % (
                                str(data), str(redis_set), str(e)))
                except:
                    pass

            # @added 20201212 - Feature #3884: ANALYZER_CHECK_LAST_TIMESTAMP
            analyzer_check_last_last_timestamp_new_timestamp = True
            if ANALYZER_CHECK_LAST_TIMESTAMP and metrics_last_timestamp_dict and last_timeseries_timestamp:
                try:
                    last_analyzed_timestamp = metrics_last_timestamp_dict[base_name]
                except:
                    last_analyzed_timestamp = None
                if last_analyzed_timestamp:
                    try:
                        if int(float(last_analyzed_timestamp)) == int(last_timeseries_timestamp):
                            analyzer_check_last_last_timestamp_new_timestamp = False
                            if base_name not in all_stale_metrics:
                                if int(spin_start) - int(last_timeseries_timestamp) >= settings.STALE_PERIOD:
                                    # Send it for analysis to be classifed as
                                    # stale
                                    analyzer_check_last_last_timestamp_new_timestamp = True
                        if not analyzer_check_last_last_timestamp_new_timestamp:
                            metrics_without_new_data.append(base_name)
                        else:
                            metrics_with_new_data.append(base_name)
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

                # @added 20201030 - Feature #3808: ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS
                #                   ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED
                # low_priority_metrics are added to the analyzer.low_priority_metrics.dynamically_analyzed
                # Redis set pre analysis as if they are Boring, TooShort, Stale,
                # etc they will still be recordeded as having been assessed,
                # even if they are not added to the low_priority_metrics_hash_key
                # otherwise they appear to me missing.
                try:
                    int_metric_timestamp = int(timeseries[-1][0])
                except:
                    int_metric_timestamp = 0
                if low_priority_metric and ANALYZER_MANAGE_LOW_PRIORITY_METRICS:
                    if int_metric_timestamp:
                        try:
                            self.redis_conn.hset(
                                low_priority_metrics_hash_key, metric_name,
                                int_metric_timestamp)
                        except:
                            pass
                    try:
                        self.redis_conn.sadd('analyzer.low_priority_metrics.dynamically_analyzed', metric_name)
                    except:
                        pass

                # @added 20201018 - Feature #3810: ANALYZER_MAD_LOW_PRIORITY_METRICS
                check_for_anomalous = True

                # @added 20201212 - Feature #3884: ANALYZER_CHECK_LAST_TIMESTAMP
                if ANALYZER_CHECK_LAST_TIMESTAMP:
                    if not analyzer_check_last_last_timestamp_new_timestamp:
                        check_for_anomalous = False

                # @added 20201018 - Feature #3810: ANALYZER_MAD_LOW_PRIORITY_METRICS
                if ANALYZER_MAD_LOW_PRIORITY_METRICS and ANALYZER_ANALYZE_LOW_PRIORITY_METRICS:
                    try:
                        # @modified 20201212 - Feature #3884: ANALYZER_CHECK_LAST_TIMESTAMP
                        # if low_priority_metric:
                        if low_priority_metric and check_for_anomalous:
                            mad_data = [item[1] for item in timeseries[-ANALYZER_MAD_LOW_PRIORITY_METRICS:]]
                            # @added 20201020 - Feature #3810: ANALYZER_MAD_LOW_PRIORITY_METRICS
                            # Handle very sparsely populated metrics with only a
                            # few data points
                            if len(mad_data) < ANALYZER_MAD_LOW_PRIORITY_METRICS:
                                mad_data = None
                            # @modified 20201020 - Feature #3810: ANALYZER_MAD_LOW_PRIORITY_METRICS
                            # Handle no data, just send through normal
                            # algorithms to be classified as the correct
                            # algorithm exception class. Wrapped in if mad_data
                            # conditional
                            if mad_data:
                                mad = mean(absolute(mad_data - mean(mad_data)))
                                last_value_difference = timeseries[-2][1] - timeseries[-1][1]
                                if last_value_difference < 0:
                                    last_value_difference = last_value_difference * -1
                                if last_value_difference < mad:
                                    check_for_anomalous = False
                                else:
                                    low_priority_metrics_analysed_due_to_mad_trigger += 1
                    except:
                        check_for_anomalous = True
                        if not mad_error_logged:
                            logger.error(traceback.format_exc())
                            logger.error('error :: mad error with metric %s and time series length of %s' % (
                                metric_name, str(len(timeseries))))
                            mad_error_logged = True

                # @modified 20200424 - Feature #3508: ionosphere.untrainable_metrics
                # Added negatives_found and run_negatives_present
                # anomalous, ensemble, datapoint = run_selected_algorithm(timeseries, metric_name, metric_airgaps)
                # @modified 20200501 - Feature #3400: Identify air gaps in the metric data
                # Added metric_airgaps_filled and check_for_airgaps_only
                # anomalous, ensemble, datapoint, negatives_found = run_selected_algorithm(timeseries, metric_name, metric_airgaps, run_negatives_present)
                # @modified 20200603 - Feature #3566: custom_algorithms
                # Added algorithms_run
                # @modified 20210519 - Feature #4076: CUSTOM_STALE_PERIOD
                # Added custom_stale_metrics_dict
                if check_for_anomalous:
                    anomalous, ensemble, datapoint, negatives_found, algorithms_run = run_selected_algorithm(timeseries, metric_name, metric_airgaps, metric_airgaps_filled, run_negatives_present, check_for_airgaps_only, custom_stale_metrics_dict)
                    run_selected_algorithm_count += 1
                else:
                    # Low priority metric not analysed
                    anomalous = False
                    ensemble = [False]
                    datapoint = timeseries[-1][1]
                    negatives_found = False
                    algorithms_run = ['mad']

                del metric_airgaps
                del metric_airgaps_filled

                # @added 20191016 - Branch #3262: py3
                if LOCAL_DEBUG:
                    logger.debug('debug :: metric %s - anomalous - %s' % (str(metric_name), str(anomalous)))

                # @added 20201212 - Feature #3884: ANALYZER_CHECK_LAST_TIMESTAMP
                if ANALYZER_CHECK_LAST_TIMESTAMP and check_for_anomalous:
                    try:
                        self.redis_conn.hset(
                            metrics_last_timestamp_hash_key, base_name,
                            int_metric_timestamp)
                        metrics_added_to_last_timestamp_hash_key.append(base_name)
                    except:
                        pass

                # @added 20200608 - Feature #3566: custom_algorithms
                # @modified 20201127 - Feature #3848: custom_algorithms - run_before_3sigma parameter
                # Only run DEBUG_CUSTOM_ALGORITHMS if there is a custom
                # algorithm to be run with analyzer
                if DEBUG_CUSTOM_ALGORITHMS and run_custom_algorithm_enabled_on_app:
                    logger.debug('debug :: metric %s - anomalous - %s, ensemble - %s, algorithms_run - %s' % (
                        str(metric_name), str(anomalous), str(ensemble),
                        str(algorithms_run)))

                # @added 20201017 - Feature #3818: ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED
                if ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED:
                    if metric_name not in high_priority_assigned_metrics:
                        last_metric_timestamp_key = 'last_timestamp.%s' % base_name
                        try:
                            int_metric_timestamp = int(timeseries[-1][0])
                            self.redis_conn.setex(
                                last_metric_timestamp_key, 2592000,
                                int_metric_timestamp)
                        except:
                            logger.error('error :: ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED - failed to set Redis key %s for low priority metric' % last_metric_timestamp_key)

                # @added 20200908 - Feature #3734: waterfall alerts
                from_timestamp = int(timeseries[1][0])
                triggered_algorithms_for_waterfall_alert = []
                for index, value in enumerate(ensemble):
                    if value:
                        algorithm = algorithms_run[index]
                        triggered_algorithms_for_waterfall_alert.append(algorithm)
                waterfall_panorama_data_added_at = int(time())
                waterfall_panorama_data_source = 'graphite'
                waterfall_panorama_data = [
                    base_name, datapoint, int(from_timestamp),
                    int(timeseries[-1][0]), algorithms_run,
                    triggered_algorithms_for_waterfall_alert, skyline_app,
                    waterfall_panorama_data_source, this_host,
                    waterfall_panorama_data_added_at
                ]

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
                                # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
                                # base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                                if metric_name.startswith(settings.FULL_NAMESPACE):
                                    base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                                else:
                                    base_name = metric_name

                                data = [base_name, int(metric_timestamp)]
                                self.redis_conn.sadd(redis_set, str(data))
                            except Exception as e:
                                logger.error(traceback.format_exc())
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

                    # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
                    # base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    if metric_name.startswith(settings.FULL_NAMESPACE):
                        base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    else:
                        base_name = metric_name

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
                        logger.error(traceback.format_exc())
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
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add %s to Redis set %s' % (
                            str(data), str(redis_set)))
                    anomalous = True

                # @added 20200916 - Branch #3068: SNAB
                #                   Task #3744: POC matrixprofile
                #                   Info #1792: Shapelet extraction
                snab_only_check = False
                if SNAB_ENABLED:
                    snab_recheck_key = 'snab.recheck.%s' % base_name
                    snab_recheck_key_exists = False
                    try:
                        snab_recheck_key_exists = self.redis_conn_decoded.get(snab_recheck_key)
                    except:
                        pass
                    if snab_recheck_key_exists:
                        snab_only_check = True
                        logger.info('snab.recheck Redis key exists from %s - %s' % (
                            str(snab_recheck_key_exists), snab_recheck_key))
                        anomalous = True
                        logger.info('set anomalous to True - snab_only_check %s' % str(snab_only_check))

                # @added 20200607 - Feature #3566: custom_algorithms
                # If a metric is specified in MIRAGE_ALWAYS_METRICS add it to the
                # analyzer.mirage_always_metrics Redis set and set it to
                # anomalous so it gets pushed to Mirage on every run.
                if not anomalous:
                    if MIRAGE_ALWAYS_METRICS and base_name in MIRAGE_ALWAYS_METRICS:
                        redis_set = 'analyzer.new.mirage_always_metrics'
                        data = str(base_name)
                        try:
                            self.redis_conn.sadd(redis_set, data)
                            logger.info('added %s to the analyzer.new.mirage_always_metrics Redis set because it is a MIRAGE_ALWAYS_METRICS metric' % (
                                str(base_name)))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to add %s to Redis set %s' % (
                                str(data), str(redis_set)))
                        anomalous = True
                        logger.info('set %s set anomalous to True so it can be pushed to Mirage for analysis because it is a MIRAGE_ALWAYS_METRICS metric' % (
                            str(base_name)))

                # @added 20200611 - Feature #3578: Test alerts
                if not anomalous:
                    if base_name in test_alerts:
                        metric_timestamp = timeseries[-1][0]
                        metric = [datapoint, base_name, metric_timestamp]
                        redis_set = '%s.test_alerts_data' % skyline_app
                        data = str(metric)
                        try:
                            self.redis_conn.sadd(redis_set, data)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to add %s to Redis set %s' % (
                                str(data), str(redis_set)))
                        logger.info('test_alerts includes %s, added to %s Redis set' % (
                            str(base_name), redis_set))

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
                    # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
                    # base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    if metric_name.startswith(settings.FULL_NAMESPACE):
                        base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    else:
                        base_name = metric_name

                    metric_timestamp = timeseries[-1][0]

                    # Get the anomaly breakdown - who returned True?
                    triggered_algorithms = []
                    for index, value in enumerate(ensemble):
                        if value:
                            # @modified 20200603 - Feature #3566: custom_algorithms
                            # algorithm = list(settings.ALGORITHMS)[index]
                            algorithm = algorithms_run[index]

                            anomaly_breakdown[algorithm] += 1
                            triggered_algorithms.append(algorithm)

                    # @modified 20201008 - Feature #3734: waterfall alerts
                    #                      Branch #3068: SNAB
                    # Added triggered_algorithms and algorithms_run
                    # metric = [datapoint, base_name, metric_timestamp]
                    metric = [datapoint, base_name, metric_timestamp, triggered_algorithms, algorithms_run]

                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # self.anomalous_metrics.append(metric)
                    redis_set = 'analyzer.anomalous_metrics'
                    data = str(metric)
                    try:
                        self.redis_conn.sadd(redis_set, data)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add %s to Redis set %s' % (
                            str(data), str(redis_set)))

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
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to add %s to Redis set %s' % (
                                str(data), str(redis_set)))

                    # If Crucible or Panorama are enabled determine details
                    # @modified 20200904 - Feature #3734: waterfall alerts
                    # Always determine_anomaly_details
                    # determine_anomaly_details = False
                    determine_anomaly_details = True
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
                        # @added 20200916 - Branch #3068: SNAB
                        #                   Task #3744: POC matrixprofile
                        #                   Info #1792: Shapelet extraction
                        if snab_only_check:
                            ionosphere_metric = False
                            send_to_ionosphere = False
                            logger.info('not sending snab_only_check to ionosphere for %s at %s' % (metric_name, str(metric_timestamp)))

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
                                    if VERBOSE_LOGGING:
                                        logger.info('not sending to Ionosphere - alert key exists - %s' % (base_name))
                            # @added 20200916 - Branch #3068: SNAB
                            #                   Task #3744: POC matrixprofile
                            #                   Info #1792: Shapelet extraction
                            if snab_only_check:
                                send_to_ionosphere = False
                        else:
                            # @added 20200904 - Task #3730: Validate Mirage running multiple processes
                            # Added check to see if a Mirage check has already
                            # been done for the timestamp
                            mirage_check_not_done = True

                            if mirage_metric:

                                # @added 20200904 - Task #3730: Validate Mirage running multiple processes
                                # Only add the metric check file if it the check
                                # was not already submitted in a past run
                                mirage_check_sent = False
                                mirage_check_sent_key = 'analyzer.mirage_check_sent.%s.%s' % (
                                    str(int(metric[2])), str(metric[1]))
                                try:
                                    mirage_check_sent = self.redis_conn_decoded.get(mirage_check_sent_key)
                                except Exception as e:
                                    logger.error('error :: could not query Redis for cache_key: %s' % e)
                                if mirage_check_sent:
                                    logger.info('a mirage check for %s at %s was already sent at %s, Redis key exists - %s' % (
                                        metric[1], str(metric[2]),
                                        str(mirage_check_sent), str(mirage_check_sent_key)))
                                    mirage_check_not_done = False
                                # @added 20201026 - Task #3800: Handle feedback metrics in Mirage and waterfall alerts
                                # Check to see if Analyzer has waterfall alerted
                                # on the metric if it has do not send it to
                                # Mirage again within PANORAMA_EXPIRY_TIME
                                if not mirage_check_sent:
                                    waterfalled_alert_cache_key = 'last_alert.smtp.%s' % (base_name)
                                    waterfall_alerted_on = False
                                    try:
                                        waterfall_alerted_on = self.redis_conn_decoded.get(waterfalled_alert_cache_key)
                                    except Exception as e:
                                        logger.error('error :: could not query Redis for cache_key: %s' % str(e))
                                    if waterfall_alerted_on:
                                        mirage_check_not_done = False
                                        # Unless is it older than PANORAMA_EXPIRY_TIME
                                        try:
                                            alerted_on_at = int(waterfall_alerted_on)
                                            alerted_on_seconds_ago = int(time()) - alerted_on_at
                                            if alerted_on_seconds_ago >= settings.PANORAMA_EXPIRY_TIME:
                                                mirage_check_not_done = True
                                        except:
                                            mirage_check_not_done = True
                                            pass
                                        if not mirage_check_not_done:
                                            logger.info('a waterfall alert was recently sent for %s, not sending to Mirage' % (
                                                metric[1]))

                            if mirage_metric and mirage_check_not_done:
                                if VERBOSE_LOGGING:
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
                                        if VERBOSE_LOGGING:
                                            logger.info('%s added Ionosphere Mirage %sh Redis data timeseries json file :: %s' % (
                                                skyline_app, str(int(full_duration_in_hours)), ionosphere_json_file))
                                    except Exception as e:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: failed to add %s Ionosphere Mirage Redis data timeseries json file - %s - %s' % (skyline_app, ionosphere_json_file, e))

                                # @added 20200904 - Task #3730: Validate Mirage running multiple processes
                                # Add mirage check files immediately if possible
                                # rather than waiting to add all in the alerting
                                # phase.  This is done to reduce the time it
                                # takes to get through the analysis pipeline.
                                use_hours_to_resolve = None

                                # @modified 20201109 - Feature #3830: metrics_manager
                                # Use the metrics_manager mirage.hash_key.metrics_resolutions
                                # hash key
                                # mirage_metric_cache_key = 'mirage.metrics.%s' % base_name

                                try:
                                    # @modified 20201109 - Feature #3830: metrics_manager
                                    # Use the metrics_manager mirage.hash_key.metrics_resolutions
                                    # hash key
                                    # raw_hours_to_resolve = self.redis_conn.get(mirage_metric_cache_key)
                                    raw_hours_to_resolve = self.redis_conn_decoded.hget('mirage.hash_key.metrics_resolutions', base_name)
                                    use_hours_to_resolve = int(raw_hours_to_resolve)
                                except:
                                    # logger.info('mirage check will not be added here (will be added in run alert check) as no mirage.metrics Redis key found to determine hours_to_resolve - %s' % str(mirage_metric_cache_key))
                                    logger.info('mirage check will not be added here (will be added in run alert check) as %s was not found in the mirage.hash_key.metrics_resolutions Redis hash key' % base_name)
                                    use_hours_to_resolve = None

                                anomaly_check_file = None
                                # Only add if the hours to resolve are known
                                # from an existing mirage.metrics Redis key, if
                                # the hours_to_resolve are not known then the
                                # check will be added in the original and normal
                                # run alert check.
                                if use_hours_to_resolve:
                                    try:
                                        anomaly_check_file = '%s/%s.%s.txt' % (settings.MIRAGE_CHECK_PATH, str(metric_timestamp), sane_metricname)
                                    except:
                                        anomaly_check_file = None
                                mirage_anomaly_check_file_created = False
                                if anomaly_check_file:
                                    try:
                                        with open(anomaly_check_file, 'w') as fh:
                                            # metric_name, anomalous datapoint, hours to resolve, timestamp
                                            # @modified 20200916 - Branch #3068: SNAB
                                            #                      Task #3744: POC matrixprofile
                                            # metric_name, anomalous datapoint, hours to resolve, timestamp, snab_only
                                            # fh.write('metric = "%s"\nvalue = "%s"\nhours_to_resolve = "%s"\nmetric_timestamp = "%s"\n' % (metric[1], metric[0], str(use_hours_to_resolve), str(metric[2])))
                                            # @modified 20210304 - Feature #3642: Anomaly type classification
                                            #                      Feature #3970: custom_algorithm - adtk_level_shift
                                            # Added triggered_algorithms to mirage_check_file
                                            # fh.write('metric = "%s"\nvalue = "%s"\nhours_to_resolve = "%s"\nmetric_timestamp = "%s"\nsnab_only_check = "%s"\n' % (metric[1], metric[0], str(use_hours_to_resolve), str(metric[2]), str(snab_only_check)))
                                            fh.write('metric = "%s"\nvalue = "%s"\nhours_to_resolve = "%s"\nmetric_timestamp = "%s"\nsnab_only_check = "%s"\ntriggered_algorithms = %s\n' % (metric[1], metric[0], str(use_hours_to_resolve), str(metric[2]), str(snab_only_check), str(triggered_algorithms)))
                                        mirage_anomaly_check_file_created = True
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: failed to write anomaly_check_file')
                                if mirage_anomaly_check_file_created:
                                    if python_version == 2:
                                        os.chmod(anomaly_check_file, 0o644)
                                    if python_version == 3:
                                        os.chmod(anomaly_check_file, mode=0o644)
                                    logger.info('added mirage check :: %s,%s,%s' % (metric[1], metric[0], use_hours_to_resolve))
                                    try:
                                        redis_set = 'analyzer.sent_to_mirage'
                                        data = str(metric[1])
                                        try:
                                            self.redis_conn.sadd(redis_set, data)
                                        except:
                                            logger.error(traceback.format_exc())
                                            logger.error('error :: failed to add %s to Redis set %s' % (
                                                str(data), str(redis_set)))
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error(
                                            'error :: failed add %s to analyzer.sent_to_mirage Redis set' %
                                            metric[1])

                                    # @added 20200904 - Feature #3734: waterfall alerts
                                    added_to_waterfall_timestamp = int(time())
                                    # [metric, timestamp, value, added_to_waterfall_timestamp, waterfall_panorama_data]
                                    waterfall_data = [metric[1], int(metric[2]), metric[0], added_to_waterfall_timestamp, waterfall_panorama_data]
                                    redis_set = 'analyzer.waterfall_alerts.sent_to_mirage'

                                    # @modified 20200916 - Branch #3068: SNAB
                                    #                      Task #3744: POC matrixprofile
                                    # Only add to waterfall_alerts if it is not
                                    # a snab_only_check
                                    if not snab_only_check:
                                        try:
                                            self.redis_conn.sadd(redis_set, str(waterfall_data))
                                            if VERBOSE_LOGGING:
                                                logger.info('added to Redis set %s - %s' % (redis_set, str(waterfall_data)))
                                        except Exception as e:
                                            logger.error(traceback.format_exc())
                                            logger.error('error :: failed to add %s to Redis set %s - %s' % (
                                                str(waterfall_data), str(redis_set), e))

                                    # @added 20200904 - Task #3730: Validate Mirage running multiple processes
                                    # Add a Redis key for the metric and timestamp
                                    # so that it does not get added multiple
                                    # times on consecutive runs if the metric
                                    # does not change
                                    mirage_check_sent_key = 'analyzer.mirage_check_sent.%s.%s' % (
                                        str(int(metric[2])), str(metric[1]))
                                    try:
                                        self.redis_conn.setex(mirage_check_sent_key, 300, added_to_waterfall_timestamp)
                                    except Exception as e:
                                        logger.error('error :: could not query Redis for cache_key: %s' % e)

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
                                str(ionosphere_parent_id),
                                # @added 20201001 - Task #3748: POC SNAB
                                # Added algorithms_run required to determine the anomalyScore
                                # so this needs to be sent to Ionosphere so Ionosphere
                                # can send it back on an alert
                                algorithms_run)
                            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                            # Moved to Redis key block below
                            # self.sent_to_ionosphere.append(base_name)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to send_anomalous_metric_to to ionosphere')

                        # @added 20200804 - Feature #3462: Add IONOSPHERE_MANAGE_PURGE
                        #                   Feature #3472: ionosphere.training_data Redis set
                        #                   Feature #3474: webapp api - training_data
                        # Add training data to the ionosphere.training_data so that
                        # the ionosphere purge_old_data_dirs can happen less
                        # frequently for reduced I/O
                        redis_set = 'ionosphere.training_data'
                        data = [base_name, int(metric_timestamp), settings.FULL_DURATION]
                        try:
                            logger.info('adding to Redis set %s - %s' % (
                                redis_set, str(data)))
                            self.redis_conn.sadd(redis_set, str(data))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to add %s to %s Redis set' % (str(data), redis_set))

                        # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        redis_set = 'analyzer.sent_to_ionosphere'
                        data = str(base_name)
                        try:
                            self.redis_conn.sadd(redis_set, data)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to add %s to Redis set %s' % (
                                str(data), str(redis_set)))

                        # @added 20200904 - Feature #3734: waterfall alerts
                        # Only add if the metric is ionosphere_enabled
                        if ionosphere_metric:
                            added_to_waterfall_timestamp = int(time())
                            # [metric, timestamp, value, added_to_waterfall_timestamp]
                            waterfall_data = [metric[1], metric[2], metric[0], added_to_waterfall_timestamp, waterfall_panorama_data]
                            redis_set = 'analyzer.waterfall_alerts.sent_to_ionosphere'
                            try:
                                self.redis_conn.sadd(redis_set, str(waterfall_data))
                                logger.info('added to Redis set %s - %s' % (redis_set, str(waterfall_data)))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to add %s to Redis set %s' % (
                                    str(waterfall_data), str(redis_set)))

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
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to set Redis key %s' % ionosphere_training_data_key)

                    if ionosphere_metric:
                        analyzer_metric = False

                    # Only send Analyzer metrics
                    # @modified 20200916 - Branch #3068: SNAB
                    #                      Task #3744: POC matrixprofile
                    # Only add to waterfall_alerts if it is not
                    # a snab_only_check
                    # if analyzer_metric and settings.PANORAMA_ENABLED:
                    if analyzer_metric and settings.PANORAMA_ENABLED and not snab_only_check:
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
                               # @modified 20200603 - Feature #3566: custom_algorithms
                               # metric_timestamp, str(settings.ALGORITHMS),
                               metric_timestamp, str(algorithms_run),
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
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to add panorama anomaly file :: %s - %s' % (panaroma_anomaly_file, e))

                        # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        redis_set = 'analyzer.sent_to_panorama'
                        data = str(base_name)
                        try:
                            self.redis_conn.sadd(redis_set, data)
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to add %s to Redis set %s - %s' % (
                                str(data), str(redis_set), e))
                    else:
                        # @modified 20160207 - Branch #922: Ionosphere
                        # Handle if all other apps are not enabled
                        other_app = 'none'
                        if mirage_metric:
                            other_app = 'Mirage'
                        if ionosphere_metric:
                            other_app = 'Ionosphere'
                        if VERBOSE_LOGGING:
                            logger.info('not adding panorama anomaly file for %s - %s' % (other_app, metric))

                    # If Crucible is enabled - save timeseries and create a
                    # Crucible check
                    # @modified 20200916 - Branch #3068: SNAB
                    #                      Task #3744: POC matrixprofile
                    # Only add to waterfall_alerts if it is not
                    # a snab_only_check
                    # if settings.ENABLE_CRUCIBLE and settings.ANALYZER_CRUCIBLE_ENABLED:
                    if settings.ENABLE_CRUCIBLE and settings.ANALYZER_CRUCIBLE_ENABLED and not snab_only_check:
                        crucible_anomaly_dir = str(settings.CRUCIBLE_DATA_FOLDER) + '/' + timeseries_dir + '/' + metric_timestamp
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
                               # @modified 20200603 - Feature #3566: custom_algorithms
                               # metric_timestamp, str(settings.ALGORITHMS),
                               metric_timestamp, str(algorithms_run),
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
                            logger.error(traceback.format_exc())

                        # Create timeseries json file with the timeseries
                        json_file = '%s/%s.json' % (crucible_anomaly_dir, base_name)
                        timeseries_json = str(timeseries).replace('[', '(').replace(']', ')')
                        try:
                            write_data_to_file(skyline_app, json_file, 'w', timeseries_json)
                            logger.info('added crucible timeseries file :: %s' % (json_file))
                        except:
                            logger.error('error :: failed to add crucible timeseries file :: %s' % (json_file))
                            logger.error(traceback.format_exc())

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

            # added 20201020 - Feature #3792: algorithm_exceptions - EmptyTimeseries
            # For performance remove metrics with empty time series data from
            # metrics.unique_metrics so that they are no longer assessed
            except EmptyTimeseries:
                exceptions['EmptyTimeseries'] += 1
                full_uniques = settings.FULL_NAMESPACE + 'unique_metrics'
                try:
                    self.redis_conn.srem(full_uniques, metric_name)
                    logger.info('removed empty time series metric %s from %s Redis set' % (
                        metric_name, full_uniques))
                except:
                    logger.error('error :: failed to remove empty time series metric %s from %s Redis set' % (
                        metric_name, full_uniques))
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
                logger.error(traceback.format_exc())

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

        # @added 20201017 - Feature #3818: ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED
        if ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED and low_priority_metrics_sent_to_analyzer_batch:
            logger.info('ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED - sent %s low priority metrics to analyzer_batch' % (
                str(low_priority_metrics_sent_to_analyzer_batch)))
            if skipped_low_priority_metrics_in_analyzer_batch_queue:
                logger.info('ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED - skipped %s low priority metrics already in analyzer_batch queue' % (
                    str(skipped_low_priority_metrics_in_analyzer_batch_queue)))

        # @added 20201018 - Feature #3810: ANALYZER_MAD_LOW_PRIORITY_METRICS
        if ANALYZER_MAD_LOW_PRIORITY_METRICS and ANALYZER_ANALYZE_LOW_PRIORITY_METRICS:
            try:
                skipped_low_priority_metrics = len(low_priority_assigned_metrics) - low_priority_metrics_analysed_due_to_mad_trigger
                logger.info('ANALYZER_MAD_LOW_PRIORITY_METRICS - skipped %s low priority metrics as mad did not trigger' % (
                    str(skipped_low_priority_metrics)))
                logger.info('ANALYZER_MAD_LOW_PRIORITY_METRICS - analyzed %s low priority metrics as mad triggered over %s datapoints ' % (
                    str(low_priority_metrics_analysed_due_to_mad_trigger),
                    str(ANALYZER_MAD_LOW_PRIORITY_METRICS)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: ANALYZER_MAD_LOW_PRIORITY_METRICS log error')
        else:
            logger.info('ANALYZER_MAD_LOW_PRIORITY_METRICS not enabled')
            if not ANALYZER_ANALYZE_LOW_PRIORITY_METRICS:
                logger.info('ANALYZER_ANALYZE_LOW_PRIORITY_METRICS set to False, no low priority metrics were analyzed')

        del low_priority_assigned_metrics

        # @added 20201212 - Feature #3884: ANALYZER_CHECK_LAST_TIMESTAMP
        if ANALYZER_CHECK_LAST_TIMESTAMP:
            try:
                logger.info('ANALYZER_CHECK_LAST_TIMESTAMP - there were %s metrics with no new data that were not analysed' % str(len(metrics_without_new_data)))
                logger.info('ANALYZER_CHECK_LAST_TIMESTAMP - there were %s metrics with new data that were analysed' % str(len(metrics_with_new_data)))
                logger.info('ANALYZER_CHECK_LAST_TIMESTAMP - %s metrics last analysed timestamps where updated in %s Redis hash key' % (
                    str(len(metrics_added_to_last_timestamp_hash_key)),
                    metrics_last_timestamp_hash_key))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: ANALYZER_CHECK_LAST_TIMESTAMP log error')

        # @added 20210519 - Feature #4076: CUSTOM_STALE_PERIOD
        #                   Branch #1444: thunder
        try:
            logger.info('updated analyzer.metrics.last_timeseries_timestamp Redis hash key with timestamps for %s metrics' % (
                str(metrics_updated_in_last_timeseries_timestamp_hash_key_count)))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: updated analyzer.metrics.last_timeseries_timestamp Redis hash key log error - %s' % e)

        # run_selected_algorithm_count = 0
        logger.info('%s metrics run through run_selected_algorithm' % (
            str(run_selected_algorithm_count)))

        # @added 20201111 - Feature #3480: batch_processing
        #                   Bug #2050: analyse_derivatives - change in monotonicity
        if not manage_derivative_metrics:
            logger.info('derivative metrics were managed - z.derivative_metrics keys that existed - %s' % str(len(derivative_z_metric_keys_existing)))
            logger.info('derivative metrics were managed - z.derivative_metrics keys added - %s' % str(len(derivative_z_metric_keys_added)))
            if len(derivative_z_metric_keys_added) > 0:
                try:
                    self.redis_conn.sadd('analyzer.z_derivative_metrics_added', *set(derivative_z_metric_keys_added))
                    logger.info('derivative metrics were managed - %s metrics added to the analyzer.z_derivative_metrics_added Redis set' % str(len(derivative_z_metric_keys_added)))

                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to add multiple members to the analyzer.z_derivative_metrics_added Redis set')
            logger.info('derivative metrics were managed - z.derivative_metrics keys removed - %s' % str(len(derivative_z_metric_keys_removed)))
            if len(derivative_z_metric_keys_removed) > 0:
                try:
                    self.redis_conn.sadd('analyzer.z_derivative_metrics_removed', *set(derivative_z_metric_keys_removed))
                    logger.info('derivative metrics were managed - %s metrics added to the analyzer.z_derivative_metrics_removed Redis set' % str(len(derivative_z_metric_keys_removed)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to add multiple members to the analyzer.z_derivative_metrics_removed Redis set')
            logger.info('derivative metrics were managed - metrics added to the derivative_metrics Redis set - %s' % str(len(metrics_added_to_derivative_metrics)))
            logger.info('derivative metrics were managed - metrics removed from the derivative_metrics Redis set - %s' % str(len(metrics_removed_from_derivative_metrics)))
            if len(metrics_removed_from_derivative_metrics) > 0:
                try:
                    self.redis_conn.sadd('analyzer.derivative_metrics_removed', *set(metrics_removed_from_derivative_metrics))
                    logger.info('derivative metrics were managed - %s metrics added to the analyzer.derivative_metrics_removed Redis set' % str(len(metrics_removed_from_derivative_metrics)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to add multiple members to the analyzer.z_derivative_metrics_removed Redis set')
            logger.info('derivative metrics were managed - metrics added to the non_derivative_metrics Redis set - %s' % str(len(metrics_added_to_non_derivative_metrics)))
            # @modified 20201111 - Feature #3480: batch_processing
            #                      Bug #2050: analyse_derivatives - change in monotonicity
            # Set only once in the main thread
            # try:
            #    # @modified 20170901 - Bug #2154: Infrequent missing new_ Redis keys
            #    # self.redis_conn.setex('analyzer.derivative_metrics_expiry', 120, key_timestamp)
            #    self.redis_conn.setex('analyzer.derivative_metrics_expiry', 300, int(time()))
            #    logger.info('set analyzer.derivative_metrics_expiry Redis key')
            # except:
            #     logger.error('error :: failed to set key :: analyzer.analyzer.derivative_metrics_expiry')

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

        # @added 20210513 - Feature #4068: ANALYZER_SKIP
        if ANALYZER_SKIP:
            logger.info('skipped %s metrics matched from ANALYZER_SKIP' % str(analyzer_skip_metrics_skipped))

        # Add values to the queue so the parent process can collate
        for key, value in anomaly_breakdown.items():
            self.anomaly_breakdown_q.put((key, value))

        for key, value in exceptions.items():
            self.exceptions_q.put((key, value))

        if LOCAL_DEBUG:
            logger.debug('debug :: Memory usage spin_process end: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

        # @added 20201007 - Feature #3774: SNAB_LOAD_TEST_ANALYZER
        # This is not full analysis as the metrics are not checked if they are
        # mirage metrics, airgaps, flux.filled, etc, etc.  They are simply
        # run through the algorithms, it is a rough estimate.
        if SNAB_LOAD_TEST_ANALYZER:
            load_test_process_with = SNAB_LOAD_TEST_ANALYZER / settings.ANALYZER_PROCESSES
            current_unique_metrics_count = len(snab_analyzer_load_test_unique_metrics)
            logger.info('SNAB_LOAD_TEST_ANALYZER set to test %s metrics' % str(SNAB_LOAD_TEST_ANALYZER))
            snab_load_tests_to_do = 0
            if current_unique_metrics_count < SNAB_LOAD_TEST_ANALYZER:
                snab_load_tests_to_do = int(load_test_process_with - current_unique_metrics_count)
                logger.info('SNAB_LOAD_TEST_ANALYZER - %s unique metrics were analyzed, %s snab.analyzer_load_test metrics to be done on this process' % (
                    str(current_unique_metrics_count), str(snab_load_tests_to_do)))
            snab_load_test_anomalous = 0
            snab_load_test_not_anomalous = 0
            if snab_load_tests_to_do:
                start_snab_analyzer_load_test = time()
                snab_load_tests_done = 0
                metric_airgaps = []
                metric_airgaps_filled = []
                run_negatives_present = True
                while snab_load_tests_done < snab_load_tests_to_do:
                    load_test_time = int(time())
                    if load_test_time > (snab_analyzer_load_test_start_time + (settings.MAX_ANALYZER_PROCESS_RUNTIME - 5)):
                        logger.info('SNAB_LOAD_TEST_ANALYZER - load test approaching MAX_ANALYZER_PROCESS_RUNTIME, stopping after analysing %s load test metrics' % (
                            str(snab_load_tests_done)))
                        break
                    # Distill timeseries strings into lists
                    for i, metric_name in enumerate(assigned_metrics):
                        self.check_if_parent_is_alive()
                        load_test_time = int(time())
                        if load_test_time > (snab_analyzer_load_test_start_time + (settings.MAX_ANALYZER_PROCESS_RUNTIME - 5)):
                            logger.info('SNAB_LOAD_TEST_ANALYZER - load test approaching MAX_ANALYZER_PROCESS_RUNTIME, stopping after analysing %s load test metrics' % (
                                str(snab_load_tests_done)))
                            break
                        if snab_load_tests_done >= snab_load_tests_to_do:
                            break
                        try:
                            raw_series = raw_assigned[i]
                            unpacker = Unpacker(use_list=False)
                            unpacker.feed(raw_series)
                            timeseries = list(unpacker)
                        except:
                            timeseries = []
                        anomalous = None
                        try:
                            anomalous, ensemble, datapoint, negatives_found, algorithms_run = run_selected_algorithm(timeseries, metric_name, metric_airgaps, metric_airgaps_filled, run_negatives_present, check_for_airgaps_only)
                            del metric_airgaps
                            del metric_airgaps_filled
                        except:
                            pass
                        if anomalous:
                            snab_load_test_anomalous += 1
                        else:
                            snab_load_test_not_anomalous += 1
                        snab_load_tests_done += 1
                    logger.info('SNAB_LOAD_TEST_ANALYZER - load testing completed on %s load test metrics' % (
                        str(snab_load_tests_done)))
                snab_load_test_runtime = time() - start_snab_analyzer_load_test
                logger.info('SNAB_LOAD_TEST_ANALYZER - results load test metrics - anomalous: %s, not_anomalous: %s' % (
                    str(snab_load_test_anomalous), str(snab_load_test_not_anomalous)))
                logger.info('SNAB_LOAD_TEST_ANALYZER - load testing completed on %s load test metrics in %.2f seconds' % (
                    str(snab_load_tests_done), snab_load_test_runtime))

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
            # @modified 20200606 - Bug #3572: Apply list to settings import
            DO_NOT_ALERT_ON_STALE_METRICS = list(settings.DO_NOT_ALERT_ON_STALE_METRICS)
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
                logger.error(traceback.format_exc())

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
                logger.error(traceback.format_exc())
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
        # @modified 20200606 - Bug #3572: Apply list to settings import
        algorithm_tmp_file_prefix = str(settings.SKYLINE_TMP_DIR) + '/' + skyline_app + '.'
        algorithms_to_time = []
        if send_algorithm_run_metrics:
            algorithms_to_time = list(settings.ALGORITHMS)
            # @added 20200603 - Feature #3566: custom_algorithms
            if CUSTOM_ALGORITHMS:
                for custom_algorithm in settings.CUSTOM_ALGORITHMS:
                    algorithms_to_time.append(custom_algorithm)
            logger.info('%s :: algorithms_to_time - %s' % (skyline_app, str(algorithms_to_time)))

        if LOCAL_DEBUG:
            logger.debug('debug :: Memory usage in run after algorithms_to_time: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

        while 1:
            now = time()

            if LOCAL_DEBUG:
                logger.debug('debug :: Memory usage before unique_metrics lookup: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: Analyzer cannot ping Pedis at socket path %s, reconnect will be attempted in 10 seconds - %s' % (settings.REDIS_SOCKET_PATH, e))
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
                    logger.error(traceback.format_exc())
                    # logger.error('error :: Analyzer cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                    logger.error('error :: Analyzer cannot connect to get_redis_conn')
                continue

            # Report app up
            try:
                # @modified 20210524 - Branch #1444: thunder
                # Report app AND Redis as up
                # self.redis_conn.setex(skyline_app, 120, now)
                redis_is_up = self.redis_conn.setex(skyline_app, 120, now)
                if redis_is_up:
                    try:
                        self.redis_conn.setex('redis', 120, now)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: could not update the Redis redis key - %s' % (
                            e))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: Analyzer could not update the Redis %s key - %s' % (
                    skyline_app, e))

            # Discover unique metrics
            # @modified 20160803 - Adding additional exception handling to Analyzer
            try:
                # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                #                   Branch #3262: py3
                # unique_metrics = list(self.redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
                unique_metrics = list(self.redis_conn_decoded.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
            except:
                logger.error('error :: Analyzer could not get the unique_metrics list from Redis')
                logger.error(traceback.format_exc())
                sleep(10)
                continue

            if len(unique_metrics) == 0:
                logger.info('no metrics in redis. try adding some - see README')
                sleep(10)
                continue

            # @added 20211129 - Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
            try:
                batch_metrics_list = list(self.redis_conn_decoded.smembers('aet.analyzer.batch_processing_metrics'))
            except:
                logger.error('error :: Analyzer could not get aet.analyzer.batch_processing_metrics from Redis')
                logger.error(traceback.format_exc())

            # @added 20200528 - Feature #3560: External alert config
            external_alerts = {}
            external_from_cache = None
            internal_alerts = {}
            internal_from_cache = None
            all_alerts = list(settings.ALERTS)
            all_from_cache = None
            if EXTERNAL_ALERTS:
                try:
                    external_alerts, external_from_cache, internal_alerts, internal_from_cache, all_alerts, all_from_cache = get_external_alert_configs(skyline_app)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: could not determine external alert configs')
                logger.info('retrieved %s external_alerts configurations from_cache %s, %s internal_alerts from_cache %s and %s all_alerts from_cache %s' % (
                    str(len(external_alerts)), str(external_from_cache),
                    str(len(internal_alerts)), str(internal_from_cache),
                    str(len(all_alerts)), str(all_from_cache)))
                if LOCAL_DEBUG:
                    logger.debug('debug :: all_alerts :: %s' % str(all_alerts))
            if not all_alerts:
                logger.error('error :: all_alerts is not set, so creating from settings.ALERTS')
                all_alerts = list(settings.ALERTS)

            # @added 20201017 - Feature #3788: snab_flux_load_test
            #                   Feature #3560: External alert config
            # refresh_redis_alert_sets = False
            last_all_alerts_set = None
            try:
                last_all_alerts_data = self.redis_conn_decoded.get('analyzer.last_all_alerts')
                if last_all_alerts_data:
                    last_all_alerts = literal_eval(last_all_alerts_data)
                    # A normal sorted cannot be used as the list has dicts in it
                    last_all_alerts_set = sorted(last_all_alerts, key=lambda item: item[0])
                    logger.info('last_all_alerts_set from analyzer.last_all_alerts Redis set has %s items' % str(len(last_all_alerts_set)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to generate a list from the analyzer.last_all_alerts Redis key')
                last_all_alerts_set = None
            all_alerts_set = None
            if all_alerts:
                try:
                    all_alerts_list = [list(row) for row in all_alerts]
                    # A normal sorted cannot be used as the list has dicts in it
                    all_alerts_set = sorted(all_alerts_list, key=lambda item: item[0])
                    logger.info('all_alerts_set from all_alerts has %s items' % str(len(all_alerts_set)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to create a sorted list from all_alerts object of type %s' % str(type(all_alerts_list)))
                try:
                    self.redis_conn.set('analyzer.last_all_alerts', str(all_alerts_set))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to set analyzer.last_all_alerts Redis key')
            if str(all_alerts_set) != str(last_all_alerts_set):
                logger.info('alert settings have changed alerts will be refreshed, reset current smtp_alerter_metrics list')
                # refresh_redis_alert_sets = True

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
                    # @modified 20201030 - Feature #3808: ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS
                    # self.redis_conn.delete('analyzer.boring')
                    # logger.info('deleted Redis analyzer.boring set to refresh')
                    self.redis_conn.rename('analyzer.boring', 'aet.analyzer.boring')
                    logger.info('renamed Redis analyzer.boring set to aet.analyzer.boring to refresh')
                except:
                    logger.info('no Redis set to rename - analyzer.boring')
                try:
                    # self.redis_conn.delete('analyzer.too_short')
                    # logger.info('deleted Redis analyzer.too_short set to refresh')
                    self.redis_conn.rename('analyzer.too_short', 'aet.analyzer.too_short')
                    logger.info('renamed Redis analyzer.too_short to aet.analyzer.too_short set to refresh')
                except:
                    logger.info('no Redis set to rename - analyzer.too_short')
            try:
                # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                #                   Branch #3262: py3
                # stale_metrics = list(self.redis_conn.smembers('analyzer.stale'))
                stale_metrics = list(self.redis_conn_decoded.smembers('analyzer.stale'))
            except:
                stale_metrics = []
            if stale_metrics:
                try:
                    # self.redis_conn.delete('analyzer.stale')
                    # logger.info('deleted Redis analyzer.stale set to refresh')
                    self.redis_conn.rename('analyzer.stale', 'aet.analyzer.stale')
                    logger.info('renamed Redis analyzer.stale to aet.analyzer.stale set to refresh')
                except:
                    logger.info('no Redis set to rename - analyzer.stale')
            # @added 20180807 - Feature #2492: alert on stale metrics
            try:
                self.redis_conn.delete('analyzer.alert_on_stale_metrics')
                logger.info('deleted Redis analyzer.alert_on_stale_metrics set to refresh')
            except:
                logger.info('no Redis set to delete - analyzer.alert_on_stale_metrics')

            # @modified 20201107 - Feature #3830: metrics_manager
            # All metric management is now done in metrics_manager
            manage_mirage_unique_metrics = False
            if settings.ENABLE_MIRAGE and not ANALYZER_USE_METRICS_MANAGER:
                if manage_mirage_unique_metrics:
                    try:
                        # @added 20200723 - Feature #3560: External alert config
                        # Speed this up and optimizing the management of Redis
                        # sets.  Do not delete but use the srem multiple members
                        # method.
                        # logger.info('deleting Redis mirage.unique_metrics set to refresh')
                        # self.redis_conn.delete('mirage.unique_metrics')
                        # mirage_unique_metrics = []
                        logger.info('Redis mirage.unique_metrics set to refresh, checking for metrics to remove')
                        mirage_metrics_to_remove = []
                        # TODO use sets method
                        try:
                            mirage_unique_metrics = list(self.redis_conn_decoded.smembers('mirage.unique_metrics'))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to generate a list from the pruned mirage.unique_metrics')
                            mirage_unique_metrics = []
                        for mirage_metric in mirage_unique_metrics:
                            if mirage_metric not in unique_metrics:
                                mirage_metrics_to_remove.append(mirage_metric)
                        if mirage_metrics_to_remove:
                            logger.info('removing %s metrics from mirage.unique_metrics' % str(len(mirage_metrics_to_remove)))
                            self.redis_conn.srem('mirage.unique_metrics', *set(mirage_metrics_to_remove))
                            # Reload the new set
                            try:
                                mirage_unique_metrics = list(self.redis_conn_decoded.smembers('mirage.unique_metrics'))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to generate a list from the pruned mirage.unique_metrics')
                    except:
                        # logger.error('error :: could not query Redis to delete mirage.unique_metric set')
                        logger.error(traceback.format_exc())
                        logger.error('error :: could not remove multiple memeber from the mirage.unique_metric Redis set')

                    # @added 20200723 - Feature #3560: External alert config
                    # Speed this up
                    number_of_alerts_checked = 0
                    mirage_metrics_added = []

                    # @added 20201007 - Bug #3766: Refresh mirage.metrics_expiration_times for batch processing
                    #                   Task #3662: Change mirage.last_check keys to timestamp value
                    #                   Feature #3486: analyzer_batch
                    #                   Feature #3480: batch_processing
                    # Determine what metrics are in the new
                    # analyzer.mirage.metrics_expiration_times Redis set
                    analyzer_mirage_metrics_expiration_times_list = []
                    try:
                        # @modified 20201013 - Bug #3766: Refresh mirage.metrics_expiration_times for batch processing
                        # analyzer_mirage_metrics_expiration_times_list = list(self.redis_conn.smembers('analyzer.mirage.metrics_expiration_times'))
                        analyzer_mirage_metrics_expiration_times_list = list(self.redis_conn_decoded.smembers('analyzer.mirage.metrics_expiration_times'))
                        logger.info('%s items in Redis set analyzer.mirage.metrics_expiration_times' % str(len(analyzer_mirage_metrics_expiration_times_list)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to get Redis set analyzer.mirage.metrics_expiration_times')
                    mirage_metrics_expiration_times_known = []

                    # @added 20201104 - Feature #3788: snab_flux_load_test
                    #                   Feature #3560: External alert config
                    #                   Bug #3766: Refresh mirage.metrics_expiration_times for batch processing
                    # The analyzer.mirage.metrics_expiration_times set also
                    # needs to be refreshed, which takes the time... but if it
                    # is not, metrics that are change in alert setings context
                    # from smtp_alerting_metrics to a non_smtp_alerter_metrics
                    # does not get removed from the smtp_alerter_metrics as it
                    # is in this list and therefore never gets to run through
                    # the below pattern_match
                    analyzer_mirage_metrics_expiration_times_list = []

                    if analyzer_mirage_metrics_expiration_times_list:
                        try:
                            for item in analyzer_mirage_metrics_expiration_times_list:
                                metric_expiration = literal_eval(item)
                                mirage_metrics_expiration_times_known.append(str(metric_expiration[0]))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to determine metrics in analyzer.mirage.metrics_expiration_times')

                    # @modified 20200610 - Feature #3560: External alert config
                    # Use the all_alerts list which includes external alert configs
                    # for alert in settings.ALERTS:
                    # @modifed 20200723 - Feature #3560: External alert config
                    # Speed this up, change to metric as main iter
                    # for alert in all_alerts:
                    #     for metric in unique_metrics:
                    for metric in unique_metrics:
                        mirage_metric = False

                        # @added 20201007 - Bug #3766: Refresh mirage.metrics_expiration_times for batch processing
                        #                   Task #3662: Change mirage.last_check keys to timestamp value
                        #                   Feature #3486: analyzer_batch
                        #                   Feature #3480: batch_processing
                        # Determine what metrics are in the new
                        # analyzer.mirage.metrics_expiration_times Redis set
                        add_to_metric_expiration_times_set = False
                        smtp_expiration = None

                        if metric in mirage_unique_metrics:
                            if metric.startswith(settings.FULL_NAMESPACE):
                                base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
                            else:
                                base_name = metric
                            if base_name not in mirage_metrics_expiration_times_known:
                                add_to_metric_expiration_times_set = True

                            # @modified 20201007 - Bug #3766: Refresh mirage.metrics_expiration_times for batch processing
                            # continue
                            if not add_to_metric_expiration_times_set:
                                continue

                        for alert in all_alerts:
                            # @added 20200723 - Feature #3560: External alert config
                            # Speed this up
                            if mirage_metric:
                                # @modified 20201007 - Bug #3766: Refresh mirage.metrics_expiration_times for batch processing
                                # continue
                                if not add_to_metric_expiration_times_set:
                                    continue
                            try:
                                if alert[4]['type'] == 'external':
                                    continue
                            except:
                                pass
                            if alert[1] != 'smtp':
                                continue

                            # @added 20200904 - Task #3730: Validate Mirage running multiple processes
                            # Set default as None
                            SECOND_ORDER_RESOLUTION_FULL_DURATION = None

                            if not mirage_metric:
                                number_of_alerts_checked += 1
                                # ALERT_MATCH_PATTERN = alert[0]
                                # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
                                # base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
                                if metric.startswith(settings.FULL_NAMESPACE):
                                    base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
                                else:
                                    base_name = metric

                                # @modified 20200622 - Task #3586: Change all alert pattern checks to matched_or_regexed_in_list
                                #                      Feature #3512: matched_or_regexed_in_list function
                                # Changed original alert matching pattern to use new
                                # method
                                pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, [alert[0]])
                                if LOCAL_DEBUG and pattern_match:
                                    logger.debug('debug :: %s matched alert - %s' % (base_name, alert[0]))
                                try:
                                    del metric_matched_by
                                except:
                                    pass
                                if not pattern_match:
                                    continue

                                # mirage_metric = False
                                if not mirage_metric:
                                    try:
                                        # @modified 20200610 - Feature #3560: External alert config
                                        # SECOND_ORDER_RESOLUTION_FULL_DURATION = alert[3]
                                        SECOND_ORDER_RESOLUTION_FULL_DURATION = int(alert[3])
                                        if SECOND_ORDER_RESOLUTION_FULL_DURATION > 24:
                                            mirage_metric = True
                                    except:
                                        mirage_metric = False
                                # @added 20201007 - Bug #3766: Refresh mirage.metrics_expiration_times for batch processing
                                if mirage_metric:
                                    smtp_expiration = alert[2]

                        if mirage_metric:
                            # if metric not in mirage_unique_metrics:
                            if metric not in mirage_metrics_added:
                                try:
                                    self.redis_conn.sadd('mirage.unique_metrics', metric)
                                    if LOCAL_DEBUG:
                                        logger.debug('debug :: added %s to mirage.unique_metrics' % metric)
                                    # @added 20200723 - Feature #3560: External alert config
                                    # Speed this up
                                    mirage_metrics_added.append(metric)
                                except:
                                    if LOCAL_DEBUG:
                                        logger.error('error :: failed to add %s to mirage.unique_metrics set' % metric)

                                # @added 20200904 - Task #3730: Validate Mirage running multiple processes
                                # Also always add the mirage.metrics Redis key for the
                                # metric which contains its hours_to_resolve so
                                # that the spin_process can add the mirage check
                                # files immediately, rather than waiting to add
                                # the mirage checks all in the alerting phase.
                                # This is done to reduce the time it takes to
                                # get through the analysis pipeline.
                                if SECOND_ORDER_RESOLUTION_FULL_DURATION:
                                    mirage_metric_redis_key = 'mirage.metrics.%s' % base_name
                                    try:
                                        self.redis_conn.setex(mirage_metric_redis_key, int(alert[2]), SECOND_ORDER_RESOLUTION_FULL_DURATION)
                                        # @added 20201109 - Feature #3830: metrics_manager
                                        # Although it would seem sensible to
                                        # manage these mirage.metrics. keys with
                                        # metrics_manager, they are also used in
                                        # waterfall alert and must be dynamic
                                        # based on their EXPIRATION_TIME
                                        # therefore they cannot be created by
                                        # metric_manager
                                    except:
                                        logger.error('error :: failed to set key :: %s' % mirage_metric_redis_key)

                                try:
                                    key_timestamp = int(time())
                                    self.redis_conn.setex('analyzer.manage_mirage_unique_metrics', 300, key_timestamp)
                                except:
                                    logger.error('error :: failed to set key :: analyzer.manage_mirage_unique_metrics')
                            # @added 20200805 - Task #3662: Change mirage.last_check keys to timestamp value
                            #                   Feature #3486: analyzer_batch
                            #                   Feature #3480: batch_processing
                            # Add the mirage metric and its EXPIRATION_TIME to
                            # the mirage.metrics_expiration_times so that Mirage
                            # can determine the metric EXPIRATION_TIME without
                            # having to create and iterate the all_alerts
                            # object in the Mirage analysis phase so that the
                            # reported anomaly timestamp can be used to determine
                            # whether the EXPIRATION_TIME should be applied to a
                            # batch metric in the alerting and Ionosphere contexts
                            # mirage_alert_expiration_data = [base_name, int(alert[2])]
                            mirage_alert_expiration_data = [base_name, int(smtp_expiration)]
                            try:
                                # @modified 20201006 - Bug #3766: Refresh mirage.metrics_expiration_times for batch processing
                                #                      Task #3662: Change mirage.last_check keys to timestamp value
                                #                      Feature #3486: analyzer_batch
                                #                      Feature #3480: batch_processing
                                # Use a different set to refresh the set
                                # self.redis_conn.sadd('mirage.metrics_expiration_times', str(mirage_alert_expiration_data))
                                self.redis_conn.sadd('analyzer.mirage.metrics_expiration_times', str(mirage_alert_expiration_data))
                                if LOCAL_DEBUG:
                                    logger.debug('debug :: added %s to analyzer.mirage.metrics_expiration_times' % str(mirage_alert_expiration_data))
                            except:
                                if LOCAL_DEBUG:
                                    logger.error('error :: failed to add %s to analyzer.mirage.metrics_expiration_times set' % str(mirage_alert_expiration_data))

                    # @added 20200723 - Feature #3560: External alert config
                    # Add debug count
                    logger.info('number of alerts and metrics checked for mirage.unique_metrics - %s' % str(number_of_alerts_checked))
                    del mirage_metrics_added

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
                            logger.debug('debug :: fetched the mirage.unique_metrics Redis set')
                            logger.debug('debug :: %s' % str(mirage_unique_metrics))
                    except:
                        logger.info('failed to fetch the mirage.unique_metrics Redis set')
                        mirage_unique_metrics == []
                        mirage_unique_metrics_count = len(mirage_unique_metrics)
            # @modified 20160207 - Branch #922: Ionosphere
            # Handle if Mirage is not enabled
            else:
                mirage_unique_metrics = []
            # END Redis mirage.unique_metrics_set

            # @added 20201107 - Feature #3830: metrics_manager
            mirage_unique_metrics = []
            if settings.ENABLE_MIRAGE:
                try:
                    mirage_unique_metrics = list(self.redis_conn_decoded.smembers('mirage.unique_metrics'))
                except:
                    logger.info('failed to fetch the mirage.unique_metrics Redis set')
                    mirage_unique_metrics = []
                mirage_unique_metrics_count = len(mirage_unique_metrics)
                logger.info('mirage.unique_metrics Redis set count - %s' % str(mirage_unique_metrics_count))

            if LOCAL_DEBUG:
                # Do we use list or dict, which is better performance?
                # With dict - Use EAFP (easier to ask forgiveness than
                # permission)
                try:
                    blah = dict["mykey"]
                    # key exists in dict
                except:
                    # key doesn't exist in dict
                    blah = False
                logger.debug('debug :: Memory usage in run after unique_metrics: %s (kb), using blah %s' % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, str(blah)))

            # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # The Redis analyzer.smtp_alerter_metrics list is created here to
            # replace the self.smtp_alerter_metrics Manager.list in the below
            # section
            smtp_alerter_metrics = []
            try:
                # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                #                   Branch #3262: py3
                # smtp_alerter_metrics = list(self.redis_conn.smembers('analyzer.smtp_alerter_metrics'))
                # @modified 20200723 - Feature #3560: External alert config
                # Speed this up use the aet.analyzer.smtp_alerter_metrics and
                # a method was added to remove non-existent metrics from it
                # further on
                # smtp_alerter_metrics = list(self.redis_conn_decoded.smembers('analyzer.smtp_alerter_metrics'))
                smtp_alerter_metrics = list(self.redis_conn_decoded.smembers('aet.analyzer.smtp_alerter_metrics'))
                logger.info('currently %s metrics in smtp_alerter_metrics from aet.analyzer.smtp_alerter_metrics' % (str(len(smtp_alerter_metrics))))
                if LOCAL_DEBUG:
                    logger.debug('debug :: smtp_alerter_metrics :: %s' % (str(smtp_alerter_metrics)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to generate a list from the aet.analyzer.smtp_alerter_metrics Redis set')
                smtp_alerter_metrics = []

            if LOCAL_DEBUG:
                logger.debug('debug :: unique_metrics :: %s' % (str(unique_metrics)))

            # @added 20201107 - Feature #3830: metrics_manager
            try:
                non_smtp_alerter_metrics = list(self.redis_conn_decoded.smembers('aet.analyzer.non_smtp_alerter_metrics'))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to generate a list from aet.analyzer.non_smtp_alerter_metrics Redis set for priority based assigned_metrics')
                non_smtp_alerter_metrics = []

            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # logger.info('smtp_alerter_metrics     :: %s' % str(len(self.smtp_alerter_metrics)))
            # logger.info('non_smtp_alerter_metrics :: %s' % str(len(self.non_smtp_alerter_metrics)))
            logger.info('smtp_alerter_metrics     :: %s' % str(len(smtp_alerter_metrics)))
            logger.info('non_smtp_alerter_metrics :: %s' % str(len(non_smtp_alerter_metrics)))
            # del non_smtp_alerter_metrics_set

            # @added 20180423 - Feature #2360: CORRELATE_ALERTS_ONLY
            #                   Branch #2270: luminosity
            # Add a Redis set of smtp_alerter_metrics for Luminosity to only
            # cross correlate on metrics with an alert setting
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # Use variable from the Redis set which replaced Manager.list
            # for metric in self.smtp_alerter_metrics:

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
                    logger.error(traceback.format_exc())

                try:
                    with open(algorithm_timings_file, 'w') as f:
                        pass
                except:
                    logger.error('error :: could not create file %s' % algorithm_timings_file)
                    logger.error(traceback.format_exc())

            if LOCAL_DEBUG:
                logger.debug('debug :: Memory usage in run before removing algorithm_count_files and algorithm_timings_files: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Remove any existing algorithm.error files from any previous runs
            # that did not cleanup for any reason
            pattern = '%s.*.algorithm.error' % skyline_app
            try:
                for f in os.listdir(settings.SKYLINE_TMP_DIR):
                    if re.search(pattern, f):
                        try:
                            # @modified 20200904 - Feature #3486: analyzer_batch
                            #                      Feature #3480: batch_processing
                            #                      Task #3730: Validate Mirage running multiple processes
                            # Do not operate on analyzer_batch files
                            if 'analyzer_batch' not in f:
                                os.remove(os.path.join(settings.SKYLINE_TMP_DIR, f))
                                logger.info('cleaning up old error file - %s' % (str(f)))
                        except OSError:
                            pass
            except:
                logger.error('error :: failed to cleanup algorithm.error files')
                logger.error(traceback.format_exc())

            if LOCAL_DEBUG:
                logger.debug('debug :: Memory usage in run before spawning processes: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

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

            # @added 20200607 - Feature #3566: custom_algorithms
            try:
                self.redis_conn.rename('analyzer.new.mirage_always_metrics', 'analyzer.mirage_always_metrics')
            except:
                pass

            # @added 20201007 - Feature #3774: SNAB_LOAD_TEST_ANALYZER
            # The number of metrics to load test Analyzer with
            if SNAB_LOAD_TEST_ANALYZER:
                current_unique_metrics_count = len(unique_metrics)
                logger.info('SNAB_LOAD_TEST_ANALYZER set to test %s metrics' % str(SNAB_LOAD_TEST_ANALYZER))
                if current_unique_metrics_count < SNAB_LOAD_TEST_ANALYZER:
                    snab_load_test_metrics_to_add = SNAB_LOAD_TEST_ANALYZER - current_unique_metrics_count
                    logger.info('SNAB_LOAD_TEST_ANALYZER - there are currently %s unique metrics, %s snab.analyzer_load_test metrics will be added' % (
                        str(current_unique_metrics_count), str(snab_load_test_metrics_to_add)))
                snab_load_test_start_redis_key = 'snab.analyzer_load_test.start'
                try:
                    self.redis_conn.setex(
                        snab_load_test_start_redis_key, settings.MAX_ANALYZER_PROCESS_RUNTIME, int(time()))
                except Exception as e:
                    logger.error('error :: could not set Redis snab.analyzer_load_test.start key: %s' % e)

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
                    logger.error(traceback.format_exc())

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

            # @added 20201127 - Feature #3848: custom_algorithms - run_before_3sigma parameter
            # Determine if any custom_algorithms are to be run by analyzer so that
            # DEBUG_CUSTOM_ALGORITHMS only runs if a custom_algorithm is assigned to
            # analyzer
            run_custom_algorithm_enabled_on_app = None
            if CUSTOM_ALGORITHMS and DEBUG_CUSTOM_ALGORITHMS:
                for custom_algorithm in CUSTOM_ALGORITHMS:
                    try:
                        use_with = CUSTOM_ALGORITHMS[custom_algorithm]['use_with']
                        if skyline_app in use_with:
                            run_custom_algorithm_enabled_on_app = True
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: run failed to determine if custom_algorithms are to be run with %s' % (
                            skyline_app))

            # @added 20200601 - Feature #3508: ionosphere.untrainable_metrics
            #                   Feature #3400: Identify air gaps in the metric data
            # Check to non 3sigma algorithm errors too
            if LOCAL_DEBUG:
                logger.debug('debug :: adding negatives_present to check_algorithm_errors')
            check_algorithm_errors = ['negatives_present']
            if IDENTIFY_AIRGAPS:
                if LOCAL_DEBUG:
                    logger.debug('debug :: adding identify_airgaps to check_algorithm_errors')
                check_algorithm_errors.append('identify_airgaps')
            for algorithm in settings.ALGORITHMS:
                # @modified 20201127 - Feature #3848: custom_algorithms - run_before_3sigma parameter
                # Only run DEBUG_CUSTOM_ALGORITHMS if there is a custom
                # algorithm to be run with analyzer
                # if LOCAL_DEBUG or DEBUG_CUSTOM_ALGORITHMS:
                if LOCAL_DEBUG or (DEBUG_CUSTOM_ALGORITHMS and run_custom_algorithm_enabled_on_app):
                    logger.debug('debug :: adding %s to check_algorithm_errors' % (algorithm))
                check_algorithm_errors.append(algorithm)

            # @added 20200603 - Feature #3566: custom_algorithms
            if CUSTOM_ALGORITHMS:
                for custom_algorithm in settings.CUSTOM_ALGORITHMS:
                    # @modified 20201127 - Feature #3848: custom_algorithms - run_before_3sigma parameter
                    # Only run DEBUG_CUSTOM_ALGORITHMS if there is a custom
                    # algorithm to be run with analyzer
                    # if LOCAL_DEBUG or DEBUG_CUSTOM_ALGORITHMS:
                    if LOCAL_DEBUG or (DEBUG_CUSTOM_ALGORITHMS and run_custom_algorithm_enabled_on_app):
                        logger.debug('debug :: adding custom_algorithm %s to check_algorithm_errors' % (custom_algorithm))
                    check_algorithm_errors.append(custom_algorithm)

            # Log the last reported error by any algorithms that errored in the
            # spawned processes from algorithms.py
            for completed_pid in spawned_pids:
                logger.info('spin_process with pid %s completed' % (str(completed_pid)))
                # @modified 20200601 - Feature #3508: ionosphere.untrainable_metrics
                #                      Feature #3400: Identify air gaps in the metric data
                # Check to non 3sigma algorithm errors too
                # for algorithm in settings.ALGORITHMS:
                try:
                    for algorithm in check_algorithm_errors:
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
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to check algorithm errors')

            if LOCAL_DEBUG:
                logger.debug('debug :: Memory usage after spin_process spawned processes finish: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

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
            # modified 20201020 - Feature #3792: algorithm_exceptions - EmptyTimeseries
            # Added EmptyTimeseries
            exceptions_metrics = ['Boring', 'Stale', 'TooShort', 'Other', 'EmptyTimeseries']
            for i_exception in exceptions_metrics:
                if i_exception not in exceptions.keys():
                    exceptions[i_exception] = 0

            # @added 20200603 - Feature #3566: custom_algorithms
            anomaly_breakdown_algorithms = list(settings.ALGORITHMS)
            if CUSTOM_ALGORITHMS:
                for custom_algorithm in settings.CUSTOM_ALGORITHMS:
                    anomaly_breakdown_algorithms.append(custom_algorithm)

            # @modified 20200603 - Feature #3566: custom_algorithms
            # for i_anomaly_breakdown in settings.ALGORITHMS:
            for i_anomaly_breakdown in anomaly_breakdown_algorithms:
                if i_anomaly_breakdown not in anomaly_breakdown.keys():
                    anomaly_breakdown[i_anomaly_breakdown] = 0

            if LOCAL_DEBUG:
                logger.debug('debug :: Memory usage in run before alerts: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

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
                        logger.debug('debug :: analyzer.anomalous_metrics Redis set surfaced - %s' % str(literal_analyzer_anomalous_metrics))
                    else:
                        logger.debug('debug :: analyzer.anomalous_metrics no Redis set data')
                for metric_list_string in literal_analyzer_anomalous_metrics:
                    metric = literal_eval(metric_list_string)
                    analyzer_anomalous_metrics.append(metric)
            except:
                logger.error(traceback.format_exc())
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
                            logger.debug('debug :: added %s to %s Redis set' % (
                                str(data), redis_set))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add %s to Redis set %s' % (
                            str(data), str(redis_set)))
            except:
                logger.error(traceback.format_exc())
                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # logger.error('error :: failed to add to self.all_anomalous_metrics')
                logger.error('error :: failed to add to analyzer.all_anomalous_metrics Redis set')
            ionosphere_alerts = []
            context = 'Analyzer'
            logger.info('checking for ionosphere.analyzer.alert.* Redis keys')
            try:
                # @modified 20200430 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # ionosphere_alerts = list(self.redis_conn.scan_iter(match='ionosphere.analyzer.alert.*'))
                ionosphere_alerts = list(self.redis_conn_decoded.scan_iter(match='ionosphere.analyzer.alert.*'))
                if LOCAL_DEBUG:
                    logger.debug('debug :: ionosphere.analyzer.alert.* Redis keys - %s' % (
                        str(ionosphere_alerts)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to scan ionosphere.analyzer.alert.* from Redis')
            logger.info('%s ionosphere_alerts from ionosphere.analyzer.alert.* Redis keys' % str(len(ionosphere_alerts)))

            if not ionosphere_alerts:
                ionosphere_alerts = []

            # @added 20180914 - Bug #2594: Analyzer Ionosphere alert on Analyzer data point
            ionosphere_anomalous_metrics = []

            ionosphere_metric_alerts = []
            if len(ionosphere_alerts) != 0:
                logger.info('Ionosphere alert/s requested :: %s' % str(ionosphere_alerts))
                for cache_key in ionosphere_alerts:
                    try:
                        # @modified 20201110 - Feature #3734: waterfall alerts
                        # Now that alerts can get back to Analyzer via waterfall
                        # alerting from Ionosphere the new _decoded function is
                        # required
                        # alert_on = self.redis_conn.get(cache_key)
                        alert_on = self.redis_conn_decoded.get(cache_key)
                        send_alert_for = literal_eval(alert_on)
                        value = float(send_alert_for[0])
                        base_name = str(send_alert_for[1])
                        metric_timestamp = int(float(send_alert_for[2]))
                        triggered_algorithms = send_alert_for[3]
                        # @added 20201001 - Task #3748: POC SNAB
                        # Added algorithms_run required to determine the
                        # anomalyScore for snab
                        algorithms_run = send_alert_for[5]

                        # @added 20201001 - Task #3748: POC SNAB
                        # Added triggered_algorithms and algorithms_run required
                        # to determine the anomalyScore for snab
                        # anomalous_metric = [value, base_name, metric_timestamp]
                        anomalous_metric = [value, base_name, metric_timestamp, triggered_algorithms, algorithms_run]

                        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        # self.all_anomalous_metrics.append(anomalous_metric)
                        redis_set = 'analyzer.all_anomalous_metrics'
                        data = str(anomalous_metric)
                        try:
                            self.redis_conn.sadd(redis_set, data)
                        except:
                            logger.error(traceback.format_exc())
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

            # @added 20200907 - Feature #3734: waterfall alerts
            # Add alert for expired waterfall_alert items
            # [metric, timestamp, value, added_to_waterfall_timestamp]
            # waterfall_data = [metric[1], metric[2], metric[0], added_to_waterfall_timestamp]
            waterfall_alert_check_timestamp = int(time())
            waterfall_alerts_to_alert_on = []
            # A list to add a metric,timestamp string to in order to override
            # the mirage_metric or ionosphere_metric in the alerting block
            alerting_waterfall_alerts = []
            waterfall_redis_sets = [
                'analyzer.waterfall_alerts.sent_to_ionosphere',
                'analyzer.waterfall_alerts.sent_to_mirage'
            ]
            for waterfall_redis_set in waterfall_redis_sets:
                redis_set = waterfall_redis_set
                literal_waterfall_alerts = []
                try:
                    literal_waterfall_alerts = list(self.redis_conn_decoded.smembers(redis_set))
                except:
                    literal_waterfall_alerts = []
                waterfall_alerts = []
                logger.info('checking for expired checks in %s waterfall alerts in Redis set %s' % (
                    str(len(literal_waterfall_alerts)), redis_set))
                for literal_waterfall_alert in literal_waterfall_alerts:
                    waterfall_alert = literal_eval(literal_waterfall_alert)
                    waterfall_alerts.append(waterfall_alert)
                for waterfall_alert in waterfall_alerts:
                    if waterfall_alert_check_timestamp >= (int(waterfall_alert[3]) + 300):
                        try:
                            self.redis_conn.srem(redis_set, str(waterfall_alert))
                            logger.info('removed waterfall alert item to alert on from Redis set %s - %s' % (
                                redis_set, str(waterfall_alert)))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to remove expired waterfall alert item for %s from Redis set %s' % (
                                base_name, redis_set))

                        # @added 20201008 - Feature #3734: waterfall alerts
                        #                   Branch #3068: SNAB
                        #                   Bug #3776: waterfall_alert - no analyzer triggered_algorithms in waterfall_panorama_data on MIRAGE_ALWAYS_METRICS
                        append_waterfall_alert = True
                        try:
                            append_waterfall_alert = True
                            if waterfall_alert[0] in MIRAGE_ALWAYS_METRICS:
                                # If triggered_algorithms is less than CONSENSUS
                                # do not alert
                                # @modified 20201009 - Bug #3776: waterfall_alert - no analyzer triggered_algorithms in waterfall_panorama_data on MIRAGE_ALWAYS_METRICS
                                # Correct the operator
                                # if len(waterfall_alert[4][5]) > settings.CONSENSUS:
                                if len(waterfall_alert[4][5]) < settings.CONSENSUS:
                                    append_waterfall_alert = False
                                    logger.info('not waterfall alerting for MIRAGE_ALWAYS_METRICS metric that has no  item to alert on from Redis set %s - %s' % (
                                        redis_set, str(waterfall_alert)))
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to determine if waterfall alert metric is a MIRAGE_ALWAYS_METRICS metric with less than CONSENSUS triggered_algorithms - %s' % (
                                e))

                        # @modified 20201008 - Feature #3734: waterfall alerts
                        #                      Branch #3068: SNAB
                        #                      Bug #3776: waterfall_alert - no analyzer triggered_algorithms in waterfall_panorama_data on MIRAGE_ALWAYS_METRICS
                        # Only alert if it is not a MIRAGE_ALWAYS_METRICS
                        # check
                        if append_waterfall_alert:
                            waterfall_alerts_to_alert_on.append(waterfall_alert)

            for waterfall_alert in waterfall_alerts_to_alert_on:
                try:
                    value = float(waterfall_alert[2])
                    base_name = str(waterfall_alert[0])
                    metric_timestamp = int(waterfall_alert[1])
                    # @added 20201008 - Feature #3734: waterfall alerts
                    #                   Branch #3068: SNAB
                    # Added triggered_algorithms and algorithms_run
                    algorithms_run = waterfall_alert[4][4]
                    triggered_algorithms = waterfall_alert[4][5]

                    # @modified 20201008 - Feature #3734: waterfall alerts
                    #                      Branch #3068: SNAB
                    # Added triggered_algorithms and algorithms_run
                    # anomalous_metric = [value, base_name, metric_timestamp]
                    anomalous_metric = [value, base_name, metric_timestamp, triggered_algorithms, algorithms_run]

                    redis_set = 'analyzer.all_anomalous_metrics'
                    data = str(anomalous_metric)
                    waterfall_alert_check_string = '%s.%s' % (str(metric_timestamp), base_name)
                    try:
                        self.redis_conn.sadd(redis_set, data)
                        alerting_waterfall_alerts.append(waterfall_alert_check_string)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add %s to Redis set %s for waterfall alert to alert on' % (
                            str(data), str(redis_set)))
                    logger.info('waterfall alerting on %s' % base_name)
                    redis_waterfall_alert_key = 'analyzer.waterfall.alert.%s' % waterfall_alert_check_string
                    try:
                        self.redis_conn.setex(redis_waterfall_alert_key, 300, waterfall_alert_check_timestamp)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add Redis key - %s for waterfall alert' % (
                            redis_waterfall_alert_key))

                    # @added 20201001 - Feature #3734: waterfall alerts
                    #                   Task #3748: POC SNAB
                    #                   Branch #3068: SNAB
                    # Added Panorama anomaly details for waterfall alerts
                    # Note:
                    # The values are enclosed is single quoted intentionally
                    # as the imp.load_source used results in a shift in the
                    # decimal position when double quoted, e.g.
                    # value = "5622.0" gets imported as
                    # 2016-03-02 12:53:26 :: 28569 :: metric variable - value - 562.2
                    # single quoting results in the desired,
                    # 2016-03-02 13:16:17 :: 1515 :: metric variable - value - 5622.0
                    logger.info('adding panorama anomaly file for waterfall alert on %s' % base_name)
                    panorama_data = None
                    try:
                        panorama_data = waterfall_alert[4]
                        from_timestamp = str(int(panorama_data[2]))
                        int_metric_timestamp = int(panorama_data[3])
                        algorithms_run = panorama_data[4]
                        triggered_algorithms = panorama_data[5]
                        source = panorama_data[7]
                        added_at = str(int(time()))
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
                            % (base_name, str(value), from_timestamp,
                               str(int_metric_timestamp), str(algorithms_run),
                               triggered_algorithms, skyline_app, source,
                               this_host, added_at)
                        if VERBOSE_LOGGING:
                            logger.info('panorama anomaly data for waterfall alert - %s' % str(panorama_data))
                        # Create an anomaly file with details about the anomaly
                        sane_metricname = filesafe_metricname(str(base_name))
                        panaroma_anomaly_file = '%s/%s.%s.txt' % (
                            settings.PANORAMA_CHECK_PATH, added_at, sane_metricname)
                        try:
                            write_data_to_file(
                                skyline_app, panaroma_anomaly_file, 'w',
                                panaroma_anomaly_data)
                            if VERBOSE_LOGGING:
                                logger.info('added panorama anomaly file for waterfall alert :: %s' % (panaroma_anomaly_file))
                        except:
                            logger.error('error :: failed to add panorama anomaly file :: %s' % (panaroma_anomaly_file))
                            logger.error(traceback.format_exc())
                        redis_set = 'mirage.sent_to_panorama'
                        data = str(base_name)
                        try:
                            self.redis_conn.sadd(redis_set, data)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to add %s to Redis set %s' % (
                                str(data), str(redis_set)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add panorama anomaly data file for waterfall alert')

                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to add waterfall alert to alert on to analyzer.all_anomalous_metrics')

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
                        logger.debug('debug :: analyzer_batch.alert.* Redis keys - %s' % (
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
                            # @added 20201008 - Feature #3734: waterfall alerts
                            #                   Branch #3068: SNAB
                            # Added algorithms_run
                            algorithms_run = send_alert_for[4]

                            # @modified 20201008 - Feature #3734: waterfall alerts
                            #                      Branch #3068: SNAB
                            # Added triggered_algorithms and algorithms_run
                            # anomalous_metric = [value, base_name, metric_timestamp]
                            anomalous_metric = [value, base_name, metric_timestamp, triggered_algorithms, algorithms_run]
                            redis_set = 'analyzer.all_anomalous_metrics'
                            data = str(anomalous_metric)
                            try:
                                self.redis_conn.sadd(redis_set, data)
                            except:
                                logger.error(traceback.format_exc())
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

            # @modified 20161229 - Feature #1830: Ionosphere alerts
            # Handle alerting for Ionosphere
            # for metric in self.anomalous_metrics:
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # for metric in self.all_anomalous_metrics:
            # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
            #                   Branch #3262: py3
            # all_anomalous_metrics = list(self.redis_conn.smembers('analyzer.all_anomalous_metrics'))
            # @modified 20200608 - Feature #3560: External alert config
            # Moved from inside for alert in settings.ALERTS so it is only
            # queried once
            all_anomalous_metrics = []
            try:
                all_anomalous_metrics = list(self.redis_conn_decoded.smembers('analyzer.all_anomalous_metrics'))
                if LOCAL_DEBUG:
                    logger.debug('debug :: for alert in settings.ALERTS analyzer.all_anomalous_metrics - %s' % (
                        str(all_anomalous_metrics)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to query Redis set analyzer.all_anomalous_metrics')

            # @added 20201014 - Feature #3734: waterfall alerts
            # Remove ionosphere training data as all the
            # resources required are not available for training
            remove_ionosphere_training_data_for = []

            spin_process_sent_to_mirage_list = []

            # Send alerts
            if settings.ENABLE_ALERTS:
                try:
                    logger.info('checking for alerts')
                    # @modified 20200610 - Feature #3560: External alert config
                    # Use the all_alerts list which includes external alert configs
                    # for alert in settings.ALERTS:
                    for alert in all_alerts:
                        # @modified 20200608 - Feature #3560: External alert config
                        # Moved to above so only called once
                        # all_anomalous_metrics = list(self.redis_conn_decoded.smembers('analyzer.all_anomalous_metrics'))
                        for metric_list_string in all_anomalous_metrics:

                            # @added 20201014 - Feature #3734: waterfall alerts
                            second_order_resolution_seconds = 0

                            metric = literal_eval(metric_list_string)
                            if LOCAL_DEBUG:
                                logger.debug('debug :: metric in all_anomalous_metrics - %s' % (
                                    str(metric)))

                            # @added 20180914 - Bug #2594: Analyzer Ionosphere alert on Analyzer data point
                            # Set each metric to the default Analyzer context
                            context = 'Analyzer'

                            pattern_match = False

                            # @modified 20200622 - Task #3586: Change all alert pattern checks to matched_or_regexed_in_list
                            #                      Feature #3512: matched_or_regexed_in_list function
                            # Changed original alert matching pattern to use new
                            # method
                            try:
                                pattern_match, matched_by = matched_or_regexed_in_list(skyline_app, metric[1], [alert[0]])
                                if LOCAL_DEBUG and pattern_match:
                                    logger.debug('debug :: %s matched alert - %s' % (metric[1], alert[0]))
                                # try:
                                #     del metric_matched_by
                                # except:
                                #     pass
                            except:
                                pattern_match = False

                            if not pattern_match:
                                if LOCAL_DEBUG:
                                    logger.debug('debug :: metric not pattern matched, continuing - %s' % (
                                        str(metric[1])))
                                continue

                            mirage_metric = False
                            analyzer_metric = True

                            # @added 20200610 - Feature #3560: External alert config
                            # @modified 20200624 - Feature #3560: External alert config
                            # Set the alert key to the external alerter id
                            # external_alerter_alerter = None
                            external_alerter_id = None
                            try:
                                if alert[4]['type'] == 'external':
                                    # @modified 20200624 - Feature #3560: External alert config
                                    # Set the alert key to the external alerter id
                                    # external_alerter_alerter = alert[4]['alerter']
                                    external_alerter_id = alert[4]['id'].replace('external-', '')
                            except:
                                external_alerter_id = None

                            # @modified 20200610 - Feature #3560: External alert config
                            # Use the all_alerts list which includes external alert configs
                            # @modified 20200624 - Feature #3560: External alert config
                            # Set the alert key to the external alerter id
                            # if external_alerter_alerter:
                            #     cache_key = 'last_alert.%s.%s.%s' % (str(external_alerter_alerter), alert[1], metric[1])
                            if external_alerter_id:
                                cache_key = 'last_alert.%s.%s.%s' % (str(external_alerter_id), alert[1], metric[1])
                            else:
                                cache_key = 'last_alert.%s.%s' % (alert[1], metric[1])
                            if LOCAL_DEBUG:
                                logger.debug('debug :: last_alert cache key %s' % (
                                    str(cache_key)))

                            if settings.ENABLE_MIRAGE:
                                try:
                                    # @modified 20181023 - Feature #2618: alert_slack
                                    # SECOND_ORDER_RESOLUTION_FULL_DURATION = alert[3]
                                    # @modified 20200701 - Feature #3560: External alert config
                                    #                      Task #3608: Update Skyline to Python 3.8.3 and deps
                                    # SECOND_ORDER_RESOLUTION_FULL_DURATION = int(alert[3])
                                    try:
                                        SECOND_ORDER_RESOLUTION_FULL_DURATION = int(alert[3])
                                    except:
                                        SECOND_ORDER_RESOLUTION_FULL_DURATION = 0
                                    # @modified 20200610 - Feature #3560: External alert config
                                    # Wrapped in if so that if alert[3] 0 is
                                    # also handled in the all_alerts list
                                    if SECOND_ORDER_RESOLUTION_FULL_DURATION > 24:
                                        mirage_metric = True
                                        analyzer_metric = False
                                        # @modified 20201104 - #3560: External alert config
                                        # Only log if in debug mode
                                        if LOCAL_DEBUG:
                                            logger.debug(
                                                'debug :: mirage check :: %s at %s hours - matched by %s' %
                                                (metric[1],
                                                    str(SECOND_ORDER_RESOLUTION_FULL_DURATION),
                                                    str(matched_by)))
                                        context = 'Mirage'
                                    # @added 20201014 - Feature #3734: waterfall alerts
                                    # Only gets set on the first stmp alerter in
                                    # order to determine what ionosphere.training_data
                                    # item to remove on waterfall alerts
                                    if not second_order_resolution_seconds:
                                        second_order_resolution_seconds = int(SECOND_ORDER_RESOLUTION_FULL_DURATION * 3600)

                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to determine if metric is a Mirage metric')
                                    mirage_metric = False
                                    if LOCAL_DEBUG:
                                        logger.debug('debug :: not Mirage metric - %s' % metric[1])

                            # @added 20200907 - Feature #3734: waterfall alerts
                            waterfall_alert_check_string = '%s.%s' % (str(int(metric[2])), metric[1])
                            if waterfall_alert_check_string in alerting_waterfall_alerts:
                                mirage_metric = False
                                analyzer_metric = True
                                logger.info(
                                    'waterfall alerting for mirage metric check - %s at %s' %
                                    (metric[1], int(metric[2])))

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
                                # matching it became evident that Analyzer could
                                # alert on a Mirage metric via a match on a parent
                                # namespace pattern later in the ALERTS tuples.
                                # If it is a Mirage metric, we add a mirage.metrics
                                # key for Analyzer to check in the analyzer_metric
                                # step below.  This allows us to wildcard and
                                # substring match, but the mirage.metrics keys act
                                # as a dynamic SKIP_LIST for Analyzer

                                # @modified 20201109 - Feature #3830: metrics_manager
                                # Use the metrics_manager mirage.hash_key.metrics_resolutions
                                # hash key
                                if not ANALYZER_USE_METRICS_MANAGER:
                                    mirage_metric_cache_key = 'mirage.metrics.%s' % metric[1]
                                    try:
                                        # @modified 20200904 - Task #3730: Validate Mirage running multiple processes
                                        # Use the resolution instead of the value,
                                        # the packb value is not used anyway
                                        # self.redis_conn.setex(mirage_metric_cache_key, alert[2], packb(metric[0]))
                                        self.redis_conn.setex(mirage_metric_cache_key, alert[2], SECOND_ORDER_RESOLUTION_FULL_DURATION)
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: failed to add mirage.metrics Redis key - %s' % str(mirage_metric_cache_key))

                                # metric_timestamp = int(time())
                                # anomaly_check_file = '%s/%s.%s.txt' % (settings.MIRAGE_CHECK_PATH, metric_timestamp, metric[1])
                                # @added 20170206 - Bug #1904: Handle non filesystem friendly metric names in check files
                                anomaly_check_file = None
                                try:
                                    sane_metricname = filesafe_metricname(str(metric[1]))
                                    anomaly_check_file = '%s/%s.%s.txt' % (settings.MIRAGE_CHECK_PATH, str(int(metric[2])), sane_metricname)
                                    # @added 20211129 - Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
                                    if str(metric[1]) in batch_metrics_list:
                                        anomaly_check_file = '%s/%s.%s.%s.txt' % (
                                            settings.MIRAGE_CHECK_PATH,
                                            str(int(time())),
                                            str(int(metric[2])),
                                            sane_metricname)
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to determine anomaly_check_file')

                                # @added 20200904 - Task #3730: Validate Mirage running multiple processes
                                # Deduplicate checks sent to Mirage. At times
                                # analyzer will send the same metric and
                                # timestamp to Mirage on consequtive runs as the
                                # metric may not have updated
                                send_check_to_mirage = True
                                spin_process_sent_to_mirage = False

                                if anomaly_check_file:
                                    anomaly_check_file_created = False
                                    # @added 20200904 - Task #3730: Validate Mirage running multiple processes
                                    # Only add the mirage check files if it was
                                    # added by the spin_process
                                    sent_to_mirage = []
                                    try:
                                        sent_to_mirage = list(self.redis_conn_decoded.smembers('analyzer.sent_to_mirage'))
                                    except:
                                        sent_to_mirage = []
                                    if metric[1] in sent_to_mirage:
                                        spin_process_sent_to_mirage = True

                                # @added 20200904 - Task #3730: Validate Mirage running multiple processes
                                # Only add the metric check file if it was not
                                # added immediately in the spin_process
                                if spin_process_sent_to_mirage:
                                    if metric[1] not in spin_process_sent_to_mirage_list:
                                        if VERBOSE_LOGGING:
                                            logger.info('spin_process already added mirage check for %s' % (metric[1]))
                                        send_check_to_mirage = False
                                        spin_process_sent_to_mirage_list.append(metric[1])

                                # Only add the metric check file if it the check
                                # was not already submitted in a past run
                                if send_check_to_mirage:
                                    mirage_check_sent = False
                                    mirage_check_sent_key = 'analyzer.mirage_check_sent.%s.%s' % (
                                        str(int(metric[2])), str(metric[1]))
                                    try:
                                        mirage_check_sent = self.redis_conn_decoded.get(mirage_check_sent_key)
                                    except Exception as e:
                                        logger.error('error :: could not query Redis for cache_key: %s' % e)
                                    if mirage_check_sent:
                                        if VERBOSE_LOGGING:
                                            logger.info('a mirage check for %s at %s was already sent at %s, Redis key exists - %s' % (
                                                metric[1], str(metric[2]),
                                                str(mirage_check_sent), str(mirage_check_sent_key)))
                                        send_check_to_mirage = False

                                if send_check_to_mirage:
                                    try:
                                        redis_set = 'analyzer.sent_to_mirage'
                                        data = str(metric[1])
                                        try:
                                            self.redis_conn.sadd(redis_set, data)
                                        except Exception as e:
                                            logger.error(traceback.format_exc())
                                            logger.error('error :: failed to add %s to Redis set %s - %s' % (
                                                str(data), str(redis_set), e))
                                    except Exception as e:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: failed add %s to analyzer.sent_to_mirage Redis set - %s' % (
                                            metric[1], e))
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
                                            # @added 20211127 - Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
                                            # Attempt to get resolution from Redis
                                            try:
                                                use_hours_to_resolve_str = self.redis_conn_decoded.hget('mirage.hash_key.metrics_resolutions', metric[1])
                                                if use_hours_to_resolve_str:
                                                    use_hours_to_resolve = int(use_hours_to_resolve_str)
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

                                        # @added 20200904 - Feature #3734: waterfall alerts
                                        # added_to_waterfall_timestamp = int(time())
                                        # # [metric, timestamp, value, added_to_waterfall_timestamp]
                                        # waterfall_data = [metric[1], metric[2], metric[0], added_to_waterfall_timestamp]
                                        # redis_set = 'analyzer.waterfall_alerts.sent_to_mirage'
                                        # try:
                                        #     self.redis_conn.sadd(redis_set, str(waterfall_data))
                                        #     logger.info('added to Redis set %s - %s' % (redis_set, str(waterfall_data)))
                                        # except:
                                        #     logger.error(traceback.format_exc())
                                        #     logger.error('error :: failed to add %s to Redis set %s' % (
                                        #         str(waterfall_data), str(redis_set)))

                                        # @added 20200904 - Task #3730: Validate Mirage running multiple processes
                                        # Add a Redis key for the metric and timestamp
                                        # so that it does not get added multiple
                                        # times on consecutive runs if the metric
                                        # does not change
                                        mirage_check_sent_key = 'analyzer.mirage_check_sent.%s.%s' % (
                                            str(int(metric[2])), str(metric[1]))
                                        try:
                                            # @modified 20201006 - Feature #3734: waterfall alerts
                                            # No longer using added_to_waterfall_timestamp
                                            # self.redis_conn.setex(mirage_check_sent_key, 300, added_to_waterfall_timestamp)
                                            self.redis_conn.setex(mirage_check_sent_key, 300, int(metric[2]))
                                        except Exception as e:
                                            logger.error('error :: could not query Redis for cache_key: %s' % e)

                                # Alert for analyzer if enabled
                                # This sends an Analyzer ALERT for every check that
                                # is sent to Mirage.
                                if settings.ENABLE_FULL_DURATION_ALERTS:

                                    # @added 20210801 - Feature #4214: alert.paused
                                    alert_paused = False
                                    try:
                                        cache_key = 'alert.paused.%s.%s' % (alert[1], base_name)
                                        alert_paused = self.redis_conn_decoded.get(cache_key)
                                    except Exception as e:
                                        logger.error('error :: alert_paused check failed: %s' % str(e))
                                    if alert_paused:
                                        logger.info('alert_paused for %s %s until %s' % (
                                            alert[1], base_name, str(alert_paused)))
                                        continue

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
                                        logger.debug('debug :: ENABLE_FULL_DURATION_ALERTS not enabled')
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
                                        # @added 20200907 - Feature #3734: waterfall alerts
                                        waterfall_alert_check_string = '%s.%s' % (str(int(metric[2])), metric[1])
                                        if waterfall_alert_check_string not in alerting_waterfall_alerts:
                                            if VERBOSE_LOGGING:
                                                logger.info('not alerting - Ionosphere metric - %s' % str(metric[1]))
                                            continue
                                        else:
                                            logger.info(
                                                'waterfall alerting for ionosphere metric check - %s at %s' %
                                                (metric[1], int(metric[2])))
                                else:
                                    logger.error('error :: Ionosphere not report up')
                                    logger.info('taking over alerting from Ionosphere if alert is matched on - %s' % str(metric[1]))
                            else:
                                if LOCAL_DEBUG:
                                    logger.debug('debug :: metric_name not in ionosphere_unique_metrics - %s' % str(metric_name))

                            # @added 20200907 - Feature #3734: waterfall alerts
                            waterfall_alert_check_string = '%s.%s' % (str(int(metric[2])), metric[1])
                            if waterfall_alert_check_string not in alerting_waterfall_alerts:
                                # @added 20161231 - Feature #1830: Ionosphere alerts
                                #                   Analyzer also alerting on Mirage metrics now #22
                                # Do not alert on Mirage metrics
                                if metric_name in mirage_unique_metrics:
                                    if VERBOSE_LOGGING:
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
                                        # @modified 20201109 - Feature #3830: metrics_manager
                                        # Use the metrics_manager mirage.hash_key.metrics_resolutions
                                        # hash key
                                        # mirage_metric_key = self.redis_conn.get(mirage_metric_cache_key)
                                        raw_mirage_metric_key = self.redis_conn_decoded.hget('mirage.hash_key.metrics_resolutions', metric[1])
                                        if raw_mirage_metric_key:
                                            mirage_metric_key = int(raw_mirage_metric_key)
                                        else:
                                            mirage_metric_key = False
                                        if LOCAL_DEBUG:
                                            logger.debug('debug :: %s metric resolution - %s' % (metric[1], str(mirage_metric_key)))
                                    except Exception as e:
                                        logger.warning('warning :: could not determine resolution for %s from mirage.hash_key.metrics_resolutions Redis hash key, setting mirage_metric_key to False.  error: %s' % (
                                            metric[1], e))
                                        mirage_metric_key = False

                                    # @added 20200907 - Feature #3734: waterfall alerts
                                    waterfall_alert_check_string = '%s.%s' % (str(int(metric[2])), metric[1])
                                    if waterfall_alert_check_string in alerting_waterfall_alerts:
                                        mirage_metric_key = False
                                        logger.info(
                                            'waterfall alerting for mirage metric check overriding mirage_metric_key alerting for - %s at %s' %
                                            (metric[1], int(metric[2])))

                                if mirage_metric_key:
                                    if LOCAL_DEBUG:
                                        logger.debug('debug :: mirage metric key exists, skipping %s' % metric[1])
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
                                        # str(metric_timestamp), metric)
                                        str(int(metric[2])), metric)
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
                                            logger.error(traceback.format_exc())
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

                                # @added 20200907 - Feature #3734: waterfall alerts
                                waterfall_alert_check_string = '%s.%s' % (str(int(metric[2])), metric[1])
                                if waterfall_alert_check_string not in alerting_waterfall_alerts:

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

                                # @added 20210801 - Feature #4214: alert.paused
                                alert_paused = False
                                try:
                                    # cache_key = 'alert.paused.%s.%s' % (alert[1], base_name)
                                    cache_key = 'alert.paused.%s.%s' % (alert[1], metric[1])
                                    alert_paused = self.redis_conn_decoded.get(cache_key)
                                except Exception as e:
                                    logger.error('error :: alert_paused check failed: %s' % str(e))
                                    alert_paused = False
                                if alert_paused:
                                    logger.info('alert_paused for %s %s until %s' % (
                                        alert[1], metric[1], str(alert_paused)))

                                try:
                                    if alert[1] != 'smtp':

                                        # @added 20201008 - Feature #3772: Add the anomaly_id to the http_alerter json
                                        #                   Feature #3734: waterfall alerts
                                        #                   Branch #3068: SNAB
                                        new_alert = None
                                        if 'http_alerter' in alert[1]:
                                            anomaly_id = None
                                            anomaly_id_redis_key = 'panorama.anomaly_id.%s.%s' % (
                                                str(int(metric[2])), metric[1])
                                            try_get_anomaly_id_redis_key_count = 0
                                            while try_get_anomaly_id_redis_key_count < 20:
                                                try_get_anomaly_id_redis_key_count += 1
                                                try:
                                                    anomaly_id = int(self.redis_conn_decoded.get(anomaly_id_redis_key))
                                                    break
                                                except:
                                                    sleep(1)
                                            if not anomaly_id:
                                                logger.error('error :: failed to determine anomaly_id from Redis key - %s' % anomaly_id_redis_key)
                                            else:
                                                logger.info('determined anomaly_id as %s, appending to alert' % str(anomaly_id))
                                            # Do not modify the alert list object, create a new one
                                            new_alert = list(alert)
                                            new_alert.append(['anomaly_id', anomaly_id])

                                            # @added 20201111 - Feature #3772: Add the anomaly_id to the http_alerter json
                                            # Add the real anomalyScore
                                            try:
                                                # @added 20201112 - Feature #3772: Add the anomaly_id to the http_alerter json
                                                # Determine triggered and run
                                                # algorithms
                                                triggered_algorithms = metric[3]
                                                algorithms_run = metric[4]

                                                if triggered_algorithms and algorithms_run:
                                                    anomalyScore = len(triggered_algorithms) / len(algorithms_run)
                                                else:
                                                    anomalyScore = 1.0
                                            except:
                                                logger.error(traceback.format_exc())
                                                logger.error('error :: failed to determine anomalyScore for %s' % base_name)
                                                anomalyScore = 1.0
                                            new_alert.append(['anomalyScore', anomalyScore])

                                        # @modified 20201008 - Feature #3772: Add the anomaly_id to the http_alerter json
                                        #                      Feature #3734: waterfall alerts
                                        #                      Branch #3068: SNAB
                                        if new_alert:
                                            # @added 20210801 - Feature #4214: alert.paused
                                            if not alert_paused:
                                                trigger_alert(new_alert, metric, context)
                                                logger.info('trigger_alert :: alert: %s, metric: %s, context: %s' % (
                                                    str(new_alert), str(metric), str(context)))
                                        else:
                                            # @added 20210801 - Feature #4214: alert.paused
                                            if not alert_paused:
                                                trigger_alert(alert, metric, context)
                                                logger.info('trigger_alert :: alert: %s, metric: %s, context: %s' % (
                                                    str(alert), str(metric), str(context)))
                                    else:
                                        # @added 20210801 - Feature #4214: alert.paused
                                        if not alert_paused:
                                            smtp_trigger_alert(alert, metric, context)
                                            logger.info('smtp_trigger_alert :: alert: %s, metric: %s, context: %s' % (
                                                str(alert), str(metric), str(context)))
                                            if LOCAL_DEBUG:
                                                logger.debug('debug :: smtp_trigger_alert spawned')
                                    if LOCAL_DEBUG:
                                        logger.info(
                                            'debug :: Memory usage in run after triggering alert: %s (kb)' %
                                            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                                except:
                                    logger.error('error :: trigger_alert - %s' % traceback.format_exc())
                                    logger.error(
                                        'error :: failed to trigger_alert :: %s %s via %s' %
                                        (metric[1], metric[0], alert[1]))

                                # @added 20201014 - Feature #3734: waterfall alerts
                                # Remove ionosphere training data as all the
                                # resources required are not available for training
                                if waterfall_alert_check_string in alerting_waterfall_alerts:
                                    if second_order_resolution_seconds:
                                        wf_metric = metric[1]
                                        wf_metric_timeseries_dir = wf_metric.replace('.', '/')
                                        wf_metric_training_dir = '%s/%s/%s' % (
                                            settings.IONOSPHERE_DATA_FOLDER, str(int(metric[2])),
                                            str(wf_metric_timeseries_dir))
                                        data = [metric[1], int(metric[2]), second_order_resolution_seconds]
                                        remove_ionosphere_training_data_for.append([data, wf_metric_training_dir])
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed in check to alert step')

            logger.info('alerts checked')

            # @added 20201112 - Feature #3772: Add the anomaly_id to the http_alerter json
            # Allow time for the alerter to add the upper and lower 3sigma
            # bounds from the training data before removing
            remove_training_data_redis_set = 'analyzer.waterfall_alerts.remove_training_data'

            # @added 20201014 - Feature #3734: waterfall alerts
            # Remove ionosphere training data as all the
            # resources required are not available for training
            for metric_to_remove in remove_ionosphere_training_data_for:
                # @added 20201112 - Feature #3772: Add the anomaly_id to the http_alerter json
                # Allow time for the alerter to add the upper and lower 3sigma
                # bounds from the training data before removing
                try:
                    self.redis_conn.sadd(remove_training_data_redis_set, str(metric_to_remove))
                    logger.info('added item %s to Redis set - %s' % (
                        str(metric_to_remove), remove_training_data_redis_set))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to add %s to Redis set %s' % (str(metric_to_remove), remove_training_data_redis_set))
            try:
                training_data_to_remove = list(self.redis_conn_decoded.smembers(remove_training_data_redis_set))
                logger.info('%s items in Redis set %s' % (
                    str(len(training_data_to_remove)),
                    remove_training_data_redis_set))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get Redis set %s' % remove_training_data_redis_set)
            remove_ionosphere_training_data_for = []
            if training_data_to_remove:
                try:
                    for item in training_data_to_remove:
                        metric_to_remove = literal_eval(item)
                        remove_ionosphere_training_data_for.append(metric_to_remove)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to create remove_ionosphere_training_data_for list')
            for metric_to_remove in remove_ionosphere_training_data_for:
                remove_training_dir = False
                try:
                    data = metric_to_remove[0]
                    if int(time()) > (data[1] + 120):
                        remove_training_dir = True
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to determine whether to remove training data for - %s' % str(metric_to_remove))
                if remove_training_dir:
                    wf_metric_training_dir = metric_to_remove[1]

                    # @added 20201016 - Feature #3734: waterfall alerts
                    # Remove mirage and ionosphere check file if they exists as they
                    # have been alerted on
                    try:
                        sane_metricname = filesafe_metricname(str(data[0]))
                        check_file = '%s.%s.txt' % (str(data[1]), sane_metricname)
                        mirage_check_file = '%s/%s' % (settings.MIRAGE_CHECK_PATH, check_file)
                        if os.path.isfile(mirage_check_file):
                            try:
                                os.remove(mirage_check_file)
                                logger.info('removed mirage check file as waterfall alerted on - %s' % mirage_check_file)
                            except OSError:
                                logger.error('error - failed to remove %s, continuing' % mirage_check_file)
                        ionosphere_check_file = '%s/%s' % (settings.IONOSPHERE_CHECK_PATH, check_file)
                        if os.path.isfile(ionosphere_check_file):
                            try:
                                os.remove(ionosphere_check_file)
                                logger.info('removed ionosphere check file as waterfall alerted on - %s' % ionosphere_check_file)
                            except OSError:
                                logger.error('error - failed to remove %s, continuing' % ionosphere_check_file)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to determine if there are check files to remove for - %s' % (str(metric_to_remove)))

                    if os.path.exists(wf_metric_training_dir):
                        try:
                            rmtree(wf_metric_training_dir)
                            logger.info('removed ionosphere training dir for waterfall alert metric - %s' % wf_metric_training_dir)
                        except:
                            logger.error('error :: failed to rmtree for waterfall alert metric - %s' % wf_metric_training_dir)
                    try:
                        self.redis_conn.srem('ionosphere.training_data', str(data))
                        logger.info('removed ionosphere.training_data Redis set item as this is an analyzer waterfall alert and no training data is available - %s' % (
                            str(data)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to remove item from Redis ionosphere.training_data set - %s' % (str(data)))
                    try:
                        self.redis_conn.srem(remove_training_data_redis_set, str(metric_to_remove))
                        logger.info('removed item - %s from Redis set - %s' % (
                            str(metric_to_remove), remove_training_data_redis_set))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to remove %s to Redis set %s' % (str(metric_to_remove), remove_training_data_redis_set))

            # @added 20200611 - Feature #3578: Test alerts
            # @modified 20200625 - Feature #3578: Test alerts
            # Do not delete the set here as a test alert could be sent via
            # webapp between Analyzer starting its run and reading the set and
            # deleting here. Rather remove the entry in the test alert block
            # below
            # test_alerts_redis_set = '%s.test_alerts' % skyline_app
            # try:
            #     self.redis_conn.delete(test_alerts_redis_set)
            # except:
            #    pass
            test_alerts_data = []
            test_alerts_redis_set = '%s.test_alerts_data' % skyline_app
            try:
                test_alerts_data = list(self.redis_conn_decoded.smembers(test_alerts_redis_set))
                if test_alerts_data:
                    logger.info('determined %s test alerts to send from %s Redis set' % (
                        str(len(test_alerts_data)), test_alerts_redis_set))
                    try:
                        self.redis_conn.delete(test_alerts_redis_set)
                    except:
                        pass
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to generate a list from %s Redis set' % test_alerts_redis_set)
                test_alerts_data = []
            if test_alerts_data:
                for alert in all_alerts:
                    for metric_list_string in test_alerts_data:
                        metric = literal_eval(metric_list_string)
                        pattern_match = False

                        # @modified 20200622 - Task #3586: Change all alert pattern checks to matched_or_regexed_in_list
                        #                      Feature #3512: matched_or_regexed_in_list function
                        # Changed original alert matching pattern to use new
                        # method
                        base_name = str(metric[1])
                        try:
                            pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, [alert[0]])
                            if LOCAL_DEBUG and pattern_match:
                                logger.debug('debug :: %s matched alert - %s' % (base_name, alert[0]))
                            try:
                                del metric_matched_by
                            except:
                                pass
                        except:
                            pattern_match = False

                        if not pattern_match:
                            continue
                        try:
                            test_value = 'TEST - %s' % str(metric[0])
                            metric_test = (test_value, metric[1], metric[2])
                            if alert[1] != 'smtp':
                                if alert[1] == 'slack':
                                    trigger_alert(alert, metric_test, context)
                                    logger.info('trigger_alert :: test_alert - alert: %s, metric: %s, context: %s' % (
                                        str(alert), str(metric_test), str(context)))
                                else:
                                    trigger_alert(alert, metric, context)
                                    logger.info('trigger_alert :: test_alert - alert: %s, metric: %s, context: %s' % (
                                        str(alert), str(metric), str(context)))
                            else:
                                smtp_trigger_alert(alert, metric_test, context)
                                logger.info('smtp_trigger_alert :: tests_alert - alert: %s, metric: %s, context: %s' % (
                                    str(alert), str(metric_test), str(context)))
                        except:
                            logger.error('error :: test_alert - trigger_alert - %s' % traceback.format_exc())
                            logger.error(
                                'error :: test_alert - failed to trigger_alert :: %s %s via %s' %
                                (metric[1], metric[0], alert[1]))

                        # @added 20200625 - Feature #3578: Test alerts
                        # Remove the metric from the test alert Redis set
                        test_alerts_redis_set = '%s.test_alerts' % skyline_app
                        try:
                            self.redis_conn.srem(test_alerts_redis_set, base_name)
                        except:
                            pass

            # @modified 20160207 - Branch #922: Ionosphere
            # Handle if alerts are not enabled
            else:
                logger.info('no test alerts were found')

            if LOCAL_DEBUG:
                logger.debug('debug :: Memory usage in run after alerts: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

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
                logger.error(traceback.format_exc())
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
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to generate list from Redis set analyzer.real_anomalous_metrics')
                            real_anomalous_metrics.sort(key=operator.itemgetter(1))
                            fh.write('handle_data(%s)' % real_anomalous_metrics)
                    except:
                        logger.error(traceback.format_exc())
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
                logger.debug('debug :: Memory usage in run before algorithm test run times: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            algorithms_to_time = []
            # @modified 20201127 - Feature #3848: custom_algorithms - run_before_3sigma parameter
            # Only run DEBUG_CUSTOM_ALGORITHMS if there is a custom
            # algorithm to be run with analyzer
            # if LOCAL_DEBUG or DEBUG_CUSTOM_ALGORITHMS:
            if LOCAL_DEBUG or (DEBUG_CUSTOM_ALGORITHMS and run_custom_algorithm_enabled_on_app):
                logger.debug('debug :: algorithms_to_time reset to - %s' % (str(algorithms_to_time)))
            algorithms_to_time = list(settings.ALGORITHMS)
            # if LOCAL_DEBUG or DEBUG_CUSTOM_ALGORITHMS:
            if LOCAL_DEBUG or (DEBUG_CUSTOM_ALGORITHMS and run_custom_algorithm_enabled_on_app):
                logger.debug('debug :: algorithms_to_time from settings.ALGORITHMS - %s' % (str(algorithms_to_time)))
            if CUSTOM_ALGORITHMS:
                for custom_algorithm in settings.CUSTOM_ALGORITHMS:
                    algorithms_to_time.append(custom_algorithm)
                    # if LOCAL_DEBUG or DEBUG_CUSTOM_ALGORITHMS:
                    if LOCAL_DEBUG or (DEBUG_CUSTOM_ALGORITHMS and run_custom_algorithm_enabled_on_app):
                        logger.debug('debug :: algorithms_to_time added %s from settings.CUSTOM_ALGORITHMS - %s' % (str(custom_algorithm), str(algorithms_to_time)))

            # if LOCAL_DEBUG or DEBUG_CUSTOM_ALGORITHMS:
            if LOCAL_DEBUG or (DEBUG_CUSTOM_ALGORITHMS and run_custom_algorithm_enabled_on_app):
                logger.debug('debug :: algorithms_to_time - %s' % (str(algorithms_to_time)))
                logger.debug('debug :: processing algorithms_to_time - %s' % (str(algorithms_to_time)))

            for algorithm in algorithms_to_time:
                # if LOCAL_DEBUG or DEBUG_CUSTOM_ALGORITHMS:
                if LOCAL_DEBUG or (DEBUG_CUSTOM_ALGORITHMS and run_custom_algorithm_enabled_on_app):
                    logger.debug('debug :: processing algorithm count and timings - %s' % (str(algorithm)))

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
                logger.debug('debug :: Memory usage in run after algorithm run times: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

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
                except Exception as e:
                    alert_on_stale_metrics = []
                    logger.warn('warning :: alert_on_stale_metrics list could not be determined from analyzer.alert_on_stale_metrics - %s' % e)

                # @added 20210519 - Branch #1444: thunder
                #                   Feature #4076: CUSTOM_STALE_PERIOD
                #                   Feature #2492: alert on stale metrics
                if not alert_on_stale_metrics:
                    try:
                        alert_on_stale_metrics = list(self.redis_conn_decoded.smembers('aet.analyzer.alert_on_stale_metrics'))
                        logger.info('alert_on_stale_metrics determine from aet.analyzer.alert_on_stale_metrics :: %s' % str(alert_on_stale_metrics))
                    except Exception as e:
                        alert_on_stale_metrics = []
                        logger.warn('warning :: alert_on_stale_metrics list could not be determined from aet.analyzer.alert_on_stale_metrics - %s' % e)
                if alert_on_stale_metrics:
                    # Get all the known custom stale periods
                    custom_stale_metrics_dict = {}
                    try:
                        custom_stale_metrics_dict = self.redis_conn_decoded.hgetall(custom_stale_metrics_hash_key)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to create custom_stale_metrics_dict from Redis hash key %s - %s' % (
                            custom_stale_metrics_hash_key, e))
                    custom_stale_metrics = []
                    if custom_stale_metrics_dict:
                        try:
                            custom_stale_metrics = list(custom_stale_metrics_dict.keys())
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: metrics_manager :: failed to create list of known_custom_stale_metrics from known_custom_stale_metrics_dict - %s' % (
                                e))

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

                    # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
                    # base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    if metric_name.startswith(settings.FULL_NAMESPACE):
                        base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    else:
                        base_name = metric_name

                    cache_key = 'last_alert.stale.%s' % (base_name)
                    last_alert = False
                    try:
                        last_alert = self.redis_conn.get(cache_key)
                    except Exception as e:
                        logger.error('error :: could not query Redis for cache_key: %s' % e)
                    if not last_alert:
                        stale_metrics_to_alert_on.append(base_name)

                        # @added 20210519 - Feature #4076: CUSTOM_STALE_PERIOD
                        use_stale_period = int(settings.STALE_PERIOD)
                        if base_name in custom_stale_metrics:
                            try:
                                use_stale_period = int(custom_stale_metrics_dict[base_name])
                            except AttributeError:
                                use_stale_period = int(settings.STALE_PERIOD)
                            except TypeError:
                                use_stale_period = int(settings.STALE_PERIOD)
                            except Exception as e:
                                logger.error('error :: could not get custom stale period from custom_stale_metrics_dict for %s - %s' % (
                                    base_name, e))
                                use_stale_period = int(settings.STALE_PERIOD)

                        try:
                            # @modified 20180828 - Bug #2568: alert on stale metrics not firing
                            # Those expired in the year 2067
                            # self.redis_conn.setex(cache_key, int(time()), int(settings.STALE_PERIOD))
                            # @modified 20210519 - Feature #4076: CUSTOM_STALE_PERIOD
                            # self.redis_conn.setex(cache_key, int(settings.STALE_PERIOD), int(time()))
                            self.redis_conn.setex(cache_key, use_stale_period, int(time()))
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
                logger.debug('debug :: literal_real_anomalous_metrics - %s' % str(literal_real_anomalous_metrics))
                for metric_list_string in literal_real_anomalous_metrics:
                    metric = literal_eval(metric_list_string)
                    real_anomalous_metrics.append(metric)
            except:
                logger.error(traceback.format_exc())
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
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to generate a list from analyzer.mirage_periodic_check_metrics Redis set')
                    mirage_periodic_checks = '0'

            # @added 20210520 - Branch #1444: thunder
            # Added Redis hash keys for thunder to monitor the analyzer state and
            # notify on degradation of service and operational changes
            cache_key = 'analyzer.run_time'
            try:
                self.redis_conn.hset(cache_key, 'value', float(run_time))
                self.redis_conn.hset(cache_key, 'timestamp', int(time()))
            except Exception as e:
                logger.error('error :: Analyzer could not update the Redis %s key - %s' % (
                    cache_key, e))
            cache_key = 'analyzer.total_metrics'
            try:
                self.redis_conn.hset(cache_key, 'value', int(total_metrics))
                self.redis_conn.hset(cache_key, 'timestamp', int(time()))
            except Exception as e:
                logger.error('error :: Analyzer could not update the Redis %s key - %s' % (
                    cache_key, e))
            cache_key = 'analyzer.total_analyzed'
            try:
                self.redis_conn.hset(cache_key, 'value', int(total_analyzed))
                self.redis_conn.hset(cache_key, 'timestamp', int(time()))
            except Exception as e:
                logger.error('error :: Analyzer could not update the Redis %s key - %s' % (
                    cache_key, e))

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

            # @added 20200623 - Feature #3588: Metric http_alerter queues
            #                   Feature #3396: http_alerter
            # Send metrics of the http_alerter queue sizes so they can be
            # monitored via Boundary checks
            if HTTP_ALERTERS_ENABLED:
                resend_queues = ['analyzer', 'mirage', 'ionosphere']
                for source in resend_queues:
                    http_alerter_queue_size = '0'
                    try:
                        redis_set = '%s.http_alerter.queue' % source
                        http_alerter_queue_size = str(len(list(self.redis_conn_decoded.smembers(redis_set))))
                        if resend_queue:
                            logger.info('%s items in the %s Redis set' % (str(len(resend_queue)), redis_set))
                            resend_items = []
                    except Exception as e:
                        logger.error('error :: could not determine http_alerter_queue_size from Redis set %s - %s' % (redis_set, e))
                        http_alerter_queue_size = '0'
                    send_metric_name = '%s.http_alerter.queue.%s' % (skyline_app_graphite_namespace, source)
                    send_graphite_metric(skyline_app, send_metric_name, http_alerter_queue_size)

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
                logger.debug('debug :: Memory usage before reset counters: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # @added 20191107 - Feature #3306: Record anomaly_end_timestamp
            #                   Branch #3262: py3
            # Set the anomaly_end_timestamp for any metrics no longer anomalous
            not_anomalous_metrics = []
            not_anomalous_metrics_data = []
            # @modified 20200608 - Feature #3306: Record anomaly_end_timestamp
            # Declare outside try
            redis_set = 'current.anomalies'
            try:
                # redis_set = 'current.anomalies'
                current_anomalies = self.redis_conn_decoded.smembers(redis_set)
            except:
                logger.error(traceback.format_exc())
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
                        # @added 20200608 - Feature #3306: Record anomaly_end_timestamp
                        # Remove entries from the current.anomalies set if the
                        # timestamp is older than FULL_DURATION
                        if anomaly_timestamp < (now - settings.FULL_DURATION):
                            redis_set = 'current.anomalies'
                            try:
                                self.redis_conn.srem(redis_set, str(list_data))
                                logger.info('removed %s from Redis set %s as the anomaly_timestamp is older than FULL_DURATION - %s' % (
                                    metric, redis_set, str(list_data)))
                                continue
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to remove %s for Redis set %s' % (str(list_data), redis_set))

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
                                redis_set = 'current.anomalies'
                                try:
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
                    logger.error(traceback.format_exc())
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
                    logger.error(traceback.format_exc())
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

            # @added 20201110 - Feature #3830: metrics_manager
            if not ANALYZER_USE_METRICS_MANAGER:
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
                logger.debug('debug :: Memory usage after reset counters: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                logger.debug('debug :: Memory usage before sleep: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

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
                    logger.info('derivative metrics were managed - set analyzer.derivative_metrics_expiry Redis key')
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
                        logger.error('error :: could not get Redis set derivative_metrics: %s' % str(e))
                    else:
                        logger.info(
                            'there is no Redis set derivative_metrics, expected as ANALYZER_ENABLED is set to %s - %s' % (
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
                    except Exception as e2:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add default data to Redis new_derivative_metrics set - %s' % e2)
                        logger.error('error :: which followed after failed creating list from smembers(\'new_derivative_metrics\') - %s' % e)

                # @modified 20190517 - Branch #3002: docker
                # Wrapped in if derivative_metrics
                # @modified 20190922 - Branch #3002: docker
                # Handle no new_derivative_metrics
                # if derivative_metrics:
                if derivative_metrics and test_new_derivative_metrics:
                    try:
                        self.redis_conn.rename('new_derivative_metrics', 'derivative_metrics')
                        logger.info('derivative metrics were managed - new_derivative_metrics Redis set renamed to derivative_metrics')
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
                        logger.info('derivative metrics were managed - new_non_derivative_metrics Redis set renamed to non_derivative_metrics')
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
                    # @modified 20200606 - Bug #3572: Apply list to settings imports
                    non_derivative_monotonic_metrics = list(settings.NON_DERIVATIVE_MONOTONIC_METRICS)
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
                # @modified 20200727 - Feature #3650: ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
                #                      Feature #3480: batch_processing
                #                      Feature #3486: analyzer_batch
                # Do not remove analyzer.batch_processing_metrics, but rename it
                # to aet.analyzer.batch_processing_metrics for use in horizon/roomba.py
                # self.redis_conn.delete('analyzer.batch_processing_metrics')
                self.redis_conn.rename('analyzer.batch_processing_metrics', 'aet.analyzer.batch_processing_metrics')
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

            # @added 20201006 - Bug #3766: Refresh mirage.metrics_expiration_times for batch processing
            #                   Task #3662: Change mirage.last_check keys to timestamp value
            #                   Feature #3486: analyzer_batch
            #                   Feature #3480: batch_processing
            # Use a different set to refresh the set
            # @modified 20201110 - Feature #3830: metrics_manager
            # Now managed by metrics_manager
            if not ANALYZER_USE_METRICS_MANAGER:
                analyzer_mirage_metrics_expiration_times_set = None
                try:
                    analyzer_mirage_metrics_expiration_times_set = list(self.redis_conn_decoded.smembers('analyzer.mirage.metrics_expiration_times'))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to get Redis set - analyzer.mirage.metrics_expiration_times')
                if len(analyzer_mirage_metrics_expiration_times_set) > 0:
                    try:
                        self.redis_conn.rename('analyzer.mirage.metrics_expiration_times', 'mirage.metrics_expiration_times')
                        logger.info('renamed Redis set analyzer.mirage.metrics_expiration_times to mirage.metrics_expiration_times')
                    except:
                        # pass
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to renamed Redis set analyzer.mirage.metrics_expiration_times to mirage.metrics_expiration_times')
                    try:
                        self.redis_conn.sunionstore('aet.analyzer.mirage.metrics_expiration_times', 'mirage.metrics_expiration_times')
                        logger.info('copied Redis set mirage.metrics_expiration_times to aet.analyzer.mirage.metrics_expiration_times with %s items' % str(len(analyzer_mirage_metrics_expiration_times_set)))
                    except:
                        # pass
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to copy Redis set analyzer.mirage.metrics_expiration_times to aet.analyzer.mirage.metrics_expiration_times')

                else:
                    logger.info('Redis set analyzer.mirage.metrics_expiration_times has no entries, nothing to update')

            # @added 20201030 - Feature #3808: ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS
            if ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS and ANALYZER_ANALYZE_LOW_PRIORITY_METRICS:
                last_dynamic_low_priority_set_count = 0
                try:
                    last_dynamic_low_priority_set_count = self.redis_conn_decoded.scard('analyzer.low_priority_metrics.dynamically_analyzed')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to get Redis set - analyzer.low_priority_metrics.last_dynamically_analyzed')
                    last_dynamic_low_priority_set_count = 0
                try:
                    self.redis_conn.rename('analyzer.low_priority_metrics.dynamically_analyzed', 'aet.analyzer.low_priority_metrics.dynamically_analyzed')
                    logger.info('renamed Redis set analyzer.low_priority_metrics.last_dynamically_analyzed to aet.analyzer.low_priority_metrics.dynamically_analyzed')
                # @modified 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
                #                      Feature #3808: ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS
                # Handle key not existing as this is idempotent
                # except:
                #     logger.error(traceback.format_exc())
                #     logger.error('error :: failed to rename Redis set - analyzer.low_priority_metrics.last_dynamically_analyzed to aet.analyzer.low_priority_metrics.last_dynamically_analyzed')
                except Exception as e:
                    traceback_str = traceback.format_exc()
                    if 'no such key' in str(e):
                        logger.warn('warning :: failed to rename Redis set - analyzer.low_priority_metrics.last_dynamically_analyzed as the key does not exist')
                    else:
                        logger.error(traceback_str)
                        logger.error('error :: failed to rename Redis set - analyzer.low_priority_metrics.last_dynamically_analyzed to aet.analyzer.low_priority_metrics.last_dynamically_analyzed')
                low_priority_metric_count = 0
                low_priority_metric_count_list = []
                try:
                    low_priority_metric_count_list = list(self.redis_conn_decoded.smembers('analyzer.low_priority_metrics.count'))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to generate list from Redis set - analyzer.low_priority_metrics.count')
                    low_priority_metric_count_list = []
                if low_priority_metric_count_list:
                    low_priority_metric_counts = []
                    for item in low_priority_metric_count_list:
                        try:
                            low_priority_metric_counts.append(int(item))
                        except:
                            pass
                    low_priority_metric_count = sum(low_priority_metric_counts)
                    try:
                        self.redis_conn.delete('analyzer.low_priority_metrics.count')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to delete Redis set - analyzer.low_priority_metrics.count')
                if last_dynamic_low_priority_set_count and low_priority_metric_count:
                    logger.info('ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS processed %s of the total %s low priority metrics' % (
                        str(low_priority_metric_count),
                        str(last_dynamic_low_priority_set_count)))
                    logger.info('low_priority_metrics.dynamically_analyzed :: %s' % str(low_priority_metric_count))
                    send_metric_name = '%s.low_priority_metrics.dynamically_analyzed' % skyline_app_graphite_namespace
                    send_graphite_metric(skyline_app, send_metric_name, str(low_priority_metric_count))
                    logger.info('low_priority_metrics.total :: %s' % str(last_dynamic_low_priority_set_count))
                    send_metric_name = '%s.low_priority_metrics.total' % skyline_app_graphite_namespace
                    send_graphite_metric(skyline_app, send_metric_name, str(last_dynamic_low_priority_set_count))
                # Keep a record of missing low priority metrics for debug purposes
                # these tend to be sparsely populated metrics
                missing_unknown_low_priority_metrics_set_count = 0
                try:
                    missing_unknown_low_priority_metrics_set_count = self.redis_conn.scard('analyzer.missing_low_priority_metrics')
                except:
                    pass
                if missing_unknown_low_priority_metrics_set_count:
                    try:
                        self.redis_conn.delete('aet.analyzer.missing_low_priority_metrics')
                    except:
                        pass
                    try:
                        self.redis_conn.rename('analyzer.missing_low_priority_metrics', 'aet.analyzer.missing_low_priority_metrics')
                    except:
                        pass

            # @added 20201209 - Feature #3870: metrics_manager - check_data_sparsity
            # Data sparsity is checked by metrics_manager however due to the
            # fact that metrics_manager only runs every RUN_EVERY seconds, it
            # will not submit metrics_sparsity metrics to Graphite every minute.
            # metrics_sparsity metrics can be important if you want to ensure
            # that data is being received constantly as expected.  Therefore
            # Skyline creates metrics that ensure that data sparsity in the
            # metrics can be monitored.  A positive level shift in the
            # skyline.analyzer.<HOSTNAME>.metrics_sparsity.metrics_sparsity_increasing
            # metric signifies that the incoming metrics are no longer fully
            # populated.  Therefore analyzer sends the last metric_manager
            # values if metrics_manager did not send them so they themselves are
            # not sparsely populated
            if CHECK_DATA_SPARSITY:
                # If the Redis key exists do not submit metrics_manager metrics
                # to Graphite
                metrics_manager_sent_to_graphite = 0
                try:
                    metrics_manager_sent_to_graphite_data = self.redis_conn_decoded.get('analyzer.metrics_manager.sent.metrics_sparsity.metrics')
                    if metrics_manager_sent_to_graphite_data:
                        metrics_manager_sent_to_graphite = int(metrics_manager_sent_to_graphite_data)
                        sent_seconds_ago = time() - metrics_manager_sent_to_graphite
                        logger.info('not sending Graphite metrics for metrics_manager, metrics_manager sent %.2f seconds ago' % sent_seconds_ago)
                except Exception as e:
                    logger.error('error :: metrics_manager :: could not get Redis key analyzer.metrics_manager.sent.metrics_sparsity.metrics: %s' % e)
                # If metrics_manager did not send metrics to Graphite, send the
                # last values sent
                if not metrics_manager_sent_to_graphite:
                    metrics_sparsity_use_namespace = skyline_app_graphite_namespace + '.metrics_sparsity'

                    send_metric_name = metrics_sparsity_use_namespace + '.metrics_fully_populated'
                    last_metrics_fully_populated = None
                    try:
                        last_metrics_fully_populated_data = self.redis_conn_decoded.get('analyzer.metrics_manager.metrics_sparsity.metrics_fully_populated')
                        if last_metrics_fully_populated_data:
                            last_metrics_fully_populated = float(last_metrics_fully_populated_data)
                    except Exception as e:
                        logger.error('error :: metrics_manager :: failed to determine metrics_fully_populated value from Redis key analyzer.metrics_manager.metrics_sparsity.metrics_fully_populated: %s' % e)
                    if last_metrics_fully_populated or last_metrics_fully_populated == 0:
                        try:
                            send_graphite_metric(skyline_app, send_metric_name, str(last_metrics_fully_populated))
                            logger.info('sent Graphite metric for metrics_manager - %s %s' % (send_metric_name, str(last_metrics_fully_populated)))
                        except Exception as e:
                            logger.error('error :: metrics_manager :: could not send send_graphite_metric %s %s: %s' % (
                                send_metric_name, str(last_metrics_fully_populated), e))

                    send_metric_name = metrics_sparsity_use_namespace + '.metrics_sparsity_increasing'
                    last_metrics_sparsity_increasing = None
                    try:
                        last_metrics_sparsity_increasing_data = self.redis_conn_decoded.get('analyzer.metrics_manager.metrics_sparsity.metrics_sparsity_increasing')
                        if last_metrics_sparsity_increasing_data:
                            last_metrics_sparsity_increasing = float(last_metrics_sparsity_increasing_data)
                    except Exception as e:
                        logger.error('error :: metrics_manager :: failed to determine metrics_sparsity_increasing value from Redis key analyzer.metrics_manager.metrics_sparsity.metrics_sparsity_increasing: %s' % e)
                    if last_metrics_sparsity_increasing or last_metrics_sparsity_increasing == 0:
                        try:
                            send_graphite_metric(skyline_app, send_metric_name, str(last_metrics_sparsity_increasing))
                            logger.info('sent Graphite metric for metrics_manager - %s %s' % (send_metric_name, str(last_metrics_sparsity_increasing)))
                        except Exception as e:
                            logger.error('error :: metrics_manager :: could not send send_graphite_metric %s %s: %s' % (
                                send_metric_name, str(last_metrics_sparsity_increasing), e))

                    send_metric_name = metrics_sparsity_use_namespace + '.metrics_sparsity_decreasing'
                    last_metrics_sparsity_decreasing = None
                    try:
                        last_metrics_sparsity_decreasing_data = self.redis_conn_decoded.get('analyzer.metrics_manager.metrics_sparsity.metrics_sparsity_decreasing')
                        if last_metrics_sparsity_decreasing_data:
                            last_metrics_sparsity_decreasing = float(last_metrics_sparsity_decreasing_data)
                    except Exception as e:
                        logger.error('error :: metrics_manager :: failed to determine metrics_sparsity_decreasing value from Redis key analyzer.metrics_manager.metrics_sparsity.metrics_sparsity_decreasing: %s' % e)
                    if last_metrics_sparsity_decreasing or last_metrics_sparsity_decreasing == 0:
                        try:
                            send_graphite_metric(skyline_app, send_metric_name, str(last_metrics_sparsity_decreasing))
                            logger.info('sent Graphite metric for metrics_manager - %s %s' % (send_metric_name, str(last_metrics_sparsity_decreasing)))
                        except Exception as e:
                            logger.error('error :: metrics_manager :: could not send send_graphite_metric %s %s: %s' % (
                                send_metric_name, str(last_metrics_sparsity_decreasing), e))

                    send_metric_name = metrics_sparsity_use_namespace + '.avg_sparsity'
                    last_avg_sparsity = None
                    try:
                        last_avg_sparsity_data = self.redis_conn_decoded.get('analyzer.metrics_manager.metrics_sparsity.avg_sparsity')
                        if last_avg_sparsity_data:
                            last_avg_sparsity = float(last_avg_sparsity_data)
                    except Exception as e:
                        logger.error('error :: metrics_manager :: failed to determine last_avg_sparsity from Redis key analyzer.metrics_manager.metrics_sparsity.avg_sparsity: %s' % e)
                    if last_avg_sparsity or last_avg_sparsity == 0:
                        try:
                            send_graphite_metric(skyline_app, send_metric_name, str(last_avg_sparsity))
                            logger.info('sent Graphite metric for metrics_manager - %s %s' % (send_metric_name, str(last_avg_sparsity)))
                        except Exception as e:
                            logger.error('error :: metrics_manager :: could not send send_graphite_metric %s %s: %s' % (
                                send_metric_name, str(last_avg_sparsity), e))

                    # @added 20210619 - Feature #4148: analyzer.metrics_manager.resolutions
                    send_metric_name = skyline_app_graphite_namespace + '.metrics_manager_run_time'
                    metrics_manager_run_time = None
                    try:
                        metrics_manager_run_time_data = self.redis_conn_decoded.get('analyzer.metrics_manager.run_time')
                        if metrics_manager_run_time_data:
                            metrics_manager_run_time = float(metrics_manager_run_time_data)
                    except Exception as e:
                        logger.error('error :: failed to determine run_time from Redis key analyzer.metrics_manager.run_time: %s' % e)
                    if metrics_manager_run_time or metrics_manager_run_time == 0:
                        try:
                            send_graphite_metric(skyline_app, send_metric_name, str(metrics_manager_run_time))
                            logger.info('sent Graphite metric for metrics_manager - %s %s' % (send_metric_name, str(metrics_manager_run_time)))
                        except Exception as e:
                            logger.error('error :: metrics_manager :: could not send send_graphite_metric %s %s: %s' % (
                                send_metric_name, str(metrics_manager_run_time), e))

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
