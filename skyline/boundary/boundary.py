"""
boundary.py
"""
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
from os import path, kill, getpid
from math import ceil
import traceback
import operator
import re
import os
import errno
import sys
# import os.path
# @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
# literal_eval required to evaluate Redis sets
from ast import literal_eval

from msgpack import Unpacker, packb

import settings
# @modified 20171216 - Task #2236: Change Boundary to only send to Panorama on alert
# Added move_file
from skyline_functions import (
    # @added 20220726 - Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    # Moved send_graphite_metric
    # send_graphite_metric, write_data_to_file, move_file,
    write_data_to_file, move_file,
    # @added 20181126 - Task #2742: Update Boundary
    #                   Feature #2034: analyse_derivatives
    nonNegativeDerivative, in_list,
    # @added 20191025 - Bug #3266: py3 Redis binary objects not strings
    #                   Branch #3262: py3
    # Added a single functions to deal with Redis connection and the
    # charset='utf-8', decode_responses=True arguments required in py3
    get_redis_conn, get_redis_conn_decoded,
    # @added 20200506 - Feature #3532: Sort all time series
    sort_timeseries,
    # @added 20220407 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
    #                   Feature #4520: settings - ZERO_FILL_NAMESPACES
    mkdir_p,
)

from boundary_alerters import trigger_alert
from boundary_algorithms import run_selected_algorithm
from algorithm_exceptions import (TooShort, Stale, Boring)

# @added 20200622 - Task #3586: Change all alert pattern checks to matched_or_regexed_in_list
#                   Feature #3512: matched_or_regexed_in_list function
# Changed original alert matching pattern to use new
# method
from matched_or_regexed_in_list import matched_or_regexed_in_list

# @added 20220406 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
#                   Feature #4520: settings - ZERO_FILL_NAMESPACES
from functions.metrics.last_known_value_metrics_list import last_known_value_metrics_list
from functions.metrics.zero_fill_metrics_list import zero_fill_metrics_list
# @added 20220407 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
#                   Feature #4520: settings - ZERO_FILL_NAMESPACES
from functions.timeseries.full_duration_timeseries_fill import full_duration_timeseries_fill

# @added 20220504 - Feature #2580: illuminance
from functions.illuminance.add_illuminance_entries import add_illuminance_entries

# @added 20220726 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.graphite.send_graphite_metric import send_graphite_metric

# @added 20230220 - Feature #4854: boundary - labelled_metrics
#                   Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.timeseries.strictly_increasing_monotonicity import strictly_increasing_monotonicity
from functions.prometheus.metric_name_labels_parser import metric_name_labels_parser

skyline_app = 'boundary'
skyline_app_logger = skyline_app + 'Log'
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

python_version = int(sys.version_info[0])
this_host = str(os.uname()[1])

try:
    SERVER_METRIC_PATH = '.' + settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

skyline_app_graphite_namespace = 'skyline.' + skyline_app + SERVER_METRIC_PATH

REDIS_SOCKET = settings.REDIS_SOCKET_PATH
BOUNDARY_METRICS = settings.BOUNDARY_METRICS
FULL_NAMESPACE = settings.FULL_NAMESPACE
ENABLE_BOUNDARY_DEBUG = settings.ENABLE_BOUNDARY_DEBUG
try:
    BOUNDARY_AUTOAGGRERATION = settings.BOUNDARY_AUTOAGGRERATION
except:
    BOUNDARY_AUTOAGGRERATION = False
try:
    BOUNDARY_AUTOAGGRERATION_METRICS = settings.BOUNDARY_AUTOAGGRERATION_METRICS
except:
    BOUNDARY_AUTOAGGRERATION_METRICS = (
        ("auotaggeration_metrics_not_declared", 60)
    )
# @added 20191107 - Branch #3262: py3
alert_test_file = '%s/%s_alert_test.txt' % (settings.SKYLINE_TMP_DIR, skyline_app)


class Boundary(Thread):
    """
    Boundary Thread
    """
    def __init__(self, parent_pid):
        """
        Initialize the Boundary
        """
        super(Boundary, self).__init__()
        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
#        if settings.REDIS_PASSWORD:
#            self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
#        else:
#            self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)

        self.redis_conn = get_redis_conn(skyline_app)

        self.daemon = True

        # @added 20191022 - Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
#        if settings.REDIS_PASSWORD:
#            self.redis_conn_decoded = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
#        else:
#            self.redis_conn_decoded = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)

        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

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
        # self.boundary_metrics = Manager().list()
        # self.anomalous_metrics = Manager().list()
        self.exceptions_q = Queue()
        self.anomaly_breakdown_q = Queue()
        # @added 20171214 - Bug #2232: Expiry boundary last_seen keys appropriately
        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
        # self.not_anomalous_metrics = Manager().list()

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
            logger.warning('warning :: parent or current process dead')
            sys.exit(0)

    def unique_noHash(self, seq):
        """
        Unique no hashed things
        """
        seen = set()
        return [x for x in seq if str(x) not in seen and not seen.add(str(x))]

    # This is to make a dump directory in /tmp if ENABLE_BOUNDARY_DEBUG is True
    # for dumping the metric timeseries data into for debugging purposes
    # @modified 20220407 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
    #                      Feature #4520: settings - ZERO_FILL_NAMESPACES
    # Deprecated use skyline_functions mkdir_p
    # def mkdir_p(self, dir_path):
    #     try:
    #         os.makedirs(dir_path)
    #         return True
    #     except OSError as exc:
    #         # Python >2.5
    #         if exc.errno == errno.EEXIST and os.path.isdir(dir_path):
    #             pass
    #         else:
    #             raise

    # @modified 20171216 - Task #2236: Change Boundary to only send to Panorama on alert
    # Pass added_at as an argument to spin_process so that the panaroma_anomaly_file
    # can be moved from SKYLINE_TMP_DIR to the PANORAMA_CHECK_PATH
    # def spin_process(self, i, boundary_metrics):
    def spin_process(self, i, boundary_metrics, added_at):
        """
        Assign a bunch of metrics for a process to analyze.
        """
        # Determine assigned metrics
        bp = settings.BOUNDARY_PROCESSES
        bm_range = len(boundary_metrics)
        keys_per_processor = int(ceil(float(bm_range) / float(bp)))
        if i == settings.BOUNDARY_PROCESSES:
            assigned_max = len(boundary_metrics)
        else:
            # This is a skyine bug, the original skyline code uses 1 as the
            # beginning position of the index, python indices begin with 0
            # assigned_max = len(boundary_metrics)
            # This closes the etsy/skyline pull request opened by @languitar on 17 Jun 2014
            # https://github.com/etsy/skyline/pull/94 Fix analyzer worker metric assignment
            assigned_max = min(len(boundary_metrics), i * keys_per_processor)
        assigned_min = (i - 1) * keys_per_processor
        assigned_keys = range(assigned_min, assigned_max)

        # Compile assigned metrics
        assigned_metrics_and_algos = [boundary_metrics[index] for index in assigned_keys]
        if ENABLE_BOUNDARY_DEBUG:
            logger.debug('debug :: printing assigned_metrics_and_algos')
            for assigned_metric_and_algo in assigned_metrics_and_algos:
                logger.debug('debug :: assigned_metric_and_algo - %s' % str(assigned_metric_and_algo))

        # Compile assigned metrics
        assigned_metrics = []
#        for i in assigned_metrics_and_algos:
#            assigned_metrics.append(i[0])
        for iaa in assigned_metrics_and_algos:
            assigned_metrics.append(iaa[0])

        # unique unhashed things
        def unique_noHash(seq):
            seen = set()
            return [x for x in seq if str(x) not in seen and not seen.add(str(x))]

        unique_assigned_metrics = unique_noHash(assigned_metrics)

        if ENABLE_BOUNDARY_DEBUG:
            logger.debug('debug :: unique_assigned_metrics - %s' % str(unique_assigned_metrics))
            logger.debug('debug :: printing unique_assigned_metrics:')
            for unique_assigned_metric in unique_assigned_metrics:
                logger.debug('debug :: unique_assigned_metric - %s' % str(unique_assigned_metric))

        logger.info('assigned unique_assigned_metrics: %s' % str(len(unique_assigned_metrics)))

        # Check if this process is unnecessary
        if len(unique_assigned_metrics) == 0:
            return

        # @added 20230222 - Feature #4854: boundary - labelled_metrics
        #                   Task #2732: Prometheus to Skyline
        #                   Branch #4300: prometheus
        unique_assigned_redis_metrics = []
        unique_assigned_redistimeseries_metrics = []
        for metric in unique_assigned_metrics:
            if '_tenant_id="' in metric:
                unique_assigned_redistimeseries_metrics.append(metric)
            else:
                unique_assigned_redis_metrics.append(metric)
        unique_assigned_redis_metrics_indices = {}
        for index, metric in enumerate(unique_assigned_redis_metrics):
            unique_assigned_redis_metrics_indices[metric] = index

        # Multi get series
        try:
            # @modified 20230222 - Feature #4854: boundary - labelled_metrics
            # raw_assigned = self.redis_conn.mget(unique_assigned_metrics)
            raw_assigned = self.redis_conn.mget(unique_assigned_redis_metrics)
        except:
            logger.error('error :: failed to mget assigned_metrics from redis')
            return

        logger.info('got %s raw_assigned from Redis mget' % str(len(raw_assigned)))

        # Make process-specific dicts
        exceptions = defaultdict(int)
        anomaly_breakdown = defaultdict(int)

        # Reset boundary_algortims
        all_boundary_algorithms = []
        for metric in BOUNDARY_METRICS:
            all_boundary_algorithms.append(metric[1])

        # The unique algorithms that are being used
        boundary_algorithms = unique_noHash(all_boundary_algorithms)
        if ENABLE_BOUNDARY_DEBUG:
            logger.debug('debug :: boundary_algorithms - %s' % str(boundary_algorithms))

        # @added 20220406 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
        #                   Feature #4520: settings - ZERO_FILL_NAMESPACES
        last_known_value_metrics = []
        try:
            last_known_value_metrics = last_known_value_metrics_list(skyline_app)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: last_known_value_metrics_list failed - %s' % err)
        zero_fill_metrics = []
        try:
            zero_fill_metrics = zero_fill_metrics_list(skyline_app)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: zero_fill_metrics_list failed - %s' % err)

        # @added 20220420 - Feature #4530: namespace.analysed_events
        analysed_metrics = []

        # @added 20220504 - Feature #2580: illuminance
        illuminance_dict = {}

        discover_run_metrics = []

        # @added 20230222 - Feature #4854: boundary - labelled_metrics
        #                   Task #2732: Prometheus to Skyline
        #                   Branch #4300: prometheus
        analysis_stats = {
            'unique_metrics_with_timeseries': 0,
            'unique_metrics_with_no_timeseries': 0,
            'labelled_metrics_with_timeseries': 0,
            'labelled_metrics_with_no_timeseries': 0,
            'not_anomalous': 0,
            'anomalous': 0,
        }
        boundary_errors = {}

        # Distill metrics into a run list
        for index, metric_name, in enumerate(unique_assigned_metrics):

            # @modified 20230222 - Feature #4854: boundary - labelled_metrics
            #                      Task #2732: Prometheus to Skyline
            #                      Branch #4300: prometheus
            # Only replace if the metric starts with FULL_NAMESPACE because a
            # labelled_metric could have the FULL_NAMESPACE string in it
            # base_name = metric_name.replace(FULL_NAMESPACE, '', 1)
            if metric_name.startswith(FULL_NAMESPACE):
                base_name = metric_name.replace(FULL_NAMESPACE, '', 1)
            else:
                base_name = str(metric_name)

            # Determine the metrics BOUNDARY_METRICS metric tuple settings
            # @modified 20200622 - Task #3586: Change all alert pattern checks to matched_or_regexed_in_list
            #                      Feature #3512: matched_or_regexed_in_list function
            # Changed original alert matching pattern to use new
            # method
            # for metrick in BOUNDARY_METRICS:
            #    CHECK_MATCH_PATTERN = metrick[0]
            # Determine the metrics BOUNDARY_METRICS metric tuple settings
            for boundary_alerter in BOUNDARY_METRICS:
                # CHECK_MATCH_PATTERN = boundary_alerter[0]
                try:
                    pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, [boundary_alerter[0]])
                    if ENABLE_BOUNDARY_DEBUG and pattern_match:
                        logger.debug('debug :: %s matched alert - %s' % (base_name, boundary_alerter[0]))
                    try:
                        del metric_matched_by
                    except:
                        pass
                except:
                    pattern_match = False
                # check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
                # pattern_match = check_match_pattern.match(base_name)
                metric_pattern_matched = False
                if pattern_match:
                    metric_pattern_matched = True
                    algo_pattern_matched = False
                    for algo in boundary_algorithms:
                        for metric in BOUNDARY_METRICS:
                            # CHECK_MATCH_PATTERN = metric[0]
                            try:
                                pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, [metric[0]])
                                if ENABLE_BOUNDARY_DEBUG and pattern_match:
                                    logger.debug('debug :: %s and %s matched alert - %s' % (base_name, algo, metric[0]))
                                try:
                                    del metric_matched_by
                                except:
                                    pass
                            except:
                                pattern_match = False
                            # check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
                            # pattern_match = check_match_pattern.match(base_name)
                            if pattern_match:
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info("debug :: metric and algo pattern MATCHED - " + metric[0] + " | " + base_name + " | " + str(metric[1]))
                                metric_expiration_time = False
                                metric_min_average = False
                                metric_min_average_seconds = False
                                metric_trigger = False
                                algorithm = False
                                algo_pattern_matched = True
                                # algorithm = metric[1]
                                algorithm = str(boundary_alerter[1])
                                try:
                                    if metric[2]:
                                        metric_expiration_time = metric[2]
                                except:
                                    metric_expiration_time = False
                                try:
                                    if metric[3]:
                                        metric_min_average = metric[3]
                                except:
                                    metric_min_average = False
                                try:
                                    if metric[4]:
                                        metric_min_average_seconds = metric[4]
                                except:
                                    metric_min_average_seconds = 1200
                                try:
                                    if metric[5]:
                                        metric_trigger = metric[5]
                                    # @modified 20210708 - Bug #4166: Allow boundary to send same image to multiple slack channels
                                    # If the trigger is 0 set it as 0 rather than False
                                    if metric[5] == 0:
                                        metric_trigger = metric[5]
                                except:
                                    metric_trigger = False
                                try:
                                    if metric[6]:
                                        alert_threshold = metric[6]
                                except:
                                    alert_threshold = False
                                try:
                                    if metric[7]:
                                        metric_alerters = metric[7]
                                except:
                                    metric_alerters = False
                            if metric_pattern_matched and algo_pattern_matched:
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.debug('debug :: added metric - %s, %s, %s, %s, %s, %s, %s, %s, %s' % (str(i), metric_name, str(metric_expiration_time), str(metric_min_average), str(metric_min_average_seconds), str(metric_trigger), str(alert_threshold), metric_alerters, algorithm))
                                # discover_run_metrics.append([i, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger, alert_threshold, metric_alerters, algorithm])
                                discover_run_metrics.append([index, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger, alert_threshold, metric_alerters, algorithm])

        if ENABLE_BOUNDARY_DEBUG:
            logger.debug('debug :: printing discover_run_metrics')
            for discover_run_metric in discover_run_metrics:
                logger.debug('debug :: discover_run_metrics - %s' % str(discover_run_metric))
            logger.debug('debug :: build unique boundary metrics to analyze')

        # Determine the unique set of metrics to run
        run_metrics = unique_noHash(discover_run_metrics)

        if ENABLE_BOUNDARY_DEBUG:
            logger.debug('debug :: printing run_metrics')
            for run_metric in run_metrics:
                logger.debug('debug :: run_metrics - %s' % str(run_metric))

        # Distill timeseries strings and submit to run_selected_algorithm
        for metric_and_algo in run_metrics:
            self.check_if_parent_is_alive()

            # @added 20230220 - Feature #4854: boundary - labelled_metrics
            #                   Task #2732: Prometheus to Skyline
            #                   Branch #4300: prometheus
            labelled_metric = False
            datapoint = None

            try:
                raw_assigned_id = metric_and_algo[0]
                metric_name = metric_and_algo[1]
                base_name = metric_name.replace(FULL_NAMESPACE, '', 1)
                metric_expiration_time = metric_and_algo[2]
                metric_min_average = metric_and_algo[3]
                metric_min_average_seconds = metric_and_algo[4]
                metric_trigger = metric_and_algo[5]
                alert_threshold = metric_and_algo[6]
                metric_alerters = metric_and_algo[7]
                algorithm = str(metric_and_algo[8])

                if ENABLE_BOUNDARY_DEBUG:
                    logger.debug('debug :: unpacking timeseries for %s - %s' % (metric_name, str(raw_assigned_id)))

                # @added 20230220 - Feature #4854: boundary - labelled_metrics
                #                   Task #2732: Prometheus to Skyline
                #                   Branch #4300: prometheus
                use_base_name = base_name
                if '_tenant_id="' in metric_name:
                    try:
                        metric_id = get_metric_id_from_base_name(skyline_app, base_name)
                    except Exception as err:
                        logger.error('error :: get_metric_id_from_base_name failed for %s - %s' % (
                            str(base_name), err))
                        metric_id = 0
                    redis_ts_key = 'labelled_metrics.%s' % str(metric_id)
                    labelled_metric = str(redis_ts_key)
                    use_base_name = str(metric_id)

                # @modified 20230220 - Feature #4854: boundary - labelled_metrics
                #                      Task #2732: Prometheus to Skyline
                #                      Branch #4300: prometheus
                # Change to trying to get data from RedisTimeseries if data is
                # not present in the raw_assigned data
                # if raw_assigned[raw_assigned_id]:
                if not labelled_metric:

                    raw_assigned_index = unique_assigned_redis_metrics_indices[metric_name]

                    try:
                        # raw_series = raw_assigned[metric_and_algo[0]]
                        raw_series = raw_assigned[raw_assigned_index]
                        unpacker = Unpacker(use_list=False)
                        unpacker.feed(raw_series)
                        timeseries = list(unpacker)
                    except Exception as err:
                        boundary_errors[metric_name] = {'err': str(str), 'traceback': str(traceback.format_exc())}
                        timeseries = []

                    # @added 20230222 - Feature #4854: boundary - labelled_metrics
                    #                   Task #2732: Prometheus to Skyline
                    #                   Branch #4300: prometheus
                    if timeseries:
                        analysis_stats['unique_metrics_with_timeseries'] += 1
                    else:
                        analysis_stats['unique_metrics_with_no_timeseries'] += 1

                else:
                    # @added 20230220 - Feature #4854: boundary - labelled_metrics
                    #                   Task #2732: Prometheus to Skyline
                    #                   Branch #4300: prometheus
                    until_timestamp = int(time())
                    from_timestamp = until_timestamp - settings.FULL_DURATION
                    full_duration_timeseries = []
                    try:
                        full_duration_timeseries = self.redis_conn_decoded.ts().range(redis_ts_key, (from_timestamp * 1000), (until_timestamp * 1000))
                    except Exception as err:
                        if str(err) == 'TSDB: the key does not exist':
                            full_duration_timeseries = []
                        else:
                            logger.error('error :: failed to get Redis timeseries for %s - %s' % (
                                str(base_name), err))
                            full_duration_timeseries = []
                    if not full_duration_timeseries:
                        timeseries = []
                    else:
                        timeseries = [[int(mts / 1000), value] for mts, value in full_duration_timeseries]
                    # @added 20230222 - Feature #4854: boundary - labelled_metrics
                    #                   Task #2732: Prometheus to Skyline
                    #                   Branch #4300: prometheus
                    if timeseries:
                        analysis_stats['labelled_metrics_with_timeseries'] += 1
                    else:
                        analysis_stats['labelled_metrics_with_no_timeseries'] += 1

                # @added 20200507 - Feature #3532: Sort all time series
                # To ensure that there are no unordered timestamps in the time
                # series which are artefacts of the collector or carbon-relay, sort
                # all time series by timestamp before analysis.
                original_timeseries = timeseries
                if original_timeseries:
                    timeseries = sort_timeseries(original_timeseries)
                    del original_timeseries

                if ENABLE_BOUNDARY_DEBUG:
                    logger.debug('debug :: unpacked OK - %s - %s' % (metric_name, str(raw_assigned_id)))

                autoaggregate = False
                autoaggregate_value = 0

                # Determine if the namespace is to be aggregated
                if BOUNDARY_AUTOAGGRERATION:
                    for autoaggregate_metric in BOUNDARY_AUTOAGGRERATION_METRICS:
                        autoaggregate = False
                        autoaggregate_value = 0
                        # CHECK_MATCH_PATTERN = autoaggregate_metric[0]
                        base_name = metric_name.replace(FULL_NAMESPACE, '', 1)

                        # @modified 20200622 - Task #3586: Change all alert pattern checks to matched_or_regexed_in_list
                        #                      Feature #3512: matched_or_regexed_in_list function
                        # Changed original alert matching pattern to use new
                        # method
                        try:
                            pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, [autoaggregate_metric[0]])
                            if ENABLE_BOUNDARY_DEBUG and pattern_match:
                                logger.debug('debug :: %s matched alert - %s' % (base_name, autoaggregate_metric[0]))
                            try:
                                del metric_matched_by
                            except:
                                pass
                        except:
                            pattern_match = False
                        # check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
                        # pattern_match = check_match_pattern.match(base_name)

                        if pattern_match:
                            autoaggregate = True
                            autoaggregate_value = autoaggregate_metric[1]

                if ENABLE_BOUNDARY_DEBUG:
                    logger.debug('debug :: BOUNDARY_AUTOAGGRERATION passed - %s - %s' % (metric_name, str(autoaggregate)))

                if ENABLE_BOUNDARY_DEBUG:
                    logger.info(
                        'debug :: analysing - %s, %s, %s, %s, %s, %s, %s, %s, %s, %s' % (
                            metric_name, str(metric_expiration_time),
                            str(metric_min_average),
                            str(metric_min_average_seconds),
                            str(metric_trigger), str(alert_threshold),
                            metric_alerters, autoaggregate,
                            autoaggregate_value, algorithm)
                    )
                    # Dump the the timeseries data to a file
                    # @modified 20170913 - Task #2160: Test skyline with bandit
                    # Added nosec to exclude from bandit tests
                    # @modified 20230109 - Task #4778: v4.0.0 - update dependencies
                    # timeseries_dump_dir = "/tmp/skyline/boundary/" + algorithm  # nosec
                    timeseries_dump_dir = '%s/boundary/%s' % (settings.SKYLINE_TMP_DIR, algorithm)

                    # self.mkdir_p(timeseries_dump_dir)
                    mkdir_p(timeseries_dump_dir)
                    timeseries_dump_file = timeseries_dump_dir + "/" + metric_name + ".json"
                    with open(timeseries_dump_file, 'w+') as f:
                        f.write(str(timeseries))
                        f.close()

                # Check if a metric has its own unique BOUNDARY_METRICS alert
                # tuple, this allows us to paint an entire metric namespace with
                # the same brush AND paint a unique metric or namespace with a
                # different brush or scapel
                has_unique_tuple = False
                run_tupple = False
                boundary_metric_tuple = (base_name, algorithm, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger, alert_threshold, metric_alerters)
                wildcard_namespace = True
                for metric_tuple in BOUNDARY_METRICS:
                    if not has_unique_tuple:
                        # @modified 20200622 - Task #3586: Change all alert pattern checks to matched_or_regexed_in_list
                        #                      Feature #3512: matched_or_regexed_in_list function
                        # Changed original alert matching pattern to use new
                        # method
                        # CHECK_MATCH_PATTERN = metric_tuple[0]
                        try:
                            pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, [metric_tuple[0]])
                            if ENABLE_BOUNDARY_DEBUG and pattern_match:
                                logger.debug('debug :: %s matched alert - %s' % (base_name, str(metric_tuple[0])))
                            try:
                                del metric_matched_by
                            except:
                                pass
                        except:
                            pattern_match = False
                        # check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
                        # pattern_match = check_match_pattern.match(base_name)
                        if pattern_match:
                            if metric_tuple[0] == base_name:
                                wildcard_namespace = False
                            if not has_unique_tuple:
                                if boundary_metric_tuple == metric_tuple:
                                    has_unique_tuple = True
                                    run_tupple = True
                                    if ENABLE_BOUNDARY_DEBUG:
                                        logger.info('unique_tuple:')
                                        logger.info('boundary_metric_tuple: %s' % str(boundary_metric_tuple))
                                        logger.info('metric_tuple: %s' % str(metric_tuple))

                if not has_unique_tuple:
                    if wildcard_namespace:
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.info('wildcard_namespace:')
                            logger.info('boundary_metric_tuple: %s' % str(boundary_metric_tuple))
                        run_tupple = True
                    else:
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.info('wildcard_namespace: BUT WOULD NOT RUN')
                            logger.info('boundary_metric_tuple: %s' % str(boundary_metric_tuple))

                if ENABLE_BOUNDARY_DEBUG:
                    logger.info('WOULD RUN run_selected_algorithm - %s' % run_tupple)

                if run_tupple:
                    # @added 20181126 - Task #2742: Update Boundary
                    #                   Feature #2034: analyse_derivatives
                    # Convert the values of metrics strictly increasing monotonically
                    # to their deriative products
                    known_derivative_metric = False
                    try:
                        # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                        #                      Branch #3262: py3
                        # derivative_metrics = list(self.redis_conn.smembers('derivative_metrics'))
                        # @modified 20211012 - Feature #4280: aet.metrics_manager.derivative_metrics Redis hash
                        # derivative_metrics = list(self.redis_conn_decoded.smembers('derivative_metrics'))
                        derivative_metrics = list(self.redis_conn_decoded.smembers('aet.metrics_manager.derivative_metrics'))
                    except:
                        derivative_metrics = []
                    redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(base_name))
                    if redis_metric_name in derivative_metrics:
                        known_derivative_metric = True
                    if known_derivative_metric:
                        try:
                            non_derivative_monotonic_metrics = settings.NON_DERIVATIVE_MONOTONIC_METRICS
                        except:
                            non_derivative_monotonic_metrics = []
                        skip_derivative = in_list(redis_metric_name, non_derivative_monotonic_metrics)
                        if skip_derivative:
                            known_derivative_metric = False

                    # @added 20230220 - Feature #4854: boundary - labelled_metrics
                    #                   Task #2732: Prometheus to Skyline
                    #                   Branch #4300: prometheus
                    check_monotonicity = False
                    if labelled_metric:
                        try:
                            counter_metric = self.redis_conn_decoded.hget('skyline.labelled_metrics.id.type', str(metric_id))
                            if counter_metric:
                                if int(counter_metric) == 1:
                                    known_derivative_metric = True
                            else:
                                check_monotonicity = True
                        except Exception as err:
                            logger.error('error :: failed to determine metric type from skyline.labelled_metrics.id.type for metric_id: %s, %s' % (
                                str(metric_id), err))
                            check_monotonicity = True
                    if check_monotonicity:
                        try:
                            known_derivative_metric = strictly_increasing_monotonicity(timeseries)
                        except Exception as err:
                            logger.error('error :: strictly_increasing_monotonicity failed on metric_id: %s, %s' % (
                                str(metric_id), err))

                    if known_derivative_metric:
                        try:
                            derivative_timeseries = nonNegativeDerivative(timeseries)
                            timeseries = derivative_timeseries
                        except:
                            logger.error('error :: nonNegativeDerivative failed')

                    # @added 20220407 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
                    #                   Feature #4520: settings - ZERO_FILL_NAMESPACES
                    if base_name in zero_fill_metrics:
                        if not known_derivative_metric:
                            try:
                                timeseries = full_duration_timeseries_fill(self, skyline_app, base_name, timeseries, 'zero')
                            except Exception as err:
                                logger.error(traceback.format_exc())
                                logger.error('error :: full_duration_timeseries_fill failed - %s' % err)
                        else:
                            # Fix the badly defined metric if a metric has been defined
                            # as a zero_fill metric but is a derivative_metric apply
                            # last_known_value AFTER nonNegativeDerivative because
                            # zero filling derivative metrics does not have the desired
                            # effect
                            last_known_value_metrics.append(base_name)
                    if base_name in last_known_value_metrics:
                        try:
                            timeseries = full_duration_timeseries_fill(self, skyline_app, base_name, timeseries, 'last_known_value')
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: full_duration_timeseries_fill failed - %s' % err)

                    # @added 20200624 - Task #3594: Add timestamp to ENABLE_BOUNDARY_DEBUG output
                    #                   Feature #3532: Sort all time series
                    try:
                        metric_timestamp = int(timeseries[-1][0])
                    except:
                        metric_timestamp = None

                    # @modified 20230221 - Feature #4854: boundary - labelled_metrics
                    #                      Task #2732: Prometheus to Skyline
                    #                      Branch #4300: prometheus
                    # Do not analyse if no timeseries and wrapped run_selected_algorithm
                    # in try and except
                    if not timeseries:
                        anomalous = None
                    else:
                        # Submit the timeseries and settings to run_selected_algorithm
                        try:
                            anomalous, ensemble, datapoint, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger, alert_threshold, metric_alerters, algorithm = run_selected_algorithm(
                                timeseries, metric_name,
                                metric_expiration_time,
                                metric_min_average,
                                metric_min_average_seconds,
                                metric_trigger,
                                alert_threshold,
                                metric_alerters,
                                autoaggregate,
                                autoaggregate_value,
                                algorithm
                            )
                        except TypeError:
                            exceptions['DeletedByRoomba'] += 1
                            anomalous = None
                        except TooShort:
                            exceptions['TooShort'] += 1
                            logger.info('TooShort metric :: %s - %s' % (base_name, str(metric_timestamp)))
                            anomalous = None
                        except Stale:
                            exceptions['Stale'] += 1
                            logger.info('Stale metric :: %s - %s' % (base_name, str(metric_timestamp)))
                            anomalous = None
                        except Boring:
                            exceptions['Boring'] += 1
                            logger.info('Boring metric :: %s - %s' % (base_name, str(metric_timestamp)))
                            anomalous = None
                        except Exception as err:
                            logger.warning('warning :: run_selected_algorithm failed on %s - %s' % (metric_name, err))
                            boundary_errors[metric_name] = {'err': str(str), 'traceback': str(traceback.format_exc())}
                            anomalous = None

                        if ENABLE_BOUNDARY_DEBUG:
                            # @modified 20200624 - Task #3594: Add timestamp to ENABLE_BOUNDARY_DEBUG output
                            #                      Feature #3532: Sort all time series
                            # logger.debug('debug :: analysed - %s' % (metric_name))
                            try:
                                logger.debug('debug :: analysed - %s, with datapoint %s at timestamp %s' % (
                                    metric_name, str(datapoint),
                                    str(metric_timestamp)))
                            except:
                                logger.error('error :: debug :: analysed - %s, but unknown datapoint or timestamp' % (
                                    metric_name))

                        # @added 20220420 - Feature #4530: namespace.analysed_events
                        analysed_metrics.append(base_name)

                    # @added 20171214 - Bug #2232: Expiry boundary last_seen keys appropriately
                    # If it's not anomalous, add it to list
                    if not anomalous:
                        not_anomalous_metric = [datapoint, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger, alert_threshold, metric_alerters, algorithm]

                        # @added 20230220 - Feature #4854: boundary - labelled_metrics
                        #                   Task #2732: Prometheus to Skyline
                        #                   Branch #4300: prometheus
                        if labelled_metric:
                            not_anomalous_metric = [datapoint, labelled_metric, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger, alert_threshold, metric_alerters, algorithm]

                        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        # self.not_anomalous_metrics.append(not_anomalous_metric)
                        try:
                            self.redis_conn.sadd('boundary.not_anomalous_metrics', str(not_anomalous_metric))
                        except Exception as e:
                            logger.error('error :: could not add %s to Redis set boundary.not_anomalous_metrics: %s' % (
                                str(not_anomalous_metric), e))
                else:
                    anomalous = False
                    if ENABLE_BOUNDARY_DEBUG:
                        logger.debug('debug :: more unique metric tuple not analysed - %s' % (metric_name))

                if not anomalous:
                    analysis_stats['not_anomalous'] += 1
                else:
                    analysis_stats['anomalous'] += 1

                # If it's anomalous, add it to list
                if anomalous:
                    # @added 20200122 - Feature #3396: http_alerter
                    # Add the metric timestamp for the http_alerter resend queue
                    metric_timestamp = int(timeseries[-1][0])

                    # @added 20200122 - Feature #3396: http_alerter
                    # Add the metric timestamp for the http_alerter resend queue
                    # anomalous_metric = [datapoint, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger, alert_threshold, metric_alerters, algorithm]
                    anomalous_metric = [datapoint, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger, alert_threshold, metric_alerters, algorithm, metric_timestamp]
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # self.anomalous_metrics.append(anomalous_metric)
                    try:
                        self.redis_conn.sadd('boundary.anomalous_metrics', str(anomalous_metric))
                    except Exception as e:
                        logger.error('error :: could not add %s to Redis set boundary.anomalous_metrics: %s' % (
                            str(anomalous_metric), e))

                    # Get the anomaly breakdown - who returned True?
                    triggered_algorithms = []
                    for index, value in enumerate(ensemble):
                        if value:
                            anomaly_breakdown[algorithm] += 1
                            triggered_algorithms.append(algorithm)

                    # @added 20220504 - Feature #2580: illuminance
                    try:

                        # @modified 20230220 - Feature #4854: boundary - labelled_metrics
                        #                      Task #2732: Prometheus to Skyline
                        #                      Branch #4300: prometheus
                        # illuminance_dict[base_name] = {
                        illuminance_dict[use_base_name] = {
                            'timestamp': int(metric_timestamp),
                            'value': float(datapoint),
                            'triggered_algorithms_count': len(triggered_algorithms)}
                    except Exception as err:
                        logger.error('error :: failed to add %s to illuminance_dict' % (
                            str(use_base_name)))

                    # If Crucible or Panorama are enabled determine details
                    determine_anomaly_details = False
                    if settings.ENABLE_CRUCIBLE and settings.BOUNDARY_CRUCIBLE_ENABLED:
                        determine_anomaly_details = True
                    if settings.PANORAMA_ENABLED:
                        determine_anomaly_details = True

                    if determine_anomaly_details:
                        metric_timestamp = str(int(timeseries[-1][0]))
                        # Handle single data point time series
                        # from_timestamp = str(int(timeseries[1][0]))
                        from_timestamp = str(int(timeseries[0][0]))
                        timeseries_dir = base_name.replace('.', '/')

                    # If Panorama is enabled - create a Panorama check
                    if settings.PANORAMA_ENABLED:
                        # Note:
                        # The values are enclosed is single quoted intentionally
                        # as the imp.load_source used results in a shift in the
                        # decimal position when double quoted, e.g.
                        # value = "5622.0" gets imported as
                        # 2016-03-02 12:53:26 :: 28569 :: metric variable - value - 562.2
                        # single quoting results in the desired,
                        # 2016-03-02 13:16:17 :: 1515 :: metric variable - value - 5622.0
                        # @modified 20171216 - Task #2236: Change Boundary to only send to Panorama on alert
                        # added_at now passedas an argument to spin_process so that the panaroma_anomaly_file
                        # can be moved from SKYLINE_TMP_DIR to the PANORAMA_CHECK_PATH
                        # added_at = str(int(time()))
                        # @modified 20230220 - Feature #4854: boundary - labelled_metrics
                        #                      Task #2732: Prometheus to Skyline
                        #                      Branch #4300: prometheus
                        # source = 'graphite'
                        source = 'redis'
                        if labelled_metric:
                            source = 'redistimeseries'

                        panaroma_anomaly_data = 'metric = \'%s\'\n' \
                                                'value = \'%s\'\n' \
                                                'from_timestamp = \'%s\'\n' \
                                                'metric_timestamp = \'%s\'\n' \
                                                'algorithms = [\'%s\']\n' \
                                                'triggered_algorithms = [\'%s\']\n' \
                                                'app = \'%s\'\n' \
                                                'source = \'%s\'\n' \
                                                'added_by = \'%s\'\n' \
                                                'added_at = \'%s\'\n' \
                            % (base_name, str(datapoint), from_timestamp,
                               metric_timestamp, str(algorithm), str(algorithm),
                               skyline_app, source, this_host, added_at)

                        # Create an anomaly file with details about the anomaly
                        # @modified 20171214 - Task #2236: Change Boundary to only send to Panorama on alert
                        # Only send to Panorama IF Boundary is going to alert,
                        # so here the file is written to SKYLINE_TMP_DIR
                        # instead and moved in def run() if an alert is sent
                        # panaroma_anomaly_file = '%s/%s.%s.txt' % (
                        #     settings.PANORAMA_CHECK_PATH, added_at,
                        #     base_name)
                        # @modified 20171228 - Task #2236: Change Boundary to only send to Panorama on alert
                        # Added algorithm as it is required if the metric has
                        # multiple rules covering a number of algorithms
                        tmp_panaroma_anomaly_file = '%s/%s.%s.%s.panorama_anomaly.txt' % (
                            settings.SKYLINE_TMP_DIR, str(added_at), str(algorithm),
                            base_name)

                        # @added 20230220 - Feature #4854: boundary - labelled_metrics
                        #                   Task #2732: Prometheus to Skyline
                        #                   Branch #4300: prometheus
                        if labelled_metric:
                            tmp_panaroma_anomaly_file = '%s/%s.%s.%s.panorama_anomaly.txt' % (
                                settings.SKYLINE_TMP_DIR, str(added_at), str(algorithm),
                                labelled_metric)

                        try:
                            write_data_to_file(
                                skyline_app, tmp_panaroma_anomaly_file, 'w',
                                panaroma_anomaly_data)
                            logger.info('added tmp panorama anomaly file :: %s' % (tmp_panaroma_anomaly_file))
                        except:
                            logger.error('error :: failed to add tmp panorama anomaly file :: %s' % (tmp_panaroma_anomaly_file))
                            logger.info(traceback.format_exc())

                    # If crucible is enabled - save timeseries and create a
                    # crucible check
                    if settings.ENABLE_CRUCIBLE and settings.BOUNDARY_CRUCIBLE_ENABLED:
                        crucible_anomaly_dir = settings.CRUCIBLE_DATA_FOLDER + '/' + timeseries_dir + '/' + metric_timestamp
                        if not os.path.exists(crucible_anomaly_dir):
                            if python_version == 2:
                                # @modified 20191022 - Task #2828: Skyline - Python 3.7
                                #                      Branch #3262: py3
                                # mode_arg = int('0755')
                                # os.makedirs(crucible_anomaly_dir, mode_arg)
                                os.makedirs(crucible_anomaly_dir, 0o755)
                            if python_version == 3:
                                os.makedirs(crucible_anomaly_dir, mode=0o755)

                        # Note:
                        # Due to only one algorithm triggering here the
                        # algorithm related arrays here are a different format
                        # to there output format in analyzer

                        # Note:
                        # The value is enclosed is single quoted intentionally
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
                               metric_timestamp, str(algorithm),
                               triggered_algorithms, crucible_anomaly_dir,
                               skyline_app, metric_timestamp)

                        # Create an anomaly file with details about the anomaly
                        crucible_anomaly_file = '%s/%s.txt' % (crucible_anomaly_dir, base_name)
                        with open(crucible_anomaly_file, 'w') as fh:
                            fh.write(crucible_anomaly_data)
                        if python_version == 2:
                            # @modified 20191021 - Branch #3262: py3
                            # os.chmod(crucible_anomaly_file, 0644)
                            os.chmod(crucible_anomaly_file, 0o644)
                        if python_version == 3:
                            os.chmod(crucible_anomaly_file, mode=0o644)
                        logger.info('added crucible anomaly file :: %s/%s.txt' % (crucible_anomaly_dir, base_name))

                        # Create timeseries json file with the timeseries
                        json_file = '%s/%s.json' % (crucible_anomaly_dir, base_name)
                        timeseries_json = str(timeseries).replace('[', '(').replace(']', ')')
                        with open(json_file, 'w') as fh:
                            # timeseries
                            fh.write(timeseries_json)
                        if python_version == 2:
                            # @modified 20191021 - Branch #3262: py3
                            # os.chmod(json_file, 0644)
                            os.chmod(json_file, 0o644)
                        if python_version == 3:
                            os.chmod(json_file, mode=0o644)
                        logger.info('added crucible timeseries file :: %s/%s.json' % (crucible_anomaly_dir, base_name))

                        # Create a crucible check file
                        crucible_check_file = '%s/%s.%s.txt' % (settings.CRUCIBLE_CHECK_PATH, metric_timestamp, base_name)
                        with open(crucible_check_file, 'w') as fh:
                            fh.write(crucible_anomaly_data)
                        if python_version == 2:
                            # @modified 20191021 - Branch #3262: py3
                            # os.chmod(crucible_check_file, 0644)
                            os.chmod(crucible_check_file, 0o644)
                        if python_version == 3:
                            os.chmod(crucible_check_file, mode=0o644)
                        logger.info('added crucible check :: %s,%s' % (base_name, metric_timestamp))

            # It could have been deleted by the Roomba
            except TypeError:
                exceptions['DeletedByRoomba'] += 1
            except TooShort:
                exceptions['TooShort'] += 1
                logger.info('TooShort metric :: %s - %s' % (base_name, str(metric_timestamp)))
            except Stale:
                exceptions['Stale'] += 1
                logger.info('Stale metric :: %s - %s' % (base_name, str(metric_timestamp)))
            except Boring:
                exceptions['Boring'] += 1
                logger.info('Boring metric :: %s - %s' % (base_name, str(metric_timestamp)))
            # except:
            #     logger.error(traceback.format_exc())
            #     exceptions['Other'] += 1
            #     logger.error('error :: exceptions[\'Other\']')
            except Exception as err:
                logger.warning('warning :: analysis failed on %s - %s' % (base_name, err))
                boundary_errors[base_name] = {'err': str(str), 'traceback': str(traceback.format_exc())}
                exceptions['Other'] += 1

        # @added 20220420 - Feature #4530: namespace.analysed_events
        namespace_analysed = defaultdict(int)
        for base_name in list(set(analysed_metrics)):
            parent_namespace = base_name.split('.')[0]

            # @added 20230220 - Feature #4854: boundary - labelled_metrics
            #                   Task #2732: Prometheus to Skyline
            #                   Branch #4300: prometheus
            if '_tenant_id="' in base_name:
                try:
                    metric_dict = metric_name_labels_parser(skyline_app, base_name)
                except:
                    pass
                if metric_dict:
                    try:
                        parent_namespace = metric_dict['labels']['_tenant_id']
                    except:
                        pass

            namespace_analysed[parent_namespace] += 1
        date_string = str(strftime('%Y-%m-%d', gmtime()))
        namespace_analysed_events_hash = 'namespace.analysed_events.%s.%s' % (skyline_app, date_string)
        for namespace in list(namespace_analysed.keys()):
            try:
                self.redis_conn.hincrby(namespace_analysed_events_hash, namespace, namespace_analysed[namespace])
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to increment %s Redis hash - %s' % (
                    namespace_analysed_events_hash, err))
        try:
            self.redis_conn.expire(namespace_analysed_events_hash, (86400 * 15))
            logger.info('updated %s Redis hash' % namespace_analysed_events_hash)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to set expire %s Redis hash - %s' % (
                namespace_analysed_events_hash, err))

        # @added 20220504 - Feature #2580: illuminance
        if len(illuminance_dict) > 0:
            logger.info('calling add_illuminance_entries with %s entries to add' % (
                str(len(illuminance_dict))))
            current_illuminance_dict = {}
            try:
                current_illuminance_dict = add_illuminance_entries(self, skyline_app, int(added_at), illuminance_dict)
            except Exception as err:
                logger.error('error :: add_illuminance_entries failed - %s' % (
                    err))
            logger.info('illuminance Redis hash now has %s entries' % (
                str(len(current_illuminance_dict))))

        # Add values to the queue so the parent process can collate
        for key, value in anomaly_breakdown.items():
            self.anomaly_breakdown_q.put((key, value))
            if ENABLE_BOUNDARY_DEBUG:
                logger.debug('debug :: anomaly_breakdown.item - %s, %s' % (str(key), str(value)))
        for key, value in exceptions.items():
            self.exceptions_q.put((key, value))
            if ENABLE_BOUNDARY_DEBUG:
                logger.debug('debug :: exceptions.item - %s, %s' % (str(key), str(value)))

        # @added 20230222 - Feature #4854: boundary - labelled_metrics
        #                   Task #2732: Prometheus to Skyline
        #                   Branch #4300: prometheus
        analysis_stats['assigned_metrics'] = len(unique_assigned_metrics)
        analysis_stats['analysed_metrics'] = len(analysed_metrics)
        logger.info('analysis_stats: %s' % str(analysis_stats))
        if boundary_errors:
            # Convert dicts to strings as Redis hash requires strings
            for key, value in boundary_errors.items():
                boundary_errors[key] = str(value)
            hash_key = 'boundary.errors.%s' % str(time())
            logger.warning('warning :: recording %s boundary_errors to Redis hash %s' % (
                str(len(boundary_errors)), hash_key))
            try:
                self.redis_conn.hset(hash_key, mapping=boundary_errors)
                self.redis_conn.expire(hash_key, 300)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to set %s Redis hash - %s' % (
                    hash_key, err))

    def run(self):
        """
        Called when the process intializes.
        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        if os.path.isfile(skyline_app_logwait):
            try:
                os.remove(skyline_app_logwait)
            except OSError:
                logger.error('error :: failed to remove %s, continuing' % skyline_app_logwait)

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
            logger.error('error :: bin/%s.d log management seems to have failed, continuing' % skyline_app)
            try:
                os.remove(skyline_app_loglock)
                logger.info('log lock file removed')
            except OSError:
                logger.error('error :: failed to remove %s, continuing' % skyline_app_loglock)
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
            ENABLE_BOUNDARY_DEBUG = settings.ENABLE_BOUNDARY_DEBUG
            logger.info('ENABLE_BOUNDARY_DEBUG is set from settings.py to %s' % str(ENABLE_BOUNDARY_DEBUG))
        except:
            logger.info('warning :: ENABLE_BOUNDARY_DEBUG is not declared in settings.py, defaults to False')
            ENABLE_BOUNDARY_DEBUG = False
        try:
            # BOUNDARY_METRICS = settings.BOUNDARY_METRICS
            boundary_metrics_count = len(BOUNDARY_METRICS)
            logger.info('BOUNDARY_METRICS is set from settings.py with %s Boundary metrics' % str(boundary_metrics_count))
            if ENABLE_BOUNDARY_DEBUG:
                logger.debug('debug :: BOUNDARY_METRICS - %s' % str(BOUNDARY_METRICS))
        except:
            # BOUNDARY_METRICS = []
            logger.info('warning :: BOUNDARY_METRICS is not declared in settings.py, defaults to []')
        try:
            # BOUNDARY_AUTOAGGRERATION = settings.BOUNDARY_AUTOAGGRERATION
            logger.info('BOUNDARY_AUTOAGGRERATION is set from settings.py to %s' % str(BOUNDARY_AUTOAGGRERATION))
        except:
            BOUNDARY_AUTOAGGRERATION = False
            logger.info('warning :: BOUNDARY_AUTOAGGRERATION is not declared in settings.py, defaults to False')
        try:
            # BOUNDARY_AUTOAGGRERATION_METRICS = settings.BOUNDARY_AUTOAGGRERATION_METRICS
            logger.info('BOUNDARY_AUTOAGGRERATION_METRICS is set from settings.py -  %s' % str(BOUNDARY_AUTOAGGRERATION_METRICS))
        except:
            BOUNDARY_AUTOAGGRERATION_METRICS = (
                ('autoaggeration_metrics_not_declared', 60)
            )
            logger.info('warning :: BOUNDARY_AUTOAGGRERATION_METRICS is not declared in settings.py, defaults to %s' % (
                str(BOUNDARY_AUTOAGGRERATION_METRICS[0])))

        # @modified 20191022 - Branch #3262: py3
        # python-2.x and python3.x handle while 1 and while True differently
        # while 1:
        running = True
        while running:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
                if ENABLE_BOUNDARY_DEBUG:
                    logger.debug('debug :: Redis is up')
            except:
                logger.error('error :: skyline cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                sleep(10)
                # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                # if settings.REDIS_PASSWORD:
                #     self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                # else:
                #     self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                # self.redis_conn = get_redis_conn(skyline_app)
                continue

            # Report app up
            self.redis_conn.setex(skyline_app, 120, now)

            # Discover unique metrics
            # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            # unique_metrics = list(self.redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
            # @modified 20230217 - Feature #4854: boundary - labelled_metrics
            #                      Task #2732: Prometheus to Skyline
            #                      Branch #4300: prometheus
            # Change from using the unique_metrics to using metrics_manager.boundary_metrics
            # which includes labelled_metrics too and computed and refreshed
            # every 5 minutes in metrics_manager
            # redis_set = settings.FULL_NAMESPACE + 'unique_metrics'
            # try:
            #     unique_metrics = list(self.redis_conn_decoded.smembers(redis_set))
            # except:
            #     logger.error(traceback.format_exc())
            #     logger.error('error :: failed to generate list from Redis set %s' % redis_set)

            # @added 20230217 - Feature #4854: boundary - labelled_metrics
            #                   Task #2732: Prometheus to Skyline
            #                   Branch #4300: prometheus
            # Added the ability for boundary to handle labelled_metrics as well
            # by changing to using metrics_manager.boundary_metrics
            boundary_metrics_redis_dict = {}
            try:
                boundary_metrics_redis_dict = self.redis_conn_decoded.hgetall('metrics_manager.boundary_metrics')
            except Exception as err:
                logger.error('error :: failed to hgetall Redis hash key metrics_manager.boundary_metrics - %s' % (
                    err))
            boundary_metrics_list = list(boundary_metrics_redis_dict.keys())

            # @modified 20230217 - Feature #4854: boundary - labelled_metrics
            #                      Task #2732: Prometheus to Skyline
            #                      Branch #4300: prometheus
            # Change from using the unique_metrics to using metrics_manager.boundary_metrics
            # if len(unique_metrics) == 0:
            if len(boundary_metrics_list) == 0:
                logger.info('no identified BOUNDARY_METRICS from metrics_manager.boundary_metrics')
                sleep(10)
                continue
            if ENABLE_BOUNDARY_DEBUG:
                logger.debug('debug :: %s metrics in Redis hash metrics_manager.boundary_metrics' % (
                    # str(len(unique_metrics)), redis_set))
                    str(len(boundary_metrics_list))))

            # Reset boundary_metrics
            boundary_metrics = []

            # Build boundary metrics
            # @modified 20230217 - Feature #4854: boundary - labelled_metrics
            #                      Task #2732: Prometheus to Skyline
            #                      Branch #4300: prometheus
            # Change from using the unique_metrics to using metrics_manager.boundary_metrics
            # for metric_name in unique_metrics:
            for metric_name in boundary_metrics_list:
                if '_tenant_id="' not in metric_name:
                    if settings.FULL_NAMESPACE:
                        metric_name = '%s%s' % (settings.FULL_NAMESPACE, metric_name)
                for metric in BOUNDARY_METRICS:
                    if ENABLE_BOUNDARY_DEBUG:
                        logger.debug('debug :: pattern matching %s against BOUNDARY_METRICS %s' % (
                            str(metric_name), str(metric)))
                    use_old_pattern = False
                    if use_old_pattern:
                        try:
                            CHECK_MATCH_PATTERN = metric[0]
                            check_match_pattern = re.compile(CHECK_MATCH_PATTERN)

                            # @added 20191021 - Branch #3262: py3
                            metric_name = str(metric_name)

                            # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
                            # base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                            if metric_name.startswith(settings.FULL_NAMESPACE):
                                base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                            else:
                                base_name = metric_name

                            pattern_match = check_match_pattern.match(base_name)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: pattern matching - %s, %s' % (str(metric_name), str(metric)))

                    # @modified 20210718 - Task #3586: Change all alert pattern checks to matched_or_regexed_in_list
                    #                      Feature #3512: matched_or_regexed_in_list function
                    try:
                        if metric_name.startswith(settings.FULL_NAMESPACE):
                            base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                        else:
                            base_name = metric_name
                        pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, [metric[0]])
                        if ENABLE_BOUNDARY_DEBUG and pattern_match:
                            logger.debug('debug :: %s matched alert - %s' % (base_name, metric[0]))
                        try:
                            del metric_matched_by
                        except:
                            pass
                    except:
                        pattern_match = False

                    if pattern_match:
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.debug('debug :: boundary metric - pattern MATCHED - ' + metric[0] + " | " + base_name)
                        boundary_metrics.append([metric_name, metric[1]])

            if ENABLE_BOUNDARY_DEBUG:
                logger.debug('debug :: boundary metrics - ' + str(boundary_metrics))

            # @added 20191106 - Branch #3262: py3
            if os.path.isfile(alert_test_file):
                test_alert = None
                try:
                    with open((alert_test_file), 'r') as fh:
                        raw_test_alert = fh.read()
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
                        logger.info('test alert to %s for %s' % (test_alerter, metric_name))
                        # @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
                        # trigger_alert(test_alerter, 1, metric_name, 10, 1, 'testing', int(time()))
                        trigger_alert(test_alerter, 1, metric_name, 10, 1, 'testing', int(time()), 0)
                    except:
                        logger.error('error :: test trigger_alert - %s' % traceback.format_exc())
                try:
                    os.remove(alert_test_file)
                except OSError:
                    logger.error('error - failed to remove %s, continuing' % alert_test_file)

            if len(boundary_metrics) == 0:
                logger.info('no Boundary metrics in redis. try adding some - see README')
                sleep(10)
                continue

            # @added 20171216 - Task #2236: Change Boundary to only send to Panorama on alert
            # Pass added_at as an argument t spin_process so that the panaroma_anomaly_file
            # can be moved from SKYLINE_TMP_DIR to the PANORAMA_CHECK_PATH
            added_at = str(int(time()))

            # Spawn processes
            pids = []
            for i in range(1, settings.BOUNDARY_PROCESSES + 1):
                if i > len(boundary_metrics):
                    logger.info('WARNING: Skyline Boundary is set for more cores than needed.')
                    break

                # @modified 20171216 - Task #2236: Change Boundary to only send to Panorama on alert
                # Pass added_at as an argument to spin_process so that the panaroma_anomaly_file
                # can be moved from SKYLINE_TMP_DIR to the PANORAMA_CHECK_PATH
                # p = Process(target=self.spin_process, args=(i, boundary_metrics))
                p = Process(target=self.spin_process, args=(i, boundary_metrics, added_at))
                pids.append(p)
                p.start()

            # Send wait signal to zombie processes
            for p in pids:
                p.join()

            # Grab data from the queue and populate dictionaries
            exceptions = {}

            # @added 20211107 - send default values
            exceptions['Boring'] = 0
            exceptions['Stale'] = 0
            exceptions['TooShort'] = 0

            anomaly_breakdown = {}
            while 1:
                try:
                    key, value = self.anomaly_breakdown_q.get_nowait()
                    if key not in list(anomaly_breakdown.keys()):
                        anomaly_breakdown[key] = value
                    else:
                        anomaly_breakdown[key] += value
                except Empty:
                    break

            while 1:
                try:
                    key, value = self.exceptions_q.get_nowait()
                    if key not in list(exceptions.keys()):
                        exceptions[key] = value
                    else:
                        exceptions[key] += value
                except Empty:
                    break

            # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # Use Redis set instead of Manager() list
            boundary_not_anomalous_metrics = []
            try:
                # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # literal_boundary_not_anomalous_metrics = list(self.redis_conn.smembers('boundary.not_anomalous_metrics'))
                literal_boundary_not_anomalous_metrics = list(self.redis_conn_decoded.smembers('boundary.not_anomalous_metrics'))
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to generate list from Redis set boundary.not_anomalous_metrics')
                literal_boundary_not_anomalous_metrics = []
            for metric_list_string in literal_boundary_not_anomalous_metrics:
                metric = literal_eval(metric_list_string)
                boundary_not_anomalous_metrics.append(metric)

            # @added 20171214 - Bug #2232: Expiry boundary last_seen keys appropriately
            # Expire keys
            if settings.BOUNDARY_ENABLE_ALERTS:
                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # for not_anomalous_metric in self.not_anomalous_metrics:
                for not_anomalous_metric in boundary_not_anomalous_metrics:
                    metric_name = not_anomalous_metric[1]
                    base_name = metric_name.replace(FULL_NAMESPACE, '', 1)
                    algorithm = str(not_anomalous_metric[8])
                    if ENABLE_BOUNDARY_DEBUG:
                        logger.info("debug :: not_anomalous_metric - " + str(not_anomalous_metric))
                    anomaly_cache_key_expiration_time = 1
                    # @modified 20171228 - Task #2236: Change Boundary to only send to Panorama on alert
                    # Wrapped in try - Added algorithm as it is required if the metric has
                    # multiple rules covering a number of algorithms
                    try:
                        anomaly_cache_key = 'anomaly_seen.%s.%s' % (algorithm, base_name)
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.debug('debug :: anomaly_cache_key - anomaly_seen.%s.%s' % (algorithm, base_name))
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to determine string for anomaly_cache_key')
                        anomaly_cache_key = 'anomaly_seen.%s' % (base_name)
                    times_seen = 0
                    try:
                        self.redis_conn.setex(anomaly_cache_key, anomaly_cache_key_expiration_time, packb(int(times_seen)))
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.debug('debug :: redis - anomaly_cache_key set OK - %s' % str(anomaly_cache_key))
                    except:
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.debug('debug :: redis failed - anomaly_cache_key set failed - %s' % str(anomaly_cache_key))
                    # @added 20171216 - Task #2236: Change Boundary to only send to Panorama on alert
                    # Remove tmp_panaroma_anomaly_file
                    # @modified 20171228 - Task #2236: Change Boundary to only send to Panorama on alert
                    # Added algorithm as it is required if the metric has
                    # multiple rules covering a number of algorithms
                    tmp_panaroma_anomaly_file = '%s/%s.%s.%s.panorama_anomaly.txt' % (
                        settings.SKYLINE_TMP_DIR, str(added_at), str(algorithm), base_name)
                    if ENABLE_BOUNDARY_DEBUG:
                        logger.debug('debug :: set tmp_panaroma_anomaly_file to - %s' % (str(tmp_panaroma_anomaly_file)))
                    if os.path.isfile(tmp_panaroma_anomaly_file):
                        try:
                            if ENABLE_BOUNDARY_DEBUG:
                                logger.debug('debug :: removing tmp_panaroma_anomaly_file - %s' % (str(tmp_panaroma_anomaly_file)))
                            os.remove(str(tmp_panaroma_anomaly_file))
                        except OSError:
                            if ENABLE_BOUNDARY_DEBUG:
                                logger.debug('debug :: error removing tmp_panaroma_anomaly_file - %s' % (str(tmp_panaroma_anomaly_file)))

            # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # Use Redis set instead of Manager() list
            boundary_anomalous_metrics = []
            try:
                # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # literal_boundary_anomalous_metrics = list(self.redis_conn.smembers('boundary.anomalous_metrics'))
                literal_boundary_anomalous_metrics = list(self.redis_conn_decoded.smembers('boundary.anomalous_metrics'))
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to generate list from Redis set boundary.anomalous_metrics')
                literal_boundary_anomalous_metrics = []
            for metric_list_string in literal_boundary_anomalous_metrics:
                try:
                    metric = literal_eval(metric_list_string)
                    boundary_anomalous_metrics.append(metric)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to literal_eval metric_list_string - %s' % str(metric_list_string))

            # Send alerts
            if settings.BOUNDARY_ENABLE_ALERTS:
                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # for anomalous_metric in self.anomalous_metrics:
                for anomalous_metric in boundary_anomalous_metrics:
                    datapoint = str(anomalous_metric[0])
                    metric_name = anomalous_metric[1]
                    base_name = metric_name.replace(FULL_NAMESPACE, '', 1)
                    expiration_time = str(anomalous_metric[2])
                    metric_trigger = str(anomalous_metric[5])
                    alert_threshold = int(anomalous_metric[6])
                    metric_alerters = anomalous_metric[7]
                    algorithm = str(anomalous_metric[8])
                    # @added 20200122 - Feature #3396: http_alerter
                    # Add the metric timestamp for the http_alerter resend queue
                    metric_timestamp = anomalous_metric[9]

                    if ENABLE_BOUNDARY_DEBUG:
                        logger.info("debug :: anomalous_metric - " + str(anomalous_metric))

                    # Determine how many times has the anomaly been seen if the
                    # ALERT_THRESHOLD is set to > 1 and create a cache key in
                    # redis to keep count so that alert_threshold can be honored
                    if alert_threshold == 0:
                        times_seen = 1
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.info("debug :: alert_threshold - " + str(alert_threshold))

                    if alert_threshold == 1:
                        times_seen = 1
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.info("debug :: alert_threshold - " + str(alert_threshold))

                    # @added 20230220 - Feature #4854: boundary - labelled_metrics
                    #                   Task #2732: Prometheus to Skyline
                    #                   Branch #4300: prometheus
                    labelled_metric = False
                    use_base_name = str(metric_name)
                    if '_tenant_id="' in metric_name:
                        try:
                            metric_id = get_metric_id_from_base_name(skyline_app, base_name)
                        except Exception as err:
                            logger.error('error :: get_metric_id_from_base_name failed for %s - %s' % (
                                str(base_name), err))
                            metric_id = 0
                        labelled_metric = 'labelled_metrics.%s' % str(metric_id)
                        use_base_name = str(labelled_metric)

                    if alert_threshold > 1:
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.debug('debug :: alert_threshold - ' + str(alert_threshold))
                        anomaly_cache_key_count_set = False
                        anomaly_cache_key_expiration_time = (int(alert_threshold) + 1) * 60

                        # @modified 20230220 - Feature #4854: boundary - labelled_metrics
                        #                      Task #2732: Prometheus to Skyline
                        #                      Branch #4300: prometheus
                        # anomaly_cache_key = 'anomaly_seen.%s.%s' % (algorithm, base_name)
                        anomaly_cache_key = 'anomaly_seen.%s.%s' % (algorithm, use_base_name)

                        try:
                            anomaly_cache_key_count = self.redis_conn.get(anomaly_cache_key)
                            if not anomaly_cache_key_count:
                                try:
                                    if ENABLE_BOUNDARY_DEBUG:
                                        logger.debug('debug :: redis no anomaly_cache_key - ' + str(anomaly_cache_key))
                                    times_seen = 1
                                    if ENABLE_BOUNDARY_DEBUG:
                                        logger.debug('debug :: redis setex anomaly_cache_key - ' + str(anomaly_cache_key))
                                    self.redis_conn.setex(anomaly_cache_key, anomaly_cache_key_expiration_time, packb(int(times_seen)))
                                    logger.info('set anomaly seen key :: %s seen %s' % (anomaly_cache_key, str(times_seen)))
                                except Exception as e:
                                    logger.error('error :: redis setex failed :: %s' % str(anomaly_cache_key))
                                    logger.error('error :: could not set key: %s' % e)
                            else:
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.debug('debug :: redis anomaly_cache_key retrieved OK - ' + str(anomaly_cache_key))
                                anomaly_cache_key_count_set = True
                        except:
                            if ENABLE_BOUNDARY_DEBUG:
                                logger.debug('debug :: redis failed - anomaly_cache_key retrieval failed - ' + str(anomaly_cache_key))
                            anomaly_cache_key_count_set = False

                        if anomaly_cache_key_count_set:
                            unpacker = Unpacker(use_list=False)
                            unpacker.feed(anomaly_cache_key_count)
                            raw_times_seen = list(unpacker)
                            times_seen = int(raw_times_seen[0]) + 1
                            try:
                                self.redis_conn.setex(anomaly_cache_key, anomaly_cache_key_expiration_time, packb(int(times_seen)))
                                logger.info('set anomaly seen key :: %s seen %s' % (anomaly_cache_key, str(times_seen)))
                            except:
                                times_seen = 1
                                logger.error('error :: set anomaly seen key failed :: %s seen %s' % (anomaly_cache_key, str(times_seen)))

                    # Alert the alerters if times_seen > alert_threshold
                    if times_seen >= alert_threshold:
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.debug('debug :: times_seen %s is greater than or equal to alert_threshold %s' % (str(times_seen), str(alert_threshold)))

                        # @added 20171216 - Task #2236: Change Boundary to only send to Panorama on alert
                        tmp_panaroma_anomaly_file = '%s/%s.%s.%s.panorama_anomaly.txt' % (
                            settings.SKYLINE_TMP_DIR, str(added_at),
                            # @modified 20171228 - Task #2236: Change Boundary to only send to Panorama on alert
                            # Added algorithm as it is required if the metric has
                            # multiple rules covering a number of algorithms
                            str(algorithm), base_name)

                        # @added 20230220 - Feature #4854: boundary - labelled_metrics
                        #                      Task #2732: Prometheus to Skyline
                        #                      Branch #4300: prometheus
                        if labelled_metric:
                            tmp_panaroma_anomaly_file = '%s/%s.%s.%s.panorama_anomaly.txt' % (
                                settings.SKYLINE_TMP_DIR, str(added_at),
                                str(algorithm), labelled_metric)

                        if ENABLE_BOUNDARY_DEBUG:
                            logger.debug('debug :: tmp_panaroma_anomaly_file - %s' % (str(tmp_panaroma_anomaly_file)))
                        if os.path.isfile(tmp_panaroma_anomaly_file):
                            panaroma_anomaly_file = '%s/%s.%s.txt' % (
                                settings.PANORAMA_CHECK_PATH, str(added_at), base_name)

                            # @added 20230220 - Feature #4854: boundary - labelled_metrics
                            #                      Task #2732: Prometheus to Skyline
                            #                      Branch #4300: prometheus
                            if labelled_metric:
                                panaroma_anomaly_file = '%s/%s.%s.txt' % (
                                    settings.PANORAMA_CHECK_PATH, str(added_at), labelled_metric)

                            logger.info('moving tmp_panaroma_anomaly_file - %s to panaroma_anomaly_file %s' % (str(tmp_panaroma_anomaly_file), str(panaroma_anomaly_file)))
                            # @modified 20171228 - Task #2236: Change Boundary to only send to Panorama on alert
                            # Added skyline_app
                            try:
                                # @modified 20171228 - Task #2236: Change Boundary to only send to Panorama on alert
                                # Correct move
                                # move_file(skyline_app, tmp_panaroma_anomaly_file, panaroma_anomaly_file)
                                move_file(skyline_app, settings.PANORAMA_CHECK_PATH, tmp_panaroma_anomaly_file)
                            except:
                                logger.info(traceback.format_exc())
                                logger.error('error :: failed to move tmp_panaroma_anomaly_file to panaroma_anomaly_file')
                            # @added 20171228 - Task #2236: Change Boundary to only send to Panorama on alert
                            # Rename moved file as the filename is used in Panorama
                            try:
                                tmp_panaroma_anomaly_file_to_rename = '%s/%s.%s.%s.panorama_anomaly.txt' % (
                                    settings.PANORAMA_CHECK_PATH, str(added_at),
                                    str(algorithm), base_name)

                                # @added 20230220 - Feature #4854: boundary - labelled_metrics
                                #                      Task #2732: Prometheus to Skyline
                                #                      Branch #4300: prometheus
                                if labelled_metric:
                                    tmp_panaroma_anomaly_file_to_rename = '%s/%s.%s.%s.panorama_anomaly.txt' % (
                                        settings.PANORAMA_CHECK_PATH, str(added_at),
                                        str(algorithm), labelled_metric)

                                os.rename(tmp_panaroma_anomaly_file_to_rename, panaroma_anomaly_file)
                            except:
                                logger.info(traceback.format_exc())
                                logger.error('error :: failed to rename tmp_panaroma_anomaly_filename to panaroma_anomaly_filename')
                        else:
                            logger.warning('warning :: tmp_panaroma_anomaly_file does not exist')

                        for alerter in metric_alerters.split("|"):
                            # Determine alerter limits
                            send_alert = False
                            alerts_sent = 0
                            if ENABLE_BOUNDARY_DEBUG:
                                logger.debug('debug :: checking alerter - %s' % alerter)
                            try:
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.debug('debug :: determining alerter_expiration_time for settings')
                                alerter_expiration_time_setting = settings.BOUNDARY_ALERTER_OPTS['alerter_expiration_time'][alerter]
                                alerter_expiration_time = int(alerter_expiration_time_setting)
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.debug('debug :: determined alerter_expiration_time from settings - %s' % str(alerter_expiration_time))
                            except:
                                # Set an arbitrary expiry time if not set
                                alerter_expiration_time = 160
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info("debug :: could not determine alerter_expiration_time from settings")
                            try:
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info("debug :: determining alerter_limit from settings")
                                alerter_limit_setting = settings.BOUNDARY_ALERTER_OPTS['alerter_limit'][alerter]
                                alerter_limit = int(alerter_limit_setting)
                                alerter_limit_set = True
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info("debug :: determined alerter_limit from settings - %s" % str(alerter_limit))
                            except:
                                alerter_limit_set = False
                                send_alert = True
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info("debug :: could not determine alerter_limit from settings")

                            # If the alerter_limit is set determine how many
                            # alerts the alerter has sent
                            if alerter_limit_set:
                                alerter_sent_count_key = 'alerts_sent.%s' % (alerter)
                                try:
                                    alerter_sent_count_key_data = self.redis_conn.get(alerter_sent_count_key)
                                    if not alerter_sent_count_key_data:
                                        if ENABLE_BOUNDARY_DEBUG:
                                            logger.info("debug :: redis no alerter key, no alerts sent for - " + str(alerter_sent_count_key))
                                        alerts_sent = 0
                                        send_alert = True
                                        if ENABLE_BOUNDARY_DEBUG:
                                            logger.info("debug :: alerts_sent set to %s" % str(alerts_sent))
                                            logger.info("debug :: send_alert set to %s" % str(send_alert))
                                    else:
                                        if ENABLE_BOUNDARY_DEBUG:
                                            logger.debug('debug :: redis alerter key retrieved, unpacking %s' % str(alerter_sent_count_key))
                                        unpacker = Unpacker(use_list=False)
                                        unpacker.feed(alerter_sent_count_key_data)
                                        raw_alerts_sent = list(unpacker)
                                        alerts_sent = int(raw_alerts_sent[0])
                                        if ENABLE_BOUNDARY_DEBUG:
                                            logger.info("debug :: alerter %s alerts sent %s " % (str(alerter), str(alerts_sent)))
                                except:
                                    logger.info("No key set - %s" % alerter_sent_count_key)
                                    alerts_sent = 0
                                    send_alert = True
                                    if ENABLE_BOUNDARY_DEBUG:
                                        logger.info("debug :: alerts_sent set to %s" % str(alerts_sent))
                                        logger.info("debug :: send_alert set to %s" % str(send_alert))

                                if alerts_sent < alerter_limit:
                                    send_alert = True
                                    if ENABLE_BOUNDARY_DEBUG:
                                        logger.info("debug :: alerts_sent %s is less than alerter_limit %s" % (str(alerts_sent), str(alerter_limit)))
                                        logger.info("debug :: send_alert set to %s" % str(send_alert))

                            # @added 20210801 - Feature #4214: alert.paused
                            alert_paused = False
                            try:
                                cache_key = 'alert.paused.%s.%s' % (alerter, base_name)
                                # @added 20230220 - Feature #4854: boundary - labelled_metrics
                                #                   Task #2732: Prometheus to Skyline
                                #                   Branch #4300: prometheus
                                if labelled_metric:
                                    cache_key = 'alert.paused.%s.%s' % (alerter, labelled_metric)

                                alert_paused = self.redis_conn_decoded.get(cache_key)
                            except Exception as e:
                                logger.error('error :: alert_paused check failed: %s' % str(e))
                            if alert_paused:
                                send_alert = False
                                logger.info('alert_paused for %s %s until %s' % (
                                    alerter, base_name, str(alert_paused)))

                            # Send alert
                            alerter_alert_sent = False
                            if send_alert:
                                cache_key = 'last_alert.boundary.%s.%s.%s' % (alerter, base_name, algorithm)
                                # @added 20230220 - Feature #4854: boundary - labelled_metrics
                                #                   Task #2732: Prometheus to Skyline
                                #                   Branch #4300: prometheus
                                if labelled_metric:
                                    cache_key = 'last_alert.boundary.%s.%s.%s' % (alerter, labelled_metric, algorithm)

                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info("debug :: checking cache_key - %s" % cache_key)
                                try:
                                    last_alert = self.redis_conn.get(cache_key)
                                    if not last_alert:
                                        try:
                                            self.redis_conn.setex(cache_key, int(anomalous_metric[2]), packb(int(anomalous_metric[0])))
                                            if ENABLE_BOUNDARY_DEBUG:
                                                logger.debug('debug :: key setex OK - %s' % (cache_key))
                                            # @modified 20200122 - Feature #3396: http_alerter
                                            # Add the metric timestamp for the http_alerter resend queue
                                            # trigger_alert(alerter, datapoint, base_name, expiration_time, metric_trigger, algorithm)
                                            # @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
                                            trigger_alert(alerter, datapoint, base_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold)
                                            logger.info('alert sent :: %s - %s - via %s - %s %s %s times' % (
                                                base_name, str(datapoint), alerter, algorithm, str(metric_trigger), str(alert_threshold)))
                                            trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold)
                                            # logger.info('alert sent :: %s - %s - via syslog - %s' % (base_name, datapoint, algorithm))
                                            logger.info('alert sent :: %s - %s - via syslog - %s %s %s times' % (
                                                base_name, str(datapoint), algorithm, str(metric_trigger), str(alert_threshold)))
                                            alerter_alert_sent = True
                                        except Exception as e:
                                            logger.error('error :: alert failed :: %s - %s - via %s - %s' % (base_name, str(datapoint), alerter, algorithm))
                                            logger.error('error :: could not send alert: %s' % str(e))
                                            trigger_alert('syslog', datapoint, base_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold)
                                    else:
                                        if ENABLE_BOUNDARY_DEBUG:
                                            logger.debug("debug :: cache_key exists not alerting via %s for %s is less than alerter_limit %s" % (
                                                alerter, cache_key, str(alert_threshold)))
                                        # @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
                                        # trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold)
                                        trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm, metric_timestamp, times_seen)
                                        # logger.info('alert sent :: %s - %s - via syslog - %s' % (base_name, str(datapoint), algorithm))
                                        logger.info('alert sent :: %s - %s - via syslog - %s %s %s times' % (
                                            # base_name, str(datapoint), algorithm, str(metric_trigger), str(alert_threshold)))
                                            base_name, str(datapoint), algorithm, str(metric_trigger), str(times_seen)))
                                except:
                                    # @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
                                    trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold)
                                    # logger.info('alert sent :: %s - %s - via syslog - %s' % (base_name, datapoint, algorithm))
                                    logger.info('alert sent :: %s - %s - via syslog - %s %s %s times' % (
                                        base_name, str(datapoint), algorithm, str(metric_trigger), str(alert_threshold)))
                            else:
                                # trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold)
                                trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm, metric_timestamp, times_seen)
                                # logger.info('alert sent :: %s - %s - via syslog - %s' % (base_name, datapoint, algorithm))
                                logger.info('alert sent :: %s - %s - via syslog - %s %s %s times' % (
                                    # base_name, str(datapoint), algorithm, str(metric_trigger), str(alert_threshold)))
                                    base_name, str(datapoint), algorithm, str(metric_trigger), str(times_seen)))
                            # Update the alerts sent for the alerter cache key,
                            # to allow for alert limiting
                            if alerter_alert_sent and alerter_limit_set:
                                try:
                                    alerter_sent_count_key = 'alerts_sent.%s' % (alerter)
                                    new_alerts_sent = int(alerts_sent) + 1
                                    self.redis_conn.setex(alerter_sent_count_key, alerter_expiration_time, packb(int(new_alerts_sent)))
                                    logger.info('set %s - %s' % (alerter_sent_count_key, str(new_alerts_sent)))
                                except:
                                    logger.error('error :: failed to set %s - %s' % (alerter_sent_count_key, str(new_alerts_sent)))
                    else:
                        # Always alert to syslog, even if alert_threshold is not
                        # breached or if send_alert is not True
                        # @modified 20201214 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
                        # trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold)
                        trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm, metric_timestamp, times_seen)
                        # logger.info('alert sent :: %s - %s - via syslog - %s' % (base_name, datapoint, algorithm))
                        logger.info('alert sent :: %s - %s - via syslog - %s %s %s times' % (
                            # base_name, str(datapoint), algorithm, str(metric_trigger), str(alert_threshold)))
                            base_name, str(datapoint), algorithm, str(metric_trigger), str(times_seen)))

                    # @added 20171216 - Task #2236: Change Boundary to only send to Panorama on alert
                    # Remove tmp_panaroma_anomaly_file
                    tmp_panaroma_anomaly_file = '%s/%s.%s.%s.panorama_anomaly.txt' % (
                        # @modified 20171228 - Task #2236: Change Boundary to only send to Panorama on alert
                        # Added algorithm
                        settings.SKYLINE_TMP_DIR, str(added_at), str(algorithm),
                        base_name)

                    # @added 20230220 - Feature #4854: boundary - labelled_metrics
                    #                   Task #2732: Prometheus to Skyline
                    #                   Branch #4300: prometheus
                    if labelled_metric:
                        tmp_panaroma_anomaly_file = '%s/%s.%s.%s.panorama_anomaly.txt' % (
                            settings.SKYLINE_TMP_DIR, str(added_at), str(algorithm),
                            labelled_metric)

                    if os.path.isfile(tmp_panaroma_anomaly_file):
                        try:
                            os.remove(str(tmp_panaroma_anomaly_file))
                            logger.info('removed - %s' % str(tmp_panaroma_anomaly_file))
                        except OSError:
                            pass

            # Write anomalous_metrics to static webapp directory
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # if len(self.anomalous_metrics) > 0:
            if len(boundary_anomalous_metrics) > 0:
                filename = path.abspath(path.join(path.dirname(__file__), '..', settings.ANOMALY_DUMP))
                with open(filename, 'w') as fh:
                    # Make it JSONP with a handle_data() function
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # anomalous_metrics = list(self.anomalous_metrics)
                    anomalous_metrics = boundary_anomalous_metrics
                    anomalous_metrics.sort(key=operator.itemgetter(1))
                    fh.write('handle_data(%s)' % anomalous_metrics)

            # @added 20200121 - Feature #3396: http_alerter
            full_resend_queue = []
            resend_queue = []
            try:
                redis_set = 'boundary.http_alerter.queue'
                try:
                    # @modified 20221102 - Bug #4720: dotted_representation breaking alert resend_queue
                    #                      Feature #4652: http_alerter - dotted_representation
                    # Change to a hash
                    # resend_queue = list(self.redis_conn_decoded.smembers(redis_set))
                    resend_queue = []
                    if resend_queue:
                        logger.info('%s items in the %s Redis set' % (str(len(resend_queue)), redis_set))
                    else:
                        logger.info('0 items in the %s Redis set' % (redis_set))
                except Exception as e:
                    logger.error('error :: could not determine http_alerter item from Redis set %s - %s' % (redis_set, e))
                    resend_queue = []
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get %s.http_alerter.queue from Redis' % skyline_app)
                resend_queue = []

            # @added 20221102 - Bug #4720: dotted_representation breaking alert resend_queue
            #                   Feature #4652: http_alerter - dotted_representation
            # Change to a hash
            resend_queue_dict = {}
            try:
                redis_alert_queue_hash = 'boundary.http_alerter.queue.hash'
                try:
                    resend_queue_dict = self.redis_conn_decoded.hgetall(redis_alert_queue_hash)
                    if resend_queue_dict:
                        logger.info('%s items in the %s Redis hash' % (str(len(resend_queue_dict)), redis_alert_queue_hash))
                    else:
                        logger.info('0 items in the %s Redis hash' % redis_alert_queue_hash)
                except Exception as err:
                    logger.error('error :: could not determine http_alerter item from Redis hash %s - %s' % (
                        redis_alert_queue_hash, err))
                    resend_queue_dict = {}
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get boundary.http_alerter.queue.hash from Redis- %s' % err)
                resend_queue_dict = {}

            if resend_queue:
                try:
                    http_alerters_down = []
                    for index, resend_item in enumerate(full_resend_queue):
                        resend_item_list = literal_eval(resend_item)
                        # data = [alerter, datapoint, metric_name, expiration_time, metric_trigger, algorithm, metric_timestamp, str(metric_alert_dict)]
                        alerter = str(resend_item_list[0])
                        datapoint = float(resend_item_list[1])
                        metric_name = str(resend_item_list[2])
                        expiration_time = int(resend_item_list[3])
                        metric_trigger = float(resend_item_list[4])
                        algorithm = str(resend_item_list[5])
                        metric_timestamp = int(resend_item_list[6])
                        metric_alert_dict = literal_eval(resend_item_list[7])

                        # To ensure that Boundary does not loop through every alert in the queue
                        # for an alerter_endpoint, if the alerter_endpoint is down and wait for
                        # the connect timeout on each one, if an alerter_endpoint fails a Redis
                        # key is created to check against to see if the alerter_endpoint is down
                        alerter_name = alerter
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
                            if alerter_endpoint_cache_key not in http_alerters_down:
                                http_alerters_down.append(alerter_endpoint_cache_key)
                                logger.info('%s Redis exists not alerting for any alerts for this endpoint' % str(alerter_endpoint_cache_key))
                            continue
                        logger.info('resend_queue item :: %s' % (str(resend_item_list)))
                        try:
                            trigger_alert(alerter, datapoint, metric_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold)
                            logger.info('trigger_alert :: %s resend %s' % (
                                str(resend_item_list), str(metric_alert_dict)))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to trigger_alert for resend queue item')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to parse resend_queue')

            if resend_queue_dict:
                try:
                    http_alerters_down = []
                    for resend_item_key in list(resend_queue_dict.keys()):
                        resend_item_list = literal_eval(resend_queue_dict[resend_item_key])
                        # data = [alerter, datapoint, metric_name, expiration_time, metric_trigger, algorithm, metric_timestamp, str(metric_alert_dict)]
                        alerter = resend_item_list[0]
                        datapoint = resend_item_list[1]
                        metric_name = resend_item_list[2]
                        expiration_time = resend_item_list[3]
                        metric_trigger = resend_item_list[4]
                        algorithm = resend_item_list[5]
                        metric_timestamp = resend_item_list[6]
                        metric_alert_dict = resend_item_list[7]

                        # To ensure that Boundary does not loop through every alert in the queue
                        # for an alerter_endpoint, if the alerter_endpoint is down and wait for
                        # the connect timeout on each one, if an alerter_endpoint fails a Redis
                        # key is created to check against to see if the alerter_endpoint is down
                        alerter_name = alerter
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
                            if alerter_endpoint_cache_key not in http_alerters_down:
                                http_alerters_down.append(alerter_endpoint_cache_key)
                                logger.info('%s Redis exists not alerting for any alerts for this endpoint' % str(alerter_endpoint_cache_key))
                            continue
                        logger.info('resend_queue item :: %s' % (str(resend_item_list)))
                        try:
                            trigger_alert(alerter, datapoint, metric_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold)
                            logger.info('trigger_alert :: %s resend %s' % (
                                str(resend_item_list), str(metric_alert_dict)))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to trigger_alert for resend queue item')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to parse resend_queue')

            run_time = time() - now
            total_metrics = str(len(boundary_metrics))
            total_analyzed = str(len(boundary_metrics) - sum(exceptions.values()))
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # total_anomalies = str(len(self.anomalous_metrics))
            total_anomalies = str(len(boundary_anomalous_metrics))

            # Log progress
            logger.info('seconds to run    :: %.2f' % run_time)
            logger.info('total metrics     :: %s' % total_metrics)
            logger.info('total analyzed    :: %s' % total_analyzed)
            logger.info('total anomalies   :: %s' % total_anomalies)
            logger.info('exception stats   :: %s' % exceptions)
            logger.info('anomaly breakdown :: %s' % anomaly_breakdown)

            # Log to Graphite
            graphite_run_time = '%.2f' % run_time
            send_metric_name = skyline_app_graphite_namespace + '.run_time'
            try:
                send_graphite_metric(self, skyline_app, send_metric_name, graphite_run_time)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: send_graphite_metric failed - %s' % err)

            send_metric_name = skyline_app_graphite_namespace + '.total_analyzed'
            send_graphite_metric(self, skyline_app, send_metric_name, total_analyzed)

            send_metric_name = skyline_app_graphite_namespace + '.total_anomalies'
            send_graphite_metric(self, skyline_app, send_metric_name, total_anomalies)

            send_metric_name = skyline_app_graphite_namespace + '.total_metrics'
            send_graphite_metric(self, skyline_app, send_metric_name, total_metrics)
            for key, value in exceptions.items():
                send_metric_name = '%s.exceptions.%s' % (skyline_app_graphite_namespace, key)
                send_graphite_metric(self, skyline_app, send_metric_name, str(value))
            for key, value in anomaly_breakdown.items():
                send_metric_name = '%s.anomaly_breakdown.%s' % (skyline_app_graphite_namespace, key)
                send_graphite_metric(self, skyline_app, send_metric_name, str(value))

            # Check canary metric
            raw_series = self.redis_conn.get(settings.FULL_NAMESPACE + settings.CANARY_METRIC)
            if raw_series is not None:
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)

                # @added 20200507 - Feature #3532: Sort all time series
                # To ensure that there are no unordered timestamps in the time
                # series which are artefacts of the collector or carbon-relay, sort
                # all time series by timestamp before analysis.
                original_timeseries = timeseries
                if original_timeseries:
                    timeseries = sort_timeseries(original_timeseries)
                    del original_timeseries

                time_human = (timeseries[-1][0] - timeseries[0][0]) / 3600
                projected = 24 * (time() - now) / time_human

                logger.info('canary duration   :: %.2f' % time_human)
                send_metric_name = skyline_app_graphite_namespace + '.duration'
                send_graphite_metric(self, skyline_app, send_metric_name, str(time_human))

                send_metric_name = skyline_app_graphite_namespace + '.projected'
                send_graphite_metric(self, skyline_app, send_metric_name, str(projected))

            # Reset counters
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # self.anomalous_metrics[:] = []
            try:
                self.redis_conn.delete('boundary.anomalous_metrics')
            except:
                logger.info('failed to delete boundary.anomalous_metrics Redis set')

            # @added 20171214 - Bug #2232: Expiry boundary last_seen keys appropriately
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # self.not_anomalous_metrics[:] = []
            try:
                self.redis_conn.delete('boundary.not_anomalous_metrics')
            except:
                logger.info('failed to delete boundary.not_anomalous_metrics Redis set')

            # Only run once per
            process_runtime = time() - now
            try:
                boundary_optimum_run_duration = settings.BOUNDARY_OPTIMUM_RUN_DURATION
            except:
                boundary_optimum_run_duration = 60

            if process_runtime < boundary_optimum_run_duration:
                sleep_for = (boundary_optimum_run_duration - process_runtime)
                logger.info('sleeping %.2f for seconds' % sleep_for)
                sleep(sleep_for)
