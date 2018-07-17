from __future__ import division
import logging
try:
    from Queue import Empty
except:
    from queue import Empty
from redis import StrictRedis
from time import time, sleep
from threading import Thread
from collections import defaultdict
from multiprocessing import Process, Manager, Queue
from msgpack import Unpacker, packb
from os import path, kill, getpid
from math import ceil
import traceback
import operator
import re
import os
import errno

import sys
import os.path
import settings
# @modified 20171216 - Task #2236: Change Boundary to only send to Panorama on alert
# Added move_file
from skyline_functions import (
    send_graphite_metric, write_data_to_file, move_file)

from boundary_alerters import trigger_alert
from boundary_algorithms import run_selected_algorithm
from algorithm_exceptions import (TooShort, Stale, Boring)

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


class Boundary(Thread):
    def __init__(self, parent_pid):
        """
        Initialize the Boundary
        """
        super(Boundary, self).__init__()
        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        if settings.REDIS_PASSWORD:
            self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        else:
            self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        self.boundary_metrics = Manager().list()
        self.anomalous_metrics = Manager().list()
        self.exceptions_q = Queue()
        self.anomaly_breakdown_q = Queue()
        # @added 20171214 - Bug #2232: Expiry boundary last_seen keys appropriately
        self.not_anomalous_metrics = Manager().list()

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            exit(0)

    def unique_noHash(self, seq):
        seen = set()
        return [x for x in seq if str(x) not in seen and not seen.add(str(x))]

    # This is to make a dump directory in /tmp if ENABLE_BOUNDARY_DEBUG is True
    # for dumping the metric timeseries data into for debugging purposes
    def mkdir_p(self, path):
        try:
            os.makedirs(path)
            return True
        except OSError as exc:
            # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

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
            logger.info('debug :: printing assigned_metrics_and_algos')
            for assigned_metric_and_algo in assigned_metrics_and_algos:
                logger.info('debug :: assigned_metric_and_algo - %s' % str(assigned_metric_and_algo))

        # Compile assigned metrics
        assigned_metrics = []
        for i in assigned_metrics_and_algos:
            assigned_metrics.append(i[0])

        # unique unhashed things
        def unique_noHash(seq):
            seen = set()
            return [x for x in seq if str(x) not in seen and not seen.add(str(x))]

        unique_assigned_metrics = unique_noHash(assigned_metrics)

        if ENABLE_BOUNDARY_DEBUG:
            logger.info('debug :: unique_assigned_metrics - %s' % str(unique_assigned_metrics))
            logger.info('debug :: printing unique_assigned_metrics:')
            for unique_assigned_metric in unique_assigned_metrics:
                logger.info('debug :: unique_assigned_metric - %s' % str(unique_assigned_metric))

        # Check if this process is unnecessary
        if len(unique_assigned_metrics) == 0:
            return

        # Multi get series
        try:
            raw_assigned = self.redis_conn.mget(unique_assigned_metrics)
        except:
            logger.error('error :: failed to mget assigned_metrics from redis')
            return

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
            logger.info('debug :: boundary_algorithms - %s' % str(boundary_algorithms))

        discover_run_metrics = []

        # Distill metrics into a run list
        for i, metric_name, in enumerate(unique_assigned_metrics):
            self.check_if_parent_is_alive()

            try:
                if ENABLE_BOUNDARY_DEBUG:
                    logger.info('debug :: unpacking timeseries for %s - %s' % (metric_name, str(i)))
                raw_series = raw_assigned[i]
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)
            except Exception as e:
                exceptions['Other'] += 1
                logger.error('error :: redis data error: ' + traceback.format_exc())
                logger.error('error :: %e' % e)

            base_name = metric_name.replace(FULL_NAMESPACE, '', 1)

            # Determine the metrics BOUNDARY_METRICS metric tuple settings
            for metrick in BOUNDARY_METRICS:
                CHECK_MATCH_PATTERN = metrick[0]
                check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
                pattern_match = check_match_pattern.match(base_name)
                metric_pattern_matched = False
                if pattern_match:
                    metric_pattern_matched = True
                    algo_pattern_matched = False
                    for algo in boundary_algorithms:
                        for metric in BOUNDARY_METRICS:
                            CHECK_MATCH_PATTERN = metric[0]
                            check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
                            pattern_match = check_match_pattern.match(base_name)
                            if pattern_match:
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info("debug :: metric and algo pattern MATCHED - " + metric[0] + " | " + base_name + " | " + str(metric[1]))
                                metric_expiration_time = False
                                metric_min_average = False
                                metric_min_average_seconds = False
                                metric_trigger = False
                                algorithm = False
                                algo_pattern_matched = True
                                algorithm = metric[1]
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
                                    logger.info('debug :: added metric - %s, %s, %s, %s, %s, %s, %s, %s, %s' % (str(i), metric_name, str(metric_expiration_time), str(metric_min_average), str(metric_min_average_seconds), str(metric_trigger), str(alert_threshold), metric_alerters, algorithm))
                                discover_run_metrics.append([i, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger, alert_threshold, metric_alerters, algorithm])

        if ENABLE_BOUNDARY_DEBUG:
            logger.info('debug :: printing discover_run_metrics')
            for discover_run_metric in discover_run_metrics:
                logger.info('debug :: discover_run_metrics - %s' % str(discover_run_metric))
            logger.info('debug :: build unique boundary metrics to analyze')

        # Determine the unique set of metrics to run
        run_metrics = unique_noHash(discover_run_metrics)

        if ENABLE_BOUNDARY_DEBUG:
            logger.info('debug :: printing run_metrics')
            for run_metric in run_metrics:
                logger.info('debug :: run_metrics - %s' % str(run_metric))

        # Distill timeseries strings and submit to run_selected_algorithm
        for metric_and_algo in run_metrics:
            self.check_if_parent_is_alive()

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
                algorithm = metric_and_algo[8]

                if ENABLE_BOUNDARY_DEBUG:
                    logger.info('debug :: unpacking timeseries for %s - %s' % (metric_name, str(raw_assigned_id)))

                raw_series = raw_assigned[metric_and_algo[0]]
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)

                if ENABLE_BOUNDARY_DEBUG:
                    logger.info('debug :: unpacked OK - %s - %s' % (metric_name, str(raw_assigned_id)))

                autoaggregate = False
                autoaggregate_value = 0

                # Determine if the namespace is to be aggregated
                if BOUNDARY_AUTOAGGRERATION:
                    for autoaggregate_metric in BOUNDARY_AUTOAGGRERATION_METRICS:
                        autoaggregate = False
                        autoaggregate_value = 0
                        CHECK_MATCH_PATTERN = autoaggregate_metric[0]
                        base_name = metric_name.replace(FULL_NAMESPACE, '', 1)
                        check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
                        pattern_match = check_match_pattern.match(base_name)
                        if pattern_match:
                            autoaggregate = True
                            autoaggregate_value = autoaggregate_metric[1]

                if ENABLE_BOUNDARY_DEBUG:
                    logger.info('debug :: BOUNDARY_AUTOAGGRERATION passed - %s - %s' % (metric_name, str(autoaggregate)))

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
                    timeseries_dump_dir = "/tmp/skyline/boundary/" + algorithm  # nosec
                    self.mkdir_p(timeseries_dump_dir)
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
                        CHECK_MATCH_PATTERN = metric_tuple[0]
                        check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
                        pattern_match = check_match_pattern.match(base_name)
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
                    logger.info('WOULD RUN run_selected_algorithm = %s' % run_tupple)

                if run_tupple:
                    # Submit the timeseries and settings to run_selected_algorithm
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
                    if ENABLE_BOUNDARY_DEBUG:
                        logger.info('debug :: analysed - %s' % (metric_name))
                    # @added 20171214 - Bug #2232: Expiry boundary last_seen keys appropriately
                    # If it's not anomalous, add it to list
                    if not anomalous:
                        not_anomalous_metric = [datapoint, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger, alert_threshold, metric_alerters, algorithm]
                        self.not_anomalous_metrics.append(not_anomalous_metric)
                else:
                    anomalous = False
                    if ENABLE_BOUNDARY_DEBUG:
                        logger.info('debug :: more unique metric tuple not analysed - %s' % (metric_name))

                # If it's anomalous, add it to list
                if anomalous:
                    anomalous_metric = [datapoint, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger, alert_threshold, metric_alerters, algorithm]
                    self.anomalous_metrics.append(anomalous_metric)
                    # Get the anomaly breakdown - who returned True?
                    triggered_algorithms = []
                    for index, value in enumerate(ensemble):
                        if value:
                            anomaly_breakdown[algorithm] += 1
                            triggered_algorithms.append(algorithm)

                    # If Crucible or Panorama are enabled determine details
                    determine_anomaly_details = False
                    if settings.ENABLE_CRUCIBLE and settings.BOUNDARY_CRUCIBLE_ENABLED:
                        determine_anomaly_details = True
                    if settings.PANORAMA_ENABLED:
                        determine_anomaly_details = True

                    if determine_anomaly_details:
                        metric_timestamp = str(int(timeseries[-1][0]))
                        from_timestamp = str(int(timeseries[1][0]))
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
                        source = 'graphite'
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
                            settings.SKYLINE_TMP_DIR, added_at, str(algorithm),
                            base_name)
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
                                mode_arg = int('0755')
                                os.makedirs(crucible_anomaly_dir, mode_arg)
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
                            os.chmod(crucible_anomaly_file, 0644)
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
                            os.chmod(json_file, 0644)
                        if python_version == 3:
                            os.chmod(json_file, mode=0o644)
                        logger.info('added crucible timeseries file :: %s/%s.json' % (crucible_anomaly_dir, base_name))

                        # Create a crucible check file
                        crucible_check_file = '%s/%s.%s.txt' % (settings.CRUCIBLE_CHECK_PATH, metric_timestamp, base_name)
                        with open(crucible_check_file, 'w') as fh:
                            fh.write(crucible_anomaly_data)
                        if python_version == 2:
                            os.chmod(crucible_check_file, 0644)
                        if python_version == 3:
                            os.chmod(crucible_check_file, mode=0o644)
                        logger.info('added crucible check :: %s,%s' % (base_name, metric_timestamp))

            # It could have been deleted by the Roomba
            except TypeError:
                exceptions['DeletedByRoomba'] += 1
            except TooShort:
                exceptions['TooShort'] += 1
            except Stale:
                exceptions['Stale'] += 1
            except Boring:
                exceptions['Boring'] += 1
            except:
                exceptions['Other'] += 1
                logger.info("exceptions['Other'] traceback follows:")
                logger.info(traceback.format_exc())

        # Add values to the queue so the parent process can collate
        for key, value in anomaly_breakdown.items():
            self.anomaly_breakdown_q.put((key, value))

        for key, value in exceptions.items():
            self.exceptions_q.put((key, value))

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
            logger.error('error :: bin/%s.d log management seems to have failed, continuing' % skyline_app)
            try:
                os.remove(skyline_app_loglock)
                logger.info('log lock file removed')
            except OSError:
                logger.error('error :: failed to remove %s, continuing' % skyline_app_loglock)
                pass
        else:
            logger.info('bin/%s.d log management done' % skyline_app)

        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except:
                logger.error('error :: skyline cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                sleep(10)
                # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                if settings.REDIS_PASSWORD:
                    self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                else:
                    self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                continue

            # Report app up
            self.redis_conn.setex(skyline_app, 120, now)

            # Discover unique metrics
            unique_metrics = list(self.redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))

            if len(unique_metrics) == 0:
                logger.info('no metrics in redis. try adding some - see README')
                sleep(10)
                continue

            # Reset boundary_metrics
            boundary_metrics = []

            # Build boundary metrics
            for metric_name in unique_metrics:
                for metric in BOUNDARY_METRICS:
                    CHECK_MATCH_PATTERN = metric[0]
                    check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
                    base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    pattern_match = check_match_pattern.match(base_name)
                    if pattern_match:
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.info('debug :: boundary metric - pattern MATCHED - ' + metric[0] + " | " + base_name)
                        boundary_metrics.append([metric_name, metric[1]])

            if ENABLE_BOUNDARY_DEBUG:
                logger.info('debug :: boundary metrics - ' + str(boundary_metrics))

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

            # @added 20171214 - Bug #2232: Expiry boundary last_seen keys appropriately
            # Expire keys
            if settings.BOUNDARY_ENABLE_ALERTS:
                for not_anomalous_metric in self.not_anomalous_metrics:
                    metric_name = not_anomalous_metric[1]
                    base_name = metric_name.replace(FULL_NAMESPACE, '', 1)
                    algorithm = not_anomalous_metric[8]
                    if ENABLE_BOUNDARY_DEBUG:
                        logger.info("debug :: not_anomalous_metric - " + str(not_anomalous_metric))
                    anomaly_cache_key_expiration_time = 1
                    # @modified 20171228 - Task #2236: Change Boundary to only send to Panorama on alert
                    # Wrapped in try - Added algorithm as it is required if the metric has
                    # multiple rules covering a number of algorithms
                    try:
                        anomaly_cache_key = 'anomaly_seen.%s.%s' % (algorithm, base_name)
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.info('debug :: anomaly_cache_key - anomaly_seen.%s.%s' % (algorithm, base_name))
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to determine string for anomaly_cache_key')
                        anomaly_cache_key = 'anomaly_seen.%s' % (base_name)
                    times_seen = 0
                    try:
                        self.redis_conn.setex(anomaly_cache_key, anomaly_cache_key_expiration_time, packb(int(times_seen)))
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.info('debug :: redis - anomaly_cache_key set OK - %s' % str(anomaly_cache_key))
                    except:
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.info('debug :: redis failed - anomaly_cache_key set failed - %s' % str(anomaly_cache_key))
                    # @added 20171216 - Task #2236: Change Boundary to only send to Panorama on alert
                    # Remove tmp_panaroma_anomaly_file
                    # @modified 20171228 - Task #2236: Change Boundary to only send to Panorama on alert
                    # Added algorithm as it is required if the metric has
                    # multiple rules covering a number of algorithms
                    tmp_panaroma_anomaly_file = '%s/%s.%s.%s.panorama_anomaly.txt' % (
                        settings.SKYLINE_TMP_DIR, added_at, algorithm, base_name)
                    if ENABLE_BOUNDARY_DEBUG:
                        logger.info('debug :: set tmp_panaroma_anomaly_file to - %s' % (str(tmp_panaroma_anomaly_file)))
                    if os.path.isfile(tmp_panaroma_anomaly_file):
                        try:
                            if ENABLE_BOUNDARY_DEBUG:
                                logger.info('debug :: removing tmp_panaroma_anomaly_file - %s' % (str(tmp_panaroma_anomaly_file)))
                            os.remove(str(tmp_panaroma_anomaly_file))
                        except OSError:
                            if ENABLE_BOUNDARY_DEBUG:
                                logger.info('debug :: error removing tmp_panaroma_anomaly_file - %s' % (str(tmp_panaroma_anomaly_file)))
                            pass

            # Send alerts
            if settings.BOUNDARY_ENABLE_ALERTS:
                for anomalous_metric in self.anomalous_metrics:
                    datapoint = str(anomalous_metric[0])
                    metric_name = anomalous_metric[1]
                    base_name = metric_name.replace(FULL_NAMESPACE, '', 1)
                    expiration_time = str(anomalous_metric[2])
                    metric_trigger = str(anomalous_metric[5])
                    alert_threshold = int(anomalous_metric[6])
                    metric_alerters = anomalous_metric[7]
                    algorithm = anomalous_metric[8]
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

                    if alert_threshold > 1:
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.info('debug :: alert_threshold - ' + str(alert_threshold))
                        anomaly_cache_key_count_set = False
                        anomaly_cache_key_expiration_time = (int(alert_threshold) + 1) * 60
                        anomaly_cache_key = 'anomaly_seen.%s.%s' % (algorithm, base_name)
                        try:
                            anomaly_cache_key_count = self.redis_conn.get(anomaly_cache_key)
                            if not anomaly_cache_key_count:
                                try:
                                    if ENABLE_BOUNDARY_DEBUG:
                                        logger.info('debug :: redis no anomaly_cache_key - ' + str(anomaly_cache_key))
                                    times_seen = 1
                                    if ENABLE_BOUNDARY_DEBUG:
                                        logger.info('debug :: redis setex anomaly_cache_key - ' + str(anomaly_cache_key))
                                    self.redis_conn.setex(anomaly_cache_key, anomaly_cache_key_expiration_time, packb(int(times_seen)))
                                    logger.info('set anomaly seen key :: %s seen %s' % (anomaly_cache_key, str(times_seen)))
                                except Exception as e:
                                    logger.error('error :: redis setex failed :: %s' % str(anomaly_cache_key))
                                    logger.error('error :: could not set key: %s' % e)
                            else:
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info('debug :: redis anomaly_cache_key retrieved OK - ' + str(anomaly_cache_key))
                                anomaly_cache_key_count_set = True
                        except:
                            if ENABLE_BOUNDARY_DEBUG:
                                logger.info('debug :: redis failed - anomaly_cache_key retrieval failed - ' + str(anomaly_cache_key))
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
                            logger.info('debug :: times_seen %s is greater than or equal to alert_threshold %s' % (str(times_seen), str(alert_threshold)))

                        # @added 20171216 - Task #2236: Change Boundary to only send to Panorama on alert
                        tmp_panaroma_anomaly_file = '%s/%s.%s.%s.panorama_anomaly.txt' % (
                            settings.SKYLINE_TMP_DIR, added_at,
                            # @modified 20171228 - Task #2236: Change Boundary to only send to Panorama on alert
                            # Added algorithm as it is required if the metric has
                            # multiple rules covering a number of algorithms
                            algorithm, base_name)
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.info('debug :: tmp_panaroma_anomaly_file - %s' % (str(tmp_panaroma_anomaly_file)))
                        if os.path.isfile(tmp_panaroma_anomaly_file):
                            panaroma_anomaly_file = '%s/%s.%s.txt' % (
                                settings.PANORAMA_CHECK_PATH, added_at, base_name)
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
                                    settings.PANORAMA_CHECK_PATH, added_at,
                                    algorithm, base_name)
                                os.rename(tmp_panaroma_anomaly_file_to_rename, panaroma_anomaly_file)
                            except:
                                logger.info(traceback.format_exc())
                                logger.error('error :: failed to rename tmp_panaroma_anomaly_filename to panaroma_anomaly_filename')
                        else:
                            logger.error('error :: tmp_panaroma_anomaly_file does not exist')

                        for alerter in metric_alerters.split("|"):
                            # Determine alerter limits
                            send_alert = False
                            alerts_sent = 0
                            if ENABLE_BOUNDARY_DEBUG:
                                logger.info('debug :: checking alerter - %s' % alerter)
                            try:
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info('debug :: determining alerter_expiration_time for settings')
                                alerter_expiration_time_setting = settings.BOUNDARY_ALERTER_OPTS['alerter_expiration_time'][alerter]
                                alerter_expiration_time = int(alerter_expiration_time_setting)
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info('debug :: determined alerter_expiration_time from settings - %s' % str(alerter_expiration_time))
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
                                            logger.info("debug :: redis alerter key retrieved, unpacking" + str(alerter_sent_count_key))
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

                            # Send alert
                            alerter_alert_sent = False
                            if send_alert:
                                cache_key = 'last_alert.boundary.%s.%s.%s' % (alerter, base_name, algorithm)
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info("debug :: checking cache_key - %s" % cache_key)
                                try:
                                    last_alert = self.redis_conn.get(cache_key)
                                    if not last_alert:
                                        try:
                                            self.redis_conn.setex(cache_key, int(anomalous_metric[2]), packb(int(anomalous_metric[0])))
                                            if ENABLE_BOUNDARY_DEBUG:
                                                logger.info('debug :: key setex OK - %s' % (cache_key))
                                            trigger_alert(alerter, datapoint, base_name, expiration_time, metric_trigger, algorithm)
                                            logger.info('alert sent :: %s - %s - via %s - %s' % (base_name, datapoint, alerter, algorithm))
                                            trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm)
                                            logger.info('alert sent :: %s - %s - via syslog - %s' % (base_name, datapoint, algorithm))
                                            alerter_alert_sent = True
                                        except Exception as e:
                                            logger.error('error :: alert failed :: %s - %s - via %s - %s' % (base_name, datapoint, alerter, algorithm))
                                            logger.error('error :: could not send alert: %s' % str(e))
                                            trigger_alert('syslog', datapoint, base_name, expiration_time, metric_trigger, algorithm)
                                    else:
                                        if ENABLE_BOUNDARY_DEBUG:
                                            logger.info("debug :: cache_key exists not alerting via %s for %s is less than alerter_limit %s" % (alerter, cache_key))
                                        trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm)
                                        logger.info('alert sent :: %s - %s - via syslog - %s' % (base_name, datapoint, algorithm))
                                except:
                                    trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm)
                                    logger.info('alert sent :: %s - %s - via syslog - %s' % (base_name, datapoint, algorithm))
                            else:
                                trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm)
                                logger.info('alert sent :: %s - %s - via syslog - %s' % (base_name, datapoint, algorithm))

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
                        trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm)
                        logger.info('alert sent :: %s - %s - via syslog - %s' % (base_name, datapoint, algorithm))

                    # @added 20171216 - Task #2236: Change Boundary to only send to Panorama on alert
                    # Remove tmp_panaroma_anomaly_file
                    tmp_panaroma_anomaly_file = '%s/%s.%s.%s.panorama_anomaly.txt' % (
                        # @modified 20171228 - Task #2236: Change Boundary to only send to Panorama on alert
                        # Added algorithm
                        settings.SKYLINE_TMP_DIR, added_at, algorithm, base_name)
                    if os.path.isfile(tmp_panaroma_anomaly_file):
                        try:
                            os.remove(str(tmp_panaroma_anomaly_file))
                            logger.info('removed - %s' % str(tmp_panaroma_anomaly_file))
                        except OSError:
                            pass

            # Write anomalous_metrics to static webapp directory
            if len(self.anomalous_metrics) > 0:
                filename = path.abspath(path.join(path.dirname(__file__), '..', settings.ANOMALY_DUMP))
                with open(filename, 'w') as fh:
                    # Make it JSONP with a handle_data() function
                    anomalous_metrics = list(self.anomalous_metrics)
                    anomalous_metrics.sort(key=operator.itemgetter(1))
                    fh.write('handle_data(%s)' % anomalous_metrics)

            run_time = time() - now
            total_metrics = str(len(boundary_metrics))
            total_analyzed = str(len(boundary_metrics) - sum(exceptions.values()))
            total_anomalies = str(len(self.anomalous_metrics))

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
            send_graphite_metric(skyline_app, send_metric_name, graphite_run_time)

            send_metric_name = skyline_app_graphite_namespace + '.total_analyzed'
            send_graphite_metric(skyline_app, send_metric_name, total_analyzed)

            send_metric_name = skyline_app_graphite_namespace + '.total_anomalies'
            send_graphite_metric(skyline_app, send_metric_name, total_anomalies)

            send_metric_name = skyline_app_graphite_namespace + '.total_metrics'
            send_graphite_metric(skyline_app, send_metric_name, total_metrics)
            for key, value in exceptions.items():
                send_metric_name = '%s.exceptions.%s' % (skyline_app_graphite_namespace, key)
                send_graphite_metric(skyline_app, send_metric_name, str(value))
            for key, value in anomaly_breakdown.items():
                send_metric_name = '%s.anomaly_breakdown.%s' % (skyline_app_graphite_namespace, key)
                send_graphite_metric(skyline_app, send_metric_name, str(value))

            # Check canary metric
            raw_series = self.redis_conn.get(settings.FULL_NAMESPACE + settings.CANARY_METRIC)
            if raw_series is not None:
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)
                time_human = (timeseries[-1][0] - timeseries[0][0]) / 3600
                projected = 24 * (time() - now) / time_human

                logger.info('canary duration   :: %.2f' % time_human)
                send_metric_name = skyline_app_graphite_namespace + '.duration'
                send_graphite_metric(skyline_app, send_metric_name, str(time_human))

                send_metric_name = skyline_app_graphite_namespace + '.projected'
                send_graphite_metric(skyline_app, send_metric_name, str(projected))

            # Reset counters
            self.anomalous_metrics[:] = []
            # @added 20171214 - Bug #2232: Expiry boundary last_seen keys appropriately
            self.not_anomalous_metrics[:] = []

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
