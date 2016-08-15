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
import os
from os import path, kill, getpid
from math import ceil
import traceback
import operator
import re
from sys import version_info
import os.path
import resource

import settings
from skyline_functions import send_graphite_metric, write_data_to_file

from alerters import trigger_alert
from algorithms import run_selected_algorithm
from algorithm_exceptions import *

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

skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)

LOCAL_DEBUG = False


class Analyzer(Thread):
    """
    The Analyzer class which controls the analyzer thread and spawned processes.
    """

    def __init__(self, parent_pid):
        """
        Initialize the Analyzer

        Create the :obj:`self.anomalous_metrics` list

        Create the :obj:`self.exceptions_q` queue

        Create the :obj:`self.anomaly_breakdown_q` queue

        """
        super(Analyzer, self).__init__()
        self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        self.anomalous_metrics = Manager().list()
        self.exceptions_q = Queue()
        self.anomaly_breakdown_q = Queue()
        # @modified 20160813 - Bug #1558: Memory leak in Analyzer
        # Not used
        # self.mirage_metrics = Manager().list()

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            exit(0)

    def spawn_alerter_process(self, alert, metric):
        """
        Spawn a process to trigger an alert.  This is used by smtp alerters so
        that matplotlib objects are cleared down and the alerter cannot create
        a memory leak in this manner and plt.savefig keeps the object in memory
        until the process terminates.  Seeing as data is being surfaced and
        processed in the alert_smtp context, multiprocessing the alert creation
        and handling prevents any memory leaks in the parent.
        # @added 20160814 - Bug #1558: Memory leak in Analyzer
        #                   Issue #21 Memory leak in Analyzer
        # https://github.com/earthgecko/skyline/issues/21
        """

        trigger_alert(alert, metric)

    def spin_process(self, i, unique_metrics):
        """
        Assign a bunch of metrics for a process to analyze.

        Multiple get the assigned_metrics to the process from Redis.

        For each metric:

        - unpack the `raw_timeseries` for the metric.
        - Analyse each timeseries against `ALGORITHMS` to determine if it is
          anomalous.
        - If anomalous add it to the :obj:`self.anomalous_metrics` list
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

        # @modified 20160801 - Adding additional exception handling to Analyzer
        # Check the unique_metrics list is valid
        try:
            len(unique_metrics)
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
            logger.error('error :: failed to get assigned_metrics from Redis')
            logger.info(traceback.format_exc())

        # Make process-specific dicts
        exceptions = defaultdict(int)
        anomaly_breakdown = defaultdict(int)

        # @added 20160803 - Adding additional exception handling to Analyzer
        if raw_assigned_failed:
            return

        # Distill timeseries strings into lists
        for i, metric_name in enumerate(assigned_metrics):
            self.check_if_parent_is_alive()

            try:
                raw_series = raw_assigned[i]
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)
            except:
                timeseries = []

            try:
                anomalous, ensemble, datapoint = run_selected_algorithm(timeseries, metric_name)

                # If it's anomalous, add it to list
                if anomalous:
                    base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    metric = [datapoint, base_name]
                    self.anomalous_metrics.append(metric)

                    # Get the anomaly breakdown - who returned True?
                    triggered_algorithms = []
                    for index, value in enumerate(ensemble):
                        if value:
                            algorithm = settings.ALGORITHMS[index]
                            anomaly_breakdown[algorithm] += 1
                            triggered_algorithms.append(algorithm)

                    # If Crucible or Panorama are enabled determine details
                    determine_anomaly_details = False
                    if settings.ENABLE_CRUCIBLE and settings.ANALYZER_CRUCIBLE_ENABLED:
                        determine_anomaly_details = True
                    if settings.PANORAMA_ENABLED:
                        determine_anomaly_details = True

                    if determine_anomaly_details:
                        metric_timestamp = str(int(timeseries[-1][0]))
                        from_timestamp = str(int(timeseries[1][0]))
                        timeseries_dir = base_name.replace('.', '/')

                    # If Panorama is enabled - create a Panorama check
                    if settings.PANORAMA_ENABLED:
                        if not os.path.exists(settings.PANORAMA_CHECK_PATH):
                            if python_version == 2:
                                mode_arg = int('0755')
                            if python_version == 3:
                                mode_arg = mode=0o755
                            os.makedirs(settings.PANORAMA_CHECK_PATH, mode_arg)

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
                            base_name)
                        try:
                            write_data_to_file(
                                skyline_app, panaroma_anomaly_file, 'w',
                                panaroma_anomaly_data)
                            logger.info('added panorama anomaly file :: %s' % (panaroma_anomaly_file))
                        except:
                            logger.error('error :: failed to add panorama anomaly file :: %s' % (panaroma_anomaly_file))
                            logger.info(traceback.format_exc())

                    # If Crucible is enabled - save timeseries and create a
                    # Crucible check
                    if settings.ENABLE_CRUCIBLE and settings.ANALYZER_CRUCIBLE_ENABLED:
                        crucible_anomaly_dir = settings.CRUCIBLE_DATA_FOLDER + '/' + timeseries_dir + '/' + metric_timestamp
                        if not os.path.exists(crucible_anomaly_dir):
                            if python_version == 2:
                                mode_arg = int('0755')
                            if python_version == 3:
                                mode_arg = mode=0o755
                            os.makedirs(crucible_anomaly_dir, mode_arg)

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

                        # Create an anomaly file with details about the anomaly
                        crucible_anomaly_file = '%s/%s.txt' % (crucible_anomaly_dir, base_name)
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
                        crucible_check_file = '%s/%s.%s.txt' % (settings.CRUCIBLE_CHECK_PATH, metric_timestamp, base_name)
                        try:
                            write_data_to_file(
                                skyline_app, crucible_check_file, 'w',
                                crucible_anomaly_data)
                            logger.info('added crucible check :: %s,%s' % (base_name, metric_timestamp))
                        except:
                            logger.error('error :: failed to add crucible check file :: %s' % (crucible_check_file))
                            logger.info(traceback.format_exc())

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
                logger.info(traceback.format_exc())

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

        if not os.path.exists(settings.SKYLINE_TMP_DIR):
            if python_version == 2:
                mode_arg = int('0755')
            if python_version == 3:
                mode_arg = mode=0o755
            # @modified 20160803 - Adding additional exception handling to Analyzer
            try:
                os.makedirs(settings.SKYLINE_TMP_DIR, mode_arg)
            except:
                logger.error('error :: failed to create %s' % settings.SKYLINE_TMP_DIR)
                logger.info(traceback.format_exc())

        def smtp_trigger_alert(alert, metric):
            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            try:
                p = Process(target=self.spawn_alerter_process, args=(alert, metric))
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
                    self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                except:
                    logger.error('error :: Analyzer cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                    logger.info(traceback.format_exc())

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
                unique_metrics = list(self.redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
            except:
                logger.error('error :: Analyzer could not get the unique_metrics list from Redis')
                logger.info(traceback.format_exc())
                sleep(10)
                continue

            if len(unique_metrics) == 0:
                logger.info('no metrics in redis. try adding some - see README')
                sleep(10)
                continue

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run after unique_metrics: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

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
                    p.terminate()
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

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run before alerts: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Send alerts
            if settings.ENABLE_ALERTS:
                for alert in settings.ALERTS:
                    for metric in self.anomalous_metrics:
                        ALERT_MATCH_PATTERN = alert[0]
                        METRIC_PATTERN = metric[1]
                        pattern_match = False
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
                            continue

                        mirage_metric = False
                        analyzer_metric = True
                        cache_key = 'last_alert.%s.%s' % (alert[1], metric[1])

                        if settings.ENABLE_MIRAGE:
                            try:
                                SECOND_ORDER_RESOLUTION_FULL_DURATION = alert[3]
                                mirage_metric = True
                                analyzer_metric = False
                                logger.info(
                                    'mirage check :: %s at %s hours - matched by %s' %
                                    (metric[1],
                                        str(SECOND_ORDER_RESOLUTION_FULL_DURATION),
                                        matched_by))
                            except:
                                mirage_metric = False
                                if LOCAL_DEBUG:
                                    logger.info('debug :: not Mirage metric - %s' % metric[1])

                        if mirage_metric:
                            # Write anomalous metric to test at second
                            # order resolution by crucible to the check
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
                                logger.error('error :: failed to add mirage.metrics Redis key')

                            metric_timestamp = int(time())
                            anomaly_check_file = '%s/%s.%s.txt' % (settings.MIRAGE_CHECK_PATH, metric_timestamp, metric[1])
                            with open(anomaly_check_file, 'w') as fh:
                                # metric_name, anomalous datapoint, hours to resolve, timestamp
                                fh.write('metric = "%s"\nvalue = "%s"\nhours_to_resolve = "%s"\nmetric_timestamp = "%s"\n' % (metric[1], metric[0], alert[3], metric_timestamp))
                            if LOCAL_DEBUG:
                                logger.info(
                                    'debug :: Memory usage in run after writing mirage check file: %s (kb)' %
                                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                            if python_version == 2:
                                mode_arg = int('0644')
                            if python_version == 3:
                                mode_arg = '0o644'
                            os.chmod(anomaly_check_file, mode_arg)
                            if LOCAL_DEBUG:
                                logger.info(
                                    'debug :: Memory usage in run after chmod mirage check file: %s (kb)' %
                                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

                            logger.info('added mirage check :: %s,%s,%s' % (metric[1], metric[0], alert[3]))
                            # Alert for analyzer if enabled
                            if settings.ENABLE_FULL_DURATION_ALERTS:
                                self.redis_conn.setex(cache_key, alert[2], packb(metric[0]))
                                logger.info('triggering alert ENABLE_FULL_DURATION_ALERTS :: %s %s via %s' % (metric[1], metric[0], alert[1]))
                                try:
                                    if alert[1] != 'smtp':
                                        trigger_alert(alert, metric)
                                    else:
                                        smtp_trigger_alert(alert, metric)
                                except:
                                    logger.error(
                                        'error :: failed to trigger_alert ENABLE_FULL_DURATION_ALERTS :: %s %s via %s' %
                                        (metric[1], metric[0], alert[1]))
                            else:
                                if LOCAL_DEBUG:
                                    logger.info('debug :: ENABLE_FULL_DURATION_ALERTS not enabled')
                            continue

                        if analyzer_metric:
                            # @added 20160815 - Analyzer also alerting on Mirage metrics now #22
                            # If the metric has a dynamic mirage.metrics key,
                            # skip it
                            mirage_metric_cache_key = 'mirage.metrics.%s' % metric[1]
                            mirage_metric_key = False
                            if settings.ENABLE_MIRAGE:
                                try:
                                    mirage_metric_key = self.redis_conn.get(mirage_metric_cache_key)
                                except Exception as e:
                                    logger.error('error :: could not query Redis for mirage_metric_cache_key: %s' % e)

                            if mirage_metric_key:
                                if LOCAL_DEBUG:
                                    logger.info('debug :: mirage metric key exists, skipping %s' % metric[1])
                                continue

                            try:
                                last_alert = self.redis_conn.get(cache_key)
                            except Exception as e:
                                logger.error('error :: could not query Redis for cache_key: %s' % e)
                                continue

                            if last_alert:
                                continue

                            try:
                                self.redis_conn.setex(cache_key, alert[2], packb(metric[0]))
                            except:
                                logger.error('error :: failed to set alert cache key :: %s' % (cache_key))
                            logger.info(
                                'triggering alert :: %s %s via %s - matched by %s' %
                                (metric[1], metric[0], alert[1], matched_by))
                            if LOCAL_DEBUG:
                                logger.info(
                                    'debug :: Memory usage in run before triggering alert: %s (kb)' %
                                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                            try:
                                if alert[1] != 'smtp':
                                    trigger_alert(alert, metric)
                                else:
                                    smtp_trigger_alert(alert, metric)
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

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run after alerts: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Write anomalous_metrics to static webapp directory
            if len(self.anomalous_metrics) > 0:
                filename = path.abspath(path.join(path.dirname(__file__), '..', settings.ANOMALY_DUMP))
                with open(filename, 'w') as fh:
                    # Make it JSONP with a handle_data() function
                    anomalous_metrics = list(self.anomalous_metrics)
                    anomalous_metrics.sort(key=operator.itemgetter(1))
                    fh.write('handle_data(%s)' % anomalous_metrics)

            # Using count files rather that multiprocessing.Value to enable metrics for
            # metrics for algorithm run times, etc
            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run before algorithm run times: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

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

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run after algorithm run times: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            run_time = time() - now
            total_metrics = str(len(unique_metrics))
            total_analyzed = str(len(unique_metrics) - sum(exceptions.values()))
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
            try:
                raw_series = self.redis_conn.get(settings.FULL_NAMESPACE + settings.CANARY_METRIC)
            except:
                logger.error('error :: failed to get CANARY_METRIC from Redis')
                raw_series = None

            if raw_series is not None:
                try:
                    unpacker = Unpacker(use_list=False)
                    unpacker.feed(raw_series)
                    timeseries = list(unpacker)
                    time_human = (timeseries[-1][0] - timeseries[0][0]) / 3600
                    projected = 24 * (time() - now) / time_human
                    logger.info('canary duration   :: %.2f' % time_human)
                except:
                    logger.error(
                        'error :: failed to unpack/calculate canary duration')

                send_metric_name = skyline_app_graphite_namespace + '.duration'
                send_graphite_metric(skyline_app, send_metric_name, str(time_human))

                send_metric_name = skyline_app_graphite_namespace + '.projected'
                send_graphite_metric(skyline_app, send_metric_name, str(projected))

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage before reset counters: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Reset counters
            self.anomalous_metrics[:] = []
            unique_metrics = []
            raw_series = None
            del timeseries[:]
            unpacker = None
            # We del all variables that are floats as they become unique objects and
            # can result in what appears to be a memory leak, but in not, just the
            # way Python handles floats
            del time_human
            del projected
            del run_time

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage after reset counters: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                logger.info('debug :: Memory usage before sleep: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Sleep if it went too fast
            # if time() - now < 5:
            #    logger.info('sleeping due to low run time...')
            #    sleep(10)
            # @modified 20160504 - @earthgecko - development internal ref #1338, #1340)
            # Etsy's original for this was a value of 5 seconds which does
            # not make skyline Analyzer very efficient in terms of installations
            # where 100s of 1000s of metrics are being analyzed.  This lead to
            # Analyzer running over several metrics multiple time in a minute
            # and always working.  Therefore this was changed from if you took
            # less than 5 seconds to run only then sleep.  This behaviour
            # resulted in Analyzer analysing a few 1000 metrics in 9 seconds and
            # then doing it again and again in a single minute.  Therefore the
            # ANALYZER_OPTIMUM_RUN_DURATION setting was added to allow this to
            # self optimise in cases where skyline is NOT deployed to analyze
            # 100s of 1000s of metrics.  This relates to optimising performance
            # for any deployments in the few 1000s and 60 second resolution
            # area, e.g. smaller and local deployments.
            process_runtime = time() - now
            analyzer_optimum_run_duration = settings.ANALYZER_OPTIMUM_RUN_DURATION
            if process_runtime < analyzer_optimum_run_duration:
                sleep_for = (analyzer_optimum_run_duration - process_runtime)
                logger.info('sleeping for %.2f seconds due to low run time...' % sleep_for)
                sleep(sleep_for)
