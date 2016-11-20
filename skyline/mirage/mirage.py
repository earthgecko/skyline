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
from msgpack import packb
from os import kill, getpid
import traceback
import re
# imports required for surfacing graphite JSON formatted timeseries for use in
# Mirage
import json
import sys
import requests
try:
    import urlparse
except ImportError:
    import urllib.parse
import os
import errno
import imp
from os import listdir
import datetime

import os.path
import resource

import settings
# @modified 20160922 - Branch #922: Ionosphere
# Added the send_anomalous_metric_to skyline_functions.py
from skyline_functions import write_data_to_file, load_metric_vars, fail_check, send_anomalous_metric_to

from mirage_alerters import trigger_alert
from negaters import trigger_negater
from mirage_algorithms import run_selected_algorithm
from algorithm_exceptions import *
from os.path import join, isfile

"""
ENABLE_MEMORY_PROFILING - DEVELOPMENT ONLY

# @added 20160806 - Bug #1558: Memory leak in Analyzer
# Added all the memory profiling blocks - mem_top, pympler, objgraph, gc
Garbage collection et al, should not be run in anything but development model,
therefore these variables are hard coded and not accessible via settings.py,
if you are in here reading this then knock yourself out.  gc and dump_garbage
can be useful for getting an idea about what all the objects in play are, but
garbage collection will just take longer and longer to run.

"""
LOCAL_DEBUG = False
ENABLE_MEMORY_PROFILING = False
garbage_collection_enabled = False

if ENABLE_MEMORY_PROFILING:
    # @added 20160806 - Bug #1558: Memory leak in Analyzer
    # As per http://stackoverflow.com/a/1641280
    # This got useable understandable data
    if garbage_collection_enabled:
        from gc import get_objects
        # Debug with garbage collection - http://code.activestate.com/recipes/65333/
        import gc

skyline_app = 'mirage'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

python_version = int(sys.version_info[0])

this_host = str(os.uname()[1])

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)


class Mirage(Thread):
    def __init__(self, parent_pid):
        """
        Initialize the Mirage
        """
        super(Mirage, self).__init__()
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        self.anomalous_metrics = Manager().list()
        self.mirage_exceptions_q = Queue()
        self.mirage_anomaly_breakdown_q = Queue()
        self.not_anomalous_metrics = Manager().list()
        self.metric_variables = Manager().list()

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            exit(0)

    def spawn_alerter_process(self, alert, metric, second_order_resolution_seconds):
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

        trigger_alert(alert, metric, second_order_resolution_seconds)

    def mkdir_p(self, path):
        try:
            os.makedirs(path)
            return True
        # Python >2.5
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def surface_graphite_metric_data(self, metric_name, graphite_from, graphite_until):

        # @added 20160803 - Unescaped Graphite target - https://github.com/earthgecko/skyline/issues/20
        #                   bug1546: Unescaped Graphite target
        new_metric_namespace = metric_name.replace(':', '\:')
        metric_namespace = new_metric_namespace.replace('(', '\(')
        metric_name = metric_namespace.replace(')', '\)')

        # We use absolute time so that if there is a lag in mirage the correct
        # timeseries data is still surfaced relevant to the anomalous datapoint
        # timestamp
        if settings.GRAPHITE_PORT != '':
            url = '%s://%s:%s/render/?from=%s&until=%s&target=%s&format=json' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                str(settings.GRAPHITE_PORT), graphite_from, graphite_until,
                metric_name)
        else:
            url = '%s://%s/render/?from=%s&until=%s&target=%s&format=json' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                graphite_from, graphite_until, metric_name)
        r = requests.get(url)
        js = r.json()
        datapoints = js[0]['datapoints']

        converted = []
        for datapoint in datapoints:
            try:
                new_datapoint = [float(datapoint[1]), float(datapoint[0])]
                converted.append(new_datapoint)
            except:
                continue

        parsed = urlparse.urlparse(url)
        target = urlparse.parse_qs(parsed.query)['target'][0]

        metric_data_folder = settings.MIRAGE_DATA_FOLDER + "/" + target
        self.mkdir_p(metric_data_folder)
        with open(metric_data_folder + "/" + target + '.json', 'w') as f:
            f.write(json.dumps(converted))
            f.close()
            return True

        return False

    def load_metric_vars(self, filename):
        if os.path.isfile(filename):
            f = open(filename)
            # @added 20160822 - Bug #1460: panorama check file fails
            # Do not make this global
            # global metric_vars
            try:
                metric_vars = imp.load_source('metric_vars', '', f)
                f.close()
                # return True
                return metric_vars
            except:
                logger.error('error :: failed to load metric_vars')
                logger.info(traceback.format_exc())

        return False

    def dump_garbage(self):
        """
        DEVELOPMENT ONLY

        # @added 20160806 - Bug #1558: Memory leak in Analyzer
        # Debug with garbage collection - http://code.activestate.com/recipes/65333/

        show us what's the garbage about
        """

        if ENABLE_MEMORY_PROFILING and garbage_collection_enabled:
            # force collection
            if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                logger.info('debug :: GARBAGE')
            try:
                gc.collect()
                gc_collect_ok = True
            except:
                logger.error('error :: gc.collect failed')
                logger.info(traceback.format_exc())
                gc_collect_ok = False

            if gc_collect_ok:
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info('debug :: GARBAGE OBJECTS')
                for x in gc.garbage:
                    s = str(x)
                    if len(s) > 80:
                        s = s[:80]
                    # print type(x), "\n  ", s
                    try:
                        log_string = type(x), "\n  ", s
                    except:
                        logger.error('error :: print x and s')
                        logger.info(traceback.format_exc())

                    # if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    #     logger.info(log_string)
        else:
            return None

    def spin_process(self, i, run_timestamp):
        """
        Assign a metric for a process to analyze.
        """

        # Discover metric to analyze
        metric_var_files = [f for f in listdir(settings.MIRAGE_CHECK_PATH) if isfile(join(settings.MIRAGE_CHECK_PATH, f))]

        # Check if this process is unnecessary
        if len(metric_var_files) == 0:
            return

        metric_var_files_sorted = sorted(metric_var_files)
        metric_check_file = '%s/%s' % (
            settings.MIRAGE_CHECK_PATH, str(metric_var_files_sorted[0]))

        # Load metric variables
        # @modified 20160822 - Bug #1460: panorama check file fails
        # Changed to panorama style skyline_functions load_metric_vars
        # self.load_metric_vars(metric_check_file)
        # Load and validate metric variables
        try:
            metric_vars = load_metric_vars(skyline_app, str(metric_check_file))
        except:
            logger.info(traceback.format_exc())
            logger.error('error :: failed to load metric variables from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        # Test metric variables
        if len(metric_vars.metric) == 0:
            return
        else:
            metric = metric_vars.metric
            metric_name = ['metric_name', metric_vars.metric]
            self.metric_variables.append(metric_name)
        if len(metric_vars.value) == 0:
            return
        else:
            metric_value = ['metric_value', metric_vars.value]
            self.metric_variables.append(metric_value)
        if len(metric_vars.hours_to_resolve) == 0:
            return
        else:
            hours_to_resolve = ['hours_to_resolve', metric_vars.hours_to_resolve]
            self.metric_variables.append(hours_to_resolve)
        if len(metric_vars.metric_timestamp) == 0:
            return
        else:
            metric_timestamp = ['metric_timestamp', metric_vars.metric_timestamp]
            self.metric_variables.append(metric_timestamp)

        # Ignore any metric check with a timestamp greater than 10 minutes ago
        int_metric_timestamp = int(metric_vars.metric_timestamp)
        int_run_timestamp = int(run_timestamp)
        metric_timestamp_age = int_run_timestamp - int_metric_timestamp
        if metric_timestamp_age > settings.MIRAGE_STALE_SECONDS:
            logger.info('stale check :: %s check request is %s seconds old - discarding' % (metric_vars.metric, metric_timestamp_age))
            # Remove metric check file
#            try:
#                os.remove(metric_check_file)
#            except OSError:
#                pass
#            return
            if os.path.exists(metric_check_file):
                os.remove(metric_check_file)
                logger.info('removed %s' % (metric_check_file))
            else:
                logger.info('could not remove %s' % (metric_check_file))

        # Calculate hours second order resolution to seconds
        second_order_resolution_seconds = int(metric_vars.hours_to_resolve) * 3600

        # Calculate graphite from and until parameters from the metric timestamp
        graphite_until = datetime.datetime.fromtimestamp(int(metric_vars.metric_timestamp)).strftime('%H:%M_%Y%m%d')
        int_second_order_resolution_seconds = int(second_order_resolution_seconds)
        second_resolution_timestamp = int_metric_timestamp - int_second_order_resolution_seconds
        graphite_from = datetime.datetime.fromtimestamp(int(second_resolution_timestamp)).strftime('%H:%M_%Y%m%d')

        # Remove any old json file related to the metric
        metric_json_file = '%s/%s/%s.json' % (
            settings.MIRAGE_DATA_FOLDER, str(metric_vars.metric),
            str(metric_vars.metric))
        try:
            os.remove(metric_json_file)
        except OSError:
            pass

        # Get data from graphite
        logger.info(
            'retrieve data :: surfacing %s timeseries from graphite for %s seconds' % (
                metric_vars.metric, second_order_resolution_seconds))
        self.surface_graphite_metric_data(metric_vars.metric, graphite_from, graphite_until)

        # Check there is a json timeseries file to test
        if not os.path.isfile(metric_json_file):
            logger.error(
                'error :: retrieve failed - failed to surface %s timeseries from graphite' % (
                    metric_vars.metric))
            # Remove metric check file
            try:
                os.remove(metric_check_file)
            except OSError:
                pass
            return
        else:
            logger.info('retrieved data :: for %s at %s seconds' % (
                metric_vars.metric, second_order_resolution_seconds))

        # Make process-specific dicts
        exceptions = defaultdict(int)
        anomaly_breakdown = defaultdict(int)

        self.check_if_parent_is_alive()

        with open((metric_json_file), 'r') as f:
            timeseries = json.loads(f.read())
            logger.info('data points surfaced :: %s' % (len(timeseries)))

        try:
            logger.info('analyzing :: %s at %s seconds' % (metric_vars.metric, second_order_resolution_seconds))
            anomalous, ensemble, datapoint = run_selected_algorithm(timeseries, metric_vars.metric, second_order_resolution_seconds)

            # If it's anomalous, add it to list
            if anomalous:
                base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
                anomalous_metric = [datapoint, base_name]
                self.anomalous_metrics.append(anomalous_metric)
                logger.info('anomaly detected  :: %s with %s' % (metric_vars.metric, metric_vars.value))
                # It runs so fast, this allows us to process 30 anomalies/min
                sleep(2)

                # Get the anomaly breakdown - who returned True?
                triggered_algorithms = []
                for index, value in enumerate(ensemble):
                    if value:
                        algorithm = settings.MIRAGE_ALGORITHMS[index]
                        anomaly_breakdown[algorithm] += 1
                        triggered_algorithms.append(algorithm)

                # If Crucible or Panorama are enabled determine details
                determine_anomaly_details = False
                if settings.ENABLE_CRUCIBLE and settings.MIRAGE_CRUCIBLE_ENABLED:
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
                           metric_timestamp, str(settings.MIRAGE_ALGORITHMS),
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

                # If crucible is enabled - save timeseries and create a
                # crucible check
                if settings.ENABLE_CRUCIBLE and settings.MIRAGE_CRUCIBLE_ENABLED:
                    metric_timestamp = str(int(timeseries[-1][0]))
                    from_timestamp = str(int(timeseries[1][0]))
                    timeseries_dir = base_name.replace('.', '/')
                    crucible_anomaly_dir = settings.CRUCIBLE_DATA_FOLDER + '/' + timeseries_dir + '/' + metric_timestamp
                    if not os.path.exists(crucible_anomaly_dir):
                        if python_version == 2:
                            mode_arg = int('0755')
                        if python_version == 3:
                            mode_arg = mode=0o755
                        os.makedirs(crucible_anomaly_dir, mode_arg)

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
                           metric_timestamp, str(settings.MIRAGE_ALGORITHMS),
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

                # @added 20160922 - Branch #922: Ionosphere
                # Also added the send_anomalous_metric_to skyline_functions.py
                # function
                if settings.IONOSPHERE_ENABLED:
                    send_anomalous_metric_to(
                        skyline_app, 'ionosphere', timeseries_dir,
                        metric_timestamp, base_name, str(datapoint),
                        from_timestamp, triggered_algorithms, timeseries)

            else:
                base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
                not_anomalous_metric = [datapoint, base_name]
                self.not_anomalous_metrics.append(not_anomalous_metric)
                logger.info('not anomalous     :: %s with %s' % (metric_vars.metric, metric_vars.value))

        # It could have been deleted by the Roomba
        except TypeError:
            exceptions['DeletedByRoomba'] += 1
            logger.info('exceptions        :: DeletedByRoomba')
        except TooShort:
            exceptions['TooShort'] += 1
            logger.info('exceptions        :: TooShort')
        except Stale:
            exceptions['Stale'] += 1
            logger.info('exceptions        :: Stale')
        except Boring:
            exceptions['Boring'] += 1
            logger.info('exceptions        :: Boring')
        except:
            exceptions['Other'] += 1
            logger.info('exceptions        :: Other')
            logger.info(traceback.format_exc())

        # Add values to the queue so the parent process can collate
        for key, value in anomaly_breakdown.items():
            self.mirage_anomaly_breakdown_q.put((key, value))

        for key, value in exceptions.items():
            self.mirage_exceptions_q.put((key, value))

        metric_var_files = []
        timeseries = []

        # Remove metric check file
        try:
            os.remove(metric_check_file)
        except OSError:
            pass

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

        def smtp_trigger_alert(alert, metric, second_order_resolution_seconds):
            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            try:
                p = Process(target=self.spawn_alerter_process, args=(alert, metric, second_order_resolution_seconds))
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

        """
        DEVELOPMENT ONLY

        # @added 20160806 - Bug #1558: Memory leak in Analyzer
        # Debug with garbage collection - http://code.activestate.com/recipes/65333/

        """
        if ENABLE_MEMORY_PROFILING and garbage_collection_enabled:
            # Debug with garbage collection - http://code.activestate.com/recipes/65333/
            gc.enable()
            gc.set_debug(gc.DEBUG_LEAK)

            # As per http://stackoverflow.com/a/1641280
            # This got useable understandable data with gc
            before = defaultdict(int)
            after = defaultdict(int)
            for i in get_objects():
                before[type(i)] += 1

        if LOCAL_DEBUG:
            logger.info('debug :: Memory usage in run at start: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

        while 1:
            now = time()

            """
            DEVELOPMENT ONLY

            # @added 20160806 - Bug #1558: Memory leak in Analyzer
            # Debug with garbage collection - http://code.activestate.com/recipes/65333/

            """
            if ENABLE_MEMORY_PROFILING and garbage_collection_enabled:
                # As per http://stackoverflow.com/a/1641280
                # This got useable understandable data with gc
                before = defaultdict(int)
                after = defaultdict(int)
                for i in get_objects():
                    before[type(i)] += 1

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage before looking for checks: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except:
                logger.info('skyline can not connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                sleep(10)
                logger.info('connecting to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                continue

            """
            Determine if any metric to analyze
            """
            while True:

                # Report app up
                self.redis_conn.setex(skyline_app, 120, now)

                metric_var_files = [f for f in listdir(settings.MIRAGE_CHECK_PATH) if isfile(join(settings.MIRAGE_CHECK_PATH, f))]
                if len(metric_var_files) == 0:
                    logger.info('sleeping no metrics...')
                    sleep(10)
                else:
                    sleep(1)

                # Clean up old files
                now_timestamp = time()
                stale_age = now_timestamp - settings.MIRAGE_STALE_SECONDS
                for current_file in listdir(settings.MIRAGE_CHECK_PATH):
                    if os.path.isfile(settings.MIRAGE_CHECK_PATH + "/" + current_file):
                        t = os.stat(settings.MIRAGE_CHECK_PATH + "/" + current_file)
                        c = t.st_ctime
                        # delete file if older than a week
                        if c < stale_age:
                                os.remove(settings.MIRAGE_CHECK_PATH + "/" + current_file)
                                logger.info('removed %s' % (current_file))

                # Discover metric to analyze
                metric_var_files = ''
                metric_var_files = [f for f in listdir(settings.MIRAGE_CHECK_PATH) if isfile(join(settings.MIRAGE_CHECK_PATH, f))]
                if len(metric_var_files) > 0:
                    break

            metric_var_files_sorted = sorted(metric_var_files)
            # metric_check_file = settings.MIRAGE_CHECK_PATH + "/" + metric_var_files_sorted[0]

            logger.info('processing %s' % metric_var_files_sorted[0])

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
                logger.error('failed to cleanup mirage_algorithm.error files - %s' % (traceback.format_exc()))

            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            MIRAGE_PROCESSES = 1
            run_timestamp = int(now)
            for i in range(1, MIRAGE_PROCESSES + 1):
                p = Process(target=self.spin_process, args=(i, run_timestamp))
                pids.append(p)
                pid_count += 1
                logger.info('starting %s of %s spin_process/es' % (str(pid_count), str(MIRAGE_PROCESSES)))
                p.start()
                spawned_pids.append(p.pid)

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
                    logger.info('%s :: %s spin_process/es completed in %.2f seconds' % (
                        skyline_app, str(MIRAGE_PROCESSES), time_to_run))
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
                for algorithm in settings.MIRAGE_ALGORITHMS:
                    algorithm_error_file = '%s/%s.%s.%s.algorithm.error' % (
                        settings.SKYLINE_TMP_DIR, skyline_app,
                        str(completed_pid), algorithm)
                    if os.path.isfile(algorithm_error_file):
                        logger.info(
                            'error - spin_process with pid %s has reported an error with the %s algorithm' % (
                                str(completed_pid), algorithm))
                        try:
                            with open(algorithm_error_file, 'r') as f:
                                error_string = f.read()
                            logger.error('%s' % str(error_string))
                        except:
                            logger.error('failed to read %s error file' % algorithm)
                        try:
                            os.remove(algorithm_error_file)
                        except OSError:
                            pass

            # Grab data from the queue and populate dictionaries
            exceptions = dict()
            anomaly_breakdown = dict()
            while 1:
                try:
                    key, value = self.mirage_anomaly_breakdown_q.get_nowait()
                    if key not in anomaly_breakdown.keys():
                        anomaly_breakdown[key] = value
                    else:
                        anomaly_breakdown[key] += value
                except Empty:
                    break

            while 1:
                try:
                    key, value = self.mirage_exceptions_q.get_nowait()
                    if key not in exceptions.keys():
                        exceptions[key] = value
                    else:
                        exceptions[key] += value
                except Empty:
                    break

            for metric_variable in self.metric_variables:
                if metric_variable[0] == 'metric_name':
                    metric_name = metric_variable[1]
                if metric_variable[0] == 'metric_value':
                    metric_value = metric_variable[1]
                if metric_variable[0] == 'hours_to_resolve':
                    hours_to_resolve = metric_variable[1]
                # if metric_variable[0] == 'metric_timestamp':
                #     metric_timestamp = metric_variable[1]

            logger.info('analysis done - %s' % metric_name)

            # Send alerts
            # Calculate hours second order resolution to seconds
            logger.info('analyzed at %s hours resolution' % hours_to_resolve)
            second_order_resolution_seconds = int(hours_to_resolve) * 3600
            logger.info('analyzed at %s seconds resolution' % second_order_resolution_seconds)

            if settings.MIRAGE_ENABLE_ALERTS:
                for alert in settings.ALERTS:
                    for metric in self.anomalous_metrics:
                        ALERT_MATCH_PATTERN = alert[0]
                        METRIC_PATTERN = metric[1]
                        try:
                            alert_match_pattern = re.compile(ALERT_MATCH_PATTERN)
                            pattern_match = alert_match_pattern.match(METRIC_PATTERN)
                        except:
                            pattern_match = False
                        # @modified 20160806 - Reintroduced the original
                        # substring matching after wildcard matching, to allow
                        # more flexibility
                        if not pattern_match:
                            if alert[0] in metric[1]:
                                pattern_match = True
                        if pattern_match:
                            cache_key = 'mirage.last_alert.%s.%s' % (alert[1], metric[1])
                            try:
                                last_alert = self.redis_conn.get(cache_key)
                                if not last_alert:
                                    self.redis_conn.setex(cache_key, alert[2], packb(metric[0]))
                                    # trigger_alert(alert, metric, second_order_resolution_seconds)
                                    try:
                                        if alert[1] != 'smtp':
                                            trigger_alert(alert, metric, second_order_resolution_seconds)
                                        else:
                                            smtp_trigger_alert(alert, metric, second_order_resolution_seconds)
                                        logger.info('sent %s alert: For %s' % (alert[1], metric[1]))
                                    except Exception as e:
                                        logger.error('error :: could not send %s alert for %s: %s' % (alert[1], metric[1], e))
                            except Exception as e:
                                logger.error('error :: could not query Redis for cache_key')

            if settings.NEGATE_ANALYZER_ALERTS:
                if len(self.anomalous_metrics) == 0:
                    for negate_alert in settings.ALERTS:
                        for not_anomalous_metric in self.not_anomalous_metrics:
                            NEGATE_ALERT_MATCH_PATTERN = negate_alert[0]
                            NOT_ANOMALOUS_METRIC_PATTERN = not_anomalous_metric[1]
                            alert_match_pattern = re.compile(NEGATE_ALERT_MATCH_PATTERN)
                            negate_pattern_match = alert_match_pattern.match(NOT_ANOMALOUS_METRIC_PATTERN)
                            if negate_pattern_match:
                                try:
                                    logger.info('negate alert sent: For %s' % (not_anomalous_metric[1]))
                                    trigger_negater(negate_alert, not_anomalous_metric, second_order_resolution_seconds, metric_value)
                                except Exception as e:
                                    logger.error('error :: could not send alert: %s' % e)

            # Log progress

            if len(self.anomalous_metrics) > 0:
                logger.info('seconds since last anomaly :: %.2f' % (time() - now))
                logger.info('total anomalies   :: %d' % len(self.anomalous_metrics))
                logger.info('exception stats   :: %s' % exceptions)
                logger.info('anomaly breakdown :: %s' % anomaly_breakdown)

            # Reset counters
            self.anomalous_metrics[:] = []
            self.not_anomalous_metrics[:] = []

            # Reset metric_variables
            self.metric_variables[:] = []

            """
            DEVELOPMENT ONLY

            # @added 20160806 - Bug #1558: Memory leak in Analyzer
            # Debug with garbage collection - http://code.activestate.com/recipes/65333/

            """
            if ENABLE_MEMORY_PROFILING and garbage_collection_enabled:
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    for i in get_objects():
                        after[type(i)] += 1
                    gc_results = [(k, after[k] - before[k]) for k in after if after[k] - before[k]]
                    for gc_result in gc_results:
                        logger.info('debug :: %s' % str(gc_result))

                # @added 20160806 - Bug #1558: Memory leak in Analyzer
                # Debug with garbage collection - http://code.activestate.com/recipes/65333/
                # show the dirt ;-)
                try:
                    logger.info('garbage collecting')
                    all_the_garbage = self.dump_garbage()
                except:
                    logger.error('error :: during garbage collecting')
                    logger.info(traceback.format_exc())
                    all_the_garbage = 'gc errored'

                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info(all_the_garbage)

                logger.info('garbage collected')

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage end of run: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Sleep if it went too fast
            if time() - now < 1:
                logger.info('sleeping due to low run time...')
#                sleep(10)
                sleep(1)
