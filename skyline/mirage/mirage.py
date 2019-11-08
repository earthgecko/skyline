import logging
try:
    from Queue import Empty
except:
    from queue import Empty
# from redis import StrictRedis
from time import time, sleep
from threading import Thread
from collections import defaultdict
# @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
# Use Redis sets in place of Manager().list() to reduce memory and number of
# processes
# from multiprocessing import Process, Manager, Queue
from multiprocessing import Process, Queue
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
# import errno
# import imp
from os import listdir
import datetime

import os.path
import resource
from shutil import rmtree
from ast import literal_eval

import settings
# @modified 20160922 - Branch #922: Ionosphere
# Added the send_anomalous_metric_to skyline_functions.py
from skyline_functions import (
    write_data_to_file, fail_check, send_anomalous_metric_to,
    mkdir_p, send_graphite_metric, filesafe_metricname,
    # @added 20170603 - Feature #2034: analyse_derivatives
    nonNegativeDerivative, in_list,
    # @added 20191030 - Bug #3266: py3 Redis binary objects not strings
    #                   Branch #3262: py3
    # Added a single functions to deal with Redis connection and the
    # charset='utf-8', decode_responses=True arguments required in py3
    get_redis_conn, get_redis_conn_decoded)

from mirage_alerters import trigger_alert
from negaters import trigger_negater
from mirage_algorithms import run_selected_algorithm
from algorithm_exceptions import TooShort, Stale, Boring
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
failed_checks_dir = '%s_failed' % settings.MIRAGE_CHECK_PATH
# @added 20191107 - Branch #3262: py3
alert_test_file = '%s/%s_alert_test.txt' % (settings.SKYLINE_TMP_DIR, skyline_app)


class Mirage(Thread):
    def __init__(self, parent_pid):
        """
        Initialize the Mirage
        """
        super(Mirage, self).__init__()
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
        # Disabled all the Manager().list() below and replaced with Redis sets
        # self.anomalous_metrics = Manager().list()
        self.mirage_exceptions_q = Queue()
        self.mirage_anomaly_breakdown_q = Queue()
        # self.not_anomalous_metrics = Manager().list()
        # self.metric_variables = Manager().list()
        # self.ionosphere_metrics = Manager().list()
        # self.sent_to_crucible = Manager().list()
        # self.sent_to_panorama = Manager().list()
        # self.sent_to_ionosphere = Manager().list()
        # @added 20170603 - Feature #2034: analyse_derivatives
        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        # @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # Use get_redis_conn and get_redis_conn_decoded to use on Redis sets when the bytes
        # types need to be decoded as utf-8 to str
        # if settings.REDIS_PASSWORD:
        #     self.redis_conn = StrictRedis(
        #         password=settings.REDIS_PASSWORD,
        #         unix_socket_path=settings.REDIS_SOCKET_PATH)
        # else:
        #     self.redis_conn = StrictRedis(
        #         unix_socket_path=settings.REDIS_SOCKET_PATH)

        # @added 20191030 - Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
        # Added a single functions to deal with Redis connection and the
        # charset='utf-8', decode_responses=True arguments required in py3
        self.redis_conn = get_redis_conn(skyline_app)
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            exit(0)

    def spawn_alerter_process(self, alert, metric, second_order_resolution_seconds, context):
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

        trigger_alert(alert, metric, second_order_resolution_seconds, context)

    def surface_graphite_metric_data(self, metric_name, graphite_from, graphite_until):

        # @added 20160803 - Unescaped Graphite target - https://github.com/earthgecko/skyline/issues/20
        #                   bug1546: Unescaped Graphite target
        # @modified 20191107 - Branch #3263: py3
        # Commented out colon
        # new_metric_namespace = metric_name.replace(':', '\:')
        # metric_namespace = new_metric_namespace.replace('(', '\(')
        metric_namespace = metric_name.replace('(', '\(')
        metric_name = metric_namespace.replace(')', '\)')

        try:
            # We use absolute time so that if there is a lag in mirage the correct
            # timeseries data is still surfaced relevant to the anomalous datapoint
            # timestamp
            if settings.GRAPHITE_PORT != '':
                # @modified 20190520 - Branch #3002: docker
                # Use GRAPHITE_RENDER_URI
                # url = '%s://%s:%s/render/?from=%s&until=%s&target=%s&format=json' % (
                #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                #     str(settings.GRAPHITE_PORT), graphite_from, graphite_until,
                #     metric_name)
                url = '%s://%s:%s/%s/?from=%s&until=%s&target=%s&format=json' % (
                    settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                    str(settings.GRAPHITE_PORT), settings.GRAPHITE_RENDER_URI,
                    graphite_from, graphite_until, metric_name)
            else:
                # @modified 20190520 - Branch #3002: docker
                # Use GRAPHITE_RENDER_URI
                # url = '%s://%s/render/?from=%s&until=%s&target=%s&format=json' % (
                #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                #     graphite_from, graphite_until, metric_name)
                url = '%s://%s/%s/?from=%s&until=%s&target=%s&format=json' % (
                    settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                    settings.GRAPHITE_RENDER_URI, graphite_from, graphite_until,
                    metric_name)
            r = requests.get(url)
            js = r.json()
            datapoints = js[0]['datapoints']
        except:
            logger.info(traceback.format_exc())
            logger.error('error :: surface_graphite_metric_data :: failed to get data from Graphite')
            return False

        converted = []
        for datapoint in datapoints:
            try:
                new_datapoint = [float(datapoint[1]), float(datapoint[0])]
                converted.append(new_datapoint)
            # @modified 20170913 - Task #2160: Test skyline with bandit
            # Added nosec to exclude from bandit tests
            except:  # nosec
                continue

        parsed = urlparse.urlparse(url)
        target = urlparse.parse_qs(parsed.query)['target'][0]

        metric_data_folder = settings.MIRAGE_DATA_FOLDER + "/" + target
        mkdir_p(metric_data_folder)
        with open(metric_data_folder + "/" + target + '.json', 'w') as f:
            f.write(json.dumps(converted))
            f.close()
            return True

        return False

    # @added 20170127 - Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
    #                   Bug #1460: panorama check file fails
    #                   Panorama check file fails #24
    # Get rid of the skyline_functions imp as imp is deprecated in py3 anyway
    def mirage_load_metric_vars(self, metric_vars_file):
        """
        Load the metric variables for a check from a metric check variables file

        :param metric_vars_file: the path and filename to the metric variables files
        :type metric_vars_file: str
        :return: the metric_vars list or ``False``
        :rtype: list

        """
        if os.path.isfile(metric_vars_file):
            logger.info(
                'loading metric variables from metric_check_file - %s' % (
                    str(metric_vars_file)))
        else:
            logger.error(
                'error :: loading metric variables from metric_check_file - file not found - %s' % (
                    str(metric_vars_file)))
            return False

        metric_vars = []
        with open(metric_vars_file) as f:
            for line in f:
                no_new_line = line.replace('\n', '')
                no_equal_line = no_new_line.replace(' = ', ',')
                array = str(no_equal_line.split(',', 1))
                add_line = literal_eval(array)
                metric_vars.append(add_line)

        string_keys = ['metric']
        float_keys = ['value']
        int_keys = ['hours_to_resolve', 'metric_timestamp']

        metric_vars_array = []
        for var_array in metric_vars:
            # @modified 20181023 - Feature #2618: alert_slack
            # Wrapped in try except for debugging issue where the
            # hours_to_resolve was interpolating to hours_to_resolve = "t"
            try:
                key = None
                value = None
                if var_array[0] in string_keys:
                    key = var_array[0]
                    _value_str = str(var_array[1]).replace("'", '')
                    value_str = str(_value_str).replace('"', '')
                    value = str(value_str)
                    if var_array[0] == 'metric':
                        metric = value
                if var_array[0] in float_keys:
                    key = var_array[0]
                    _value_str = str(var_array[1]).replace("'", '')
                    value_str = str(_value_str).replace('"', '')
                    value = float(value_str)
                if var_array[0] in int_keys:
                    key = var_array[0]
                    _value_str = str(var_array[1]).replace("'", '')
                    value_str = str(_value_str).replace('"', '')
                    value = int(float(value_str))
                if key:
                    metric_vars_array.append([key, value])

                if len(metric_vars_array) == 0:
                    logger.error(
                        'error :: loading metric variables - none found' % (
                            str(metric_vars_file)))
                    return False
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to load metric variables from check file - %s' % (metric_vars_file))
                return False

        logger.info('debug :: metric_vars for %s' % str(metric))
        logger.info('debug :: %s' % str(metric_vars_array))

        return metric_vars_array

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
                        log_string = 'unused variable for testing only'
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: print x and s')
                    if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                        logger.info(log_string)
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

        check_file_name = os.path.basename(str(metric_check_file))
        check_file_timestamp = check_file_name.split('.', 1)[0]
        check_file_metricname_txt = check_file_name.split('.', 1)[1]
        check_file_metricname = check_file_metricname_txt.replace('.txt', '')
        check_file_metricname_dir = check_file_metricname.replace('.', '/')
        metric_failed_check_dir = '%s/%s/%s' % (failed_checks_dir, check_file_metricname_dir, check_file_timestamp)

        # Load metric variables
        # @modified 20160822 - Bug #1460: panorama check file fails
        # Changed to panorama style skyline_functions load_metric_vars
        # self.load_metric_vars(metric_check_file)
        # Load and validate metric variables
        try:
            # @modified 20170127 - Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
            #                      Bug #1460: panorama check file fails
            #                      Panorama check file fails #24
            # Get rid of the skyline_functions imp as imp is deprecated in py3 anyway
            # metric_vars = load_metric_vars(skyline_app, str(metric_check_file))
            metric_vars_array = self.mirage_load_metric_vars(str(metric_check_file))
        except:
            logger.info(traceback.format_exc())
            logger.error('error :: failed to load metric variables from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        # Test metric variables
#        if len(metric_vars.metric) == 0:
#            logger.error('error :: failed to load metric variable from check file - %s' % (metric_check_file))
#            return
#        else:
#            metric = metric_vars.metric
#            metric_name = ['metric_name', metric_vars.metric]
#            self.metric_variables.append(metric_name)
#            logger.info('debug :: added metric_name %s from check file - %s' % (metric_name, metric_check_file))

        metric = None
        try:
            key = 'metric'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            metric = str(value_list[0])
            metric_name = ['metric_name', metric]
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # self.metric_variables.append(metric_name)
            redis_set = 'mirage.metric_variables'
            data = str(metric_name)
            try:
                self.redis_conn.sadd(redis_set, data)
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to add %s to Redis set %s' % (
                    str(data), str(redis_set)))

            logger.info('debug :: added metric_name %s from check file - %s' % (str(metric_name), metric_check_file))
        except:
            logger.info(traceback.format_exc())
            logger.error('error :: failed to read metric variable from check file - %s' % (metric_check_file))
            return
        if not metric:
            logger.error('error :: failed to load metric variable from check file - %s' % (metric_check_file))
            return

#        if len(metric_vars.value) == 0:
#            return
#        else:
#            metric_value = ['metric_value', metric_vars.value]
#            self.metric_variables.append(metric_value)
        value = None
        try:
            key = 'value'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            value = float(value_list[0])
            metric_value = ['metric_value', value]
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # self.metric_variables.append(metric_value)
            redis_set = 'mirage.metric_variables'
            data = str(metric_value)
            try:
                self.redis_conn.sadd(redis_set, data)
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to add %s to Redis set %s' % (
                    str(data), str(redis_set)))
        except:
            logger.error('error :: failed to read value variable from check file - %s' % (metric_check_file))
            return
        if not value:
            # @modified 20181119 - Bug #2708: Failing to load metric vars
            if value == 0.0:
                pass
            else:
                logger.error('error :: failed to load value variable from check file - %s' % (metric_check_file))
                return


#        if len(metric_vars.hours_to_resolve) == 0:
#            return
#        else:
#            hours_to_resolve = ['hours_to_resolve', metric_vars.hours_to_resolve]
#            self.metric_variables.append(hours_to_resolve)
        hours_to_resolve = None
        try:
            key = 'hours_to_resolve'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            hours_to_resolve = int(value_list[0])
            hours_to_resolve_list = ['hours_to_resolve', hours_to_resolve]
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # self.metric_variables.append(hours_to_resolve_list)
            redis_set = 'mirage.metric_variables'
            data = str(hours_to_resolve_list)
            try:
                self.redis_conn.sadd(redis_set, data)
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to add %s to Redis set %s' % (
                    str(data), str(redis_set)))
        except:
            logger.error('error :: failed to read hours_to_resolve variable from check file - %s' % (metric_check_file))
            return
        if not hours_to_resolve:
            logger.error('error :: failed to load hours_to_resolve variable from check file - %s' % (metric_check_file))
            return

#        if len(metric_vars.metric_timestamp) == 0:
#            return
#        else:
#            metric_timestamp = ['metric_timestamp', metric_vars.metric_timestamp]
#            self.metric_variables.append(metric_timestamp)
        metric_timestamp = None
        try:
            key = 'metric_timestamp'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            metric_timestamp = int(value_list[0])
            metric_timestamp_list = ['metric_timestamp', metric_timestamp]
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # self.metric_variables.append(metric_timestamp_list)
            redis_set = 'mirage.metric_variables'
            data = str(metric_timestamp_list)
            try:
                self.redis_conn.sadd(redis_set, data)
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to add %s to Redis set %s' % (
                    str(data), str(redis_set)))
        except:
            logger.error('error :: failed to read metric_timestamp variable from check file - %s' % (metric_check_file))
            return
        if not metric_timestamp:
            logger.error('error :: failed to load metric_timestamp variable from check file - %s' % (metric_check_file))
            return

        metric_data_dir = '%s/%s' % (settings.MIRAGE_DATA_FOLDER, str(metric))

        # Ignore any metric check with a timestamp greater than MIRAGE_STALE_SECONDS
        int_metric_timestamp = int(metric_timestamp)
        int_run_timestamp = int(run_timestamp)
        metric_timestamp_age = int_run_timestamp - int_metric_timestamp
        if metric_timestamp_age > settings.MIRAGE_STALE_SECONDS:
            logger.info('stale check :: %s check request is %s seconds old - discarding' % (metric, str(metric_timestamp_age)))
            # Remove metric check file
#            try:
#                os.remove(metric_check_file)
#            except OSError:
#                pass
#            return
            if os.path.isfile(metric_check_file):
                os.remove(metric_check_file)
                logger.info('removed check file - %s' % (metric_check_file))
            else:
                logger.info('could not remove check file - %s' % (metric_check_file))

            # Remove the metric directory
            if os.path.exists(metric_data_dir):
                try:
                    rmtree(metric_data_dir)
                    logger.info('removed data dir - %s' % metric_data_dir)
                except:
                    logger.error('error :: failed to rmtree - %s' % metric_data_dir)
            return

        # Calculate hours second order resolution to seconds
        second_order_resolution_seconds = int(hours_to_resolve) * 3600

        # Calculate graphite from and until parameters from the metric timestamp
        graphite_until = datetime.datetime.fromtimestamp(int(float(metric_timestamp))).strftime('%H:%M_%Y%m%d')
        int_second_order_resolution_seconds = int(float(second_order_resolution_seconds))
        second_resolution_timestamp = int_metric_timestamp - int_second_order_resolution_seconds
        graphite_from = datetime.datetime.fromtimestamp(int(second_resolution_timestamp)).strftime('%H:%M_%Y%m%d')

        # Remove any old json file related to the metric
        metric_json_file = '%s/%s.json' % (metric_data_dir, str(metric))
        try:
            os.remove(metric_json_file)
        except OSError:
            pass

        # Get data from graphite
        logger.info(
            'retrieve data :: surfacing %s time series from graphite for %s seconds' % (
                metric, str(second_order_resolution_seconds)))
        self.surface_graphite_metric_data(metric, graphite_from, graphite_until)

        # Check there is a json timeseries file to test
        if not os.path.isfile(metric_json_file):
            logger.error(
                'error :: retrieve failed - failed to surface %s time series from graphite' % (
                    metric))
            # Remove metric check file
            try:
                os.remove(metric_check_file)
            except OSError:
                pass
            # Remove the metric directory
            try:
                rmtree(metric_data_dir)
                logger.info('removed data dir - %s' % metric_data_dir)
            except:
                logger.error('error :: failed to rmtree %s' % metric_data_dir)
            return
        else:
            logger.info('retrieved data :: for %s at %s seconds' % (
                metric, str(second_order_resolution_seconds)))

        # Make process-specific dicts
        exceptions = defaultdict(int)
        anomaly_breakdown = defaultdict(int)

        self.check_if_parent_is_alive()

        with open((metric_json_file), 'r') as f:
            timeseries = json.loads(f.read())
            logger.info('data points surfaced :: %s' % (str(len(timeseries))))

        # @added 20170212 - Feature #1886: Ionosphere learn
        # Only process if the metric has sufficient data
        first_timestamp = None
        try:
            first_timestamp = int(timeseries[0][0])
        except:
            logger.error('error :: could not determine first timestamp')
        timestamp_now = int(time())
        valid_if_before_timestamp = timestamp_now - int(settings.FULL_DURATION)
        valid_mirage_timeseries = True
        if first_timestamp:
            if first_timestamp > valid_if_before_timestamp:
                valid_mirage_timeseries = False

        # @added 20170603 - Feature #2034: analyse_derivatives
        # Convert the values of metrics strictly increasing monotonically
        # to their deriative products
        known_derivative_metric = False
        try:
            # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            # derivative_metrics = list(self.redis_conn.smembers('derivative_metrics'))
            derivative_metrics = list(self.redis_conn_decoded.smembers('derivative_metrics'))
        except:
            derivative_metrics = []
        redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(metric))
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
        if known_derivative_metric:
            try:
                derivative_timeseries = nonNegativeDerivative(timeseries)
                timeseries = derivative_timeseries
            except:
                logger.error('error :: nonNegativeDerivative failed')

        try:
            if valid_mirage_timeseries:
                logger.info('analyzing :: %s at %s seconds' % (metric, second_order_resolution_seconds))
                anomalous, ensemble, datapoint = run_selected_algorithm(timeseries, metric, second_order_resolution_seconds)
            else:
                logger.info('not analyzing :: %s at %s seconds as there is not sufficiently older datapoints in the timeseries - not valid_mirage_timeseries' % (metric, second_order_resolution_seconds))
                anomalous = False
                datapoint = timeseries[-1][1]
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

        if not anomalous:
            base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
            not_anomalous_metric = [datapoint, base_name]
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # self.not_anomalous_metrics.append(not_anomalous_metric)
            redis_set = 'mirage.not_anomalous_metrics'
            data = str(not_anomalous_metric)
            try:
                self.redis_conn.sadd(redis_set, data)
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to add %s to Redis set %s' % (
                    str(data), str(redis_set)))

            logger.info('not anomalous     :: %s with %s' % (metric, value))

        # If it's anomalous, add it to list
        if anomalous:
            base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
            # metric_timestamp = int(timeseries[-1][0])
            metric_timestamp = int_metric_timestamp
            anomalous_metric = [datapoint, base_name, metric_timestamp]
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # self.anomalous_metrics.append(anomalous_metric)
            redis_set = 'mirage.anomalous_metrics'
            data = str(anomalous_metric)
            try:
                self.redis_conn.sadd(redis_set, data)
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to add %s to mirage.anomalous_metrics Redis set' % (
                    str(data), str(redis_set)))

            logger.info('anomaly detected  :: %s with %s' % (metric, str(value)))
            # It runs so fast, this allows us to process 30 anomalies/min
            sleep(2)

            # Get the anomaly breakdown - who returned True?
            triggered_algorithms = []
            for index, value in enumerate(ensemble):
                if value:
                    algorithm = settings.MIRAGE_ALGORITHMS[index]
                    anomaly_breakdown[algorithm] += 1
                    triggered_algorithms.append(algorithm)

            # @added 20170206 - Bug #1904: Handle non filesystem friendly metric names in check files
            sane_metricname = filesafe_metricname(str(base_name))

            # If Crucible or Panorama are enabled determine details
            determine_anomaly_details = False
            if settings.ENABLE_CRUCIBLE and settings.MIRAGE_CRUCIBLE_ENABLED:
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
                # metric_timestamp = str(int(timeseries[-1][0]))
                from_timestamp = str(int(timeseries[1][0]))
                timeseries_dir = base_name.replace('.', '/')

            cache_key = 'mirage.last_alert.smtp.%s' % (base_name)
            last_alert = False
            try:
                last_alert = self.redis_conn.get(cache_key)
            except Exception as e:
                logger.error('error :: could not query Redis for cache_key: %s' % str(e))

            # @added 20170308 - Feature #1960: ionosphere_layers
            # Allow Ionosphere to send Panorama checks, it is an ionosphere_metric
            try:
                # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # ionosphere_unique_metrics = list(self.redis_conn.smembers('ionosphere.unique_metrics'))
                ionosphere_unique_metrics = list(self.redis_conn_decoded.smembers('ionosphere.unique_metrics'))
            except:
                ionosphere_unique_metrics = []

            added_at = str(int(time()))
            # If Panorama is enabled - create a Panorama check
            # @modified 20170308 - Feature #1960: ionosphere_layers
            # Allow Ionosphere to send Panorama checks for ionosphere_metrics
            # if settings.PANORAMA_ENABLED:
            send_to_panorama = False
            redis_metric_name = '%s%s' % (str(settings.FULL_NAMESPACE), str(base_name))
            if settings.PANORAMA_ENABLED:
                send_to_panorama = True
            if redis_metric_name in ionosphere_unique_metrics:
                send_to_panorama = False
            if send_to_panorama:
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
                       str(int_metric_timestamp), str(settings.MIRAGE_ALGORITHMS),
                       triggered_algorithms, skyline_app, source,
                       this_host, added_at)

                # Create an anomaly file with details about the anomaly
                panaroma_anomaly_file = '%s/%s.%s.txt' % (
                    settings.PANORAMA_CHECK_PATH, added_at, sane_metricname)
                try:
                    write_data_to_file(
                        skyline_app, panaroma_anomaly_file, 'w',
                        panaroma_anomaly_data)
                    logger.info('added panorama anomaly file :: %s' % (panaroma_anomaly_file))
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # Move to Redis set block below
                    # self.sent_to_panorama.append(base_name)
                except:
                    logger.error('error :: failed to add panorama anomaly file :: %s' % (panaroma_anomaly_file))
                    logger.info(traceback.format_exc())
                # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # Moved from the above self.sent_to_panorama
                redis_set = 'mirage.sent_to_panorama'
                data = str(base_name)
                try:
                    self.redis_conn.sadd(redis_set, data)
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: failed to add %s to Redis set %s' % (
                        str(data), str(redis_set)))

            # If crucible is enabled - save timeseries and create a
            # crucible check
            if settings.ENABLE_CRUCIBLE and settings.MIRAGE_CRUCIBLE_ENABLED:
                from_timestamp = str(int(timeseries[1][0]))
                timeseries_dir = base_name.replace('.', '/')
                crucible_anomaly_dir = settings.CRUCIBLE_DATA_FOLDER + '/' + timeseries_dir + '/' + metric_timestamp
                if not os.path.exists(crucible_anomaly_dir):
                    mkdir_p(crucible_anomaly_dir)

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
                       str(int_metric_timestamp), str(settings.MIRAGE_ALGORITHMS),
                       triggered_algorithms, crucible_anomaly_dir,
                       skyline_app, added_at)

                # Create an anomaly file with details about the anomaly
                crucible_anomaly_file = '%s/%s.txt' % (crucible_anomaly_dir, sane_metricname)
                try:
                    write_data_to_file(
                        skyline_app, crucible_anomaly_file, 'w',
                        crucible_anomaly_data)
                    logger.info('added crucible anomaly file :: %s' % (crucible_anomaly_file))
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # self.sent_to_crucible.append(base_name)
                except:
                    logger.error('error :: failed to add crucible anomaly file :: %s' % (crucible_anomaly_file))
                    logger.info(traceback.format_exc())

                # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # Moved from the above self.sent_to_crucible
                redis_set = 'mirage.sent_to_crucible'
                data = str(base_name)
                try:
                    self.redis_conn.sadd(redis_set, data)
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: failed to add %s to Redis set %s' % (
                        str(data), str(redis_set)))

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
                except:
                    logger.error('error :: failed to add crucible check file :: %s' % (crucible_check_file))
                    logger.info(traceback.format_exc())

            # @added 20160922 - Branch #922: Ionosphere
            # Also added the send_anomalous_metric_to skyline_functions.py
            # function
            if ionosphere_enabled:
                if not last_alert:
                    # @modified 20161228 Feature #1830: Ionosphere alerts
                    # Added full_duration which needs to be recorded to allow Mirage metrics
                    # to be profiled on Redis timeseries data at FULL_DURATION
                    # e.g. mirage.redis.24h.json
                    full_duration = str(second_order_resolution_seconds)
                    # @modified 20170127 - Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
                    # Added ionosphere_parent_id, always zero from Analyzer and Mirage
                    ionosphere_parent_id = 0
                    send_anomalous_metric_to(
                        skyline_app, 'ionosphere', timeseries_dir,
                        str(int_metric_timestamp), base_name, str(datapoint),
                        from_timestamp, triggered_algorithms, timeseries,
                        full_duration, str(ionosphere_parent_id))
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # Moved to the mirage.sent_to_ionosphere Redis set Redis set
                    # block below
                    # self.sent_to_ionosphere.append(base_name)
                else:
                    logger.info('alert expiry key exists not sending to Ionosphere :: %s' % base_name)
                # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # Moved from the above self.sent_to_ionosphere
                if not last_alert:
                    redis_set = 'mirage.sent_to_ionosphere'
                    data = str(base_name)
                    try:
                        self.redis_conn.sadd(redis_set, data)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to add %s to Redis set %s' % (
                            str(data), str(redis_set)))

        # Add values to the queue so the parent process can collate
        for key, value in anomaly_breakdown.items():
            self.mirage_anomaly_breakdown_q.put((key, value))

        for key, value in exceptions.items():
            self.mirage_exceptions_q.put((key, value))

        metric_var_files = []
        timeseries = []

        if os.path.isfile(metric_check_file):
            # Remove metric check file
            try:
                os.remove(metric_check_file)
                logger.info('removed check file - %s' % metric_check_file)
            except OSError:
                logger.error('error :: failed to remove check file - %s' % metric_check_file)
                pass

        # Remove the metric directory
        if os.path.exists(metric_data_dir):
            try:
                rmtree(metric_data_dir)
                logger.info('removed data dir - %s' % metric_data_dir)
            except:
                logger.error('error :: failed to rmtree %s' % metric_data_dir)

        # @added 20190408 - Feature #2882: Mirage - periodic_check
        # Remove the training_dir for mirage_periodic_check_metrics if not
        # anomalous
        if not anomalous:
            try:
                # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # mirage_periodic_check_metrics = list(self.redis_conn.smembers('mirage.periodic_check.metrics'))
                mirage_periodic_check_metrics = list(self.redis_conn_decoded.smembers('mirage.periodic_check.metrics'))
            except:
                logger.error('error :: failed to get mirage_periodic_check_metrics from Redis')
                mirage_periodic_check_metrics = []
            if metric in mirage_periodic_check_metrics:
                timeseries_dir = base_name.replace('.', '/')
                training_dir = '%s/%s/%s' % (
                    settings.IONOSPHERE_DATA_FOLDER, str(metric_timestamp),
                    str(timeseries_dir))
                if os.path.exists(training_dir):
                    try:
                        rmtree(training_dir)
                        logger.info('removed Mirage periodic check training_data dir - %s' % training_dir)
                    except:
                        logger.error('error :: failed to rmtree  Mirage periodic check training_dir - %s' % training_dir)
            del mirage_periodic_check_metrics

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

        def smtp_trigger_alert(alert, metric, second_order_resolution_seconds, context):
            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            try:
                p = Process(target=self.spawn_alerter_process, args=(alert, metric, second_order_resolution_seconds, context))
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
                    logger.info('%s :: stopping spin_process - %s' % (skyline_app, str(p.is_alive())))
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
                # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                if settings.REDIS_PASSWORD:
                    self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                else:
                    self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                if self.redis_conn.ping():
                    logger.info('connected to redis')
                continue

            """
            Determine if any metric to analyze or Ionosphere alerts to be sent
            """
            while True:

                # Report app up
                self.redis_conn.setex(skyline_app, 120, now)

                # @added 20161228 - Feature #1828: ionosphere - mirage Redis data features
                # If Ionosphere is going to pass alerts back to the app
                # here we are going to have break out and force a alerting
                # only run.
                ionosphere_alerts = None
                ionosphere_alerts_returned = False

                metric_var_files = [f for f in listdir(settings.MIRAGE_CHECK_PATH) if isfile(join(settings.MIRAGE_CHECK_PATH, f))]
                # @modified 20190408 - Bug #2904: Initial Ionosphere echo load and Ionosphere feedback
                #                      Feature #2484: FULL_DURATION feature profiles
                # Do not pospone the Ionosphere alerts check on based on whether
                # there are checks on not
                # if len(metric_var_files) == 0:
                if not ionosphere_alerts_returned:
                    # @modified 20161228 - Feature #1830: Ionosphere alerts
                    try:
                        ionosphere_alerts = list(self.redis_conn.scan_iter(match='ionosphere.mirage.alert.*'))
                        ionosphere_alerts_returned = True
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to scan ionosphere.mirage.alert.* from Redis')
                        ionosphere_alerts = []

                    if len(ionosphere_alerts) == 0:
                        ionosphere_alerts_returned = False
                    else:
                        logger.info('Ionosphere alert requested :: %s' % str(ionosphere_alerts))

                    # @modified 20190408 - Bug #2904: Initial Ionosphere echo load and Ionosphere feedback
                    #                      Feature #2484: FULL_DURATION feature profiles
                    # Do not pospone the Ionosphere alerts check
                    # if not ionosphere_alerts_returned:
                    #     logger.info('sleeping no metrics...')
                    #     sleep(10)

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
                            metric = (1, metric_name, int(time()))
                            alert = (metric_name, test_alerter, 10)
                            if settings.SLACK_ENABLED and test_alerter == 'slack':
                                logger.info('test alert to slack for %s' % (metric_name))
                                trigger_alert(alert, metric, 604800, skyline_app)
                            if test_alerter == 'smtp':
                                smtp_trigger_alert(alert, metric, 604800, skyline_app)
                        except:
                            logger.error('error :: test trigger_alert - %s' % traceback.format_exc())
                            logger.error('error :: failed to test trigger_alert :: %s' % metric_name)
                    try:
                        os.remove(alert_test_file)
                    except OSError:
                        logger.error('error - failed to remove %s, continuing' % alert_test_file)
                        pass

                # @modified 20190408 - Bug #2904: Initial Ionosphere echo load and Ionosphere feedback
                #                      Feature #2484: FULL_DURATION feature profiles
                # Move this len(metric_var_files) from above and apply the
                # appropriatte sleep
                if len(metric_var_files) == 0:
                    if not ionosphere_alerts_returned:
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
                                logger.info('removed stale check - %s' % (current_file))

                # Discover metric to analyze
                metric_var_files = ''
                # @added 20161228 - Feature #1830: Ionosphere alerts
                # Prioritises Ionosphere alerts
                if ionosphere_alerts_returned:
                    break

                metric_var_files = [f for f in listdir(settings.MIRAGE_CHECK_PATH) if isfile(join(settings.MIRAGE_CHECK_PATH, f))]
                if len(metric_var_files) > 0:
                    break

            # @modified 20161228 - Feature #1830: Ionosphere alerts
            # Only spawn process if this is not an Ionosphere alert
            if not ionosphere_alerts_returned:
                metric_var_files_sorted = sorted(metric_var_files)
                # metric_check_file = settings.MIRAGE_CHECK_PATH + "/" + metric_var_files_sorted[0]

                processing_check_file = metric_var_files_sorted[0]
                logger.info('processing %s' % processing_check_file)

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
                # @modified 20161224 - send mirage metrics to graphite
                # run_timestamp = int(now)
                run_timestamp = int(time())
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
                        # p.join()

                for p in pids:
                    if p.is_alive():
                        logger.info('%s :: stopping spin_process - %s' % (skyline_app, str(p.is_alive())))
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

                # @added 20191021 - Bug #3288: Always send anomaly_breakdown and exception metrics
                #                   Branch #3262: py3
                exceptions_metrics = ['Boring', 'Stale', 'TooShort', 'Other']
                for i_exception in exceptions_metrics:
                    if i_exception not in exceptions.keys():
                        exceptions[i_exception] = 0
                for i_anomaly_breakdown in settings.MIRAGE_ALGORITHMS:
                    if i_anomaly_breakdown not in anomaly_breakdown.keys():
                        anomaly_breakdown[i_anomaly_breakdown] = 0

                # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # Use Redis set and not self.metric_variables
                metric_variables = []
                # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # literal_metric_variables = list(self.redis_conn.smembers('mirage.metric_variables'))
                literal_metric_variables = list(self.redis_conn_decoded.smembers('mirage.metric_variables'))
                for item_list_string in literal_metric_variables:
                    list_item = literal_eval(item_list_string)
                    metric_variables.append(list_item)

                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # for metric_variable in self.metric_variables:
                for metric_variable in metric_variables:
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

                # Remove metric check file
                metric_check_file = '%s/%s' % (settings.MIRAGE_CHECK_PATH, processing_check_file)
                if os.path.isfile(metric_check_file):
                    try:
                        os.remove(metric_check_file)
                        logger.info('removed check file - %s' % metric_check_file)
                    except OSError:
                        pass

                # Remove the metric directory
                timeseries_dir = metric_name.replace('.', '/')
                metric_data_dir = '%s/%s' % (settings.MIRAGE_CHECK_PATH, timeseries_dir)
                if os.path.exists(metric_data_dir):
                    try:
                        rmtree(metric_data_dir)
                        logger.info('removed - %s' % metric_data_dir)
                    except:
                        logger.error('error :: failed to rmtree %s' % metric_data_dir)

                if settings.MIRAGE_ENABLE_ALERTS:
                    # @added 20161228 - Feature #1830: Ionosphere alerts
                    #                   Branch #922: Ionosphere
                    # Bringing Ionosphere online - do alert on Ionosphere metrics
                    try:
                        # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                        #                      Branch #3262: py3
                        # ionosphere_unique_metrics = list(self.redis_conn.smembers('ionosphere.unique_metrics'))
                        ionosphere_unique_metrics = list(self.redis_conn_decoded.smembers('ionosphere.unique_metrics'))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to get ionosphere.unique_metrics from Redis')
                        ionosphere_unique_metrics = []

            # @added 20161228 - Feature #1830: Ionosphere alerts
            #                   Branch #922: Ionosphere
            # Send alerts for Ionosphere
            alert_context = 'Mirage'
            if ionosphere_alerts_returned:
                alert_context = 'Ionosphere'
                ionosphere_unique_metrics = []
                logger.info('Ionosphere alerts requested emptying ionosphere_unique_metrics so Mirage will alert')
                exceptions = dict()
                # @modified 20190524 - Branch #3002
                # Wrapped in try except
                try:
                    run_timestamp = int(time())
                    ionosphere_alert_on = list(self.redis_conn.scan_iter(match='ionosphere.mirage.alert.*'))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to get ionosphere.mirage.alert.* from Redis key scan')
                    ionosphere_alert_on = []

                for cache_key in ionosphere_alert_on:
                    try:
                        alert_on = self.redis_conn.get(cache_key)
                        send_alert_for = literal_eval(alert_on)
                        value = float(send_alert_for[0])
                        base_name = str(send_alert_for[1])
                        metric_timestamp = int(float(send_alert_for[2]))
                        triggered_algorithms = send_alert_for[3]
                        second_order_resolution_seconds = int(send_alert_for[4])
                        anomalous_metric = [value, base_name, metric_timestamp, second_order_resolution_seconds]
                        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        # self.anomalous_metrics.append(anomalous_metric)
                        redis_set = 'mirage.anomalous_metrics'
                        data = str(anomalous_metric)
                        try:
                            self.redis_conn.sadd(redis_set, data)
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: failed to add %s to Redis set %s' % (
                                str(data), str(redis_set)))

                        anomaly_breakdown = dict()
                        for algorithm in triggered_algorithms:
                            anomaly_breakdown[algorithm] = 1
                        self.redis_conn.delete(cache_key)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add an Ionosphere anomalous_metric for %s' % base_name)

            # @added 20181114 - Bug #2682: Reduce mirage ionosphere alert loop
            # To reduce the amount of I/O used by Mirage in this loop check
            # and reduce the number of log entries for 'not alerting - Ionosphere metric'
            # a check is made if the metric_name has already been check, if
            # so continue
            not_alerting_for_ionosphere = 'none'

            # @added 20190408 - Bug #2904: Initial Ionosphere echo load and Ionosphere feedback
            #                   Feature #2484: FULL_DURATION feature profiles
            # Only check Ionosphere is up once per cycle
            ionosphere_up = False

            # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            mirage_anomalous_metrics = []
            try:
                # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # literal_mirage_anomalous_metrics = list(self.redis_conn.smembers('mirage.anomalous_metrics'))
                literal_mirage_anomalous_metrics = list(self.redis_conn_decoded.smembers('mirage.anomalous_metrics'))
                for metric_list_string in literal_mirage_anomalous_metrics:
                    metric = literal_eval(metric_list_string)
                    mirage_anomalous_metrics.append(metric)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to determine list from mirage.anomalous_metrics Redis set')
                mirage_anomalous_metrics = []

            for alert in settings.ALERTS:
                # @added 20181114 - Bug #2682: Reduce mirage ionosphere alert loop
                not_an_ionosphere_metric_check_done = 'none'
                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # for metric in self.anomalous_metrics:
                for metric in mirage_anomalous_metrics:
                    # @added 20161228 - Feature #1830: Ionosphere alerts
                    #                   Branch #922: Ionosphere
                    # Bringing Ionosphere online - do alert on Ionosphere
                    # metrics if Ionosphere is up
                    metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(metric[1]))
                    if metric_name in ionosphere_unique_metrics:
                        # @added 20181114 - Bug #2682: Reduce mirage ionosphere alert loop
                        if not_alerting_for_ionosphere == metric_name:
                            continue

                        # @modified 20190408 - Bug #2904: Initial Ionosphere echo load and Ionosphere feedback
                        #                      Feature #2484: FULL_DURATION feature profiles
                        # Only check Ionosphere is up once per cycle
                        # ionosphere_up = False
                        if not ionosphere_up:
                            try:
                                ionosphere_up = self.redis_conn.get('ionosphere')
                            except Exception as e:
                                logger.error('error :: could not query Redis for ionosphere key: %s' % str(e))
                        if ionosphere_up:
                            # @modified 20190408 - Bug #2904: Initial Ionosphere echo load and Ionosphere feedback
                            #                      Feature #2484: FULL_DURATION feature profiles
                            # Wrapped this block up on conditional based on
                            # ionosphere_alerts_returned
                            if not ionosphere_alerts_returned:
                                logger.info('not alerting - Ionosphere metric - %s' % str(metric[1]))
                                # @added 20181114 - Bug #2682: Reduce mirage ionosphere alert loop
                                not_alerting_for_ionosphere = metric_name
                                continue
                        else:
                            logger.error('error :: Ionosphere not report up')
                            logger.info('taking over alerting from Ionosphere if alert is matched on - %s' % str(metric[1]))
                    else:
                        # @modified 20181114 - Bug #2682: Reduce mirage ionosphere alert loop
                        # logger.info('not an Ionosphere metric checking whether to alert - %s' % str(metric[1]))
                        if not_an_ionosphere_metric_check_done == metric_name:
                            # Do not log multiple times for this either
                            not_an_ionosphere_metric_check_done = metric_name
                        else:
                            if not ionosphere_alerts_returned:
                                logger.info('not an Ionosphere metric checking whether to alert - %s' % str(metric[1]))
                                not_an_ionosphere_metric_check_done = metric_name

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
                                if ionosphere_alerts_returned:
                                    # @modified 20190410 - Feature #2882: Mirage - periodic_check
                                    # Only set if not set
                                    try:
                                        second_order_resolution_seconds + 1
                                        set_second_order_resolution_seconds = False
                                    except:
                                        set_second_order_resolution_seconds = True
                                    if set_second_order_resolution_seconds:
                                        try:
                                            second_order_resolution_seconds = int(metric[3]) * 3600
                                        except:
                                            logger.error(traceback.format_exc())
                                            logger.error('error :: failed to determine full_duration from the Ionosphere alert for %s' % (metric[1]))
                                            logger.info('using settings.FULL_DURATION - %s' % (str(settings.FULL_DURATION)))
                                            second_order_resolution_seconds = settings.FULL_DURATION
                                # @modified 20190524 - Branch #3002
                                # Wrapped in try except
                                try:
                                    self.redis_conn.setex(cache_key, alert[2], packb(metric[0]))
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to set Redis key %s for %s' % (
                                        str(cache_key), metric[1]))
                                # trigger_alert(alert, metric, second_order_resolution_seconds, context)

                                try:
                                    if alert[1] != 'smtp':
                                        trigger_alert(alert, metric, second_order_resolution_seconds, alert_context)
                                    else:
                                        smtp_trigger_alert(alert, metric, second_order_resolution_seconds, alert_context)
                                    logger.info('sent %s alert: For %s' % (alert[1], metric[1]))
                                except Exception as e:
                                    logger.error('error :: could not send %s alert for %s: %s' % (alert[1], metric[1], e))
                            else:
                                logger.info('alert Redis key %s exists not alerting for %s' % (str(cache_key), metric[1]))

                        except Exception as e:
                            logger.error('error :: could not query Redis for cache_key')

            # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            mirage_not_anomalous_metrics = []
            try:
                # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # literal_mirage_not_anomalous_metrics = list(self.redis_conn.smembers('mirage.not_anomalous_metrics'))
                literal_mirage_not_anomalous_metrics = list(self.redis_conn_decoded.smembers('mirage.not_anomalous_metrics'))
                for metric_list_string in literal_mirage_not_anomalous_metrics:
                    metric = literal_eval(metric_list_string)
                    mirage_not_anomalous_metrics.append(metric)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to determine list from mirage.not_anomalous_metrics Redis set')
                mirage_not_anomalous_metrics = []

            if settings.NEGATE_ANALYZER_ALERTS:
                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # if len(self.anomalous_metrics) == 0:
                if len(mirage_anomalous_metrics) == 0:
                    for negate_alert in settings.ALERTS:
                        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        # for not_anomalous_metric in self.not_anomalous_metrics:
                        for not_anomalous_metric in mirage_not_anomalous_metrics:
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
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # if len(self.anomalous_metrics) > 0:
            if len(mirage_anomalous_metrics) > 0:
                logger.info('seconds since last anomaly :: %.2f' % (time() - now))
                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # logger.info('total anomalies    :: %d' % len(self.anomalous_metrics))
                logger.info('total anomalies    :: %d' % len(mirage_anomalous_metrics))
                logger.info('exception stats    :: %s' % exceptions)
                logger.info('anomaly breakdown  :: %s' % anomaly_breakdown)

            # Log to Graphite
            run_time = time() - run_timestamp
            logger.info('seconds to run    :: %.2f' % run_time)
            graphite_run_time = '%.2f' % run_time
            send_metric_name = skyline_app_graphite_namespace + '.run_time'
            send_graphite_metric(skyline_app, send_metric_name, graphite_run_time)

            if settings.ENABLE_CRUCIBLE and settings.MIRAGE_CRUCIBLE_ENABLED:
                try:
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # sent_to_crucible = str(len(self.sent_to_crucible))#
                    # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # sent_to_crucible = str(len(list(self.redis_conn.smembers('mirage.sent_to_crucible'))))
                    sent_to_crucible = str(len(list(self.redis_conn_decoded.smembers('mirage.sent_to_crucible'))))
                except:
                    sent_to_crucible = '0'
                logger.info('sent_to_crucible   :: %s' % sent_to_crucible)
                send_metric_name = '%s.sent_to_crucible' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, sent_to_crucible)

            if settings.PANORAMA_ENABLED:
                try:
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # sent_to_panorama = str(len(self.sent_to_panorama))
                    # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # sent_to_panorama = str(len(list(self.redis_conn.smembers('mirage.sent_to_panorama'))))
                    sent_to_panorama = str(len(list(self.redis_conn_decoded.smembers('mirage.sent_to_panorama'))))
                except:
                    sent_to_panorama = '0'
                logger.info('sent_to_panorama   :: %s' % sent_to_panorama)
                send_metric_name = '%s.sent_to_panorama' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, sent_to_panorama)

            if settings.IONOSPHERE_ENABLED:
                try:
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # sent_to_ionosphere = str(len(self.sent_to_ionosphere))
                    # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # sent_to_ionosphere = str(len(list(self.redis_conn.smembers('mirage.sent_to_ionosphere'))))
                    sent_to_ionosphere = str(len(list(self.redis_conn_decoded.smembers('mirage.sent_to_ionosphere'))))
                except Exception as e:
                    logger.error('error :: could not determine sent_to_ionosphere: %s' % e)
                    sent_to_ionosphere = '0'
                logger.info('sent_to_ionosphere :: %s' % sent_to_ionosphere)
                send_metric_name = '%s.sent_to_ionosphere' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, sent_to_ionosphere)

            # Reset counters
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # Use Redis sets instead of Manager().list()
            # self.anomalous_metrics[:] = []
            # self.not_anomalous_metrics[:] = []

            # Reset metric_variables
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # Use Redis sets instead of Manager().list()
            # self.metric_variables[:] = []
            # self.sent_to_crucible[:] = []
            # self.sent_to_panorama[:] = []
            # self.sent_to_ionosphere[:] = []

            # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # Use Redis sets instead of Manager().list()
            delete_redis_sets = [
                'mirage.anomalous_metrics',
                'mirage.not_anomalous_metrics',
                'mirage.metric_variables',
                'mirage.sent_to_crucible',
                'mirage.sent_to_panorama',
                'mirage.sent_to_ionosphere',
            ]
            for i_redis_set in delete_redis_sets:
                redis_set_to_delete = i_redis_set
                try:
                    self.redis_conn.delete(redis_set_to_delete)
                    logger.info('deleted Redis set - %s' % redis_set_to_delete)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to delete Redis set - %s' % redis_set_to_delete)

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
