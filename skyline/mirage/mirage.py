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
    # @modified 20191113 - Branch #3262: py3
    # import urllib.parse
    import urllib.parse as urlparse

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
    get_redis_conn, get_redis_conn_decoded,
    # @added 20201009 - Feature #3780: skyline_functions - sanitise_graphite_url
    #                   Bug #3778: Handle single encoded forward slash requests to Graphite
    sanitise_graphite_url,
    # @added 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
    encode_graphite_metric_name)

# @added 20200425 - Feature #3512: matched_or_regexed_in_list function
#                   Feature #3508: ionosphere.untrainable_metrics
#                   Feature #3486: analyzer_batch
from matched_or_regexed_in_list import matched_or_regexed_in_list

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

try:
    MIRAGE_PERIODIC_CHECK = settings.MIRAGE_PERIODIC_CHECK
except:
    MIRAGE_PERIODIC_CHECK = False

# @added 20200413 - Feature #3486: analyzer_batch
#                   Feature #3480: batch_processing
try:
    from settings import BATCH_PROCESSING
except:
    BATCH_PROCESSING = None
try:
    # @modified 20200606 - Bug #3572: Apply list to settings import
    BATCH_PROCESSING_NAMESPACES = list(settings.BATCH_PROCESSING_NAMESPACES)
except:
    BATCH_PROCESSING_NAMESPACES = []

# @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
# Determine if any metrcs have negatives values some they can be
# added to the ionosphere.untrainable_metrics Redis set
try:
    # @modified 20200606 - Bug #3572: Apply list to settings import
    # from settings import KNOWN_NEGATIVE_METRICS
    KNOWN_NEGATIVE_METRICS = list(settings.KNOWN_NEGATIVE_METRICS)
except:
    KNOWN_NEGATIVE_METRICS = []

# @added 20200604 - Mirage - populate_redis
try:
    from settings import MIRAGE_AUTOFILL_TOOSHORT
except:
    MIRAGE_AUTOFILL_TOOSHORT = False

# @added 20200607 - Feature #3566: custom_algorithms
try:
    CUSTOM_ALGORITHMS = settings.CUSTOM_ALGORITHMS
except:
    CUSTOM_ALGORITHMS = None
try:
    DEBUG_CUSTOM_ALGORITHMS = settings.DEBUG_CUSTOM_ALGORITHMS
except:
    DEBUG_CUSTOM_ALGORITHMS = False
# @added 20200723 - Feature #3472: ionosphere.training_data Redis set
#                   Feature #3566: custom_algorithms
try:
    MIRAGE_ALWAYS_METRICS = list(settings.MIRAGE_ALWAYS_METRICS)
except:
    MIRAGE_ALWAYS_METRICS = []

# @added 20200610 - Feature #3560: External alert config
try:
    EXTERNAL_ALERTS = settings.EXTERNAL_ALERTS
except:
    EXTERNAL_ALERTS = {}
if EXTERNAL_ALERTS:
    from external_alert_configs import get_external_alert_configs

# @added 20200913 - Branch #3068: SNAB
#                   Task #3744: POC matrixprofile
#                   Info #1792: Shapelet extraction
try:
    SNAB_ENABLED = settings.SNAB_ENABLED
except:
    SNAB_ENABLED = False
try:
    SNAB_CHECKS = settings.SNAB_CHECKS.copy()
except:
    SNAB_CHECKS = {}
# @added 20200916 - Branch #3068: SNAB
#                   Task #3744: POC matrixprofile
mirage_snab_only_checks_redis_set = 'mirage.snab_only_checks'

# @added 20201026 - Task #3800: Handle feedback metrics in Mirage and waterfall alerts
# Handle feedback metrics in a similar style to Ionosphere
try:
    SKYLINE_FEEDBACK_NAMESPACES = list(settings.SKYLINE_FEEDBACK_NAMESPACES)
except:
    # Let us take a guess
    try:
        graphite_host = str(settings.GRAPHITE_HOST)
        graphite_hostname = graphite_host.split('.', -1)[0]
        SKYLINE_FEEDBACK_NAMESPACES = [settings.SERVER_METRICS_NAME, graphite_hostname]
    except:
        SKYLINE_FEEDBACK_NAMESPACES = [this_host]

# @added 20210701 - Feature #4152: DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES
try:
    DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES = list(settings.DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES)
except:
    DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES = []

# @added 20201208 - Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
#                   Task #3868: POC MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
try:
    MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS = settings.MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
except:
    MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS = False

# @added 20210323 - Feature #3642: Anomaly type classification
try:
    LUMINOSITY_CLASSIFY_ANOMALIES = settings.LUMINOSITY_CLASSIFY_ANOMALIES
except:
    LUMINOSITY_CLASSIFY_ANOMALIES = False

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
            # @added 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
            # Log warning
            logger.warn('warning :: parent or current process dead')
            exit(0)

    # @modified 20210304 - Feature #3642: Anomaly type classification
    #                      Feature #3970: custom_algorithm - adtk_level_shift
    # Added triggered_algorithms
    def spawn_alerter_process(self, alert, metric, second_order_resolution_seconds, context, triggered_algorithms):
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

        # @modified 20210304 - Feature #3642: Anomaly type classification
        #                      Feature #3970: custom_algorithm - adtk_level_shift
        # Added triggered_algorithms
        trigger_alert(alert, metric, second_order_resolution_seconds, context, triggered_algorithms)

    # @modified 20201208 - Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
    #                      Task #3868: POC MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
    # def surface_graphite_metric_data(self, metric_name, graphite_from, graphite_until):
    def surface_graphite_metric_data(self, metric_name, graphite_from, graphite_until, high_res=False):

        # @added 20160803 - Unescaped Graphite target - https://github.com/earthgecko/skyline/issues/20
        #                   bug1546: Unescaped Graphite target
        # @modified 20191107 - Branch #3263: py3
        # Commented out colon
        # new_metric_namespace = metric_name.replace(':', '\:')
        # metric_namespace = new_metric_namespace.replace('(', '\(')
        metric_namespace = metric_name.replace('(', '\(')
        metric_name = metric_namespace.replace(')', '\)')

        # @added 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
        encoded_graphite_metric_name = encode_graphite_metric_name(skyline_app, metric_name)

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
                    # @modified 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
                    # graphite_from, graphite_until, metric_name)
                    graphite_from, graphite_until, encoded_graphite_metric_name)
            else:
                # @modified 20190520 - Branch #3002: docker
                # Use GRAPHITE_RENDER_URI
                # url = '%s://%s/render/?from=%s&until=%s&target=%s&format=json' % (
                #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                #     graphite_from, graphite_until, metric_name)
                url = '%s://%s/%s/?from=%s&until=%s&target=%s&format=json' % (
                    settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                    settings.GRAPHITE_RENDER_URI, graphite_from, graphite_until,
                    # @modified 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
                    # metric_name)
                    encoded_graphite_metric_name)

            r = requests.get(url)
            js = r.json()
            datapoints = js[0]['datapoints']
        except:
            logger.error(traceback.format_exc())
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

        metric_data_folder = str(settings.MIRAGE_DATA_FOLDER) + "/" + target
        mkdir_p(metric_data_folder)

        # @added 20201208 - Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
        #                   Task #3868: POC MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
        if high_res:
            with open(metric_data_folder + "/" + target + '.high_res', 'w') as f:
                f.write(json.dumps(converted))
                f.close()
                return True

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

        # @added 20200429 - Feature #3486: analyzer_batch
        #                   Feature #3480: batch_processing
        # Allow the check file to already hold a valid python list on one line
        # so that a check can be added by simply echoing to debug metric_vars
        # line from to log for any failed checks into a new Mirage check file
        # The original above pattern is still the default, this is for the check
        # files to be added by the operator from the log or for debugging.
        try_literal_eval = False
        if metric_vars:
            if isinstance(metric_vars, list):
                pass
            else:
                try_literal_eval = True
                logger.info('metric_vars is not a list, set to try_literal_eval')
            if len(metric_vars) < 2:
                try_literal_eval = True
                logger.info('metric_vars is not a list of lists, set to try_literal_eval')
        else:
            try_literal_eval = True
            logger.info('metric_vars is not defined, set to try_literal_eval')
        if try_literal_eval:
            try:
                with open(metric_vars_file) as f:
                    for line in f:
                        metric_vars = literal_eval(line)
                        if metric_vars:
                            break
            except:
                logger.error(traceback.format_exc())
                logger.error('metric_vars not loaded with literal_eval')
                metric_vars = []

        string_keys = ['metric']
        float_keys = ['value']
        int_keys = ['hours_to_resolve', 'metric_timestamp']
        # @added 20200916 - Branch #3068: SNAB
        #                   Task #3744: POC matrixprofile
        boolean_keys = ['snab_only_check']

        # @added 20210304 - Feature #3642: Anomaly type classification
        #                   Feature #3970: custom_algorithm - adtk_level_shift
        # Added triggered_algorithms to mirage_check_file
        list_keys = ['triggered_algorithms']

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
                # @added 20200916 - Branch #3068: SNAB
                #                   Task #3744: POC matrixprofile
                # Handle new snab_only_check boolean
                if var_array[0] in boolean_keys:
                    key = var_array[0]
                    logger.debug(
                        'debug :: boolean key - key: %s, value: %s' % (
                            str(var_array[0]), str(var_array[1])))
                    if str(var_array[1]) == '"True"':
                        value = True
                    else:
                        value = False

                # @added 20210304 - Feature #3642: Anomaly type classification
                #                   Feature #3970: custom_algorithm - adtk_level_shift
                # Added triggered_algorithms to mirage_check_file
                if var_array[0] in list_keys:
                    key = var_array[0]
                    logger.debug(
                        'debug :: list key - key: %s, value: %s' % (
                            str(var_array[0]), str(var_array[1])))
                    _value_str = str(var_array[1]).replace("'", '')
                    try:
                        value = literal_eval(var_array[1])
                    except Exception as e:
                        logger.error(
                            'error :: loading metric variables - failed to literal_eval list for %s, %s - %s' % (
                                str(key), str(var_array[1]), e))
                        value = []

                if key:
                    metric_vars_array.append([key, value])

                if len(metric_vars_array) == 0:
                    logger.error(
                        'error :: loading metric variables - none found' % (
                            str(metric_vars_file)))
                    return False
            except:
                logger.error(traceback.format_exc())
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
                logger.error(traceback.format_exc())
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
                        logger.error(traceback.format_exc())
                        logger.error('error :: print x and s')
                    if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                        logger.info(log_string)
        else:
            return None

# @added 20200604 - Mirage - populate_redis
    def populate_redis(self, i, metric):
        """
        Get FULL_DURATION data from Graphite for a metric and populate Redis
        """
        # Check if it has been done via the mirage.redis_populate key
        redis_populated = False
        redis_populated_key = 'mirage.redis_populated.%s' % metric
        try:
            redis_populated = self.redis_conn_decoded.get(redis_populated_key)
        except Exception as e:
            logger.error(
                'error :: populate_redis :: could not query cache_key - %s - %s' % (
                    redis_populated_key, e))
            redis_populated = False
        # Do not handle batch processing metrics
        batch_processing_metrics = []
        try:
            # @modified 20220113 - Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
            # Use aet.analyzer.batch_processing_metrics
            # batch_processing_metrics = list(self.redis_conn_decoded.smembers('analyzer.batch_processing_metrics'))
            batch_processing_metrics = list(self.redis_conn_decoded.smembers('aet.analyzer.batch_processing_metrics'))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: populate_redis :: failed to get analyzer.batch_processing_metrics from Redis')
            batch_processing_metrics = None
        if batch_processing_metrics:
            if metric in batch_processing_metrics:
                redis_populated = True
                logger.info('populate_redis :: %s is a batch processing metric, not handling, creating Redis key %s' % (
                    metric, redis_populated_key))
                try:
                    self.redis_conn.setex(redis_populated_key, settings.FULL_DURATION, int(time()))
                    logger.info('populate_redis :: created Redis key %s' % (redis_populated_key))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: populate_redis :: failed to create Redis key %s' % redis_populated_key)

        if redis_populated:
            logger.info('populate_redis :: the Redis key %s already exists, it has been done' % (redis_populated_key))
            try:
                self.redis_conn.srem('mirage.populate_redis', metric)
                logger.info('populate_redis :: removed item - %s - from Redis set mirage.populate_redis' % (metric))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: populate_redis :: failed to remove item %s from Redis set mirage.populate_redis' % metric)
            return
        time_now = int(time())
        time_from = int(time_now - settings.FULL_DURATION)
        # Calculate graphite from and until parameters from the metric timestamp
        graphite_until = datetime.datetime.fromtimestamp(int(float(time_now))).strftime('%H:%M_%Y%m%d')
        graphite_from = datetime.datetime.fromtimestamp(int(time_from)).strftime('%H:%M_%Y%m%d')
        # Remove any old json file related to the metric
        metric_data_folder = '%s/%s' % (settings.MIRAGE_DATA_FOLDER, metric)
        metric_json_file = '%s/%s.json' % (metric_data_folder, str(metric))
        try:
            os.remove(metric_json_file)
        except OSError:
            pass
        # Get data from graphite
        logger.info('populate_redis :: surfacing %s time series from Graphite' % (metric))
        try:
            self.surface_graphite_metric_data(metric, graphite_from, graphite_until)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: populate_redis :: failed to surface_graphite_metric_data to populate %s' % (
                str(metric_json_file)))
        # Check there is a json timeseries file to use
        if not os.path.isfile(metric_json_file):
            logger.error(
                'error :: populate_redis :: retrieve failed - failed to surface %s time series from graphite' % (
                    metric))
            try:
                self.redis_conn.setex(redis_populated_key, settings.FULL_DURATION, time_now)
                logger.info('populate_redis :: created Redis key %s' % (redis_populated_key))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: populate_redis :: failed to create Redis key %s' % redis_populated_key)
            try:
                self.redis_conn.srem('mirage.populate_redis', metric)
                logger.info('populate_redis :: removed item - %s - from Redis set mirage.populate_redis' % (metric))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: populate_redis :: failed to remove item %s from Redis set mirage.populate_redis' % metric)
            return
        else:
            logger.info('populate_redis :: retrieved data :: for %s' % (
                metric))
        self.check_if_parent_is_alive()
        timeseries = []
        try:
            with open((metric_json_file), 'r') as f:
                timeseries = json.loads(f.read())
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: populate_redis :: failed to get timeseries from json - %s' % metric_json_file)
            timeseries = []
        if not timeseries:
            logger.info('populate_redis :: no timeseries data for %s, setting redis_populated_key and removing from mirage.populate_redis' % metric)
            try:
                self.redis_conn.setex(redis_populated_key, settings.FULL_DURATION, time_now)
                logger.info('populate_redis :: created Redis key %s' % (redis_populated_key))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: populate_redis :: failed to create Redis key %s' % redis_populated_key)
            try:
                self.redis_conn.srem('mirage.populate_redis', metric)
                logger.info('populate_redis :: removed item - %s - from Redis set mirage.populate_redis' % (metric))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: populate_redis :: failed to remove item %s from Redis set mirage.populate_redis' % metric)
            return
        try:
            os.remove(metric_json_file)
        except OSError:
            pass
        FULL_NAMESPACE = settings.FULL_NAMESPACE
        pipe = None
        logger.info('populate_redis :: time series data for %s, populating Redis with %s data points' % (
            metric, str(len(timeseries))))
        try:
            pipe = self.redis_conn.pipeline()
        except Exception as e:
            logger.error('error :: populate_redis :: error on Redis pipe: %s' % (str(e)))
            pipe = None
        redis_populated = False
        try:
            for metric_data in timeseries:
                key = ''.join((FULL_NAMESPACE, metric))
                try:
                    pipe.append(str(key), packb(metric_data))
                except Exception as e:
                    logger.error('error :: populate_redis :: error on pipe.append: %s' % (str(e)))
            pipe.execute()
            redis_populated = True
        except Exception as e:
            logger.error('error :: populate_redis :: error on pipe.execute: %s' % (str(e)))
        if redis_populated:
            del timeseries
            try:
                self.redis_conn.setex(redis_populated_key, settings.FULL_DURATION, time_now)
                logger.info('populate_redis :: created Redis key %s' % (redis_populated_key))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: populate_redis :: failed to create Redis key %s' % redis_populated_key)
            # Add to Redis set so that Analyzer sorts and deduplicates the data
            # on the next run
            try:
                self.redis_conn.sadd('mirage.filled', metric)
                logger.info('populate_redis :: add %s to Redis set mirage.filled for Analyzer to sort and deduplicate the Redis data' % metric)
            except Exception as e:
                logger.error('error :: populate_redis :: failed add metric to Redis set mirage.filled: %s' % e)
            try:
                self.redis_conn.setex(redis_populated_key, settings.FULL_DURATION, time_now)
                logger.info('populate_redis :: created Redis key %s' % (redis_populated_key))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: populate_redis :: failed to create Redis key %s' % redis_populated_key)
            try:
                self.redis_conn.srem('mirage.populate_redis', metric)
                logger.info('populate_redis :: removed item - %s - from Redis set mirage.populate_redis' % (metric))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: populate_redis :: failed to remove item %s from Redis set mirage.populate_redis' % metric)
            return

    # @modified 20200909 - Task #3730: Validate Mirage running multiple processes
    # def spin_process(self, i, run_timestamp):
    def spin_process(self, i, run_timestamp, metric_check_filename):
        """
        Assign a metric for a process to analyze.
        """

#        if int(i) > 1:
#            i_less_one = int(i) - 1
#            sleep_for_str = '0.%s' % str(i_less_one)
#            logger.info('process %s sleeping for %s' % (str(i), sleep_for_str))
#            sleep(float(sleep_for_str))

        # Discover metric to analyze
#        metric_var_files = [f for f in listdir(settings.MIRAGE_CHECK_PATH) if isfile(join(settings.MIRAGE_CHECK_PATH, f))]

        # Check if this process is unnecessary
#        if len(metric_var_files) == 0:
#            logger.info('no check files found, nothing to do')
#            return

        # metric_var_files_sorted = sorted(metric_var_files)

        # @added 20200903 - Task #3730: Validate Mirage running multiple processes
        # Ensure the process locks the check
#        metric_check_filename = None
#        for i_metric_check_file in metric_var_files_sorted:
#            check_assigned = False
#            cache_key = 'mirage.check.lock.%s' % str(i_metric_check_file)
#            try:
#                check_assigned = self.redis_conn.get(cache_key)
#                if not check_assigned:
#                    try:
#                        self.redis_conn.setex(cache_key, 120, int(time()))
#                        metric_check_filename = str(i_metric_check_file)
#                        logger.info('assigned self check file and set Redis key - %s' % (cache_key))
#                        self.redis_conn.sadd('mirage.checks.done', metric_check_filename)
#                        break
#                    except:
#                        logger.error(traceback.format_exc())
#                        logger.error('error :: failed to set Redis key - %s' % cache_key)
#                else:
#                    logger.info('already assigned, Redis key exists - %s' % (cache_key))
#
#            except:
#                logger.error(traceback.format_exc())
#                logger.error('error :: failed to check if Redis key exists - %s' % cache_key)
        if not metric_check_filename:
            logger.info('no check to assign to process, nothing to do')
            return

        metric_check_file = '%s/%s' % (
            # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
            # settings.MIRAGE_CHECK_PATH, str(metric_var_files_sorted[0]))
            settings.MIRAGE_CHECK_PATH, metric_check_filename)

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
            logger.error(traceback.format_exc())
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

        # @added 20200106 - Branch #3262: py3
        #                   Task #3034: Reduce multiprocessing Manager list usage
        # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
        # redis_set_to_delete = 'mirage.metric_variables'
        redis_metric_variables_set = 'mirage.%s.metric_variables' % str(i)
        redis_set_to_delete = redis_metric_variables_set
        try:
            self.redis_conn.delete(redis_set_to_delete)
            logger.info('deleted Redis set - %s' % redis_set_to_delete)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to delete Redis set - %s' % redis_set_to_delete)

        try:
            key = 'metric'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            metric = str(value_list[0])
            metric_name = ['metric_name', metric]
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # self.metric_variables.append(metric_name)
            # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
            redis_set = 'mirage.metric_variables'
            data = str(metric_name)
            try:
                # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                # self.redis_conn.sadd(redis_set, data)
                self.redis_conn.sadd(redis_metric_variables_set, data)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to add %s to Redis set %s' % (
                    str(data), str(redis_set)))

            logger.info('debug :: added metric_name %s from check file - %s' % (str(metric_name), metric_check_file))
        except:
            logger.error(traceback.format_exc())
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
                # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                # self.redis_conn.sadd(redis_set, data)
                self.redis_conn.sadd(redis_metric_variables_set, data)
            except:
                logger.error(traceback.format_exc())
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
                # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                # self.redis_conn.sadd(redis_set, data)
                self.redis_conn.sadd(redis_metric_variables_set, data)
            except:
                logger.error(traceback.format_exc())
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
                # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                # self.redis_conn.sadd(redis_set, data)
                self.redis_conn.sadd(redis_metric_variables_set, data)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to add %s to Redis set %s' % (
                    str(data), str(redis_set)))
        except:
            logger.error('error :: failed to read metric_timestamp variable from check file - %s' % (metric_check_file))
            return
        if not metric_timestamp:
            logger.error('error :: failed to load metric_timestamp variable from check file - %s' % (metric_check_file))
            return

        # @added 20200916 - Branch #3068: SNAB
        #                   Task #3744: POC matrixprofile
        snab_only_check = None
        try:
            key = 'snab_only_check'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            snab_only_check = value_list[0]
        except:
            snab_only_check = None
        snab_only_check_list = ['snab_only_check', snab_only_check]
        redis_set = 'mirage.metric_variables'
        data = str(snab_only_check_list)
        try:
            self.redis_conn.sadd(redis_metric_variables_set, data)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to add %s to Redis set %s' % (
                str(data), str(redis_set)))

        # @added 20210304 - Feature #3642: Anomaly type classification
        #                   Feature #3970: custom_algorithm - adtk_level_shift
        # Added triggered_algorithms to mirage_check_file
        try:
            key = 'triggered_algorithms'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            triggered_algorithms = value_list[0]
        except:
            triggered_algorithms = []

        metric_data_dir = '%s/%s' % (settings.MIRAGE_DATA_FOLDER, str(metric))

        # Ignore any metric check with a timestamp greater than MIRAGE_STALE_SECONDS
        int_metric_timestamp = int(metric_timestamp)
        int_run_timestamp = int(run_timestamp)
        metric_timestamp_age = int_run_timestamp - int_metric_timestamp

        periodic_mirage_check = False

        if MIRAGE_PERIODIC_CHECK:
            try:
                mirage_periodic_check_metrics = list(self.redis_conn_decoded.smembers('mirage.periodic_check.metrics'))
            except:
                logger.error('error :: failed to get mirage_periodic_check_metrics from Redis')
                mirage_periodic_check_metrics = []
            redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(metric))
            if redis_metric_name in mirage_periodic_check_metrics:
                logger.info('this is a periodic Mirage check for %s' % metric)
                periodic_mirage_check = True

        # @added 20200413 - Feature #3486: analyzer_batch
        #                   Feature #3480: batch_processing
        # Do not evaluate batch metrics against MIRAGE_STALE_SECONDS
        if BATCH_PROCESSING:
            # Is this a analyzer_batch related anomaly
            analyzer_batch_anomaly = None
            analyzer_batch_metric_anomaly_key = 'analyzer_batch.anomaly.%s.%s' % (
                str(metric_timestamp), metric)
            try:
                # analyzer_batch_anomaly = self.redis_conn.get(analyzer_batch_metric_anomaly_key)
                analyzer_batch_anomaly = self.redis_conn_decoded.get(analyzer_batch_metric_anomaly_key)
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
                logger.info('batch processing - setting metric_timestamp_age from %s to 1 so that will not be discarded as stale on %s' % (
                    str(metric_timestamp_age), metric))
                metric_timestamp_age = 1

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

            # @added 20200903 - Task #3730: Validate Mirage running multiple processes
            redis_set = 'mirage.stale_check_discarded'
            try:
                self.redis_conn.sadd(redis_set, str(metric))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to add %s to Redis set %s' % (
                    str(metric), str(redis_set)))
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

        # @modified 20191113 - Branch #3262: py3
        # Wrapped in try
        try:
            self.surface_graphite_metric_data(metric, graphite_from, graphite_until)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to surface_graphite_metric_data to populate %s' % (
                str(metric_json_file)))

        # Check there is a json timeseries file to test
        if not os.path.isfile(metric_json_file):
            logger.error(
                'error :: retrieve failed - failed to surface %s time series from graphite' % (
                    metric))

            # @added 20200905 - Feature #3734: waterfall alerts
            # Try a metric 3 times before removing the check file
            remove_check_file = True
            check_failed_key = 'mirage.check.data_retrieval_failed.%s.%s' % (
                str(int_metric_timestamp), metric)
            fail_count = 0
            try:
                fail_count = self.redis_conn.get(check_failed_key)
            except:
                fail_count = 0
            if not fail_count:
                fail_count = 0
            fail_count += 1
            if fail_count < 3:
                remove_check_file = False
                try:
                    self.redis_conn.setex(check_failed_key, 300, fail_count)
                    logger.info('updated fail_count to %s in %s' % (str(fail_count), check_failed_key))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to set Redis key %s with %s' % (
                        str(check_failed_key), str(fail_count)))
            else:
                logger.error('error :: fail_count is %s in %s, removing check file' % (str(fail_count), check_failed_key))

            if remove_check_file:
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

####
        # @added 20201208 - Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
        #                   Task #3868: POC MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
        use_high_res_file = False
        if MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS:
            high_resolution_seconds = int_second_order_resolution_seconds - 3600
            high_resolution_timestamp = int_metric_timestamp - high_resolution_seconds
            graphite_from = datetime.datetime.fromtimestamp(high_resolution_timestamp).strftime('%H:%M_%Y%m%d')

            # Remove any old file related to the metric
            metric_high_res_file = '%s/%s.high_res' % (metric_data_dir, str(metric))
            try:
                os.remove(metric_high_res_file)
            except OSError:
                pass
            # Get data from graphite
            logger.info(
                'retrieve data :: surfacing high resolution %s time series from graphite for %s seconds' % (
                    metric, str(high_resolution_seconds)))
            try:
                high_res = True
                self.surface_graphite_metric_data(metric, graphite_from, graphite_until, high_res)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to surface_graphite_metric_data to populate %s' % (
                    str(metric_high_res_file)))
            # Check there is a json high_res timeseries file to test
            if not os.path.isfile(metric_high_res_file):
                logger.error(
                    'error :: retrieve failed - failed to surface high resolution %s time series from graphite' % (
                        metric))
            else:
                logger.info('retrieved data :: for %s at %s seconds' % (
                    metric, str(high_resolution_seconds)))
                use_high_res_file = metric_high_res_file
####

        # Make process-specific dicts
        exceptions = defaultdict(int)
        anomaly_breakdown = defaultdict(int)

        self.check_if_parent_is_alive()

        with open((metric_json_file), 'r') as f:
            timeseries = json.loads(f.read())
            logger.info('data points surfaced :: %s' % (str(len(timeseries))))

        # @added 20201208 - Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
        #                   Task #3868: POC MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
        high_resolution_analysis = False
        if use_high_res_file:
            high_resolution_timeseries = []
            try:
                with open((use_high_res_file), 'r') as f:
                    high_resolution_timeseries = json.loads(f.read())
                    logger.info('high resolution data points surfaced :: %s' % (str(len(timeseries))))
            except Exception as e:
                logger.error('error :: could not create high_resolution_timeseries from %s - %s' % (
                    str(use_high_res_file), str(e)))
            if high_resolution_timeseries:
                low_resolution_timeseries = list(timeseries)
                logger.info('overriding timeseries with high_resolution_timeseries for analysis')
                timeseries = high_resolution_timeseries
                second_order_low_resolution_seconds = int(second_order_resolution_seconds)
                second_order_resolution_seconds = int(high_resolution_seconds)
                high_resolution_analysis = True

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
            # @modified 20211012 - Feature #4280: aet.metrics_manager.derivative_metrics Redis hash
            # derivative_metrics = list(self.redis_conn_decoded.smembers('derivative_metrics'))
            derivative_metrics = list(self.redis_conn_decoded.smembers('aet.metrics_manager.derivative_metrics'))
        except:
            derivative_metrics = []
        redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(metric))
        if redis_metric_name in derivative_metrics:
            known_derivative_metric = True
        if known_derivative_metric:
            try:
                # @modified 20200606 - Bug #3572: Apply list to settings import
                non_derivative_monotonic_metrics = list(settings.NON_DERIVATIVE_MONOTONIC_METRICS)
            except:
                non_derivative_monotonic_metrics = []
            skip_derivative = in_list(redis_metric_name, non_derivative_monotonic_metrics)
            if skip_derivative:
                known_derivative_metric = False
        if known_derivative_metric and valid_mirage_timeseries:
            try:
                derivative_timeseries = nonNegativeDerivative(timeseries)
                timeseries = derivative_timeseries
            except:
                logger.error('error :: nonNegativeDerivative failed')

        # @added 20200916 - Branch #3068: SNAB
        #                   Task #3744: POC matrixprofile
        if snab_only_check:
            snab_recheck_key = 'snab.recheck.%s' % metric
            snab_recheck_key_exists = False
            try:
                snab_recheck_key_exists = self.redis_conn_decoded.get(snab_recheck_key)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get Redis key %s' % (
                    snab_recheck_key))
            original_added_at = 0
            if snab_recheck_key_exists:
                logger.info('snab recheck key exists - %s' % snab_recheck_key)
                try:
                    original_added_at = int(snab_recheck_key_exists)
                except:
                    # The key expired
                    original_added_at = int(time()) - 300
            else:
                logger.info('snab recheck key does not exists - %s' % snab_recheck_key)

            snab_recheck_original_anomaly_timestamp_key = 'snab.recheck.anomaly_timestamp.%s' % metric
            snab_recheck_original_anomaly_timestamp_key_exists = False
            try:
                snab_recheck_original_anomaly_timestamp_key_exists = self.redis_conn_decoded.get(snab_recheck_original_anomaly_timestamp_key)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get Redis key %s' % (
                    snab_recheck_key))
            original_anomaly_timestamp = int(timeseries[-1][0])
            if snab_recheck_original_anomaly_timestamp_key_exists:
                logger.info('snab.recheck.anomaly_timestamp key exists - %s' % snab_recheck_original_anomaly_timestamp_key)
                try:
                    original_anomaly_timestamp = int(snab_recheck_original_anomaly_timestamp_key_exists)
                except:
                    # The key expired
                    pass
            else:
                logger.info('snab.recheck.anomaly_timestamp key does not exists - %s' % snab_recheck_original_anomaly_timestamp_key)

            snab_json_file_created = False
            if snab_recheck_key_exists and snab_recheck_original_anomaly_timestamp_key_exists:
                # Create timeseries json file with the timeseries
                use_snab_timestamp = metric_timestamp
                try:
                    use_snab_timestamp = int(timeseries[-1][0])
                except:
                    pass
                snab_json_file = '%s/%s.%s.json' % (
                    # settings.SNAB_DATA_DIR, str(int(metric_timestamp)), str(metric))
                    settings.SNAB_DATA_DIR, str(int(use_snab_timestamp)), str(metric))
                timeseries_json = str(timeseries).replace('[', '(').replace(']', ')')
                try:
                    snab_data_last_datapoint = [timeseries[-1][0], timeseries[-1][1]]
                except:
                    snab_data_last_datapoint = [None, None, 'there was no timeseries data']
                if timeseries_json:
                    try:
                        write_data_to_file(skyline_app, snab_json_file, 'w', timeseries_json)
                        logger.info('added snab timeseries file with last entry - %s :: %s' % (
                            str(snab_data_last_datapoint), snab_json_file))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error(
                            'error :: failed to add snab timeseries file :: %s' %
                            snab_json_file)
                    if not os.path.isfile(snab_json_file):
                        logger.error('error - the snab_json_file was not created - %s' % (
                            str(snab_json_file)))
                    else:
                        logger.info('snab_json_file exists - %s' % snab_json_file)
                        snab_json_file_created = True
                else:
                    logger.error(
                        'error :: no timeseries_json to add snab timeseries file :: %s' %
                        snab_json_file)
            else:
                logger.info('not adding snab_json_file as snab recheck keys no longer not exist')

            if snab_json_file_created:
                data = {
                    'metric': metric,
                    'anomaly_data': snab_json_file,
                    'timestamp': int(timeseries[-1][0]),
                    'original_anomaly_timestamp': original_anomaly_timestamp,
                    'value': timeseries[-1][1],
                    'original_added_at': original_added_at,
                }
                try:
                    self.redis_conn.sadd(mirage_snab_only_checks_redis_set, str(data))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to add %s to %s Redis set' % (
                        metric, str(mirage_snab_only_checks_redis_set)))
            # Remove metric check file
            try:
                os.remove(metric_check_file)
            except OSError:
                pass
            # Remove the metric directory
            try:
                rmtree(metric_data_dir)
                logger.info('removed data dir for snab_check_only - %s' % metric_data_dir)
            except:
                logger.error('error :: failed to rmtree for snab_check_only - %s' % metric_data_dir)
            return

        # @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
        # Determine if any metrcs have negatives values some they can be
        # added to the ionosphere.untrainable_metrics Redis set
        run_negatives_present = False
        if settings.IONOSPHERE_ENABLED and valid_mirage_timeseries:
            run_negatives_present = True
            known_negative_metric_matched_by = None
            known_negative_metric, known_negative_metric_matched_by = matched_or_regexed_in_list(skyline_app, metric, KNOWN_NEGATIVE_METRICS)
            if known_negative_metric:
                run_negatives_present = False
                logger.info('will not check %s for negative values' % (metric))
            else:
                logger.info('will check %s for negative values' % (metric))

        # @added 20201001 - Branch #3068: SNAB
        #                   Task #3748: POC SNAB
        # Add timings
        snab_check_namespace = False
        if SNAB_ENABLED and SNAB_CHECKS:
            for app in SNAB_CHECKS:
                if app == skyline_app:
                    for snab_context in SNAB_CHECKS[app]:
                        if snab_check_namespace:
                            break
                        for algorithm in SNAB_CHECKS[app][snab_context]:
                            if snab_check_namespace:
                                break
                            try:
                                for namespace in SNAB_CHECKS[app][snab_context][algorithm]['namespaces']:
                                    if namespace in redis_metric_name:
                                        snab_check_namespace = True
                                        break
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to check if %s is a snab_check_metric' % redis_metric_name)

        # @added 20200607 - Feature #3566: custom_algorithms
        algorithms_run = list(settings.MIRAGE_ALGORITHMS)

        # @added 20200904 - Feature #3734: waterfall alerts
        anomalous = None

        # @added 20201001 - Branch #3068: SNAB
        #                   Task #3748: POC SNAB
        # Add timings
        analysis_start_time = time()

        try:
            if valid_mirage_timeseries:
                logger.info('analyzing :: %s at %s seconds' % (metric, second_order_resolution_seconds))
                # @modified 20200425 - Feature #3508: ionosphere.untrainable_metrics
                # Added run_negatives_present and negatives_found
                # anomalous, ensemble, datapoint = run_selected_algorithm(timeseries, metric, second_order_resolution_seconds)
                # @modified 20200607 - Feature #3566: custom_algorithms
                # Added algorithms_run
                # @modified 20210304 - Feature #3642: Anomaly type classification
                #                      Feature #3970: custom_algorithm - adtk_level_shift
                # Added triggered_algorithms
                anomalous, ensemble, datapoint, negatives_found, algorithms_run = run_selected_algorithm(timeseries, metric, second_order_resolution_seconds, run_negatives_present, triggered_algorithms)
            else:
                logger.info('not analyzing :: %s at %s seconds as there is not sufficiently older datapoints in the timeseries - not valid_mirage_timeseries' % (metric, second_order_resolution_seconds))
                anomalous = False
                datapoint = timeseries[-1][1]
        # It could have been deleted by the Roomba
        except TypeError:
            # @added 20200430 - Feature #3480: batch_processing
            # Added logging here as the DeletedByRoomba exception is
            # generally not related to that but related to some other fail
            # in the processing of the run algorithms phase.
            # It could have been deleted by the Roomba, but Mirage does not use
            # Redis data so probably, definitely was not :)
            logger.error(traceback.format_exc())
            logger.error('error :: added as DeletedByRoomba but possibly not see traceback above')

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
            logger.error(traceback.format_exc())

        # @added 20201001 - Branch #3068: SNAB
        #                   Task #3748: POC SNAB
        # Add timings
        analysis_run_time = time() - analysis_start_time
        logger.info('algorithms analysis completed in %.2f seconds' % (
            analysis_run_time))

        # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
        # base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
        if metric.startswith(settings.FULL_NAMESPACE):
            base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
        else:
            base_name = metric

        # @added 20210505 - Bug #4048: Mirage - removing feedback metrics to be processed
        feedback_cache_key_exists = False
        feedback_cache_key = 'mirage.feedback_metric.checked.%s' % (base_name)
        try:
            feedback_cache_key_exists = self.redis_conn_decoded.get(feedback_cache_key)
        except Exception as e:
            logger.error('error :: failed to get %s key from Redis - %s' % (
                str(feedback_cache_key), e))
        if feedback_cache_key_exists:
            feedback_processed_cache_key = 'mirage.feedback_metric.processed.%s' % (base_name)
            logger.info('feedback metric processed adding Redis key with 600 TTL - %s' % feedback_processed_cache_key)
            try:
                self.redis_conn.setex(feedback_processed_cache_key, 600, int(analysis_start_time))
            except Exception as e:
                logger.error('error :: failed to add %s key to Redis - %s' % (
                    str(feedback_processed_cache_key), e))

        # @added 20201208 - Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
        #                   Task #3868: POC MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
        if high_resolution_analysis:
            second_order_resolution_seconds = int(second_order_low_resolution_seconds)
            datapoint = low_resolution_timeseries[-1][1]
            timeseries = low_resolution_timeseries
            logger.info('analysis on high resolution done, reverting to original timeseries and second_order_resolution_seconds')

        # @added 20200904 - Feature #3734: waterfall alerts
        # Remove the metric from the waterfall_alerts Redis set
        # [metric, timestamp, value, added_to_waterfall_timestamp]
        # waterfall_data = [metric[1], metric[2], metric[0], added_to_waterfall_timestamp. waterfall_panorama_data]
        redis_set = 'analyzer.waterfall_alerts.sent_to_mirage'
        literal_analyzer_waterfall_alerts = []
        try:
            literal_analyzer_waterfall_alerts = list(self.redis_conn_decoded.smembers(redis_set))
        except:
            literal_analyzer_waterfall_alerts = []
        analyzer_waterfall_alerts = []
        for literal_waterfall_alert in literal_analyzer_waterfall_alerts:
            waterfall_alert = literal_eval(literal_waterfall_alert)
            analyzer_waterfall_alerts.append(waterfall_alert)

        if not anomalous:

            not_anomalous_metric = [datapoint, base_name]
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # self.not_anomalous_metrics.append(not_anomalous_metric)
            redis_set = 'mirage.not_anomalous_metrics'
            data = str(not_anomalous_metric)
            try:
                self.redis_conn.sadd(redis_set, data)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to add %s to Redis set %s' % (
                    str(data), str(redis_set)))

            # @added 20200904 - Feature #3734: waterfall alerts
            # Remove the metric from the waterfall_alerts Redis set
            # [metric, timestamp, value, added_to_waterfall_timestamp]
            # waterfall_data = [metric[1], metric[2], metric[0], added_to_waterfall_timestamp, waterfall_panorama_data]
            redis_set = 'analyzer.waterfall_alerts.sent_to_mirage'
            for waterfall_alert in analyzer_waterfall_alerts:
                if waterfall_alert[0] == base_name:
                    if int(waterfall_alert[1]) == metric_timestamp:
                        try:
                            self.redis_conn.srem(redis_set, str(waterfall_alert))
                            logger.info('removed waterfall alert item from Redis set %s - %s' % (
                                redis_set, str(waterfall_alert)))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to remove waterfall alert item for %s at %s from Redis set %s' % (
                                base_name, str(metric_timestamp), redis_set))
                    # @added 20201128 - Feature #3734: waterfall alerts
                    # If the check just done is new than an existing analyzer
                    # waterfall alert metric timestamp remove those keys as well
                    if int(waterfall_alert[1]) < metric_timestamp:
                        try:
                            self.redis_conn.srem(redis_set, str(waterfall_alert))
                            logger.info('removed waterfall alert item with older timestamp from Redis set %s - %s' % (
                                redis_set, str(waterfall_alert)))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to remove waterfall alert item for %s at %s from Redis set %s' % (
                                base_name, str(metric_timestamp), redis_set))

            # @added 20210330 - Feature #3994: Panorama - mirage not anomalous
            # A hash is added to the mirage.panorama.not_anomalous_metrics for
            # every metric that is found to be not anomalous.  This provides
            # data for /panorama?not_anomalous and /panorama?not_anomalous_metric
            # method which are used for plots in the webapp and json response.
            # The mirage.panorama.not_anomalous_metrics Redis hash is managed in
            # analyzer/metrics_manager
            not_anomalous_timestamp = None
            try:
                not_anomalous_timestamp = int(timeseries[-1][0])
            except:
                not_anomalous_timestamp = int(metric_timestamp)
            redis_hash = 'mirage.panorama.not_anomalous_metrics'
            try:
                data = {
                    base_name: {
                        'timestamp': not_anomalous_timestamp,
                        'value': datapoint,
                        'hours_to_resolve': int(hours_to_resolve),
                    }
                }
                self.redis_conn.hset(redis_hash, time(), str(data))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to add %s to Redis hash %s' % (
                    str(data), str(redis_hash)))

            logger.info('not anomalous     :: %s with %s' % (metric, value))

        # If it's anomalous, add it to list
        if anomalous:
            # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
            # base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
            if metric.startswith(settings.FULL_NAMESPACE):
                base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
            else:
                base_name = metric
            # metric_timestamp = int(timeseries[-1][0])
            metric_timestamp = int_metric_timestamp

            # Get the anomaly breakdown - who returned True?
            triggered_algorithms = []
            for index, value in enumerate(ensemble):
                if value:
                    # @modified 20200607 - Feature #3566: custom_algorithms
                    # algorithm = settings.MIRAGE_ALGORITHMS[index]
                    algorithm = algorithms_run[index]

                    anomaly_breakdown[algorithm] += 1
                    triggered_algorithms.append(algorithm)

            # @modified 20201007 - Feature #3772: Add the anomaly_id to the http_alerter json
            #                      Branch #3068: SNAB
            # Added second_order_resolution_seconds, triggered_algorithms and algorithms_run
            # anomalous_metric = [datapoint, base_name, metric_timestamp]
            anomalous_metric = [datapoint, base_name, metric_timestamp, second_order_resolution_seconds, triggered_algorithms, algorithms_run]

            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # self.anomalous_metrics.append(anomalous_metric)
            redis_set = 'mirage.anomalous_metrics'
            data = str(anomalous_metric)
            try:
                self.redis_conn.sadd(redis_set, data)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to add %s to mirage.anomalous_metrics Redis set' % (
                    str(data)))

            # @modified 20201001 - Branch #3068: SNAB
            #                      Task #3748: POC SNAB
            # Added analysis_run_time
            if snab_check_namespace:
                redis_key = 'mirage.analysis_run_time.%s.%s' % (base_name, str(metric_timestamp))
                try:
                    self.redis_conn.setex(redis_key, 120, str(analysis_run_time))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to add snab analysis_run_time Redis key - %s' % (
                        redis_key))

            logger.info('anomaly detected  :: %s with %s' % (metric, str(datapoint)))
            # It runs so fast, this allows us to process 30 anomalies/min
            # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
            # Removed limit
            # sleep(2)

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
                    remove_after_timestamp = int(last_negative_timestamp + second_order_resolution_seconds)
                    data = str([base_name, metric_timestamp, datapoint, last_negative_timestamp, last_negative_value, second_order_resolution_seconds, remove_after_timestamp])
                    self.redis_conn.sadd(redis_set, data)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to add %s to Redis set %s' % (
                        str(data), str(redis_set)))

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
                # @modified 20200805 - Task #3662: Change mirage.last_check keys to timestamp value
                #                      Feature #3486: analyzer_batch
                #                      Feature #3480: batch_processing
                # Changed the last_alert cache key to hold the last
                # anomaly timestamp
                # last_alert = self.redis_conn.get(cache_key)
                last_alert = self.redis_conn_decoded.get(cache_key)
            except Exception as e:
                logger.error('error :: could not query Redis for cache_key: %s' % str(e))

            # @added 20200805 - Task #3662: Change mirage.last_check keys to timestamp value
            #                   Feature #3486: analyzer_batch
            #                   Feature #3480: batch_processing
            # Evaluate the reported anomaly timestamp to determine whether
            # EXPIRATION_TIME should be applied to a batch metric
            analyzer_batch_anomaly = None
            if last_alert:
                # Is this a analyzer_batch related anomaly
                analyzer_batch_metric_anomaly_key = 'analyzer_batch.anomaly.%s.%s' % (
                    str(int_metric_timestamp), base_name)
                try:
                    analyzer_batch_anomaly = self.redis_conn_decoded.get(analyzer_batch_metric_anomaly_key)
                except Exception as e:
                    logger.error(
                        'error :: could not query cache_key - %s - %s' % (
                            analyzer_batch_metric_anomaly_key, e))
                    analyzer_batch_anomaly = None
                if analyzer_batch_anomaly:
                    logger.info('identified as an analyzer_batch triggered anomaly from the presence of the Redis key %s' % analyzer_batch_metric_anomaly_key)
                else:
                    logger.info('not identified as an analyzer_batch triggered anomaly as no Redis key found - %s' % analyzer_batch_metric_anomaly_key)
                if last_alert and analyzer_batch_anomaly:
                    # @modified 20201107 - Feature #3830: metrics_manager
                    # Optimise to use metrics_manager HGETALL rather than
                    # iterating the list of lists
                    # mirage_metrics_expiration_times = []
                    # try:
                    #     mirage_metrics_expiration_times = list(self.redis_conn_decoded.smembers('mirage.metrics_expiration_times'))
                    #     if LOCAL_DEBUG:
                    #         logger.info('debug :: fetched the mirage.metrics_expiration_times Redis set')
                    # except:
                    #     logger.info('failed to fetch the mirage.metrics_expiration_times Redis set')
                    #     mirage_metrics_expiration_times = []
                    # metric_expiration_time = 3600
                    # try:
                    #     for item_list_string in mirage_metrics_expiration_times:
                    #         mirage_alert_expiration_data = literal_eval(item_list_string)
                    #         if mirage_alert_expiration_data[0] == base_name:
                    #             metric_expiration_time = int(mirage_alert_expiration_data[1])
                    #             break
                    # except:
                    #     if LOCAL_DEBUG:
                    #         logger.error('error :: failed to determine mirage_alert_expiration_data for %s from the mirage.metrics_expiration_times Redis set' % str(base_name))
                    #     metric_expiration_time = 3600
                    mirage_metrics_expiration_times = {}
                    try:
                        mirage_metrics_expiration_times = self.redis_conn_decoded.hgetall('mirage.hash_key.metrics_expiration_times')
                        logger.info('%s entries in mirage.hash_key.metrics_expiration_times Redis hash key' % str(len(mirage_metrics_expiration_times)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to get Redis hash key mirage.hash_key.metrics_expiration_times')
                        mirage_metrics_expiration_times = {}
                    try:
                        logger.info('%s entries in mirage.hash_key.metrics_expiration_times Redis hash key' % str(len(mirage_metrics_expiration_times)))
                        metric_expiration_time = int(mirage_metrics_expiration_times[base_name])
                        logger.info('%s has expiration time of %s' % (base_name, str(metric_expiration_time)))
                    except:
                        if LOCAL_DEBUG:
                            logger.error('error :: failed to determine mirage_alert_expiration_data for %s from the mirage.hash_key.metrics_expiration_times Redis hash key' % str(base_name))
                        metric_expiration_time = 3600

                    last_timestamp = None
                    try:
                        last_timestamp = int(last_alert)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to determine last_timestamp from the last Mirage alert key - %s' % cache_key)
                        last_timestamp = None
                    seconds_between_batch_anomalies = None
                    if last_timestamp:
                        try:
                            seconds_between_batch_anomalies = int(int_metric_timestamp) - int(last_timestamp)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to determine seconds_between_batch_anomalies for batch metric Panorama key- %s' % cache_key)
                            last_timestamp = None
                    if seconds_between_batch_anomalies:
                        if seconds_between_batch_anomalies >= int(metric_expiration_time):
                            logger.info('the difference between the last anomaly timestamp (%s) and the batch anomaly timestamp (%s) for batch metric %s is greater than the metric EXPIRATION_TIME of %s' % (
                                str(last_timestamp), str(int_metric_timestamp), base_name,
                                str(metric_expiration_time)))
                            logger.info('alerting on anomaly for batch metric %s, so setting last_alert to None' % (
                                metric))
                            last_alert = None
                        else:
                            logger.info('the difference between the last anomaly timestamp (%s) and the batch anomaly timestamp (%s) for batch metric %s is less than the metric EXPIRATION_TIME of %s, not alerting' % (
                                str(last_timestamp), str(int_metric_timestamp), base_name,
                                str(metric_expiration_time)))
                        if int(int_metric_timestamp) < last_timestamp:
                            logger.info('batch anomaly timestamp (%s) less than the last_check timestamp (%s), alerting on anomaly for batch metric %s, so setting last_alert to None' % (
                                str(int_metric_timestamp), str(last_timestamp), base_name))
                            last_alert = None

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

                if analyzer_batch_anomaly:
                    from_timestamp = int_metric_timestamp - int(second_order_resolution_seconds)
                    from_timestamp = str(from_timestamp)

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
                       # @modified 20200607 - Feature #3566: custom_algorithms
                       # str(int_metric_timestamp), str(settings.MIRAGE_ALGORITHMS),
                       str(int_metric_timestamp), str(algorithms_run),
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
                    logger.error(traceback.format_exc())
                # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # Moved from the above self.sent_to_panorama
                redis_set = 'mirage.sent_to_panorama'
                data = str(base_name)
                try:
                    self.redis_conn.sadd(redis_set, data)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to add %s to Redis set %s' % (
                        str(data), str(redis_set)))

                # @added 20210323 - Feature #3642: Anomaly type classification
                if LUMINOSITY_CLASSIFY_ANOMALIES:
                    redis_set = 'luminosity.classify_anomalies'
                    data_dict = {
                        'metric': metric,
                        'timestamp': int_metric_timestamp,
                        'value': datapoint,
                        'algorithms': algorithms_run,
                        'triggered_algorithms': triggered_algorithms,
                        'app': skyline_app,
                        'added_at': int(added_at),
                    }
                    data = [metric, int_metric_timestamp, int(added_at), data_dict]
                    try:
                        self.redis_conn.sadd(redis_set, str(data))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add %s to Redis set %s' % (
                            str(data), str(redis_set)))

                # @added 20200904 - Feature #3734: waterfall alerts
                # Remove the metric from the waterfall_alerts Redis set
                # [metric, timestamp, value, added_to_waterfall_timestamp]
                # waterfall_data = [metric[1], metric[2], metric[0], added_to_waterfall_timestamp, waterfall_panorama_data]
                redis_set = 'analyzer.waterfall_alerts.sent_to_mirage'
                for waterfall_alert in analyzer_waterfall_alerts:
                    if waterfall_alert[0] == base_name:
                        if int(waterfall_alert[1]) == metric_timestamp:
                            try:
                                self.redis_conn.srem(redis_set, str(waterfall_alert))
                                logger.info('removed waterfall alert item from Redis set %s - %s' % (
                                    redis_set, str(waterfall_alert)))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to remove waterfall alert item for %s at %s from Redis set %s' % (
                                    base_name, str(metric_timestamp), redis_set))

            # If crucible is enabled - save timeseries and create a
            # crucible check
            if settings.ENABLE_CRUCIBLE and settings.MIRAGE_CRUCIBLE_ENABLED:
                from_timestamp = str(int(timeseries[1][0]))
                timeseries_dir = base_name.replace('.', '/')
                crucible_anomaly_dir = str(settings.CRUCIBLE_DATA_FOLDER) + '/' + timeseries_dir + '/' + metric_timestamp
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
                       # @modified 20200607 - Feature #3566: custom_algorithms
                       # str(int_metric_timestamp), str(settings.MIRAGE_ALGORITHMS),
                       str(int_metric_timestamp), str(algorithms_run),
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
                    logger.error(traceback.format_exc())

                # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # Moved from the above self.sent_to_crucible
                redis_set = 'mirage.sent_to_crucible'
                data = str(base_name)
                try:
                    self.redis_conn.sadd(redis_set, data)
                except:
                    logger.error(traceback.format_exc())
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
                    logger.error(traceback.format_exc())

                # Create a crucible check file
                crucible_check_file = '%s/%s.%s.txt' % (settings.CRUCIBLE_CHECK_PATH, metric_timestamp, sane_metricname)
                try:
                    write_data_to_file(
                        skyline_app, crucible_check_file, 'w',
                        crucible_anomaly_data)
                    logger.info('added crucible check :: %s,%s' % (base_name, metric_timestamp))
                except:
                    logger.error('error :: failed to add crucible check file :: %s' % (crucible_check_file))
                    logger.error(traceback.format_exc())

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
                        full_duration, str(ionosphere_parent_id),
                        # @added 20201001 - Task #3748: POC SNAB
                        # Added algorithms_run required to determine the anomalyScore
                        # so this needs to be sent to Ionosphere so Ionosphere
                        # can send it back on an alert
                        algorithms_run)
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # Moved to the mirage.sent_to_ionosphere Redis set Redis set
                    # block below
                    # self.sent_to_ionosphere.append(base_name)

                    # @added 20200804 - Feature #3462: Add IONOSPHERE_MANAGE_PURGE
                    #                   Feature #3472: ionosphere.training_data Redis set
                    #                   Feature #3474: webapp api - training_data
                    # Add training data to the ionosphere.training_data so that
                    # the ionosphere purge_old_data_dirs can happen less
                    # frequently for reduced I/O
                    redis_set = 'ionosphere.training_data'
                    data = [base_name, int(int_metric_timestamp), second_order_resolution_seconds]
                    try:
                        logger.info('adding to Redis set %s - %s' % (
                            redis_set, str(data)))
                        self.redis_conn.sadd(redis_set, str(data))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add %s to %s Redis set' % (str(data), redis_set))
                else:
                    logger.info('alert expiry key exists not sending to Ionosphere :: %s' % base_name)

                # @added 20200904 - Feature #3734: waterfall alerts
                # Remove the metric from the waterfall_alerts Redis set
                # [metric, timestamp, value, added_to_waterfall_timestamp]
                # waterfall_data = [metric[1], metric[2], metric[0], added_to_waterfall_timestamp, waterfall_panorama_data]
                # Do not remove if this is only for training_data creation
                if redis_metric_name in ionosphere_unique_metrics:
                    redis_set = 'analyzer.waterfall_alerts.sent_to_mirage'
                    mirage_waterfall_data = []
                    for waterfall_alert in analyzer_waterfall_alerts:
                        if waterfall_alert[0] == base_name:
                            if int(waterfall_alert[1]) == metric_timestamp:
                                mirage_waterfall_data = waterfall_alert
                                try:
                                    self.redis_conn.srem(redis_set, str(waterfall_alert))
                                    logger.info('removed waterfall alert item from Redis set %s - %s' % (
                                        redis_set, str(waterfall_alert)))
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to remove waterfall alert item for %s at %s from Redis set %s' % (
                                        base_name, str(metric_timestamp), redis_set))

                # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # Moved from the above self.sent_to_ionosphere
                if not last_alert:
                    redis_set = 'mirage.sent_to_ionosphere'
                    data = str(base_name)
                    try:
                        self.redis_conn.sadd(redis_set, data)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add %s to Redis set %s' % (
                            str(data), str(redis_set)))

                    # @added 20200904 - Feature #3734: waterfall alerts
                    # Add mirage waterfall alert
                    # Only add if this is an ionosphere_enabled metric_check_file
                    if redis_metric_name in ionosphere_unique_metrics:
                        if mirage_waterfall_data:
                            # waterfall_data = [metric[1], metric[2], metric[0], added_to_waterfall_timestamp, waterfall_panorama_data]
                            waterfall_data = mirage_waterfall_data

                            # @added 20201008 - Bug #3776: waterfall_alert - no analyzer triggered_algorithms in waterfall_panorama_data on MIRAGE_ALWAYS_METRICS
                            # When a MIRAGE_ALWAYS_METRICS on metric is sent
                            # through to Mirage from Analyzer the sometimes has
                            # no algorithms that triggered_algorithms as the
                            # metric is sent every run, this can be expected.
                            # However if the Mirage three-sigma check does
                            # trigger algorithms they need to be added here so
                            # that when metric and event are sent to Panorama
                            # the triggered_algorithms is populated
                            if base_name in MIRAGE_ALWAYS_METRICS:
                                from_timestamp = str(int(timeseries[1][0]))
                                waterfall_panorama_data = [
                                    base_name, datapoint, int(timeseries[1][0]),
                                    int(timeseries[-1][0]), algorithms_run,
                                    triggered_algorithms, skyline_app,
                                    skyline_app, this_host,
                                    waterfall_alert[4][9]
                                ]
                                # Use the original added_to_waterfall_timestamp
                                added_to_waterfall_timestamp = waterfall_data[3]
                                # @modified 20201009 - Bug #3776: waterfall_alert - no analyzer triggered_algorithms in waterfall_panorama_data on MIRAGE_ALWAYS_METRICS
                                # corrected datapoint, timestamp order
                                waterfall_data = [
                                    base_name, int(timeseries[-1][0]), datapoint,
                                    added_to_waterfall_timestamp, waterfall_panorama_data
                                ]

                            redis_set = 'mirage.waterfall_alerts.sent_to_ionosphere'
                            try:
                                self.redis_conn.sadd(redis_set, str(waterfall_data))
                                logger.info('added to Redis set %s - %s' % (redis_set, str(waterfall_data)))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to add %s to Redis set %s' % (
                                    str(waterfall_data), str(redis_set)))

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

        # @added 20200723 - Feature #3472: ionosphere.training_data Redis set
        #                   Feature #3566: custom_algorithms
        # Optimize for MIRAGE_ALWAYS_METRICS which can create a lot
        # of training_data dirs a Analyzer always hands them off to
        # mirage.
        remove_ionosphere_data_dir = False
        if not anomalous:
            if base_name in MIRAGE_ALWAYS_METRICS:
                remove_ionosphere_data_dir = True
        if not anomalous and periodic_mirage_check:
            remove_ionosphere_data_dir = True

        # @added 20190408 - Feature #2882: Mirage - periodic_check
        # Remove the training_dir for mirage_periodic_check_metrics if not
        # anomalous
        # @modified 20200723 - Feature #3472: ionosphere.training_data Redis set
        #                   Feature #3566: custom_algorithms
        # if not anomalous and periodic_mirage_check:
        if remove_ionosphere_data_dir:
            timeseries_dir = base_name.replace('.', '/')
            training_dir = '%s/%s/%s' % (
                settings.IONOSPHERE_DATA_FOLDER, str(metric_timestamp),
                str(timeseries_dir))
            if os.path.exists(training_dir):
                try:
                    rmtree(training_dir)
                    logger.info('removed Mirage always or periodic check training_data dir - %s' % training_dir)
                except:
                    logger.error('error :: failed to rmtree Mirage always or periodic check training_dir - %s' % training_dir)
            if not anomalous and periodic_mirage_check:
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

        # @modified 20210304 - Feature #3642: Anomaly type classification
        #                      Feature #3970: custom_algorithm - adtk_level_shift
        # Added triggered_algorithms
        def smtp_trigger_alert(alert, metric, second_order_resolution_seconds, context, triggered_algorithms):
            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            try:
                # @modified 20210304 - Feature #3642: Anomaly type classification
                #                      Feature #3970: custom_algorithm - adtk_level_shift
                # Added triggered_algorithms
                p = Process(target=self.spawn_alerter_process, args=(alert, metric, second_order_resolution_seconds, context, triggered_algorithms))
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

        # @added 20200903 - Task #3730: Validate Mirage running multiple processes
        last_sent_to_graphite = int(time())

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
                # @modified 20191113 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # if settings.REDIS_PASSWORD:
                #     self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                # else:
                #     self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                try:
                    self.redis_conn = get_redis_conn(skyline_app)
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                except:
                    logger.info('falied to connect to Redis')
                if self.redis_conn.ping():
                    logger.info('connected to redis')
                continue

            """
            Determine if any metric to analyze or Ionosphere alerts to be sent
            """
            while True:

                # Report app up
                # @modified 20210524 - Branch #1444: thunder
                # Report app AND Redis as up
                # self.redis_conn.setex(skyline_app, 120, now)
                try:
                    redis_is_up = self.redis_conn.setex(skyline_app, 120, now)
                    if redis_is_up:
                        try:
                            self.redis_conn.setex('redis', 120, now)
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: could not update the Redis redis key - %s' % (
                                e))
                except Exception as e:
                    logger.error('error :: failed to update Redis key for %s up - %s' % (skyline_app, e))

                # @added 20200604 - Mirage - populate_redis
                # This functionality enables Mirage to populate the Skyline
                # Redis instance with FULL_DURATION data from Graphite if
                # Analyzer flags the time series as TooShort and adds it too the
                # mirage.populate_redis Redis set. Or possibly if there are
                # airgaps in the Redis data due to a network partition.  It will
                # fill a metric about every 10 seconds or so, unless there are
                # Mirage checks or ionosphere_alerts to send
                populate_redis_with_metrics = []
                if MIRAGE_AUTOFILL_TOOSHORT:
                    try:
                        populate_redis_with_metrics = list(self.redis_conn_decoded.smembers('mirage.populate_redis'))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to get Redis set mirage.populate_redis')
                        populate_redis_with_metrics = []
                metric_to_populate_redis = None
                populate_redis_with_metrics_count = 0
                if populate_redis_with_metrics:
                    populate_redis_with_metrics_count = len(populate_redis_with_metrics)
                    logger.info('%s metrics found in mirage.populate_redis Redis set' % str(populate_redis_with_metrics_count))
                    try:
                        metric_to_populate_redis = str(populate_redis_with_metrics[0])
                        try:
                            del populate_redis_with_metrics
                        except:
                            pass
                        logger.info('processing %s from mirage.populate_redis Redis set' % metric_to_populate_redis)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to determine metric to populate_redis')
                        metric_to_populate_redis = None
                if metric_to_populate_redis:
                    try:
                        # Spawn a populate_redis processes
                        pids = []
                        spawned_pids = []
                        p = Process(target=self.populate_redis, args=(1, metric_to_populate_redis))
                        pids.append(p)
                        logger.info('starting populate_redis process')
                        p.start()
                        spawned_pids.append(p.pid)
                        p_starts = time()
                        while time() - p_starts <= 10:
                            if any(p.is_alive() for p in pids):
                                # Just to avoid hogging the CPU
                                sleep(.1)
                            else:
                                # All the processes are done, break now.
                                time_to_run = time() - p_starts
                                logger.info('populate_redis process completed in %.2f seconds' % (
                                    time_to_run))
                                break
                        else:
                            # We only enter this if we didn't 'break' above.
                            logger.info('timed out, killing populate_redis process')
                            for p in pids:
                                p.terminate()
                                # p.join()
                        for p in pids:
                            if p.is_alive():
                                logger.info('stopping populate_redis process - %s' % (str(p.is_alive())))
                                p.join()
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to spawn populate_redis process')

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
                        # @modified 20200430 - Bug #3266: py3 Redis binary objects not strings
                        #                      Branch #3262: py3
                        # ionosphere_alerts = list(self.redis_conn.scan_iter(match='ionosphere.mirage.alert.*'))
                        ionosphere_alerts = list(self.redis_conn_decoded.scan_iter(match='ionosphere.mirage.alert.*'))
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
                            # @added 20210304 - Feature #3642: Anomaly type classification
                            #                   Feature #3970: custom_algorithm - adtk_level_shift
                            triggered_algorithms = ['testing']

                            if settings.SLACK_ENABLED and test_alerter == 'slack':
                                logger.info('test alert to slack for %s' % (metric_name))
                                # @modified 20210304 - Feature #3642: Anomaly type classification
                                #                      Feature #3970: custom_algorithm - adtk_level_shift
                                # Added triggered_algorithms
                                trigger_alert(alert, metric, 604800, skyline_app, triggered_algorithms)
                            if test_alerter == 'smtp':
                                # @modified 20210304 - Feature #3642: Anomaly type classification
                                #                      Feature #3970: custom_algorithm - adtk_level_shift
                                # Added triggered_algorithms
                                smtp_trigger_alert(alert, metric, 604800, skyline_app, triggered_algorithms)
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
                        # @modified 20200604 - Mirage - populate_redis
                        # Do not sleep if there are metrics to populate in Redis
                        if populate_redis_with_metrics_count == 0:
                            # @added 20200903 - Task #3730: Validate Mirage running multiple processes
                            sleep_for = 10
                            next_send_to_graphite = last_sent_to_graphite + 60
                            seconds_to_next_send_to_graphite = next_send_to_graphite - int(time())
                            if seconds_to_next_send_to_graphite < 10:
                                if seconds_to_next_send_to_graphite > 1:
                                    sleep_for = seconds_to_next_send_to_graphite
                                else:
                                    break

                            logger.info('sleeping no metrics...')
                            # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                            # sleep(10)
                            sleep(sleep_for)
                        else:
                            logger.info('no checks or alerts, continuing to process populate_redis metrics')
                # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                # Removed sleep, no delay
                # else:
                #    sleep(1)

                # @added 20220113 - Feature #3486: analyzer_batch
                #                   Feature #3480: batch_processing
                # Do not remove batch_processing checks
                batch_processing_metrics = []
                try:
                    batch_processing_metrics = list(self.redis_conn_decoded.smembers('aet.analyzer.batch_processing_metrics'))
                except:
                    logger.error('error :: Analyzer could not get aet.analyzer.batch_processing_metrics from Redis')
                    logger.error(traceback.format_exc())

                # Clean up old files
                now_timestamp = time()
                stale_age = now_timestamp - settings.MIRAGE_STALE_SECONDS
                for current_file in listdir(settings.MIRAGE_CHECK_PATH):
                    if os.path.isfile(settings.MIRAGE_CHECK_PATH + "/" + current_file):
                        t = os.stat(settings.MIRAGE_CHECK_PATH + "/" + current_file)
                        c = t.st_ctime

                        # @added 20220113 - Feature #3486: analyzer_batch
                        #                   Feature #3480: batch_processing
                        # Do not remove batch_processing checks
                        for b_metric in batch_processing_metrics:
                            if b_metric in current_file:
                                continue

                        # delete file if older than a week
                        if c < stale_age:
                            os.remove(settings.MIRAGE_CHECK_PATH + "/" + current_file)
                            logger.info('removed stale check - %s' % (current_file))
                            # @added 20200903 - Task #3730: Validate Mirage running multiple processes
                            redis_set = 'mirage.stale_check_discarded'
                            try:
                                self.redis_conn.sadd(redis_set, str(current_file))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to add %s to Redis set %s' % (
                                    str(current_file), str(redis_set)))

                # @added 20201026 - Task #3800: Handle feedback metrics in Mirage and waterfall alerts
                # Handle feedback metrics in a similar style to Ionosphere
                # Do not run checks if namespace has matched multiple times in
                # the last 10 minutes.
                if len(metric_var_files) > 3:
                    analyzer_waterfall_alerts = []
                    feedback_metric_loop_error_logged = False
                    for current_file in listdir(settings.MIRAGE_CHECK_PATH):
                        feedback_metric = False
                        remove_feedback_metric_check = False
                        remove_alerted_on_metric_check = False
                        try:
                            current_file_no_extension = current_file.replace('.txt', '')
                            current_file_no_extension_elements = current_file_no_extension.split('.')
                            base_name = '.'.join(current_file_no_extension_elements[1:])
                            metric_timestamp = int(current_file_no_extension_elements[0])
                        except:
                            pass
                        try:
                            metric_namespace_elements = base_name.split('.')
                            for to_skip in SKYLINE_FEEDBACK_NAMESPACES:
                                if to_skip in base_name:
                                    feedback_metric = True
                                    break
                                to_skip_namespace_elements = to_skip.split('.')
                                elements_matched = set(metric_namespace_elements) & set(to_skip_namespace_elements)
                                if len(elements_matched) == len(to_skip_namespace_elements):
                                    feedback_metric = True
                                    break
                            feedback_cache_key_exists = False
                            feedback_cache_key = 'mirage.feedback_metric.checked.%s' % (base_name)
                            feedback_metric_process_time = int(time())

                            # @added 20210701 - Feature #4152: DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES
                            if feedback_metric:
                                for do_not_skip in DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES:
                                    pattern_match = False
                                    try:
                                        pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES)
                                        del metric_matched_by
                                    except Exception as e:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: matched_or_regexed_in_list failed checking %s in DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES - %s' % (
                                            base_name, e))
                                        pattern_match = False
                                    if pattern_match:
                                        feedback_metric = False
                                        logger.info('%s matched DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES, will analyse' % base_name)

                            if feedback_metric:
                                try:
                                    feedback_cache_key_exists = self.redis_conn_decoded.get(feedback_cache_key)
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to get %s key from Redis' % (
                                        str(feedback_cache_key)))
                                if feedback_cache_key_exists:
                                    feedback_metric_last_processed_seconds_ago = feedback_metric_process_time - int(feedback_cache_key_exists)
                                    logger.info('feedback metric identified as last processed %s seconds ago via Redis key %s' % (
                                        str(feedback_metric_last_processed_seconds_ago), feedback_cache_key))
                                    remove_feedback_metric_check = True

                                    # @added 20210505 - Bug #4048: Mirage - removing feedback metrics to be processed
                                    feedback_processed_cache_key = 'mirage.feedback_metric.processed.%s' % (base_name)
                                    feedback_processed_cache_key_exists = None
                                    try:
                                        feedback_processed_cache_key_exists = self.redis_conn_decoded.get(feedback_processed_cache_key)
                                    except Exception as e:
                                        logger.error('error :: failed to get %s key from Redis - %s' % (
                                            str(feedback_processed_cache_key), e))
                                    if not feedback_processed_cache_key_exists:
                                        remove_feedback_metric_check = False
                                        logger.info('feedback metric Redis key %s does not exist, not removing metric' % feedback_processed_cache_key)

                                if len(metric_var_files) > 10 and not feedback_cache_key_exists:
                                    logger.info('Mirage is busy removing feedback metric check')
                                    remove_feedback_metric_check = True
                                # @modified 20201128 - Feature #3734: waterfall alerts
                                # Only add if does not exist and always add
                                # else:
                                #        try:
                                #            self.redis_conn.setex(feedback_cache_key, 600, feedback_metric_process_time)
                                # if not feedback_cache_key_exists:
                                if not feedback_cache_key_exists and not remove_feedback_metric_check:
                                    logger.info('feedback metric identified as not processed in last 600 seconds adding Redis key with 600 TTL and processing - %s' % feedback_cache_key)
                                    try:
                                        self.redis_conn.setex(feedback_cache_key, 600, feedback_metric_process_time)
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: failed to add %s key to Redis' % (
                                            str(feedback_cache_key)))
                        except:
                            if not feedback_metric_loop_error_logged:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to check feedback and alerted on metrics')
                                feedback_metric_loop_error_logged = True
                        # Remove checks that have been alerted on by Mirage or
                        # via an Analyzer waterfall alert
                        if len(metric_var_files) > 10 and not remove_feedback_metric_check:
                            cache_key = 'mirage.last_alert.smtp.%s' % (base_name)
                            alerted_on = False
                            try:
                                alerted_on = self.redis_conn_decoded.get(cache_key)
                            except Exception as e:
                                logger.error('error :: could not query Redis for cache_key: %s' % str(e))
                            if not alerted_on:
                                # Check for Analyzer alert key from waterfall alert
                                cache_key = 'last_alert.smtp.%s' % (base_name)
                                try:
                                    alerted_on = self.redis_conn_decoded.get(cache_key)
                                except Exception as e:
                                    logger.error('error :: could not query Redis for cache_key: %s' % str(e))
                            if alerted_on:
                                remove_alerted_on_metric_check = True
                                # Unless is it older than PANORAMA_EXPIRY_TIME
                                try:
                                    alerted_on_at = int(alerted_on)
                                    alerted_on_seconds_ago = int(time()) - alerted_on_at
                                    if alerted_on_seconds_ago >= settings.PANORAMA_EXPIRY_TIME:
                                        remove_alerted_on_metric_check = False
                                except Exception as e:
                                    logger.error('error :: failed determining if alerted more than PANORAMA_EXPIRY_TIME seconds ago - %s' % str(e))
                                    remove_alerted_on_metric_check = True
                        if remove_feedback_metric_check or remove_alerted_on_metric_check:
                            if remove_feedback_metric_check:
                                log_str = 'feedback metric'
                            if remove_alerted_on_metric_check:
                                log_str = 'alerted on metric'
                            logger.info('removing %s %s check file and from analyzer.waterfall_alerts.sent_to_mirage Redis set' % (
                                log_str, base_name))
                            try:
                                os.remove(settings.MIRAGE_CHECK_PATH + "/" + current_file)
                            except:
                                logger.error('error :: failed to remove %s %s check file - %s' % (
                                    log_str, base_name, current_file))
                            # Remove the metric from the waterfall_alerts Redis set
                            # waterfall_data = [metric[1], metric[2], metric[0], added_to_waterfall_timestamp. waterfall_panorama_data]
                            if not analyzer_waterfall_alerts:
                                redis_set = 'analyzer.waterfall_alerts.sent_to_mirage'
                                literal_analyzer_waterfall_alerts = []
                                try:
                                    literal_analyzer_waterfall_alerts = list(self.redis_conn_decoded.smembers(redis_set))
                                except:
                                    literal_analyzer_waterfall_alerts = []
                                analyzer_waterfall_alerts = []
                                for literal_waterfall_alert in literal_analyzer_waterfall_alerts:
                                    waterfall_alert = literal_eval(literal_waterfall_alert)
                                    analyzer_waterfall_alerts.append(waterfall_alert)
                            for waterfall_alert in analyzer_waterfall_alerts:
                                if waterfall_alert[0] == base_name:
                                    if int(waterfall_alert[1]) == metric_timestamp:
                                        try:
                                            self.redis_conn.srem(redis_set, str(waterfall_alert))
                                            logger.info('removed waterfall alert item for %s from Redis set %s - %s' % (
                                                log_str, redis_set, str(waterfall_alert)))
                                            # @modified 20201128 - Feature #3734: waterfall alerts
                                            # Do not break, check and remove
                                            # waterfall_alert items with older
                                            # timestamps as well
                                            # break
                                        except:
                                            logger.error(traceback.format_exc())
                                            logger.error('error :: failed to remove waterfall alert item for %s %s at %s from Redis set %s' % (
                                                log_str, base_name, str(metric_timestamp), redis_set))
                                # @added 20201128 - Feature #3734: waterfall alerts
                                # If the check just done is new than an existing analyzer
                                # waterfall alert metric timestamp remove those keys as well
                                if int(waterfall_alert[1]) < metric_timestamp:
                                    try:
                                        self.redis_conn.srem(redis_set, str(waterfall_alert))
                                        logger.info('removed waterfall alert item with older timestamp for %s from Redis set %s - %s' % (
                                            log_str, redis_set, str(waterfall_alert)))
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: failed to remove waterfall alert item for %s %s at %s from Redis set %s' % (
                                            log_str, base_name, str(metric_timestamp), redis_set))

                # Discover metric to analyze
                metric_var_files = ''
                # @added 20161228 - Feature #1830: Ionosphere alerts
                # Prioritises Ionosphere alerts
                if ionosphere_alerts_returned:
                    break

                metric_var_files = [f for f in listdir(settings.MIRAGE_CHECK_PATH) if isfile(join(settings.MIRAGE_CHECK_PATH, f))]
                if len(metric_var_files) > 0:
                    break

            process_metric_check_files = False

            # @modified 20161228 - Feature #1830: Ionosphere alerts
            # Only spawn process if this is not an Ionosphere alert
            if not ionosphere_alerts_returned:
                metric_var_files_sorted = sorted(metric_var_files)
                # metric_check_file = settings.MIRAGE_CHECK_PATH + "/" + metric_var_files_sorted[0]
                if metric_var_files_sorted:
                    process_metric_check_files = True

            if process_metric_check_files:

                # @added 20200903 - Task #3730: Validate Mirage running multiple processes
                check_files_to_process = len(metric_var_files_sorted)
                logger.info('%s checks to process' % str(check_files_to_process))

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

                # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                # MIRAGE_PROCESSES = 1
                if len(metric_var_files) > 1:
                    try:
                        MIRAGE_PROCESSES = int(settings.MIRAGE_PROCESSES)
                        if len(metric_var_files) < MIRAGE_PROCESSES:
                            MIRAGE_PROCESSES = len(metric_var_files)
                    except:
                        MIRAGE_PROCESSES = 1
                else:
                    MIRAGE_PROCESSES = 1

                # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                # processing_check_file = metric_var_files_sorted[0]
                # logger.info('processing %s' % processing_check_file)
                # @modified 20200909 - Task #3730: Validate Mirage running multiple processes
                # for i in range(1, MIRAGE_PROCESSES + 1):
                #     up_to = i - 1
                #     processing_check_file = metric_var_files_sorted[up_to]
                #     logger.info('processing %s' % processing_check_file)

                # @modified 20161224 - send mirage metrics to graphite
                # run_timestamp = int(now)
                run_timestamp = int(time())
                for i in range(1, MIRAGE_PROCESSES + 1):

                    # @added 20200909 - Task #3730: Validate Mirage running multiple processes
                    up_to = i - 1
                    processing_check_file = metric_var_files_sorted[up_to]
                    logger.info('processing %s' % processing_check_file)
                    # @modified 20200909 - Task #3730: Validate Mirage running multiple processes
                    # p = Process(target=self.spin_process, args=(i, run_timestamp, metric_var_files_sorted))
                    p = Process(target=self.spin_process, args=(i, run_timestamp, processing_check_file))
                    pids.append(p)
                    pid_count += 1
                    logger.info('starting %s of %s spin_process/es' % (str(pid_count), str(MIRAGE_PROCESSES)))
                    p.start()
                    # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                    # spawned_pids.append(p.pid)
                    spawned_pids.append([p.pid, i])
                    logger.info('started spin_process %s with pid %s' % (str(pid_count), str(p.pid)))

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

                # @added 20200607 - Feature #3508: ionosphere.untrainable_metrics
                # Check to non 3sigma algorithm errors too
                if LOCAL_DEBUG:
                    logger.debug('debug :: adding negatives_present to check_algorithm_errors')
                check_algorithm_errors = ['negatives_present']
                for algorithm in list(settings.MIRAGE_ALGORITHMS):
                    if LOCAL_DEBUG or DEBUG_CUSTOM_ALGORITHMS:
                        logger.debug('debug :: adding %s to check_algorithm_errors' % (algorithm))
                    check_algorithm_errors.append(algorithm)
                # @added 20200607 - Feature #3566: custom_algorithms
                if CUSTOM_ALGORITHMS:
                    for custom_algorithm in settings.CUSTOM_ALGORITHMS:
                        if LOCAL_DEBUG or DEBUG_CUSTOM_ALGORITHMS:
                            logger.debug('debug :: adding custom_algorithm %s to check_algorithm_errors' % (custom_algorithm))
                        check_algorithm_errors.append(custom_algorithm)

                if LOCAL_DEBUG or DEBUG_CUSTOM_ALGORITHMS:
                    logger.debug('debug :: checking for algorithm error files')

                # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                # for completed_pid in spawned_pids:
                for completed_pid, mirage_process in spawned_pids:
                    logger.info('spin_process with pid %s completed' % (str(completed_pid)))
                    # @modified 20200607 - Feature #3566: custom_algorithms
                    #                      Feature #3508: ionosphere.untrainable_metrics
                    # Check to non 3sigma algorithm errors too and wrapped in try
                    try:
                        # for algorithm in settings.MIRAGE_ALGORITHMS:
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
                    if LOCAL_DEBUG or DEBUG_CUSTOM_ALGORITHMS:
                        logger.debug('debug :: checked for algorithm error files')

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
                            # @added 20191113 - Branch #3262: py3
                            # Log
                            logger.info('anomaly_breakdown.keys are empty')
                            break

                    while 1:
                        try:
                            key, value = self.mirage_exceptions_q.get_nowait()
                            if key not in exceptions.keys():
                                exceptions[key] = value
                            else:
                                exceptions[key] += value
                        except Empty:
                            # @added 20191113 - Branch #3262: py3
                            # Log
                            logger.info('exceptions.keys are empty')
                            break

                    # @added 20191021 - Bug #3288: Always send anomaly_breakdown and exception metrics
                    #                   Branch #3262: py3
                    exceptions_metrics = ['Boring', 'Stale', 'TooShort', 'Other']
                    for i_exception in exceptions_metrics:
                        if i_exception not in exceptions.keys():
                            exceptions[i_exception] = 0
                    # @modified 20200607 - Feature #3566: custom_algorithms
                    # for i_anomaly_breakdown in settings.MIRAGE_ALGORITHMS:
                    for i_anomaly_breakdown in check_algorithm_errors:
                        if i_anomaly_breakdown not in anomaly_breakdown.keys():
                            anomaly_breakdown[i_anomaly_breakdown] = 0

                    # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # Use Redis set and not self.metric_variables

                    metric_variables = []
                    # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # literal_metric_variables = list(self.redis_conn.smembers('mirage.metric_variables'))
                    # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                    # Handle per process
                    # literal_metric_variables = list(self.redis_conn_decoded.smembers('mirage.metric_variables'))
                    metric_variable_redis_set = 'mirage.%s.metric_variables' % str(mirage_process)
                    literal_metric_variables = list(self.redis_conn_decoded.smembers(metric_variable_redis_set))
                    for item_list_string in literal_metric_variables:
                        list_item = literal_eval(item_list_string)
                        metric_variables.append(list_item)

                    # @added 20200903 - Task #3730: Validate Mirage running multiple processes
                    # Handle per process
                    try:
                        self.redis_conn.delete(metric_variable_redis_set)
                        logger.info('deleted Redis set - %s' % metric_variable_redis_set)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to delete Redis set - %s' % metric_variable_redis_set)

                    # @added 20191113 - Branch #3262: py3
                    # Set default values
                    metric_name = None
                    metric_value = None
                    hours_to_resolve = 0

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

                    logger.info('analysis done - %s' % str(metric_name))

                    # Send alerts
                    # Calculate hours second order resolution to seconds
                    # @modified 20191113 - Branch #3262: py3
                    # Only if set
                    if hours_to_resolve:
                        logger.info('analyzed at %s hours resolution' % hours_to_resolve)
                        second_order_resolution_seconds = int(hours_to_resolve) * 3600
                        logger.info('analyzed at %s seconds resolution' % str(second_order_resolution_seconds))

                    # Remove metric check file
                    metric_check_file = 'None'
                    try:
                        metric_check_file = '%s/%s' % (settings.MIRAGE_CHECK_PATH, processing_check_file)
                        if LOCAL_DEBUG:
                            logger.debug('debug :: interpolated metric_check_file to %s' % metric_check_file)
                    except:
                        logger.error('error :: failed to interpolate metric_check_file')

                    if os.path.isfile(metric_check_file):
                        try:
                            os.remove(metric_check_file)
                            logger.info('removed check file - %s' % metric_check_file)
                        except OSError:
                            if LOCAL_DEBUG:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to remove metric_check_file - %s' % metric_check_file)
                            pass
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to remove metric_check_file - %s' % metric_check_file)
                    else:
                        if LOCAL_DEBUG:
                            logger.debug('debug :: no metric_check_file to remove OK - %s' % metric_check_file)

                    # Remove the metric directory
                    # @modified 20191113 - Branch #3262: py3
                    # Convert None to str
                    # timeseries_dir = metric_name.replace('.', '/')
                    metric_data_dir = 'None'
                    try:
                        metric_name_str = str(metric_name)
                        timeseries_dir = metric_name_str.replace('.', '/')
                        metric_data_dir = '%s/%s' % (settings.MIRAGE_CHECK_PATH, timeseries_dir)
                        if LOCAL_DEBUG:
                            logger.debug('debug :: metric_data_dir interpolated to %s' % str(metric_data_dir))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to interpolate metric_data_dir')
                        metric_data_dir = 'None'

                    if os.path.exists(metric_data_dir):
                        try:
                            rmtree(metric_data_dir)
                            logger.info('removed - %s' % metric_data_dir)
                        except:
                            logger.error('error :: failed to rmtree %s' % metric_data_dir)
                    else:
                        if LOCAL_DEBUG:
                            logger.debug('debug :: metric_data_dir does not exist - %s' % str(metric_data_dir))

                ionosphere_unique_metrics = []
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
                else:
                    if LOCAL_DEBUG:
                        logger.debug('debug :: settings.MIRAGE_ENABLE_ALERTS is not True')

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
                    # @modified 20200430 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # ionosphere_alert_on = list(self.redis_conn.scan_iter(match='ionosphere.mirage.alert.*'))
                    ionosphere_alert_on = list(self.redis_conn_decoded.scan_iter(match='ionosphere.mirage.alert.*'))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to get ionosphere.mirage.alert.* from Redis key scan')
                    ionosphere_alert_on = []

                for cache_key in ionosphere_alert_on:
                    try:
                        # @modified 20200322 - Bug #3266: py3 Redis binary objects not strings
                        #                      Branch #3262: py3
                        # alert_on = self.redis_conn.get(cache_key)
                        alert_on = self.redis_conn_decoded.get(cache_key)
                        send_alert_for = literal_eval(alert_on)
                        value = float(send_alert_for[0])
                        base_name = str(send_alert_for[1])
                        metric_timestamp = int(float(send_alert_for[2]))
                        triggered_algorithms = send_alert_for[3]
                        # @added 20201001 - Task #3748: POC SNAB
                        # Added algorithms_run required to determine the anomalyScore
                        algorithms_run = send_alert_for[5]

                        second_order_resolution_seconds = int(send_alert_for[4])

                        # @modified 20201007 - Feature #3772: Add the anomaly_id to the http_alerter json
                        #                      Branch #3068: SNAB
                        # Added triggered_algorithms and algorithms_run
                        # anomalous_metric = [value, base_name, metric_timestamp, second_order_resolution_seconds]
                        anomalous_metric = [value, base_name, metric_timestamp, second_order_resolution_seconds, triggered_algorithms, algorithms_run]

                        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        # self.anomalous_metrics.append(anomalous_metric)
                        redis_set = 'mirage.anomalous_metrics'
                        data = str(anomalous_metric)
                        try:
                            self.redis_conn.sadd(redis_set, data)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to add %s to Redis set %s' % (
                                str(data), str(redis_set)))

                        # @added 20210323 - Feature #3642: Anomaly type classification
                        if LUMINOSITY_CLASSIFY_ANOMALIES:
                            redis_set = 'luminosity.classify_anomalies'
                            added_at = int(time())
                            data_dict = {
                                'metric': base_name,
                                'timestamp': int(metric_timestamp),
                                'value': value,
                                'algorithms': algorithms_run,
                                'triggered_algorithms': triggered_algorithms,
                                'app': skyline_app,
                                'added_at': added_at,
                            }
                            data = [base_name, int(metric_timestamp), added_at, data_dict]
                            try:
                                self.redis_conn.sadd(redis_set, str(data))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to add %s to Redis set %s' % (
                                    str(data), str(redis_set)))

                        anomaly_breakdown = dict()
                        for algorithm in triggered_algorithms:
                            anomaly_breakdown[algorithm] = 1
                        self.redis_conn.delete(cache_key)
                    except:
                        logger.error(traceback.format_exc())
                        # @modified 20200322 - Bug #3266: py3 Redis binary objects not strings
                        #                      Branch #3262: py3
                        # logger.error('error :: failed to add an Ionosphere anomalous_metric for %s' % base_name)
                        logger.error('error :: failed to add an Ionosphere anomalous_metric for cache key %s' % cache_key)
            else:
                if LOCAL_DEBUG:
                    logger.debug('debug :: no ionosphere_alerts_returned - %s' % str(ionosphere_alerts_returned))

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

            # @added 20200907 - Feature #3734: waterfall alerts
            # Add alert for expired waterfall_alert items
            # [metric, timestamp, value, added_to_waterfall_timestamp]
            # waterfall_data = [metric[1], metric[2], metric[0], added_to_waterfall_timestamp, waterfall_panorama_data]
            waterfall_alert_check_timestamp = int(time())
            waterfall_alerts_to_alert_on = []
            # A list to add a metric,timestamp string to in order to override
            # the ionosphere_metric in the alerting block
            alerting_waterfall_alerts = []
            waterfall_redis_sets = [
                'mirage.waterfall_alerts.sent_to_ionosphere',
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
                            waterfall_alerts_to_alert_on.append(waterfall_alert)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to remove feedback metric waterfall alert item for %s from Redis set %s' % (
                                base_name, redis_set))
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
                    # mirage_anomalous_metrics.append([value, base_name, metric_timestamp])
                    anomalous_metric = [value, base_name, metric_timestamp, triggered_algorithms, algorithms_run]
                    mirage_anomalous_metrics.append(anomalous_metric)

                    waterfall_alert_check_string = '%s.%s' % (str(metric_timestamp), base_name)
                    alerting_waterfall_alerts.append(waterfall_alert_check_string)
                    logger.info('waterfall alerting on %s' % base_name)
                    redis_waterfall_alert_key = 'mirage.waterfall.alert.%s' % waterfall_alert_check_string
                    try:
                        self.redis_conn.setex(redis_waterfall_alert_key, 300, waterfall_alert_check_timestamp)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add Redis key - %s for waterfall alert' % (
                            redis_waterfall_alert_key))

                    # @added 20200929 - Feature #3734: waterfall alerts
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
                        logger.info('panorama anomaly data for waterfall alert - %s' % str(panorama_data))
                        # Create an anomaly file with details about the anomaly
                        sane_metricname = filesafe_metricname(str(base_name))
                        panaroma_anomaly_file = '%s/%s.%s.txt' % (
                            settings.PANORAMA_CHECK_PATH, added_at, sane_metricname)
                        try:
                            write_data_to_file(
                                skyline_app, panaroma_anomaly_file, 'w',
                                panaroma_anomaly_data)
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
                    logger.error('error :: failed to add waterfall alert to alert on to mirage_anomalous_metrics')

            # @added 20200913 - Branch #3068: SNAB
            #                   Task #3744: POC matrixprofile
            #                   Info #1792: Shapelet extraction
            snab_checks_sent = []
            added_to_snab_at = int(time())

            # @added 20200929 - Task #3748: POC SNAB
            #                   Branch #3068: SNAB
            # Added three-sigma snab.panorama items
            panorama_added_at = int(time())
            panorama_three_sigma_snab_added = []

            # @added 20200610 - Feature #3560: External alert config
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

            not_ionosphere_metrics = []

            # @modified 20200610 - Feature #3560: External alert config
            # for alert in settings.ALERTS:
            for alert in all_alerts:

                # @added 20181114 - Bug #2682: Reduce mirage ionosphere alert loop
                not_an_ionosphere_metric_check_done = 'none'

                if LOCAL_DEBUG:
                    logger.debug('debug :: %s metrics in mirage_anomalous_metrics' % str(len(mirage_anomalous_metrics)))

                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                # for metric in self.anomalous_metrics:
                for metric in mirage_anomalous_metrics:
                    # @added 20161228 - Feature #1830: Ionosphere alerts
                    #                   Branch #922: Ionosphere
                    # Bringing Ionosphere online - do alert on Ionosphere
                    # metrics if Ionosphere is up
                    try:
                        metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(metric[1]))
                        if LOCAL_DEBUG:
                            logger.debug('debug :: metric_name interpolated to %s' % str(metric_name))
                    except:
                        if LOCAL_DEBUG:
                            logger.error(traceback.format_exc())
                            logger.debug('debug :: failed to interpolate metric_name')

                    # @added 20200907 - Feature #3734: waterfall alerts
                    waterfall_alert_check_string = '%s.%s' % (str(int(metric[2])), metric[1])

                    # @modified 20200907 - Feature #3734: waterfall alerts
                    # if metric_name in ionosphere_unique_metrics:
                    if metric_name in ionosphere_unique_metrics and waterfall_alert_check_string not in alerting_waterfall_alerts:
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
                                if metric_name not in not_ionosphere_metrics:
                                    logger.info('not an Ionosphere metric checking whether to alert - %s' % str(metric[1]))
                                    not_an_ionosphere_metric_check_done = metric_name
                                    not_ionosphere_metrics.append(metric_name)
                    # ALERT_MATCH_PATTERN = alert[0]
                    # METRIC_PATTERN = metric[1]
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

                    if pattern_match:

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
                        # cache_key = 'mirage.last_alert.%s.%s' % (alert[1], metric[1])
                        # @modified 20200624 - Feature #3560: External alert config
                        # Set the alert key to the external alerter id
                        # if external_alerter_alerter:
                        #     cache_key = 'mirage.last_alert.%s.%s.%s' % (str(external_alerter_alerter), alert[1], metric[1])
                        if external_alerter_id:
                            cache_key = 'mirage.last_alert.%s.%s.%s' % (str(external_alerter_id), alert[1], metric[1])
                        else:
                            cache_key = 'mirage.last_alert.%s.%s' % (alert[1], metric[1])

                        try:
                            # @modified 20200805 - Task #3662: Change mirage.last_check keys to timestamp value
                            #                      Feature #3486: analyzer_batch
                            #                      Feature #3480: batch_processing
                            # Changed the last_alert cache key to hold the last
                            # anomaly timestamp
                            # last_alert = self.redis_conn.get(cache_key)
                            last_alert = self.redis_conn_decoded.get(cache_key)

                            # @added 20200805 - Task #3662: Change mirage.last_check keys to timestamp value
                            #                   Feature #3486: analyzer_batch
                            #                   Feature #3480: batch_processing
                            # Evaluate the reported anomaly timestamp to determine whether
                            # EXPIRATION_TIME should be applied to a batch metric
                            if last_alert:
                                # Is this a analyzer_batch related anomaly
                                analyzer_batch_anomaly = None
                                metric_timestamp = metric[2]
                                analyzer_batch_metric_anomaly_key = 'analyzer_batch.anomaly.%s.%s' % (
                                    str(metric_timestamp), metric[1])
                                try:
                                    analyzer_batch_anomaly = self.redis_conn_decoded.get(analyzer_batch_metric_anomaly_key)
                                except Exception as e:
                                    logger.error(
                                        'error :: could not query cache_key - %s - %s' % (
                                            analyzer_batch_metric_anomaly_key, e))
                                    analyzer_batch_anomaly = None
                                if analyzer_batch_anomaly:
                                    logger.info('identified as an analyzer_batch triggered anomaly from the presence of the Redis key %s' % analyzer_batch_metric_anomaly_key)
                                else:
                                    logger.info('not identified as an analyzer_batch triggered anomaly as no Redis key found - %s' % analyzer_batch_metric_anomaly_key)
                                if last_alert and analyzer_batch_anomaly:
                                    last_timestamp = None
                                    try:
                                        last_timestamp = int(last_alert)
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: failed to determine last_timestamp from the last Mirage alert key - %s' % cache_key)
                                        last_timestamp = None
                                    seconds_between_batch_anomalies = None
                                    if last_timestamp:
                                        try:
                                            seconds_between_batch_anomalies = int(metric_timestamp) - int(last_timestamp)
                                        except:
                                            logger.error(traceback.format_exc())
                                            logger.error('error :: failed to determine seconds_between_batch_anomalies for batch metric Panorama key- %s' % cache_key)
                                            last_timestamp = None
                                    if seconds_between_batch_anomalies:
                                        if seconds_between_batch_anomalies > int(alert[2]):
                                            logger.info('the difference between the last anomaly timestamp (%s) and the batch anomaly timestamp (%s) for batch metric %s is greater than the metric EXPIRATION_TIME of %s' % (
                                                str(last_timestamp), str(metric_timestamp), metric[1],
                                                str(alert[2])))
                                            logger.info('alerting on anomaly for batch metric %s, so setting last_alert to None' % (
                                                metric))
                                            last_alert = None
                                        else:
                                            logger.info('the difference between the last anomaly timestamp (%s) and the batch anomaly timestamp (%s) for batch metric %s is less than the metric EXPIRATION_TIME of %s, not alerting' % (
                                                str(last_timestamp), str(metric_timestamp), metric[1],
                                                str(alert[2])))

                                        # @added 20200805 - Task #3662: Change mirage.last_check keys to timestamp value
                                        #                   Task #3562: Change panorama.last_check keys to timestamp value
                                        #                   Feature #3486: analyzer_batch
                                        #                   Feature #3480: batch_processing
                                        # If the metric is a batch processing metric and the anomaly
                                        # timestamp is less than the last_check timestamp, insert
                                        # the anomaly
                                        if int(metric_timestamp) < last_timestamp:
                                            logger.info('batch anomaly timestamp (%s) less than the last_check timestamp (%s), alerting on anomaly for batch metric %s, so setting last_alert to None' % (
                                                str(metric_timestamp), str(last_timestamp), metric))
                                            last_alert = None

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
                                            second_order_resolution_seconds = int(settings.FULL_DURATION)
                                # @modified 20190524 - Branch #3002
                                # Wrapped in try except
                                try:
                                    # @modified 20200805 - Task #3662: Change mirage.last_check keys to timestamp value
                                    #                      Feature #3486: analyzer_batch
                                    #                      Feature #3480: batch_processing
                                    # Change the last_alert cache key to hold the
                                    # the anomaly timestamp for which the alert
                                    # was sent, not the packb anomaly value.
                                    # Using the timestamp of the anomaly allows
                                    # it to be used to determine if a batch
                                    # anomaly should be alerted on based on the
                                    # comparison of the timestamps rather than
                                    # just the presence of the last_alert key
                                    # based on it not having reach its TTL as
                                    # analyzer_batch could send multiple
                                    # anomalies in one batch that might be
                                    # EXPIRATION_TIME apart.
                                    # self.redis_conn.setex(cache_key, alert[2], packb(metric[0]))
                                    self.redis_conn.setex(cache_key, str(alert[2]), int(metric[2]))
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to set Redis key %s for %s' % (
                                        str(cache_key), metric[1]))

                                # @added 20200929 - Task #3748: POC SNAB
                                #                   Branch #3068: SNAB
                                # Determine if this is a snab_check_metric so
                                # that info can be passed to the alerter
                                snab_check_metric = False
                                if SNAB_ENABLED:
                                    for app in SNAB_CHECKS:
                                        if app == skyline_app:
                                            for snab_context in SNAB_CHECKS[app]:
                                                if snab_check_metric:
                                                    break
                                                if snab_context == 'testing':
                                                    for algorithm in SNAB_CHECKS[app][snab_context]:
                                                        try:
                                                            algorithm_source = SNAB_CHECKS[app][snab_context][algorithm]['algorithm_source']
                                                        except:
                                                            break
                                                        if not os.path.isfile(algorithm_source):
                                                            break
                                                        try:
                                                            for namespace in SNAB_CHECKS[app][snab_context][algorithm]['namespaces']:
                                                                if namespace in base_name:
                                                                    snab_check_metric = True
                                                                    break
                                                        except:
                                                            logger.error(traceback.format_exc())
                                                            logger.error('error :: failed to check if %s is a snab_check_metric' % base_name)
                                        if snab_check_metric:
                                            algorithm_group = 'three-sigma'

                                            # @added 20201007 - Feature #3772: Add the anomaly_id to the http_alerter json
                                            #                   Branch #3068: SNAB
                                            # Added second_order_resolution_seconds, triggered_algorithms and algorithms_run
                                            try:
                                                triggered_algorithms = metric[4]
                                                algorithms_run = metric[5]
                                            except:
                                                triggered_algorithms = []
                                                algorithms_run = []

                                            # @added 20201001 - Task #3748: POC SNAB
                                            # Added anomalyScore
                                            try:
                                                if triggered_algorithms and algorithms_run:
                                                    anomalyScore = len(triggered_algorithms) / len(algorithms_run)
                                                else:
                                                    anomalyScore = 1.0
                                            except:
                                                logger.error(traceback.format_exc())
                                                logger.error('error :: failed to determine anomalyScore for %s' % base_name)
                                                anomalyScore = 1.0
                                            # @added 20201001 - Branch #3068: SNAB
                                            #                      Task #3748: POC SNAB
                                            # Added analysis_run_time
                                            analysis_run_time = 0
                                            redis_key = 'mirage.analysis_run_time.%s.%s' % (base_name, str(metric[2]))
                                            try:
                                                analysis_run_time_data = self.redis_conn_decoded.get(redis_key)
                                                if analysis_run_time_data:
                                                    analysis_run_time = float(analysis_run_time_data)
                                            except:
                                                logger.error(traceback.format_exc())
                                                logger.error('error :: failed to determine analysis_run_time from Redis key - %s' % (
                                                    redis_key))

                                            try:
                                                snab_panorama_details = {
                                                    'metric': base_name,
                                                    'timestamp': int(metric[2]),
                                                    # @added 20201001 - Task #3748: POC SNAB
                                                    # Added anomalyScore and analysis_run_time
                                                    'anomalyScore': anomalyScore,
                                                    'analysis_run_time': analysis_run_time,
                                                    'source': skyline_app,
                                                    'algorithm_group': algorithm_group,
                                                    'algorithm': None,
                                                    'added_at': panorama_added_at,
                                                }
                                            except:
                                                logger.error(traceback.format_exc())
                                                logger.error('error :: failed to create snab_panorama_details dict %s' % base_name)
                                                snab_panorama_details = None

                                            # Only send once for a metric and timestamp
                                            if snab_panorama_details not in panorama_three_sigma_snab_added:
                                                self.redis_conn.sadd('snab.panorama', str(snab_panorama_details))
                                                logger.info('added snab.panorama three-sigma item - %s' % (
                                                    str(snab_panorama_details)))
                                                panorama_three_sigma_snab_added.append(snab_panorama_details)

                                # @added 20210801 - Feature #4214: alert.paused
                                alert_paused = False
                                try:
                                    cache_key = 'alert.paused.%s.%s' % (alert[1], base_name)
                                    alert_paused = self.redis_conn_decoded.get(cache_key)
                                except Exception as e:
                                    logger.error('error :: alert_paused check failed: %s' % str(e))
                                    alert_paused = False
                                if alert_paused:
                                    logger.info('alert_paused for %s %s until %s' % (
                                        alert[1], base_name, str(alert_paused)))

                                # trigger_alert(alert, metric, second_order_resolution_seconds, context)
                                try:

                                    # @added 20210409 - Feature #3642: Anomaly type classification
                                    #                   Feature #3970: custom_algorithm - adtk_level_shift
                                    # Always determine triggered_algorithms and
                                    # calculate anomalyScore
                                    try:
                                        triggered_algorithms = metric[4]
                                        algorithms_run = metric[5]
                                    except:
                                        triggered_algorithms = []
                                        algorithms_run = []
                                    try:
                                        if triggered_algorithms and algorithms_run:
                                            anomalyScore = len(triggered_algorithms) / len(algorithms_run)
                                        else:
                                            anomalyScore = 1.0
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: failed to determine anomalyScore for %s' % base_name)
                                        anomalyScore = 1.0

                                    if alert[1] != 'smtp':

                                        new_alert = None

                                        # @added 20201007 - Feature #3772: Add the anomaly_id to the http_alerter json
                                        if 'http_alerter' in alert[1]:
                                            anomaly_id = None
                                            anomaly_id_redis_key = 'panorama.anomaly_id.%s.%s' % (
                                                str(int(metric[2])), base_name)
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

                                            # @added 20201130 - Feature #3772: Add the anomaly_id to the http_alerter json
                                            # Determine the triggered_algorithms
                                            # and algorithms_run
                                            try:
                                                triggered_algorithms = metric[4]
                                                algorithms_run = metric[5]
                                            except:
                                                triggered_algorithms = []
                                                algorithms_run = []

                                            # @added 20201111 - Feature #3772: Add the anomaly_id to the http_alerter json
                                            # Add the real anomalyScore
                                            try:
                                                if triggered_algorithms and algorithms_run:
                                                    anomalyScore = len(triggered_algorithms) / len(algorithms_run)
                                                else:
                                                    anomalyScore = 1.0
                                            except:
                                                logger.error(traceback.format_exc())
                                                logger.error('error :: failed to determine anomalyScore for %s' % base_name)
                                                anomalyScore = 1.0
                                            new_alert.append(['anomalyScore', anomalyScore])

                                        # @added 20200929 - Task #3748: POC SNAB
                                        #                   Branch #3068: SNAB
                                        # Determine if this is a snab_check_metric so
                                        # that info can be passed to the alerter
                                        if SNAB_ENABLED and snab_check_metric:
                                            if alert[1] == 'slack':
                                                anomaly_id = None
                                                snab_id = None
                                                snab_details = None
                                                anomaly_id_redis_key = 'panorama.anomaly_id.%s.%s' % (
                                                    str(int(metric[2])), base_name)
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
                                                if anomaly_id:
                                                    # snab_id_redis_key = 'snab.id.%s.%s.%s.%s' % (algorithm_group, str(metric_timestamp), base_name, panorama_added_at)
                                                    snab_id_redis_key = 'snab.id.%s.%s.%s.%s' % (algorithm_group, str(int(metric[2])), base_name, panorama_added_at)
                                                    try_get_snab_id_redis_key_count = 0
                                                    while try_get_snab_id_redis_key_count < 20:
                                                        try_get_snab_id_redis_key_count += 1
                                                        try:
                                                            snab_id = int(self.redis_conn_decoded.get(snab_id_redis_key))
                                                            break
                                                        except:
                                                            sleep(1)
                                                    if not snab_id:
                                                        logger.error('error :: failed to determine snab_id from Redis key - %s' % snab_id_redis_key)
                                                snab_details = ['snab_details', snab_id, anomaly_id, anomalyScore]
                                                # Do not modify the alert list object, create a new one
                                                new_alert = list(alert)
                                                new_alert.append(snab_details)

                                        # @modified 20201008 - Feature #3772: Add the anomaly_id to the http_alerter json
                                        #                      Feature #3734: waterfall alerts
                                        #                      Branch #3068: SNAB
                                        # Use the appropriate alert context
                                        if new_alert:
                                            # @added 20210801 - Feature #4214: alert.paused
                                            if not alert_paused:
                                                logger.info('trigger_alert :: alert: %s, metric: %s, second_order_resolution_seconds: %s, context: %s' % (
                                                    str(new_alert), str(metric),
                                                    str(second_order_resolution_seconds),
                                                    str(alert_context)))
                                                # @modified 20210304 - Feature #3642: Anomaly type classification
                                                #                      Feature #3970: custom_algorithm - adtk_level_shift
                                                # Added triggered_algorithms
                                                trigger_alert(new_alert, metric, second_order_resolution_seconds, alert_context, triggered_algorithms)
                                        else:
                                            # @added 20210801 - Feature #4214: alert.paused
                                            if not alert_paused:
                                                logger.info('trigger_alert :: alert: %s, metric: %s, second_order_resolution_seconds: %s, context: %s' % (
                                                    str(alert), str(metric),
                                                    str(second_order_resolution_seconds),
                                                    str(alert_context)))
                                                # @modified 20210304 - Feature #3642: Anomaly type classification
                                                #                      Feature #3970: custom_algorithm - adtk_level_shift
                                                # Added triggered_algorithms
                                                trigger_alert(alert, metric, second_order_resolution_seconds, alert_context, triggered_algorithms)
                                    else:
                                        # @added 20210801 - Feature #4214: alert.paused
                                        if not alert_paused:
                                            logger.info('smtp_trigger_alert :: alert: %s, metric: %s, second_order_resolution_seconds: %s, context: %s' % (
                                                str(alert), str(metric),
                                                str(second_order_resolution_seconds),
                                                str(alert_context)))
                                            # @modified 20210304 - Feature #3642: Anomaly type classification
                                            #                      Feature #3970: custom_algorithm - adtk_level_shift
                                            # Added triggered_algorithms
                                            smtp_trigger_alert(alert, metric, second_order_resolution_seconds, alert_context, triggered_algorithms)
                                    if not alert_paused:
                                        logger.info('sent %s alert: For %s' % (alert[1], metric[1]))
                                except Exception as e:
                                    logger.error('error :: could not send %s alert for %s: %s' % (alert[1], metric[1], e))

                                # @added 20200913 - Branch #3068: SNAB
                                #                   Task #3744: POC matrixprofile
                                #                   Info #1792: Shapelet extraction
                                if SNAB_ENABLED:
                                    timeseries_dir = base_name.replace('.', '/')
                                    training_dir = '%s/%s/%s' % (
                                        settings.IONOSPHERE_DATA_FOLDER, str(int(metric[2])),
                                        str(timeseries_dir))
                                    anomaly_data = '%s/%s.json' % (training_dir, base_name)
                                    check_full_duration = (int(alert[3]) * 60 * 60)

                                    # TODO:
                                    # Due to how the matrixprofile identifies
                                    # discords, if a metric triggers as
                                    # anomalous with 3sigma it must be checked
                                    # for at x window periods thereafter as
                                    # matrixprofile may only identify a discord
                                    # later when the time series changes again.

                                    for app in SNAB_CHECKS:
                                        if app == skyline_app and check_full_duration:
                                            for snab_context in SNAB_CHECKS[app]:
                                                if snab_context == 'testing':
                                                    for algorithm in SNAB_CHECKS[app][snab_context]:
                                                        try:
                                                            alert_slack_channel = SNAB_CHECKS[app][snab_context][algorithm]['alert_slack_channel']
                                                        except:
                                                            alert_slack_channel = None
                                                        try:
                                                            algorithm_source = SNAB_CHECKS[app][snab_context][algorithm]['algorithm_source']
                                                        except:
                                                            logger.error(traceback.format_exc())
                                                            logger.error('error :: failed to verify algorithm_source for %s for the %s context' % (
                                                                algorithm, snab_context))
                                                            break
                                                        if not os.path.isfile(algorithm_source):
                                                            logger.error('error :: algorithm_source file %s does not exist for %s for the %s context' % (
                                                                algorithm_source, algorithm, snab_context))
                                                            break
                                                        try:
                                                            algorithm_parameters = SNAB_CHECKS[app][snab_context][algorithm]['algorithm_parameters']
                                                        except:
                                                            algorithm_parameters = {}
                                                        try:
                                                            max_execution_time = SNAB_CHECKS[app][snab_context][algorithm]['max_execution_time']
                                                        except:
                                                            max_execution_time = 1.0
                                                        try:
                                                            algo_debug_logging = SNAB_CHECKS[app][snab_context][algorithm]['debug_logging']
                                                        except:
                                                            algo_debug_logging = False
                                                        try:
                                                            for namespace in SNAB_CHECKS[app][snab_context][algorithm]['namespaces']:
                                                                if namespace in base_name:
                                                                    snab_check_details = {
                                                                        'metric': base_name,
                                                                        'timestamp': int(metric[2]),
                                                                        'original_anomaly_timestamp': int(metric[2]),
                                                                        'value': metric[0],
                                                                        'full_duration': check_full_duration,
                                                                        'anomaly_data': anomaly_data,
                                                                        'source': 'mirage',
                                                                        'added_at': added_to_snab_at,
                                                                        'original_added_at': added_to_snab_at,
                                                                        'context': snab_context,
                                                                        'algorithm': algorithm,
                                                                        'algorithm_source': algorithm_source,
                                                                        'algorithm_parameters': algorithm_parameters,
                                                                        'max_execution_time': max_execution_time,
                                                                        'debug_logging': algo_debug_logging,
                                                                        'alert_slack_channel': alert_slack_channel,
                                                                        'processed': None,
                                                                        'analysed': None,
                                                                        'anomalous': None,
                                                                        'anomalyScore': None,
                                                                        'snab_only': False,
                                                                    }
                                                                    add_snab_check = True
                                                                    if base_name in snab_checks_sent:
                                                                        add_snab_check = False
                                                                        break
                                                                    if add_snab_check:
                                                                        self.redis_conn.sadd('snab.work', str(snab_check_details))
                                                                        logger.info('added snab check for %s with algorithm %s for alerter %s' % (
                                                                            base_name, algorithm, str(alert[1])))
                                                                        snab_checks_sent.append(base_name)
                                                                    break
                                                        except:
                                                            logger.error(traceback.format_exc())
                                                            logger.error('error :: failed to check and add check_details to snab.work Redis set if required')

                                # Remove the metric from the waterfall_alerts Redis set
                                # [metric, timestamp, value, added_to_waterfall_timestamp]
                                # waterfall_data = [metric[1], metric[2], metric[0], added_to_waterfall_timestamp, waterfall_panorama_data]
                                redis_set = 'mirage.waterfall_alerts.sent_to_ionosphere'

                                # @added 20200905 - Feature #3734: waterfall alerts
                                # Remove the metric from the waterfall_alerts Redis set
                                # [metric, timestamp, value, added_to_waterfall_timestamp]
                                # waterfall_data = [metric[1], metric[2], metric[0], added_to_waterfall_timestamp, waterfall_panorama_data]
                                redis_set = 'mirage.waterfall_alerts.sent_to_ionosphere'
                                literal_waterfall_alerts = []
                                try:
                                    literal_waterfall_alerts = list(self.redis_conn_decoded.smembers(redis_set))
                                except:
                                    literal_waterfall_alerts = []
                                waterfall_alerts = []
                                for literal_waterfall_alert in literal_waterfall_alerts:
                                    waterfall_alert = literal_eval(literal_waterfall_alert)
                                    waterfall_alerts.append(waterfall_alert)
                                for waterfall_alert in waterfall_alerts:
                                    if waterfall_alert[0] == metric:
                                        if int(waterfall_alert[1]) == metric_timestamp:
                                            try:
                                                self.redis_conn.srem(redis_set, str(waterfall_alert))
                                                logger.info('removed waterfall alert item from Redis set %s - %s' % (
                                                    redis_set, str(waterfall_alert)))
                                            except:
                                                logger.error(traceback.format_exc())
                                                logger.error('error :: failed to remove waterfall alert item for %s at %s from Redis set %s' % (
                                                    base_name, str(metric_timestamp), redis_set))
# TODO - Add anomaly to Panorama when a waterfall alert triggers
                            else:
                                logger.info('alert Redis key %s exists not alerting for %s' % (str(cache_key), metric[1]))

                        except Exception as e:
                            logger.error('error :: could not query Redis for cache_key - %s' % e)

            # @added 20200916 - Branch #3068: SNAB
            #                   Task #3744: POC matrixprofile
            snab_only_checks = []
            if SNAB_ENABLED:
                try:
                    literal_mirage_snab_only_checks = list(self.redis_conn_decoded.smembers(mirage_snab_only_checks_redis_set))
                    for snab_only_check_string in literal_mirage_snab_only_checks:
                        snab_only_check = literal_eval(snab_only_check_string)
                        snab_only_checks.append(snab_only_check)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to determine list from mirage.not_anomalous_metrics Redis set')
                    snab_only_checks = []
            if not snab_only_checks:
                logger.info('there are no snab_only_checks')

            if snab_only_checks:
                snab_only_checks_sent = []
                for alert in all_alerts:
                    for snab_check in snab_only_checks:
                        try:
                            metric = snab_check['metric']
                            anomaly_data = snab_check['anomaly_data']
                            anomaly_timestamp = snab_check['timestamp']
                            original_anomaly_timestamp = snab_check['original_anomaly_timestamp']
                            value = snab_check['value']
                            original_added_at = snab_check['original_added_at']
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to details from snab_only_checks snab_check dict - %s' % str(snab_check))
                        try:
                            pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, metric, [alert[0]])
                            if LOCAL_DEBUG and pattern_match:
                                logger.debug('debug :: %s matched alert - %s' % (base_name, alert[0]))
                            try:
                                del metric_matched_by
                            except:
                                pass
                        except:
                            pattern_match = False
                        if pattern_match:
                            check_full_duration = (int(alert[3]) * 60 * 60)
                            for app in SNAB_CHECKS:
                                if app == skyline_app and check_full_duration:
                                    for snab_context in SNAB_CHECKS[app]:
                                        if snab_context == 'testing':
                                            for algorithm in SNAB_CHECKS[app][snab_context]:
                                                try:
                                                    alert_slack_channel = SNAB_CHECKS[app][snab_context][algorithm]['alert_slack_channel']
                                                except:
                                                    alert_slack_channel = None
                                                try:
                                                    algorithm_source = SNAB_CHECKS[app][snab_context][algorithm]['algorithm_source']
                                                except:
                                                    logger.error(traceback.format_exc())
                                                    logger.error('error :: failed to verify algorithm_source for %s for the %s context' % (
                                                        algorithm, snab_context))
                                                    break
                                                if not os.path.isfile(algorithm_source):
                                                    logger.error('error :: algorithm_source file %s does not exist for %s for the %s context' % (
                                                        algorithm_source, algorithm, snab_context))
                                                    break
                                                try:
                                                    algorithm_parameters = SNAB_CHECKS[app][snab_context][algorithm]['algorithm_parameters']
                                                except:
                                                    algorithm_parameters = {}
                                                try:
                                                    max_execution_time = SNAB_CHECKS[app][snab_context][algorithm]['max_execution_time']
                                                except:
                                                    max_execution_time = 1.0
                                                try:
                                                    algo_debug_logging = SNAB_CHECKS[app][snab_context][algorithm]['debug_logging']
                                                except:
                                                    algo_debug_logging = False
                                                try:
                                                    for namespace in SNAB_CHECKS[app][snab_context][algorithm]['namespaces']:
                                                        if namespace in metric:
                                                            snab_check_details = {
                                                                'metric': metric,
                                                                'timestamp': int(anomaly_timestamp),
                                                                'original_anomaly_timestamp': int(original_anomaly_timestamp),
                                                                'value': value,
                                                                'full_duration': check_full_duration,
                                                                'anomaly_data': anomaly_data,
                                                                'source': 'mirage',
                                                                'added_at': added_to_snab_at,
                                                                'original_added_at': original_added_at,
                                                                'context': snab_context,
                                                                'algorithm': algorithm,
                                                                'algorithm_source': algorithm_source,
                                                                'algorithm_parameters': algorithm_parameters,
                                                                'max_execution_time': max_execution_time,
                                                                'debug_logging': algo_debug_logging,
                                                                'alert_slack_channel': alert_slack_channel,
                                                                'processed': None,
                                                                'analysed': None,
                                                                'anomalous': None,
                                                                'anomalyScore': None,
                                                                'snab_only': True,
                                                            }
                                                            add_snab_check = True
                                                            if metric in snab_only_checks_sent:
                                                                add_snab_check = False
                                                                break
                                                            if add_snab_check:
                                                                self.redis_conn.sadd('snab.work', str(snab_check_details))
                                                                logger.info('added snab_only check for %s with algorithm %s for alerter %s' % (
                                                                    metric, algorithm, str(alert[1])))
                                                                snab_only_checks_sent.append(metric)
                                                            break
                                                except:
                                                    logger.error(traceback.format_exc())
                                                    logger.error('error :: failed to check and add check_details to snab.work Redis set if required')

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
                    # @modified 20200610 - Feature #3560: External alert config
                    # for negate_alert in settings.ALERTS:
                    for negate_alert in all_alerts:
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
                logger.info('exception stats    :: %s' % str(exceptions))
                logger.info('anomaly breakdown  :: %s' % str(anomaly_breakdown))

            # Log to Graphite
            if process_metric_check_files:
                run_time = time() - run_timestamp
                logger.info('seconds to run    :: %.2f' % run_time)
                graphite_run_time = '%.2f' % run_time
                send_metric_name = skyline_app_graphite_namespace + '.run_time'
                send_graphite_metric(skyline_app, send_metric_name, graphite_run_time)

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
            # @modified 20201107 - Feature #3830: metrics_manager
            # Use metrics_manager data, now managed there
            # mirage_metrics_expiration_times = []
            # try:
            #     mirage_metrics_expiration_times = list(self.redis_conn_decoded.smembers('mirage.metrics_expiration_times'))
            #     if LOCAL_DEBUG:
            #         logger.info('debug :: fetched the mirage.metrics_expiration_times Redis set')
            # except:
            #     logger.info('failed to fetch the mirage.metrics_expiration_times Redis set')
            #     mirage_metrics_expiration_times = []
            # try:
            #     mirage_unique_metrics = list(self.redis_conn_decoded.smembers('mirage.unique_metrics'))
            #     mirage_unique_metrics_count = len(mirage_unique_metrics)
            #     logger.info('mirage.unique_metrics Redis set count - %s' % str(mirage_unique_metrics_count))
            #     if LOCAL_DEBUG:
            #         logger.info('debug :: fetched the mirage.unique_metrics Redis set')
            #         logger.info('debug :: %s' % str(mirage_unique_metrics))
            # except:
            #     logger.info('failed to fetch the mirage.unique_metrics Redis set')
            #     mirage_unique_metrics == []
            # for metric in mirage_unique_metrics:
            #     if metric.startswith(settings.FULL_NAMESPACE):
            #         base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
            #     else:
            #         base_name = metric
            #     mirage_alert_expiration_data = [base_name, int(alert[2])]
            #     if str(mirage_alert_expiration_data) not in mirage_metrics_expiration_times:
            #         try:
            #             self.redis_conn.sadd('mirage.metrics_expiration_times', str(mirage_alert_expiration_data))
            #             if LOCAL_DEBUG:
            #                 logger.info('debug :: added %s to mirage.metrics_expiration_times' % str(mirage_alert_expiration_data))
            #         except:
            #             if LOCAL_DEBUG:
            #                 logger.error('error :: failed to add %s to mirage.metrics_expiration_times set' % str(mirage_alert_expiration_data))

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
                # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                # Handle once per minute
                # 'mirage.sent_to_crucible',
                # 'mirage.sent_to_panorama',
                # 'mirage.sent_to_ionosphere',
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
                    logger.error(traceback.format_exc())
                    all_the_garbage = 'gc errored'

                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info(all_the_garbage)

                logger.info('garbage collected')

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage end of run: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # @added 20200903 - Task #3730: Validate Mirage running multiple processes
            # Send checks.stale_discarded and checks.pending metrics
            if int(time()) >= (last_sent_to_graphite + 60):
                stale_check_discarded = []
                try:
                    stale_check_discarded = list(self.redis_conn_decoded.smembers('mirage.stale_check_discarded'))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to get mirage.stale_check_discarded set from Redis')
                    stale_check_discarded = []
                stale_check_discarded_count = len(stale_check_discarded)
                logger.info('checks.stale_discarded   :: %s' % str(stale_check_discarded_count))
                send_metric_name = '%s.checks.stale_discarded' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, str(stale_check_discarded_count))
                checks_pending = [f_pending for f_pending in listdir(settings.MIRAGE_CHECK_PATH) if isfile(join(settings.MIRAGE_CHECK_PATH, f_pending))]
                checks_pending_count = len(checks_pending)
                logger.info('checks.pending   :: %s' % str(checks_pending_count))
                send_metric_name = '%s.checks.pending' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, str(checks_pending_count))
                checks_done = []
                try:
                    checks_done = list(self.redis_conn_decoded.smembers('mirage.checks.done'))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to get mirage.checks.done set from Redis')
                    checks_done = []
                checks_done_count = len(checks_done)
                logger.info('checks.done   :: %s' % str(checks_done_count))
                send_metric_name = '%s.checks.done' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, str(checks_done_count))
                # @modified 20200903 - Task #3730: Validate Mirage running multiple processes
                # Only send panorama, ionosphere and crucible metrics once a minute
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
                last_sent_to_graphite = int(time())
                delete_redis_sets = [
                    'mirage.sent_to_crucible',
                    'mirage.sent_to_panorama',
                    'mirage.sent_to_ionosphere',
                    'mirage.stale_check_discarded',
                    'mirage.checks.done',
                    # @added 20200916 - Branch #3068: SNAB
                    #                   Task #3744: POC matrixprofile
                    mirage_snab_only_checks_redis_set,
                ]
                for i_redis_set in delete_redis_sets:
                    redis_set_to_delete = i_redis_set
                    try:
                        self.redis_conn.delete(redis_set_to_delete)
                        logger.info('deleted Redis set - %s' % redis_set_to_delete)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to delete Redis set - %s' % redis_set_to_delete)

            # Sleep if it went too fast
            if time() - now < 1:
                logger.info('sleeping due to low run time...')
#                sleep(10)
                sleep(1)
