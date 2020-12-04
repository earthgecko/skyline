import logging
try:
    from Queue import Empty
except:
    from queue import Empty
from redis import StrictRedis
# import time
from time import time, sleep
from threading import Thread
# @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
# Use Redis sets in place of Manager().list to reduce memory and number of
# processes
# from multiprocessing import Process, Manager
from multiprocessing import Process
from msgpack import packb
import os
from os.path import join, isfile
from os import kill, getpid, listdir
from sys import exit, version_info
import traceback
import re
import json
import gzip
import requests

# @modified 20200328 - Task #3290: Handle urllib2 in py3
#                      Branch #3262: py3
# Use urlretrieve
# try:
#     import urlparse
# except ImportError:
#     import urllib.parse
# try:
#     import urllib2
# except ImportError:
#     import urllib.request
#     import urllib.error
try:
    import urllib
except:
    # For backwards compatibility with py2 load urlib.request as urllib so
    # that urllib.urlretrieve is available to both as the same module.
    # from urllib import request as urllib
    import urllib.request
    import urllib.error

import errno
import datetime
import shutil

import os.path
# sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
# sys.path.insert(0, os.path.dirname(__file__))

from ast import literal_eval

import settings

# @modified 20200327 - Branch #3262: py3
# from skyline_functions import load_metric_vars, fail_check, mkdir_p
# @modified 20200428 - Feature #3500: webapp - crucible_process_metrics
#                      Feature #1448: Crucible web UI
# Added write_data_to_file and filesafe_metricname to send to Panorama
from skyline_functions import (
    fail_check, mkdir_p, write_data_to_file, filesafe_metricname,
    # @added 20200506 - Feature #3532: Sort all time series
    sort_timeseries,
    # @added 20201009 - Feature #3780: skyline_functions - sanitise_graphite_url
    #                   Bug #3778: Handle single encoded forward slash requests to Graphite
    sanitise_graphite_url)

from crucible_algorithms import run_algorithms

skyline_app = 'crucible'
skyline_app_logger = skyline_app + 'Log'
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = settings.LOG_PATH + '/' + skyline_app + '.log'
skyline_app_loglock = skyline_app_logfile + '.lock'
skyline_app_logwait = skyline_app_logfile + '.wait'

python_version = int(version_info[0])

this_host = str(os.uname()[1])

try:
    SERVER_METRIC_PATH = '.' + settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

skyline_app_graphite_namespace = 'skyline.' + skyline_app + SERVER_METRIC_PATH

FULL_NAMESPACE = settings.FULL_NAMESPACE
ENABLE_CRUCIBLE_DEBUG = settings.ENABLE_CRUCIBLE_DEBUG
crucible_data_folder = str(settings.CRUCIBLE_DATA_FOLDER)
failed_checks_dir = crucible_data_folder + '/failed_checks'


class Crucible(Thread):
    def __init__(self, parent_pid):
        """
        Initialize Crucible
        """
        super(Crucible, self).__init__()
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
        # self.process_list = Manager().list()
        # self.metric_variables = Manager().list()
        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        if settings.REDIS_PASSWORD:
            self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        else:
            self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)

    def check_if_parent_is_alive(self):
        """
        Check if the parent process is alive
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            # @added 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
            # Log warning
            logger.warn('warning :: parent or current process dead')
            exit(0)

    # @added 20200327 - Branch #3262: py3
    # Get rid of the skyline_functions imp as imp is deprecated in py3 anyway
    def new_load_metric_vars(self, metric_vars_file):
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

        # @added 20200607 - Feature #3630: webapp - crucible_process_training_data
        # Added training_data_json
        string_keys = [
            'metric', 'anomaly_dir', 'added_by', 'app', 'run_script',
            'graphite_override_uri_parameters', 'training_data_json']

        float_keys = ['value']
        # @modified 20170127 - Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
        # Added ionosphere_parent_id, always zero from Analyzer and Mirage
        # @modified 20200420 - Feature #3500: webapp - crucible_process_metrics
        #                      Feature #1448: Crucible web UI
        # Added alert_interval to int_keys and add_to_panorama to the boolean_keys
        int_keys = [
            'from_timestamp', 'metric_timestamp', 'added_at', 'full_duration',
            'ionosphere_parent_id', 'alert_interval']
        # @added 20201001 - Task #3748: POC SNAB
        # Added algorithms_run required to determine the anomalyScore
        # so this needs to be sent to Ionosphere so Ionosphere
        # can send it back on an alert, but the same file gets sent to Crucible so...
        array_keys = ['triggered_algorithms', 'algorithms', 'algorithms_run']
        boolean_keys = ['graphite_metric', 'run_crucible_tests', 'add_to_panorama']

        metric_vars_array = []
        for var_array in metric_vars:
            key = None
            value = None
            if var_array[0] in string_keys:
                key = var_array[0]
                value_str = str(var_array[1]).replace("'", '')
                value = str(value_str)
                if var_array[0] == 'metric':
                    metric = value
            if var_array[0] in float_keys:
                key = var_array[0]
                value_str = str(var_array[1]).replace("'", '')
                value = float(value_str)
            if var_array[0] in int_keys:
                key = var_array[0]
                value_str = str(var_array[1]).replace("'", '')
                value = int(value_str)
            if var_array[0] in array_keys:
                key = var_array[0]
                value = literal_eval(str(var_array[1]))
            if var_array[0] in boolean_keys:
                key = var_array[0]
                if str(var_array[1]) == 'True':
                    value = True
                else:
                    value = False
            if key:
                metric_vars_array.append([key, value])

            if len(metric_vars_array) == 0:
                logger.error(
                    'error :: loading metric variables - none found' % (
                        str(metric_vars_file)))
                return False

        logger.info('debug :: metric_vars for %s' % str(metric))
        logger.info('debug :: %s' % str(metric_vars_array))

        return metric_vars_array

    def spin_process(self, i, run_timestamp, metric_check_file):
        """
        Assign a metric for a process to analyze.

        :param i: python process id
        :param run_timestamp: the epoch timestamp at which this process was called
        :param metric_check_file: full path to the metric check file

        :return: returns True

        """

        child_process_pid = os.getpid()
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('child_process_pid - %s' % str(child_process_pid))

        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
        # self.process_list.append(child_process_pid)

        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('processing metric check - %s' % metric_check_file)

        if not os.path.isfile(str(metric_check_file)):
            logger.error('error :: file not found - metric_check_file - %s' % (str(metric_check_file)))
            return

        check_file_name = os.path.basename(str(metric_check_file))
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('check_file_name - %s' % check_file_name)
        check_file_timestamp = check_file_name.split('.', 1)[0]
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('check_file_timestamp - %s' % str(check_file_timestamp))
        check_file_metricname_txt = check_file_name.split('.', 1)[1]
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('check_file_metricname_txt - %s' % check_file_metricname_txt)
        check_file_metricname = check_file_metricname_txt.replace('.txt', '')
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('check_file_metricname - %s' % check_file_metricname)
        check_file_metricname_dir = check_file_metricname.replace('.', '/')
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('check_file_metricname_dir - %s' % check_file_metricname_dir)

        metric_failed_check_dir = failed_checks_dir + '/' + check_file_metricname_dir + '/' + check_file_timestamp
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('metric_failed_check_dir - %s' % metric_failed_check_dir)

        # failed_check_file = failed_checks_dir + '/' + check_file_name
        failed_check_file = metric_failed_check_dir + '/' + check_file_name

        # Load and validate metric variables
        try:
            # @modified 20200327 - Branch #3262: py3
            # metric_vars = load_metric_vars(skyline_app, str(metric_check_file))
            metric_vars_array = self.new_load_metric_vars(str(metric_check_file))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to import metric variables from check file -  %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            # TBD - a failed check Panorama update will go here, perhaps txt
            #       files are not the only "queue" that will be used, both, but
            #       Panorama, may be just a part of Skyline Flux, the flux DB
            #       would allow for a very nice, distributed "queue" and a
            #       distributed Skyline workforce...
            #       Any Skyline node could just have one role, e.g. lots of
            #       Skyline nodes running crucible only and instead of reading
            #       the local filesystem for input, they could read the Flux DB
            #       queue or both...
            return

        # Test metric variables
        # We use a pythonic methodology to test if the variables are defined,
        # this ensures that if any of the variables are not set for some reason
        # we can handle unexpected data or situations gracefully and try and
        # ensure that the process does not hang.

        # if len(str(metric_vars.metric)) == 0:
        # if not metric_vars.metric:
#        try:
#            metric_vars.metric
#        except:
#            logger.error('error :: failed to read metric variable from check file -  %s' % (metric_check_file))
#            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
#            return
#        else:
#            metric = str(metric_vars.metric)
#            if settings.ENABLE_CRUCIBLE_DEBUG:
#                logger.info('metric variable - metric - %s' % metric)
        metric = None
        try:
            # metric_vars.metric
            key = 'metric'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            metric = str(value_list[0])
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('debug :: metric variable - metric - %s' % metric)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to read metric variable from check file - %s' % (metric_check_file))
            metric = None
        if not metric:
            logger.error('error :: failed to load metric variable from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        # if len(metric_vars.value) == 0:
        # if not metric_vars.value:
#        try:
#            metric_vars.value
#        except:
#            logger.error('error :: failed to read value variable from check file -  %s' % (metric_check_file))
#            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
#            return
#        else:
#            value = str(metric_vars.value)
#            if settings.ENABLE_CRUCIBLE_DEBUG:
#                logger.info('metric variable - value - %s' % (value))
        value = None
        try:
            key = 'value'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            value = float(value_list[0])
        except:
            logger.error('error :: failed to read value variable from check file - %s' % (metric_check_file))
            value = None
        if not value:
            # @modified 20181119 - Bug #2708: Failing to load metric vars
            if value == 0.0:
                pass
            else:
                logger.error('error :: failed to load value variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                return

        # if len(metric_vars.from_timestamp) == 0:
        # if not metric_vars.from_timestamp:
#        try:
#            metric_vars.from_timestamp
#        except:
#            logger.error('error :: failed to read from_timestamp variable from check file -  %s' % (metric_check_file))
#            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
#            return
#        else:
#            from_timestamp = str(metric_vars.from_timestamp)
#            if settings.ENABLE_CRUCIBLE_DEBUG:
#                logger.info('metric variable - from_timestamp - %s' % from_timestamp)
        from_timestamp = None
        try:
            key = 'from_timestamp'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            from_timestamp = int(value_list[0])
        except:
            logger.error('error :: failed to read from_timestamp variable from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        # if len(metric_vars.metric_timestamp) == 0:
        # if not metric_vars.metric_timestamp:
#        try:
#            metric_vars.metric_timestamp
#        except:
#            logger.error('error :: failed to read metric_timestamp variable from check file -  %s' % (metric_check_file))
#            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
#            return
#        else:
#            metric_timestamp = str(metric_vars.metric_timestamp)
#            if settings.ENABLE_CRUCIBLE_DEBUG:
#                logger.info('metric variable - metric_timestamp - %s' % metric_timestamp)
        metric_timestamp = None
        try:
            key = 'metric_timestamp'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            metric_timestamp = str(value_list[0])
        except:
            logger.error('error :: failed to read metric_timestamp variable from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        # if len(metric_vars.algorithms) == 0:
        # if not metric_vars.algorithms:
#        algorithms = []
#        try:
#            metric_vars.algorithms
#        except:
#            logger.error('error :: failed to read algorithms variable from check file setting to all')
#            algorithms = ['all']
# #        if not algorithms:
# #            algorithms = []
# #            for i_algorithm in metric_vars.algorithms:
# #                algorithms.append(i_algorithm)
#        if settings.ENABLE_CRUCIBLE_DEBUG:
#            logger.info('metric variable - algorithms - %s' % str(algorithms))
        algorithms = []
        try:
            key = 'algorithms'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            algorithms = value_list[0]
        except:
            logger.info('failed to read algorithms variable from check file setting to all')
            algorithms = ['all']

        # if len(metric_vars.anomaly_dir) == 0:
        # if not metric_vars.anomaly_dir:
#        try:
#            metric_vars.anomaly_dir
#        except:
#            logger.error('error :: failed to read anomaly_dir variable from check file -  %s' % (metric_check_file))
#            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
#            return
#        else:
#            anomaly_dir = str(metric_vars.anomaly_dir)
#            if settings.ENABLE_CRUCIBLE_DEBUG:
#                logger.info('metric variable - anomaly_dir - %s' % anomaly_dir)
        anomaly_dir = None
        try:
            key = 'anomaly_dir'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            anomaly_dir = str(value_list[0])
        except:
            logger.error('failed to read anomaly_dir variable from check file setting to all')
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('metric variable - anomaly_dir - %s' % anomaly_dir)

        # if len(str(metric_vars.graphite_metric)) == 0:
#        try:
#            metric_vars.graphite_metric
#        except:
#            logger.info('failed to read graphite_metric variable from check file setting to False')
#            # yes this is a string
#            graphite_metric = 'False'
#        else:
#            graphite_metric = str(metric_vars.graphite_metric)
#            if settings.ENABLE_CRUCIBLE_DEBUG:
#                logger.info('metric variable - graphite_metric - %s' % graphite_metric)
        graphite_metric = None
        try:
            key = 'graphite_metric'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            graphite_metric = str(value_list[0])
        except:
            logger.info('failed to read graphite_metric variable from check file setting to False')
            # yes this is a string
            graphite_metric = 'False'
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('metric variable - graphite_metric - %s' % graphite_metric)

        # if len(str(metric_vars.run_crucible_tests)) == 0:
        try:
            # metric_vars.run_crucible_tests
            key = 'run_crucible_tests'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            run_crucible_tests = str(value_list[0])
        except:
            logger.info('failed to read run_crucible_tests variable from check file setting to False')
            # yes this is a string
            run_crucible_tests = 'False'
#        else:
#            run_crucible_tests = str(metric_vars.run_crucible_tests)
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('metric variable - run_crucible_tests - %s' % run_crucible_tests)

        try:
            # metric_vars.added_by
            key = 'added_by'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            added_by = str(value_list[0])
        except:
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('failed to read added_by variable from check file setting to crucible - set to crucible')
            added_by = 'crucible'
#        else:
#            added_by = str(metric_vars.added_by)
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('metric variable - added_by - %s' % added_by)

        try:
            # metric_vars.run_script
            key = 'run_script'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            run_script = str(value_list[0])
        except:
            run_script = False
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('metric variable - run_script - not present set to False')
#        else:
#            run_script = str(metric_vars.run_script)
#            if settings.ENABLE_CRUCIBLE_DEBUG:
#                logger.info('metric variable - run_script - %s' % run_script)

        # @added 20190612 - Feature #3108: crucible - graphite_override_uri_parameters_specific_url
        # This metric variable is used to to declare absolute graphite uri
        # parameters
        try:
            # metric_vars.graphite_override_uri_parameters
            key = 'graphite_override_uri_parameters'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            graphite_override_uri_parameters = str(value_list[0])
        except:
            logger.info('failed to read graphite_override_uri_parameters variable from check file setting to False')
            # yes this is a string
            graphite_override_uri_parameters = False
#        else:
#            graphite_override_uri_parameters = str(metric_vars.graphite_override_uri_parameters)
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('metric variable - graphite_override_uri_parameters - %s' % graphite_override_uri_parameters)

        add_to_panorama = False
        try:
            key = 'add_to_panorama'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            add_to_panorama = str(value_list[0])
            if add_to_panorama == 'True':
                add_to_panorama = True
            else:
                add_to_panorama = False
        except:
            logger.info('failed to read add_to_panorama variable from check file setting to False')
            add_to_panorama = False
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('metric variable - add_to_panorama - %s' % str(add_to_panorama))

        # @modified 20200420 - Feature #3500: webapp - crucible_process_metrics
        #                      Feature #1448: Crucible web UI
        # Added alert_interval
        try:
            key = 'alert_interval'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            alert_interval = int(value_list[0])
        except:
            logger.info('failed to read alert_interval variable from check file setting to 0')
            alert_interval = 0
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('metric variable - alert_interval - %s' % str(alert_interval))

        # @added 20200422 - Feature #3500: webapp - crucible_process_metrics
        #                   Feature #1448: Crucible web UI
        # In order for metrics to be analysed in Crucible like the
        # Analyzer or Mirage analysis, the time series data needs to
        # be padded.  Added pad_timeseries in the webapp.
        padded_timeseries = False

        # @added 20200607 - Feature #3630: webapp - crucible_process_training_data
        # Added training_data_json
        try:
            key = 'training_data_json'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            training_data_json = str(value_list[0])
        except:
            training_data_json = None
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('metric variable - training_data_json - %s' % str(training_data_json))

        # Only check if the metric does not a EXPIRATION_TIME key set, crucible
        # uses the alert EXPIRATION_TIME for the relevant alert setting contexts
        # whether that be analyzer, mirage, boundary, etc and sets its own
        # cache_keys in redis.  This prevents large amounts of data being added
        # in terms of tieseries json and image files, crucible samples at the
        # same EXPIRATION_TIME as alerts.

        source_app = 'crucible'
        expiration_timeout = 1800
        remove_all_anomaly_files = False
        check_expired = False
        check_time = time()

        if added_by == 'analyzer' or added_by == 'mirage':
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('Will check %s ALERTS' % added_by)
            if settings.ENABLE_ALERTS:
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('Checking %s ALERTS' % added_by)
                for alert in settings.ALERTS:
                    ALERT_MATCH_PATTERN = alert[0]
                    METRIC_PATTERN = metric
                    alert_match_pattern = re.compile(ALERT_MATCH_PATTERN)
                    pattern_match = alert_match_pattern.match(METRIC_PATTERN)
                    if pattern_match:
                        source_app = added_by
                        expiration_timeout = alert[2]
                        if settings.ENABLE_CRUCIBLE_DEBUG:
                            logger.info('matched - %s - %s - EXPIRATION_TIME is %s' % (source_app, metric, str(expiration_timeout)))
                        check_age = int(check_time) - int(metric_timestamp)
                        if int(check_age) > int(expiration_timeout):
                            check_expired = True
                            if settings.ENABLE_CRUCIBLE_DEBUG:
                                logger.info('the check is older than EXPIRATION_TIME for the metric - not checking - check_expired')

        if added_by == 'boundary':
            if settings.BOUNDARY_ENABLE_ALERTS:
                for alert in settings.BOUNDARY_METRICS:
                    ALERT_MATCH_PATTERN = alert[0]
                    METRIC_PATTERN = metric
                    alert_match_pattern = re.compile(ALERT_MATCH_PATTERN)
                    pattern_match = alert_match_pattern.match(METRIC_PATTERN)
                    if pattern_match:
                        source_app = 'boundary'
                        expiration_timeout = alert[2]
                        if settings.ENABLE_CRUCIBLE_DEBUG:
                            logger.info('matched - %s - %s - EXPIRATION_TIME is %s' % (source_app, metric, str(expiration_timeout)))
                        check_age = int(check_time) - int(metric_timestamp)
                        if int(check_age) > int(expiration_timeout):
                            check_expired = True
                            if settings.ENABLE_CRUCIBLE_DEBUG:
                                logger.info('the check is older than EXPIRATION_TIME for the metric - not checking - check_expired')

        # @added 20200421 - Feature #3500: webapp - crucible_process_metrics
        #                   Feature #1448: Crucible web UI
        # If the check is added by the webapp explicitly set the check_expired
        # to True so that analysis will run
        if added_by == 'webapp':
            check_expired = True

        cache_key = 'crucible.last_check.%s.%s' % (source_app, metric)
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('cache_key - crucible.last_check.%s.%s' % (source_app, metric))

        # Only use the cache_key EXPIRATION_TIME if this is not a request to
        # run_crucible_tests on a timeseries
        if run_crucible_tests == 'False':
            if check_expired:
                logger.info('check_expired - not checking Redis key')
                last_check = True
            else:
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('Checking if cache_key exists')
                try:
                    last_check = self.redis_conn.get(cache_key)
                except Exception as e:
                    logger.error('error :: could not query cache_key for %s - %s - %s' % (source_app, metric, e))
                    logger.info('all anomaly files will be removed')
                    remove_all_anomaly_files = True

            if not last_check:
                try:
                    self.redis_conn.setex(cache_key, expiration_timeout, packb(value))
                    logger.info('set cache_key for %s - %s with timeout of %s' % (source_app, metric, str(expiration_timeout)))
                except Exception as e:
                    logger.error('error :: could not query cache_key for %s - %s - %s' % (source_app, metric, e))
                    logger.info('all anomaly files will be removed')
                    remove_all_anomaly_files = True
            else:
                if check_expired:
                    logger.info('check_expired - all anomaly files will be removed')
                    remove_all_anomaly_files = True
                else:
                    logger.info('cache_key is set and not expired for %s - %s - all anomaly files will be removed' % (source_app, metric))
                    remove_all_anomaly_files = True

        # anomaly dir
        if not os.path.exists(str(anomaly_dir)):
            try:
                # mkdir_p(skyline_app, str(anomaly_dir))
                mkdir_p(anomaly_dir)
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('created anomaly dir - %s' % str(anomaly_dir))
            except:
                logger.error('error :: failed to create anomaly_dir - %s' % str(anomaly_dir))

        if not os.path.exists(str(anomaly_dir)):
            logger.error('error :: anomaly_dir does not exist')
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return
        else:
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('anomaly dir exists - %s' % str(anomaly_dir))

        failed_check_file = anomaly_dir + '/' + metric_timestamp + '.failed.check.' + metric + '.txt'
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('failed_check_file - %s' % str(failed_check_file))

        # Retrieve data from graphite is necessary
        anomaly_graph = anomaly_dir + '/' + metric + '.png'
        anomaly_json = anomaly_dir + '/' + metric + '.json'
        anomaly_json_gz = anomaly_json + '.gz'
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('anomaly_graph - %s' % str(anomaly_graph))
            logger.info('anomaly_json - %s' % str(anomaly_json))
            logger.info('anomaly_json_gz - %s' % str(anomaly_json_gz))

        # Some things added to crucible may not be added by a skyline app per se
        # and if run_crucible_tests is string True then no anomaly files should
        # be removed.
        if run_crucible_tests == 'True':
            remove_all_anomaly_files = False

        # Remove check and anomaly files if the metric has a EXPIRATION_TIME
        # cache_key set
        if remove_all_anomaly_files:
            if os.path.isfile(anomaly_graph):
                try:
                    os.remove(anomaly_graph)
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('anomaly_graph removed - %s' % str(anomaly_graph))
                except OSError:
                    pass
            if os.path.isfile(anomaly_json):
                try:
                    os.remove(anomaly_json)
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('anomaly_json removed - %s' % str(anomaly_json))
                except OSError:
                    pass
            if os.path.isfile(anomaly_json_gz):
                try:
                    os.remove(anomaly_json_gz)
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('anomaly_json_gz removed - %s' % str(anomaly_json_gz))
                except OSError:
                    pass

            anomaly_txt_file = anomaly_dir + '/' + metric + '.txt'
            if os.path.isfile(anomaly_txt_file):
                try:
                    os.remove(anomaly_txt_file)
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('anomaly_txt_file removed - %s' % str(anomaly_txt_file))
                except OSError:
                    pass

            # TBD - this data would have to be added to the panaorama DB before
            #       it is removed
            if os.path.isfile(str(metric_check_file)):
                try:
                    os.remove(str(metric_check_file))
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('metric_check_file removed - %s' % str(metric_check_file))
                except OSError:
                    pass

            if os.path.exists(str(anomaly_dir)):
                try:
                    os.rmdir(str(anomaly_dir))
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('anomaly_dir removed - %s' % str(anomaly_dir))
                except OSError:
                    pass

            logger.info('check and anomaly files removed')
            return

        # Check if the image exists
        if graphite_metric == 'True':

            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('graphite_metric - %s' % (graphite_metric))

            # Graphite timeouts
            connect_timeout = int(settings.GRAPHITE_CONNECT_TIMEOUT)
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('connect_timeout - %s' % str(connect_timeout))

            read_timeout = int(settings.GRAPHITE_READ_TIMEOUT)
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('read_timeout - %s' % str(read_timeout))

            graphite_until = datetime.datetime.fromtimestamp(int(metric_timestamp)).strftime('%H:%M_%Y%m%d')
            graphite_from = datetime.datetime.fromtimestamp(int(from_timestamp)).strftime('%H:%M_%Y%m%d')

            # graphite URL
            if settings.GRAPHITE_PORT != '':
                url = settings.GRAPHITE_PROTOCOL + '://' + settings.GRAPHITE_HOST + ':' + settings.GRAPHITE_PORT + '/render/?from=' + graphite_from + '&until=' + graphite_until + '&target=' + metric + '&format=json'
            else:
                url = settings.GRAPHITE_PROTOCOL + '://' + settings.GRAPHITE_HOST + '/render/?from=' + graphite_from + '&until=' + graphite_until + '&target=' + metric + '&format=json'

            # @added 20190612 - Feature #3108: crucible - graphite_override_uri_parameters
            # This metric variable is used to to declare absolute graphite uri
            # parameters
            # from=00%3A00_20190527&until=23%3A59_20190612&target=movingMedian(nonNegativeDerivative(stats.zpf-watcher-prod-1-30g-doa2.vda.readTime)%2C24)
            if graphite_override_uri_parameters:
                if settings.GRAPHITE_PORT != '':
                    url = settings.GRAPHITE_PROTOCOL + '://' + settings.GRAPHITE_HOST + ':' + settings.GRAPHITE_PORT + '/render/?' + graphite_override_uri_parameters + '&format=json'
                else:
                    url = settings.GRAPHITE_PROTOCOL + '://' + settings.GRAPHITE_HOST + '/render/?' + graphite_override_uri_parameters + '&format=json'
                logger.info('graphite url set from graphite_override_uri_parameters - %s' % (url))

            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('graphite url - %s' % (url))

            if not os.path.isfile(anomaly_graph):
                image_url = url.replace('&format=json', '')
                graphite_image_file = anomaly_dir + '/' + metric + '.png'
                if 'width' not in image_url:
                    image_url += '&width=586'
                if 'height' not in image_url:
                    image_url += '&height=308'
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('graphite image url - %s' % (image_url))

                # @modified 20200327 - Feature #3108: crucible - graphite_override_uri_parameters
                #                      Branch #3262: py3
                # Added graphite_override_uri_parameters conditional
                if graphite_override_uri_parameters:
                    logger.info('retrieving png - surfacing %s graph from graphite with %s' % (metric, url))
                else:
                    logger.info('retrieving png - surfacing %s graph from graphite from %s to %s' % (metric, graphite_from, graphite_until))

                # @modified 20200328 - Task #3290: Handle urllib2 in py3
                #                      Branch #3262: py3
                # Use urlretrieve - image_url_timeout and image_data not longer
                # required
                # image_url_timeout = int(connect_timeout)
                # image_data = None

                try:
                    # @modified 20170913 - Task #2160: Test skyline with bandit
                    # Added nosec to exclude from bandit tests
                    # @modified 20200328 - Task #3290: Handle urllib2 in py3
                    #                      Branch #3262: py3
                    # Use urlretrieve
                    # image_data = urllib2.urlopen(image_url, timeout=image_url_timeout).read()  # nosec
                    if python_version == 2:
                        # @modified 20200808 - Task #3608: Update Skyline to Python 3.8.3 and deps
                        # Added nosec for bandit [B310:blacklist] Audit url open for
                        # permitted schemes. Allowing use of file:/ or custom schemes is
                        # often unexpected.
                        if image_url.lower().startswith('http'):
                            urllib.urlretrieve(image_url, graphite_image_file)  # nosec
                        else:
                            logger.error(
                                'error :: %s :: image_url does not start with http - %s' % (str(image_url)))
                    if python_version == 3:
                        # @modified 20200808 - Task #3608: Update Skyline to Python 3.8.3 and deps
                        # Added nosec for bandit [B310:blacklist] Audit url open for
                        # permitted schemes. Allowing use of file:/ or custom schemes is
                        # often unexpected.
                        if image_url.lower().startswith('http'):
                            urllib.request.urlretrieve(image_url, graphite_image_file)  # nosec
                        else:
                            logger.error(
                                'error :: %s :: image_url does not start with http - %s' % (str(image_url)))
                    logger.info('url OK - %s' % (image_url))
                # except urllib2.URLError:
                except:
                    logger.error(traceback.print_exc())
                    logger.error('error :: url bad - %s' % (image_url))
                    # image_data = None

                try:
                    if python_version == 2:
                        os.chmod(graphite_image_file, 0o644)
                    if python_version == 3:
                        os.chmod(graphite_image_file, mode=0o644)
                    logger.info('graphite_image_file permissions set OK - %s' % (graphite_image_file))
                except:
                    logger.error(traceback.print_exc())
                    logger.error('error :: graphite_image_file permissions could not be set - %s' % (graphite_image_file))

                # @modified 20200328 - Task #3290: Handle urllib2 in py3
                #                      Branch #3262: py3
                # Use urlretrieve so no need to write data to file
                # if image_data is not None:
                #    with open(graphite_image_file, 'w') as f:
                #        f.write(image_data)
                #    logger.info('retrieved - %s' % (anomaly_graph))
                #    if python_version == 2:
                #        # @modified 20200327 - Branch #3262: py3
                #        # os.chmod(graphite_image_file, 0644)
                #        os.chmod(graphite_image_file, 0o644)
                #    if python_version == 3:
                #        os.chmod(graphite_image_file, mode=0o644)
                # else:
                #    logger.error('error :: failed to retrieved - %s' % (anomaly_graph))
            else:
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('anomaly_graph file exists - %s' % str(anomaly_graph))

            if not os.path.isfile(anomaly_graph):
                logger.error('error :: retrieve failed to surface %s graph from graphite' % (metric))
            else:
                logger.info('graph image exists - %s' % (anomaly_graph))

            # Check if the json exists
            if not os.path.isfile(anomaly_json_gz):
                if not os.path.isfile(anomaly_json):
                    logger.info('surfacing timeseries data for %s from graphite from %s to %s' % (metric, graphite_from, graphite_until))
                    if requests.__version__ >= '2.4.0':
                        use_timeout = (int(connect_timeout), int(read_timeout))
                    else:
                        use_timeout = int(connect_timeout)
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('use_timeout - %s' % (str(use_timeout)))

                    # @added 20201009 - Feature #3780: skyline_functions - sanitise_graphite_url
                    #                   Bug #3778: Handle single encoded forward slash requests to Graphite
                    sanitised = False
                    sanitised, url = sanitise_graphite_url(skyline_app, url)

                    try:
                        r = requests.get(url, timeout=use_timeout)
                        js = r.json()
                        datapoints = js[0]['datapoints']
                        if settings.ENABLE_CRUCIBLE_DEBUG:
                            logger.info('data retrieved OK')
                    except:
                        datapoints = [[None, int(graphite_until)]]
                        logger.error('error :: data retrieval failed')

                    converted = []
                    for datapoint in datapoints:
                        try:
                            new_datapoint = [float(datapoint[1]), float(datapoint[0])]
                            converted.append(new_datapoint)
                        # @modified 20170913 - Task #2160: Test skyline with bandit
                        # Added nosec to exclude from bandit tests
                        except:  # nosec
                            continue

                    try:
                        with open(anomaly_json, 'w') as f:
                            f.write(json.dumps(converted))
                        if python_version == 2:
                            # @modified 20200327 - Branch #3262: py3
                            # os.chmod(anomaly_json, 0644)
                            os.chmod(anomaly_json, 0o644)
                        if python_version == 3:
                            os.chmod(anomaly_json, mode=0o644)
                        if settings.ENABLE_CRUCIBLE_DEBUG:
                            logger.info('json file - %s' % anomaly_json)
                    except:
                        logger.error(traceback.print_exc())
                        logger.error('error :: failed to write or chmod anomaly_json - %s' % (anomaly_json))

                    # @added 20200422 - Feature #3500: webapp - crucible_process_metrics
                    #                   Feature #1448: Crucible web UI
                    # Clean up converted
                    if converted:
                        try:
                            del converted
                            del js
                            del datapoints
                        except:
                            pass

                if not os.path.isfile(anomaly_json):
                    logger.error('error :: failed to surface %s json from graphite' % (metric))
                    # Move metric check file
                    try:
                        shutil.move(metric_check_file, failed_check_file)
                        logger.info('moved check file to - %s' % failed_check_file)
                    except OSError:
                        logger.error('error :: failed to move check file to - %s' % failed_check_file)
                        pass
                    return

        # @added 20200607 - Feature #3630: webapp - crucible_process_training_data
        # Added training_data_json
        if training_data_json:
            if not os.path.isfile(training_data_json):
                logger.error('error :: no training data json data found - %s' % training_data_json)
                # Move metric check file
                try:
                    shutil.move(metric_check_file, failed_check_file)
                    logger.info('moved check file to - %s' % failed_check_file)
                except OSError:
                    logger.error('error :: failed to move check file to - %s' % failed_check_file)
                    pass
                return
            else:
                logger.info('setting anomaly json to training_data_json - %s' % (training_data_json))
                anomaly_json = training_data_json

        # Check timeseries json exists - raw or gz
        if not os.path.isfile(anomaly_json):
            if not os.path.isfile(anomaly_json_gz):
                logger.error('error :: no json data found')
                # Move metric check file
                try:
                    shutil.move(metric_check_file, failed_check_file)
                    logger.info('moved check file to - %s' % failed_check_file)
                except OSError:
                    logger.error('error :: failed to move check file to - %s' % failed_check_file)
                    pass
                return
            else:
                logger.info('timeseries json gzip exists - %s' % (anomaly_json_gz))
        else:
            logger.info('timeseries json exists - %s' % (anomaly_json))

        # If timeseries json and run_crucible_tests is str(False) gzip and
        # return here as there is nothing further to do
        if run_crucible_tests == 'False':
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('run_crucible_tests - %s' % run_crucible_tests)
            # gzip the json timeseries data
            if os.path.isfile(anomaly_json):
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('gzipping - %s' % anomaly_json)
                try:
                    f_in = open(anomaly_json)
                    f_out = gzip.open(anomaly_json_gz, 'wb')
                    f_out.writelines(f_in)
                    f_out.close()
                    f_in.close()
                    os.remove(anomaly_json)
                    if python_version == 2:
                        # @modified 20200327 - Branch #3262: py3
                        # os.chmod(anomaly_json_gz, 0644)
                        os.chmod(anomaly_json_gz, 0o644)
                    if python_version == 3:
                        os.chmod(anomaly_json_gz, mode=0o644)
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('gzipped - %s' % anomaly_json_gz)
                    try:
                        os.remove(metric_check_file)
                        logger.info('removed check file - %s' % metric_check_file)
                    except OSError:
                        pass
                    return
                except:
                    logger.error('error :: Failed to gzip data file - %s' % str(traceback.print_exc()))
                    try:
                        os.remove(metric_check_file)
                        logger.info('removed check file - %s' % metric_check_file)
                    except OSError:
                        pass
                    return

            if os.path.isfile(anomaly_json_gz):
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('gzip exists - %s' % anomaly_json)
                    try:
                        os.remove(metric_check_file)
                        logger.info('removed check file - %s' % metric_check_file)
                    except OSError:
                        pass
                    return
            nothing_to_do = 'true - for debug only'

        # self.check_if_parent_is_alive()
        # Run crucible algorithms
        logger.info('running crucible tests - %s' % (metric))

        if os.path.isfile(anomaly_json_gz):
            if not os.path.isfile(anomaly_json):
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('ungzipping - %s' % anomaly_json_gz)
                try:
                    # with gzip.open(anomaly_json_gz, 'rb') as fr:
                    fr = gzip.open(anomaly_json_gz, 'rb')
                    raw_timeseries = fr.read()
                    fr.close()
                except Exception as e:
                    logger.error(traceback.print_exc())
                    logger.error('error :: could not ungzip %s - %s' % (anomaly_json_gz, e))
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('ungzipped')
                    logger.info('writing to - %s' % anomaly_json)
                with open(anomaly_json, 'w') as fw:
                    fw.write(raw_timeseries)
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('anomaly_json done')
                if python_version == 2:
                    # @modified 20200327 - Branch #3262: py3
                    # os.chmod(anomaly_json, 0644)
                    os.chmod(anomaly_json, 0o644)
                if python_version == 3:
                    os.chmod(anomaly_json, mode=0o644)
        else:
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('No gzip - %s' % anomaly_json_gz)
            nothing_to_do = 'true - for debug only'

        if os.path.isfile(anomaly_json):
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('anomaly_json exists - %s' % anomaly_json)

        if os.path.isfile(anomaly_json):
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('loading timeseries from - %s' % anomaly_json)
            timeseries = None
            try:
                with open(anomaly_json, 'r') as f:
                    timeseries = json.loads(f.read())
                    raw_timeseries = f.read()
                    timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
                    timeseries = literal_eval(timeseries_array_str)
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('loaded time series from - %s' % anomaly_json)
            except:
                # logger.error(traceback.format_exc())
                logger.info('failed to load with JSON, literal_eval will be tried - %s' % anomaly_json)
            # @added 20180715 - Task #2444: Evaluate CAD
            #                   Task #2446: Optimize Ionosphere
            #                   Branch #2270: luminosity
            # If the json.loads fails use literal_eval
            if not timeseries:
                try:
                    with open(anomaly_json, 'r') as f:
                        raw_timeseries = f.read()
                        timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
                        timeseries = literal_eval(timeseries_array_str)
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('loaded time series with literal_eval from - %s' % anomaly_json)
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: failed to load JSON - %s' % anomaly_json)
        else:
            try:
                logger.error('error :: file not found - %s' % anomaly_json)
                shutil.move(metric_check_file, failed_check_file)
                if python_version == 2:
                    # @modified 20200327 - Branch #3262: py3
                    # os.chmod(failed_check_file, 0644)
                    os.chmod(failed_check_file, 0o644)
                if python_version == 3:
                    os.chmod(failed_check_file, mode=0o644)
                logger.info('moved check file to - %s' % failed_check_file)
            except OSError:
                logger.error('error :: failed to move check file to - %s' % failed_check_file)
                pass
            return

        if not timeseries:
            try:
                logger.info('failing check, no time series from - %s' % anomaly_json)
                shutil.move(metric_check_file, failed_check_file)
                if python_version == 2:
                    # @modified 20200327 - Branch #3262: py3
                    # os.chmod(failed_check_file, 0644)
                    os.chmod(failed_check_file, 0o644)
                if python_version == 3:
                    os.chmod(failed_check_file, mode=0o644)
                logger.info('moved check file to - %s' % failed_check_file)
            except OSError:
                logger.error('error :: failed to move check file to - %s' % failed_check_file)
                pass
            return

        # @added 20200507 - Feature #3532: Sort all time series
        # To ensure that there are no unordered timestamps in the time
        # series which are artefacts of the collector or carbon-relay, sort
        # all time series by timestamp before analysis.
        original_timeseries = timeseries
        if original_timeseries:
            timeseries = sort_timeseries(original_timeseries)
            del original_timeseries

        start_timestamp = int(timeseries[0][0])
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('start_timestamp - %s' % str(start_timestamp))
        end_timestamp = int(timeseries[-1][0])
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('end_timestamp - %s' % str(end_timestamp))

        full_duration = end_timestamp - start_timestamp
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('full_duration - %s' % str(full_duration))

        # @added 20200422 - Feature #3500: webapp - crucible_process_metrics
        #                   Feature #1448: Crucible web UI
        # In order for metrics to be analysed in Crucible like the
        # Analyzer or Mirage analysis, the time series data needs to
        # be padded.  Added pad_timeseries in the webapp so check
        # here if the time series is padded
        if graphite_override_uri_parameters and added_by == 'webapp':
            if start_timestamp < from_timestamp:
                padded_timeseries = True
                padded_with = from_timestamp - start_timestamp
                logger.info('padded time series identified, padded with %s seconds' % str(padded_with))

        self.check_if_parent_is_alive()

        run_algorithms_start_timestamp = int(time())
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('run_algorithms_start_timestamp - %s' % str(run_algorithms_start_timestamp))

        # @added 20200427 - Feature #3500: webapp - crucible_process_metrics
        #                   Feature #1448: Crucible web UI
        # Set variables so on fail the process does not hang
        anomalous = None
        ensemble = None
        alert_interval_discarded_anomalies_count = 0

        # For debug only but do not remove as this is an item in the final
        # return
        nothing_to_do = ''

        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('run_algorithms - %s,%s,%s,%s,%s,%s' % (metric, str(end_timestamp), str(full_duration), anomaly_json, skyline_app, str(algorithms)))
        try:
            # @modified 20200421 - Feature #3500: webapp - crucible_process_metrics
            #                      Feature #1448: Crucible web UI
            # Pass alert_interval, add_to_panaroma and alert_interval_discarded_anomalies_count
            # anomalous, ensemble = run_algorithms(timeseries, str(metric), end_timestamp, full_duration, str(anomaly_json), skyline_app, algorithms)
            # @modified 20200422 - Feature #3500: webapp - crucible_process_metrics
            #                      Feature #1448: Crucible web UI
            # Added padded_timeseries and from_timestamp
            anomalous, ensemble, alert_interval_discarded_anomalies_count = run_algorithms(timeseries, str(metric), end_timestamp, full_duration, str(anomaly_json), skyline_app, algorithms, alert_interval, add_to_panorama, padded_timeseries, from_timestamp)
        except:
            logger.error('error :: run_algorithms failed - %s' % str(traceback.print_exc()))
            try:
                shutil.move(metric_check_file, failed_check_file)
                if python_version == 2:
                    os.chmod(failed_check_file, 0o644)
                if python_version == 3:
                    os.chmod(failed_check_file, mode=0o644)
                logger.info('moved check file to - %s' % failed_check_file)
            except OSError:
                logger.error('error :: failed to move check file to - %s' % failed_check_file)
                pass
            return

        run_algorithms_end_timestamp = int(time())
        run_algorithms_seconds = run_algorithms_end_timestamp - run_algorithms_start_timestamp

        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('anomalous, ensemble - %s, %s' % (anomalous, str(ensemble)))

        if anomalous:
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('anomalous - %s' % (anomalous))
            nothing_to_do = 'true - for debug only'

        logger.info('run_algorithms took %s seconds' % str(run_algorithms_seconds))

        # Update anomaly file
        # @modified 20200421 - Feature #3500: webapp - crucible_process_metrics
        #                      Feature #1448: Crucible web UI
        # Added run_algorithms_seconds and alert_interval_discarded_anomalies
        crucible_data = 'crucible_tests_run = \'%s\'\n' \
                        'crucible_triggered_algorithms = %s\n' \
                        'tested_by = \'%s\'\n' \
                        'run_algorithms_seconds = \'%s\'\n' \
                        'alert_interval_discarded_anomalies = \'%s\'\n' \
            % (str(run_timestamp), str(ensemble), str(this_host),
                str(run_algorithms_seconds), str(alert_interval_discarded_anomalies_count))
        crucible_anomaly_file = '%s/%s.txt' % (anomaly_dir, metric)
        with open(crucible_anomaly_file, 'a') as fh:
            fh.write(crucible_data)
        if python_version == 2:
            # @modified 20200327 - Branch #3262: py3
            # os.chmod(crucible_anomaly_file, 0644)
            os.chmod(crucible_anomaly_file, 0o644)
        if python_version == 3:
            os.chmod(crucible_anomaly_file, mode=0o644)
        logger.info('updated crucible anomaly file - %s/%s.txt' % (anomaly_dir, metric))

        # gzip the json timeseries data after analysis
        if os.path.isfile(anomaly_json):
            if not os.path.isfile(anomaly_json_gz):
                remove_json = False
                try:
                    f_in = open(anomaly_json)
                    f_out = gzip.open(anomaly_json_gz, 'wb')
                    f_out.writelines(f_in)
                    f_out.close()
                    f_in.close()
                    remove_json = True
                    logger.info('gzipped - %s' % (anomaly_json_gz))
                except:
                    logger.error('error :: Failed to gzip data file - %s' % str(traceback.print_exc()))
                if remove_json:
                    try:
                        if python_version == 2:
                            # @modified 20200327 - Branch #3262: py3
                            # os.chmod(anomaly_json_gz, 0644)
                            os.chmod(anomaly_json_gz, 0o644)
                        if python_version == 3:
                            os.chmod(anomaly_json_gz, mode=0o644)
                        logger.info('gzipped - %s' % (anomaly_json_gz))
                    except:
                        logger.error('error :: Failed to chmod anomaly_json_gz file - %s' % str(traceback.print_exc()))
                    try:
                        os.remove(anomaly_json)
                    except:
                        logger.error('error :: Failed to remove anomaly_json file - %s' % str(traceback.print_exc()))

            else:
                os.remove(anomaly_json)

        if run_script:
            if os.path.isfile(run_script):
                logger.info('running - %s' % (run_script))
                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                os.system('%s %s' % (str(run_script), str(crucible_anomaly_file)))  # nosec

        # @added 20200428 - Feature #3500: webapp - crucible_process_metrics
        #                   Feature #1448: Crucible web UI
        # Send Skyline consensus anomalie to Panorama
        if add_to_panorama:
            added_to_panorama = False
            user_id = 1
            skyline_anomalies_score_file = anomaly_dir + '/' + 'skyline.anomalies_score.txt'
            if os.path.isfile(skyline_anomalies_score_file):
                try:
                    with open(skyline_anomalies_score_file) as f:
                        output = f.read()
                    skyline_anomalies = literal_eval(output)
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: failed to get Skyline anomalies scores from %s' % skyline_anomalies_score_file)
                    skyline_anomalies = None
                skyline_consensus_anomalies = []
                if skyline_anomalies:
                    # skyline_anomalies format
                    # [timestamp, value, anomaly_score, triggered_algorithms]
                    # [1583234400.0, 44.39999999990687, 2, ['histogram_bins', 'median_absolute_deviation']],
                    # Convert float timestamp from Graphite to int
                    for timestamp, value, anomaly_score, triggered_algorithms in skyline_anomalies:
                        if anomaly_score >= settings.CONSENSUS:
                            skyline_consensus_anomalies.append([int(timestamp), value, anomaly_score, triggered_algorithms])
                    del skyline_anomalies
                added_at = int(time())
                if skyline_consensus_anomalies:
                    sane_metricname = filesafe_metricname(str(metric))
                    for timestamp, datapoint, anomaly_score, triggered_algorithms in skyline_consensus_anomalies:
                        # To allow multiple Panorama anomaly files to added quickly just
                        # increment the added_at by 1 seconds so that all the files have a
                        # unique name
                        added_at += 1
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
                                                'label = \'added by Crucible\'\n' \
                                                'user_id = \'%s\'\n' \
                            % (metric, str(datapoint), from_timestamp,
                               str(timestamp), str(settings.ALGORITHMS),
                               triggered_algorithms, skyline_app, source,
                               this_host, str(added_at), str(user_id))
                        # Create an anomaly file with details about the anomaly
                        panaroma_anomaly_file = '%s/%s.%s.txt' % (
                            settings.PANORAMA_CHECK_PATH, added_at, sane_metricname)
                        try:
                            write_data_to_file(
                                skyline_app, panaroma_anomaly_file, 'w',
                                panaroma_anomaly_data)
                            logger.info('added panorama anomaly file :: %s' % (panaroma_anomaly_file))
                            added_to_panorama = True
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: send_crucible_job_metric_to_panorama - failed to add panorama anomaly file :: %s' % (panaroma_anomaly_file))
            if added_to_panorama:
                crucible_sent_to_panorama_file = '%s/%s.%s.%s.sent_to_panorama.txt' % (
                    anomaly_dir, str(added_at), sane_metricname)
                panorama_done_data = [added_at, int(user_id), skyline_consensus_anomalies]
                try:
                    write_data_to_file(
                        skyline_app, crucible_sent_to_panorama_file, 'w',
                        str(panorama_done_data))
                    logger.info('added panorama crucible job file :: %s' % (crucible_sent_to_panorama_file))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: crucible send to panorama - failed to add panorama file :: %s' % (crucible_sent_to_panorama_file))

        try:
            os.remove(metric_check_file)
            logger.info('complete removed check file - %s %s' % (metric_check_file, nothing_to_do))
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

        logger.info('process intialized')

        while 1:
            now = time()
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('process started - %s' % int(now))

            # Make sure check_dir exists and has not been removed
            try:
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('checking check dir exists - %s' % settings.CRUCIBLE_CHECK_PATH)
                os.path.exists(settings.CRUCIBLE_CHECK_PATH)
            except:
                logger.error('error :: check dir did not exist - %s' % settings.CRUCIBLE_CHECK_PATH)
                mkdir_p(settings.CRUCIBLE_CHECK_PATH)
                logger.info('check dir created - %s' % settings.CRUCIBLE_CHECK_PATH)
                os.path.exists(settings.CRUCIBLE_CHECK_PATH)
                # continue

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
                logger.info('connected to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
            except:
                logger.info('skyline can not connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                sleep(10)
                logger.info('connecting to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                if settings.REDIS_PASSWORD:
                    self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                else:
                    self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                continue

            """
            Determine if any metric has been added to test
            """
            while True:

                # Report app up
                self.redis_conn.setex(skyline_app, 120, now)

                metric_var_files = [f for f in listdir(settings.CRUCIBLE_CHECK_PATH) if isfile(join(settings.CRUCIBLE_CHECK_PATH, f))]

                # @added 20200428 - Feature #3500: webapp - crucible_process_metrics
                #                   Feature #1448: Crucible web UI
                # If crucible is busy, stop processing around midnight to allow
                # for log rotation as log rotation can make the process hang if
                # running
                pause_for_log_rotation = False
                current_now = datetime.datetime.now()
                current_hour = current_now.strftime('%H')
                if current_hour == '23':
                    current_minute = current_now.strftime('%M')
                    before_midnight_minutes = ['55', '56', '57', '58', '59']
                    if current_minute in before_midnight_minutes:
                        logger.info('setting metric_var_files to [] for log rotation')
                        metric_var_files = []
                        pause_for_log_rotation = True
                if current_hour == '00':
                    current_minute = current_now.strftime('%M')
                    after_midnight_minutes = ['00', '01', '02']
                    if current_minute in after_midnight_minutes:
                        logger.info('setting metric_var_files to [] for log rotation')
                        metric_var_files = []
                        pause_for_log_rotation = True

#                if len(metric_var_files) == 0:
                if not metric_var_files:
                    logger.info('sleeping 10 no metric check files')
                    sleep(10)

                # Discover metric to analyze
                metric_var_files = ''
                metric_var_files = [f for f in listdir(settings.CRUCIBLE_CHECK_PATH) if isfile(join(settings.CRUCIBLE_CHECK_PATH, f))]

                if pause_for_log_rotation:
                    logger.info('setting metric_var_files to [] for log rotation')
                    metric_var_files = []

#                if len(metric_var_files) > 0:
                if metric_var_files:
                    break

            metric_var_files_sorted = sorted(metric_var_files)
            metric_check_file = settings.CRUCIBLE_CHECK_PATH + "/" + str(metric_var_files_sorted[0])

            # @added 20200421 - Feature #3516: Handle multiple CRUCIBLE_PROCESSES
            # TODO
            # Handled multiple processes
            # Prioritise current metrics over webapp metrics

            logger.info('assigning check for processing - %s' % str(metric_var_files_sorted[0]))

            # Reset process_list
            # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
            # try:
            #     self.process_list[:] = []
            # except:
            #     logger.error('error :: failed to reset self.process_list')

            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            run_timestamp = int(now)

            # @modified 20200427 - Feature #3516: Handle multiple CRUCIBLE_PROCESSES
            use_range = 1
            if len(metric_var_files_sorted) > 1:
                if settings.CRUCIBLE_PROCESSES > 1:
                    if len(metric_var_files_sorted) < settings.CRUCIBLE_PROCESSES:
                        use_range = settings.CRUCIBLE_PROCESSES - len(metric_var_files_sorted)
                    else:
                        use_range = settings.CRUCIBLE_PROCESSES
                    logger.info('dynamically determined to submit to and use %s processors' % str(use_range))
            logger.info('will use %s processors' % str(use_range))

            # @modified 20200427 - Feature #3516: Handle multiple CRUCIBLE_PROCESSES
            # for i in range(1, settings.CRUCIBLE_PROCESSES + 1):
            for i in range(1, use_range + 1):
                # @added 20200427 - Feature #3516: Handle multiple CRUCIBLE_PROCESSES
                if len(metric_var_files_sorted) > 1:
                    if use_range > 1:
                        if i > 1:
                            list_element = i - 1
                            metric_check_file = settings.CRUCIBLE_CHECK_PATH + "/" + str(metric_var_files_sorted[list_element])
                            logger.info('assigning additional check to processor %s for processing - %s' % (str(i), str(metric_var_files_sorted[list_element])))
                p = Process(target=self.spin_process, args=(i, run_timestamp, str(metric_check_file)))
                pids.append(p)
                pid_count += 1
                # @modified 20200427 - Feature #3516: Handle multiple CRUCIBLE_PROCESSES
                # logger.info('starting %s of %s spin_process/es' % (str(pid_count), str(settings.CRUCIBLE_PROCESSES)))
                logger.info('starting %s of %s spin_process/es' % (str(pid_count), str(use_range)))
                p.start()
                spawned_pids.append(p.pid)

            # Send wait signal to zombie processes
            # for p in pids:
            #     p.join()
            # Self monitor processes and terminate if any spin_process has run
            # for longer than CRUCIBLE_TESTS_TIMEOUT
            p_starts = time()
            sleep_count = 0
            while time() - p_starts <= settings.CRUCIBLE_TESTS_TIMEOUT:
                if any(p.is_alive() for p in pids):
                    # Just to avoid hogging the CPU
                    sleep(.1)
                    # @added 20200421 - Feature #3500: webapp - crucible_process_metrics
                    #                   Feature #1448: Crucible web UI
                    sleep_count += 1
                    if (sleep_count % 100 == 0):
                        logger.info('%s :: spin_process/es still running pid/s - %s' % (skyline_app, str(pids)))
                else:
                    # All the processes are done, break now.
                    time_to_run = time() - p_starts
                    # @modified 20200427 - Feature #3516: Handle multiple CRUCIBLE_PROCESSES
                    # logger.info('%s :: %s spin_process/es completed in %.2f seconds' % (skyline_app, str(settings.CRUCIBLE_PROCESSES), time_to_run))
                    logger.info('%s :: %s spin_process/es completed in %.2f seconds' % (skyline_app, str(use_range), time_to_run))
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

            while os.path.isfile(metric_check_file):
                sleep(1)
