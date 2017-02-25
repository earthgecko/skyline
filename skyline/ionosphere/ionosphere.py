from __future__ import division
import logging
import os
from os import kill, getpid, listdir
from os.path import join, isfile
from sys import version_info
try:
    from Queue import Empty
except:
    from queue import Empty
from time import time, sleep
from threading import Thread
from multiprocessing import Process, Manager
import re
from shutil import rmtree
import csv
from ast import literal_eval

from redis import StrictRedis
# from msgpack import Unpacker, packb
import traceback
import mysql.connector
from mysql.connector import errorcode

from sqlalchemy.sql import select
# import requests
# from requests.auth import HTTPBasicAuth

# @added 20161213 - Branch #1790: test_tsfresh
# To match the new order introduced via the test_tsfresh method
import numpy as np
# import pandas as pd

# @added 20170107 - Feature #1852: Ionosphere - features_profile matched graphite graphs
#                   Feature #1844: ionosphere_matched DB table
from tsfresh import __version__ as tsfresh_version

import settings
from skyline_functions import (
    fail_check, mysql_select, write_data_to_file, send_graphite_metric, mkdir_p)

# @added 20161221 - calculate features for every anomaly, instead of making the
# user do it in the frontend or calling the webapp constantly in a cron like
# manner.  Decouple Ionosphere from the webapp.
from features_profile import calculate_features_profile

# @modified 20170107 - Feature #1844: ionosphere_matched DB table
# Added ionosphere_matched_meta
from database import (
    get_engine, ionosphere_table_meta, metrics_table_meta,
    ionosphere_matched_table_meta)
from tsfresh_feature_names import TSFRESH_FEATURES

# @added 20170114 - Feature #1854: Ionosphere learn
# @modified 20170117 - Feature #1854: Ionosphere learn - generations
# Renamed the function from simple learn to the meme it has become
# from learn import learn
from learn import ionosphere_learn

skyline_app = 'ionosphere'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

python_version = int(version_info[0])

this_host = str(os.uname()[1])

# Converting one settings variable into a local variable, just because it is a
# long string otherwise.
try:
    ENABLE_IONOSPHERE_DEBUG = settings.ENABLE_IONOSPHERE_DEBUG
except:
    logger.error('error :: cannot determine ENABLE_IONOSPHERE_DEBUG from settings' % skyline_app)
    ENABLE_IONOSPHERE_DEBUG = False

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)

max_age_seconds = settings.IONOSPHERE_CHECK_MAX_AGE

# Database configuration
config = {'user': settings.PANORAMA_DBUSER,
          'password': settings.PANORAMA_DBUSERPASS,
          'host': settings.PANORAMA_DBHOST,
          'port': settings.PANORAMA_DBPORT,
          'database': settings.PANORAMA_DATABASE,
          'raise_on_warnings': True}

failed_checks_dir = '%s_failed' % settings.IONOSPHERE_CHECK_PATH
last_purge_key = '%s.last_purge_ts' % skyline_app

LOCAL_DEBUG = False


class Ionosphere(Thread):
    """
    The Ionosphere class which controls the ionosphere thread and spawned
    processes.
    """

    def __init__(self, parent_pid):
        """
        Initialize Ionosphere

        Create the :obj:`self.anomalous_metrics` list

        """
        super(Ionosphere, self).__init__()
        self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        self.mysql_conn = mysql.connector.connect(**config)
        self.anomalous_metrics = Manager().list()
        self.not_anomalous = Manager().list()
        self.features_profiles_checked = Manager().list()
        self.training_metrics = Manager().list()
        self.sent_to_panorama = Manager().list()
        # @added 20170108 - Feature #1830: Ionosphere alerts
        # Added lists of ionosphere_smtp_alerter_metrics and
        # ionosphere_non_smtp_alerter_metrics
        self.ionosphere_smtp_alerter_metrics = Manager().list()
        self.ionosphere_non_smtp_alerter_metrics = Manager().list()

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            exit(0)

    """
    These are the ionosphere mysql functions used to surface and input
    ionosphere data for timeseries.
    """

    def mysql_insert(self, insert):
        """
        Insert data into mysql table

        :param insert: the insert string
        :type insert: str
        :return: int
        :rtype: int or boolean

        - **Example usage**::

            query = 'insert into host (host) VALUES (\'this_host\')'
            result = self.mysql_insert(query)

        .. note::
            - If the MySQL query fails a boolean will be returned not a tuple
                * ``False``
                * ``None``

        """

        try:
            cnx = mysql.connector.connect(**config)
            if ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: connected to mysql')
        except mysql.connector.Error as err:
            logger.error('error :: mysql error - %s' % str(err))
            logger.error('error :: failed to connect to mysql')
            raise

        if cnx:
            try:
                cursor = cnx.cursor()
                cursor.execute(insert)
                inserted_id = cursor.lastrowid
                # Make sure data is committed to the database
                cnx.commit()
                cursor.close()
                cnx.close()
                return inserted_id
            except mysql.connector.Error as err:
                logger.error('error :: mysql error - %s' % str(err))
                logger.error('Failed to insert record')
                cnx.close()
                raise
        else:
            cnx.close()
            return False

        return False

    def purge_old_data_dirs(self, dir_path, older_than):
        time_now = time()
        logger.info(
            'Cleaning old training data from %s older than %s seconds' %
            (dir_path, str(older_than)))
        try:
            for path, folders, files in os.walk(dir_path):
                for folder in folders[:]:
                    folder_path = os.path.join(path, folder)
                    # Only timestamped directories are removed
                    if re.match('\d{10}', folder):
                        if ENABLE_IONOSPHERE_DEBUG:
                            logger.info('debug :: matched - %s' % folder_path)
                        if (time_now - os.path.getmtime(folder_path)) > older_than:
                            try:
                                rmtree(folder_path)
                                logger.info('removed - %s' % folder_path)
                            except:
                                logger.error('error :: failed to rmtree %s' % folder_path)
        except:
            logger.info(traceback.format_exc())
            logger.error('error :: purge_old_data_dirs - os.walk')

        last_purge_ts = int(time())
        try:
            self.redis_conn.setex(last_purge_key, 1800, last_purge_ts)
            logger.info('updated Redis key for %s' % last_purge_key)
        except:
            logger.error('error :: failed to update Redis key for %s' % last_purge_key)

        backup_purge_ts_file = '%s/last_purge_ts.txt' % (settings.IONOSPHERE_DATA_FOLDER)
        try:
            write_data_to_file(skyline_app, backup_purge_ts_file, 'w', last_purge_ts)
            logger.info('updated the backup_purge_ts_file with %s' % str(last_purge_ts))
        except:
            logger.error('error :: failed to update the backup_purge_ts_file - %s' % backup_purge_ts_file)
        return

    def remove_metric_check_file(self, metric_check_file):
        if os.path.isfile(str(metric_check_file)):
            try:
                os.remove(str(metric_check_file))
                logger.info('metric_check_file removed - %s' % str(metric_check_file))
            except OSError:
                pass
        return

# @added 20161228 - Feature #1828: ionosphere - mirage Redis data features
#                   Branch #922: Ionosphere
# Bringing Ionosphere online - do alert on Ionosphere metrics

    def manage_ionosphere_unique_metrics(self):
        """
        Create a Redis set of all Ionosphere enabled metrics.

        :param i: python process id
        :return: returns True

        """

        def get_an_engine():
            try:
                engine, log_msg, trace = get_engine(skyline_app)
                return engine, log_msg, trace
            except:
                trace = traceback.format_exc()
                logger.error('%s' % trace)
                log_msg = 'error :: failed to get MySQL engine for manage_ionosphere_unique_metrics'
                logger.error('%s' % log_msg)
                return None, log_msg, trace

        ionosphere_unique_metrics_count = 0
        redis_ionosphere_unique_metrics = None
        ionosphere_unique_metrics = []
        try:
            # ionosphere_unique_metrics = list(self.redis_conn.smembers('ionosphere.unique_metrics'))
            redis_ionosphere_unique_metrics = self.redis_conn.smembers('ionosphere.unique_metrics')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to get ionosphere.unique_metrics from Redis')
            # ionosphere_unique_metrics = []

        if redis_ionosphere_unique_metrics is not None:
            ionosphere_unique_metrics = list(redis_ionosphere_unique_metrics)
            ionosphere_unique_metrics_count = len(ionosphere_unique_metrics)
            logger.info('Redis ionosphere.unique_metrics set has %s metrics' % (str(ionosphere_unique_metrics_count)))
        else:
            logger.info('Redis ionosphere.unique_metrics unknown setting to []')
            ionosphere_unique_metrics = []

        manage_ionosphere_unique_metrics = True
        manage_ionosphere_unique_metrics_key = []
        try:
            manage_ionosphere_unique_metrics_key = self.redis_conn.get('ionosphere.manage_ionosphere_unique_metrics')
        except Exception as e:
            if LOCAL_DEBUG:
                logger.error('error :: could not query Redis for ionosphere.manage_ionosphere_unique_metrics key: %s' % str(e))

        if manage_ionosphere_unique_metrics_key is not None:
            manage_ionosphere_unique_metrics = False

        logger.info('getting MySQL engine for ionosphere_enabled_metrics')
        try:
            engine, log_msg, trace = get_an_engine()
            logger.info(log_msg)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: could not get a MySQL engine for ionosphere_enabled_metrics')
            return False
        if not engine:
            logger.error('error :: MySQL engine not obtained for ionosphere_enabled_metrics')
            return False

        # Determine the metrics that have ionosphere_enabled
        # @added 20170103 - Task #1658: Patterning Skyline Ionosphere
        # TODO: We need 2 sets not just ionosphere.unique_metrics otherwise
        #       if a metric is switch from Analyzer to Mirage will send all
        #       matched anomalies to Ionosphere even if there is no features
        #       profile at the specified duration.
        # ionosphere.analyzer.unique_metrics (at FULL_DURATION)
        # ionosphere.mirage.unique_metrics (NOT at FULL_DURATION)
        # @modified 20170108 - Feature #1852: Ionosphere - features_profile matched graphite graphs
        # Yes those ^^ are needed, MySQL join?
        ionosphere_enabled_metrics = []
        ionosphere_metrics_count = 0
        query_ok = False
        try:
            stmt = 'select metric from metrics where ionosphere_enabled=1'
            connection = engine.connect()
            for row in engine.execute(stmt):
                metric_basename = row['metric']
                metric_name = '%s%s' % (str(settings.FULL_NAMESPACE), str(metric_basename))
                ionosphere_enabled_metrics.append(metric_name)
            connection.close()
            query_ok = True
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: could not determine ionosphere_enabled metrics from the DB to manage ionosphere.unique_metrics Redis set')

        ionosphere_metrics_count = len(ionosphere_enabled_metrics)
        logger.info('db has %s ionosphere_enabled metrics' % (str(ionosphere_metrics_count)))

        if manage_ionosphere_unique_metrics:
            # Testing the query was fine and Ionosphere metrics can go to 0 if
            # all were disabled
            if query_ok:
                manage_ionosphere_unique_metrics = True
            else:
                manage_ionosphere_unique_metrics = False

        if manage_ionosphere_unique_metrics:
            for metric_name in ionosphere_enabled_metrics:
                try:
                    self.redis_conn.sadd('ionosphere.new_unique_metrics', metric_name)
                    # logger.info('added %s to ionosphere.new_unique_metrics Redis set' % metric_name)
                except:
                    logger.error(traceback.format_exc())
                    logger.info('error :: failed to add %s to ionosphere.new_unique_metrics Redis set' % metric_name)
                try:
                    self.redis_conn.setex('ionosphere.manage_ionosphere_unique_metrics', 300, time())
                except:
                    logger.error('error :: failed to set key :: ionosphere.manage_ionosphere_unique_metrics')
            try:
                logger.info('replacing Redis ionosphere.unique_metrics via rename of ionosphere.new_unique_metrics')
                self.redis_conn.rename('ionosphere.new_unique_metrics', 'ionosphere.unique_metrics')
                manage_ionosphere_unique_metrics = False
                ionosphere_unique_metrics = []
            except Exception as e:
                logger.error('error :: could not delete Redis set ionosphere.unique_metrics: %s' % str(e))
            try:
                self.redis_conn.setex('ionosphere.manage_ionosphere_unique_metrics', 300, time())
            except:
                logger.error('error :: failed to set key :: ionosphere.manage_ionosphere_unique_metrics')
            redis_ionosphere_unique_metrics = []
            try:
                # ionosphere_unique_metrics = list(self.redis_conn.smembers('ionosphere.unique_metrics'))
                redis_ionosphere_unique_metrics = self.redis_conn.smembers('ionosphere.unique_metrics')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get ionosphere.unique_metrics from Redis')
                # ionosphere_unique_metrics = []

            if redis_ionosphere_unique_metrics is not None:
                ionosphere_unique_metrics = list(redis_ionosphere_unique_metrics)
                ionosphere_unique_metrics_count = len(ionosphere_unique_metrics)
                logger.info('the new Redis ionosphere.unique_metrics set has %s metrics' % (str(ionosphere_unique_metrics_count)))
            else:
                logger.info('Redis ionosphere.unique_metrics unknown setting to []')
                ionosphere_unique_metrics = []

        if engine:
            try:
                engine.dispose()
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: calling engine.dispose()')

        return True

    # @added 20161230 - Feature #1830: Ionosphere alerts
    #                   Bug #1460: panorama check file fails
    #                   Panorama check file fails #24
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

        string_keys = ['metric', 'anomaly_dir', 'added_by', 'app', 'source']
        float_keys = ['value']
        # @modified 20170127 - Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
        # Added ionosphere_parent_id, always zero from Analyzer and Mirage
        int_keys = [
            'from_timestamp', 'metric_timestamp', 'added_at', 'full_duration',
            'ionosphere_parent_id']
        array_keys = ['algorithms', 'triggered_algorithms']
        boolean_keys = ['graphite_metric', 'run_crucible_tests']

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

# @added 20170109 - Feature #1854: Ionosphere learn
# Added the spawn_learn_process after determining to is not fit to bolt learn
# inside of ionosphere.py in its entirety, no point in more conditional nesting
# and bulking up ionosphere.py with more learn parameter to spin_process etc
# ionosphere.py works, as good as it gets, so extended with learn.py.  This uses
# the same no memory leak pattern that was adopted for smtp_alerts.
    def spawn_learn_process(self, i, timestamp):
        """
        Spawn a process to learn.

        This is used for Ionosphere to learn if anomalous metrics remain
        anomalous over time, as the resolution decreases.  It follows the
        multiprocessing methodology the was introduced in Analyzer and Mirage
        in the context of the process objects being cleared down and the learn
        processes cannot create memory leaks as the process always terminates or
        is terminated this prevents any memory leaks in the parent.

        """
        # @modified 20170117 - Feature #1854: Ionosphere learn - generations
        # Renamed the function from simple learn to the meme it has become
        # learn(timestamp)
        ionosphere_learn(timestamp)

    def spin_process(self, i, metric_check_file):
        """
        Assign an anomalous metric to check against features profiles.

        :param i: python process id
        :param metric_check_file: full path to the metric check file
        :type i: object
        :type metric_check_file: str
        :return: int
        :rtype: int or boolean

        """

        def get_an_engine():
            try:
                engine, log_msg, trace = get_engine(skyline_app)
                return engine, log_msg, trace
            except:
                logger.error(traceback.format_exc())
                log_msg = 'error :: failed to get MySQL engine in spin_process'
                logger.error('error :: failed to get MySQL engine in spin_process')
                return None, log_msg, trace

        def engine_disposal(engine):
            if engine:
                try:
                    engine.dispose()
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: calling engine.dispose()')
            return

        child_process_pid = os.getpid()
        logger.info('child_process_pid - %s' % str(child_process_pid))

        if settings.ENABLE_IONOSPHERE_DEBUG:
            logger.info('debug :: processing metric check - %s' % metric_check_file)

        if not os.path.isfile(str(metric_check_file)):
            logger.error('error :: file not found - metric_check_file - %s' % (str(metric_check_file)))
            return

        check_file_name = os.path.basename(str(metric_check_file))
        if settings.ENABLE_IONOSPHERE_DEBUG:
            logger.info('debug :: check_file_name - %s' % check_file_name)
        check_file_timestamp = check_file_name.split('.', 1)[0]
        if settings.ENABLE_IONOSPHERE_DEBUG:
            logger.info('debug :: check_file_timestamp - %s' % str(check_file_timestamp))
        check_file_metricname_txt = check_file_name.split('.', 1)[1]
        if settings.ENABLE_IONOSPHERE_DEBUG:
            logger.info('debug :: check_file_metricname_txt - %s' % check_file_metricname_txt)
        check_file_metricname = check_file_metricname_txt.replace('.txt', '')
        if settings.ENABLE_IONOSPHERE_DEBUG:
            logger.info('debug :: check_file_metricname - %s' % check_file_metricname)
        check_file_metricname_dir = check_file_metricname.replace('.', '/')
        if settings.ENABLE_IONOSPHERE_DEBUG:
            logger.info('debug :: check_file_metricname_dir - %s' % check_file_metricname_dir)

        metric_failed_check_dir = '%s/%s/%s' % (failed_checks_dir, check_file_metricname_dir, check_file_timestamp)

        failed_check_file = '%s/%s' % (metric_failed_check_dir, check_file_name)
        if settings.ENABLE_IONOSPHERE_DEBUG:
            logger.info('debug :: failed_check_file - %s' % failed_check_file)

        try:
            # Load and validate metric variables
            # @modified 20161231 - Feature #1830: Ionosphere alerts
            #                      Bug #1460: panorama check file fails
            #                      Panorama check file fails #24
            # Get rid of the skyline_functions imp as imp is deprecated in py3 anyway
            # Use def new_load_metric_vars(self, metric_vars_file):
            # metric_vars = load_metric_vars(skyline_app, str(metric_check_file))
            metric_vars_array = self.new_load_metric_vars(str(metric_check_file))
        except:
            logger.info(traceback.format_exc())
            logger.error('error :: failed to load metric variables from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        # Test metric variables
        # We use a pythonic methodology to test if the variables are defined,
        # this ensures that if any of the variables are not set for some reason
        # we can handle unexpected data or situations gracefully and try and
        # ensure that the process does not hang.
        metric = None
        try:
            # metric_vars.metric
            # metric = str(metric_vars.metric)
            key = 'metric'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            metric = str(value_list[0])
            base_name = metric
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - metric - %s' % metric)
        except:
            logger.info(traceback.format_exc())
            logger.error('error :: failed to read metric variable from check file - %s' % (metric_check_file))
            metric = None

        if not metric:
            logger.error('error :: failed to load metric variable from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        value = None
        try:
            # metric_vars.value
            # value = str(metric_vars.value)
            key = 'value'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            value = float(value_list[0])
            anomalous_value = value
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - value - %s' % str(value))
        except:
            logger.error('error :: failed to read value variable from check file - %s' % (metric_check_file))
            value = None

        if not value:
            logger.error('error :: failed to load value variable from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        from_timestamp = None
        try:
            # metric_vars.from_timestamp
            # from_timestamp = str(metric_vars.from_timestamp)
            key = 'from_timestamp'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            from_timestamp = int(value_list[0])
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - from_timestamp - %s' % str(from_timestamp))
        except:
            # @added 20160822 - Bug #1460: panorama check file fails
            # Added exception handling here
            logger.info(traceback.format_exc())
            logger.error('error :: failed to read from_timestamp variable from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        if not from_timestamp:
            logger.error('error :: failed to load from_timestamp variable from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        metric_timestamp = None
        try:
            # metric_vars.metric_timestamp
            # metric_timestamp = str(metric_vars.metric_timestamp)
            key = 'metric_timestamp'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            metric_timestamp = int(value_list[0])
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - metric_timestamp - %s' % str(metric_timestamp))
        except:
            logger.error('error :: failed to read metric_timestamp variable from check file - %s' % (metric_check_file))
            metric_timestamp = None

        if not metric_timestamp:
            logger.error('error :: failed to load metric_timestamp variable from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        try:
            # metric_vars.algorithms
            # algorithms = metric_vars.algorithms
            key = 'algorithms'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            algorithms = value_list[0]
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - algorithms - %s' % str(algorithms))
        except:
            logger.error('error :: failed to read algorithms variable from check file setting to all - %s' % (metric_check_file))
            algorithms = 'all'

        try:
            # metric_vars.triggered_algorithms
            # triggered_algorithms = metric_vars.triggered_algorithms
            key = 'triggered_algorithms'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            triggered_algorithms = value_list[0]
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - triggered_algorithms - %s' % str(triggered_algorithms))
        except:
            logger.error('error :: failed to read triggered_algorithms variable from check file setting to all - %s' % (metric_check_file))
            triggered_algorithms = 'all'

        added_by = None
        try:
            # metric_vars.added_by
            # added_by = str(metric_vars.added_by)
            key = 'added_by'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            added_by = str(value_list[0])
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - added_by - %s' % added_by)
        except:
            logger.error('error :: failed to read added_by variable from check file - %s' % (metric_check_file))
            added_by = None

        if not added_by:
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        # @added 20170117 - Feature #1854: Ionosphere learn - generations
        if str(added_by) == 'ionosphere_learn':
            logger.info('debug :: metric variable - added_by - %s' % added_by)

        try:
            # metric_vars.added_at
            # added_at = str(metric_vars.added_at)
            key = 'added_at'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            added_at = int(value_list[0])
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - added_at - %s' % str(added_at))
        except:
            logger.error('error :: failed to read added_at variable from check file setting to all - %s' % (metric_check_file))
            added_at = metric_timestamp

        # @added 20161228 - Feature #1828: ionosphere - mirage Redis data features
        # Added full_duration which needs to be recorded to allow Mirage metrics
        # to be profiled on Redis timeseries data at FULL_DURATION
        full_duration = None
        try:
            # metric_vars.full_duration
            # full_duration = str(metric_vars.full_duration)
            key = 'full_duration'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            full_duration = int(value_list[0])
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - full_duration - %s' % str(full_duration))
        except:
            logger.error('error :: failed to read full_duration variable from check file - %s' % (metric_check_file))
            full_duration = None

        if not full_duration:
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        # @added 20170127 - Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
        # Added ionosphere_parent_id, always zero from Analyzer and Mirage
        ionosphere_parent_id = None
        ionosphere_parent_id_determined = False
        try:
            key = 'ionosphere_parent_id'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            ionosphere_parent_id = int(value_list[0])
            ionosphere_parent_id_determined = True
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - ionosphere_parent_id - %s' % str(ionosphere_parent_id))
        except:
            logger.error('error :: failed to read ionosphere_parent_id variable from check file - %s' % (metric_check_file))
            ionosphere_parent_id = None

        if not ionosphere_parent_id_determined:
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        # @modified 20170116 - Feature #1854: Ionosphere learn
        # Do not check the cache key or anomaly age if added by ionosphere_learn
        if added_by != 'ionosphere_learn':
            # @added 20170101 - Feature #1830: Ionosphere alerts
            # Remove check file is an alert key exists
            cache_key = 'ionosphere.%s.alert.%s.%s' % (added_by, metric_timestamp, base_name)
            last_alert = False
            try:
                last_alert = self.redis_conn.get(cache_key)
            except Exception as e:
                logger.error('error :: could not query Redis for cache_key: %s' % e)
            if not last_alert:
                logger.info('debug :: no alert cache key - %s' % cache_key)
            else:
                logger.info('debug :: removing check - alert cache key exists - %s' % cache_key)
                self.remove_metric_check_file(str(metric_check_file))
                return

            now = time()
            anomaly_age = int(now) - int(metric_timestamp)
            if anomaly_age > max_age_seconds:
                logger.info(
                    'Ionosphere check max age exceeded - %s - %s seconds old, older than %s seconds discarding' % (
                        metric, str(anomaly_age), str(max_age_seconds)))
                with open(metric_check_file, 'rt') as fr:
                    metric_check_file_contents = fr.readlines()
                    logger.info(
                        'debug :: metric check file contents\n%s' % (str(metric_check_file_contents)))
                    self.remove_metric_check_file(str(metric_check_file))
                    return
        else:
            logger.info('processing check_file for ionosphere_learn - %s' % str(metric_check_file))

        # @added 20161222 - ionosphere should extract features for every anomaly
        # check that is sent through and calculate a feature_profile ready for
        # submission by the user if they so choose.  Further ionosphere could
        # make itself more useful by comparing any training data profiles to
        # further anomalies, however the feature profiles for subsequent
        # anomalies may be similar enough to match a few times and each a closer
        # match to the next.
        training_metric = False

        # Check if the metric has ionosphere_enabled, if not remove the check
        # file but not the data directory
        # @modified 20161230 - Feature #1830: Ionosphere alerts
        # Use SQLAlchemy method
        # query = "SELECT ionosphere_enabled FROM metrics WHERE metric='%s'" % metric
        # result = mysql_select(skyline_app, query)
        # if str(result[0]) != '1':
        #     logger.info('Ionosphere not enabled on %s' % (metric))
        #     # @modified 20161222 - do not remove metric file until features
        #     # calculated
        #     # self.remove_metric_check_file(str(metric_check_file))
        #     # return
        #     training_metric = True
        try:
            engine, log_msg, trace = get_an_engine()
            logger.info(log_msg)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: could not get a MySQL engine to determine ionosphere_enabled')

        if not engine:
            logger.error('error :: engine not obtained to determine ionosphere_enabled')

        # Get the metrics_table metadata
        metrics_table = None
        try:
            metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
            logger.info('metrics_table OK for %s' % base_name)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to get metrics_table meta for %s' % base_name)

        metrics_id = None
        metric_ionosphere_enabled = None

        # @added 20170115 - Feature #1854: Ionosphere learn - generations
        # Create the metrics_db_object so it is available to determine all
        # the details of all features profiles for the metric, this has all
        # the generations values avaialble in it.  Here we go! Learn!
        metrics_db_object = None

        try:
            connection = engine.connect()
            # stmt = select([metrics_table.c.ionosphere_enabled]).where(metrics_table.c.metric == str(metric))
            stmt = select([metrics_table]).where(metrics_table.c.metric == base_name)
            result = connection.execute(stmt)
            row = result.fetchone()
            metrics_id = row['id']
            metric_ionosphere_enabled = row['ionosphere_enabled']
            # @added 20170115 - Feature #1854: Ionosphere learn - generations
            # Create the metrics_db_object so it is available throughout
            # Here we go! Learn!
            metrics_db_object = row

            connection.close()

            if metric_ionosphere_enabled is not None:
                training_metric = False
            else:
                # @modified 20161222 - do not remove metric file until features
                # calculated
                # self.remove_metric_check_file(str(metric_check_file))
                # return
                training_metric = True
                # self.training_metrics.append(base_name)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: could not determine ionosphere_enabled from metrics table for - %s' % base_name)
            metric_ionosphere_enabled = None
            training_metric = True
            # self.training_metrics.append(base_name)

        # @added 20170116 - Feature #1854: Ionosphere learn - generations
        # If this is added_by ionosphere_learn the id is only
        # added if the use_full_duration_days features profile
        # is less than max_generations as if it is at the max
        # then a new features profile cannot be created from it
        # even if it is a match.
        metric_max_generations = None
        if added_by == 'ionosphere_learn':
            try:
                metric_max_generations = int(metrics_db_object['max_generations'])
                logger.info('determing max_generations for ionosphere_learn check - %s - %s' % (str(metric_max_generations), base_name))
            except:
                logger.error(traceback.format_exc())
                logger.error('error ::ionosphere_learn check could not determine the metric max_generations from the metrics_db_object for %s' % base_name)
            if not metric_max_generations:
                logger.error('error ::ionosphere_learn check cannot continue without max_generations for %s' % base_name)
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                if engine:
                    engine_disposal(engine)
                return

        # @added 20170108 - Feature #1830: Ionosphere alerts
        # Only process smtp_alerter_metrics
        if training_metric:
            if base_name in self.ionosphere_non_smtp_alerter_metrics:
                logger.error('error :: Ionosphere does not handle metrics that do not have a smtp alert context removing check for %s' % (base_name))
                self.remove_metric_check_file(str(metric_check_file))
                if engine:
                    engine_disposal(engine)
                return
            else:
                self.training_metrics.append(base_name)

        logger.info(
            'ionosphere_enabled is %s for metric id %s - %s' % (
                str(metric_ionosphere_enabled), str(metrics_id),
                base_name))

        if training_metric:
            logger.info('Ionosphere is not enabled on %s' % (base_name))
        else:
            logger.info('Ionosphere is enabled on %s' % (base_name))

        # @added 20161210 - Branch #922: ionosphere
        #                   Task #1658: Patterning Skyline Ionosphere
        # Only continue if there is a training data json timeseries file
        metric_timeseries_dir = base_name.replace('.', '/')

        # @modified 20170115 - Feature #1854: Ionosphere learn
        # Allowing the bifurcation of the metric_training_data_dir based on
        # whether added_by is ionosphere_learn or not, this allows Ionosphere to
        # be brought online to start evaluating the learn features profiles at
        # 30 days or whatever the learn_full_duration_days is for the metric
        # that is being automatically learnt uses these fuller duration features
        # to determine if a new training data set has been created for an
        # ionosphere_enabled metric.  Here Ionosphere starts to try and get
        # clever, let us hope not too clever, but this is where the
        # max_percent_diff_from_origin and max_generations comes in. So ...
        # here we go, a really "Crazy feedback loop" @astanway :)  I would say
        # that this is going to be way more useful than the last referenced one
        # in https://github.com/etsy/skyline/pull/90#r13592782 ;)  This is it
        # 20170115202500 UTC Ionosphere really is now really going to begin.
        # Here we go! Learn!
        # metric_training_data_dir = '%s/%s/%s' % (
        #     settings.IONOSPHERE_DATA_FOLDER, metric_timestamp,
        #     metric_timeseries_dir)
        if added_by != 'ionosphere_learn':
            metric_training_data_dir = '%s/%s/%s' % (
                settings.IONOSPHERE_DATA_FOLDER, metric_timestamp,
                metric_timeseries_dir)
        else:
            # Here we go! Learn you bugger!  SUCH A BIG THANKS TO tsfresh!
            # And flowjob and The White Stripes, @matzhouse, her and the Dude.
            metric_training_data_dir = '%s/%s/%s' % (
                settings.IONOSPHERE_LEARN_FOLDER, metric_timestamp,
                metric_timeseries_dir)

        anomaly_json = '%s/%s.json' % (metric_training_data_dir, base_name)
        if os.path.isfile(anomaly_json):
            logger.info('training data ts json available - %s' % (anomaly_json))
        else:
            logger.error('error :: training data ts json was not found - %s' % (anomaly_json))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            if engine:
                engine_disposal(engine)
            return

        # @added 20161228 - Feature #1828: ionosphere - mirage Redis data features
        # The timeseries full_duration needs to be recorded to allow Mirage metrics to
        # be profiled on Redis timeseries data at FULL_DURATION
        # e.g. mirage.redis.24h.json
        if training_metric:
            logger.info('training metric - %s' % (base_name))

        if added_by == 'mirage':
            logger.info('checking training data Redis json is available')
            # Always calculate features for both the SECOND_ORDER_RESOLUTION_SECONDS
            # timeseries data and the FULL_DURATION Redis timeseries data.
            # It is always preferable to create a features profile on a FULL_DURATION
            # data set, unless the user is flagging the actual Mirage timeseries as
            # not anomalous.  In the Mirage context the not anomalous may often be more
            # "visibile" in the FULL_DURATION view and if so should be matched on the
            # FULL_DURATION timeseries data, even if it is a Mirage metric.
            # Features profiles can be created for a Mirage metric on both the
            # FULL_DURATION and the SECOND_ORDER_RESOLUTION_SECONDS data sets, however
            # only one should be needed.
            # A features profile should always be created at the highest resolution
            # possible, FULL_DURATION data, wherever possible.
            try:
                full_duration_hours = str(int(settings.FULL_DURATION / 3600))
                redis_anomaly_json = '%s/%s.mirage.redis.%sh.json' % (metric_training_data_dir, base_name, full_duration_hours)
                if os.path.isfile(redis_anomaly_json):
                    logger.info('training data Redis full duration ts json available - %s' % (redis_anomaly_json))
                else:
                    logger.info('no training data Redis full duration json was not found - %s' % (redis_anomaly_json))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: training data Redis full duration json was not found - %s' % (redis_anomaly_json))

        # @added 20161209 - Branch #922: ionosphere
        #                   Task #1658: Patterning Skyline Ionosphere
        # Use SQLAlchemy, mysql.connector is still upstairs ^^ but starting the
        # move to SQLAlchemy now that all the webapp Ionosphere SQLAlchemy
        # patterns work and the database lay out if defined we can begin on the
        # data side.  Ionosphere was put together backwards, like tsfresh was
        # learnt.  It was the people input first here in many ways, which is
        # exactly how it was suppose to be.
        # This is now the Ionosphere meat.
        # Get a MySQL engine only if not training_metric
        if not training_metric:

            if not metrics_id:
                logger.error('error :: metric id not known')
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                if engine:
                    engine_disposal(engine)
                return False

            # @added 20170101 - Feature #1836: ionosphere - local features profiles disk cache
            # Cache fp ids for 300 seconds?

            logger.info('getting MySQL engine')
            try:
                engine, log_msg, trace = get_an_engine()
                logger.info(log_msg)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: could not get a MySQL engine to get fp_ids')

            if not engine:
                logger.error('error :: engine not obtained to get fp_ids')

            try:
                ionosphere_table, log_msg, trace = ionosphere_table_meta(skyline_app, engine)
                logger.info(log_msg)
                logger.info('ionosphere_table OK')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get ionosphere_table meta for %s' % base_name)

            # Determine the fp_ids that exist for the metric
            fp_ids = []
            fp_ids_found = False
            # @added 20170108 - Feature #1842: Ionosphere - Graphite now graphs
            # Added all_fp_ids so that we can handle multiple durations and not
            # error and reminds me of the needed metrics by FULL_DURATION
            # ionosphere.analyzer.unique_metrics (at FULL_DURATION)
            # ionosphere.mirage.unique_metrics (NOT at FULL_DURATION)
            all_fp_ids = []

            # @added 20170115 - Feature #1854: Ionosphere learn - generations
            # Create the fp_ids_db_object so it is available to determine all
            # the details of all features profiles for the metric, this has all
            # the generations values avaialble in it.
            # Here we go! Learn!
            fp_ids_db_object = None

            try:
                connection = engine.connect()
                stmt = select([ionosphere_table]).where(ionosphere_table.c.metric_id == metrics_id)
                result = connection.execute(stmt)
                for row in result:
                    # @added 20170116 - Feature #1854: Ionosphere learn
                    # if a features profiles is not enabled or deleted, skip it
                    if row['enabled'] != 1:
                        continue
                    if row['deleted'] == 1:
                        continue

                    fp_id = row['id']

                    # @added 20170108 - Feature #1842: Ionosphere - Graphite now graphs
                    # Added all_fp_ids
                    all_fp_ids.append(int(fp_id))

                    if int(row['full_duration']) == int(full_duration):
                        # @modified 20170116 - Feature #1854: Ionosphere learn - generations
                        # Handle ionosphere_learn
                        if added_by != 'ionosphere_learn':
                            fp_ids.append(int(fp_id))
                            logger.info('using fp id %s matched full_duration %s - %s' % (str(fp_id), str(full_duration), base_name))
                        else:
                            # @added 20170116 - Feature #1854: Ionosphere learn - generations
                            # If this is added_by ionosphere_learn the id is only
                            # added if the use_full_duration_days features profile
                            # is less than max_generations as if it is at the max
                            # then a new features profile cannot be created from it
                            # even if it is were to match.  Ionosphere learn is
                            # limited here on generation.
                            # Set the default as max e.g. not allowed
                            current_fp_generation = int(metric_max_generations)
                            try:
                                current_fp_generation = row['generation']
                                if int(current_fp_generation) < int(metric_max_generations):
                                    fp_ids.append(int(fp_id))
                                    logger.info(
                                        'valid ionosphere_learn generation %s - fp id %s matched full_duration %s - %s' % (
                                            str(current_fp_generation), str(fp_id),
                                            str(full_duration), base_name))
                                else:
                                    logger.info(
                                        'ionosphere_learn cannot check due to max_generations of %s would be exceeded, current generation %s - fp id %s matched full_duration %s - %s' % (
                                            str(metric_max_generations), str(current_fp_generation), str(fp_id),
                                            str(full_duration), base_name))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error(
                                    'error :: ionosphere_learn check could not determine the fp generation of fp id %s from the row object for %s' % (
                                        str(fp_id), base_name))
                    else:
                        logger.info('not using fp id %s not matched full_duration %s - %s' % (str(fp_id), str(full_duration), base_name))
                # @added 20170115 - Feature #1854: Ionosphere learn - generations
                # Create the fp_ids_db_object so it is available throughout
                # Here we go! Learn!
                fp_ids_db_object = row
                connection.close()
                fp_count = len(fp_ids)
                logger.info('determined %s fp ids for %s' % (str(fp_count), base_name))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: could not determine fp ids from DB for %s' % base_name)
                fp_count = 0

            if len(fp_ids) == 0:
                logger.info('there are no fp ids that match full duration for %s' % base_name)
            else:
                fp_ids_found = True

            if not fp_ids_found:
                logger.info('no fp ids were found for %s at %s' % (base_name, str(full_duration)))
                # @modified 20170108 - Feature #1842: Ionosphere - Graphite now graphs
                # Use all_fp_ids so that we can handle multiple durations
                # fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                if len(all_fp_ids) == 0:
                    logger.error('error :: Ionosphere is enabled on %s but has no feature_profiles' % (base_name))
                    fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                else:
                    self.remove_metric_check_file(str(metric_check_file))
                if engine:
                    engine_disposal(engine)
                return

        # @added 20161221 - TODO: why not calculate the features of every
        # anomaly so the the use does not have to do it and wait for the
        # features to be calculated.
        # Check the features were calculated by the webapp
        calculated_feature_file = '%s/%s.tsfresh.input.csv.features.transposed.csv' % (metric_training_data_dir, base_name)
        calculated_feature_file_found = False
        if os.path.isfile(calculated_feature_file):
            logger.info('calculated features available - %s' % (calculated_feature_file))
            calculated_feature_file_found = True

        # @added 20170115 - Feature #1854: Ionosphere learn - generations
        # ionosphere_learn should always provide the features profile csv
        # Ionosphere does not create features profiles for learn, it only
        # checks them.
        # Here we go! Learn!
        if added_by == 'ionosphere_learn':
            if not calculated_feature_file_found:
                logger.error('error :: no ionosphere_learn calculated_feature_file file found - %s' % calculated_feature_file)
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                if engine:
                    engine_disposal(engine)
                return

        if not calculated_feature_file_found:
            if training_metric:
                # Allow Graphite resources to be created if they are not an alert
                # was not sent therefore features do not need to be calculated
                check_time = int(time())
                check_age = check_time - int(added_at)
                if check_age < 5:
                    sleep(5)
                graphite_file_count = len([f for f in os.listdir(metric_training_data_dir)
                                           if f.endswith('.png') and
                                           os.path.isfile(os.path.join(metric_training_data_dir, f))])
                if graphite_file_count == 0:
                    logger.info('not calculating features no anomaly Graphite alert resources created in %s' % (metric_training_data_dir))
                    self.remove_metric_check_file(str(metric_check_file))
                    if engine:
                        engine_disposal(engine)
                    return
                else:
                    logger.info('anomaly Graphite alert resources found in %s' % (metric_training_data_dir))

        context = skyline_app
        f_calc = None
        if not calculated_feature_file_found:
            try:
                fp_csv, successful, fp_exists, fp_id, log_msg, traceback_format_exc, f_calc = calculate_features_profile(skyline_app, metric_timestamp, base_name, context)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to calculate features')
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                if engine:
                    engine_disposal(engine)
                return

        if os.path.isfile(calculated_feature_file):
            logger.info('calculated features available - %s' % (calculated_feature_file))
            calculated_feature_file_found = True
            if f_calc:
                send_metric_name = '%s.features_calculation_time' % skyline_app_graphite_namespace
                f_calc_time = '%.2f' % float(f_calc)
                try:
                    send_graphite_metric(skyline_app, send_metric_name, f_calc_time)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to send calculate features')

            if training_metric:
                logger.info('training metric done')
                self.remove_metric_check_file(str(metric_check_file))
                # TODO: make ionosphere more useful, compare any other
                # available training_metric profiles here and match, not in the
                # db context, in the training context.
                if engine:
                    engine_disposal(engine)
                return

        if not calculated_feature_file_found:
            logger.error('error :: calculated features file not available - %s' % (calculated_feature_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            if engine:
                engine_disposal(engine)
            return

        # @modified 20161213 - Branch #1790: test_tsfresh
        # TODO: Match the test_tsfresh method
        # Create an array of the calculated features
        calculated_features = []
        if calculated_feature_file_found:
            count_id = 0
            with open(calculated_feature_file, 'rb') as fr:
                reader = csv.reader(fr, delimiter=',')
                for i, line in enumerate(reader):
                    if str(line[0]) != '':
                        if ',' in line[0]:
                            feature_name = '"%s"' % str(line[0])
                        else:
                            feature_name = str(line[0])
                        count_id += 1
                        calc_value = float(line[1])
                        calculated_features.append([feature_name, calc_value])

        if len(calculated_features) == 0:
            logger.error('error :: no calculated features were determined from - %s' % (calculated_feature_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            if engine:
                engine_disposal(engine)
            return

        # Compare calculated features to feature values for each fp id
        not_anomalous = False
        if calculated_feature_file_found:
            for fp_id in fp_ids:
                if not metrics_id:
                    logger.error('error :: metric id not known')
                    fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                    if engine:
                        engine_disposal(engine)
                    return False

                self.features_profiles_checked.append(fp_id)
                features_count = None
                fp_features = []
                # Get features for fp_id from z_fp_<metric_id> table where the
                # features profile is the same full_duration
                metric_fp_table = 'z_fp_%s' % str(metrics_id)
                try:
                    engine, log_msg, trace = get_an_engine()
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: could not get a MySQL engine for feature_id and values from %s' % metric_fp_table)

                if not engine:
                    logger.error('error :: engine not obtained for feature_id and values from %s' % metric_fp_table)

                try:
                    stmt = 'SELECT feature_id, value FROM %s WHERE fp_id=%s' % (metric_fp_table, str(fp_id))
                    connection = engine.connect()
                    for row in engine.execute(stmt):
                        fp_feature_id = int(row['feature_id'])
                        fp_value = float(row['value'])
                        fp_features.append([fp_feature_id, fp_value])
                    connection.close()
                    features_count = len(fp_features)
                    logger.info('determined %s features for fp_id %s' % (str(features_count), str(fp_id)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: could not determine feature_id, value from %s' % metric_fp_table)

                # @added 20170107 - Feature #1852: Ionosphere - features_profile matched graphite graphs
                #                   Feature #1844: ionosphere_matched DB table
                # Added the calculated features sum for verification purposes
                all_calc_features_sum_list = []
                for feature_name, calc_value in calculated_features:
                    all_calc_features_sum_list.append(float(calc_value))
                all_calc_features_sum = sum(all_calc_features_sum_list)

                # Convert feature names in calculated_features to their id
                logger.info('converting tsfresh feature names to Skyline feature ids')
                calc_features_by_id = []
                for feature_name, calc_value in calculated_features:
                    for skyline_feature_id, name in TSFRESH_FEATURES:
                        if feature_name == name:
                            calc_features_by_id.append([skyline_feature_id, float(calc_value)])

                # Determine what features each data has, extract only values for
                # common features.
                logger.info('determining common features')
                relevant_fp_feature_values = []
                relevant_calc_feature_values = []
                for skyline_feature_id, calc_value in calc_features_by_id:
                    for fp_feature_id, fp_value in fp_features:
                        if skyline_feature_id == fp_feature_id:
                            relevant_fp_feature_values.append(fp_value)
                            relevant_calc_feature_values.append(calc_value)

                # Determine the sum of each set
                relevant_fp_feature_values_count = len(relevant_fp_feature_values)
                relevant_calc_feature_values_count = len(relevant_calc_feature_values)
                if relevant_fp_feature_values_count != relevant_calc_feature_values_count:
                    logger.error('error :: mismatch in number of common features')
                    logger.error('error :: relevant_fp_feature_values_count - %s' % str(relevant_fp_feature_values_count))
                    logger.error('error :: relevant_calc_feature_values_count - %s' % str(relevant_calc_feature_values_count))
                    continue
                else:
                    logger.info('comparing on %s common features' % str(relevant_fp_feature_values_count))

                if relevant_fp_feature_values_count == 0:
                    logger.error('error :: relevant_fp_feature_values_count is zero')
                    continue

                # Determine the sum of each set
                sum_fp_values = sum(relevant_fp_feature_values)
                sum_calc_values = sum(relevant_calc_feature_values)
                logger.info(
                    'sum of the values of the %s common features in features profile - %s' % (
                        str(relevant_fp_feature_values_count), str(sum_fp_values)))
                logger.info(
                    'sum of the values of the %s common features in the calculated features - %s' % (
                        str(relevant_calc_feature_values_count), str(sum_calc_values)))

                # Determine whether each set is positive or negative
                # # if the same carry on
                # # if both negative, make then both positive
                # Sum fp values, Sum calculated - handle negatives like features_sum :: -3389570699080000.0000000000
                # Determine whether each set is positive or negative
                # # if the same carry on
                # # if both negative, make then both positive postive_sums
                fp_sum_array = [sum_fp_values]
                calc_sum_array = [sum_calc_values]

                percent_different = 100
                sums_array = np.array([sum_fp_values, sum_calc_values], dtype=float)
                try:
                    calc_percent_different = np.diff(sums_array) / sums_array[:-1] * 100.
                    percent_different = calc_percent_different[0]
                    logger.info('percent_different between common features sums - %s' % str(percent_different))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to calculate percent_different')
                    continue

                almost_equal = None
                try:
                    np.testing.assert_array_almost_equal(fp_sum_array, calc_sum_array)
                    almost_equal = True
                except:
                    almost_equal = False

                if almost_equal:
                    not_anomalous = True
                    # @modified 20170118 - Bug #1860: Debug learn not matched in ionosphere
                    # This broke it, no variable was interpolated
                    # logger.info('common features sums are almost equal, not anomalous' % str(relevant_fp_feature_values_count))
                    logger.info('common features sums are almost equal, not anomalous')

                # @added 20161229 - Feature #1830: Ionosphere alerts
                # Update the features profile checked count and time
                logger.info('updating checked details in db for %s' % (str(fp_id)))
                # update matched_count in ionosphere_table
                checked_timestamp = int(time())

                try:
                    engine, log_msg, trace = get_an_engine()
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: could not get a MySQL engine to update checked details in db for %s' % (str(fp_id)))
                if not engine:
                    logger.error('error :: engine not obtained to update checked details in db for %s' % (str(fp_id)))

                try:
                    connection = engine.connect()
                    connection.execute(
                        ionosphere_table.update(
                            ionosphere_table.c.id == fp_id).
                        values(checked_count=ionosphere_table.c.checked_count + 1,
                               last_checked=checked_timestamp))
                    connection.close()
                    logger.info('updated checked_count for %s' % str(fp_id))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: could not update checked_count and last_checked for %s ' % str(fp_id))

                # if diff_in_sums <= 1%:
                if percent_different < 0:
                    new_pdiff = percent_different * -1
                    percent_different = new_pdiff

                if percent_different < settings.IONOSPHERE_FEATURES_PERCENT_SIMILAR:
                    not_anomalous = True
                    # log
                    logger.info('not anomalous - features profile match - %s' % base_name)
                    logger.info(
                        'calculated features sum are within %s percent of fp_id %s with %s, not anomalous' %
                        (str(settings.IONOSPHERE_FEATURES_PERCENT_SIMILAR),
                            str(fp_id), str(percent_different)))

                if not_anomalous:
                    self.not_anomalous.append(base_name)
                    # update matched_count in ionosphere_table
                    matched_timestamp = int(time())
                    try:
                        engine, log_msg, trace = get_an_engine()
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: could not get a MySQL engine to update matched details in db for %s' % (str(fp_id)))
                    if not engine:
                        logger.error('error :: engine not obtained to update matched details in db for %s' % (str(fp_id)))

                    try:
                        connection = engine.connect()
                        connection.execute(
                            ionosphere_table.update(
                                ionosphere_table.c.id == fp_id).
                            values(matched_count=ionosphere_table.c.matched_count + 1,
                                   last_matched=matched_timestamp))
                        connection.close()
                        logger.info('updated matched_count for %s' % str(fp_id))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: could not update matched_count and last_matched for %s ' % str(fp_id))

                    # @added 20170107 - Feature #1844: ionosphere_matched DB table
                    # Added ionosphere_matched update
                    try:
                        engine, log_msg, trace = get_an_engine()
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: could not get a MySQL engine to update ionosphere_matched for %s' % (str(fp_id)))
                    if not engine:
                        logger.error('error :: engine not obtained to update ionosphere_matched for %s' % (str(fp_id)))

                    try:
                        ionosphere_matched_table, log_msg, trace = ionosphere_matched_table_meta(skyline_app, engine)
                        logger.info(log_msg)
                        logger.info('ionosphere_matched_table OK')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to get ionosphere_matched_table meta for %s' % base_name)

                    try:
                        connection = engine.connect()
                        # @modified 20170107 - Feature #1852: Ionosphere - features_profile matched graphite graphs
                        #                      Feature #1844: ionosphere_matched DB table
                        # Added all_calc_features_sum, all_calc_features_count,
                        # sum_calc_values, common_features_count, tsfresh_version
                        ins = ionosphere_matched_table.insert().values(
                            fp_id=int(fp_id),
                            metric_timestamp=int(metric_timestamp),
                            all_calc_features_sum=float(all_calc_features_sum),
                            all_calc_features_count=len(all_calc_features_sum_list),
                            sum_common_values=float(sum_calc_values),
                            common_features_count=int(relevant_calc_feature_values_count),
                            tsfresh_version=str(tsfresh_version))
                        result = connection.execute(ins)
                        connection.close()
                        new_matched_id = result.inserted_primary_key[0]
                        logger.info('new ionosphere_matched id: %s' % str(new_matched_id))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error(
                            'error :: could not update ionosphere_matched for %s with with timestamp %s' % (
                                str(fp_id), str(metric_timestamp)))

                    # @added 20170115 - Feature #1854: Ionosphere learn - generations
                    # Stop on the first match
                    break

                # https://docs.scipy.org/doc/numpy/reference/generated/numpy.testing.assert_almost_equal.html
                # @added 20161214 - Add a between timeframe option, e.g. if
                # fp match, only see this as not anomalous if hour (and or min)
                # is between x and y - handle rollovers, cron log archives, etc.
                logger.info('debug :: %s is a features profile for %s' % (str(fp_id), base_name))

            # @added 20170115 - Feature #1854: Ionosphere learn - generations
            # If this is an ionosphere_learn check them we handle it before
            # the others and exit and ionosphere_learn uses the Redis work
            # queue. Here we go! Learn!
            if added_by == 'ionosphere_learn':
                if not_anomalous:
                    logger.info('an ionosphere_learn metric has been found to be not anomalous before')
                    logger.info(
                        'ionosphere_learn metric matches the generation %s features profile id %s - %s' % (
                            str(current_fp_generation), str(fp_id), base_name))
                    # Added Redis to work_set, learn will then go off and create
                    # the features profile with the parent training data if
                    # less than max_generations, although ionosphere_learn
                    # should not should Ionosphere any work if the result would
                    # be greater than max_generations
                    logger.info('adding work item to Redis set ionosphere.learn.work')
                    ionosphere_job = 'learn_fp_learnt'
                    try:
                        logger.info(
                            'LEARNT :: adding work to Redis ionosphere.learn.work set - [\'Soft\', \'%s\', %s, \'%s\', %s, %s] to create a learnt features profile' % (
                                str(ionosphere_job), str(metric_timestamp), base_name,
                                str(fp_id), str(current_fp_generation)))
                        self.redis_conn.sadd('ionosphere.learn.work', ['Soft', str(ionosphere_job), int(metric_timestamp), base_name, int(fp_id), int(current_fp_generation)])
                    except:
                        logger.error(traceback.format_exc())
                        logger.error(
                            'error :: failed adding work to Redis ionosphere.learn.work set - [\'Soft\', \'%s\', %s, \'%s\', %s, %s] to make a learn features profile later' % (
                                str(ionosphere_job), str(metric_timestamp), base_name,
                                str(fp_id), str(current_fp_generation)))

                # Exit the ionosphere_learn check
                self.remove_metric_check_file(str(metric_check_file))
                if engine:
                    engine_disposal(engine)
                return

            if not not_anomalous:
                logger.info('anomalous - no feature profiles were matched - %s' % base_name)

                # @added 20170116 - Feature #1854: Ionosphere learn
                # If this is an ionosphere_learn check an Ionosphere alert will
                # not be sent back to Analyzer, Mirage or the ionosphere.learn.work
                # Redis set. We exit, work is done.
                if added_by == 'ionosphere_learn':
                    logger.info('ionosphere_learn check complete - %s' % base_name)
                    self.remove_metric_check_file(str(metric_check_file))
                    if engine:
                        engine_disposal(engine)
                    return

                self.anomalous_metrics.append(base_name)
                # Send to panorama as Analyzer and Mirage will only alert on the
                # anomaly, they will not push it to Panorama
                if settings.PANORAMA_ENABLED:
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
                        % (base_name, str(anomalous_value), str(int(from_timestamp)),
                           str(int(metric_timestamp)), str(settings.ALGORITHMS),
                           str(triggered_algorithms), skyline_app, source,
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
                        self.sent_to_panorama.append(base_name)
                    except:
                        logger.error('error :: failed to add panorama anomaly file :: %s' % (panaroma_anomaly_file))
                        logger.info(traceback.format_exc())

            #     alert ... hmmm the harder part, maybe not all the resources
            #     are already created, so just determining ALERTS and firing a
            #     trigger_alert (pull in alerter.py and mirage_alerters.py?)
            #     OR send back to app via Redis
                # @modified 20170116 - Feature #1854: Ionosphere learn
                # Only do the cache_key if not ionosphere_learn
                if added_by != 'ionosphere_learn':
                    cache_key = 'ionosphere.%s.alert.%s.%s' % (added_by, metric_timestamp, base_name)
                    try:
                        self.redis_conn.setex(
                            cache_key, 300,
                            [float(anomalous_value), base_name, int(metric_timestamp), triggered_algorithms, full_duration])
                        logger.info(
                            'add Redis alert key - %s - [%s, \'%s\', %s, %s]' %
                            (cache_key, str(anomalous_value), base_name, str(int(metric_timestamp)),
                                str(triggered_algorithms)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error(
                            'error :: failed to add Redis key - %s - [%s, \'%s\', %s, %s]' %
                            (cache_key, str(anomalous_value), base_name, str(int(metric_timestamp)),
                                str(triggered_algorithms)))

                # @added 20170116 - Feature #1854: Ionosphere learn
                # Added an ionosphere_learn job for the timeseries that did not
                # match any profiles.  Here we go! Learn!
                if added_by != 'ionosphere_learn':
                    ionosphere_job = 'learn_fp_generation'
                    logger.info(
                        'adding an ionosphere_learn %s job for the timeseries that did not match any profiles - %s' % (
                            ionosphere_job, base_name))
                    try:
                        logger.info(
                            'adding work to Redis ionosphere.learn.work set - [\'Soft\', \'%s\', %s, \'%s\', None, None] to make a learn features profile later' % (
                                str(ionosphere_job), str(int(metric_timestamp)),
                                base_name))
                        self.redis_conn.sadd('ionosphere.learn.work', ['Soft', str(ionosphere_job), int(metric_timestamp), base_name, None, None])
                    except:
                        logger.error(traceback.format_exc())
                        logger.error(
                            'error :: failed adding work to Redis ionosphere.learn.work set - [\'Soft\', \'%s\', %s, \'%s\', None, None] to make a learn features profile later' % (
                                str(ionosphere_job), str(int(metric_timestamp)),
                                base_name))

                self.remove_metric_check_file(str(metric_check_file))
                if engine:
                    engine_disposal(engine)
                return

        self.remove_metric_check_file(str(metric_check_file))
        if engine:
            engine_disposal(engine)
        return

    def run(self):
        """
        Called when the process intializes.
        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        if os.path.isfile(skyline_app_logwait):
            try:
                logger.info('removing %s' % skyline_app_logwait)
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
                if ENABLE_IONOSPHERE_DEBUG:
                    logger.info('debug :: connected to Redis')
            except:
                logger.error('error :: cannot connect to redis at socket path %s' % (
                    settings.REDIS_SOCKET_PATH))
                sleep(30)
                self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                continue

            # Report app up
            try:
                self.redis_conn.setex(skyline_app, 120, now)
                logger.info('updated Redis key for %s up' % skyline_app)
            except:
                logger.error('error :: failed to update Redis key for %s up' % skyline_app)

            # purge_old_data_dirs after every check file run, this takes less
            # than a second and keeps the purging somewhat consistent with
            # input rate.
            try:
                logger.info('purging any old training data')
                self.purge_old_data_dirs(
                    settings.IONOSPHERE_DATA_FOLDER,
                    settings.IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR)
            except:
                logger.error('error :: purge_old_data_dirs - %s' % traceback.print_exc())
                if ENABLE_IONOSPHERE_DEBUG:
                    logger.info(
                        'debug :: self.purge_old_data_dirs(%s, %s)' %
                        settings.IONOSPHERE_DATA_FOLDER,
                        settings.IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR)

            # @added 20170110 - Feature #1854: Ionosphere learn
            # purge_old_data_dirs learn data
            if settings.IONOSPHERE_LEARN:
                try:
                    logger.info('purging any old learning data')
                    self.purge_old_data_dirs(
                        settings.IONOSPHERE_LEARN_FOLDER,
                        settings.IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR)
                except:
                    logger.error('error :: purge_old_data_dirs learn - %s' % traceback.print_exc())
                    if ENABLE_IONOSPHERE_DEBUG:
                        logger.info(
                            'debug :: self.purge_old_data_dirs(%s, %s)' %
                            settings.IONOSPHERE_LEARN_FOLDER,
                            settings.IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR)

            # self.populate the database metatdata tables
            # What is my host id in the Skyline panorama DB?
            host_id = False
            query = 'select id FROM hosts WHERE host=\'%s\'' % this_host
            results = mysql_select(skyline_app, query)
            if results:
                host_id = results[0][0]
                logger.info('host_id: %s' % str(host_id))
            else:
                logger.info('failed to determine host id of %s' % this_host)

            # if not known - INSERT hostname INTO host
            if not host_id:
                logger.info('inserting %s into hosts table' % this_host)
                query = 'insert into hosts (host) VALUES (\'%s\')' % this_host
                host_id = self.mysql_insert(query)
                if host_id:
                    logger.info('new host_id: %s' % str(host_id))

            if not host_id:
                logger.error(
                    'error :: failed to determine populate %s into the hosts table' %
                    this_host)
                sleep(30)
                continue

            """
            Determine if any metric has been added to add
            """
            while True:
                metric_var_files = False
                try:
                    metric_var_files = [f for f in listdir(settings.IONOSPHERE_CHECK_PATH) if isfile(join(settings.IONOSPHERE_CHECK_PATH, f))]
                except:
                    logger.error('error :: failed to list files in check dir')
                    logger.info(traceback.format_exc())

                if not metric_var_files:
                    logger.info('sleeping 20 no metric check files')
                    sleep(20)
                    up_now = time()
                    # Report app up
                    try:
                        self.redis_conn.setex(skyline_app, 120, up_now)
                        logger.info('updated Redis key for %s up' % skyline_app)
                    except:
                        logger.error('error :: failed to update Redis key for %s up' % skyline_app)

                    # @added 20161228 - Feature #1828: ionosphere - mirage Redis data features
                    #                   Branch #922: Ionosphere
                    # Bringing Ionosphere online - do alert on Ionosphere metrics
                    # Manage the ionosphere.unique_metrics Redis set which is queried
                    # by Analyzer and Mirage, yes and we use multiprocessing
                    last_update = None
                    try:
                        last_update = self.redis_conn.get('ionosphere.manage_ionosphere_unique_metrics')
                    except Exception as e:
                        logger.error('error :: could not query Redis for ionosphere.manage_ionosphere_unique_metrics: %s' % e)

                    if not last_update:
                        pids = []
                        now = time()
                        try:
                            logger.info('starting manage_ionosphere_unique_metrics process')
                            p = Process(target=self.manage_ionosphere_unique_metrics)
                            pids.append(p)
                            p.start()
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: failed to start manage_ionosphere_unique_metrics')

                        # Self monitor process and terminate if run for too long
                        p_starts = time()
                        while time() - p_starts <= 5:
                            if any(p.is_alive() for p in pids):
                                # Just to avoid hogging the CPU
                                sleep(.1)
                            else:
                                # All the processes are done, break now.
                                time_to_run = time() - p_starts
                                logger.info(
                                    'manage_ionosphere_unique_metrics completed in %.2f seconds' % (
                                        time_to_run))
                                break
                        else:
                            # We only enter this if we didn't 'break' above.
                            logger.info('%s :: timed out, killing manage_ionosphere_unique_metrics process' % (skyline_app))
                            for p in pids:
                                try:
                                    p.terminate()
                                    # p.join()
                                    logger.info('%s :: killed manage_ionosphere_unique_metrics process' % (skyline_app))
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: killing all manage_ionosphere_unique_metrics processes')

                # Discover metric anomalies to insert
                metric_var_files = False
                try:
                    metric_var_files = [f for f in listdir(settings.IONOSPHERE_CHECK_PATH) if isfile(join(settings.IONOSPHERE_CHECK_PATH, f))]
                except:
                    logger.error('error :: failed to list files in check dir')
                    logger.info(traceback.format_exc())

                # @added 20170104 - Feature #1842: Ionosphere - Graphite now graphs
                #                   Task #1658: Patterning Skyline Ionosphere
                # Send Ionosphere metrics to Graphite every minute now that
                # Ionosphere is better tuned and Reset lists
                cache_key = '%s.sent_graphite_metrics' % skyline_app
                redis_sent_graphite_metrics = False
                try:
                    redis_sent_graphite_metrics = self.redis_conn.get(cache_key)
                except Exception as e:
                    logger.error('error :: could not query Redis for key %s: %s' % (cache_key, e))

                # Flush metrics to Graphite
                if not redis_sent_graphite_metrics:
                    try:
                        not_anomalous = str(len(self.not_anomalous))
                    except:
                        not_anomalous = '0'
                    logger.info('not_anomalous      :: %s' % not_anomalous)
                    send_metric_name = '%s.not_anomalous' % skyline_app_graphite_namespace
                    send_graphite_metric(skyline_app, send_metric_name, not_anomalous)

                    try:
                        total_anomalies = str(len(self.anomalous_metrics))
                    except:
                        total_anomalies = '0'
                    logger.info('total_anomalies    :: %s' % total_anomalies)
                    send_metric_name = '%s.total_anomalies' % skyline_app_graphite_namespace
                    send_graphite_metric(skyline_app, send_metric_name, total_anomalies)

                    try:
                        training_metrics = str(len(self.training_metrics))
                    except:
                        training_metrics = '0'
                    logger.info('training metrics   :: %s' % training_metrics)
                    send_metric_name = '%s.training_metrics' % skyline_app_graphite_namespace
                    send_graphite_metric(skyline_app, send_metric_name, training_metrics)

                    try:
                        features_profiles_checked = str(len(self.features_profiles_checked))
                    except:
                        features_profiles_checked = '0'
                    logger.info('fps checked count  :: %s' % features_profiles_checked)
                    send_metric_name = '%s.fps_checked' % skyline_app_graphite_namespace
                    send_graphite_metric(skyline_app, send_metric_name, not_anomalous)

                    if settings.PANORAMA_ENABLED:
                        try:
                            sent_to_panorama = str(len(self.sent_to_panorama))
                        except:
                            sent_to_panorama = '0'
                        logger.info('sent_to_panorama   :: %s' % sent_to_panorama)
                        send_metric_name = '%s.sent_to_panorama' % skyline_app_graphite_namespace
                        send_graphite_metric(skyline_app, send_metric_name, sent_to_panorama)

                    sent_graphite_metrics_now = int(time())
                    try:
                        self.redis_conn.setex(cache_key, 59, sent_graphite_metrics_now)
                        logger.info('updated Redis key - %s' % cache_key)
                    except:
                        logger.error('error :: failed to update Redis key - %s up' % cache_key)

                    # Reset lists
                    self.anomalous_metrics[:] = []
                    self.not_anomalous[:] = []
                    self.features_profiles_checked[:] = []
                    self.training_metrics[:] = []
                    self.sent_to_panorama[:] = []

                ionosphere_job = False
                learn_job = False

                if metric_var_files:
                    ionosphere_job = True
                    break

                # @added 20170113 - Feature #1854: Ionosphere learn
                # Added the learn variable to spawn a spawn_learn_process when
                # required.
                work_queue_items = 0
                if settings.IONOSPHERE_LEARN:
                    learn_work = None
                    try:
                        learn_work = self.redis_conn.smembers('ionosphere.learn.work')
                    except Exception as e:
                        logger.error('error :: could not query Redis for ionosphere.learn.work - %s' % e)
                    if learn_work:
                        work_queue_items = len(learn_work)
                        if work_queue_items > 0:
                            learn_job = True
                if learn_job:
                    break

            if ionosphere_job:
                metric_var_files_sorted = sorted(metric_var_files)
                metric_check_file = '%s/%s' % (settings.IONOSPHERE_CHECK_PATH, str(metric_var_files_sorted[0]))

            # @added 20170108 - Feature #1830: Ionosphere alerts
            # Adding lists of smtp_alerter_metrics and ionosphere_non_smtp_alerter_metrics
            # Timed this takes 0.013319 seconds on 689 unique_metrics
            unique_metrics = []
            try:
                unique_metrics = list(self.redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: could not get the unique_metrics list from Redis')
                unique_metrics = []

            for metric_name in unique_metrics:
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
                                if base_name not in self.ionosphere_smtp_alerter_metrics:
                                    self.ionosphere_smtp_alerter_metrics.append(base_name)
                        except:
                            pattern_match = False

                        if not pattern_match:
                            # Match by substring
                            if alert[0] in base_name:
                                if base_name not in self.ionosphere_smtp_alerter_metrics:
                                    self.ionosphere_smtp_alerter_metrics.append(base_name)

                if base_name not in self.ionosphere_smtp_alerter_metrics:
                    if base_name not in self.ionosphere_smtp_alerter_metrics:
                        self.ionosphere_non_smtp_alerter_metrics.append(base_name)
            logger.info('smtp_alerter_metrics     :: %s' % str(len(self.ionosphere_smtp_alerter_metrics)))
            logger.info('ionosphere_non_smtp_alerter_metrics :: %s' % str(len(self.ionosphere_non_smtp_alerter_metrics)))

            if ionosphere_job:
                logger.info('processing - %s' % str(metric_var_files_sorted[0]))
                function_name = 'spin_process'

            # @added 20170109 - Feature #1854: Ionosphere learn
            # Added the learn variable to spawn a spawn_learn_process when
            # required.
            # @added 20170112 - Feature #1854: Ionosphere learn - Redis ionosphere.learn.work namespace
            # Ionosphere learn needs Redis works sets
            # When a features profile is created there needs to be work added to a Redis
            # set
            # When a human makes a features profile, we want Ionosphere to make a
            # use_full_duration_days features profile valid_learning_duration (e.g.
            # 3361) later.
            if learn_job:
                logger.info('processing - learn work queue - %s' % str(work_queue_items))
                function_name = 'spawn_learn_process'

            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            now = time()
            for i in range(1, settings.IONOSPHERE_PROCESSES + 1):
                if ionosphere_job:
                    try:
                        p = Process(target=self.spin_process, args=(i, metric_check_file))
                        pids.append(p)
                        pid_count += 1
                        logger.info(
                            'starting %s of %s %s' % (
                                str(pid_count),
                                str(settings.IONOSPHERE_PROCESSES),
                                function_name))
                        p.start()
                        spawned_pids.append(p.pid)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to start %s' % function_name)
                        continue

                # @added 20170113 - Feature #1854: Ionosphere learn - Redis ionosphere.learn.work namespace
                if learn_job:
                    try:
                        p = Process(target=self.spawn_learn_process, args=(i, int(now)))
                        pids.append(p)
                        pid_count += 1
                        logger.info(
                            'starting %s of %s %s' % (
                                str(pid_count),
                                str(settings.IONOSPHERE_PROCESSES),
                                function_name))
                        p.start()
                        spawned_pids.append(p.pid)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to start %s' % function_name)
                        continue

            # Self monitor processes and terminate if any spin_process has run
            # for to long
            p_starts = time()
            while time() - p_starts <= 20:
                if any(p.is_alive() for p in pids):
                    # Just to avoid hogging the CPU
                    sleep(.1)
                else:
                    # All the processes are done, break now.
                    time_to_run = time() - p_starts
                    logger.info(
                        '%s %s completed in %.2f seconds' % (
                            str(settings.IONOSPHERE_PROCESSES),
                            function_name, time_to_run))
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('timed out, killing all %s processes' % (function_name))
                for p in pids:
                    try:
                        p.terminate()
                        # p.join()
                        logger.info('killed %s process' % (function_name))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: killing all %s processes' % function_name)

                if ionosphere_job:
                    check_file_name = os.path.basename(str(metric_check_file))
                    if settings.ENABLE_IONOSPHERE_DEBUG:
                        logger.info('debug :: check_file_name - %s' % check_file_name)
                    check_file_timestamp = check_file_name.split('.', 1)[0]
                    if settings.ENABLE_IONOSPHERE_DEBUG:
                        logger.info('debug :: check_file_timestamp - %s' % str(check_file_timestamp))
                    check_file_metricname_txt = check_file_name.split('.', 1)[1]
                    if settings.ENABLE_IONOSPHERE_DEBUG:
                        logger.info('debug :: check_file_metricname_txt - %s' % check_file_metricname_txt)
                    check_file_metricname = check_file_metricname_txt.replace('.txt', '')
                    if settings.ENABLE_IONOSPHERE_DEBUG:
                        logger.info('debug :: check_file_metricname - %s' % check_file_metricname)
                    check_file_metricname_dir = check_file_metricname.replace('.', '/')
                    if settings.ENABLE_IONOSPHERE_DEBUG:
                        logger.info('debug :: check_file_metricname_dir - %s' % check_file_metricname_dir)

                    metric_failed_check_dir = '%s/%s/%s' % (failed_checks_dir, check_file_metricname_dir, check_file_timestamp)
                    fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))

            for p in pids:
                if p.is_alive():
                    logger.info('stopping %s - %s' % (function_name, str(p.is_alive())))
                    p.join()

            # @added 20170108 - Feature #1830: Ionosphere alerts
            # Reset added lists of ionospehere_smtp_alerter_metrics and
            # ionosphere_non_smtp_alerter_metrics
            self.ionosphere_smtp_alerter_metrics[:] = []
            self.ionosphere_non_smtp_alerter_metrics[:] = []
