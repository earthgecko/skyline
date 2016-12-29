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

from redis import StrictRedis
from msgpack import Unpacker, packb
import traceback
import mysql.connector
from mysql.connector import errorcode

from sqlalchemy.sql import select
import requests
from requests.auth import HTTPBasicAuth

# @added 20161213 - Branch #1790: test_tsfresh
# To match the new order introduced via the test_tsfresh method
import numpy as np
import pandas as pd

import settings
from skyline_functions import (
    load_metric_vars, fail_check, mysql_select, write_data_to_file,
    send_graphite_metric, mkdir_p)
# @added 20161221 - calculate features for every anomaly, instead of making the
# user do it in the frontend or calling the webapp constantly in a cron like
# manner.  Decouple Ionosphere from the webapp.
from features_profile import calculate_features_profile

from database import get_engine, ionosphere_table_meta
from tsfresh_feature_names import TSFRESH_FEATURES

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
        self.anomalous_metrics = Manager().list()
        self.metric_variables = Manager().list()
        self.mysql_conn = mysql.connector.connect(**config)
        self.sent_to_panorama = Manager().list()

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
                engine, fail_msg, trace = get_engine(skyline_app)
                return engine, fail_msg, trace
            except:
                trace = traceback.format_exc()
                logger.error('%s' % trace)
                fail_msg = 'error :: failed to get MySQL engine for'
                logger.error('%s' % fail_msg)
                return None, fail_msg, trace

        try:
            ionosphere_unique_metrics = list(self.redis_conn.smembers('ionosphere.unique_metrics'))
            ionosphere_unique_metrics_count = len(ionosphere_unique_metrics)
            logger.info('Redis ionosphere.unique_metrics set has %s metrics' % (str(ionosphere_unique_metrics_count)))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to get ionosphere.unique_metrics from Redis')
            ionosphere_unique_metrics = []

        manage_ionosphere_unique_metrics = True
        try:
            manage_ionosphere_unique_metrics = self.redis_conn.get('ionosphere.manage_ionosphere_unique_metrics')
            manage_ionosphere_unique_metrics = False
        except Exception as e:
            logger.error('error :: could not query Redis for ionosphere.manage_ionosphere_unique_metrics key: %s' % str(e))

        if manage_ionosphere_unique_metrics:
            try:
                logger.info('deleting Redis ionosphere.unique_metrics set to refresh')
                self.redis_conn.delete('ionosphere.unique_metrics')
                manage_ionosphere_unique_metrics = False
                ionosphere_unique_metrics = []
            except Exception as e:
                logger.error('error :: could not delete Redis set ionosphere.unique_metrics: %s' % str(e))

        logger.info('getting MySQL engine')
        try:
            engine, fail_msg, trace = get_an_engine()
            logger.info(fail_msg)
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: could not get a MySQL engine'
            logger.error('%s' % fail_msg)
            return False

        if not engine:
            trace = 'none'
            fail_msg = 'error :: engine not obtained'
            logger.error(fail_msg)
            return False

        # Determine the metrics that have ionosphere_enabled
        ionosphere_enabled_metrics = []
        try:
            stmt = 'select metric from metrics where ionosphere_enabled=1'
            connection = engine.connect()
            for row in engine.execute(stmt):
                metric_basename = row['metric']
                metric_name = '%s%s' % (str(settings.FULL_NAMESPACE), str(metric_basename))
                ionosphere_enabled_metrics.append(metric_name)
            connection.close()
            ionosphere_metrics_count = len(ionosphere_enabled_metrics)
            logger.info('db has %s ionosphere_enabled metrics' % (str(ionosphere_metrics_count)))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: could not determine ionosphere_enabled metrics from the DB to manage ionosphere.unique_metrics Redis set')

        for metric_name in ionosphere_enabled_metrics:
            if metric_name in ionosphere_unique_metrics:
                continue
            try:
                self.redis_conn.sadd('ionosphere.unique_metrics', metric_name)
                logger.info('added %s to ionosphere.unique_metrics Redis set' % metric_name)
            except:
                logger.error(traceback.format_exc())
                logger.info('error :: failed to add %s to ionosphere.unique_metrics Redis set' % metric_name)
            try:
                self.redis_conn.setex('ionosphere.manage_ionosphere_unique_metrics', 300, time())
            except:
                logger.error('error :: failed to set key :: ionosphere.manage_ionosphere_unique_metrics')

        return True

    def spin_process(self, i, metric_check_file):
        """
        Assign a metric anomaly to process.

        :param i: python process id
        :param metric_check_file: full path to the metric check file

        :return: returns True

        """

        def get_an_engine():
            try:
                engine, fail_msg, trace = get_engine(skyline_app)
                return engine, fail_msg, trace
            except:
                trace = traceback.format_exc()
                logger.error('%s' % trace)
                fail_msg = 'error :: failed to get MySQL engine for'
                logger.error('%s' % fail_msg)
                return None, fail_msg, trace

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

        # Load and validate metric variables
        try:
            metric_vars = load_metric_vars(skyline_app, str(metric_check_file))
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
        try:
            metric_vars.metric
            metric = str(metric_vars.metric)
            base_name = metric
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - metric - %s' % metric)
        except:
            logger.error('error :: failed to read metric variable from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        try:
            metric_vars.value
            value = str(metric_vars.value)
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - value - %s' % (value))
        except:
            logger.error('error :: failed to read value variable from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        try:
            metric_vars.from_timestamp
            from_timestamp = str(metric_vars.from_timestamp)
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - from_timestamp - %s' % from_timestamp)
        except:
            # @added 20160822 - Bug #1460: panorama check file fails
            # Added exception handling here
            logger.info(traceback.format_exc())
            logger.error('error :: failed to read from_timestamp variable from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        try:
            metric_vars.metric_timestamp
            metric_timestamp = str(metric_vars.metric_timestamp)
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - metric_timestamp - %s' % metric_timestamp)
        except:
            logger.error('error :: failed to read metric_timestamp variable from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        try:
            metric_vars.algorithms
            algorithms = metric_vars.algorithms
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - algorithms - %s' % str(algorithms))
        except:
            logger.error('error :: failed to read algorithms variable from check file setting to all - %s' % (metric_check_file))
            algorithms = 'all'

        try:
            metric_vars.triggered_algorithms
            triggered_algorithms = metric_vars.triggered_algorithms
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - triggered_algorithms - %s' % str(triggered_algorithms))
        except:
            logger.error('error :: failed to read triggered_algorithms variable from check file setting to all - %s' % (metric_check_file))
            triggered_algorithms = 'all'

        try:
            metric_vars.added_by
            added_by = str(metric_vars.added_by)
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - added_by - %s' % added_by)
        except:
            logger.error('error :: failed to read added_by variable from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        try:
            metric_vars.added_at
            added_at = str(metric_vars.added_at)
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - added_at - %s' % added_at)
        except:
            logger.error('error :: failed to read added_at variable from check file setting to all - %s' % (metric_check_file))
            added_by = 'all'

        # @added 20161228 - Feature #1828: ionosphere - mirage Redis data features
        # Added full_duration which needs to be recorded to allow Mirage metrics
        # to be profiled on Redis timeseries data at FULL_DURATION
        try:
            metric_vars.full_duration
            full_duration = str(metric_vars.full_duration)
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - full_duration - %s' % full_duration)
        except:
            logger.error('error :: failed to read full_duration variable from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
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
        query = "SELECT ionosphere_enabled FROM metrics WHERE metric='%s'" % metric
        result = mysql_select(skyline_app, query)
        if str(result[0]) != '1':
            logger.info('Ionosphere not enabled on %s' % (metric))
            # @modified 20161222 - do not remove metric file until features
            # calculated
            # self.remove_metric_check_file(str(metric_check_file))
            # return
            training_metric = True
        else:
            logger.info('Ionosphere is enabled on %s' % (metric))

        # @added 20161210 - Branch #922: ionosphere
        #                   Task #1658: Patterning Skyline Ionosphere
        # Only continue if there is a training data json timeseries file
        metric_timeseries_dir = base_name.replace('.', '/')
        metric_training_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, metric_timestamp,
            metric_timeseries_dir)
        anomaly_json = '%s/%s.json' % (metric_training_data_dir, base_name)
        if os.path.isfile(anomaly_json):
            logger.info('training data ts json available - %s' % (anomaly_json))
        else:
            logger.error('error :: training data ts json was not found - %s' % (anomaly_json))
            self.remove_metric_check_file(str(metric_check_file))
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
                    logger.error('error :: training data Redis full duration json was not found - %s' % (redis_anomaly_json))
                    self.remove_metric_check_file(str(metric_check_file))
                    return
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
            logger.info('getting MySQL engine')
            try:
                engine, fail_msg, trace = get_an_engine()
                logger.info(fail_msg)
            except:
                trace = traceback.format_exc()
                logger.error(trace)
                fail_msg = 'error :: could not get a MySQL engine'
                logger.error('%s' % fail_msg)
                self.remove_metric_check_file(str(metric_check_file))
                return False

            if not engine:
                trace = 'none'
                fail_msg = 'error :: engine not obtained'
                logger.error(fail_msg)
                return False

            # Get the ionosphere_table metadata
            ionosphere_table = None
            try:
                ionosphere_table, fail_msg, trace = ionosphere_table_meta(skyline_app, engine)
                logger.info(fail_msg)
            except:
                trace = traceback.format_exc()
                logger.error('%s' % trace)
                fail_msg = 'error :: failed to get ionosphere_table meta for %s' % base_name
                logger.error('%s' % fail_msg)
                return False

            logger.info('ionosphere_table OK')

            # Determine the fp_ids that exist for the metric
            fp_ids = []
            fp_ids_found = False
            try:
                connection = engine.connect()
                stmt = select([ionosphere_table.c.fp_id]).where(ionosphere_table.c.metric == base_name)
                result = connection.execute(stmt)
                for row in result:
                    fp_id = row['id']
                    fp_ids.append(int(fp_id))
                connection.close()
                fp_count = len(fp_ids)
                logger.info('determined %s fp ids for %s' % (str(fp_count), base_name))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: could not determine fp ids from DB for %s' % base_name)

            if fp_ids == []:
                logger.error('error :: there are no fp ids for %s' % base_name)
            else:
                fp_ids_found = True

        # @added 20161221 - TODO: why not calculate the features of every
        # anomaly so the the use does not have to do it and wait for the
        # features to be calculated.
        # Check the features were calculated by the webapp
        calculated_feature_file = '%s/%s.tsfresh.input.csv.features.transposed.csv' % (metric_training_data_dir, base_name)
        calculated_feature_file_found = False
        if os.path.isfile(calculated_feature_file):
            logger.info('calculated features available - %s' % (calculated_feature_file))
            calculated_feature_file_found = True

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
                    return
                else:
                    logger.info('anomaly Graphite alert resources found in %s' % (metric_training_data_dir))

        context = skyline_app
        f_calc = None
        if not calculated_feature_file_found:
            try:
                fp_csv, successful, fp_exists, fp_id, fail_msg, traceback_format_exc, f_calc = calculate_features_profile(skyline_app, metric_timestamp, base_name, context)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to calculate features')
                self.remove_metric_check_file(str(metric_check_file))
                return

        if os.path.isfile(calculated_feature_file):
            logger.info('calculated features available - %s' % (calculated_feature_file))
            calculated_feature_file_found = True
            if isinstance(f_calc, float):
                f_calc_time = '%.2f' % f_calc
                send_metric_name = '%s.features_calculation_time' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, f_calc_time)
            if training_metric:
                logger.info('training metric done')
                self.remove_metric_check_file(str(metric_check_file))
                # TODO: make ionosphere more useful, compare any other
                # available training_metric profiles here and match, not in the
                # db context, in the training context.
                return

        # @added 20161210 - Branch #922: ionosphere
        #                   Task #1658: Patterning Skyline Ionosphere
        # Calculate features for the current timeseries if there are fp ids
        # Just call it via the webapp... fewer lines of code and already done in
        # webapp/ionosphere_backend.py and webapp/features_proifle.py
        if not calculated_feature_file_found:
            webapp_url = '%s/ionosphere?timestamp=%s&metric=%s&calc_features=true' % (
                settings.SKYLINE_URL, metric_timestamp, base_name)
            r = None
            http_status_code = 0
            if settings.WEBAPP_AUTH_ENABLED:
                # 10 second timout is sufficient locally under normal circumstances
                # as tsfresh has yet to have been take longer than 6 seconds if so
                # by the time the next request is made, the features file should
                # exist.  So this is limited psuedo-idempotency.
                timeout_and_auth = 'timeout=10, auth=(%s, %s))' % (settings.WEBAPP_AUTH_USER, settings.WEBAPP_AUTH_USER_PASSWORD)
            else:
                timeout_and_auth = 'timeout=10'
            if fp_ids_found:
                for _ in range(2):
                    try:
                        r = requests.get(webapp_url, timeout_and_auth)
                        http_status_code = r.status_code
                    except:
                        logger.error('error :: could not retrieve %s' % webapp_url)
                        sleep(5)
                        continue
                    else:
                        break
                else:
                    logger.error(traceback.format_exc())
                    logger.error('error :: could not retrieve %s after 3 tries' % webapp_url)

            if int(http_status_code) == 200:
                if os.path.isfile(calculated_feature_file):
                    logger.info('calculated features available - %s' % (calculated_feature_file))
                    calculated_feature_file_found = True
                else:
                    logger.error('error :: calculated features not available - %s' % (calculated_feature_file))
                    # send an Ionosphere alert or add a thunder branch alert, one
                    # one thing at a time.  You cannot rush timeseries.
                    self.remove_metric_check_file(str(metric_check_file))
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
                        value = float(line[1])
                        calculated_features.append([feature_name, value])

        if calculated_features == []:
            logger.error('error :: no calculated features were determined from - %s' % (calculated_feature_file))
            self.remove_metric_check_file(str(metric_check_file))
            return

        # Compare calculated features to feature values for each fp id
        not_anomalous = False
        if calculated_feature_file_found:

            metrics_id = None
            stmt = 'select id from metrics where metric=\'%s\'' % base_name
            try:
                connection = engine.connect()
                for row in engine.execute(stmt):
                    metrics_id = row['id']
                connection.close()
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: could not determine id from metrics table for - %s' % str(base_name))

            for fp_id in fp_ids:
                if not metrics_id:
                    continue

                features_count = None
                fp_features = []
                # Get features for fp_id from z_fp_<metric_id> table where the
                # features profile is the same full_duration
                try:
                    metric_fp_table = 'z_fp_%s' % str(metrics_id)
                    stmt = 'SELECT feature_id, value FROM %s WHERE fp_id=%s AND full_duration=%s' % (metric_fp_table, str(fp_id), str(full_duration))
                    connection = engine.connect()
                    for row in engine.execute(stmt):
                        fp_feature_id = int(row['feature_id'])
                        fp_value = float(row['value'])
                        fp_features.append([fp_feature_id, fp_value])
                    connection.close()
                    features_count = len(fp_features)
                    logger.info('determined %s features for fp_id %s at full_duration of %s' % (str(features_count), str(fp_id), str(full_duration)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: could not determine features from DB for %s' % str(fp_id))

                # Convert feature names in calculated_features to their id
                logger.info('converting tsfresh feature names to Skyline feature ids')
                calc_features_by_id = []
                for feature_name, value in calculated_features:
                    for skyline_feature_id, name in TSFRESH_FEATURES:
                        if feature_name == name:
                            calc_features_by_id.append([skyline_feature_id, float(value)])

                # Determine what features each data has, extract only values for
                # common features.
                logger.info('determining common features')
                relevant_fp_feature_values = []
                relevant_calc_feature_values = []
                for skyline_feature_id, value in calc_features_by_id:
                    for fp_feature_id, fp_value in fp_features:
                        if skyline_feature_id == fp_feature_id:
                            relevant_fp_feature_values.append(fp_value)
                            relevant_calc_feature_values.append(value)

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

                # Determine whether each set is positive or negative
                # # if the same carry on
                # # if both negative, make then both positive
                # Sum fp values, Sum calculated - handle negatives like features_sum :: -3389570699080000.0000000000
                # Determine whether each set is positive or negative
                # # if the same carry on
                # # if both negative, make then both positive postive_sums
                fp_sum = [sum_fp_values]
                calc_sum = [sum_calc_values]
                almost_equal = None
                try:
                    np.testing.assert_array_almost_equal(fp_sum, calc_sum)
                    almost_equal = True
                except:
                    almost_equal = False

                if almost_equal:
                    not_anomalous = True
                    logger.info('common features sums are almost equal, not anomalous' % str(relevant_fp_feature_values_count))

                sums_array = np.array([fp_sum, calc_sum], dtype=float)
                try:
                    calc_percent_different = np.diff(sums_array) / sums_array[:-1] * 100.
                    percent_different = calc_percent_different[0]
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to calculate percent_different')
                    continue

                # @added 20161229 - Feature #1830: Ionosphere alerts
                # Update the features profile checked count and time
                logger.info('updating checked details in db for %s' % (str(fp_id)))
                # update matched_count in ionosphere_table
                checked_timestamp = int(time())
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
                    # update matched_count in ionosphere_table
                    matched_timestamp = int(time())
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

                # https://docs.scipy.org/doc/numpy/reference/generated/numpy.testing.assert_almost_equal.html
                # @added 20161214 - Add a between timeframe option, e.g. if
                # fp match, only see this as not anomalous if hour (and or min)
                # is between x and y - handle rollovers, cron log archives, etc.
                logger.info('debug :: %s is a features profile for %s' % (str(fp_id), base_name))

            if not not_anomalous:
                logger.info('anomalous - no feature profiles were matched - %s' % base_name)
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
                        % (base_name, str(value), from_timestamp,
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
                        self.sent_to_panorama.append(base_name)

                    except:
                        logger.error('error :: failed to add panorama anomaly file :: %s' % (panaroma_anomaly_file))
                        logger.info(traceback.format_exc())
                else:
                    logger.info('not adding panorama anomaly file for Mirage metric - %s' % (metric))

            #     alert ... hmmm the harder part, maybe not all the resources
            #     are already created, so just determining ALERTS and firing a
            #     trigger_alert (pull in alerter.py and mirage_alerters.py?)
            #     OR send back to app via Redis
                cache_key = 'ionosphere.%s.alert.%s.%s' % (added_by, metric_timestamp, base_name)
                try:
                    self.redis_conn.setex(
                        cache_key, 300,
                        [float(value), base_name, int(metric_timestamp), triggered_algorithms, full_duration])
                    logger.info(
                        'add Redis alert key - %s - [%s, \'%s\', %s, %s]' %
                        (cache_key, str(value), base_name, str(int(metric_timestamp)),
                            str(triggered_algorithms)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error(
                        'error :: failed to add Redis key - %s - [%s, \'%s\', %s, %s]' %
                        (cache_key, str(value), base_name, str(int(metric_timestamp)),
                            str(triggered_algorithms)))

        # TO BE REMOVED
        self.remove_metric_check_file(str(metric_check_file))
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
                    # Report app up
                    try:
                        self.redis_conn.setex(skyline_app, 120, now)
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
                                    logger.error('error :: killing all spin_process processes')

                # Discover metric anomalies to insert
                metric_var_files = False
                try:
                    metric_var_files = [f for f in listdir(settings.IONOSPHERE_CHECK_PATH) if isfile(join(settings.IONOSPHERE_CHECK_PATH, f))]
                except:
                    logger.error('error :: failed to list files in check dir')
                    logger.info(traceback.format_exc())

                if metric_var_files:
                    break

            metric_var_files_sorted = sorted(metric_var_files)
            metric_check_file = '%s/%s' % (settings.IONOSPHERE_CHECK_PATH, str(metric_var_files_sorted[0]))

            logger.info('processing - %s' % str(metric_var_files_sorted[0]))

            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            now = time()
            for i in range(1, settings.IONOSPHERE_PROCESSES + 1):
                try:
                    p = Process(target=self.spin_process, args=(i, metric_check_file))
                    pids.append(p)
                    pid_count += 1
                    logger.info('starting %s of %s spin_process/es' % (str(pid_count), str(settings.IONOSPHERE_PROCESSES)))
                    p.start()
                    spawned_pids.append(p.pid)
                except:
                    logger.error('error :: to start spin_process')
                    logger.info(traceback.format_exc())
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
                        '%s :: %s spin_process/es completed in %.2f seconds' % (
                            skyline_app, str(settings.IONOSPHERE_PROCESSES),
                            time_to_run))
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('%s :: timed out, killing all spin_process processes' % (skyline_app))
                for p in pids:
                    try:
                        p.terminate()
                        # p.join()
                        logger.info('%s :: killed spin_process process' % (skyline_app))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: killing all spin_process processes')

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
                    logger.info('%s :: stopping spin_process - %s' % (skyline_app, str(p.is_alive())))
                    p.join()
