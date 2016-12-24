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
from skyline_functions import load_metric_vars, fail_check, mysql_select, write_data_to_file, mkdir_p
# @added 20161221 - calculate features for every anomaly, instead of making the
# user do it in the frontend or calling the webapp constantly in a cron like
# manner.  Decouple Ionosphere from the webapp.
from features_profile import feature_name_id, calculate_features_profile

from database import get_engine, ionosphere_table_meta, metrics_table_meta

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

    def spin_process(self, i, metric_check_file):
        """
        Assign a metric anomaly to process.

        :param i: python process id
        :param metric_check_file: full path to the metric check file

        :return: returns True

        """

        child_process_pid = os.getpid()
        if settings.ENABLE_IONOSPHERE_DEBUG:
            logger.info('debug :: child_process_pid - %s' % str(child_process_pid))

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
            logger.error('error :: failed to read algorithms variable from check file setting to all' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        try:
            metric_vars.triggered_algorithms
            triggered_algorithms = metric_vars.triggered_algorithms
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - triggered_algorithms - %s' % str(triggered_algorithms))
        except:
            logger.error('error :: failed to read triggered_algorithms variable from check file setting to all' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        try:
            metric_vars.added_by
            app = str(metric_vars.added_by)
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - added_by - %s' % app)
        except:
            logger.error('error :: failed to read added_by variable from check file setting to all' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        try:
            metric_vars.added_at
            added_at = str(metric_vars.added_at)
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - added_at - %s' % added_at)
        except:
            logger.error('error :: failed to read added_at variable from check file setting to all' % (metric_check_file))
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
                return False
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
                sleep(5)
                graphite_file_count = len([f for f in os.listdir(metric_training_data_dir)
                                           if f.endswith('.png') and
                                           os.path.isfile(os.path.join(metric_training_data_dir, f))])
                if graphite_file_count == 0:
                    logger.info('not calculating features no Graphite alert resources created in %s' % (metric_training_data_dir))
                    self.remove_metric_check_file(str(metric_check_file))
                    return

        context = skyline_app
        if not calculated_feature_file_found:
            try:
                fp_csv, successful, fp_exists, fp_id, fail_msg, traceback_format_exc, f_calc = calculate_features_profile(skyline_app, metric_timestamp, base_name, context)
            except:
                logger.error(traceback.format_exc())
                logger.error('failed to calculate features')
                self.remove_metric_check_file(str(metric_check_file))
                return

        if os.path.isfile(calculated_feature_file):
            logger.info('calculated features available - %s' % (calculated_feature_file))
            calculated_feature_file_found = True
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
                settings.SKYLINE_URL, timestamp, base_name)
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
        fp_ids_found = False
        not_anomalous = False
        if calculated_feature_file_found:
            for fp_ip in fp_ids:
                # Get features for fp_id from z_fp_<metric_id> table.
                # Determine what features each data has, extract only values for
                # common features.
                # Determine the sum of each set
                # Determine whether each set is positive or negative
                # # if the same carry on
                # # if both negative, make then both positive
                # Sum fp values, Sum calculated - handle negatives like features_sum :: -3389570699080000.0000000000
                # Determine whether each set is positive or negative
                # # if the same carry on
                # # if both negative, make then both positive postive_sums
                # if diff_in_sums <= 1%:
                #     log
                #     update matched_count in ionosphere_table
                #     not_anomalous = True
                # https://docs.scipy.org/doc/numpy/reference/generated/numpy.testing.assert_almost_equal.html
                # @added 20161214 - Add a between timeframe option, e.g. if
                # fp match, only see this as not anomalous if hour (and or min)
                # is between x and y - handle rollovers, cron log archives, etc.
                logger.debug('debug :: %s is feature profile for %s' % (str(fp_id), base_name))
                fp_ids_found = True

            # if not not_anomalous:
            #     log anomaly
            #     send to panorama
            #     send anomalous count metric to graphite
            #     alert

        # TO BE REMOVED
        self.remove_metric_check_file(str(metric_check_file))
        return fp_ids_found

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
