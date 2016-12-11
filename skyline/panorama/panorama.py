import logging
try:
    from Queue import Empty
except:
    from queue import Empty
from redis import StrictRedis
from time import time, sleep
from threading import Thread
from multiprocessing import Process, Manager
from msgpack import Unpacker, packb
import os
from os import kill, getpid, listdir
from os.path import join, isfile
import traceback
from sys import version_info
import mysql.connector
from mysql.connector import errorcode

import settings
from skyline_functions import load_metric_vars, fail_check, mkdir_p

skyline_app = 'panorama'
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
    ENABLE_PANORAMA_DEBUG = settings.ENABLE_PANORAMA_DEBUG
except:
    logger.error('error :: cannot determine ENABLE_PANORAMA_DEBUG from settings' % skyline_app)
    ENABLE_PANORAMA_DEBUG = False

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)

failed_checks_dir = '%s_failed' % settings.PANORAMA_CHECK_PATH

# @added 20160907 - Handle Panorama stampede on restart after not running #26
# Allow to expire check if greater than PANORAMA_CHECK_MAX_AGE, backwards
# compatible
try:
    test_max_age_set = 1 + settings.PANORAMA_CHECK_MAX_AGE
    if test_max_age_set > 1:
        max_age = True
    if test_max_age_set == 1:
        max_age = False
    max_age_seconds = settings.PANORAMA_CHECK_MAX_AGE
except:
    max_age = False
    max_age_seconds = 0
expired_checks_dir = '%s_expired' % settings.PANORAMA_CHECK_PATH

# Database configuration
config = {'user': settings.PANORAMA_DBUSER,
          'password': settings.PANORAMA_DBUSERPASS,
          'host': settings.PANORAMA_DBHOST,
          'port': settings.PANORAMA_DBPORT,
          'database': settings.PANORAMA_DATABASE,
          'raise_on_warnings': True}


class Panorama(Thread):
    """
    The Panorama class which controls the panorama thread and spawned processes.
    """

    def __init__(self, parent_pid):
        """
        Initialize Panorama

        Create the :obj:`self.anomalous_metrics` list

        """
        super(Panorama, self).__init__()
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
    These are the panorama mysql functions used to surface and input panorama data
    for timeseries.
    """

    def mysql_select(self, select):
        """
        Select data from mysql database

        :param select: the select string
        :type select: str
        :return: tuple
        :rtype: tuple, boolean

        - **Example usage**::

            query = 'select id, test from test'
            result = self.mysql_select(query)

        - **Example of the 0 indexed results tuple, which can hold multiple results**::

            >> print('results: %s' % str(results))
            results: [(1, u'test1'), (2, u'test2')]

            >> print('results[0]: %s' % str(results[0]))
            results[0]: (1, u'test1')

        .. note::
            - If the MySQL query fails a boolean will be returned not a tuple
                * ``False``
                * ``None``

        """

        try:
            cnx = mysql.connector.connect(**config)
            if ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: connected to mysql')
        except mysql.connector.Error as err:
            logger.error('error :: mysql error - %s' % str(err))
            logger.error('error :: failed to connect to mysql')
            return False

        if cnx:
            try:
                if ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: %s' % (str(select)))
                cursor = cnx.cursor()
                query = ('%s' % (str(select)))
                cursor.execute(query)
                result = cursor.fetchall()
                cursor.close()
                cnx.close()
                return result
            except mysql.connector.Error as err:
                logger.error('error :: mysql error - %s' % str(err))
                logger.error('error :: failed to query database - %s' % (str(select)))
                try:
                    cnx.close()
                    return False
                except:
                    return False
        else:
            if ENABLE_PANORAMA_DEBUG:
                logger.error('error :: failed to connect to mysql')

        # Close the test mysql connection
        try:
            cnx.close()
            return False
        except:
            return False

        return False

    def mysql_insert(self, insert):
        """
        Insert data into mysql table

        :param select: the insert string
        :type select: str
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
            if ENABLE_PANORAMA_DEBUG:
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

    def spin_process(self, i, metric_check_file):
        """
        Assign a metric anomaly to process.

        :param i: python process id
        :param metric_check_file: full path to the metric check file

        :return: returns True

        """

        child_process_pid = os.getpid()
        if settings.ENABLE_PANORAMA_DEBUG:
            logger.info('debug :: child_process_pid - %s' % str(child_process_pid))

        if settings.ENABLE_PANORAMA_DEBUG:
            logger.info('debug :: processing metric check - %s' % metric_check_file)

        if not os.path.isfile(str(metric_check_file)):
            logger.error('error :: file not found - metric_check_file - %s' % (str(metric_check_file)))
            return

        check_file_name = os.path.basename(str(metric_check_file))
        if settings.ENABLE_PANORAMA_DEBUG:
            logger.info('debug :: check_file_name - %s' % check_file_name)
        check_file_timestamp = check_file_name.split('.', 1)[0]
        if settings.ENABLE_PANORAMA_DEBUG:
            logger.info('debug :: check_file_timestamp - %s' % str(check_file_timestamp))
        check_file_metricname_txt = check_file_name.split('.', 1)[1]
        if settings.ENABLE_PANORAMA_DEBUG:
            logger.info('debug :: check_file_metricname_txt - %s' % check_file_metricname_txt)
        check_file_metricname = check_file_metricname_txt.replace('.txt', '')
        if settings.ENABLE_PANORAMA_DEBUG:
            logger.info('debug :: check_file_metricname - %s' % check_file_metricname)
        check_file_metricname_dir = check_file_metricname.replace('.', '/')
        if settings.ENABLE_PANORAMA_DEBUG:
            logger.info('debug :: check_file_metricname_dir - %s' % check_file_metricname_dir)

        metric_failed_check_dir = '%s/%s/%s' % (failed_checks_dir, check_file_metricname_dir, check_file_timestamp)

        failed_check_file = '%s/%s' % (metric_failed_check_dir, check_file_name)
        if settings.ENABLE_PANORAMA_DEBUG:
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
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: metric variable - metric - %s' % metric)
        except:
            logger.error('error :: failed to read metric variable from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        try:
            metric_vars.value
            value = str(metric_vars.value)
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: metric variable - value - %s' % (value))
        except:
            logger.error('error :: failed to read value variable from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        try:
            metric_vars.from_timestamp
            from_timestamp = str(metric_vars.from_timestamp)
            if settings.ENABLE_PANORAMA_DEBUG:
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
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: metric variable - metric_timestamp - %s' % metric_timestamp)
        except:
            logger.error('error :: failed to read metric_timestamp variable from check file - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        try:
            metric_vars.algorithms
            algorithms = metric_vars.algorithms
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: metric variable - algorithms - %s' % str(algorithms))
        except:
            logger.error('error :: failed to read algorithms variable from check file setting to all - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        try:
            metric_vars.triggered_algorithms
            triggered_algorithms = metric_vars.triggered_algorithms
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: metric variable - triggered_algorithms - %s' % str(triggered_algorithms))
        except:
            logger.error('error :: failed to read triggered_algorithms variable from check file setting to all - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        try:
            metric_vars.app
            app = str(metric_vars.app)
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: metric variable - app - %s' % app)
        except:
            logger.error('error :: failed to read app variable from check file setting to all  - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        try:
            metric_vars.source
            source = str(metric_vars.source)
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: metric variable - source - %s' % source)
        except:
            logger.error('error :: failed to read source variable from check file setting to all  - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        try:
            metric_vars.added_by
            added_by = str(metric_vars.added_by)
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: metric variable - added_by - %s' % added_by)
        except:
            logger.error('error :: failed to read added_by variable from check file setting to all - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        try:
            metric_vars.added_at
            added_at = str(metric_vars.added_at)
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: metric variable - added_at - %s' % added_at)
        except:
            logger.error('error :: failed to read added_at variable from check file setting to all - %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return

        record_anomaly = True
        cache_key = '%s.last_check.%s.%s' % (skyline_app, app, metric)
        if settings.ENABLE_PANORAMA_DEBUG:
            logger.info('debug :: cache_key - %s.last_check.%s.%s' % (
                skyline_app, app, metric))
        try:
            last_check = self.redis_conn.get(cache_key)
        except Exception as e:
            logger.error(
                'error :: could not query cache_key - %s.last_check.%s.%s - %s' % (
                    skyline_app, app, metric, e))
            last_check = None

        if last_check:
            record_anomaly = False
            logger.info(
                'Panorama metric key not expired - %s.last_check.%s.%s' % (
                    skyline_app, app, metric))

        # @added 20160907 - Handle Panorama stampede on restart after not running #26
        # Allow to expire check if greater than PANORAMA_CHECK_MAX_AGE
        if max_age:
            now = time()
            anomaly_age = int(now) - int(metric_timestamp)
            if anomaly_age > max_age_seconds:
                record_anomaly = False
                logger.info(
                    'Panorama check max age exceeded - %s - %s seconds old, older than %s seconds discarding' % (
                        metric, str(anomaly_age), str(max_age_seconds)))

        if not record_anomaly:
            logger.info('not recording anomaly for - %s' % (metric))
            if os.path.isfile(str(metric_check_file)):
                try:
                    os.remove(str(metric_check_file))
                    logger.info('metric_check_file removed - %s' % str(metric_check_file))
                except OSError:
                    pass

            return

        # Determine id of something thing
        def determine_id(table, key, value):
            """
            Get the id of something from Redis or the database and insert a new
            record if one does not exist for the value.

            :param table: table name
            :param key: key name
            :param value: value name
            :type table: str
            :type key: str
            :type value: str
            :return: int or boolean

            """

            query_cache_key = '%s.mysql_ids.%s.%s.%s' % (skyline_app, table, key, value)
            determined_id = None
            redis_determined_id = None
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: query_cache_key - %s' % (query_cache_key))

            try:
                redis_known_id = self.redis_conn.get(query_cache_key)
            except:
                redis_known_id = None

            if redis_known_id:
                unpacker = Unpacker(use_list=False)
                unpacker.feed(redis_known_id)
                redis_determined_id = list(unpacker)

            if redis_determined_id:
                determined_id = int(redis_determined_id[0])

            if determined_id:
                if determined_id > 0:
                    return determined_id

            # Query MySQL
            query = 'select id FROM %s WHERE %s=\'%s\'' % (table, key, value)
            results = self.mysql_select(query)

            determined_id = 0
            if results:
                determined_id = int(results[0][0])

            if determined_id > 0:
                # Set the key for a week
                if not redis_determined_id:
                    try:
                        self.redis_conn.setex(query_cache_key, 604800, packb(determined_id))
                        logger.info('set redis query_cache_key - %s - id: %s' % (
                            query_cache_key, str(determined_id)))
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to set query_cache_key - %s - id: %s' % (
                            query_cache_key, str(determined_id)))
                return int(determined_id)

            # INSERT because no known id
            insert_query = 'insert into %s (%s) VALUES (\'%s\')' % (table, key, value)
            logger.info('inserting %s into %s table' % (value, table))
            try:
                results = self.mysql_insert(insert_query)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to determine the id of %s from the insert' % (value))
                raise

            determined_id = 0
            if results:
                determined_id = int(results)
            else:
                logger.error('error :: results not set')
                raise

            if determined_id > 0:
                # Set the key for a week
                if not redis_determined_id:
                    try:
                        self.redis_conn.setex(query_cache_key, 604800, packb(determined_id))
                        logger.info('set redis query_cache_key - %s - id: %s' % (
                            query_cache_key, str(determined_id)))
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('%s' % str(e))
                        logger.error('error :: failed to set query_cache_key - %s - id: %s' % (
                            query_cache_key, str(determined_id)))
                return determined_id

            logger.error('error :: failed to determine the inserted id for %s' % value)
            return False

        try:
            added_by_host_id = determine_id('hosts', 'host', added_by)
        except:
            logger.error('error :: failed to determine id of %s' % (added_by))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return False

        try:
            app_id = determine_id('apps', 'app', app)
        except:
            logger.error('error :: failed to determine id of %s' % (app))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return False

        try:
            source_id = determine_id('sources', 'source', source)
        except:
            logger.error('error :: failed to determine id of %s' % (source))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return False

        try:
            metric_id = determine_id('metrics', 'metric', metric)
        except:
            logger.error('error :: failed to determine id of %s' % (metric))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return False

        algorithms_ids_csv = ''
        for algorithm in algorithms:
            try:
                algorithm_id = determine_id('algorithms', 'algorithm', algorithm)
            except:
                logger.error('error :: failed to determine id of %s' % (algorithm))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                return False
            if algorithms_ids_csv == '':
                algorithms_ids_csv = str(algorithm_id)
            else:
                new_algorithms_ids_csv = '%s,%s' % (algorithms_ids_csv, str(algorithm_id))
                algorithms_ids_csv = new_algorithms_ids_csv

        triggered_algorithms_ids_csv = ''
        for triggered_algorithm in triggered_algorithms:
            try:
                triggered_algorithm_id = determine_id('algorithms', 'algorithm', triggered_algorithm)
            except:
                logger.error('error :: failed to determine id of %s' % (triggered_algorithm))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                return False
            if triggered_algorithms_ids_csv == '':
                triggered_algorithms_ids_csv = str(triggered_algorithm_id)
            else:
                new_triggered_algorithms_ids_csv = '%s,%s' % (
                    triggered_algorithms_ids_csv, str(triggered_algorithm_id))
                triggered_algorithms_ids_csv = new_triggered_algorithms_ids_csv

        logger.info('inserting anomaly')
        try:
            full_duration = int(metric_timestamp) - int(from_timestamp)
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: full_duration - %s' % str(full_duration))
        except:
            logger.error('error :: failed to determine full_duration')
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return False

        try:
            anomalous_datapoint = round(float(value), 6)
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: anomalous_datapoint - %s' % str(anomalous_datapoint))
        except:
            logger.error('error :: failed to determine anomalous_datapoint')
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return False

        try:
            columns = '%s, %s, %s, %s, %s, %s, %s, %s, %s' % (
                'metric_id', 'host_id', 'app_id', 'source_id',
                'anomaly_timestamp', 'anomalous_datapoint', 'full_duration',
                'algorithms_run', 'triggered_algorithms')
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: columns - %s' % str(columns))
        except:
            logger.error('error :: failed to construct columns string')
            logger.info(traceback.format_exc())
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return False

        try:
            query = 'insert into anomalies (%s) VALUES (%d, %d, %d, %d, %s, %.6f, %d, \'%s\', \'%s\')' % (
                columns, metric_id, added_by_host_id, app_id, source_id,
                metric_timestamp, anomalous_datapoint, full_duration,
                algorithms_ids_csv, triggered_algorithms_ids_csv)
        except:
            logger.error('error :: failed to construct insert query')
            logger.info(traceback.format_exc())
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return False

        if settings.ENABLE_PANORAMA_DEBUG:
            logger.info('debug :: anomaly insert - %s' % str(query))

        try:
            anomaly_id = self.mysql_insert(query)
            logger.info('anomaly id - %d - created for %s at %s' % (
                anomaly_id, metric, metric_timestamp))
        except:
            logger.error('error :: failed to insert anomaly %s at %s' % (
                anomaly_id, metric, metric_timestamp))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return False

        # Set anomaly record cache key
        try:
            self.redis_conn.setex(
                cache_key, settings.PANORAMA_EXPIRY_TIME, packb(value))
            logger.info('set cache_key - %s.last_check.%s.%s - %s' % (
                skyline_app, app, metric, str(settings.PANORAMA_EXPIRY_TIME)))
        except Exception as e:
            logger.error(
                'error :: could not query cache_key - %s.last_check.%s.%s - %s' % (
                    skyline_app, app, metric, e))

        if os.path.isfile(str(metric_check_file)):
            try:
                os.remove(str(metric_check_file))
                logger.info('metric_check_file removed - %s' % str(metric_check_file))
            except OSError:
                pass

        return anomaly_id

    def run(self):
        """
        Called when the process intializes.

        Determine if what is known in the Skyline DB
        blah

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

        # See if I am known in the DB, if so, what are my variables
        # self.populate mysql
        # What is my host id in the Skyline panorama DB?
        #   - if not known - INSERT hostname INTO hosts
        # What are the known apps?
        #   - if returned make a dictionary
        # What are the known algorithms?
        #   - if returned make a dictionary

        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
                if ENABLE_PANORAMA_DEBUG:
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

            if ENABLE_PANORAMA_DEBUG:
                # Make sure mysql is available
                mysql_down = True
                while mysql_down:

                    query = 'SHOW TABLES'
                    results = self.mysql_select(query)

                    if results:
                        mysql_down = False
                        logger.info('debug :: tested database query - OK')
                    else:
                        logger.error('error :: failed to query database')
                        sleep(30)

            if ENABLE_PANORAMA_DEBUG:
                try:
                    query = 'SELECT id, test FROM test'
                    result = self.mysql_select(query)
                    logger.info('debug :: tested mysql SELECT query - OK')
                    logger.info('debug :: result: %s' % str(result))
                    logger.info('debug :: result[0]: %s' % str(result[0]))
                    logger.info('debug :: result[1]: %s' % str(result[1]))
# Works
# 2016-06-10 19:07:23 :: 4707 :: result: [(1, u'test1')]
                except:
                    logger.error(
                        'error :: mysql error - %s' %
                        traceback.print_exc())
                    logger.error('error :: failed to SELECT')

            # self.populate the database metatdata tables
            # What is my host id in the Skyline panorama DB?
            host_id = False
            query = 'select id FROM hosts WHERE host=\'%s\'' % this_host
            results = self.mysql_select(query)
            if results:
                host_id = results[0][0]
                logger.info('host_id: %s' % str(host_id))
            else:
                logger.info('failed to determine host id of %s' % this_host)

            #   - if not known - INSERT hostname INTO host
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

            # Like loop through the panorama dir and see if anyone has left you
            # any work, etc
            # Make sure check_dir exists and has not been removed
            try:
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: checking check dir exists - %s' % settings.PANORAMA_CHECK_PATH)
                os.path.exists(settings.PANORAMA_CHECK_PATH)
            except:
                logger.error('error :: check dir did not exist - %s' % settings.PANORAMA_CHECK_PATH)
                mkdir_p(settings.PANORAMA_CHECK_PATH)

                logger.info('check dir created - %s' % settings.PANORAMA_CHECK_PATH)
                os.path.exists(settings.PANORAMA_CHECK_PATH)
                # continue

            """
            Determine if any metric has been added to add
            """
            while True:
                metric_var_files = False
                try:
                    metric_var_files = [f for f in listdir(settings.PANORAMA_CHECK_PATH) if isfile(join(settings.PANORAMA_CHECK_PATH, f))]
                except:
                    logger.error('error :: failed to list files in check dir')
                    logger.info(traceback.format_exc())

                if not metric_var_files:
                    logger.info('sleeping 20 no metric check files')
                    sleep(20)

                # Discover metric anomalies to insert
                metric_var_files = False
                try:
                    metric_var_files = [f for f in listdir(settings.PANORAMA_CHECK_PATH) if isfile(join(settings.PANORAMA_CHECK_PATH, f))]
                except:
                    logger.error('error :: failed to list files in check dir')
                    logger.info(traceback.format_exc())

                if metric_var_files:
                    break

            metric_var_files_sorted = sorted(metric_var_files)
            metric_check_file = '%s/%s' % (settings.PANORAMA_CHECK_PATH, str(metric_var_files_sorted[0]))

            logger.info('assigning anomaly for insertion - %s' % str(metric_var_files_sorted[0]))

            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            now = time()
            for i in range(1, settings.PANORAMA_PROCESSES + 1):
                try:
                    p = Process(target=self.spin_process, args=(i, metric_check_file))
                    pids.append(p)
                    pid_count += 1
                    logger.info('starting %s of %s spin_process/es' % (str(pid_count), str(settings.PANORAMA_PROCESSES)))
                    p.start()
                    spawned_pids.append(p.pid)
                except:
                    logger.error('error :: to start spin_process')
                    logger.info(traceback.format_exc())
                    continue

            # Send wait signal to zombie processes
            # for p in pids:
            #     p.join()
            # Self monitor processes and terminate if any spin_process has run
            # for longer than CRUCIBLE_TESTS_TIMEOUT
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
                            skyline_app, str(settings.PANORAMA_PROCESSES),
                            time_to_run))
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('%s :: timed out, killing all spin_process processes' % (skyline_app))
                for p in pids:
                    p.terminate()
                    p.join()

                check_file_name = os.path.basename(str(metric_check_file))
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: check_file_name - %s' % check_file_name)
                check_file_timestamp = check_file_name.split('.', 1)[0]
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: check_file_timestamp - %s' % str(check_file_timestamp))
                check_file_metricname_txt = check_file_name.split('.', 1)[1]
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: check_file_metricname_txt - %s' % check_file_metricname_txt)
                check_file_metricname = check_file_metricname_txt.replace('.txt', '')
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: check_file_metricname - %s' % check_file_metricname)
                check_file_metricname_dir = check_file_metricname.replace('.', '/')
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: check_file_metricname_dir - %s' % check_file_metricname_dir)

                metric_failed_check_dir = '%s/%s/%s' % (failed_checks_dir, check_file_metricname_dir, check_file_timestamp)

                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
