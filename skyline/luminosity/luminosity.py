from __future__ import division
import logging
import os
from os import kill, getpid
from sys import version_info
from ast import literal_eval

# @modified 20191115 - Branch #3262: py3
# try:
#     from Queue import Empty
# except:
#     from queue import Empty

from time import time, sleep
from threading import Thread
# @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
# Use Redis sets in place of Manager().list() to reduce memory and number of
# processes
# from multiprocessing import Process, Manager
from multiprocessing import Process
# from redis import StrictRedis
import traceback
import mysql.connector
from pymemcache.client.base import Client as pymemcache_Client

import settings
from skyline_functions import (
    mysql_select, send_graphite_metric,
    # @added 20191030 - Bug #3266: py3 Redis binary objects not strings
    #                   Branch #3262: py3
    # Added a single functions to deal with Redis connection and the
    # charset='utf-8', decode_responses=True arguments required in py3
    get_redis_conn, get_redis_conn_decoded)

from database import get_engine
# from process_correlations import *

skyline_app = 'luminosity'
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
    ENABLE_LUMINOSITY_DEBUG = settings.ENABLE_LUMINOSITY_DEBUG
except:
    logger.error('error :: cannot determine ENABLE_LUMINOSITY_DEBUG from settings')
    ENABLE_LUMINOSITY_DEBUG = False

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

try:
    LUMINOSITY_PROCESSES = settings.LUMINOSITY_PROCESSES
except:
    logger.info('warning :: cannot determine LUMINOSITY_PROCESSES from settings' % skyline_app)
    # @modified 20180110 - Task #2266: Evaluate luminol for the luminosity branch
    # It is fast and lightweight
    # luminosity_processes = 2
    LUMINOSITY_PROCESSES = 1

# @aded 20210308 - Feature #3978: luminosity - classify_metrics
#                  Feature #3642: Anomaly type classification
try:
    LUMINOSITY_CLASSIFY_METRICS = settings.LUMINOSITY_CLASSIFY_METRICS
except:
    LUMINOSITY_CLASSIFY_METRICS = True

# @added 20210323 - Feature #3642: Anomaly type classification
try:
    LUMINOSITY_CLASSIFY_ANOMALIES = settings.LUMINOSITY_CLASSIFY_ANOMALIES
except:
    LUMINOSITY_CLASSIFY_ANOMALIES = False


skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)

# Database configuration
config = {'user': settings.PANORAMA_DBUSER,
          'password': settings.PANORAMA_DBUSERPASS,
          'host': settings.PANORAMA_DBHOST,
          'port': settings.PANORAMA_DBPORT,
          'database': settings.PANORAMA_DATABASE,
          'raise_on_warnings': True}


LOCAL_DEBUG = False


class PaulBourke:
    """
    # @added 20180526 - Branch #2270: luminosity
    The PaulBourke class pays homage to Paul Bourke who originally described
    the cross correlation method on which Linkedin's luminol.correlate library
    is based upon, he described this in August 1996 and it is upon this which
    Skyline Luminosity is also based.

    So Skyline has moved from:
    Shewhart's statistical process control circa 1924 tech
    To tsfresh - circa 20161029 when it entered Skyline
    Now to 1996(2015), although 1996 ideas can still be very, very useful and
    work well.

    Please do visit these following two URLs in order:
    http://paulbourke.net/miscellaneous/correlate/thanks_mr_bourke_from_skyline_for  # this will 404 but hopefully he has something watching his 404 rate :)
    http://paulbourke.net/miscellaneous/correlate/  # this will 200

    This is all part of the adventures in Skyline.  If you enjoy this sort of
    thing, then I posit to you that Sir Walter Munk is one of the least known
    remarkable scientist of tthe current and previous millienia.
    Born: October 19, 1917 (age 100 years)
    His accomplishments abound, he is like Turing, Marconi, Berners-Lee and
    Einstein rolled into Oceangraphy, but few have ever heard of him.

    If we are giving kudos to Shewhart, tsfresh and Paul Bourke, we can slip
    some Walther Munk in here too, although currently he nor his work has
    anything to do with Skyline, however there are some ideas, not necessarily
    desrcibed in the roadmap, that revolve a byproduct of monitoring surf
    conditions via a webcams, which could be used for monitoring tide levels
    too, which would be right up Dr Munk's alley.

    """
    pass


class Luminosity(Thread):
    """
    The Luminosity class which controls the luminosity thread and spawned
    processes.
    """

    def __init__(self, parent_pid):
        """
        Initialize Luminosity

        Create the :obj:`redis_conn` a Redis client object
        Create the :obj:`correlations` list
        Create the :obj:`mysql_conn` MySQLConnection object
        Create the :obj:`memcache_client` a constructor that does not make a
        connection to memcached. The first call to a method on the object will
        do that.

        """
        super(Luminosity, self).__init__()
        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        # @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # Use get_redis_conn and get_redis_conn_decoded to use on Redis sets when the bytes
        # types need to be decoded as utf-8 to str
        # if settings.REDIS_PASSWORD:
        #     self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        # else:
        #     self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)

        # @added 20191030 - Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
        # Added a single functions to deal with Redis connection and the
        # charset='utf-8', decode_responses=True arguments required in py3
        self.redis_conn = get_redis_conn(skyline_app)
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

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
        # self.correlations = Manager().list()
        # @added 20180720 - Task #2462: Implement useful metrics for Luminosity
        # self.metrics_checked_for_correlation = Manager().list()
        # self.runtimes = Manager().list()
        self.mysql_conn = mysql.connector.connect(**config)
        if settings.MEMCACHE_ENABLED:
            self.memcache_client = pymemcache_Client((settings.MEMCACHED_SERVER_IP, settings.MEMCACHED_SERVER_PORT), connect_timeout=0.1, timeout=0.2)
        else:
            self.memcache_client = None

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
            logger.warning('warning :: parent or current process dead')
            exit(0)

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

    # @added 20210323 - Feature #3642: Anomaly type classification
    def classify_anomalies(self, i, classify_anomalies_set, start_timestamp, max_run_seconds):
        """
        Classify anomalies

        :param i: python process id
        :param classify_anomalies_set: set from luminosity.classify_anomalies
        :param start_timestamp: the process star timestamp
        :param max_run_seconds: the max number of seconds to run for
        :type i: object
        :type classify_anomalies_set: set
        :type start_timestamp: int
        :type max_run_seconds: int
        :return: boolean
        :rtype: boolean

        """

        child_process_pid = os.getpid()
        logger.info('child process pid %s - classifying anomalies' % (str(child_process_pid)))
        ran = False

        try:
            from classify_anomalies import classify_anomalies
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: importing classify_anomalies')

        try:
            ran = classify_anomalies(i, classify_anomalies_set, start_timestamp, max_run_seconds)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: running classify_anomalies')
            return False
        return ran

    def classify_metrics(self, i, start_timestamp, max_run_seconds):
        """
        Classify metrics

        :param i: python process id
        :param start_timestamp: the process star timestamp
        :param max_run_seconds: the max number of seconds to run for
        :type i: object
        :type start_timestamp: int
        :type max_run_seconds: int
        :return: boolean
        :rtype: boolean

        """

        child_process_pid = os.getpid()
        logger.info('child process pid %s - classifying metrics' % (str(child_process_pid)))
        ran = False

        try:
            from classify_metrics import classify_metrics
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: importing classify_metrics')
        try:
            ran = classify_metrics(i, start_timestamp, max_run_seconds)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: running classify_metrics')
            return False
        return ran

    def spin_process(self, i, anomaly_id):
        """
        Assign an anomalous metric and determine correlated metrics

        :param i: python process id
        :param anomaly_id: the anomaly_id
        :type i: object
        :type anomaly_id: int
        :return: boolean
        :rtype: boolean

        """

        child_process_pid = os.getpid()
        logger.info('child process pid %s - processing anomaly_id - %s' % (str(child_process_pid), str(anomaly_id)))

        try:
            from process_correlations import process_correlations
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: importing process_correlations')

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

        if settings.MEMCACHE_ENABLED:
            try:
                memcache_key = '%s.last.processed.anomaly.id' % skyline_app
                self.memcache_client.set(memcache_key, int(anomaly_id))
                logger.info('processed - set the memcache key - %s - %s' % (memcache_key, str(anomaly_id)))
            except:
                logger.error('error :: failed to set  the memcache key - %s - %s' % (memcache_key, str(anomaly_id)))
            try:
                self.memcache_client.close()
            except:
                logger.error('error :: failed to close memcache_client')

        try:
            # @modified 20180720 - Task #2462: Implement useful metrics for Luminosity
            # Added runtime
            # @modified 20210124 - Feature #3956: luminosity - motifs
            # Added motifs
            base_name, anomaly_timestamp, anomalies, correlated_metrics, correlations, sorted_correlations, metrics_checked_for_correlation, runtime, motifs = process_correlations(i, anomaly_id)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: processing correlations')
            return False

        # @added 20180414 - Bug #2352: Luminosity no metrics MySQL error
        # Do not query with an empty string
        if not correlated_metrics:
            logger.info('no correlations found for %s anomaly id %s' % (
                base_name, str(anomaly_id)))
            return False
        else:
            logger.info('%s correlations found for %s anomaly id %s' % (
                str(len(correlated_metrics)), base_name, str(anomaly_id)))

        # @added 20180720 - Task #2462: Implement useful metrics for Luminosity
        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
        # self.metrics_checked_for_correlation.append(metrics_checked_for_correlation)
        # self.runtimes.append(runtime)
        redis_set = 'luminosity.correlations'
        data = str(metrics_checked_for_correlation)
        try:
            self.redis_conn.sadd(redis_set, data)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to add %s to Redis set %s' % (
                str(data), str(redis_set)))
        redis_set = 'luminosity.runtimes'
        data = str(runtime)
        try:
            self.redis_conn.sadd(redis_set, data)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to add %s to Redis set %s' % (
                str(data), str(redis_set)))

        metrics_str = ''
        for metric_name in correlated_metrics:
            if metrics_str == '':
                new_metrics_str = "'%s'" % metric_name
            else:
                new_metrics_str = "%s,'%s'" % (metrics_str, metric_name)
            metrics_str = new_metrics_str
        # metrics_str

        query = 'SELECT id,metric FROM metrics WHERE metric in (%s)' % str(metrics_str)
        try:
            results = mysql_select(skyline_app, query)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: querying MySQL - SELECT id,metric FROM metrics WHERE metric in (%s)' % str(metrics_str))
            return False

        correlated_metrics_list = []
        for metric_id, metric in results:
            correlated_metrics_list.append([int(metric_id), str(metric)])
        logger.info('number of metric ids determined from the metrics tables - %s' % str(len(correlated_metrics_list)))

        correlations_shifted_too_far = 0
        if sorted_correlations:
            logger.info('number of correlations shifted too far - %s' % str(correlations_shifted_too_far))
            if LOCAL_DEBUG or ENABLE_LUMINOSITY_DEBUG:
                logger.debug('debug :: sorted_correlations :: %s' % str(sorted_correlations))
            luminosity_correlations = []
            for metric, coefficient, shifted, shifted_coefficient in sorted_correlations:
                for metric_id, metric_name in correlated_metrics_list:
                    if metric == metric_name:
                        if shifted < 2:
                            luminosity_correlations.append([anomaly_id, int(metric_id), coefficient, shifted, shifted_coefficient])
                        else:
                            correlations_shifted_too_far += 1
            logger.info('number of correlations shifted too far - %s' % str(correlations_shifted_too_far))
            first_value_not_added = True
            values_string = 'INSERT INTO luminosity (id, metric_id, coefficient, shifted, shifted_coefficient) VALUES '

            # @added 20180420 - Branch #2270: luminosity
            # Only try and insert if there are values present
            values_present = False

            number_of_correlations_in_insert = 0
            for anomaly_id, metric_id, coefficient, shifted, shifted_coefficient in luminosity_correlations:
                if coefficient:
                    values_present = True
                    # @added 20170720 - Task #2462: Implement useful metrics for Luminosity
                    # Populate the self.correlations list to send a count to Graphite
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # self.correlations.append(coefficient)
                    redis_set = 'luminosity.correlations'
                    data = str(coefficient)
                    try:
                        self.redis_conn.sadd(redis_set, data)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add %s to Redis set %s' % (
                            str(data), str(redis_set)))

                    number_of_correlations_in_insert += 1

                ins_values = '(%s,%s,%s,%s,%s)' % (str(anomaly_id),
                                                   str(metric_id),
                                                   str(round(coefficient, 5)),
                                                   str(shifted),
                                                   str(round(shifted_coefficient, 5)))
                if first_value_not_added:
                    first_value_not_added = False
                    values_string = 'INSERT INTO luminosity (id, metric_id, coefficient, shifted, shifted_coefficient) VALUES %s' % ins_values
                else:
                    new_values_string = '%s,%s' % (values_string, ins_values)
                    values_string = new_values_string
            new_values_string = '%s;' % values_string
            values_string = new_values_string
            # logger.info('debug insert string :: %s' % str(values_string))
            # 'INSERT INTO luminosity (anomaly_id, metric_id, coefficient, shifted, shifted_coefficient) VALUES (68882,619,1.0,0,1.0),...,(68882,489,1.0,0,1.0);'
            # Needs a mysql_insert not SQLAlchemy
            luminosity_populated = False
            if luminosity_correlations and values_present:
                try:
                    self.mysql_insert(values_string)
                    luminosity_populated = True
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: MySQL insert - %s' % str(values_string))
        if luminosity_populated:
            logger.info('%s correlations added to database for %s anomaly id %s' % (
                str(number_of_correlations_in_insert), base_name, str(anomaly_id)))
            if LOCAL_DEBUG or ENABLE_LUMINOSITY_DEBUG:
                logger.debug('debug :: values_string :: %s' % str(values_string))

        return luminosity_populated

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

        # @added 20190417 - Feature #2948: LUMINOSITY_ENABLED setting
        # If Luminosity is not enabled, do nothing
        luminosity_enabled = True
        try:
            luminosity_enabled = settings.LUMINOSITY_ENABLED
            logger.info('LUMINOSITY_ENABLED is set to %s' % str(luminosity_enabled))
        except:
            logger.info('warning :: LUMINOSITY_ENABLED is not declared in settings.py, defaults to True')

        # @added 20190417 - Feature #2950: Report defaulted settings to log
        # Added all the globally declared settings to enable reporting in the
        # log the state of each setting.
        try:
            ENABLE_LUMINOSITY_DEBUG = settings.ENABLE_LUMINOSITY_DEBUG
            logger.info('ENABLE_LUMINOSITY_DEBUG is set from settings.py to %s' % str(ENABLE_LUMINOSITY_DEBUG))
        except:
            logger.info('warning :: ENABLE_LUMINOSITY_DEBUG is not declared in settings.py, defaults to False')
            ENABLE_LUMINOSITY_DEBUG = False
        try:
            SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
            if SERVER_METRIC_PATH == '.':
                SERVER_METRIC_PATH = ''
            logger.info('SERVER_METRIC_PATH is set from settings.py to %s' % str(SERVER_METRIC_PATH))
        except:
            SERVER_METRIC_PATH = ''
            logger.info('warning :: SERVER_METRIC_PATH is not declared in settings.py, defaults to \'\'')
        try:
            LUMINOSITY_PROCESSES = settings.LUMINOSITY_PROCESSES
            logger.info('LUMINOSITY_PROCESSES is set from settings.py to %s' % str(LUMINOSITY_PROCESSES))
        except:
            # @modified 20180110 - Task #2266: Evaluate luminol for the luminosity branch
            # It is fast and lightweight
            # luminosity_processes = 2
            LUMINOSITY_PROCESSES = 1
            logger.info('warning :: cannot determine LUMINOSITY_PROCESSES from settings.py, defaults to %s' % str(LUMINOSITY_PROCESSES))

        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
                if ENABLE_LUMINOSITY_DEBUG:
                    logger.info('debug :: connected to Redis')
            except:
                logger.error('error :: cannot connect to redis at socket path %s' % (
                    settings.REDIS_SOCKET_PATH))
                sleep(30)
                # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                # @modified 20191115 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # Use get_redis_conn and get_redis_conn_decoded to use on Redis sets when the bytes
                # types need to be decoded as utf-8 to str
                # if settings.REDIS_PASSWORD:
                #     self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                # else:
                #     self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                # @added 20191115 - Bug #3266: py3 Redis binary objects not strings
                #                   Branch #3262: py3
                self.redis_conn = get_redis_conn(skyline_app)
                self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

                continue

            # Report app up
            try:
                self.redis_conn.setex(skyline_app, 120, now)
                logger.info('updated Redis key for %s up' % skyline_app)
            except:
                logger.error('error :: failed to update Redis key for %s up' % skyline_app)

            # @added 20190417 - Feature #: LUMINOSITY_ENABLED setting
            # If Luminosity is not enabled, do nothing
            if not luminosity_enabled:
                logger.info('luminosity is not enabled LUMINOSITY_ENABLED set to %s, sleeping for 20 seconds' % str(settings.LUMINOSITY_ENABLED))
                sleep(20)
                continue


            """
            Determine if any new anomalies have been added
            """
            while True:
                process_anomaly_id = None
                last_processed_anomaly_id = None
                memcache_last_processed_anomaly_id_data = False
                # Check memcached before MySQL
                memcache_key = '%s.last.processed.anomaly.id' % skyline_app
                if settings.MEMCACHE_ENABLED:
                    try:
                        # @modified 20191029 - Task #3304: py3 - handle pymemcache bytes not str
                        # last_processed_anomaly_id = self.memcache_client.get(memcache_key)
                        if python_version == 2:
                            last_processed_anomaly_id = self.memcache_client.get(memcache_key)
                        else:
                            last_processed_anomaly_id = self.memcache_client.get(memcache_key).decode('utf-8')
                        # if memcache does not have the key the response to the
                        # client is None, it does not except
                    except:
                        # @modified 20200507 - stop reporting this as an error
                        # it can be expected to happen from time to time
                        # logger.error('error :: failed to get %s from memcache' % memcache_key)
                        logger.info('failed to get %s from memcache, will query DB' % memcache_key)
                    try:
                        self.memcache_client.close()
                    except:
                        logger.error('error :: failed to close memcache_client')

                if last_processed_anomaly_id:
                    logger.info('last_processed_anomaly_id found in memcache - %s' % str(last_processed_anomaly_id))
                    memcache_last_processed_anomaly_id_data = True
                else:
                    # @modified 20190517 - Bug #3016: Handle no anomaly ids in luminosity
                    #                      Branch #3002: docker
                    # Log appropriate to whether memcache is enabled or not
                    if settings.MEMCACHE_ENABLED:
                        logger.info('last_processed_anomaly_id key was NOT found in memcache - %s' % str(last_processed_anomaly_id))
                    else:
                        logger.info('memcache not enabled not checking for last_processed_anomaly_id key')

                if not last_processed_anomaly_id:
                    query = 'SELECT id FROM luminosity WHERE id=(SELECT MAX(id) FROM luminosity) ORDER BY id DESC LIMIT 1'
                    results = None
                    try:
                        results = mysql_select(skyline_app, query)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: MySQL quey failed - %s' % query)
                    if results:
                        try:
                            last_processed_anomaly_id = int(results[0][0])
                            logger.info('last_processed_anomaly_id found from DB - %s' % str(last_processed_anomaly_id))
                        except:
                            logger.error(traceback.format_exc())

                        if last_processed_anomaly_id and settings.MEMCACHE_ENABLED:
                            if not memcache_last_processed_anomaly_id_data:
                                logger.info('Populating memcache with DB result - %s' % str(last_processed_anomaly_id))
                                try:
                                    self.memcache_client.set(memcache_key, int(last_processed_anomaly_id))
                                    logger.info('populated memcache key %s with %s' % (memcache_key, str(last_processed_anomaly_id)))
                                except:
                                    logger.error('error :: failed to set  the memcache key - %s - %s' % (memcache_key, str(last_processed_anomaly_id)))
                                try:
                                    self.memcache_client.close()
                                except:
                                    logger.error('error :: failed to close memcache_client')

                if not last_processed_anomaly_id:
                    # Check MySQL
                    now = int(time())
                    after = now - 600
                    # @modified 20210525 - https://github.com/earthgecko/skyline/pull/429
                    #                      Task #4102: Merge luminosity.PR429
                    # More efficient query, although it is only getting a few rows from the DB
                    # every little helps (thanks @ashemez)
                    # query = 'SELECT * FROM anomalies WHERE anomaly_timestamp > \'%s\'' % str(after)  # nosec
                    query = 'SELECT id FROM anomalies WHERE anomaly_timestamp > \'%s\'' % str(after)  # nosec
                    results = None
                    try:
                        results = mysql_select(skyline_app, query)
                    except:
                        logger.error('error :: MySQL quey failed - %s' % query)
                    if results:
                        process_anomaly_id = int(results[0][0])
                        logger.info('found new anomaly id to process from the DB - %s' % str(process_anomaly_id))
                        # Handle the first one
                        last_processed_anomaly_id = process_anomaly_id - 1
                    else:
                        logger.info('no new anomalies in the anomalies table')

                # @added 20190517 - Bug #3016: Handle no anomaly ids in luminosity
                #                   Branch #3002: docker
                # When Skyline is first installed, if luminosity is enabled it
                # reports errors as there are no anomaly ids
                if str(last_processed_anomaly_id) == 'None':
                    last_processed_anomaly_id = 0

                # @modified 20210525 - https://github.com/earthgecko/skyline/pull/429
                #                      Task #4102: Merge luminosity.PR429
                # More efficient query, although it is only getting a few rows from the DB
                # every little helps (thanks @ashemez)
                # query = 'SELECT * FROM anomalies WHERE id > \'%s\'' % str(last_processed_anomaly_id)  # nosec
                query = 'SELECT id FROM anomalies WHERE id > \'%s\'' % str(last_processed_anomaly_id)  # nosec
                results = None
                try:
                    results = mysql_select(skyline_app, query)
                except:
                    logger.error('error :: MySQL quey failed - %s' % query)
                if results:
                    try:
                        process_anomaly_id = int(results[0][0])
                        logger.info('found the next new anomaly id to process from the DB - %s' % str(process_anomaly_id))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: from query - %s' % query)
                else:
                    logger.info('no new anomalies in the anomalies table')

                if process_anomaly_id and last_processed_anomaly_id:
                    if isinstance(last_processed_anomaly_id, int):
                        if isinstance(process_anomaly_id, int):
                            if last_processed_anomaly_id == process_anomaly_id:
                                logger.info('anomaly id already processed - %s' % str(process_anomaly_id))
                                process_anomaly_id = None

                if not process_anomaly_id:

                    if not LUMINOSITY_CLASSIFY_METRICS and not LUMINOSITY_CLASSIFY_ANOMALIES:
                        logger.info('sleeping 20 no anomalies to correlate - last processed anomaly id - %s' % str(last_processed_anomaly_id))
                        sleep(20)
                        up_now = time()
                        # Report app up
                        try:
                            self.redis_conn.setex(skyline_app, 120, up_now)
                            logger.info('updated Redis key for %s up' % skyline_app)
                        except:
                            logger.error('error :: failed to update Redis key for %s up' % skyline_app)

                    # @added 20210323 - Feature #3642: Anomaly type classification
                    if LUMINOSITY_CLASSIFY_ANOMALIES:
                        classify_anomalies_set = {}
                        try:
                            classify_anomalies_set = self.redis_conn_decoded.smembers('luminosity.classify_anomalies')
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: from query - %s' % query)
                        if len(classify_anomalies_set) > 0:
                            current_now = int(time())
                            classify_for = 19
                            logger.info('classifying anomalies for %.2f seconds' % classify_for)
                            pid_count = 0
                            pids = []
                            spawned_pids = []
                            try:
                                p = Process(target=self.classify_anomalies, args=(self.current_pid, classify_anomalies_set, current_now, classify_for))
                                pids.append(p)
                                pid_count += 1
                                logger.info('starting classify_anomalies')
                                p.start()
                                spawned_pids.append(p.pid)
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to start classify_anomalies')

                            # Self monitor processes and terminate if any classify_anomalies
                            # that has run for too long
                            p_starts = time()
                            while time() - p_starts <= classify_for:
                                if any(p.is_alive() for p in pids):
                                    # Just to avoid hogging the CPU
                                    sleep(.1)
                                else:
                                    # All the processes are done, break now.
                                    time_to_run = time() - p_starts
                                    logger.info(
                                        'classify_anomalies completed in %.2f seconds' % (
                                            time_to_run))
                                    break
                            else:
                                # We only enter this if we didn't 'break' above.
                                logger.info('timed out, killing classify_anomalies process')
                                for p in pids:
                                    try:
                                        p.terminate()
                                        # p.join()
                                        logger.info('killed classify_anomalies process')
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: killing all classify_anomalies processes')
                            for p in pids:
                                if p.is_alive():
                                    logger.info('stopping classify_anomalies - %s' % (str(p.is_alive())))
                                    p.join()
                            logger.info('classify_anomalies - complete')
                            up_now = time()
                            # Report app up
                            try:
                                self.redis_conn.setex(skyline_app, 120, up_now)
                                logger.info('updated Redis key for %s up' % skyline_app)
                            except:
                                logger.error('error :: failed to update Redis key for %s up' % skyline_app)
                            run_end = time()
                            classify_anomalies_runtime = run_end - current_now
                            logger.info('classify_anomalies took %.6f seconds' % classify_anomalies_runtime)

                    # @added 20210308 - Feature #3978: luminosity - classify_metrics
                    #                   Feature #3642: Anomaly type classification
                    if LUMINOSITY_CLASSIFY_METRICS:
                        current_now = int(time())
                        classify_for = 19
                        logger.info('classifying metrics for %.2f seconds' % classify_for)
                        pid_count = 0
                        pids = []
                        spawned_pids = []
                        try:
                            p = Process(target=self.classify_metrics, args=(self.current_pid, current_now, classify_for))
                            pids.append(p)
                            pid_count += 1
                            logger.info('starting classify_metrics')
                            p.start()
                            spawned_pids.append(p.pid)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to start classify_metrics')

                        # Self monitor processes and terminate if any classify_metrics
                        # that has run for too long
                        p_starts = time()
                        while time() - p_starts <= classify_for:
                            if any(p.is_alive() for p in pids):
                                # Just to avoid hogging the CPU
                                sleep(.1)
                            else:
                                # All the processes are done, break now.
                                time_to_run = time() - p_starts
                                logger.info(
                                    'classify_metrics completed in %.2f seconds' % (
                                        time_to_run))
                                break
                        else:
                            # We only enter this if we didn't 'break' above.
                            logger.info('timed out, killing classify_metrics process')
                            for p in pids:
                                try:
                                    p.terminate()
                                    # p.join()
                                    logger.info('killed classify_metrics process')
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: killing all classify_metrics processes')
                        for p in pids:
                            if p.is_alive():
                                logger.info('stopping classify_metrics - %s' % (str(p.is_alive())))
                                p.join()
                        logger.info('classify_metrics - complete')
                        up_now = time()
                        # Report app up
                        try:
                            self.redis_conn.setex(skyline_app, 120, up_now)
                            logger.info('updated Redis key for %s up' % skyline_app)
                        except:
                            logger.error('error :: failed to update Redis key for %s up' % skyline_app)
                        run_end = time()
                        classify_runtime = int(run_end - current_now)
                        if classify_runtime < 19:
                            sleep_for = (20 - classify_runtime)
                            logger.info('sleeping for %.2f seconds due to low classify run time' % sleep_for)
                            sleep(sleep_for)

                cache_key = '%s.sent_graphite_metrics' % skyline_app
                redis_sent_graphite_metrics = False
                try:
                    redis_sent_graphite_metrics = self.redis_conn.get(cache_key)
                except Exception as e:
                    logger.error('error :: could not query Redis for key %s: %s' % (cache_key, e))

                # Flush metrics to Graphite
                if not redis_sent_graphite_metrics:
                    try:
                        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        # correlations = str(len(self.correlations))
                        # @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
                        #                      Branch #3262: py3
                        # correlations = str(len(list(self.redis_conn.smembers('luminosity.correlations'))))
                        correlations = str(len(list(self.redis_conn_decoded.smembers('luminosity.correlations'))))
                    except:
                        correlations = '0'
                    logger.info('correlations       :: %s' % correlations)
                    send_metric_name = '%s.correlations' % skyline_app_graphite_namespace
                    send_graphite_metric(skyline_app, send_metric_name, correlations)

                    # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    try:
                        # @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
                        #                      Branch #3262: py3
                        # runtimes = list(self.redis_conn.smembers('luminosity.runtimes'))
                        runtimes = list(self.redis_conn_decoded.smembers('luminosity.runtimes'))
                    except:
                        runtimes = []

                    # @added 20180720 - Task #2462: Implement useful metrics for Luminosity
                    #                   Branch #2270: luminosity
                    # runtime metric to monitor the time it takes to process
                    # correlations
                    try:
                        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        # if len(self.runtimes) > 1:
                        #     avg_runtime = sum(self.runtimes) / len(self.runtimes)
                        # else:
                        #     avg_runtime = sum(self.runtimes)
                        if len(runtimes) > 1:
                            avg_runtime = sum(runtimes) / len(runtimes)
                        else:
                            avg_runtime = sum(runtimes)
                    except:
                        avg_runtime = '0'
                    logger.info('avg_runtime       :: %s' % str(avg_runtime))
                    send_metric_name = '%s.avg_runtime' % skyline_app_graphite_namespace
                    send_graphite_metric(skyline_app, send_metric_name, str(avg_runtime))
                    try:
                        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                        # metrics_checked_for_correlation = str(sum(self.metrics_checked_for_correlation))
                        # @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
                        #                      Branch #3262: py3
                        # metrics_checked_for_correlation = str(len(list(self.redis_conn.smembers('luminosity.metrics_checked_for_correlation'))))
                        metrics_checked_for_correlation = str(len(list(self.redis_conn_decoded.smembers('luminosity.metrics_checked_for_correlation'))))
                    except:
                        metrics_checked_for_correlation = '0'
                    logger.info('metrics_checked_for_correlation   :: %s' % metrics_checked_for_correlation)
                    send_metric_name = '%s.metrics_checked_for_correlation' % skyline_app_graphite_namespace
                    send_graphite_metric(skyline_app, send_metric_name, metrics_checked_for_correlation)

                    # @added 20210308 - Feature #3978: luminosity - classify_metrics
                    #                   Feature #3642: Anomaly type classification
                    if LUMINOSITY_CLASSIFY_METRICS:
                        metrics_proceessed_counts = []
                        classify_metrics_proceessed = []
                        try:
                            classify_metrics_proceessed = self.redis_conn_decoded.smembers('luminosity.classify_metrics.proceessed')
                        except Exception as e:
                            logger.error('error :: could not query Redis for key luminosity.classify_metrics.proceessed: %s' % str(e))
                        try:
                            self.redis_conn.delete('luminosity.classify_metrics.proceessed')
                        except Exception as e:
                            logger.error('error :: could not delete Redis for key luminosity.classify_metrics.proceessed: %s' % str(e))
                        for metrics_proceessed_item in classify_metrics_proceessed:
                            metrics_proceessed = literal_eval(metrics_proceessed_item)
                            metrics_proceessed_counts.append(metrics_proceessed[1])
                        metrics_proceessed = sum(metrics_proceessed_counts)
                        logger.info('classify_metrics_proceessed   :: %s' % metrics_proceessed)
                        send_metric_name = '%s.classify_metrics.proceessed' % skyline_app_graphite_namespace
                        send_graphite_metric(skyline_app, send_metric_name, str(metrics_proceessed))
                        metrics_classified_counts = []
                        classify_metrics_classified = []
                        try:
                            classify_metrics_classified = self.redis_conn_decoded.smembers('luminosity.classify_metrics.classified')
                        except Exception as e:
                            logger.error('error :: could not query Redis for key luminosity.classify_metrics.classified: %s' % str(e))
                        try:
                            self.redis_conn.delete('luminosity.classify_metrics.classified')
                        except Exception as e:
                            logger.error('error :: could not delete Redis for key luminosity.classify_metrics.classified: %s' % str(e))
                        for metrics_classified_item in classify_metrics_classified:
                            metrics_classified = literal_eval(metrics_classified_item)
                            metrics_classified_counts.append(metrics_classified[1])
                        metrics_classified = sum(metrics_classified_counts)
                        logger.info('classify_metrics_classified   :: %s' % metrics_classified)
                        send_metric_name = '%s.classify_metrics.classified' % skyline_app_graphite_namespace
                        send_graphite_metric(skyline_app, send_metric_name, str(metrics_classified))

                    sent_graphite_metrics_now = int(time())
                    try:
                        self.redis_conn.setex(cache_key, 59, sent_graphite_metrics_now)
                        logger.info('updated Redis key - %s' % cache_key)
                    except:
                        logger.error('error :: failed to update Redis key - %s up' % cache_key)

                    # Reset lists
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # self.correlations[:] = []
                    # @added 20180720 - Task #2462: Implement useful metrics for Luminosity
                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # self.runtimes[:] = []
                    # self.metrics_checked_for_correlation[:] = []

                    # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                    # Use Redis sets instead of Manager().list()
                    delete_redis_sets = [
                        'luminosity.correlations',
                        'luminosity.runtimes',
                        'luminosity.metrics_checked_for_correlation'
                    ]
                    for i_redis_set in delete_redis_sets:
                        redis_set_to_delete = i_redis_set
                        try:
                            self.redis_conn.delete(redis_set_to_delete)
                            logger.info('deleted Redis set - %s' % redis_set_to_delete)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to delete Redis set - %s' % redis_set_to_delete)

                # @added 20180720 - Task #2462: Implement useful metrics for Luminosity
                #                   Feature #2464: luminosity_remote_data
                # Added the ability to add a Redis key to overview the memcached
                # key luminosity.last.processed.anomaly.id some it does not have
                # to be changed via telnet to memcache.
                if not process_anomaly_id or not redis_sent_graphite_metrics:
                    cache_key = '%s.last.processed.anomaly.id' % skyline_app
                    redis_last_processed_anomaly_id_redis_key = False
                    try:
                        redis_last_processed_anomaly_id_redis_key = self.redis_conn.get(cache_key)
                    except Exception as e:
                        logger.error('error :: could not query Redis for key %s: %s' % (cache_key, e))
                    if redis_last_processed_anomaly_id_redis_key:
                        logger.info('found Redis %s key to override the mecache key setting process_anomaly_id to %s' % (cache_key, str(redis_last_processed_anomaly_id_redis_key)))
                        try:
                            process_anomaly_id = int(redis_last_processed_anomaly_id_redis_key)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to set process_anomaly_id from Rdis override key value')
                        # And remove the Redis override key as it is only meant
                        # to override once to allow for a replay for debug
                        # purposes only.
                        try:
                            self.redis_conn.setex(cache_key, 1, int(redis_last_processed_anomaly_id_redis_key))
                            logger.info('updated Redis key - %s' % cache_key)
                        except:
                            logger.error('error :: failed to update Redis key - %s up to 1 second expiring to delete it.' % cache_key)

                if process_anomaly_id:
                    break

            # Spawn process
            logger.info('spawning processes to correlate anomaly id %s' % str(process_anomaly_id))
            pids = []
            spawned_pids = []
            pid_count = 0
            now = time()
            for i in range(1, LUMINOSITY_PROCESSES + 1):
                try:
                    p = Process(target=self.spin_process, args=(i, process_anomaly_id))
                    pids.append(p)
                    pid_count += 1
                    logger.info(
                        'starting %s of %s spin_process/es' % (
                            str(pid_count),
                            str(LUMINOSITY_PROCESSES)))
                    p.start()
                    spawned_pids.append(p.pid)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to start spin_process')
                    continue

            # Self monitor processes and terminate if any spin_process has run
            # for to long
            p_starts = time()
            # @modified 20201222 - Feature #3824: get_cluster_data
            # Allow for longer cluster responses with large metric populations
            # while time() - p_starts <= 60:
            allow_to_run_for = 60
            if settings.REMOTE_SKYLINE_INSTANCES:
                allow_to_run_for = 90
            while time() - p_starts <= allow_to_run_for:
                if any(p.is_alive() for p in pids):
                    # Just to avoid hogging the CPU
                    sleep(.1)
                else:
                    # All the processes are done, break now.
                    time_to_run = time() - p_starts
                    logger.info(
                        '%s spin_process completed in %.2f seconds' % (
                            str(LUMINOSITY_PROCESSES),
                            time_to_run))
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('timed out, killing all spin_process processes')
                for p in pids:
                    try:
                        p.terminate()
                        # p.join()
                        logger.info('killed spin_process process')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: killing all spin_process processes')

            for p in pids:
                if p.is_alive():
                    logger.info('stopping spin_process - %s' % (str(p.is_alive())))
                    p.join()

            current_now = time()
            process_runtime = current_now - now

            if process_runtime < 10:
                sleep_for = (10 - process_runtime)
                logger.info('sleeping for %.2f seconds due to low run time...' % sleep_for)
                sleep(sleep_for)
                try:
                    del sleep_for
                except:
                    logger.error('error :: failed to del sleep_for')
            try:
                del process_runtime
            except:
                logger.error('error :: failed to del process_runtime')
