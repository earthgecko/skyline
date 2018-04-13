from __future__ import division
import logging
import os
from os import kill, getpid
from sys import version_info
try:
    from Queue import Empty
except:
    from queue import Empty
from time import time, sleep
from threading import Thread
from multiprocessing import Process, Manager
from redis import StrictRedis
import traceback
import mysql.connector
from pymemcache.client.base import Client as pymemcache_Client

import settings
from skyline_functions import (mysql_select, send_graphite_metric)
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
    luminosity_processes = settings.LUMINOSITY_PROCESSES
except:
    logger.info('warning :: cannot determine LUMINOSITY_PROCESSES from settings' % skyline_app)
    # @modified 20180110 - Task #2266: Evaluate luminol for the luminosity branch
    # It is fast and lightweight
    # luminosity_processes = 2
    luminosity_processes = 1

skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)

max_age_seconds = settings.IONOSPHERE_CHECK_MAX_AGE

# Database configuration
config = {'user': settings.PANORAMA_DBUSER,
          'password': settings.PANORAMA_DBUSERPASS,
          'host': settings.PANORAMA_DBHOST,
          'port': settings.PANORAMA_DBPORT,
          'database': settings.PANORAMA_DATABASE,
          'raise_on_warnings': True}


LOCAL_DEBUG = False


class Luminosity(Thread):
    """
    The Luminosity class which controls the luminosity thread and spawned
    processes.
    """

    def __init__(self, parent_pid):
        """
        Initialize Luminosity

        Create the :obj:`redis_conn` a Redis client object
        Create the :obj:`self.anomalous_metrics` list
        Create the :obj:`correlations` list
        Create the :obj:`mysql_conn` MySQLConnection object
        Create the :obj:`memcache_client` a constructor that does not make a
        connection to memcached. The first call to a method on the object will
        do that.

        """
        super(Luminosity, self).__init__()
        self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        self.anomalous_metrics = Manager().list()
        self.correlations = Manager().list()
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
            exit(0)

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
                self.memcache_client.set(memcache_key, str(anomaly_id))
                logger.info('processed - set the memcache key - %s - %s' % (memcache_key, str(anomaly_id)))
            except:
                logger.error('error :: failed to set  the memcache key - %s - %s' % (memcache_key, str(anomaly_id)))
            try:
                self.memcache_client.close()
            except:
                logger.error('error :: failed to close memcache_client')

        try:
            base_name, anomaly_timestamp, anomalies, correlated_metrics, correlations, sorted_correlations = process_correlations(i, anomaly_id)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: processing correlations')

        metrics_str = ''
        for metric_name in correlated_metrics:
            if metrics_str == '':
                new_metrics_str = "'%s'" % metric_name
            else:
                new_metrics_str = "%s,'%s'" % (metrics_str, metric_name)
            metrics_str = new_metrics_str
        metrics_str

        query = 'SELECT id,metric FROM metrics WHERE metric in (%s)' % str(metrics_str)
        try:
            results = mysql_select(skyline_app, query)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: querying MySQL - SELECT id,metric FROM metrics WHERE metric in (%s)' % str(metrics_str))

        correlated_metrics_list = []
        for metric_id, metric in results:
            correlated_metrics_list.append([int(metric_id), str(metric)])
        if sorted_correlations:
            luminosity_correlations = []
            for metric, coefficient, shifted, shifted_coefficient in sorted_correlations:
                for metric_id, metric_name in correlated_metrics_list:
                    if metric == metric_name:
                        if shifted < 2:
                            luminosity_correlations.append([anomaly_id, int(metric_id), coefficient, shifted, shifted_coefficient])
            first_value_not_added = True
            values_string = 'INSERT INTO luminosity (anomaly_id, metric_id, coefficient, shifted, shifted_coefficient) VALUES '
            for anomaly_id, metric_id, coefficient, shifted, shifted_coefficient in luminosity_correlations:
                ins_values = '(%s,%s,%s,%s,%s)' % (str(anomaly_id),
                                                   str(metric_id),
                                                   str(round(coefficient, 6)),
                                                   str(shifted),
                                                   str(round(shifted_coefficient, 6)))
                if first_value_not_added:
                    first_value_not_added = False
                    values_string = 'INSERT INTO luminosity (anomaly_id, metric_id, coefficient, shifted, shifted_coefficient) VALUES %s' % ins_values
                else:
                    new_values_string = '%s,%s' % (values_string, ins_values)
                    values_string = new_values_string
            new_values_string = '%s;' % values_string
            values_string = new_values_string
            logger.info('debug insert string :: %s' % str(values_string))
            # 'INSERT INTO luminosity (anomaly_id, metric_id, coefficient, shifted, shifted_coefficient) VALUES (68882,619,1.0,0,1.0),...,(68882,489,1.0,0,1.0);'
            # Needs a mysql_insert not SQLAlchemy

        if sorted_correlations:
            return True
        else:
            return False

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
                if ENABLE_LUMINOSITY_DEBUG:
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

            """
            Determine if any new anomalies have been added
            """
            while True:
                process_anomaly_id = None
                last_processed_anomaly_id = None
                # Check memcached before MySQL
                if settings.MEMCACHE_ENABLED:
                    try:
                        last_processed_anomaly_id_key = '%s.last_processed.anomaly_id' % (skyline_app)
                        last_processed_anomaly_id = self.memcache_client.get(last_processed_anomaly_id_key)
                        # if memcache does not have the key the response to the
                        # client is None, it does not except
                        if last_processed_anomaly_id is not None:
                            logger.info('last_processed_anomaly_id found in memcache - %s' % str(last_processed_anomaly_id))
                    except:
                        logger.error('error :: failed to get %s from memcache' % last_processed_anomaly_id_key)
                    try:
                        self.memcache_client.close()
                    except:
                        logger.error('error :: failed to close memcache_client')
                    if last_processed_anomaly_id:
                        logger.info('using memcache %s key data - %s' % (last_processed_anomaly_id_key, str(last_processed_anomaly_id)))

                if not last_processed_anomaly_id:
                    # Check MySQL
                    now = int(time())
                    after = now - 600
                    query = 'SELECT * FROM anomalies WHERE anomaly_timestamp > \'%s\'' % str(after)  # nosec
                    results = None
                    try:
                        results = mysql_select(skyline_app, query)
                    except:
                        logger.error('error :: MySQL quey failed - %s' % query)
                    if results:
                        process_anomaly_id = int(results[0][0])
                        logger.info('found new anomaly id to process from the DB - %s' % str(process_anomaly_id))
                    else:
                        logger.info('no new anomalies in the anomalies table')
                else:
                    query = 'SELECT * FROM anomalies WHERE id > \'%s\'' % str(last_processed_anomaly_id)  # nosec
                    results = None
                    try:
                        results = mysql_select(skyline_app, query)
                    except:
                        logger.error('error :: MySQL quey failed - %s' % query)
                    if results:
                        try:
                            process_anomaly_id = int(results[0][0])
                            logger.info('found the next new anomaly id to process from the DB- %s' % str(process_anomaly_id))
                            last_processed_anomaly_id = process_anomaly_id - 1
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: from query - %s' % query)
                    else:
                        logger.info('no new anomalies in the anomalies table')

                if isinstance(last_processed_anomaly_id, int):
                    if isinstance(process_anomaly_id, int):
                        if last_processed_anomaly_id == process_anomaly_id:
                            logger.info('anomaly id already processed - %s' % str(process_anomaly_id))
                            process_anomaly_id = None

                if not process_anomaly_id:
                    logger.info('sleeping 20 no anomalies to correlate')
                    sleep(20)
                    up_now = time()
                    # Report app up
                    try:
                        self.redis_conn.setex(skyline_app, 120, up_now)
                        logger.info('updated Redis key for %s up' % skyline_app)
                    except:
                        logger.error('error :: failed to update Redis key for %s up' % skyline_app)

                cache_key = '%s.sent_graphite_metrics' % skyline_app
                redis_sent_graphite_metrics = False
                try:
                    redis_sent_graphite_metrics = self.redis_conn.get(cache_key)
                except Exception as e:
                    logger.error('error :: could not query Redis for key %s: %s' % (cache_key, e))

                # Flush metrics to Graphite
                if not redis_sent_graphite_metrics:
                    try:
                        total_anomalies = str(len(self.anomalous_metrics))
                    except:
                        total_anomalies = '0'
                    logger.info('total_anomalies    :: %s' % total_anomalies)
                    send_metric_name = '%s.total_anomalies' % skyline_app_graphite_namespace
                    send_graphite_metric(skyline_app, send_metric_name, total_anomalies)

                    try:
                        correlations = str(len(self.correlations))
                    except:
                        correlations = '0'
                    logger.info('correlations       :: %s' % correlations)
                    send_metric_name = '%s.correlations' % skyline_app_graphite_namespace
                    send_graphite_metric(skyline_app, send_metric_name, correlations)

                    sent_graphite_metrics_now = int(time())
                    try:
                        self.redis_conn.setex(cache_key, 59, sent_graphite_metrics_now)
                        logger.info('updated Redis key - %s' % cache_key)
                    except:
                        logger.error('error :: failed to update Redis key - %s up' % cache_key)

                    # Reset lists
                    self.anomalous_metrics[:] = []
                    self.correlations[:] = []

                if process_anomaly_id:
                    break

            # Spawn process
            logger.info('spawning processes to correlate anomaly id %s' % str(process_anomaly_id))
            pids = []
            spawned_pids = []
            pid_count = 0
            now = time()
            for i in range(1, luminosity_processes + 1):
                try:
                    p = Process(target=self.spin_process, args=(i, process_anomaly_id))
                    pids.append(p)
                    pid_count += 1
                    logger.info(
                        'starting %s of %s spin_process/es' % (
                            str(pid_count),
                            str(luminosity_processes)))
                    p.start()
                    spawned_pids.append(p.pid)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to start spin_process')
                    continue

            # Self monitor processes and terminate if any spin_process has run
            # for to long
            p_starts = time()
            while time() - p_starts <= 60:
                if any(p.is_alive() for p in pids):
                    # Just to avoid hogging the CPU
                    sleep(.1)
                else:
                    # All the processes are done, break now.
                    time_to_run = time() - p_starts
                    logger.info(
                        '%s spin_process completed in %.2f seconds' % (
                            str(luminosity_processes),
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
