from __future__ import division
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
from msgpack import Unpacker, packb
import os
from os import path, kill, getpid
from math import ceil
import traceback
import operator
import re
from sys import version_info
import os.path
import resource
from ast import literal_eval

import settings
from skyline_functions import (
    send_graphite_metric, write_data_to_file, send_anomalous_metric_to, mkdir_p,
    filesafe_metricname)

from alerters import trigger_alert
from algorithms import run_selected_algorithm
from algorithm_exceptions import TooShort, Stale, Boring

try:
    send_algorithm_run_metrics = settings.ENABLE_ALGORITHM_RUN_METRICS
except:
    send_algorithm_run_metrics = False
if send_algorithm_run_metrics:
    from algorithms import determine_array_median

# TODO if settings.ENABLE_CRUCIBLE: and ENABLE_PANORAMA
#    from spectrum import push_to_crucible

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

python_version = int(version_info[0])

this_host = str(os.uname()[1])

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)

LOCAL_DEBUG = False


class Analyzer(Thread):
    """
    The Analyzer class which controls the analyzer thread and spawned processes.
    """

    def __init__(self, parent_pid):
        """
        Initialize the Analyzer

        Create the :obj:`self.anomalous_metrics` list

        Create the :obj:`self.exceptions_q` queue

        Create the :obj:`self.anomaly_breakdown_q` queue

        """
        super(Analyzer, self).__init__()
        self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        self.anomalous_metrics = Manager().list()
        self.exceptions_q = Queue()
        self.anomaly_breakdown_q = Queue()
        # @modified 20160813 - Bug #1558: Memory leak in Analyzer
        # Not used
        # self.mirage_metrics = Manager().list()
        # @added 20160923 - Branch #922: Ionosphere
        self.mirage_metrics = Manager().list()
        self.ionosphere_metrics = Manager().list()
        # @added 20161119 - Branch #922: ionosphere
        #                   Task #1718: review.tsfresh
        # Send a breakdown of what metrics were sent to other apps
        self.sent_to_mirage = Manager().list()
        self.sent_to_crucible = Manager().list()
        self.sent_to_panorama = Manager().list()
        self.sent_to_ionosphere = Manager().list()
        # @added 20161229 - Feature #1830: Ionosphere alerts
        self.all_anomalous_metrics = Manager().list()
        # @added 20170108 - Feature #1830: Ionosphere alerts
        # Adding lists of smtp_alerter_metrics and non_smtp_alerter_metrics
        self.smtp_alerter_metrics = Manager().list()
        self.non_smtp_alerter_metrics = Manager().list()

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            exit(0)

    def spawn_alerter_process(self, alert, metric, context):
        """
        Spawn a process to trigger an alert.

        This is used by smtp alerters so that matplotlib objects are cleared
        down and the alerter cannot create a memory leak in this manner and
        plt.savefig keeps the object in memory until the process terminates.
        Seeing as data is being surfaced and processed in the alert_smtp
        context, multiprocessing the alert creation and handling prevents any
        memory leaks in the parent.

        Added 20160814 relating to:

        * Bug #1558: Memory leak in Analyzer
        * Issue #21 Memory leak in Analyzer see https://github.com/earthgecko/skyline/issues/21

        Parameters as per :py:func:`skyline.analyzer.alerters.trigger_alert
        <analyzer.alerters.trigger_alert>`

        """

        trigger_alert(alert, metric, context)

    def spin_process(self, i, unique_metrics):
        """
        Assign a bunch of metrics for a process to analyze.

        Multiple get the assigned_metrics to the process from Redis.

        For each metric:

        - unpack the `raw_timeseries` for the metric.
        - Analyse each timeseries against `ALGORITHMS` to determine if it is
          anomalous.
        - If anomalous add it to the :obj:`self.anomalous_metrics` list
        - Add what algorithms triggered to the :obj:`self.anomaly_breakdown_q`
          queue
        - If :mod:`settings.ENABLE_CRUCIBLE` is ``True``:

          - Add a crucible data file with the details about the timeseries and
            anomaly.
          - Write the timeseries to a json file for crucible.

        Add keys and values to the queue so the parent process can collate for:\n
        * :py:obj:`self.anomaly_breakdown_q`
        * :py:obj:`self.exceptions_q`
        """

        spin_start = time()
        logger.info('spin_process started')
        if LOCAL_DEBUG:
            logger.info('debug :: Memory usage spin_process start: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

        # TESTING removal of p.join() from p.terminate()
        # sleep(4)

        # @modified 20160801 - Adding additional exception handling to Analyzer
        # Check the unique_metrics list is valid
        try:
            len(unique_metrics)
        except:
            logger.error('error :: the unique_metrics list is not valid')
            logger.info(traceback.format_exc())
            logger.info('nothing to do, no unique_metrics')
            return

        # Discover assigned metrics
        keys_per_processor = int(ceil(float(len(unique_metrics)) / float(settings.ANALYZER_PROCESSES)))
        if i == settings.ANALYZER_PROCESSES:
            assigned_max = len(unique_metrics)
        else:
            assigned_max = min(len(unique_metrics), i * keys_per_processor)
        # Fix analyzer worker metric assignment #94
        # https://github.com/etsy/skyline/pull/94 @languitar:worker-fix
        assigned_min = (i - 1) * keys_per_processor
        assigned_keys = range(assigned_min, assigned_max)

        # Compile assigned metrics
        assigned_metrics = [unique_metrics[index] for index in assigned_keys]
        if LOCAL_DEBUG:
            logger.info('debug :: Memory usage spin_process after assigned_metrics: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

        # Check if this process is unnecessary
        if len(assigned_metrics) == 0:
            return

        # Multi get series
        # @modified 20160801 - Adding additional exception handling to Analyzer
        raw_assigned_failed = True
        try:
            raw_assigned = self.redis_conn.mget(assigned_metrics)
            raw_assigned_failed = False
            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage spin_process after raw_assigned: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        except:
            logger.error('error :: failed to get assigned_metrics from Redis')
            logger.info(traceback.format_exc())

        # Make process-specific dicts
        exceptions = defaultdict(int)
        anomaly_breakdown = defaultdict(int)

        # @added 20160803 - Adding additional exception handling to Analyzer
        if raw_assigned_failed:
            return

        # @added 20161119 - Branch #922: ionosphere
        #                   Task #1718: review.tsfresh
        # Determine the unique Mirage and Ionosphere metrics once, which are
        # used later to determine how Analyzer should handle/route anomalies
        try:
            mirage_unique_metrics = list(self.redis_conn.smembers('mirage.unique_metrics'))
        except:
            mirage_unique_metrics = []

        try:
            ionosphere_unique_metrics = list(self.redis_conn.smembers('ionosphere.unique_metrics'))
        except:
            ionosphere_unique_metrics = []

        # Distill timeseries strings into lists
        for i, metric_name in enumerate(assigned_metrics):
            self.check_if_parent_is_alive()

            try:
                raw_series = raw_assigned[i]
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)
            except:
                timeseries = []

            try:
                anomalous, ensemble, datapoint = run_selected_algorithm(timeseries, metric_name)

                # If it's anomalous, add it to list
                if anomalous:
                    base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    metric_timestamp = timeseries[-1][0]
                    metric = [datapoint, base_name, metric_timestamp]
                    self.anomalous_metrics.append(metric)

                    # Get the anomaly breakdown - who returned True?
                    triggered_algorithms = []
                    for index, value in enumerate(ensemble):
                        if value:
                            algorithm = settings.ALGORITHMS[index]
                            anomaly_breakdown[algorithm] += 1
                            triggered_algorithms.append(algorithm)

                    # @added 20170206 - Bug #1904: Handle non filesystem friendly metric names in check files
                    sane_metricname = filesafe_metricname(str(base_name))

                    # If Crucible or Panorama are enabled determine details
                    determine_anomaly_details = False
                    if settings.ENABLE_CRUCIBLE and settings.ANALYZER_CRUCIBLE_ENABLED:
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
                        metric_timestamp = str(int(timeseries[-1][0]))
                        from_timestamp = str(int(timeseries[1][0]))
                        timeseries_dir = base_name.replace('.', '/')

                    # @added 20161119 - Branch #922: ionosphere
                    #                   Task #1718: review.tsfresh
                    # Set defaults which can be used later to determine how
                    # Analyzer should handle/route anomalies
                    analyzer_metric = True
                    mirage_metric = False
                    # crucible_metric = False
                    # panorama_metric = False
                    ionosphere_metric = False
                    # send_to_panaroma = False
                    send_to_ionosphere = False

                    if metric_name in ionosphere_unique_metrics:
                        # @modified 20170101 - Feature #1830: Ionosphere alerts
                        # Correct rate limitining Ionosphere on last_alert cache key
                        # Do not set analyzer_metric to False until after last_alert
                        # key has been checked otherwise Ionosphere will get every
                        # anomaly and as it will not be rate limited by the alert
                        # cache key
                        # analyzer_metric = False
                        ionosphere_metric = True
                        send_to_ionosphere = True

                    if metric_name in mirage_unique_metrics:
                        analyzer_metric = False
                        ionosphere_metric = False
                        mirage_metric = True
                        send_to_ionosphere = False

                    # @added 20170108 - Feature #1830: Ionosphere alerts
                    # Only send smtp_alerter_metrics to Ionosphere
                    smtp_alert_enabled_metric = True
                    if base_name in self.non_smtp_alerter_metrics:
                        smtp_alert_enabled_metric = False

                    if ionosphere_enabled:
                        if analyzer_metric:
                            # We do not want send all anomalous metrics to
                            # Ionosphere if they are not being alerted on as
                            # they will be pointless they will have no alert if
                            # it is within the EXPIRATION_TIME and there will be
                            # no reference graphs from an alert for the user to
                            # action.
                            cache_key = 'last_alert.smtp.%s' % (base_name)
                            last_alert = False
                            try:
                                last_alert = self.redis_conn.get(cache_key)
                            except Exception as e:
                                logger.error('error :: could not query Redis for cache_key: %s' % e)

                            if not last_alert:
                                send_to_ionosphere = True
                            else:
                                send_to_ionosphere = False
                                if ionosphere_metric:
                                    logger.info('not sending to Ionosphere - alert key exists - %s' % (base_name))
                        else:
                            if mirage_metric:
                                logger.info('not sending to Ionosphere - Mirage metric - %s' % (base_name))
                                send_to_ionosphere = False

                    # @modified 20170108 - Feature #1830: Ionosphere alerts
                    # Only send smtp_alerter_metrics to Ionosphere
                    # if send_to_ionosphere:
                    if send_to_ionosphere and smtp_alert_enabled_metric:
                        if metric_name in ionosphere_unique_metrics:
                            logger.info('sending an ionosphere metric to Ionosphere - %s' % (base_name))
                        else:
                            logger.info('sending an analyzer metric to Ionosphere for training - %s' % (base_name))
                        try:
                            # @modified 20161228 Feature #1828: ionosphere - mirage Redis data features
                            # Added full_duration
                            # @modified 20170127 - Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
                            # Added ionosphere_parent_id, always zero from Analyzer and Mirage
                            ionosphere_parent_id = 0
                            send_anomalous_metric_to(
                                skyline_app, 'ionosphere', timeseries_dir,
                                metric_timestamp, base_name, str(datapoint),
                                from_timestamp, triggered_algorithms,
                                timeseries, str(settings.FULL_DURATION),
                                str(ionosphere_parent_id))
                            self.sent_to_ionosphere.append(base_name)
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: failed to send_anomalous_metric_to to ionosphere')

                    if ionosphere_metric:
                        analyzer_metric = False

                    # Only send Analyzer metrics
                    if analyzer_metric and settings.PANORAMA_ENABLED:
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
                            % (base_name, str(datapoint), from_timestamp,
                               metric_timestamp, str(settings.ALGORITHMS),
                               triggered_algorithms, skyline_app, source,
                               this_host, added_at)

                        # Create an anomaly file with details about the anomaly
                        panaroma_anomaly_file = '%s/%s.%s.txt' % (
                            settings.PANORAMA_CHECK_PATH, added_at,
                            sane_metricname)
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
                        # @modified 20160207 - Branch #922: Ionosphere
                        # Handle if all other apps are not enabled
                        other_app = 'none'
                        if mirage_metric:
                            other_app = 'Mirage'
                        if ionosphere_metric:
                            other_app = 'Ionosphere'
                        logger.info('not adding panorama anomaly file for %s - %s' % (other_app, metric))

                    # If Crucible is enabled - save timeseries and create a
                    # Crucible check
                    if settings.ENABLE_CRUCIBLE and settings.ANALYZER_CRUCIBLE_ENABLED:
                        crucible_anomaly_dir = settings.CRUCIBLE_DATA_FOLDER + '/' + timeseries_dir + '/' + metric_timestamp
                        if not os.path.exists(crucible_anomaly_dir):
                            mkdir_p(crucible_anomaly_dir)

                        # Note:
                        # The values are enclosed is single quoted intentionally
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
                               metric_timestamp, str(settings.ALGORITHMS),
                               triggered_algorithms, crucible_anomaly_dir,
                               skyline_app, metric_timestamp)

                        # @added 20170206 - Bug #1904: Handle non filesystem friendly metric names in check files
                        sane_metricname = filesafe_metricname(str(base_name))

                        # Create an anomaly file with details about the anomaly
                        crucible_anomaly_file = '%s/%s.txt' % (crucible_anomaly_dir, sane_metricname)
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
                        crucible_check_file = '%s/%s.%s.txt' % (settings.CRUCIBLE_CHECK_PATH, metric_timestamp, sane_metricname)
                        try:
                            write_data_to_file(
                                skyline_app, crucible_check_file, 'w',
                                crucible_anomaly_data)
                            logger.info('added crucible check :: %s,%s' % (base_name, metric_timestamp))
                            self.sent_to_crucible.append(base_name)
                        except:
                            logger.error('error :: failed to add crucible check file :: %s' % (crucible_check_file))
                            logger.info(traceback.format_exc())

            # It could have been deleted by the Roomba
            except TypeError:
                exceptions['DeletedByRoomba'] += 1
            except TooShort:
                exceptions['TooShort'] += 1
            except Stale:
                exceptions['Stale'] += 1
            except Boring:
                exceptions['Boring'] += 1
            except:
                exceptions['Other'] += 1
                logger.info(traceback.format_exc())

        # Add values to the queue so the parent process can collate
        for key, value in anomaly_breakdown.items():
            self.anomaly_breakdown_q.put((key, value))

        for key, value in exceptions.items():
            self.exceptions_q.put((key, value))

        if LOCAL_DEBUG:
            logger.info('debug :: Memory usage spin_process end: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

        spin_end = time() - spin_start
        logger.info('spin_process took %.2f seconds' % spin_end)
        return

    def run(self):
        """
        - Called when the process intializes.

        - Determine if Redis is up and discover the number of `unique metrics`.

        - Divide the `unique_metrics` between the number of `ANALYZER_PROCESSES`
          and assign each process a set of metrics to analyse for anomalies.

        - Wait for the processes to finish.

        - Determine whether if any anomalous metrics require:

            - Alerting on (and set `EXPIRATION_TIME` key in Redis for alert).
            - Feed to another module e.g. mirage.
            - Alert to syslog.

        - Populate the webapp json with the anomalous_metrics details.

        - Log the details about the run to the skyline analyzer log.

        - Send skyline.analyzer metrics to `GRAPHITE_HOST`
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

        if not os.path.exists(settings.SKYLINE_TMP_DIR):
            # @modified 20160803 - Adding additional exception handling to Analyzer
            try:
                mkdir_p(settings.SKYLINE_TMP_DIR)
            except:
                logger.error('error :: failed to create %s' % settings.SKYLINE_TMP_DIR)
                logger.info(traceback.format_exc())

        def smtp_trigger_alert(alert, metric, context):
            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            try:
                p = Process(target=self.spawn_alerter_process, args=(alert, metric, context))
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
                    logger.info('%s :: stopping spawn_trigger_alert - %s' % (skyline_app, str(p.is_alive())))
                    p.join()

        # Initiate the algorithm timings if Analyzer is configured to send the
        # algorithm_breakdown metrics with ENABLE_ALGORITHM_RUN_METRICS
        algorithm_tmp_file_prefix = settings.SKYLINE_TMP_DIR + '/' + skyline_app + '.'
        algorithms_to_time = []
        if send_algorithm_run_metrics:
            algorithms_to_time = settings.ALGORITHMS

        if LOCAL_DEBUG:
            logger.info('debug :: Memory usage in run after algorithms_to_time: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

        while 1:
            now = time()

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage before unique_metrics lookup: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except:
                logger.error('error :: Analyzer cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                logger.info(traceback.format_exc())
                sleep(10)
                try:
                    self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                except:
                    logger.error('error :: Analyzer cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                    logger.info(traceback.format_exc())

                continue

            # Report app up
            try:
                self.redis_conn.setex(skyline_app, 120, now)
            except:
                logger.error('error :: Analyzer could not update the Redis %s key' % skyline_app)
                logger.info(traceback.format_exc())

            # Discover unique metrics
            # @modified 20160803 - Adding additional exception handling to Analyzer
            try:
                unique_metrics = list(self.redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
            except:
                logger.error('error :: Analyzer could not get the unique_metrics list from Redis')
                logger.info(traceback.format_exc())
                sleep(10)
                continue

            if len(unique_metrics) == 0:
                logger.info('no metrics in redis. try adding some - see README')
                sleep(10)
                continue

            # @added 20160922 - Branch #922: Ionosphere
            # Add a Redis set of mirage.unique_metrics
            if settings.ENABLE_MIRAGE:
                # @added 20161229 - Feature #1830: Ionosphere alerts
                # Self manage the Mirage metrics set
                manage_mirage_unique_metrics = True
                manage_mirage_unique_metrics_key = []
                try:
                    manage_mirage_unique_metrics_key = list(self.redis_conn.get('analyzer.manage_mirage_unique_metrics'))
                except Exception as e:
                    if LOCAL_DEBUG:
                        logger.error('error :: could not query Redis for analyzer.manage_mirage_unique_metrics key: %s' % str(e))

                if not manage_mirage_unique_metrics_key:
                    manage_mirage_unique_metrics_key = []

                if len(manage_mirage_unique_metrics_key) > 0:
                    manage_mirage_unique_metrics = False

                mirage_unique_metrics = []
                try:
                    mirage_unique_metrics = list(self.redis_conn.smembers('mirage.unique_metrics'))
                    if LOCAL_DEBUG:
                        logger.info('debug :: fetched the mirage.unique_metrics Redis set')
                        logger.info('debug :: %s' % str(mirage_unique_metrics))
                except:
                    logger.info('failed to fetch the mirage.unique_metrics Redis set')
                    mirage_unique_metrics = []

                mirage_unique_metrics_count = len(mirage_unique_metrics)
                logger.info('mirage.unique_metrics Redis set count - %s' % str(mirage_unique_metrics_count))

                if manage_mirage_unique_metrics:
                    try:
                        logger.info('deleting Redis mirage.unique_metrics set to refresh')
                        self.redis_conn.delete('mirage.unique_metrics')
                        mirage_unique_metrics = []
                    except Exception as e:
                        logger.error('error :: could not query Redis for analyzer.manage_mirage_unique_metrics key: %s' % str(e))

                for alert in settings.ALERTS:
                    for metric in unique_metrics:
                        ALERT_MATCH_PATTERN = alert[0]
                        base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
                        METRIC_PATTERN = base_name
                        pattern_match = False
                        matched_by = 'not matched'
                        try:
                            alert_match_pattern = re.compile(ALERT_MATCH_PATTERN)
                            pattern_match = alert_match_pattern.match(METRIC_PATTERN)
                            if pattern_match:
                                matched_by = 'regex'
                                pattern_match = True
                        except:
                            pattern_match = False

                        if not pattern_match:
                            if alert[0] in base_name:
                                pattern_match = True
                                matched_by = 'substring'

                        if not pattern_match:
                            continue

                        mirage_metric = False
                        try:
                            SECOND_ORDER_RESOLUTION_FULL_DURATION = alert[3]
                            mirage_metric = True
                        except:
                            mirage_metric = False

                        if mirage_metric:
                            if metric not in mirage_unique_metrics:
                                try:
                                    self.redis_conn.sadd('mirage.unique_metrics', metric)
                                    if LOCAL_DEBUG:
                                        logger.info('debug :: added %s to mirage.unique_metrics' % metric)
                                except:
                                    if LOCAL_DEBUG:
                                        logger.error('error :: failed to add %s to mirage.unique_metrics set' % metric)
                                try:
                                    key_timestamp = int(time())
                                    self.redis_conn.setex('analyzer.manage_mirage_unique_metrics', 300, key_timestamp)
                                except:
                                    logger.error('error :: failed to set key :: analyzer.manage_mirage_unique_metrics')

                        # Do we use list or dict, which is better performance?
                        # With dict - Use EAFP (easier to ask forgiveness than
                        # permission)
                        try:
                            blah = dict["mykey"]
                            # key exists in dict
                        except:
                            # key doesn't exist in dict
                            blah = False

                # If they were refresh set them again
                if mirage_unique_metrics == []:
                    try:
                        mirage_unique_metrics = list(self.redis_conn.smembers('mirage.unique_metrics'))
                        mirage_unique_metrics_count = len(mirage_unique_metrics)
                        logger.info('mirage.unique_metrics Redis set count - %s' % str(mirage_unique_metrics_count))
                        if LOCAL_DEBUG:
                            logger.info('debug :: fetched the mirage.unique_metrics Redis set')
                            logger.info('debug :: %s' % str(mirage_unique_metrics))
                    except:
                        logger.info('failed to fetch the mirage.unique_metrics Redis set')
                        mirage_unique_metrics == []
                        mirage_unique_metrics_count = len(mirage_unique_metrics)
            # @modified 20160207 - Branch #922: Ionosphere
            # Handle if Mirage is not enabled
            else:
                mirage_unique_metrics = []
            # END Redis mirage.unique_metrics_set

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run after unique_metrics: %s (kb), using blah %s' % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss), str(blah))

            # @added 20170108 - Feature #1830: Ionosphere alerts
            # Adding lists of smtp_alerter_metrics and non_smtp_alerter_metrics
            # Timed this takes 0.013319 seconds on 689 unique_metrics
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
                                if base_name not in self.smtp_alerter_metrics:
                                    self.smtp_alerter_metrics.append(base_name)
                        except:
                            pattern_match = False

                        if not pattern_match:
                            # Match by substring
                            if alert[0] in base_name:
                                if base_name not in self.smtp_alerter_metrics:
                                    self.smtp_alerter_metrics.append(base_name)

                if base_name not in self.smtp_alerter_metrics:
                    if base_name not in self.smtp_alerter_metrics:
                        self.non_smtp_alerter_metrics.append(base_name)

            logger.info('smtp_alerter_metrics     :: %s' % str(len(self.smtp_alerter_metrics)))
            logger.info('non_smtp_alerter_metrics :: %s' % str(len(self.non_smtp_alerter_metrics)))

            # Using count files rather that multiprocessing.Value to enable metrics for
            # metrics for algorithm run times, etc
            for algorithm in algorithms_to_time:
                algorithm_count_file = algorithm_tmp_file_prefix + algorithm + '.count'
                algorithm_timings_file = algorithm_tmp_file_prefix + algorithm + '.timings'
                # with open(algorithm_count_file, 'a') as f:
                # @modified 20160803 - Adding additional exception handling to Analyzer
                try:
                    with open(algorithm_count_file, 'w') as f:
                        pass
                except:
                    logger.error('error :: could not create file %s' % algorithm_count_file)
                    logger.info(traceback.format_exc())

                try:
                    with open(algorithm_timings_file, 'w') as f:
                        pass
                except:
                    logger.error('error :: could not create file %s' % algorithm_timings_file)
                    logger.info(traceback.format_exc())

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run before removing algorithm_count_files and algorithm_timings_files: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

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
                logger.error('error :: failed to cleanup algorithm.error files')
                logger.info(traceback.format_exc())

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run before spawning processes: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            for i in range(1, settings.ANALYZER_PROCESSES + 1):
                if i > len(unique_metrics):
                    logger.info('WARNING: skyline is set for more cores than needed.')
                    break

                try:
                    p = Process(target=self.spin_process, args=(i, unique_metrics))
                    pids.append(p)
                    pid_count += 1
                    logger.info('starting %s of %s spin_process/es' % (str(pid_count), str(settings.ANALYZER_PROCESSES)))
                    p.start()
                    spawned_pids.append(p.pid)
                except:
                    logger.error('error :: failed to spawn process')
                    logger.info(traceback.format_exc())

            # Send wait signal to zombie processes
            # for p in pids:
            #     p.join()
            # Self monitor processes and terminate if any spin_process has run
            # for longer than 180 seconds - 20160512 @earthgecko
            p_starts = time()
            # TESTING p.join removal
            # while time() - p_starts <= 1:
            while time() - p_starts <= settings.MAX_ANALYZER_PROCESS_RUNTIME:
                if any(p.is_alive() for p in pids):
                    # Just to avoid hogging the CPU
                    sleep(.1)
                else:
                    # All the processes are done, break now.
                    time_to_run = time() - p_starts
                    logger.info('%s :: %s spin_process/es completed in %.2f seconds' % (skyline_app, str(settings.ANALYZER_PROCESSES), time_to_run))
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('%s :: timed out, killing all spin_process processes' % (skyline_app))
                for p in pids:
                    logger.info('%s :: killing spin_process process' % (skyline_app))
                    p.terminate()
                    # p.join()
                    logger.info('%s :: killed spin_process process' % (skyline_app))

            for p in pids:
                if p.is_alive():
                    logger.info('%s :: stopping spin_process - %s' % (skyline_app, str(p.is_alive())))
                    p.join()

            # Log the last reported error by any algorithms that errored in the
            # spawned processes from algorithms.py
            for completed_pid in spawned_pids:
                logger.info('spin_process with pid %s completed' % (str(completed_pid)))
                for algorithm in settings.ALGORITHMS:
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

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage after spin_process spawned processes finish: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Grab data from the queue and populate dictionaries
            exceptions = dict()
            anomaly_breakdown = dict()
            while 1:
                try:
                    key, value = self.anomaly_breakdown_q.get_nowait()
                    if key not in anomaly_breakdown.keys():
                        anomaly_breakdown[key] = value
                    else:
                        anomaly_breakdown[key] += value
                except Empty:
                    break

            while 1:
                try:
                    key, value = self.exceptions_q.get_nowait()
                    if key not in exceptions.keys():
                        exceptions[key] = value
                    else:
                        exceptions[key] += value
                except Empty:
                    break

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run before alerts: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # @added 20161228 - Feature #1830: Ionosphere alerts
            #                   Branch #922: Ionosphere
            # Bringing Ionosphere online - do not alert on Ionosphere metrics
            try:
                ionosphere_unique_metrics = list(self.redis_conn.smembers('ionosphere.unique_metrics'))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get ionosphere.unique_metrics from Redis')
                ionosphere_unique_metrics = []

            ionosphere_unique_metrics_count = len(ionosphere_unique_metrics)
            logger.info('ionosphere.unique_metrics Redis set count - %s' % str(ionosphere_unique_metrics_count))

            # @added 20161229 - Feature #1830: Ionosphere alerts
            # Determine if Ionosphere added any alerts to be sent
            try:
                for metric in self.anomalous_metrics:
                    self.all_anomalous_metrics.append(metric)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to add to self.all_anomalous_metrics')
            ionosphere_alerts = []
            ionosphere_alerts_returned = True
            context = 'Analyzer'
            try:
                ionosphere_alerts = list(self.redis_conn.scan_iter(match='ionosphere.analyzer.alert.*'))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to scan ionosphere.analyzer.alert.* from Redis')
                ionosphere_alerts_returned = False

            if not ionosphere_alerts:
                ionosphere_alerts = []
                ionosphere_alerts_returned = False

            ionosphere_metric_alerts = []
            if len(ionosphere_alerts) == 0:
                ionosphere_alerts_returned = False
            else:
                logger.info('Ionosphere alert/s requested :: %s' % str(ionosphere_alerts))
                for cache_key in ionosphere_alerts:
                    try:
                        alert_on = self.redis_conn.get(cache_key)
                        send_alert_for = literal_eval(alert_on)
                        value = float(send_alert_for[0])
                        base_name = str(send_alert_for[1])
                        metric_timestamp = int(float(send_alert_for[2]))
                        triggered_algorithms = send_alert_for[3]
                        anomalous_metric = [value, base_name, metric_timestamp]
                        self.all_anomalous_metrics.append(anomalous_metric)
                        for algorithm in triggered_algorithms:
                            key = algorithm
                            if key not in anomaly_breakdown.keys():
                                anomaly_breakdown[key] = 1
                            else:
                                anomaly_breakdown[key] += 1
                        self.redis_conn.delete(cache_key)
                        logger.info('alerting for Ionosphere on %s' % base_name)
                        ionosphere_metric_alerts.append(base_name)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add an Ionosphere anomalous_metric for %s' % cache_key)

            # Send alerts
            if settings.ENABLE_ALERTS:
                for alert in settings.ALERTS:
                    # @modified 20161229 - Feature #1830: Ionosphere alerts
                    # Handle alerting for Ionosphere
                    # for metric in self.anomalous_metrics:
                    for metric in self.all_anomalous_metrics:
                        pattern_match = False
                        # Absolute match
                        if str(metric[1]) == str(alert[0]):
                            pattern_match = True

                        if not pattern_match:
                            ALERT_MATCH_PATTERN = alert[0]
                            METRIC_PATTERN = metric[1]
                            matched_by = 'not matched'
                            try:
                                alert_match_pattern = re.compile(ALERT_MATCH_PATTERN)
                                pattern_match = alert_match_pattern.match(METRIC_PATTERN)
                                # if LOCAL_DEBUG:
                                #     logger.info(
                                #         'debug :: regex alert pattern matched :: %s %s' %
                                #         (alert[0], metric[1]))
                                matched_by = 'regex'
                            except:
                                pattern_match = False
                        # @modified 20160806 - Reintroduced the original
                        # substring matching after wildcard matching, to allow
                        # more flexibility
                        if not pattern_match:
                            if alert[0] in metric[1]:
                                pattern_match = True
                                # if LOCAL_DEBUG:
                                #     logger.info(
                                #         'debug :: substring alert pattern matched :: %s %s' %
                                #         (alert[0], metric[1]))
                                matched_by = 'substring'

                        if not pattern_match:
                            continue

                        mirage_metric = False
                        analyzer_metric = True
                        cache_key = 'last_alert.%s.%s' % (alert[1], metric[1])

                        if settings.ENABLE_MIRAGE:
                            try:
                                SECOND_ORDER_RESOLUTION_FULL_DURATION = alert[3]
                                mirage_metric = True
                                analyzer_metric = False
                                logger.info(
                                    'mirage check :: %s at %s hours - matched by %s' %
                                    (metric[1],
                                        str(SECOND_ORDER_RESOLUTION_FULL_DURATION),
                                        matched_by))
                            except:
                                mirage_metric = False
                                if LOCAL_DEBUG:
                                    logger.info('debug :: not Mirage metric - %s' % metric[1])

                        if mirage_metric:
                            # Write anomalous metric to test at second
                            # order resolution by crucible to the check
                            # file
                            if LOCAL_DEBUG:
                                logger.info(
                                    'debug :: Memory usage in run before writing mirage check file: %s (kb)' %
                                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                            # @added 20160815 - Analyzer also alerting on Mirage metrics now #22
                            # With the reintroduction of the original substring
                            # matching it become evident that Analyzer could
                            # alert on a Mirage metric via a match on a parent
                            # namespace pattern later in the ALERTS tuples.
                            # If it is a Mirage metric, we add a mirage.metrics
                            # key for Analyzer to check in the analyzer_metric
                            # step below.  This allows us to wildcard and
                            # substring match, but the mirage.metrics keys act
                            # as a dynamic SKIP_LIST for Analyzer
                            mirage_metric_cache_key = 'mirage.metrics.%s' % metric[1]
                            try:
                                self.redis_conn.setex(mirage_metric_cache_key, alert[2], packb(metric[0]))
                            except:
                                logger.error('error :: failed to add mirage.metrics Redis key')

                            # metric_timestamp = int(time())
                            # anomaly_check_file = '%s/%s.%s.txt' % (settings.MIRAGE_CHECK_PATH, metric_timestamp, metric[1])
                            # @added 20170206 - Bug #1904: Handle non filesystem friendly metric names in check files
                            anomaly_check_file = None
                            try:
                                sane_metricname = filesafe_metricname(str(metric[1]))
                                anomaly_check_file = '%s/%s.%s.txt' % (settings.MIRAGE_CHECK_PATH, str(int(metric[2])), sane_metricname)
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to determine anomaly_check_file')

                            if anomaly_check_file:
                                anomaly_check_file_created = False
                                try:
                                    with open(anomaly_check_file, 'w') as fh:
                                        # metric_name, anomalous datapoint, hours to resolve, timestamp
                                        fh.write('metric = "%s"\nvalue = "%s"\nhours_to_resolve = "%s"\nmetric_timestamp = "%s"\n' % (metric[1], metric[0], alert[3], str(metric[2])))
                                    anomaly_check_file_created = True
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to write anomaly_check_file')
                                if LOCAL_DEBUG:
                                    logger.info(
                                        'debug :: Memory usage in run after writing mirage check file: %s (kb)' %
                                        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

                                if anomaly_check_file_created:
                                    if python_version == 2:
                                        os.chmod(anomaly_check_file, 0644)
                                    if python_version == 3:
                                        os.chmod(anomaly_check_file, mode=0o644)
                                    if LOCAL_DEBUG:
                                        logger.info(
                                            'debug :: Memory usage in run after chmod mirage check file: %s (kb)' %
                                            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

                            logger.info('added mirage check :: %s,%s,%s' % (metric[1], metric[0], alert[3]))
                            try:
                                self.sent_to_mirage.append(metric[1])
                            except:
                                logger.info(traceback.format_exc())
                                logger.error(
                                    'error :: failed update self.sent_to_mirage.append with %s' %
                                    metric[1])

                            # Alert for analyzer if enabled
                            if settings.ENABLE_FULL_DURATION_ALERTS:
                                self.redis_conn.setex(cache_key, alert[2], packb(metric[0]))
                                logger.info('triggering alert ENABLE_FULL_DURATION_ALERTS :: %s %s via %s' % (metric[1], metric[0], alert[1]))
                                try:
                                    if alert[1] != 'smtp':
                                        trigger_alert(alert, metric, context)
                                    else:
                                        smtp_trigger_alert(alert, metric, context)
                                except:
                                    logger.error(
                                        'error :: failed to trigger_alert ENABLE_FULL_DURATION_ALERTS :: %s %s via %s' %
                                        (metric[1], metric[0], alert[1]))
                            else:
                                if LOCAL_DEBUG:
                                    logger.info('debug :: ENABLE_FULL_DURATION_ALERTS not enabled')
                            continue

                        # @added 20161228 - Feature #1830: Ionosphere alerts
                        #                   Branch #922: Ionosphere
                        # Bringing Ionosphere online - do alert on Ionosphere
                        # metrics if Ionosphere is up
                        metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(metric[1]))
                        if metric_name in ionosphere_unique_metrics:
                            ionosphere_up = False
                            try:
                                ionosphere_up = self.redis_conn.get('ionosphere')
                            except Exception as e:
                                logger.error('error :: could not query Redis for ionosphere key: %s' % str(e))
                            if ionosphere_up:
                                # @modified 20161229 - Feature #1830: Ionosphere alerts
                                # Do alert if Ionosphere created a Redis
                                # ionosphere.analyzer.alert key
                                if str(metric[1]) in ionosphere_metric_alerts:
                                    logger.info('alerting as in ionosphere_alerts - Ionosphere metric - %s' % str(metric[1]))
                                    context = 'Ionosphere'
                                else:
                                    logger.info('not alerting - Ionosphere metric - %s' % str(metric[1]))
                                    continue
                            else:
                                logger.error('error :: Ionosphere not report up')
                                logger.info('taking over alerting from Ionosphere if alert is matched on - %s' % str(metric[1]))

                        # @added 20161231 - Feature #1830: Ionosphere alerts
                        #                   Analyzer also alerting on Mirage metrics now #22
                        # Do not alert on Mirage metrics
                        if metric_name in mirage_unique_metrics:
                            logger.info('not alerting - skipping Mirage metric - %s' % str(metric[1]))
                            continue

                        if analyzer_metric:
                            # @added 20160815 - Analyzer also alerting on Mirage metrics now #22
                            # If the metric has a dynamic mirage.metrics key,
                            # skip it
                            mirage_metric_cache_key = 'mirage.metrics.%s' % metric[1]
                            mirage_metric_key = False
                            if settings.ENABLE_MIRAGE:
                                try:
                                    mirage_metric_key = self.redis_conn.get(mirage_metric_cache_key)
                                except Exception as e:
                                    logger.error('error :: could not query Redis for mirage_metric_cache_key: %s' % e)

                            if mirage_metric_key:
                                if LOCAL_DEBUG:
                                    logger.info('debug :: mirage metric key exists, skipping %s' % metric[1])
                                continue

                            try:
                                last_alert = self.redis_conn.get(cache_key)
                            except Exception as e:
                                logger.error('error :: could not query Redis for cache_key: %s' % e)
                                continue

                            if last_alert:
                                if str(metric[1]) in ionosphere_metric_alerts:
                                    logger.info('not alerting on ionosphere_alerts Ionosphere metric - last_alert key exists - %s' % str(metric[1]))
                                    logger.info('so alert resources will not be created for this ionosphere_alerts Ionosphere metric - %s' % str(metric[1]))
                                continue

                            try:
                                self.redis_conn.setex(cache_key, alert[2], packb(metric[0]))
                            except:
                                logger.error('error :: failed to set alert cache key :: %s' % (cache_key))
                            logger.info(
                                'triggering alert :: %s %s via %s - matched by %s' %
                                (metric[1], metric[0], alert[1], matched_by))
                            if LOCAL_DEBUG:
                                logger.info(
                                    'debug :: Memory usage in run before triggering alert: %s (kb)' %
                                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                            try:
                                if alert[1] != 'smtp':
                                    trigger_alert(alert, metric, context)
                                else:
                                    smtp_trigger_alert(alert, metric, context)
                                    if LOCAL_DEBUG:
                                        logger.info('debug :: smtp_trigger_alert spawned')
                                if LOCAL_DEBUG:
                                    logger.info(
                                        'debug :: Memory usage in run after triggering alert: %s (kb)' %
                                        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                            except:
                                logger.error('error :: trigger_alert - %s' % traceback.format_exc())
                                logger.error(
                                    'error :: failed to trigger_alert :: %s %s via %s' %
                                    (metric[1], metric[0], alert[1]))
            # @modified 20160207 - Branch #922: Ionosphere
            # Handle if alerts are not enabled
            else:
                logger.info('alerts not enabled')

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run after alerts: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Write anomalous_metrics to static webapp directory
            # @modified 20160818 - Issue #19: Add additional exception handling to Analyzer
            anomalous_metrics_list_len = False
            try:
                len(self.anomalous_metrics)
                anomalous_metrics_list_len = True
            except:
                logger.error('error :: failed to determine length of anomalous_metrics list')

            if anomalous_metrics_list_len:
                if len(self.anomalous_metrics) > 0:
                    filename = path.abspath(path.join(path.dirname(__file__), '..', settings.ANOMALY_DUMP))
                    try:
                        with open(filename, 'w') as fh:
                            # Make it JSONP with a handle_data() function
                            anomalous_metrics = list(self.anomalous_metrics)
                            anomalous_metrics.sort(key=operator.itemgetter(1))
                            fh.write('handle_data(%s)' % anomalous_metrics)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error(
                            'error :: failed to write anomalies to %s' %
                            str(filename))

            # Using count files rather that multiprocessing.Value to enable metrics for
            # metrics for algorithm run times, etc
            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run before algorithm run times: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            for algorithm in algorithms_to_time:
                algorithm_count_file = algorithm_tmp_file_prefix + algorithm + '.count'
                algorithm_timings_file = algorithm_tmp_file_prefix + algorithm + '.timings'

                try:
                    algorithm_count_array = []
                    with open(algorithm_count_file, 'r') as f:
                        for line in f:
                            value_string = line.replace('\n', '')
                            unquoted_value_string = value_string.replace("'", '')
                            float_value = float(unquoted_value_string)
                            algorithm_count_array.append(float_value)
                except:
                    algorithm_count_array = False

                if not algorithm_count_array:
                    continue

                number_of_times_algorithm_run = len(algorithm_count_array)
                logger.info(
                    'algorithm run count - %s run %s times' % (
                        algorithm, str(number_of_times_algorithm_run)))
                if number_of_times_algorithm_run == 0:
                    continue

                try:
                    algorithm_timings_array = []
                    with open(algorithm_timings_file, 'r') as f:
                        for line in f:
                            value_string = line.replace('\n', '')
                            unquoted_value_string = value_string.replace("'", '')
                            float_value = float(unquoted_value_string)
                            algorithm_timings_array.append(float_value)
                except:
                    algorithm_timings_array = False

                if not algorithm_timings_array:
                    continue

                number_of_algorithm_timings = len(algorithm_timings_array)
                logger.info(
                    'algorithm timings count - %s has %s timings' % (
                        algorithm, str(number_of_algorithm_timings)))

                if number_of_algorithm_timings == 0:
                    continue

                try:
                    _sum_of_algorithm_timings = sum(algorithm_timings_array)
                except:
                    logger.error("sum error: " + traceback.format_exc())
                    _sum_of_algorithm_timings = round(0.0, 6)
                    logger.error('error - sum_of_algorithm_timings - %s' % (algorithm))
                    continue

                sum_of_algorithm_timings = round(_sum_of_algorithm_timings, 6)
                # logger.info('sum_of_algorithm_timings - %s - %.16f seconds' % (algorithm, sum_of_algorithm_timings))

                try:
                    _median_algorithm_timing = determine_array_median(algorithm_timings_array)
                except:
                    _median_algorithm_timing = round(0.0, 6)
                    logger.error('error - _median_algorithm_timing - %s' % (algorithm))
                    continue
                median_algorithm_timing = round(_median_algorithm_timing, 6)
                # logger.info('median_algorithm_timing - %s - %.16f seconds' % (algorithm, median_algorithm_timing))

                logger.info(
                    'algorithm timing - %s - total: %.6f - median: %.6f' % (
                        algorithm, sum_of_algorithm_timings,
                        median_algorithm_timing))
                use_namespace = skyline_app_graphite_namespace + '.algorithm_breakdown.' + algorithm
                send_metric_name = use_namespace + '.timing.times_run'
                send_graphite_metric(skyline_app, send_metric_name, str(number_of_algorithm_timings))
                send_metric_name = use_namespace + '.timing.total_time'
                send_graphite_metric(skyline_app, send_metric_name, str(sum_of_algorithm_timings))
                send_metric_name = use_namespace + '.timing.median_time'
                send_graphite_metric(skyline_app, send_metric_name, str(median_algorithm_timing))

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run after algorithm run times: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            run_time = time() - now
            total_metrics = str(len(unique_metrics))
            total_analyzed = str(len(unique_metrics) - sum(exceptions.values()))
            total_anomalies = str(len(self.anomalous_metrics))

            # Log progress
            logger.info('seconds to run     :: %.2f' % run_time)
            logger.info('total metrics      :: %s' % total_metrics)
            logger.info('total analyzed     :: %s' % total_analyzed)
            logger.info('total anomalies    :: %s' % total_anomalies)
            logger.info('exception stats    :: %s' % exceptions)
            logger.info('anomaly breakdown  :: %s' % anomaly_breakdown)

            # Log to Graphite
            graphite_run_time = '%.2f' % run_time
            send_metric_name = skyline_app_graphite_namespace + '.run_time'
            send_graphite_metric(skyline_app, send_metric_name, graphite_run_time)

            send_metric_name = skyline_app_graphite_namespace + '.total_analyzed'
            send_graphite_metric(skyline_app, send_metric_name, total_analyzed)

            send_metric_name = skyline_app_graphite_namespace + '.total_anomalies'
            send_graphite_metric(skyline_app, send_metric_name, total_anomalies)

            send_metric_name = skyline_app_graphite_namespace + '.total_metrics'
            send_graphite_metric(skyline_app, send_metric_name, total_metrics)
            for key, value in exceptions.items():
                send_metric_name = '%s.exceptions.%s' % (skyline_app_graphite_namespace, key)
                send_graphite_metric(skyline_app, send_metric_name, str(value))
            for key, value in anomaly_breakdown.items():
                send_metric_name = '%s.anomaly_breakdown.%s' % (skyline_app_graphite_namespace, key)
                send_graphite_metric(skyline_app, send_metric_name, str(value))

            # @added 20161119 - Branch #922: ionosphere
            #                   Task #1718: review.tsfresh
            # Send a breakdown of what metrics were sent to other apps
            # @added 20161119 - Branch #922: ionosphere
            #                   Task #1718: review.tsfresh
            # Send a breakdown of what metrics were sent to other apps
            if settings.ENABLE_MIRAGE:
                try:
                    sent_to_mirage = str(len(self.sent_to_mirage))
                except:
                    sent_to_mirage = '0'
                logger.info('sent_to_mirage     :: %s' % sent_to_mirage)
                send_metric_name = '%s.sent_to_mirage' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, sent_to_mirage)
                try:
                    mirage_unique_metrics_count_str = str(mirage_unique_metrics_count)
                except:
                    mirage_unique_metrics_count_str = '0'
                logger.info('Mirage metrics     :: %s' % mirage_unique_metrics_count_str)
                send_metric_name = '%s.mirage_metrics' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, mirage_unique_metrics_count_str)

            if settings.ENABLE_CRUCIBLE and settings.ANALYZER_CRUCIBLE_ENABLED:
                try:
                    sent_to_crucible = str(len(self.sent_to_crucible))
                except:
                    sent_to_crucible = '0'
                logger.info('sent_to_crucible   :: %s' % sent_to_crucible)
                send_metric_name = '%s.sent_to_crucible' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, sent_to_crucible)

            if settings.PANORAMA_ENABLED:
                try:
                    sent_to_panorama = str(len(self.sent_to_panorama))
                except:
                    sent_to_panorama = '0'
                logger.info('sent_to_panorama   :: %s' % sent_to_panorama)
                send_metric_name = '%s.sent_to_panorama' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, sent_to_panorama)

            if settings.IONOSPHERE_ENABLED:
                try:
                    sent_to_ionosphere = str(len(self.sent_to_ionosphere))
                except:
                    sent_to_ionosphere = '0'
                logger.info('sent_to_ionosphere :: %s' % sent_to_ionosphere)
                send_metric_name = '%s.sent_to_ionosphere' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, sent_to_ionosphere)
                try:
                    ionosphere_unique_metrics_count_str = str(ionosphere_unique_metrics_count)
                except:
                    ionosphere_unique_metrics_count_str = '0'
                logger.info('Ionosphere metrics :: %s' % ionosphere_unique_metrics_count_str)
                send_metric_name = '%s.ionosphere_metrics' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, ionosphere_unique_metrics_count_str)

            # Check canary metric
            try:
                raw_series = self.redis_conn.get(settings.FULL_NAMESPACE + settings.CANARY_METRIC)
            except:
                logger.error('error :: failed to get CANARY_METRIC from Redis')
                raw_series = None

            if raw_series is not None:
                try:
                    unpacker = Unpacker(use_list=False)
                    unpacker.feed(raw_series)
                    timeseries = list(unpacker)
                    time_human = (timeseries[-1][0] - timeseries[0][0]) / 3600
                    projected = 24 * (time() - now) / time_human
                    logger.info('canary duration    :: %.2f' % time_human)
                except:
                    logger.error(
                        'error :: failed to unpack/calculate canary duration')

                send_metric_name = skyline_app_graphite_namespace + '.duration'
                send_graphite_metric(skyline_app, send_metric_name, str(time_human))

                send_metric_name = skyline_app_graphite_namespace + '.projected'
                send_graphite_metric(skyline_app, send_metric_name, str(projected))

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage before reset counters: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Reset counters
            self.anomalous_metrics[:] = []
            self.mirage_metrics[:] = []
            self.ionosphere_metrics[:] = []
            # @added 20161119 - Branch #922: ionosphere
            #                   Task #1718: review.tsfresh
            self.sent_to_mirage[:] = []
            self.sent_to_crucible[:] = []
            self.sent_to_panorama[:] = []
            self.sent_to_ionosphere[:] = []
            # @added 20161229 - Feature #1830: Ionosphere alerts
            self.all_anomalous_metrics[:] = []
            # @added 20170108 - Feature #1830: Ionosphere alerts
            # Adding lists of smtp_alerter_metrics and non_smtp_alerter_metrics
            self.smtp_alerter_metrics[:] = []
            self.non_smtp_alerter_metrics[:] = []

            unique_metrics = []
            raw_series = None
            del timeseries[:]
            unpacker = None
            # We del all variables that are floats as they become unique objects and
            # can result in what appears to be a memory leak, but in not, just the
            # way Python handles floats
            del time_human
            del projected
            del run_time

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage after reset counters: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                logger.info('debug :: Memory usage before sleep: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Sleep if it went too fast
            # if time() - now < 5:
            #    logger.info('sleeping due to low run time...')
            #    sleep(10)
            # @modified 20160504 - @earthgecko - development internal ref #1338, #1340)
            # Etsy's original for this was a value of 5 seconds which does
            # not make skyline Analyzer very efficient in terms of installations
            # where 100s of 1000s of metrics are being analyzed.  This lead to
            # Analyzer running over several metrics multiple time in a minute
            # and always working.  Therefore this was changed from if you took
            # less than 5 seconds to run only then sleep.  This behaviour
            # resulted in Analyzer analysing a few 1000 metrics in 9 seconds and
            # then doing it again and again in a single minute.  Therefore the
            # ANALYZER_OPTIMUM_RUN_DURATION setting was added to allow this to
            # self optimise in cases where skyline is NOT deployed to analyze
            # 100s of 1000s of metrics.  This relates to optimising performance
            # for any deployments in the few 1000s and 60 second resolution
            # area, e.g. smaller and local deployments.
            process_runtime = time() - now
            analyzer_optimum_run_duration = settings.ANALYZER_OPTIMUM_RUN_DURATION
            if process_runtime < analyzer_optimum_run_duration:
                sleep_for = (analyzer_optimum_run_duration - process_runtime)
                logger.info('sleeping for %.2f seconds due to low run time...' % sleep_for)
                sleep(sleep_for)
