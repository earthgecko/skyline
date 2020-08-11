from __future__ import division
import logging
try:
    from Queue import Empty
except:
    from queue import Empty
from redis import StrictRedis
from time import time, sleep, strftime, gmtime
from threading import Thread
from collections import defaultdict
from multiprocessing import Process, Manager, Queue
from msgpack import Unpacker, unpackb, packb
import os
from os import path, kill, getpid, system
from math import ceil
import traceback
import operator
import socket
import re
from sys import version_info
import os.path
import resource
from ast import literal_eval

import sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))
import settings
from skyline_functions import (
    send_graphite_metric, write_data_to_file, send_anomalous_metric_to, mkdir_p,
    filesafe_metricname,
    # @added 20170602 - Feature #2034: analyse_derivatives
    nonNegativeDerivative, strictly_increasing_monotonicity, in_list,
    # @added 20200506 - Feature #3532: Sort all time series
    sort_timeseries)

from alerters import trigger_alert
from algorithms_dev import run_selected_algorithm
# from skyline import algorithm_exceptions
from algorithm_exceptions import TooShort, Stale, Boring

try:
    send_algorithm_run_metrics = settings.ENABLE_ALGORITHM_RUN_METRICS
except:
    send_algorithm_run_metrics = False
if send_algorithm_run_metrics:
    from algorithms_dev import determine_median

# TODO if settings.ENABLE_CRUCIBLE: and ENABLE_PANORAMA
#    from spectrum import push_to_crucible

skyline_app = 'analyzer_dev'
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

LOCAL_DEBUG = True

class AnalyzerDev(Thread):
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
        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        if settings.REDIS_PASSWORD:
            self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        else:
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
        # @added 20180903 - Feature #2580: illuminance
        #                   Feature #1986: flux
        self.illuminance_datapoints = Manager().list()
        # @added 20190408 - Feature #2882: Mirage - periodic_check
        self.mirage_periodic_check_metrics = Manager().list()
        self.real_anomalous_metrics = Manager().list()

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
        # assigned_keys = range(300, 310)

        # Compile assigned metrics
        assigned_metrics = [unique_metrics[index] for index in assigned_keys]
        if LOCAL_DEBUG:
            logger.info('debug :: Memory usage spin_process after assigned_metrics: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

        # @added 20190410 - Feature #2916: ANALYZER_ENABLED setting
        if not ANALYZER_ENABLED:
            len_assigned_metrics = len(assigned_metrics)
            logger.info('ANALYZER_ENABLED is set to %s removing the %s assigned_metrics' % (
                str(ANALYZER_ENABLED), str(len_assigned_metrics)))
            assigned_metrics = []
            del unique_metrics

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
            logger.info(traceback.format_exc())
            logger.error('error :: failed to get assigned_metrics from Redis')

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

        # @added 20190408 - Feature #2882: Mirage - periodic_check
        # Add Mirage periodic checks so that Mirage is analysing each metric at
        # least once per hour.
        mirage_periodic_check_metric_list = []
        try:
            mirage_periodic_check_enabled = settings.MIRAGE_PERIODIC_CHECK
        except:
            mirage_periodic_check_enabled = False
        try:
            mirage_periodic_check_interval = settings.MIRAGE_PERIODIC_CHECK_INTERVAL
        except:
            mirage_periodic_check_interval = 3600
        mirage_periodic_check_interval_minutes = int(int(mirage_periodic_check_interval) / 60)
        if mirage_unique_metrics and mirage_periodic_check_enabled:
            mirage_unique_metrics_count = len(mirage_unique_metrics)
            # Mirage periodic checks are only done on declared namespaces as to
            # process all Mirage metrics periodically would probably create a
            # substantial load on Graphite and is probably not required only key
            # metrics should be analysed by Mirage periodically.
            periodic_check_mirage_metrics = []
            try:
                mirage_periodic_check_namespaces = settings.MIRAGE_PERIODIC_CHECK_NAMESPACES
            except:
                mirage_periodic_check_namespaces = []
            for namespace in mirage_periodic_check_namespaces:
                for metric_name in mirage_unique_metrics:
                    metric_namespace_elements = metric_name.split('.')
                    mirage_periodic_metric = False
                    for periodic_namespace in mirage_periodic_check_namespaces:
                        if not namespace in mirage_periodic_check_namespaces:
                            continue
                        periodic_namespace_namespace_elements = periodic_namespace.split('.')
                        elements_matched = set(metric_namespace_elements) & set(periodic_namespace_namespace_elements)
                        if len(elements_matched) == len(periodic_namespace_namespace_elements):
                            mirage_periodic_metric = True
                            break
                    if mirage_periodic_metric:
                        if not metric_name in periodic_check_mirage_metrics:
                            periodic_check_mirage_metrics.append(metric_name)

            periodic_check_mirage_metrics_count = len(periodic_check_mirage_metrics)
            logger.info(
                'there are %s known Mirage periodic metrics' % (
                    str(periodic_check_mirage_metrics_count)))
            for metric_name in periodic_check_mirage_metrics:
                try:
                    self.redis_conn.sadd('new.mirage.periodic_check.metrics.all', metric_name)
                except Exception as e:
                    logger.error('error :: could not add %s to Redis set new.mirage.periodic_check.metrics.all: %s' % (
                        metric_name, e))
            try:
                self.redis_conn.rename('mirage.periodic_check.metrics.all', 'mirage.periodic_check.metrics.all.old')
            except:
                pass
            try:
                self.redis_conn.rename('new.mirage.periodic_check.metrics.all', 'mirage.periodic_check.metrics.all')
            except:
                pass
            try:
                self.redis_conn.delete('mirage.periodic_check.metrics.all.old')
            except:
                pass

            if periodic_check_mirage_metrics_count > mirage_periodic_check_interval_minutes:
                mirage_periodic_checks_per_minute = periodic_check_mirage_metrics_count / mirage_periodic_check_interval_minutes
            else:
                mirage_periodic_checks_per_minute = 1
            logger.info(
                '%s Mirage periodic checks can be added' % (
                    str(int(mirage_periodic_checks_per_minute))))
            for metric_name in periodic_check_mirage_metrics:
                if len(mirage_periodic_check_metric_list) == int(mirage_periodic_checks_per_minute):
                    break
                base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                mirage_periodic_check_cache_key = 'mirage.periodic_check.%s' % base_name
                mirage_periodic_check_key = False
                try:
                    mirage_periodic_check_key = self.redis_conn.get(mirage_periodic_check_cache_key)
                except Exception as e:
                    logger.error('error :: could not query Redis for cache_key: %s' % e)
                if not mirage_periodic_check_key:
                    try:
                        key_created_at = int(time())
                        self.redis_conn.setex(
                            mirage_periodic_check_cache_key,
                            mirage_periodic_check_interval, key_created_at)
                        logger.info(
                            'created Mirage periodic_check Redis key - %s' % (mirage_periodic_check_cache_key))
                        mirage_periodic_check_metric_list.append(metric_name)
                        try:
                            self.redis_conn.sadd('new.mirage.periodic_check.metrics', metric_name)
                        except Exception as e:
                            logger.error('error :: could not add %s to Redis set new.mirage.periodic_check.metrics: %s' % (
                                metric_name, e))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error(
                            'error :: failed to create Mirage periodic_check Redis key - %s' % (mirage_periodic_check_cache_key))
            try:
                self.redis_conn.rename('mirage.periodic_check.metrics', 'mirage.periodic_check.metrics.old')
            except:
                pass
            try:
                self.redis_conn.rename('new.mirage.periodic_check.metrics', 'mirage.periodic_check.metrics')
            except:
                pass
            try:
                self.redis_conn.delete('mirage.periodic_check.metrics.old')
            except:
                pass
            mirage_periodic_check_metric_list_count = len(mirage_periodic_check_metric_list)
            logger.info(
                '%s Mirage periodic checks were added' % (
                    str(mirage_periodic_check_metric_list_count)))

        try:
            ionosphere_unique_metrics = list(self.redis_conn.smembers('ionosphere.unique_metrics'))
        except:
            ionosphere_unique_metrics = []

        # @added 20170602 - Feature #2034: analyse_derivatives
        # In order to convert monotonic, incrementing metrics to a deriative
        # metric
        try:
            derivative_metrics = list(self.redis_conn.smembers('derivative_metrics'))
        except:
            derivative_metrics = []
        try:
            non_derivative_metrics = list(self.redis_conn.smembers('non_derivative_metrics'))
        except:
            non_derivative_metrics = []
        # This is here to refresh the sets
        try:
            manage_derivative_metrics = self.redis_conn.get('analyzer.derivative_metrics_expiry')
        except Exception as e:
            if LOCAL_DEBUG:
                logger.error('error :: could not query Redis for analyzer.derivative_metrics_expiry key: %s' % str(e))
            manage_derivative_metrics = False

        # @added 20170901 - Bug #2154: Infrequent missing new_ Redis keys
        # If the analyzer.derivative_metrics_expiry is going to expire in the
        # next 60 seconds, just manage the derivative_metrics in the run as
        # there is an overlap some times where the key existed at the start of
        # the run but has expired by the end of the run.
        derivative_metrics_expiry_ttl = False
        if manage_derivative_metrics:
            try:
                derivative_metrics_expiry_ttl = self.redis_conn.ttl('analyzer.derivative_metrics_expiry')
                logger.info('the analyzer.derivative_metrics_expiry key ttl is %s' % str(derivative_metrics_expiry_ttl))
            except:
                logger.error('error :: could not query Redis for analyzer.derivative_metrics_expiry key: %s' % str(e))
            if derivative_metrics_expiry_ttl:
                if int(derivative_metrics_expiry_ttl) < 60:
                    logger.info('managing derivative_metrics as the analyzer.derivative_metrics_expiry key ttl is less than 60 with %s' % str(derivative_metrics_expiry_ttl))
                    manage_derivative_metrics = False
                    try:
                        self.redis_conn.delete('analyzer.derivative_metrics_expiry')
                        logger.info('deleted the Redis key analyzer.derivative_metrics_expiry')
                    except:
                        logger.error('error :: failed to delete Redis key :: analyzer.derivative_metrics_expiry')

        try:
            non_derivative_monotonic_metrics = settings.NON_DERIVATIVE_MONOTONIC_METRICS
        except:
            non_derivative_monotonic_metrics = []

        # @added 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        # Added Redis sets for Boring, TooShort and Stale
        redis_set_errors = 0

        # Distill timeseries strings into lists
        for i, metric_name in enumerate(assigned_metrics):
            self.check_if_parent_is_alive()

            # logger.info('analysing %s' % metric_name)

            try:
                raw_series = raw_assigned[i]
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)
            except:
                timeseries = []

            # @added 20200507 - Feature #3532: Sort all time series
            # To ensure that there are no unordered timestamps in the time
            # series which are artefacts of the collector or carbon-relay, sort
            # all time series by timestamp before analysis.
            original_timeseries = timeseries
            if original_timeseries:
                timeseries = sort_timeseries(original_timeseries)
                del original_timeseries

            # @added 20170602 - Feature #2034: analyse_derivatives
            # In order to convert monotonic, incrementing metrics to a deriative
            # metric
            known_derivative_metric = False
            unknown_deriv_status = True
            if metric_name in non_derivative_metrics:
                unknown_deriv_status = False
            if unknown_deriv_status:
                if metric_name in derivative_metrics:
                    known_derivative_metric = True
                    unknown_deriv_status = False
            # This is here to refresh the sets
            if not manage_derivative_metrics:
                unknown_deriv_status = True

            base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)

            # @added 20170617 - Bug #2050: analyse_derivatives - change in monotonicity
            # First check if it has its own Redis z.derivative_metric key
            # that has not expired
            derivative_metric_key = 'z.derivative_metric.%s' % str(base_name)

            if unknown_deriv_status:
                # @added 20170617 - Bug #2050: analyse_derivatives - change in monotonicity
                last_derivative_metric_key = False
                try:
                    last_derivative_metric_key = self.redis_conn.get(derivative_metric_key)
                except Exception as e:
                    logger.error('error :: could not query Redis for last_derivative_metric_key: %s' % e)

                # Determine if it is a strictly increasing monotonically metric
                # or has been in last FULL_DURATION via its z.derivative_metric
                # key
                if not last_derivative_metric_key:
                    is_strictly_increasing_monotonically = strictly_increasing_monotonicity(timeseries)
                    if is_strictly_increasing_monotonically:
                        try:
                            last_expire_set = int(time())
                            self.redis_conn.setex(
                                derivative_metric_key, settings.FULL_DURATION, last_expire_set)
                        except Exception as e:
                            logger.error('error :: could not set Redis derivative_metric key: %s' % e)
                else:
                    # Until the z.derivative_metric key expires, it is classed
                    # as such
                    is_strictly_increasing_monotonically = True

                skip_derivative = in_list(base_name, non_derivative_monotonic_metrics)
                if skip_derivative:
                    is_strictly_increasing_monotonically = False
                if is_strictly_increasing_monotonically:
                    known_derivative_metric = True
                    try:
                        self.redis_conn.sadd('derivative_metrics', metric_name)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis derivative_metrics set')
                    try:
                        self.redis_conn.sadd('new_derivative_metrics', metric_name)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis new_derivative_metrics set')
                else:
                    try:
                        self.redis_conn.sadd('non_derivative_metrics', metric_name)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis non_derivative_metrics set')
                    try:
                        self.redis_conn.sadd('new_non_derivative_metrics', metric_name)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis new_non_derivative_metrics set')
            if known_derivative_metric:
                try:
                    derivative_timeseries = nonNegativeDerivative(timeseries)
                    timeseries = derivative_timeseries
                except:
                    logger.error('error :: nonNegativeDerivative failed')

            # @added 20180903 - Feature #2580: illuminance
            #                   Feature #1986: flux
            try:
                illuminance_datapoint = timeseries[-1][1]
                if '.illuminance' not in metric_name:
                    self.illuminance_datapoints.append(illuminance_datapoint)
            except:
                pass

            try:
                anomalous, ensemble, datapoint = run_selected_algorithm(timeseries, metric_name)

                # @added 20190408 - Feature #2882: Mirage - periodic_check
                # Add for Mirage periodic - is really anomalous add to
                # real_anomalous_metrics and if in mirage_periodic_check_metric_list
                # add as anomalous
                if anomalous:
                    # @modified 20190412 - Bug #2932: self.real_anomalous_metrics not being populated correctly
                    #                      Feature #2882: Mirage - periodic_check
                    # self.real_anomalous_metrics.append(base_name)
                    base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    metric_timestamp = timeseries[-1][0]
                    metric = [datapoint, base_name, metric_timestamp]
                    self.real_anomalous_metrics.append(metric)
                if metric_name in mirage_periodic_check_metric_list:
                    self.mirage_periodic_check_metrics.append(base_name)
                    anomalous = True

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

            # It could have been deleted by the Roomba
            except TypeError:
                # logger.error('TypeError analysing %s' % metric_name)
                exceptions['DeletedByRoomba'] += 1
            except TooShort:
                # logger.error('TooShort analysing %s' % metric_name)
                exceptions['TooShort'] += 1
            except Stale:
                # logger.error('Stale analysing %s' % metric_name)
                exceptions['Stale'] += 1
            except Boring:
                # logger.error('Boring analysing %s' % metric_name)
                exceptions['Boring'] += 1
            except:
                # logger.error('Other analysing %s' % metric_name)
                exceptions['Other'] += 1
                logger.info(traceback.format_exc())

        # Add values to the queue so the parent process can collate
        for key, value in anomaly_breakdown.items():
            self.anomaly_breakdown_q.put((key, value))

        for key, value in exceptions.items():
            self.exceptions_q.put((key, value))

        spin_end = time() - spin_start
        logger.info('spin_process took %.2f seconds' % spin_end)

    def run(self):
        """
        Called when the process intializes.

        Determine if Redis is up and discover the number of `unique metrics`.

        Divide the `unique_metrics` between the number of `ANALYZER_PROCESSES`
        and assign each process a set of metrics to analyse for anomalies.

        Wait for the processes to finish.

        Process the Determine whether if any anomalous metrics require:\n
        * alerting on (and set `EXPIRATION_TIME` key in Redis for alert).\n
        * feeding to another module e.g. mirage.

        Populated the webapp json the anomalous_metrics details.

        Log the details about the run to the skyline log.

        Send skyline.analyzer metrics to `GRAPHITE_HOST`,
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
            if python_version == 2:
                # os.makedirs(settings.SKYLINE_TMP_DIR, 0750)
                os.makedirs(settings.SKYLINE_TMP_DIR, mode=0o755)
            if python_version == 3:
                os.makedirs(settings.SKYLINE_TMP_DIR, mode=0o750)

        # Initiate the algorithm timings if Analyzer is configured to send the
        # algorithm_breakdown metrics with ENABLE_ALGORITHM_RUN_METRICS
        algorithm_tmp_file_prefix = settings.SKYLINE_TMP_DIR + '/' + skyline_app + '.'
        algorithms_to_time = []
        if send_algorithm_run_metrics:
            algorithms_to_time = settings.ALGORITHMS

        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except:
                logger.error('skyline can\'t connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                sleep(10)
                # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                if settings.REDIS_PASSWORD:
                    self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                else:
                    self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                continue

            # Report app up
            self.redis_conn.setex(skyline_app, 120, now)

            # Discover unique metrics
            unique_metrics = list(self.redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))

            if len(unique_metrics) == 0:
                logger.info('no metrics in redis. try adding some - see README')
                sleep(10)
                continue

            # Using count files rather that multiprocessing.Value to enable metrics for
            # metrics for algorithm run times, etc
            for algorithm in algorithms_to_time:
                algorithm_count_file = algorithm_tmp_file_prefix + algorithm + '.count'
                algorithm_timings_file = algorithm_tmp_file_prefix + algorithm + '.timings'
                # with open(algorithm_count_file, 'a') as f:
                with open(algorithm_count_file, 'w') as f:
                    pass
                with open(algorithm_timings_file, 'w') as f:
                    pass

            # Spawn processes
            pids = []
            pid_count = 0
            for i in range(1, settings.ANALYZER_PROCESSES + 1):
                if i > len(unique_metrics):
                    logger.info('WARNING: skyline is set for more cores than needed.')
                    break

                p = Process(target=self.spin_process, args=(i, unique_metrics))
                pids.append(p)
                pid_count += 1
                logger.info('starting %s of %s spin_process/es' % (str(pid_count), str(settings.ANALYZER_PROCESSES)))
                p.start()

            # Send wait signal to zombie processes
            # for p in pids:
            #     p.join()
            # Self monitor processes and terminate if any spin_process has run
            # for longer than 180 seconds
            p_starts = time()
            while time() - p_starts <= 180:
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
                    p.terminate()
                    # p.join()

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

            # Push to panorama
#            if len(self.panorama_anomalous_metrics) > 0:
#                logger.info('to do - push to panorama')

            # Push to crucible
#            if len(self.crucible_anomalous_metrics) > 0:
#                logger.info('to do - push to crucible')

            # Write anomalous_metrics to static webapp directory

            # Using count files rather that multiprocessing.Value to enable metrics for
            # metrics for algorithm run times, etc
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
                    _median_algorithm_timing = determine_median(algorithm_timings_array)
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
                send_mertic_name = 'algorithm_breakdown.' + algorithm + '.timing.times_run'
                self.send_graphite_metric(send_mertic_name, '%d' % number_of_algorithm_timings)
                send_mertic_name = 'algorithm_breakdown.' + algorithm + '.timing.total_time'
                self.send_graphite_metric(send_mertic_name, '%.6f' % sum_of_algorithm_timings)
                send_mertic_name = 'algorithm_breakdown.' + algorithm + '.timing.median_time'
                self.send_graphite_metric(send_mertic_name, '%.6f' % median_algorithm_timing)

            # Log progress
            logger.info('seconds to run    :: %.2f' % (time() - now))
            logger.info('total metrics     :: %d' % len(unique_metrics))
            logger.info('total analyzed    :: %d' % (len(unique_metrics) - sum(exceptions.values())))
            logger.info('total anomalies   :: %d' % len(self.anomalous_metrics))
            logger.info('exception stats   :: %s' % exceptions)
            logger.info('anomaly breakdown :: %s' % anomaly_breakdown)

            # Log to Graphite
            self.send_graphite_metric('run_time', '%.2f' % (time() - now))
            self.send_graphite_metric('total_analyzed', '%.2f' % (len(unique_metrics) - sum(exceptions.values())))
            self.send_graphite_metric('total_anomalies', '%d' % len(self.anomalous_metrics))
            self.send_graphite_metric('total_metrics', '%d' % len(unique_metrics))
            for key, value in exceptions.items():
                send_metric = 'exceptions.%s' % key
                self.send_graphite_metric(send_metric, '%d' % value)
            for key, value in anomaly_breakdown.items():
                send_metric = 'anomaly_breakdown.%s' % key
                self.send_graphite_metric(send_metric, '%d' % value)

            # Check canary metric
            raw_series = self.redis_conn.get(settings.FULL_NAMESPACE + settings.CANARY_METRIC)
            if raw_series is not None:
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)

                # @added 20200507 - Feature #3532: Sort all time series
                # To ensure that there are no unordered timestamps in the time
                # series which are artefacts of the collector or carbon-relay, sort
                # all time series by timestamp before analysis.
                original_timeseries = timeseries
                if original_timeseries:
                    timeseries = sort_timeseries(original_timeseries)
                    del original_timeseries

                time_human = (timeseries[-1][0] - timeseries[0][0]) / 3600
                projected = 24 * (time() - now) / time_human

                logger.info('canary duration   :: %.2f' % time_human)
                self.send_graphite_metric('duration', '%.2f' % time_human)
                self.send_graphite_metric('projected', '%.2f' % projected)

            # Reset counters
            self.anomalous_metrics[:] = []

            # Sleep if it went too fast
            # if time() - now < 5:
            #    logger.info('sleeping due to low run time...')
            #    sleep(10)
            # @modified 20160504 - @earthgecko - development internal ref #1338, #1340)
            # Etsy's original if this was a value of 5 seconds which does
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
                # sleep_for = 60
                logger.info('sleeping for %.2f seconds due to low run time...' % sleep_for)
                sleep(sleep_for)
