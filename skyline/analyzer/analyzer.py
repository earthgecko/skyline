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
    filesafe_metricname,
    # @added 20170602 - Feature #2034: analyse_derivatives
    nonNegativeDerivative, strictly_increasing_monotonicity, in_list)

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

try:
    DO_NOT_ALERT_ON_STALE_METRICS = settings.DO_NOT_ALERT_ON_STALE_METRICS
except:
    DO_NOT_ALERT_ON_STALE_METRICS = []

# @added 20190410 - Feature #2916: ANALYZER_ENABLED setting
try:
    ANALYZER_ENABLED = settings.ANALYZER_ENABLED
except:
    ANALYZER_ENABLED = True
try:
    ALERT_ON_STALE_METRICS = settings.ALERT_ON_STALE_METRICS
except:
    ALERT_ON_STALE_METRICS = False

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

            try:
                raw_series = raw_assigned[i]
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)
            except:
                timeseries = []

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
                                # @added 20170306 - Feature #1960: ionosphere_layers
                                # Ionosphere layers require the timeseries at
                                # FULL_DURATION so if this is a Mirage and
                                # Ionosphere metric, Analyzer needs to provide
                                # the timeseries file for later (within 60
                                # seconds) analysis, however we want the data
                                # that triggered the anomaly, as before this was
                                # only created by Mirage if an alert was
                                # triggered, but Ionosphere layers now require
                                # this file before an alert is triggered
                                timeseries_dir = base_name.replace('.', '/')
                                training_dir = '%s/%s/%s' % (
                                    settings.IONOSPHERE_DATA_FOLDER, str(metric_timestamp),
                                    str(timeseries_dir))
                                if not os.path.exists(training_dir):
                                    mkdir_p(training_dir)
                                full_duration_in_hours = int(settings.FULL_DURATION) / 3600
                                ionosphere_json_file = '%s/%s.mirage.redis.%sh.json' % (
                                    training_dir, base_name,
                                    str(int(full_duration_in_hours)))
                                if not os.path.isfile(ionosphere_json_file):
                                    timeseries_json = str(timeseries).replace('[', '(').replace(']', ')')
                                    try:
                                        write_data_to_file(skyline_app, ionosphere_json_file, 'w', timeseries_json)
                                        logger.info('%s added Ionosphere Mirage %sh Redis data timeseries json file :: %s' % (
                                            skyline_app, str(int(full_duration_in_hours)), ionosphere_json_file))
                                    except:
                                        logger.info(traceback.format_exc())
                                        logger.error('error :: failed to add %s Ionosphere Mirage Redis data timeseries json file - %s' % (skyline_app, ionosphere_json_file))

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

                        # @added 20170403 - Feature #1994: Ionosphere training_dir keys
                        #                   Feature #2000: Ionosphere - validated
                        #                   Feature #1996: Ionosphere - matches page
                        # The addition of this key data could be done in
                        # skyline_function.py, however that would introduce
                        # Redis requirements in the send_anomalous_metric_to
                        # function, which is not desirable I think. So this is
                        # a non-KISS pattern that is replicated in mirage.py as
                        # well.
                        # Each training_dir and data set is now Redis keyed to increase efficiency
                        # in terms of disk I/O for ionosphere.py and making keyed data
                        # available for each training_dir data set so that transient matched data
                        # can be surfaced for the webapp along with directory paths, etc
                        ionosphere_training_data_key = 'ionosphere.training_data.%s.%s' % (str(metric_timestamp), base_name)
                        ionosphere_training_data_key_data = [
                            ['metric_timestamp', int(metric_timestamp)],
                            ['base_name', str(base_name)],
                            ['timeseries_dir', str(timeseries_dir)],
                            ['added_by', str(skyline_app)]
                        ]
                        try:
                            self.redis_conn.setex(
                                ionosphere_training_data_key,
                                settings.IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR,
                                # @modified 20190413 - Task #2824: Test redis-py upgrade
                                #                      Task #2926: Update dependencies
                                # redis-py 3.x only accepts user data as bytes, strings or
                                # numbers (ints, longs and floats).  All 2.X users should
                                # make sure that the keys and values they pass into redis-py
                                # are either bytes, strings or numbers. Use str
                                str(ionosphere_training_data_key_data))
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
            # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
            # Added Redis sets for Boring, TooShort and Stale
            except TooShort:
                exceptions['TooShort'] += 1
                try:
                    self.redis_conn.sadd('analyzer.too_short', base_name)
                except:
                    redis_set_errors += 1
            except Stale:
                exceptions['Stale'] += 1
                try:
                    self.redis_conn.sadd('analyzer.stale', base_name)
                except:
                    redis_set_errors += 1
            except Boring:
                exceptions['Boring'] += 1
                try:
                    self.redis_conn.sadd('analyzer.boring', base_name)
                except:
                    redis_set_errors += 1
            except:
                exceptions['Other'] += 1
                logger.info(traceback.format_exc())

        # @added 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        # Added Redis sets for Boring, TooShort and Stale
        if redis_set_errors > 0:
            logger.error('error :: failed to add some metric/s to a Redis analyzer.{boring,stale,too_short} set')
        else:
            # @added 20180523 - Feature #2378: Add redis auth to Skyline and rebrow
            # Added update time to Redis sets for Boring, TooShort and Stale
            try:
                updated_analyzer_sets_at = '_updated_at_%s' % str(strftime('%Y%m%d%H%M%S', gmtime()))
                self.redis_conn.sadd('analyzer.boring', updated_analyzer_sets_at)
                self.redis_conn.sadd('analyzer.stale', updated_analyzer_sets_at)
                self.redis_conn.sadd('analyzer.too_short', updated_analyzer_sets_at)
            except:
                redis_set_errors += 1

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
                    # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                    if settings.REDIS_PASSWORD:
                        self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                    else:
                        self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: Analyzer cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)

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
                    except:
                        logger.error('error :: could not query Redis to delete mirage.unique_metric set')
                    # @added 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                    # Added Redis sets for Boring, TooShort and Stale
                    try:
                        self.redis_conn.delete('analyzer.boring')
                        logger.info('deleted Redis analyzer.boring set to refresh')
                    except:
                        logger.info('no Redis set to delete - analyzer.boring')
                    try:
                        self.redis_conn.delete('analyzer.too_short')
                        logger.info('deleted Redis analyzer.too_short set to refresh')
                    except:
                        logger.info('no Redis set to delete - analyzer.too_short')
                    try:
                        stale_metrics = list(self.redis_conn.smembers('analyzer.stale'))
                    except:
                        stale_metrics = []
                    if stale_metrics:
                        try:
                            self.redis_conn.delete('analyzer.stale')
                            logger.info('deleted Redis analyzer.stale set to refresh')
                        except:
                            logger.info('no Redis set to delete - analyzer.stale')
                    # @added 20180807 - Feature #2492: alert on stale metrics
                    try:
                        self.redis_conn.delete('analyzer.alert_on_stale_metrics')
                        logger.info('deleted Redis analyzer.alert_on_stale_metrics set to refresh')
                    except:
                        logger.info('no Redis set to delete - analyzer.alert_on_stale_metrics')

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

            # @added 20180423 - Feature #2360: CORRELATE_ALERTS_ONLY
            #                   Branch #2270: luminosity
            # Add a Redis set of smtp_alerter_metrics for Luminosity to only
            # cross correlate on metrics with an alert setting
            for metric in self.smtp_alerter_metrics:
                self.redis_conn.sadd('new_analyzer.smtp_alerter_metrics', metric)
            try:
                self.redis_conn.rename('analyzer.smtp_alerter_metrics', 'analyzer.smtp_alerter_metrics.old')
            except:
                pass
            try:
                self.redis_conn.rename('new_analyzer.smtp_alerter_metrics', 'analyzer.smtp_alerter_metrics')
            except:
                pass
            try:
                self.redis_conn.delete('analyzer.smtp_alerter_metrics.old')
            except:
                pass

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
            context = 'Analyzer'
            try:
                ionosphere_alerts = list(self.redis_conn.scan_iter(match='ionosphere.analyzer.alert.*'))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to scan ionosphere.analyzer.alert.* from Redis')

            if not ionosphere_alerts:
                ionosphere_alerts = []

            # @added 20180914 - Bug #2594: Analyzer Ionosphere alert on Analyzer data point
            ionosphere_anomalous_metrics = []

            ionosphere_metric_alerts = []
            if len(ionosphere_alerts) != 0:
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
                        # @added 20180914 - Bug #2594: Analyzer Ionosphere alert on Analyzer data point
                        ionosphere_anomalous_metrics.append(anomalous_metric)

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
                        # @added 20180914 - Bug #2594: Analyzer Ionosphere alert on Analyzer data point
                        # Set each metric to the default Analyzer context
                        context = 'Analyzer'

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
                                # @modified 20181023 - Feature #2618: alert_slack
                                # SECOND_ORDER_RESOLUTION_FULL_DURATION = alert[3]
                                SECOND_ORDER_RESOLUTION_FULL_DURATION = int(alert[3])
                                mirage_metric = True
                                analyzer_metric = False
                                logger.info(
                                    'mirage check :: %s at %s hours - matched by %s' %
                                    (metric[1],
                                        str(SECOND_ORDER_RESOLUTION_FULL_DURATION),
                                        matched_by))
                                context = 'Mirage'
                            except:
                                mirage_metric = False
                                if LOCAL_DEBUG:
                                    logger.info('debug :: not Mirage metric - %s' % metric[1])

                        if mirage_metric:
                            # Write anomalous metric to test at second
                            # order resolution by Mirage to the check
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
                                logger.info(traceback.format_exc())
                                logger.error('error :: failed to add mirage.metrics Redis key - %s' % str(mirage_metric_cache_key))

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
                                    # @added 20190410 - Feature #2882: Mirage - periodic_check
                                    # Allow for even non Mirage metrics to have
                                    # periodic checks done.  If the Mirage
                                    # SECOND_ORDER_RESOLUTION_HOURS are not set
                                    # in the alert tuple default to 7 days, 168
                                    # hours
                                    try:
                                        use_hours_to_resolve = int(alert[3])
                                    except:
                                        use_hours_to_resolve = 168

                                    with open(anomaly_check_file, 'w') as fh:
                                        # metric_name, anomalous datapoint, hours to resolve, timestamp
                                        # @modified 20190410 - Feature #2882: Mirage - periodic_check
                                        # fh.write('metric = "%s"\nvalue = "%s"\nhours_to_resolve = "%s"\nmetric_timestamp = "%s"\n' % (metric[1], metric[0], alert[3], str(metric[2])))
                                        fh.write('metric = "%s"\nvalue = "%s"\nhours_to_resolve = "%s"\nmetric_timestamp = "%s"\n' % (metric[1], metric[0], str(use_hours_to_resolve), str(metric[2])))
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

                            # @added 20180914 - Bug #2594: Analyzer Ionosphere alert on Analyzer data point
                            # Due to ionosphere_alerts being added to the
                            # self.all_anomalous_metrics after Analyzer metrics
                            # here we need to ensure that we only alert on the
                            # last item for the metric in the list so that the
                            # alert is not sent out with any current
                            # anomaly data from the current Analyzer run, but
                            # with the data from the ionosphere_alerts item.
                            if context == 'Ionosphere':
                                for check_metric in ionosphere_anomalous_metrics:
                                    if metric[1] == check_metric[1]:
                                        if metric[2] != check_metric[2]:
                                            # If the timestamps do not match
                                            # then it is the list item from
                                            # the Analyzer anomaly, not the
                                            # Ionosphere list item
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
                # @modified 20190408 - Feature #2882: Mirage - periodic_check
                # Do not count Mirage periodic checks
                # len(self.anomalous_metrics)
                len(self.real_anomalous_metrics)
                anomalous_metrics_list_len = True
            except:
                logger.error('error :: failed to determine length of real_anomalous_metrics list')

            if anomalous_metrics_list_len:
                # @modified 20190408 - Feature #2882: Mirage - periodic_check
                # Do not count Mirage periodic checks
                # if len(self.anomalous_metrics) > 0:
                if len(self.real_anomalous_metrics) > 0:
                    filename = path.abspath(path.join(path.dirname(__file__), '..', settings.ANOMALY_DUMP))
                    try:
                        with open(filename, 'w') as fh:
                            # Make it JSONP with a handle_data() function
                            # @modified 20190408 - Feature #2882: Mirage - periodic_check
                            # Do not count Mirage periodic checks
                            # anomalous_metrics = list(self.anomalous_metrics)
                            anomalous_metrics = list(self.real_anomalous_metrics)
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

                # We del all variables that are floats as they become unique objects and
                # can result in what appears to be a memory leak, but is not, just the
                # way Python handles floats
                try:
                    del _sum_of_algorithm_timings
                except:
                    logger.error('error :: failed to del _sum_of_algorithm_timings')
                try:
                    del _median_algorithm_timing
                except:
                    logger.error('error :: failed to del _median_algorithm_timing')
                try:
                    del sum_of_algorithm_timings
                except:
                    logger.error('error :: failed to del sum_of_algorithm_timings')
                try:
                    del median_algorithm_timing
                except:
                    logger.error('error :: failed to del median_algorithm_timing')

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage in run after algorithm run times: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # @added 20180807 - Feature #2492: alert on stale metrics
            # @modified 20190410 - Feature #2916: ANALYZER_ENABLED setting
            # If Analyzer is not enabled do not alert on stale metrics
            # if settings.ALERT_ON_STALE_METRICS:
            if ALERT_ON_STALE_METRICS and ANALYZER_ENABLED:
                stale_metrics_to_alert_on = []
                alert_on_stale_metrics = []
                try:
                    alert_on_stale_metrics = list(self.redis_conn.smembers('analyzer.alert_on_stale_metrics'))
                    logger.info('alert_on_stale_metrics :: %s' % str(alert_on_stale_metrics))
                except:
                    alert_on_stale_metrics = []
                for metric_name in alert_on_stale_metrics:
                    metric_namespace_elements = metric_name.split('.')
                    process_metric = True
                    for do_not_alert_namespace in DO_NOT_ALERT_ON_STALE_METRICS:
                        if do_not_alert_namespace in metric_name:
                            process_metric = False
                            break
                        do_not_alert_namespace_namespace_elements = do_not_alert_namespace.split('.')
                        elements_matched = set(metric_namespace_elements) & set(do_not_alert_namespace_namespace_elements)
                        if len(elements_matched) == len(do_not_alert_namespace_namespace_elements):
                            process_metric = False
                            break

                    if not process_metric:
                        continue
                    base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    cache_key = 'last_alert.stale.%s' % (base_name)
                    last_alert = False
                    try:
                        last_alert = self.redis_conn.get(cache_key)
                    except Exception as e:
                        logger.error('error :: could not query Redis for cache_key: %s' % e)
                    if not last_alert:
                        stale_metrics_to_alert_on.append(base_name)
                        try:
                            # @modified 20180828 - Bug #2568: alert on stale metrics not firing
                            # Those expired in the year 2067
                            # self.redis_conn.setex(cache_key, int(time()), int(settings.STALE_PERIOD))
                            self.redis_conn.setex(cache_key, int(settings.STALE_PERIOD), int(time()))
                        except:
                            logger.error('error :: failed to add %s Redis key' % cache_key)
                if stale_metrics_to_alert_on:
                    alert = ('stale_metrics', 'stale_digest', settings.STALE_PERIOD)
                    metric = (len(stale_metrics_to_alert_on), stale_metrics_to_alert_on, int(time()))
                    context = 'Analyzer'
                    try:
                        trigger_alert(alert, metric, context)
                        logger.info('digest alerted on %s stale metrics' % str(len(stale_metrics_to_alert_on)))
                        logger.info('stale metrics :: %s' % str(stale_metrics_to_alert_on))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: could not send alert on stale digest email')
                    # @modified 20181117 - Feature #2492: alert on stale metrics
                    # Stop analyzer stalling if there is no
                    # stale_metrics_to_alert_on list to delete
                    try:
                        del stale_metrics_to_alert_on
                        del alert
                        del metric
                    except:
                        pass
                else:
                    logger.info('there are no stale metrics to alert on')

            run_time = time() - now
            total_metrics = str(len(unique_metrics))

            # @modified 20190410 - Feature #2916: ANALYZER_ENABLED setting
            # total_analyzed = str(len(unique_metrics) - sum(exceptions.values()))
            if ANALYZER_ENABLED:
                total_analyzed = str(len(unique_metrics) - sum(exceptions.values()))
            else:
                total_analyzed = '0'

            # @modified 20190408 - Feature #2882: Mirage - periodic_check
            # Do not count Mirage periodic checks
            # total_anomalies = str(len(self.anomalous_metrics))
            total_anomalies = str(len(self.real_anomalous_metrics))

            # @added 20190408 - Feature #2882: Mirage - periodic_check
            mirage_periodic_checks = str(len(self.mirage_periodic_check_metrics))

            # Log progress
            logger.info('seconds to run     :: %.2f' % run_time)
            logger.info('total metrics      :: %s' % total_metrics)
            logger.info('total analyzed     :: %s' % total_analyzed)
            logger.info('total anomalies    :: %s' % total_anomalies)
            logger.info('exception stats    :: %s' % exceptions)
            logger.info('anomaly breakdown  :: %s' % anomaly_breakdown)
            # @added 20190408 - Feature #2882: Mirage - periodic_check
            logger.info('mirage_periodic_checks  :: %s' % mirage_periodic_checks)

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

                # @added 20190408 - Feature #2882: Mirage - periodic_check
                logger.info('mirage_periodic_checks  :: %s' % mirage_periodic_checks)
                mirage_periodic_checks = str(len(self.mirage_periodic_check_metrics))
                logger.info('mirage_periodic_checks     :: %s' % mirage_periodic_checks)
                send_metric_name = '%s.mirage_periodic_checks' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, mirage_periodic_checks)

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

            projected = None
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

            # @added 20180903 - Feature #2580: illuminance
            #                   Feature #1986: flux
            try:
                illuminance_sum = sum(self.illuminance_datapoints)
                illuminance = str(illuminance_sum)
            except:
                illuminance = 0
            logger.info('illuminance        :: %s' % illuminance)
            send_metric_name = '%s.illuminance' % skyline_app_graphite_namespace
            # @modified 20181017 - Feature #2580: illuminance
            # Disabled for now as in concept phase
            # send_graphite_metric(skyline_app, send_metric_name, illuminance)

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
            # @added 20180903 - Feature #1986: illuminance
            self.illuminance_datapoints[:] = []
            # @added 20190408 - Feature #2882: Mirage - periodic_check
            self.mirage_periodic_check_metrics[:] = []
            self.real_anomalous_metrics[:] = []

            unique_metrics = []
            raw_series = None
            unpacker = None
            # We del all variables that are floats as they become unique objects and
            # can result in what appears to be a memory leak, but it is not, it
            # is just the way that Python handles floats
            # @modified 20170809 - Bug #2136: Analyzer stalling on no metrics
            #                      Task #1544: Add additional except handling to Analyzer
            #                      Task #1372: algorithm performance tuning
            # Added except to all del methods to prevent Analyzer stalling if
            # any of the float variables are not set, like projected if no
            # metrics were being set to Redis
            try:
                del timeseries[:]
            except:
                logger.error('error :: failed to del timeseries')
            try:
                del time_human
            except:
                logger.error('error :: failed to del time_human')
            try:
                del projected
            except:
                logger.error('error :: failed to del projected - Redis probably has no metrics')
            try:
                del run_time
            except:
                logger.error('error :: failed to del run_time')

            # @added 20180903 - Feature #2580: illuminance
            #                   Feature #1986: flux
            try:
                del illuminance_sum
            except:
                logger.error('error :: failed to del illuminance_sum')
            try:
                del illuminance
            except:
                logger.error('error :: failed to del illuminance')

            if LOCAL_DEBUG:
                logger.info('debug :: Memory usage after reset counters: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                logger.info('debug :: Memory usage before sleep: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # @added 20170602 - Feature #2034: analyse_derivatives
            # This is here to replace the sets
            key_timestamp = int(time())
            try:
                manage_derivative_metrics = self.redis_conn.get('analyzer.derivative_metrics_expiry')
            except Exception as e:
                if LOCAL_DEBUG:
                    logger.error('error :: could not query Redis for analyzer.derivative_metrics_expiry key: %s' % str(e))
            if not manage_derivative_metrics:
                try:
                    # @modified 20170901 - Bug #2154: Infrequent missing new_ Redis keys
                    # self.redis_conn.setex('analyzer.derivative_metrics_expiry', 120, key_timestamp)
                    self.redis_conn.setex('analyzer.derivative_metrics_expiry', 300, key_timestamp)
                    logger.info('set analyzer.derivative_metrics_expiry Redis key')
                except:
                    logger.error('error :: failed to set key :: analyzer.analyzer.derivative_metrics_expiry')
                try:
                    self.redis_conn.rename('new_derivative_metrics', 'derivative_metrics')
                except Exception as e:
                    logger.error('error :: could not rename Redis set new_derivative_metrics: %s' % str(e))
                try:
                    self.redis_conn.rename('new_non_derivative_metrics', 'non_derivative_metrics')
                except Exception as e:
                    logger.error('error :: could not rename Redis set new_non_derivative_metrics: %s' % str(e))

                live_at = 'skyline_set_as_of_%s' % str(key_timestamp)
                try:
                    self.redis_conn.sadd('derivative_metrics', live_at)
                except Exception as e:
                    logger.error('error :: could not add live at to Redis set derivative_metrics: %s' % str(e))
                try:
                    non_derivative_monotonic_metrics = settings.NON_DERIVATIVE_MONOTONIC_METRICS
                except:
                    non_derivative_monotonic_metrics = []
                try:
                    for non_derivative_monotonic_metric in non_derivative_monotonic_metrics:
                        self.redis_conn.sadd('non_derivative_metrics', non_derivative_monotonic_metric)
                    self.redis_conn.sadd('non_derivative_metrics', live_at)
                except Exception as e:
                    logger.error('error :: could not add live at to Redis set non_derivative_metrics: %s' % str(e))

            # Sleep if it went too fast
            # if time() - now < 5:
            #    logger.info('sleeping due to low run time...')
            #    sleep(10)
            # @modified 20160504 - @earthgecko - development internal ref #1338, #1340)
            # Etsy's original for this was a value of 5 seconds which does
            # not make skyline Analyzer very efficient in terms of installations
            # where 100s of 1000s of metrics are not being analyzed.  This lead to
            # Analyzer running over several metrics multiple times in a minute
            # and always working.  Therefore this was changed from if you took
            # less than 5 seconds to run only then sleep.  This behaviour
            # resulted in Analyzer analysing a few 1000 metrics in 9 seconds and
            # then doing it again and again in a single minute.  Therefore the
            # ANALYZER_OPTIMUM_RUN_DURATION setting was added to allow this to
            # self optimise in cases where skyline is NOT deployed to analyze
            # 100s of 1000s of metrics.  This relates to optimising performance
            # for any deployments with a few 1000 metrics and 60 second resolution
            # area, e.g. smaller and local deployments.
            process_runtime = time() - now
            analyzer_optimum_run_duration = settings.ANALYZER_OPTIMUM_RUN_DURATION
            if process_runtime < analyzer_optimum_run_duration:
                sleep_for = (analyzer_optimum_run_duration - process_runtime)
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
