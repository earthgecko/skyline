import logging
from Queue import Empty
from redis import StrictRedis
from time import time, sleep
from threading import Thread
from collections import defaultdict
from multiprocessing import Process, Manager, Queue
from msgpack import Unpacker, unpackb, packb
from os import path, kill, getpid, system
from math import ceil
import traceback
import operator
import socket
import settings
import re
# imports required for surfacing graphite JSON formatted timeseries for use in
# mirage
import json
import sys
import requests
import urlparse
import os
import errno
import imp
from os import listdir
import datetime

from alerters import trigger_alert
from negaters import trigger_negater
from algorithms import run_selected_algorithm
from algorithm_exceptions import *
from os.path import dirname, join, abspath, isfile

logger = logging.getLogger("MirageLog")

try:
    SERVER_METRIC_PATH = settings.SERVER_METRICS_NAME + '.'
except:
    SERVER_METRIC_PATH = ''


class Mirage(Thread):
    def __init__(self, parent_pid):
        """
        Initialize the Mirage
        """
        super(Mirage, self).__init__()
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        self.anomalous_metrics = Manager().list()
        self.mirage_exceptions_q = Queue()
        self.mirage_anomaly_breakdown_q = Queue()
        self.not_anomalous_metrics = Manager().list()
        self.metric_variables = Manager().list()

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            exit(0)

    def mkdir_p(self, path):
        try:
            os.makedirs(path)
            return True
        # Python >2.5
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def surface_graphite_metric_data(self, metric_name, graphite_from, graphite_until):
        # We use absolute time so that if there is a lag in mirage the correct
        # timeseries data is still surfaced relevant to the anomalous datapoint
        # timestamp
        if settings.GRAPHITE_PORT != '':
            url = settings.GRAPHITE_PROTOCOL + '://' + settings.GRAPHITE_HOST + ':' + settings.GRAPHITE_PORT + '/render/?from=' + graphite_from + '&until=' + graphite_until + '&target=' + metric_name + '&format=json'
        else:
            url = settings.GRAPHITE_PROTOCOL + '://' + settings.GRAPHITE_HOST + '/render/?from=' + graphite_from + '&until=' + graphite_until + '&target=' + metric_name + '&format=json'
        r = requests.get(url)
        js = r.json()
        datapoints = js[0]['datapoints']

        converted = []
        for datapoint in datapoints:
            try:
                new_datapoint = [float(datapoint[1]), float(datapoint[0])]
                converted.append(new_datapoint)
            except:
                continue

        parsed = urlparse.urlparse(url)
        target = urlparse.parse_qs(parsed.query)['target'][0]

        metric_data_folder = settings.MIRAGE_DATA_FOLDER + "/" + target
        self.mkdir_p(metric_data_folder)
        with open(metric_data_folder + "/" + target + '.json', 'w') as f:
            f.write(json.dumps(converted))
            f.close()
            return True

        return False

    def load_metric_vars(self, filename):
        if os.path.isfile(filename) == True:
            f = open(filename)
            global metric_vars
            metric_vars = imp.load_source('metric_vars', '', f)
            f.close()
            return True

        return False

    def spin_process(self, i, run_timestamp):
        """
        Assign a metric for a process to analyze.
        """

        # Discover metric to analyze
        metric_var_files = [f for f in listdir(settings.MIRAGE_CHECK_PATH) if isfile(join(settings.MIRAGE_CHECK_PATH, f))]

        # Check if this process is unnecessary
        if len(metric_var_files) == 0:
            return

        metric_var_files_sorted = sorted(metric_var_files)
        metric_check_file = settings.MIRAGE_CHECK_PATH + "/" + metric_var_files_sorted[0]

        # Load metric variables
        self.load_metric_vars(metric_check_file)

        # Test metric variables
        if len(metric_vars.metric) == 0:
            return
        else:
            metric = metric_vars.metric
            metric_name = ['metric_name', metric_vars.metric]
            self.metric_variables.append(metric_name)
        if len(metric_vars.value) == 0:
            return
        else:
            metric_value = ['metric_value', metric_vars.value]
            self.metric_variables.append(metric_value)
        if len(metric_vars.hours_to_resolve) == 0:
            return
        else:
            hours_to_resolve = ['hours_to_resolve', metric_vars.hours_to_resolve]
            self.metric_variables.append(hours_to_resolve)
        if len(metric_vars.metric_timestamp) == 0:
            return
        else:
            metric_timestamp = ['metric_timestamp', metric_vars.metric_timestamp]
            self.metric_variables.append(metric_timestamp)

        # Ignore any metric check with a timestamp greater than 10 minutes ago
        int_metric_timestamp = int(metric_vars.metric_timestamp)
        int_run_timestamp = int(run_timestamp)
        metric_timestamp_age = int_run_timestamp - int_metric_timestamp
        if metric_timestamp_age > settings.MIRAGE_STALE_SECONDS:
            logger.info('stale check       :: %s check request is %s seconds old - discarding' % (metric_vars.metric, metric_timestamp_age))
            # Remove metric check file
#            try:
#                os.remove(metric_check_file)
#            except OSError:
#                pass
#            return
            if os.path.exists(metric_check_file):
                os.remove(metric_check_file)
                logger.info('removed %s' % (metric_check_file))
            else:
                logger.info('could not remove %s' % (metric_check_file))

        # Calculate hours second order resolution to seconds
        second_order_resolution_seconds = int(metric_vars.hours_to_resolve) * 3600

        # Calculate graphite from and until parameters from the metric timestamp
        graphite_until = datetime.datetime.fromtimestamp(int(metric_vars.metric_timestamp)).strftime('%H:%M_%Y%m%d')
        int_second_order_resolution_seconds = int(second_order_resolution_seconds)
        second_resolution_timestamp = int_metric_timestamp - int_second_order_resolution_seconds
        graphite_from = datetime.datetime.fromtimestamp(int(second_resolution_timestamp)).strftime('%H:%M_%Y%m%d')

        # Remove any old json file related to the metric
        metric_json_file = settings.MIRAGE_DATA_FOLDER + "/" + metric_vars.metric + "/" + metric_vars.metric + '.json'
        try:
            os.remove(metric_json_file)
        except OSError:
            pass

        # Get data from graphite
        logger.info('retrieve data     :: surfacing %s timeseries from graphite for %s seconds' % (metric_vars.metric, second_order_resolution_seconds))
        self.surface_graphite_metric_data(metric_vars.metric, graphite_from, graphite_until)

        # Check there is a json timeseries file to test
        if os.path.isfile(metric_json_file) != True:
            logger.error('retrieve failed   :: failed to surface %s timeseries from graphite' % (metric_vars.metric))
            # Remove metric check file
            try:
                os.remove(metric_check_file)
            except OSError:
                pass
            return
        else:
            logger.info('retrieved data    :: for %s at %s seconds' % (metric_vars.metric, second_order_resolution_seconds))

        # Make process-specific dicts
        exceptions = defaultdict(int)
        anomaly_breakdown = defaultdict(int)

        self.check_if_parent_is_alive()

        with open((metric_json_file), 'r') as f:
            timeseries = json.loads(f.read())
            logger.info('data points surfaced :: %s' % (len(timeseries)))

        try:
            logger.info('analyzing         :: %s at %s seconds' % (metric_vars.metric, second_order_resolution_seconds))
            anomalous, ensemble, datapoint = run_selected_algorithm(timeseries, metric_vars.metric, second_order_resolution_seconds)

            # If it's anomalous, add it to list
            if anomalous:
                base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
                anomalous_metric = [datapoint, base_name]
                self.anomalous_metrics.append(anomalous_metric)
                logger.info('anomaly detected  :: %s with %s' % (metric_vars.metric, metric_vars.value))
                # It runs so fast, this allows us to process 30 anomalies/min
                sleep(2)

                # Get the anomaly breakdown - who returned True?
                for index, value in enumerate(ensemble):
                    if value:
                        algorithm = settings.MIRAGE_ALGORITHMS[index]
                        anomaly_breakdown[algorithm] += 1
            else:
                base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
                not_anomalous_metric = [datapoint, base_name]
                self.not_anomalous_metrics.append(not_anomalous_metric)
                logger.info('not anomalous     :: %s with %s' % (metric_vars.metric, metric_vars.value))

        # It could have been deleted by the Roomba
        except TypeError:
            exceptions['DeletedByRoomba'] += 1
            logger.info('exceptions        :: DeletedByRoomba')
        except TooShort:
            exceptions['TooShort'] += 1
            logger.info('exceptions        :: TooShort')
        except Stale:
            exceptions['Stale'] += 1
            logger.info('exceptions        :: Stale')
        except Boring:
            exceptions['Boring'] += 1
            logger.info('exceptions        :: Boring')
        except:
            exceptions['Other'] += 1
            logger.info('exceptions        :: Other')
            logger.info(traceback.format_exc())

        # Add values to the queue so the parent process can collate
        for key, value in anomaly_breakdown.items():
            self.mirage_anomaly_breakdown_q.put((key, value))

        for key, value in exceptions.items():
            self.mirage_exceptions_q.put((key, value))

        # Remove metric check file
        try:
            os.remove(metric_check_file)
        except OSError:
            pass

    def run(self):
        """
        Called when the process intializes.
        """
        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except:
                logger.info('skyline can not connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                sleep(10)
                logger.info('connecting to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                continue

            """
            Determine if any metric to analyze
            """
            while True:

                metric_var_files = [f for f in listdir(settings.MIRAGE_CHECK_PATH) if isfile(join(settings.MIRAGE_CHECK_PATH, f))]
                if len(metric_var_files) == 0:
                    logger.info('sleeping no metrics...')
                    sleep(10)
                else:
                    sleep(1)

#                logger.info('sleeping no metrics...')
#                sleep(10)

                # Clean up old files
                now_timestamp = time()
                stale_age = now_timestamp - settings.MIRAGE_STALE_SECONDS
                for current_file in listdir(settings.MIRAGE_CHECK_PATH):
                    if os.path.isfile(settings.MIRAGE_CHECK_PATH + "/" + current_file):
                        t = os.stat(settings.MIRAGE_CHECK_PATH + "/" + current_file)
                        c = t.st_ctime
                        # delete file if older than a week
                        if c < stale_age:
                                os.remove(settings.MIRAGE_CHECK_PATH + "/" + current_file)
                                logger.info('removed %s' % (current_file))

                # Discover metric to analyze
                metric_var_files = ''
                metric_var_files = [f for f in listdir(settings.MIRAGE_CHECK_PATH) if isfile(join(settings.MIRAGE_CHECK_PATH, f))]
                if len(metric_var_files) > 0:
                    break

            metric_var_files_sorted = sorted(metric_var_files)
            metric_check_file = settings.MIRAGE_CHECK_PATH + "/" + metric_var_files_sorted[0]

            logger.info('processing %s' % metric_var_files_sorted[0])

            # Spawn processes
            pids = []
            MIRAGE_PROCESSES = 1
            run_timestamp = int(now)
            for i in range(1, MIRAGE_PROCESSES + 1):
                p = Process(target=self.spin_process, args=(i, run_timestamp))
                pids.append(p)
                p.start()

            # Send wait signal to zombie processes
            for p in pids:
                p.join()

            # Grab data from the queue and populate dictionaries
            exceptions = dict()
            anomaly_breakdown = dict()
            while 1:
                try:
                    key, value = self.mirage_anomaly_breakdown_q.get_nowait()
                    if key not in anomaly_breakdown.keys():
                        anomaly_breakdown[key] = value
                    else:
                        anomaly_breakdown[key] += value
                except Empty:
                    break

            while 1:
                try:
                    key, value = self.mirage_exceptions_q.get_nowait()
                    if key not in exceptions.keys():
                        exceptions[key] = value
                    else:
                        exceptions[key] += value
                except Empty:
                    break

            for metric_variable in self.metric_variables:
                if metric_variable[0] == 'metric_name':
                    metric_name = metric_variable[1]
                if metric_variable[0] == 'metric_value':
                    metric_value = metric_variable[1]
                if metric_variable[0] == 'hours_to_resolve':
                    hours_to_resolve = metric_variable[1]
                if metric_variable[0] == 'metric_timestamp':
                    metric_timestamp = metric_variable[1]

            logger.info('analysis done - %s' % metric_name)

            # Send alerts
            # Calculate hours second order resolution to seconds
            logger.info('analyzed at %s hours resolution' % hours_to_resolve)
            second_order_resolution_seconds = int(hours_to_resolve) * 3600
            logger.info('analyzed at %s seconds resolution' % second_order_resolution_seconds)

            if settings.MIRAGE_ENABLE_ALERTS:
                for alert in settings.ALERTS:
                    for metric in self.anomalous_metrics:
                        ALERT_MATCH_PATTERN = alert[0]
                        METRIC_PATTERN = metric[1]
                        alert_match_pattern = re.compile(ALERT_MATCH_PATTERN)
                        pattern_match = alert_match_pattern.match(METRIC_PATTERN)
                        if pattern_match:
                            cache_key = 'mirage.last_alert.%s.%s' % (alert[1], metric[1])
                            try:
                                last_alert = self.redis_conn.get(cache_key)
                                if not last_alert:
                                    self.redis_conn.setex(cache_key, alert[2], packb(metric[0]))
                                    trigger_alert(alert, metric, second_order_resolution_seconds)
                                    logger.info("Sent %s alert: For %s" % (alert[1], metric[1]))

                            except Exception as e:
                                logger.error("could not send %s alert for %s: %s" % (alert[1], metric[1], e))

            if settings.NEGATE_ANALYZER_ALERTS:
                if len(self.anomalous_metrics) == 0:
                    for negate_alert in settings.ALERTS:
                        for not_anomalous_metric in self.not_anomalous_metrics:
                            NEGATE_ALERT_MATCH_PATTERN = negate_alert[0]
                            NOT_ANOMALOUS_METRIC_PATTERN = not_anomalous_metric[1]
                            alert_match_pattern = re.compile(NEGATE_ALERT_MATCH_PATTERN)
                            negate_pattern_match = alert_match_pattern.match(NOT_ANOMALOUS_METRIC_PATTERN)
                            if negate_pattern_match:
                                try:
                                    logger.info("Negate alert sent: For %s" % (not_anomalous_metric[1]))
                                    trigger_negater(negate_alert, not_anomalous_metric, second_order_resolution_seconds, metric_value)
                                except Exception as e:
                                    logger.error("couldn't send alert: %s" % e)

            # Log progress

            if len(self.anomalous_metrics) > 0:
                logger.info('seconds since last anomaly :: %.2f' % (time() - now))
                logger.info('total anomalies   :: %d' % len(self.anomalous_metrics))
                logger.info('exception stats   :: %s' % exceptions)
                logger.info('anomaly breakdown :: %s' % anomaly_breakdown)

            # Reset counters
            self.anomalous_metrics[:] = []
            self.not_anomalous_metrics[:] = []

            # Reset metric_variables
            self.metric_variables[:] = []

            # Sleep if it went too fast
            if time() - now < 1:
                logger.info('sleeping due to low run time...')
#                sleep(10)
                sleep(1)
