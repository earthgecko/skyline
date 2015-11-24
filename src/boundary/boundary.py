import logging
from Queue import Empty
from redis import StrictRedis
from time import time, sleep
from threading import Thread
from collections import defaultdict
from multiprocessing import Process, Manager, Queue
from msgpack import Unpacker, unpackb, packb
from os import path, kill, getpid, system, listdir
from os.path import dirname, join, abspath, isfile
from math import ceil
import traceback
import operator
import socket
import settings
import re
import os
import errno

from alerters import trigger_alert
from algorithms import run_selected_algorithm
from algorithm_exceptions import *

logger = logging.getLogger("BoundaryLog")

try:
    SERVER_METRIC_PATH = settings.SERVER_METRICS_NAME + '.'
except:
    SERVER_METRIC_PATH = ''

REDIS_SOCKET = settings.REDIS_SOCKET_PATH
BOUNDARY_METRICS = settings.BOUNDARY_METRICS
FULL_NAMESPACE = settings.FULL_NAMESPACE
ENABLE_BOUNDARY_DEBUG = settings.ENABLE_BOUNDARY_DEBUG
try:
    BOUNDARY_AUTOAGGRERATION = settings.BOUNDARY_AUTOAGGRERATION
except:
    BOUNDARY_AUTOAGGRERATION = False
try:
    BOUNDARY_AUTOAGGRERATION_METRICS = settings.BOUNDARY_AUTOAGGRERATION_METRICS
except:
    BOUNDARY_AUTOAGGRERATION_METRICS = (
        ("auotaggeration_metrics_not_declared", 60)
    )


class Boundary(Thread):
    def __init__(self, parent_pid):
        """
        Initialize the Boundary
        """
        super(Boundary, self).__init__()
        self.redis_conn = StrictRedis(unix_socket_path=REDIS_SOCKET)
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        self.boundary_metrics = Manager().list()
        self.anomalous_metrics = Manager().list()
        self.exceptions_q = Queue()
        self.anomaly_breakdown_q = Queue()

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            exit(0)

    def send_graphite_metric(self, name, value):
        if settings.GRAPHITE_HOST != '':
            sock = socket.socket()

            try:
                sock.connect((settings.GRAPHITE_HOST, settings.CARBON_PORT))
            except socket.error:
                endpoint = '%s:%d' % (settings.GRAPHITE_HOST,
                                      settings.CARBON_PORT)
                logger.error("Can't connect to Graphite at %s" % endpoint)
                return False

            sock.sendall('%s %s %i\n' % (name, value, time()))
            sock.close()
            return True

        return False

    def unique_noHash(self, seq):
        seen = set()
        return [x for x in seq if str(x) not in seen and not seen.add(str(x))]

    def mkdir_p(self, path):
        try:
            os.makedirs(path)
            return True
        except OSError as exc:
            # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def spin_process(self, i, boundary_metrics):
        """
        Assign a bunch of metrics for a process to analyze.
        """
        # Determine assigned metrics
        bp = settings.BOUNDARY_PROCESSES
        bm_range = len(boundary_metrics)
        keys_per_processor = int(ceil(float(bm_range) / float(bp)))
        if i == settings.BOUNDARY_PROCESSES:
            assigned_max = len(boundary_metrics)
        else:
            # This is a skyine bug, the original skyline code uses 1 as the
            # beginning position of the index, python indices begin with 0
            # assigned_max = len(boundary_metrics)
            # This closes the etsy/skyline pull request opened by @languitar on 17 Jun 2014
            # https://github.com/etsy/skyline/pull/94 Fix analyzer worker metric assignment
            assigned_max = min(len(boundary_metrics), i * keys_per_processor)
        assigned_min = (i - 1) * keys_per_processor
        assigned_keys = range(assigned_min, assigned_max)

        # Compile assigned metrics
        assigned_metrics_and_algos = [boundary_metrics[index] for index in assigned_keys]
        if ENABLE_BOUNDARY_DEBUG:
            logger.info('debug - printing assigned_metrics_and_algos')
            for assigned_metric_and_algo in assigned_metrics_and_algos:
                logger.info('debug - assigned_metric_and_algo - %s' % str(assigned_metric_and_algo))

        # Compile assigned metrics
        assigned_metrics = []
        for i in assigned_metrics_and_algos:
            assigned_metrics.append(i[0])

        def unique_noHash(seq):
            seen = set()
            return [x for x in seq if str(x) not in seen and not seen.add(str(x))]

        unique_assigned_metrics = unique_noHash(assigned_metrics)

        if ENABLE_BOUNDARY_DEBUG:
            logger.info('debug - unique_assigned_metrics - %s' % str(unique_assigned_metrics))
            logger.info('debug - printing unique_assigned_metrics:')
            for unique_assigned_metric in unique_assigned_metrics:
                logger.info('debug - unique_assigned_metric - %s' % str(unique_assigned_metric))

        # Check if this process is unnecessary
        if len(unique_assigned_metrics) == 0:
            return

        # Multi get series
        try:
            raw_assigned = self.redis_conn.mget(unique_assigned_metrics)
        except:
            logger.error("failed to mget assigned_metrics from redis")
            return

        # Make process-specific dicts
        exceptions = defaultdict(int)
        anomaly_breakdown = defaultdict(int)

        # Reset boundary_algortims
        all_boundary_algorithms = []
        for metric in BOUNDARY_METRICS:
            all_boundary_algorithms.append(metric[1])

        boundary_algorithms = unique_noHash(all_boundary_algorithms)
        if ENABLE_BOUNDARY_DEBUG:
            logger.info('debug - boundary_algorithms - %s' % str(boundary_algorithms))

        discover_run_metrics = []

        # Distill metrics into a run list
        for i, metric_name, in enumerate(unique_assigned_metrics):
            self.check_if_parent_is_alive()

            try:
                if ENABLE_BOUNDARY_DEBUG:
                    logger.info('debug - unpacking timeseries for %s - %s' % (metric_name, str(i)))
                raw_series = raw_assigned[i]
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)
            except Exception as e:
                exceptions['Other'] += 1
                logger.error("redis data error: " + traceback.format_exc())
                logger.error("error: %e" % e)

            base_name = metric_name.replace(FULL_NAMESPACE, '', 1)

            for metrick in BOUNDARY_METRICS:
                CHECK_MATCH_PATTERN = metrick[0]
                check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
                pattern_match = check_match_pattern.match(base_name)
                metric_pattern_matched = False
                if pattern_match:
                    metric_pattern_matched = True
                    algo_pattern_matched = False
                    for algo in boundary_algorithms:
                        for metric in BOUNDARY_METRICS:
                            CHECK_MATCH_PATTERN = metric[0]
                            check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
                            pattern_match = check_match_pattern.match(base_name)
                            if pattern_match:
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info("debug - metric and algo pattern MATCHED - " + metric[0] + " | " + base_name + " | " + str(metric[1]))
                                metric_expiration_time = False
                                metric_min_average = False
                                metric_min_average_seconds = False
                                metric_trigger = False
                                algorithm = False
                                algo_pattern_matched = True
                                algorithm = metric[1]
                                try:
                                    if metric[2]:
                                        metric_expiration_time = metric[2]
                                except:
                                    metric_expiration_time = False
                                try:
                                    if metric[3]:
                                        metric_min_average = metric[3]
                                except:
                                    metric_min_average = False
                                try:
                                    if metric[4]:
                                        metric_min_average_seconds = metric[4]
                                except:
                                    metric_min_average_seconds = 1200
                                try:
                                    if metric[5]:
                                        metric_trigger = metric[5]
                                except:
                                    metric_trigger = False
                                try:
                                    if metric[6]:
                                        alert_threshold = metric[6]
                                except:
                                    alert_threshold = False
                                try:
                                    if metric[7]:
                                        metric_alerters = metric[7]
                                except:
                                    metric_alerters = False
                            if metric_pattern_matched and algo_pattern_matched:
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info('debug - added metric - %s, %s, %s, %s, %s, %s, %s, %s, %s' % (str(i), metric_name, str(metric_expiration_time), str(metric_min_average), str(metric_min_average_seconds), str(metric_trigger), str(alert_threshold), metric_alerters, algorithm))
                                discover_run_metrics.append([i, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger, alert_threshold, metric_alerters, algorithm])

        if ENABLE_BOUNDARY_DEBUG:
            logger.info('debug - printing discover_run_metrics')
            for discover_run_metric in discover_run_metrics:
                logger.info('debug - discover_run_metrics - %s' % str(discover_run_metric))
            logger.info('debug - build unique boundary metrics to analyze')

        run_metrics = unique_noHash(discover_run_metrics)

        if ENABLE_BOUNDARY_DEBUG:
            logger.info('debug - printing run_metrics')
            for run_metric in run_metrics:
                logger.info('debug - run_metrics - %s' % str(run_metric))

        # Distill timeseries strings into lists
        for metric_and_algo in run_metrics:
            self.check_if_parent_is_alive()

            try:
                raw_assigned_id = metric_and_algo[0]
                metric_name = metric_and_algo[1]
                metric_expiration_time = metric_and_algo[2]
                metric_min_average = metric_and_algo[3]
                metric_min_average_seconds = metric_and_algo[4]
                metric_trigger = metric_and_algo[5]
                alert_threshold = metric_and_algo[6]
                metric_alerters = metric_and_algo[7]
                algorithm = metric_and_algo[8]

                if ENABLE_BOUNDARY_DEBUG:
                    logger.info('debug - unpacking timeseries for %s - %s' % (metric_name, str(raw_assigned_id)))

                raw_series = raw_assigned[metric_and_algo[0]]
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)

                if ENABLE_BOUNDARY_DEBUG:
                    logger.info('debug - unpacked OK - %s - %s' % (metric_name, str(raw_assigned_id)))

                autoaggregate = False
                autoaggregate_value = 0

                if BOUNDARY_AUTOAGGRERATION:
                    for autoaggregate_metric in BOUNDARY_AUTOAGGRERATION_METRICS:
                        autoaggregate = False
                        autoaggregate_value = 0
                        CHECK_MATCH_PATTERN = autoaggregate_metric[0]
                        base_name = metric_name.replace(FULL_NAMESPACE, '', 1)
                        check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
                        pattern_match = check_match_pattern.match(base_name)
                        if pattern_match:
                            autoaggregate = True
                            autoaggregate_value = autoaggregate_metric[1]

                if ENABLE_BOUNDARY_DEBUG:
                    logger.info('debug - BOUNDARY_AUTOAGGRERATION passed - %s - %s' % (metric_name, str(autoaggregate)))

                if ENABLE_BOUNDARY_DEBUG:
                    logger.info(
                        'debug - analysing - %s, %s, %s, %s, %s, %s, %s, %s, %s, %s' % (
                            metric_name, str(metric_expiration_time),
                            str(metric_min_average),
                            str(metric_min_average_seconds),
                            str(metric_trigger), str(alert_threshold),
                            metric_alerters, autoaggregate,
                            autoaggregate_value, algorithm)
                    )
                    timeseries_dump_dir = "/tmp/skyline/boundary/" + algorithm
                    self.mkdir_p(timeseries_dump_dir)
                    timeseries_dump_file = timeseries_dump_dir + "/" + metric_name + ".json"
                    with open(timeseries_dump_file, 'w+') as f:
                        f.write(str(timeseries))
                        f.close()

                anomalous, ensemble, datapoint, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger, alert_threshold, metric_alerters, algorithm = run_selected_algorithm(
                    timeseries, metric_name,
                    metric_expiration_time,
                    metric_min_average,
                    metric_min_average_seconds,
                    metric_trigger,
                    alert_threshold,
                    metric_alerters,
                    autoaggregate,
                    autoaggregate_value,
                    algorithm
                )

                if ENABLE_BOUNDARY_DEBUG:
                    logger.info('debug - analysed - %s' % (metric_name))

                # If it's anomalous, add it to list
                if anomalous:
                    anomalous_metric = [datapoint, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger, alert_threshold, metric_alerters, algorithm]
                    self.anomalous_metrics.append(anomalous_metric)
                    # Get the anomaly breakdown - who returned True?
                    for index, value in enumerate(ensemble):
                        if value:
                            anomaly_breakdown[algorithm] += 1

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
                logger.info("exceptions['Other'] traceback follows:")
                logger.info(traceback.format_exc())

        # Add values to the queue so the parent process can collate
        for key, value in anomaly_breakdown.items():
            self.anomaly_breakdown_q.put((key, value))

        for key, value in exceptions.items():
            self.exceptions_q.put((key, value))

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
                logger.error('skyline can\'t connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                sleep(10)
                self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                continue

            # Discover unique metrics
            unique_metrics = list(self.redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))

            if len(unique_metrics) == 0:
                logger.info('no metrics in redis. try adding some - see README')
                sleep(10)
                continue

            # Reset boundary_metrics
            boundary_metrics = []

            # Build boundary metrics
            for metric_name in unique_metrics:
                for metric in BOUNDARY_METRICS:
                    CHECK_MATCH_PATTERN = metric[0]
                    check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
                    base_name = metric_name.replace(FULL_NAMESPACE, '', 1)
                    pattern_match = check_match_pattern.match(base_name)
                    if pattern_match:
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.info("debug - boundary metric - pattern MATCHED - " + metric[0] + " | " + base_name)
                        boundary_metrics.append([metric_name, metric[1]])

            if ENABLE_BOUNDARY_DEBUG:
                logger.info("debug - boundary metrics - " + str(boundary_metrics))

            if len(boundary_metrics) == 0:
                logger.info('no metrics in redis. try adding some - see README')
                sleep(10)
                continue

            # Spawn processes
            pids = []
            for i in range(1, settings.BOUNDARY_PROCESSES + 1):
                if i > len(boundary_metrics):
                    logger.info('WARNING: skyline boundary is set for more cores than needed.')
                    break

                p = Process(target=self.spin_process, args=(i, boundary_metrics))
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

            # Send alerts
            if settings.BOUNDARY_ENABLE_ALERTS:
                for anomalous_metric in self.anomalous_metrics:
                    datapoint = str(anomalous_metric[0])
                    metric_name = anomalous_metric[1]
                    base_name = metric_name.replace(FULL_NAMESPACE, '', 1)
                    expiration_time = str(anomalous_metric[2])
                    metric_trigger = str(anomalous_metric[5])
                    alert_threshold = int(anomalous_metric[6])
                    metric_alerters = anomalous_metric[7]
                    algorithm = anomalous_metric[8]
                    if ENABLE_BOUNDARY_DEBUG:
                        logger.info("debug - anomalous_metric - " + str(anomalous_metric))

                    # Determine how many times has the anomaly been seen if the
                    # ALERT_THRESHOLD is set to > 1
                    if alert_threshold == 0:
                        times_seen = 1
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.info("debug - alert_threshold - " + str(alert_threshold))

                    if alert_threshold == 1:
                        times_seen = 1
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.info("debug - alert_threshold - " + str(alert_threshold))

                    if alert_threshold > 1:
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.info("debug - alert_threshold - " + str(alert_threshold))
                        anomaly_cache_key_count_set = False
                        anomaly_cache_key_expiration_time = (int(alert_threshold) + 1) * 60
                        anomaly_cache_key = 'anomaly_seen.%s.%s' % (algorithm, base_name)
                        try:
                            anomaly_cache_key_count = self.redis_conn.get(anomaly_cache_key)
                            if not anomaly_cache_key_count:
                                try:
                                    if ENABLE_BOUNDARY_DEBUG:
                                        logger.info("debug - redis no anomaly_cache_key - " + str(anomaly_cache_key))
                                    times_seen = 1
                                    if ENABLE_BOUNDARY_DEBUG:
                                        logger.info("debug - redis setex anomaly_cache_key - " + str(anomaly_cache_key))
                                    self.redis_conn.setex(anomaly_cache_key, anomaly_cache_key_expiration_time, packb(int(times_seen)))
                                    logger.info('set anomaly seen key :: %s seen %s' % (anomaly_cache_key, str(times_seen)))
                                except Exception as e:
                                    logger.error('redis setex failed :: %s' % str(anomaly_cache_key))
                                    logger.error("couldn't set key: %s" % e)
                            else:
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info("debug - redis anomaly_cache_key retrieved OK - " + str(anomaly_cache_key))
                                anomaly_cache_key_count_set = True
                        except:
                            if ENABLE_BOUNDARY_DEBUG:
                                logger.info("debug - redis failed - anomaly_cache_key retrieval failed - " + str(anomaly_cache_key))
                            anomaly_cache_key_count_set = False

                        if anomaly_cache_key_count_set:
                            unpacker = Unpacker(use_list=False)
                            unpacker.feed(anomaly_cache_key_count)
                            raw_times_seen = list(unpacker)
                            times_seen = int(raw_times_seen[0]) + 1
                            try:
                                self.redis_conn.setex(anomaly_cache_key, anomaly_cache_key_expiration_time, packb(int(times_seen)))
                                logger.info('set anomaly seen key :: %s seen %s' % (anomaly_cache_key, str(times_seen)))
                            except:
                                times_seen = 1
                                logger.error('set anomaly seen key failed :: %s seen %s' % (anomaly_cache_key, str(times_seen)))

                    # Alert the alerters if times_seen > alert_threshold
                    if times_seen >= alert_threshold:
                        if ENABLE_BOUNDARY_DEBUG:
                            logger.info("debug - times_seen %s is greater than or equal to alert_threshold %s" % (str(times_seen), str(alert_threshold)))
                        for alerter in metric_alerters.split("|"):
                            # Determine alerter limits
                            send_alert = False
                            alerts_sent = 0
                            if ENABLE_BOUNDARY_DEBUG:
                                logger.info("debug - checking alerter - %s" % alerter)
                            try:
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info("debug - determining alerter_expiration_time for settings")
                                alerter_expiration_time_setting = settings.BOUNDARY_ALERTER_OPTS['alerter_expiration_time'][alerter]
                                alerter_expiration_time = int(alerter_expiration_time_setting)
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info("debug - determined alerter_expiration_time from settings - %s" % str(alerter_expiration_time))
                            except:
                                # Set an arbitrary expiry time if not set
                                alerter_expiration_time = 160
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info("debug - could not determine alerter_expiration_time from settings")
                            try:
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info("debug - determining alerter_limit from settings")
                                alerter_limit_setting = settings.BOUNDARY_ALERTER_OPTS['alerter_limit'][alerter]
                                alerter_limit = int(alerter_limit_setting)
                                alerter_limit_set = True
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info("debug - determined alerter_limit from settings - %s" % str(alerter_limit))
                            except:
                                alerter_limit_set = False
                                send_alert = True
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info("debug - could not determine alerter_limit from settings")

                            # If the alerter_limit is set determine how many
                            # alerts the alerter has sent
                            if alerter_limit_set:
                                alerter_sent_count_key = 'alerts_sent.%s' % (alerter)
                                try:
                                    alerter_sent_count_key_data = self.redis_conn.get(alerter_sent_count_key)
                                    if not alerter_sent_count_key_data:
                                        if ENABLE_BOUNDARY_DEBUG:
                                            logger.info("debug - redis no alerter key, no alerts sent for - " + str(alerter_sent_count_key))
                                        alerts_sent = 0
                                        send_alert = True
                                        if ENABLE_BOUNDARY_DEBUG:
                                            logger.info("debug - alerts_sent set to %s" % str(alerts_sent))
                                            logger.info("debug - send_alert set to %s" % str(sent_alert))
                                    else:
                                        if ENABLE_BOUNDARY_DEBUG:
                                            logger.info("debug - redis alerter key retrieved, unpacking" + str(alerter_sent_count_key))
                                        unpacker = Unpacker(use_list=False)
                                        unpacker.feed(alerter_sent_count_key_data)
                                        raw_alerts_sent = list(unpacker)
                                        alerts_sent = int(raw_alerts_sent[0])
                                        if ENABLE_BOUNDARY_DEBUG:
                                            logger.info("debug - alerter %s alerts sent %s " % (str(alerter), str(alerts_sent)))
                                except:
                                    logger.info("No key set - %s" % alerter_sent_count_key)
                                    alerts_sent = 0
                                    send_alert = True
                                    if ENABLE_BOUNDARY_DEBUG:
                                        logger.info("debug - alerts_sent set to %s" % str(alerts_sent))
                                        logger.info("debug - send_alert set to %s" % str(send_alert))

                                if alerts_sent < alerter_limit:
                                    send_alert = True
                                    if ENABLE_BOUNDARY_DEBUG:
                                        logger.info("debug - alerts_sent %s is less than alerter_limit %s" % (str(alerts_sent), str(alerter_limit)))
                                        logger.info("debug - send_alert set to %s" % str(send_alert))

                            alerter_alert_sent = False
                            if send_alert:
                                cache_key = 'last_alert.boundary.%s.%s.%s' % (alerter, base_name, algorithm)
                                if ENABLE_BOUNDARY_DEBUG:
                                    logger.info("debug - checking cache_key - %s" % cache_key)
                                try:
                                    last_alert = self.redis_conn.get(cache_key)
                                    if not last_alert:
                                        try:
                                            self.redis_conn.setex(cache_key, int(anomalous_metric[2]), packb(int(anomalous_metric[0])))
                                            if ENABLE_BOUNDARY_DEBUG:
                                                logger.info('debug - key setex OK - %s' % (cache_key))
                                            trigger_alert(alerter, datapoint, base_name, expiration_time, metric_trigger, algorithm)
                                            logger.info('alert sent :: %s - %s - via %s - %s' % (base_name, datapoint, alerter, algorithm))
                                            trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm)
                                            logger.info('alert sent :: %s - %s - via syslog - %s' % (base_name, datapoint, algorithm))
                                            alerter_alert_sent = True
                                        except Exception as e:
                                            logger.error('alert failed :: %s - %s - via %s - %s' % (base_name, datapoint, alerter, algorithm))
                                            logger.error("couldn't send alert: %s" % str(e))
                                            trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm)
                                    else:
                                        if ENABLE_BOUNDARY_DEBUG:
                                            logger.info("debug - cache_key exists not alerting via %s for %s is less than alerter_limit %s" % (alerter, cache_key))
                                        trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm)
                                        logger.info('alert sent :: %s - %s - via syslog - %s' % (base_name, datapoint, algorithm))
                                except:
                                    trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm)
                                    logger.info('alert sent :: %s - %s - via syslog - %s' % (base_name, datapoint, algorithm))
                            else:
                                trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm)
                                logger.info('alert sent :: %s - %s - via syslog - %s' % (base_name, datapoint, algorithm))

                            if alerter_alert_sent and alerter_limit_set:
                                try:
                                    alerter_sent_count_key = 'alerts_sent.%s' % (alerter)
                                    new_alerts_sent = int(alerts_sent) + 1
                                    self.redis_conn.setex(alerter_sent_count_key, alerter_expiration_time, packb(int(new_alerts_sent)))
                                    logger.info('set %s - %s' % (alerter_sent_count_key, str(new_alerts_sent)))
                                except:
                                    logger.error('failed to set %s - %s' % (alerter_sent_count_key, str(new_alerts_sent)))

                    else:
                        # Always alert to syslog, even if alert_threshold is not
                        # breached or if send_alert is not True
                        trigger_alert("syslog", datapoint, base_name, expiration_time, metric_trigger, algorithm)
                        logger.info('alert sent :: %s - %s - via syslog - %s' % (base_name, datapoint, algorithm))

            # Write anomalous_metrics to static webapp directory
            if len(self.anomalous_metrics) > 0:
                filename = path.abspath(path.join(path.dirname(__file__), '..', settings.ANOMALY_DUMP))
                with open(filename, 'w') as fh:
                    # Make it JSONP with a handle_data() function
                    anomalous_metrics = list(self.anomalous_metrics)
                    anomalous_metrics.sort(key=operator.itemgetter(1))
                    fh.write('handle_data(%s)' % anomalous_metrics)

            # Log progress
            logger.info('seconds to run    :: %.2f' % (time() - now))
            logger.info('total metrics     :: %d' % len(boundary_metrics))
            logger.info('total analyzed    :: %d' % (len(boundary_metrics) - sum(exceptions.values())))
            logger.info('total anomalies   :: %d' % len(self.anomalous_metrics))
            logger.info('exception stats   :: %s' % exceptions)
            logger.info('anomaly breakdown :: %s' % anomaly_breakdown)

            # Log to Graphite
            self.send_graphite_metric('skyline.boundary.' + SERVER_METRIC_PATH + 'run_time', '%.2f' % (time() - now))
            self.send_graphite_metric('skyline.boundary.' + SERVER_METRIC_PATH + 'total_analyzed', '%.2f' % (len(boundary_metrics) - sum(exceptions.values())))
            self.send_graphite_metric('skyline.boundary.' + SERVER_METRIC_PATH + 'total_anomalies', '%d' % len(self.anomalous_metrics))
            self.send_graphite_metric('skyline.boundary.' + SERVER_METRIC_PATH + 'total_metrics', '%d' % len(boundary_metrics))
            for key, value in exceptions.items():
                send_metric = 'skyline.boundary.' + SERVER_METRIC_PATH + 'exceptions.%s' % key
                self.send_graphite_metric(send_metric, '%d' % value)
            for key, value in anomaly_breakdown.items():
                send_metric = 'skyline.boundary.' + SERVER_METRIC_PATH + 'anomaly_breakdown.%s' % key
                self.send_graphite_metric(send_metric, '%d' % value)

            # Check canary metric
            raw_series = self.redis_conn.get(settings.FULL_NAMESPACE + settings.CANARY_METRIC)
            if raw_series is not None:
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)
                time_human = (timeseries[-1][0] - timeseries[0][0]) / 3600
                projected = 24 * (time() - now) / time_human

                logger.info('canary duration   :: %.2f' % time_human)
                self.send_graphite_metric('skyline.boundary.' + SERVER_METRIC_PATH + 'duration', '%.2f' % time_human)
                self.send_graphite_metric('skyline.boundary.' + SERVER_METRIC_PATH + 'projected', '%.2f' % projected)

            # Reset counters
            self.anomalous_metrics[:] = []

            # Only run once per minute
            seconds_to_run = int((time() - now))
            if seconds_to_run < 60:
                sleep_for_seconds = 60 - seconds_to_run
            else:
                sleep_for_seconds = 0
            if sleep_for_seconds > 0:
                logger.info('sleeping for %s seconds' % sleep_for_seconds)
                sleep(sleep_for_seconds)
