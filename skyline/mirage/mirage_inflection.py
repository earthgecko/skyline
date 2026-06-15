"""
mirage_inflection.py
"""
import copy
import json
import logging
import os
import sys
import traceback

from ast import literal_eval
from multiprocessing import Process, Queue
from threading import Thread
from time import time, sleep

import settings
from custom_algorithms import run_custom_algorithm_on_timeseries
from functions.custom_algorithms.create_results_json import create_results_json
from functions.graphite.send_graphite_metric import send_graphite_metric
from functions.redis.get_metric_timeseries import get_metric_timeseries
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric
from skyline_functions import (
    filesafe_metricname, get_graphite_metric, get_redis_conn_decoded, mkdir_p,
    write_data_to_file,
)


parent_skyline_app = 'mirage'
skyline_app = 'mirage_inflection'
# skyline_app_logger = '%sLog' % skyline_app
skyline_app_logger = 'mirageLog'
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

python_version = int(sys.version_info[0])

this_host = str(os.uname()[1])

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

skyline_app_graphite_namespace = 'skyline.mirage%s.inflection' % (SERVER_METRIC_PATH)


# @added 20241119 - Feature #5064: mirage.inflection
class MirageInflection(Thread):
    """
    The MirageInflection thread
    """
    def __init__(self, parent_pid):
        """
        Initialize the MirageInflection
        """
        super().__init__()
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = os.getpid()
        self.mirage_inflection_exceptions_q = Queue()
        self.mirage_inflection_anomaly_breakdown_q = Queue()
        self.redis_conn_decoded = get_redis_conn_decoded(parent_skyline_app)

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            os.kill(self.current_pid, 0)
            os.kill(self.parent_pid, 0)
        except:
            logger.info('warning :: parent or current process dead')
            sys.exit(0)

    def spin_process(self, i, run_timestamp, mirage_inflections):
        """
        Check metrics in the mirage:inflection Redis hash data.
        """

        def remove_key(key_name):
            removed = 0
            try:
                removed = self.redis_conn_decoded.hdel('mirage.inflection', key_name)
            except Exception as err:
                logger.error('error :: mirage_inflection :: hdel failed on mirage.inflection for key %s, err: %s' % (
                    key_name, err))
            return removed

        if not mirage_inflections:
            logger.info('mirage_inflection :: no mirage_inflections, nothing to do')
            return

        process_start_timestamp = int(time())

        checks_processed = 0
        for timestamp_anomaly_id, metric_data in mirage_inflections.items():
            if int(time()) >= (process_start_timestamp + 50):
                logger.info('mirage_inflection :: run time limit reached - stopping')
                break

            checks_processed += 1
            skip = False
            results = {
                'success': False,
                'algorithms': {}
            }

            logger.info('mirage_inflection :: processing timestamp_anomaly_id: %s with %s' % (
                timestamp_anomaly_id, str(metric_data)))

            try:
                base_name = metric_data['base_name']
                metric = metric_data['metric']
                full_duration = metric_data['full_duration']
                timestamp = metric_data['timestamp']
                app = metric_data['app']
                anomaly_id = metric_data['anomaly_id']
            except Exception as err:
                logger.error('error :: mirage_inflection :: failed to determine details from metric_data for %s, err: %s' % (
                    timestamp_anomaly_id, err))

            if app not in ['mirage', 'ionosphere']:
                skip = True
                logger.error('error :: mirage_inflection :: not run for %s reported anomalies, skipping' % (
                    app))

            # @added 20250314 - Feature #5064: mirage.inflection
            # Remove keys older than the mirage.inflection expiry
            timestamp_anomaly_id_int = int(float(timestamp_anomaly_id))
            if process_start_timestamp > (timestamp_anomaly_id_int + 14399):
                skip = True
                logger.info('mirage_inflection :: removing old expired timestamp_anomaly_id: %s' % (
                    timestamp_anomaly_id))

            if not skip:
                until_timestamp = int(timestamp) + 900
                redis_from_timestamp = until_timestamp - settings.FULL_DURATION
                redis_timeseries = []
                try:
                    redis_timeseries = get_metric_timeseries(parent_skyline_app, base_name, int(redis_from_timestamp), int(until_timestamp), False)
                except Exception as err:
                    logger.error('error :: mirage_inflection :: get_metric_timeseries failed for %s, err: %s' % (
                        base_name, err))
                    redis_timeseries = []
                if not redis_timeseries:
                    skip = True
                    logger.error('error :: mirage_inflection :: failed to get time series from Redis for %s, skipping' % (
                        base_name))

            if not skip:
                until_timestamp = int(timestamp) + 900
                from_timestamp = until_timestamp - full_duration
                timeseries = []
                data_source = 'graphite'
                graphite_metric = True
                if metric.startswith('labelled_metrics.'):
                    data_source == 'victoriametrics'
                    graphite_metric = False
                if data_source == 'graphite':
                    try:
                        timeseries = get_graphite_metric(
                            parent_skyline_app, metric, from_timestamp,
                            until_timestamp, 'list', 'object')
                    except Exception as err:
                        logger.error('error :: %s :: get_graphite_metric failed with %s, err: %s' % (
                            skyline_app, metric, err))
                if data_source == 'victoriametrics':
                    try:
                        timeseries = get_victoriametrics_metric(
                            parent_skyline_app, base_name, from_timestamp,
                            until_timestamp, 'list', 'object')
                    except Exception as err:
                        logger.error('error :: %s :: get_victoriametrics_metric failed with %s, err: %s' % (
                            skyline_app, base_name, err))

                if not timeseries:
                    skip = True
                    logger.error('error :: mirage_inflection :: failed to get timeseries for %s, skipping' % (
                        base_name))

            if skip:
                removed_key = remove_key(timestamp_anomaly_id)
                if removed_key:
                    logger.info('mirage_inflection :: removed timestamp_anomaly_id: %s from mirage.inflection' % (
                        timestamp_anomaly_id))
                try:
                    self.redis_conn_decoded.incr('mirage.inflection.checks.done')
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_inflection :: failed to increment mirage.inflection.checks.done Redis key - %s' % str(err))
                continue

            anomalous = None
            triggered_algorithms = []
            algorithms_run = []
            algorithms_results = {}
            # This is the list of algorithms to run, if any trigger the metric
            # will be deemed anomalous, there is a CONSENSUS of 1 here.
            algorithms_to_be_run = ['mmzrmp']
            used_full_duration = int(full_duration)
            for algorithm in algorithms_to_be_run:
                try:
                    custom_algorithm = algorithm
                    use_timeseries = list(timeseries)
                    used_full_duration = int(full_duration)
                    used_data_source = str(data_source)

                    if algorithm == 'mmzrmp':
                        algorithm_parameters = {
                            'base_name': base_name,
                            # 'anomaly_window': 15, 'return_results': True,
                            # Exclude the original anomaly period
                            'anomaly_window': 7, 'return_results': True,
                            'debug_logging': True
                        }
                        use_timeseries = list(redis_timeseries)
                        used_full_duration = int(settings.FULL_DURATION)
                        used_data_source = 'redis'

                    try:
                        anomaly_window = algorithm_parameters['anomaly_window']
                    except:
                        # anomaly_window = 15
                        anomaly_window = 7

                    algorithm_source = '/opt/skyline/github/skyline/skyline/custom_algorithms/%s.py' % custom_algorithm
                    custom_algorithms_to_run = {
                        custom_algorithm: {
                            'algorithm_source': algorithm_source,
                            'max_execution_time': 30.0,
                            'algorithm_parameters': algorithm_parameters,
                        },
                    }

                    anomalous = anomalyScore = None
                    results = {'anomalies': {}, 'anomalyScore_list': [], 'scores': []}
                    try:
                        use_debug_logging = False
                        try:
                            if algorithm_parameters['debug_logging']:
                                use_debug_logging = True
                                custom_algorithms_to_run[custom_algorithm]['debug_logging'] = True
                        except:
                            pass
                        anomalous, anomalyScore, results = run_custom_algorithm_on_timeseries('mirage', os.getpid(), metric, use_timeseries, custom_algorithm, custom_algorithms_to_run[custom_algorithm], use_debug_logging)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_inflection :: %s unhandled error - %s' % (custom_algorithm, err))
                        results = {'anomalies': {}, 'anomalyScore_list': [], 'scores': []}

                    algorithms_results[algorithm] = copy.deepcopy(results)
                    algorithms_run.append(algorithm)

                    if anomalous:
                        triggered_algorithms.append(algorithm)
                        # THIS IS A CONSENSUS OF 1
                        break

                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_inflection :: algorithm %s failed - %s' % str(err))

            removed_key = remove_key(timestamp_anomaly_id)
            if removed_key:
                logger.info('mirage_inflection :: removed timestamp_anomaly_id %s from mirage.inflection' % (
                    timestamp_anomaly_id))

            anomalous = False
            if len(triggered_algorithms) > 0:
                anomalous = True

            logger.info('mirage_inflection :: processed timestamp_anomaly_id: %s for %s, anomalous: %s' % (
                timestamp_anomaly_id, str(metric), str(anomalous)))

            if anomalous:
                last_triggered_value = None
                last_triggered_timestamp = None
                for algorithm in algorithms_results.keys():
                    if 'anomalies' not in algorithms_results[algorithm].keys():
                        continue
                    try:
                        anomaly_timestamps = sorted(list(algorithms_results[algorithm]['anomalies'].keys()))
                        if anomaly_timestamps:
                            algorithm_last_triggered_timestamp = anomaly_timestamps[-1]
                            if last_triggered_timestamp:
                                if algorithm_last_triggered_timestamp > last_triggered_timestamp:
                                    last_triggered_timestamp = int(algorithm_last_triggered_timestamp)
                                    last_triggered_value = algorithms_results[algorithm]['anomalies'][algorithm_last_triggered_timestamp]['value']
                            else:
                                last_triggered_timestamp = int(algorithm_last_triggered_timestamp)
                                last_triggered_value = algorithms_results[algorithm]['anomalies'][algorithm_last_triggered_timestamp]['value']
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_inflection :: algorithm %s failed - %s' % str(err))
                if last_triggered_timestamp:
                    metric_timestamp = last_triggered_timestamp
                    value = float(last_triggered_value)
                else:
                    value = float(timeseries[-1][1])
                    metric_timestamp = int(timeseries[-1][0])

                # Add training data resources
                labelled_metric = None
                use_metric = str(base_name)
                # Handle labelled_metrics
                if metric != base_name:
                    use_metric = str(metric)
                    labelled_metric = str(metric)
                sane_metricname = filesafe_metricname(str(use_metric))

                timeseries_dir = sane_metricname.replace('.', '/')
                training_dir = '%s/%s/%s' % (
                    settings.IONOSPHERE_DATA_FOLDER, str(metric_timestamp),
                    str(timeseries_dir))
                if not os.path.exists(training_dir):
                    mkdir_p(training_dir)

                # Save custom_algorithms_results
                custom_algorithms_results = {}
                try:
                    custom_algorithms_results = {
                        'anomalous': True,
                        'metric': base_name,
                        'labelled_metric': labelled_metric,
                        'algorithms': copy.deepcopy(algorithms_results),
                        'timeseries': list(use_timeseries),
                        'results_path': training_dir,
                        'full_duration': used_full_duration,
                        'data_source': used_data_source,
                    }
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_inflection :: failed to created the custom_algorithms_results dict, err: %s' % str(err))
                    custom_algorithms_results = {}

                results_json = '%s/%s.custom_algorithms_results.json' % (training_dir, sane_metricname)
                coereced_custom_algorithms_results = None
                results_json_created = None
                coerced = {}
                logger.info('mirage_inflection :: running create_results_json')
                try:
                    coereced_custom_algorithms_results, coerced, results_json_created = create_results_json(parent_skyline_app, custom_algorithms_results, results_json)
                except Exception as err:
                    logger.error('error :: create_results_json failed to save %s, err: %s' % (
                    results_json, err))
                if not coereced_custom_algorithms_results:
                    logger.error('error :: coereced_custom_algorithms_results were not returned')
                if results_json != str(results_json_created):
                    if str(results_json_created).endswith('txt'):
                        logger.info('warning :: mirage_inflection :: DEBUG - the custom_algorithms_results written to %s' % (
                            results_json_created))
                if results_json_created:
                    logger.info('mirage_inflection :: saved custom_algorithms_results json %s' % str(results_json_created))
                else:
                    logger.error('error :: failed to save results_json: %s' % results_json)
                if len(coerced) > 0:
                    logger.info('mirage_inflection :: custom_algorithms_results required coercing')

                # Save timeseries json
                metric_json_file = '%s/%s.json' % (training_dir, sane_metricname)
                if not os.path.isfile(metric_json_file):
                    try:
                        with open(metric_json_file, 'w') as f:
                            f.write(json.dumps(use_timeseries))
                        os.chmod(metric_json_file, mode=0o644)
                        logger.info('mirage_inflection :: added timeseries json file: %s' % metric_json_file)
                    except Exception as err:
                        logger.error('error :: mirage_inflection :: failed to save metric_json_file: %s, err: %s' % (
                            metric_json_file, err))

                # Save a check file
                parent_id = 0
                anomaly_data = 'metric = \'%s\'\n' \
                            'value = \'%s\'\n' \
                            'from_timestamp = \'%s\'\n' \
                            'metric_timestamp = \'%s\'\n' \
                            'algorithms = %s\n' \
                            'triggered_algorithms = %s\n' \
                            'anomaly_dir = \'%s\'\n' \
                            'graphite_metric = %s\n' \
                            'run_crucible_tests = False\n' \
                            'added_by = \'%s\'\n' \
                            'added_at = \'%s\'\n' \
                            'full_duration = \'%s\'\n' \
                            'ionosphere_parent_id = \'%s\'\n' \
                            'algorithms_run = %s\n' \
                    % (str(base_name), str(value), str(from_timestamp),
                        str(metric_timestamp), str(algorithms_to_be_run),
                        str(triggered_algorithms), str(training_dir),
                        str(graphite_metric), skyline_app, str(int(time())),
                        str(int(used_full_duration)), str(parent_id), algorithms_run)
                anomaly_file = '%s/%s.txt' % (training_dir, str(sane_metricname))
                try:
                    write_data_to_file(parent_skyline_app, anomaly_file, 'w', anomaly_data)
                    logger.info('mirage_inflection :: added anomaly file: %s' % anomaly_file)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_inflection :: failed to save anomaly_file: %s, err: %s' % (
                        anomaly_file, err))

                # Alert sustained anomaly.  This is just routed through
                # the normal Mirage alerting mechanism so that it does
                # not have to be all redefined here.
                anomalous_metric = [value, base_name, metric_timestamp, used_full_duration, triggered_algorithms, algorithms_run, anomaly_id]
                redis_set = 'mirage.anomalous_metrics'
                data = str(anomalous_metric)
                try:
                    self.redis_conn_decoded.sadd(redis_set, data)
                    logger.info('mirage_inflection :: added item to %s related to timestamp_anomaly_id: %s for %s, %s' % (
                        redis_set, timestamp_anomaly_id, str(metric), str(data)))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_inflection :: failed to add %s to mirage.anomalous_metrics Redis set, err: %s' % (
                        str(data), err))

            try:
                self.redis_conn_decoded.incr('mirage.inflection.checks.done')
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_inflection :: failed to increment mirage.inflection.checks.done Redis key - %s' % str(err))

        completed_at = time()
        analysis_run_time = completed_at - process_start_timestamp
        logger.info('mirage_inflection :: %s checks processed in in %.2f seconds' % (
            str(checks_processed), analysis_run_time))

    def run(self):
        """
        Called when the process intializes.
        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        now = time()
        log_wait_for = now + 5
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time()
            else:
                now = log_wait_for + 1

        logger.info('mirage_inflection :: starting %s run' % skyline_app)

        last_sent_to_graphite = int(time())
        last_sleep_log = int(time())

        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn_decoded.ping()
            except:
                logger.error('error :: mirage_inflection :: skyline can not connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                sleep(10)
                logger.info('mirage_inflection :: attempting to connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                try:
                    self.redis_conn = get_redis_conn_decoded(parent_skyline_app)
                    self.redis_conn_decoded = get_redis_conn_decoded(parent_skyline_app)
                except Exception as err:
                    logger.error('error :: mirage_inflection :: failed to connect to Redis - %s' % err)
                try:
                    self.redis_conn_decoded.ping()
                    logger.info('mirage_inflection :: connected to redis')
                except Exception as err:
                    logger.error('error :: mirage_inflection :: failed to ping Redis - %s' % err)

            # Determine if any metric to analyze or Ionosphere alerts to be sent
            while True:

                # Report app up
                try:
                    redis_is_up = self.redis_conn_decoded.setex(skyline_app, 120, now)
                    if redis_is_up:
                        try:
                            self.redis_conn_decoded.setex('redis', 120, now)
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: mirage_inflection :: could not update the Redis redis key - %s' % (
                                e))
                except Exception as e:
                    logger.error('error :: mirage_inflection :: failed to update Redis key for %s up - %s' % (skyline_app, e))

                mirage_inflections = {}

                if len(mirage_inflections) == 0:
                    sleep_for = 1
                    next_send_to_graphite = last_sent_to_graphite + 60
                    seconds_to_next_send_to_graphite = next_send_to_graphite - int(time())
                    if seconds_to_next_send_to_graphite < 10:
                        if seconds_to_next_send_to_graphite > 1:
                            sleep_for = seconds_to_next_send_to_graphite
                        else:
                            break
                    if int(time()) > last_sleep_log + 10:
                        logger.info('mirage_inflection :: sleeping no metrics...')
                        last_sleep_log = int(time())
                    sleep(sleep_for)

                # Return results that are about to timeout
                mirage_inflections = {}
                mirage_inflection_data = {}
                try:
                    mirage_inflection_data = self.redis_conn_decoded.hgetall('mirage.inflection')
                except Exception as err:
                    logger.error('mirage_inflection :: hgetall failed on mirage.inflection, err: %s' % err)
                for timestamp_anomaly_id in list(mirage_inflection_data.keys()):
                    data = None
                    try:
                        data = literal_eval(mirage_inflection_data[timestamp_anomaly_id])
                    except Exception as err:
                        logger.error('mirage_inflection :: literal_eval failed on mirage.inflection key %s, err: %s' % (
                            timestamp_anomaly_id, err))
                    if data:
                        anomaly_timestamp = data['timestamp']
                        if time() > (anomaly_timestamp + 900):
                            mirage_inflections[timestamp_anomaly_id] = data

                if len(mirage_inflections) > 0:
                    break

            checks_to_process = 0
            if mirage_inflections:

                checks_to_process = len(mirage_inflections)
                logger.info('mirage_inflection :: %s checks to process' % str(checks_to_process))

                # Spawn processes
                pids = []
                spawned_pids = []
                pid_count = 0

                MIRAGE_PROCESSES = 1

                run_timestamp = int(time())
                for i in range(1, MIRAGE_PROCESSES + 1):

                    p = Process(target=self.spin_process, args=(i, run_timestamp, mirage_inflections))

                    pids.append(p)
                    pid_count += 1
                    logger.info('mirage_inflection :: starting %s of %s spin_process/es' % (
                        str(pid_count), str(MIRAGE_PROCESSES)))
                    p.start()
                    spawned_pids.append([p.pid, i])
                    logger.info('mirage_inflection :: started spin_process %s with pid %s' % (str(pid_count), str(p.pid)))

                # Self monitor processes and terminate if any spin_process has run
                # for longer than 180 seconds - 20160512 @earthgecko
                p_starts = time()
                while time() - p_starts <= (settings.MAX_ANALYZER_PROCESS_RUNTIME * 3):
                    if any(p.is_alive() for p in pids):
                        # Just to avoid hogging the CPU
                        sleep(.1)
                    else:
                        # All the processes are done, break now.
                        time_to_run = time() - p_starts
                        logger.info('%s :: %s spin_process/es completed in %.2f seconds' % (
                            skyline_app, str(MIRAGE_PROCESSES), time_to_run))
                        break
                else:
                    # We only enter this if we didn't 'break' above.
                    logger.info('%s :: timed out, killing all spin_process processes' % (skyline_app))
                    for p in pids:
                        p.terminate()
                        # p.join()

                for p in pids:
                    if p.is_alive():
                        logger.info('%s :: stopping spin_process - %s' % (skyline_app, str(p.is_alive())))
                        # @modified 20240202 - Task #5178: Build and test skyline v4.1.0
                        # p.join()
                        killing_pid = p.pid
                        logger.info('%s :: kill spin_process with pid: %s' % (skyline_app, str(killing_pid)))
                        p.terminate()
                        logger.info('%s :: killed spin_process process with pid: %s' % (skyline_app, str(killing_pid)))

                check_algorithm_errors = ['mmzrmp']

                for completed_pid, mirage_process in spawned_pids:
                    logger.info('mirage_inflection :: spin_process with pid %s completed' % (str(completed_pid)))
                    try:
                        for algorithm in check_algorithm_errors:
                            algorithm_error_file = '%s/%s.%s.%s.algorithm.error' % (
                                settings.SKYLINE_TMP_DIR, skyline_app,
                                str(completed_pid), algorithm)
                            if os.path.isfile(algorithm_error_file):
                                logger.info(
                                    'error :: mirage_inflection :: spin_process with pid %s has reported an error with the %s algorithm' % (
                                        str(completed_pid), algorithm))
                                try:
                                    with open(algorithm_error_file, 'r') as f:
                                        error_string = f.read()
                                    logger.error('%s' % str(error_string))
                                except:
                                    logger.error('error :: mirage_inflection :: failed to read %s error file' % algorithm)
                                try:
                                    os.remove(algorithm_error_file)
                                except OSError:
                                    pass
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_inflection :: failed to check algorithm errors')

            # Log progress
            checks_done = 0
            if checks_to_process:
                try:
                    checks_done = self.redis_conn_decoded.get('mirage.inflection.checks.done')
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_inflection :: failed to get mirage.inflection.checks.done, err: %s' % (
                        err))
                logger.info('mirage_inflection :: %s checks done' % str(checks_done))

            if checks_done:
                run_time = time() - run_timestamp
            else:
                run_time = 0
            try:
                self.redis_conn_decoded.incr('mirage.inflection.run_time', int(run_time))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: mirage_inflection :: failed to increment mirage.inflection.checks.done Redis key - %s' % str(err))

            logger.info('mirage_inflection :: seconds to run    :: %.2f' % run_time)

            # Log to Graphite
            if int(time()) >= (last_sent_to_graphite + 60):
                try:
                    run_time_str = self.redis_conn_decoded.set('mirage.inflection.run_time', 0, get=True)
                    if run_time_str:
                        run_time = float(run_time_str)
                        if run_time == 0:
                            run_time = 1.0
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_inflection :: failed to get mirage.inflection.run_time key from Redis, err: %s' % err)
                graphite_run_time = '%.2f' % run_time
                send_metric_name = skyline_app_graphite_namespace + '.run_time'
                send_graphite_metric(self, parent_skyline_app, send_metric_name, graphite_run_time)

                checks_done = 0
                try:
                    checks_done_str = self.redis_conn_decoded.set('mirage.inflection.checks.done', 0, get=True)
                    if checks_done_str:
                        checks_done = int(checks_done_str)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_inflection :: failed to get mirage.inflection.checks.done key from Redis, err: %s' % err)
                    checks_done = 0
                logger.info('mirage_inflection :: checks.done   :: %s' % str(checks_done))
                send_metric_name = '%s.checks.done' % skyline_app_graphite_namespace
                send_graphite_metric(self, parent_skyline_app, send_metric_name, str(checks_done))

                last_sent_to_graphite = int(time())

            # Sleep if it went too fast
            if time() - now < 59:
                logger.info('mirage_inflection :: sleeping due to low run time...')
                sleep(1)
