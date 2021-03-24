import os
from os import getpid
import logging
import traceback
from timeit import default_timer as timer
from time import time
from ast import literal_eval

import mysql.connector

import settings
from skyline_functions import (
    get_redis_conn, get_redis_conn_decoded, get_anomaly_id, mysql_select)

try:
    from custom_algorithms import run_custom_algorithm_on_timeseries
except:
    run_custom_algorithm_on_timeseries = None

try:
    LUMINOSITY_CLASSIFY_ANOMALY_ALGORITHMS = settings.LUMINOSITY_CLASSIFY_ANOMALY_ALGORITHMS
except:
    LUMINOSITY_CLASSIFY_ANOMALY_ALGORITHMS = []

try:
    LUMINOSITY_CLASSIFY_ANOMALIES_SAVE_PLOTS = settings.LUMINOSITY_CLASSIFY_ANOMALIES_SAVE_PLOTS
except:
    LUMINOSITY_CLASSIFY_ANOMALIES_SAVE_PLOTS = False

# Database configuration
config = {'user': settings.PANORAMA_DBUSER,
          'password': settings.PANORAMA_DBUSERPASS,
          'host': settings.PANORAMA_DBHOST,
          'port': settings.PANORAMA_DBPORT,
          'database': settings.PANORAMA_DATABASE,
          'raise_on_warnings': True}

skyline_app = 'luminosity'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

redis_conn = get_redis_conn(skyline_app)
redis_conn_decoded = get_redis_conn_decoded(skyline_app)


def classify_anomalies(i, classify_anomalies_set, start_timestamp, classify_for):

    logger = logging.getLogger(skyline_app_logger)
    debug_algorithms = False
    logger.info('classify_anomalies :: with start_timestamp - %s' % str(start_timestamp))
    start_classify_anomalies = timer()

    def mysql_insert(insert):
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
            logger.error('error :: classify_anomalies :: mysql error - %s' % str(err))
            logger.error('error :: classify_anomalies :: failed to connect to mysql')
            raise

        if cnx:
            try:
                cursor = cnx.cursor()
                cursor.execute(insert)
                inserted_id = cursor.rowcount
                # Make sure data is committed to the database
                cnx.commit()
                cursor.close()
                cnx.close()
                return inserted_id
            except mysql.connector.Error as err:
                logger.error('error :: classify_anomalies :: failed to insert record - mysql error - %s' % str(err))
                cnx.close()
                raise
        else:
            cnx.close()
            return False

        return False

    # Handle luminosity running with multiple processes
    def manage_processing_key(current_pid, base_name, timestamp, classify_for, action):
        result = False
        processing_key = 'luminosity.classify_anomalies.processing.%s.%s' % (
            str(timestamp), str(base_name))
        if action == 'add':
            key_exists = None
            try:
                key_exists = redis_conn_decoded.get(processing_key)
                if key_exists:
                    result = False
                    return result
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_anomalies :: failed to query Redis for %s' % (
                    processing_key))
            try:
                data = {'pid': current_pid, 'timestamp': int(time())}
                redis_conn.setex(processing_key, classify_for, str(data))
                result = True
                logger.info('classify_anomalies :: managing %s added %s with %s' % (
                    str(base_name), processing_key, str(data)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_anomalies :: failed to create key %s' % (
                    processing_key))
        if action == 'remove':
            try:
                redis_conn.delete(processing_key)
                result = True
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_anomalies :: failed to remove key %s' % (
                    processing_key))
        return result

    classify_anomalies_list = []
    for classify_anomaly in classify_anomalies_set:
        classify_anomalies_list.append(literal_eval(classify_anomaly))
    if classify_anomalies_list:
        classify_anomalies_list = sorted(classify_anomalies_list, key=lambda x: x[2], reverse=False)

    current_pid = getpid()

    anomalies_proceessed = 0
    for classify_anomaly in classify_anomalies_list:
        anomaly_data_dict = classify_anomaly[3]
        base_name = anomaly_data_dict['metric']
        timestamp = anomaly_data_dict['timestamp']
        # logger.debug('debug :: classify_anomalies :: %s' % str(classify_anomaly))

        anomalies_proceessed += 1

        # Handle luminosity running with multiple processes
        manage_metric = False
        try:
            manage_metric = manage_processing_key(current_pid, base_name, timestamp, classify_for, 'add')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: classify_anomalies :: failed to run manage_processing_key')
        if not manage_metric:
            logger.info('classify_anomalies :: skipping as processing key exists for %s' % base_name)
            continue

        # Remove anomaly if not classified in 1800 seconds
        if (int(time()) - 1800) > int(anomaly_data_dict['added_at']):
            logger.info('classify_anomalies :: anomaly not classified in 1800 seocnds, removing from luminosity.classify_anomalies')
            try:
                redis_conn.srem('luminosity.classify_anomalies', str(classify_anomaly))
                logger.info('classify_anomalies :: removed %s, %s, %s item from luminosity.classify_anomalies Redis set' % (
                    base_name, str(timestamp), anomaly_data_dict['app']))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_anomalies :: failed to remove %s, %s, %s item from luminosity.classify_anomalies Redis set' % (
                    base_name, str(timestamp), anomaly_data_dict['app']))
            try:
                manage_processing_key(current_pid, base_name, timestamp, classify_for, 'remove')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_anomalies :: failed to run manage_processing_key - %s' % base_name)
            continue

        metric_timeseries_dir = base_name.replace('.', '/')
        metric_training_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, timestamp,
            metric_timeseries_dir)
        anomaly_json = '%s/%s.json' % (metric_training_data_dir, base_name)
        timeseries = []
        # Try load training data
        if os.path.isfile(anomaly_json):
            logger.info('classify_anomalies :: anomaly_json found - %s' % anomaly_json)
            try:
                with open((anomaly_json), 'r') as f:
                    raw_timeseries = f.read()
                timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
                del raw_timeseries
                timeseries = literal_eval(timeseries_array_str)
                del timeseries_array_str
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_anomalies :: could not create timeseries from anomaly json %s' % anomaly_json)
            logger.info('classify_anomalies :: timeseries from anomaly_json has %s datapoints' % str(len(timeseries)))
        else:
            logger.info('classify_anomalies :: no anomaly_json not found removing %s from luminosity.classify_anomalies Redis set' % (
                base_name))
            try:
                redis_conn.srem('luminosity.classify_anomalies', str(classify_anomaly))
                logger.info('classify_anomalies :: removed %s, %s, %s item from luminosity.classify_anomalies Redis set' % (
                    base_name, str(timestamp), anomaly_data_dict['app']))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_anomalies :: failed to remove %s, %s, %s item from luminosity.classify_anomalies Redis set' % (
                    base_name, str(timestamp), anomaly_data_dict['app']))

        if not timeseries:
            try:
                manage_processing_key(current_pid, base_name, timestamp, classify_for, 'remove')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_anomalies :: failed to run manage_processing_key - %s' % base_name)
            continue

        # Classify anomaly or continue classifying metric
        window = 5
        window_timestamps = [ts for ts, value in timeseries[-window:]]
        algorithms_to_process = len(LUMINOSITY_CLASSIFY_ANOMALY_ALGORITHMS)
        algorithms_processed = 0
        algorithm_results = {}
        algorithms_processed_key = 'luminosity.classify_anomalies.algorithms_processed.%s.%s' % (
            str(timestamp), str(base_name))
        try:
            algorithm_results = redis_conn_decoded.get(algorithms_processed_key)
            if not algorithm_results:
                algorithm_results = {}
            else:
                algorithm_results = literal_eval(algorithm_results)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: classify_anomalies :: failed to query Redis for %s' % (
                algorithms_processed_key))
        if not algorithm_results:
            for algorithm in LUMINOSITY_CLASSIFY_ANOMALY_ALGORITHMS:
                algorithm_results[algorithm] = {}
                algorithm_results[algorithm]['processed'] = False
                algorithm_results[algorithm]['result'] = None
            try:
                redis_conn.setex(algorithms_processed_key, 300, str(algorithm_results))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_anomalies :: failed to set Redis key %s' % (
                    algorithms_processed_key))
        for algorithm in LUMINOSITY_CLASSIFY_ANOMALY_ALGORITHMS:
            if algorithm_results[algorithm]['processed']:
                algorithms_processed += 1
                logger.info('classify_anomalies :: %s at %s already processed with %s with result %s' % (
                    str(base_name), str(timestamp), algorithm,
                    str(algorithm_results[algorithm]['result'])))
                continue
            custom_algorithm = algorithm
            custom_algorithm_dict = {}
            custom_algorithm_dict['debug_logging'] = False
            debug_algorithm_logging = False
            if debug_algorithms:
                custom_algorithm_dict['debug_logging'] = True
                debug_algorithm_logging = True
            algorithm_source = '/opt/skyline/github/skyline/skyline/custom_algorithms/%s.py' % algorithm
            custom_algorithm_dict['algorithm_source'] = algorithm_source
            if LUMINOSITY_CLASSIFY_ANOMALIES_SAVE_PLOTS:
                custom_algorithm_dict['algorithm_parameters'] = {
                    'window': window, 'c': 6.0, 'return_anomalies': True,
                    'realtime_analysis': False,
                    'save_plots_to': metric_training_data_dir,
                    'save_plots_to_absolute_dir': True,
                    'filename_prefix': 'luminosity.classify_anomaly',
                    'debug_logging': debug_algorithm_logging,
                }
                custom_algorithm_dict['max_execution_time'] = 10.0
            else:
                custom_algorithm_dict['algorithm_parameters'] = {
                    'window': window, 'c': 6.0, 'return_anomalies': True,
                    'realtime_analysis': False,
                    'debug_logging': debug_algorithm_logging,
                }
                custom_algorithm_dict['max_execution_time'] = 5.0
            result = None
            anomalyScore = None
            anomalies = []
            try:
                result, anomalyScore, anomalies = run_custom_algorithm_on_timeseries(skyline_app, current_pid, base_name, timeseries, custom_algorithm, custom_algorithm_dict, debug_algorithms)
                logger.info('classify_anomalies :: run_custom_algorithm_on_timeseries run %s on %s with result - %s, anomalyScore - %s' % (
                    custom_algorithm, base_name, str(result), str(anomalyScore)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_anomalies :: failed to run custom_algorithm %s on %s' % (
                    custom_algorithm, base_name))
            triggered = False
            if anomalies:
                anomalies.reverse()
                for ts, value in anomalies:
                    if ts in window_timestamps:
                        triggered = True
                        break
                    if ts < window_timestamps[0]:
                        break
                if triggered:
                    logger.info('classify_anomalies :: %s triggered on %s within the window at %s' % (
                        custom_algorithm, base_name, str(ts)))
                else:
                    logger.info('classify_anomalies :: %s did not trigger on %s within the window' % (
                        custom_algorithm, base_name))
            algorithm_results[algorithm]['processed'] = True
            algorithm_results[algorithm]['result'] = triggered
            try:
                redis_conn.setex(algorithms_processed_key, 300, str(algorithm_results))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_anomalies :: failed to set Redis key %s' % (
                    algorithms_processed_key))
            algorithms_processed += 1

            time_now = time()
            runtime = time_now - start_timestamp
            if runtime >= (classify_for - 0.3):
                logger.info('classify_anomalies :: stopping before timeout is reached')
                break

        time_now = time()
        runtime = time_now - start_timestamp
        if runtime >= (classify_for - 0.3):
            logger.info('classify_anomalies :: stopping before timeout is reached')
            break

        anomaly_types = []
        results_recorded = False
        if algorithms_processed == algorithms_to_process:
            for algorithm in LUMINOSITY_CLASSIFY_ANOMALY_ALGORITHMS:
                if algorithm_results[algorithm]['result']:
                    anomaly_types.append(algorithm)
            if not anomaly_types:
                results_recorded = True
            else:
                logger.info('classify_anomalies :: anomaly_types identified for %s - %s' % (
                    base_name, str(anomaly_types)))
        anomaly_id = 0
        if anomaly_types:
            try:
                anomaly_id = get_anomaly_id(skyline_app, base_name, timestamp)
            except:
                logger.error('error :: classify_anomalies :: get_anomaly_id failed to determine id')
                anomaly_id = 0
        logger.info('classify_anomalies :: anomaly_id: %s' % (
            str(anomaly_id)))
        type_data = []
        if anomaly_id:
            query = 'SELECT id,algorithm,type FROM anomaly_types'
            try:
                results = mysql_select(skyline_app, query)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: querying MySQL - SELECT id,type FROM anomaly_types')
            db_anomaly_types = {}
            for id, associated_algorithm, anomaly_type in results:
                db_anomaly_types[associated_algorithm] = {}
                db_anomaly_types[associated_algorithm]['id'] = id
                db_anomaly_types[associated_algorithm]['type'] = anomaly_type
            metric_id = 0
            query = 'SELECT id FROM metrics WHERE metric=\'%s\'' % base_name
            try:
                results = mysql_select(skyline_app, query)
                for item in results:
                    metric_id = item[0]
                    break
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_anomalies :: querying MySQL - SELECT id FROM metrics WHERE metric=\'%s\'' % base_name)
            type_data = []
            for anomaly_type in anomaly_types:
                type_data.append(int(db_anomaly_types[anomaly_type]['id']))
        logger.info('classify_anomalies :: type_data: %s' % (
            str(type_data)))

        classification_exists = None
        if type_data and anomaly_id:
            query = 'SELECT metric_id FROM anomalies_type WHERE id=%s' % anomaly_id
            try:
                results = mysql_select(skyline_app, query)
                for item in results:
                    classification_exists = item[0]
                    break
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_anomalies :: querying MySQL - SELECT metric_id FROM anomalies_type WHERE id=%s' % anomaly_id)
        if classification_exists:
            try:
                redis_conn.srem('luminosity.classify_anomalies', str(classify_anomaly))
                logger.info('classify_anomalies :: results already recorded for metric_id %s so removed %s, %s, %s item from luminosity.classify_anomalies Redis set' % (
                    str(classification_exists), base_name, str(timestamp),
                    anomaly_data_dict['app']))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_anomalies :: after results recorded failed to remove %s, %s, %s item from luminosity.classify_anomalies Redis set' % (
                    base_name, str(timestamp), anomaly_data_dict['app']))
            type_data = None

        if type_data:
            type_data_str = ''
            for id in type_data:
                if type_data_str == '':
                    type_data_str = '%s' % str(id)
                else:
                    type_data_str = '%s,%s' % (type_data_str, str(id))
            ins_values = '(%s,%s,\'%s\')' % (str(anomaly_id), str(metric_id), type_data_str)
            values_string = 'INSERT INTO anomalies_type (id, metric_id, type) VALUES %s' % ins_values
            try:
                results_recorded = mysql_insert(values_string)
                logger.debug('debug :: classify_anomalies :: INSERT: %s' % (
                    str(values_string)))
                logger.debug('debug :: classify_anomalies :: results_recorded: %s' % (
                    str(results_recorded)))
            except Exception as e:
                # Handle a process updating on SystemExit
                if 'Duplicate entry' in str(e):
                    results_recorded = True
                    logger.info('classify_anomalies :: a entry already exists in anomalies_type for anomaly id %s on %s, OK' % (
                        str(anomaly_id), str(base_name)))
                else:
                    logger.error(traceback.format_exc())
                    logger.error('error :: MySQL insert - %s' % str(values_string))
                    results_recorded = 0
            if results_recorded:
                logger.info('classify_anomalies :: added %s row to anomalies_type for anomaly id %s on %s - %s' % (
                    str(results_recorded), str(anomaly_id), base_name,
                    str(type_data)))
        if results_recorded:
            try:
                redis_conn.srem('luminosity.classify_anomalies', str(classify_anomaly))
                logger.info('classify_anomalies :: results recorded so removed %s, %s, %s item from luminosity.classify_anomalies Redis set' % (
                    base_name, str(timestamp), anomaly_data_dict['app']))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: classify_anomalies :: after results recorded failed to remove %s, %s, %s item from luminosity.classify_anomalies Redis set' % (
                    base_name, str(timestamp), anomaly_data_dict['app']))

        try:
            manage_processing_key(current_pid, base_name, timestamp, classify_for, 'remove')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: classify_anomalies :: failed to run manage_processing_key - %s' % base_name)

    end_classify_anomalies = timer()
    logger.info('classify_anomalies :: %s anomalies were processed, took %.6f seconds' % (
        str(anomalies_proceessed),
        (end_classify_anomalies - start_classify_anomalies)))

    return
