import logging
from os import path
import string
import operator
import time
import re

import traceback
from flask import request
# import mysql.connector
# from mysql.connector import errorcode

# @added 20180720 - Feature #2464: luminosity_remote_data
# Added redis and msgpack
from redis import StrictRedis
from msgpack import Unpacker

import numpy as np

import settings
from skyline_functions import (
    mysql_select,
    # @added 20180720 - Feature #2464: luminosity_remote_data
    nonNegativeDerivative, in_list, is_derivative_metric)

import skyline_version
skyline_version = skyline_version.__absolute_version__

skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

REQUEST_ARGS = ['from_date',
                'from_time',
                'from_timestamp',
                'until_date',
                'until_time',
                'until_timestamp',
                'target',
                'like_target',
                'source',
                'host',
                'algorithm',
                # @added 20161127 - Branch #922: ionosphere
                'panorama_anomaly_id',
                ]

# Converting one settings variable into a local variable, just because it is a
# long string otherwise.
try:
    ENABLE_WEBAPP_DEBUG = settings.ENABLE_WEBAPP_DEBUG
except:
    logger.error('error :: cannot determine ENABLE_WEBAPP_DEBUG from settings')
    ENABLE_WEBAPP_DEBUG = False

# @added 20180720 - Feature #2464: luminosity_remote_data
# Added REDIS_CONN
if settings.REDIS_PASSWORD:
    REDIS_CONN = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
else:
    REDIS_CONN = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)


def panorama_request():
    """
    Gets the details of anomalies from the database, using the URL arguments
    that are passed in by the :obj:`request.args` to build the MySQL select
    query string and queries the database, parse the results and creates an
    array of the anomalies that matched the query and creates the
    ``panaroma.json`` file, then returns the array.  The Webapp needs both the
    array and the JSONP file to serve to the browser for the client side
    ``panaroma.js``.

    :param None: determined from :obj:`request.args`
    :return: array
    :rtype: array

    .. note:: And creates ``panaroma.js`` for client side javascript

    """

    logger.info('determining request args')

    def get_ids_from_rows(thing, rows):
        found_ids = []
        for row in rows:
            found_id = str(row[0])
            found_ids.append(int(found_id))

        ids_first = string.replace(str(found_ids), '[', '')
        in_ids = string.replace(str(ids_first), ']', '')
        return in_ids

    try:
        request_args_len = len(request.args)
    except:
        request_args_len = False

    latest_anomalies = False
    if request_args_len == 0:
        request_args_len = 'No request arguments passed'
        # return str(request_args_len)
        latest_anomalies = True

    metric = False
    if metric:
        logger.info('Getting db id for %s' % metric)
        # @modified 20170913 - Task #2160: Test skyline with bandit
        # Added nosec to exclude from bandit tests
        query = 'select id from metrics WHERE metric=\'%s\'' % metric  # nosec
        try:
            result = mysql_select(skyline_app, query)
        except:
            logger.error('error :: failed to get id from db: %s' % traceback.format_exc())
            result = 'metric id not found in database'

        return str(result[0][0])

    search_request = True
    count_request = False

    if latest_anomalies:
        logger.info('Getting latest anomalies')
        query = 'select id, metric_id, anomalous_datapoint, anomaly_timestamp, full_duration, created_timestamp from anomalies ORDER BY id DESC LIMIT 10'
        try:
            rows = mysql_select(skyline_app, query)
        except:
            logger.error('error :: failed to get anomalies from db: %s' % traceback.format_exc())
            rows = []

    if not latest_anomalies:
        logger.info('Determining search parameters')
        query_string = 'select id, metric_id, anomalous_datapoint, anomaly_timestamp, full_duration, created_timestamp from anomalies'
        needs_and = False

        # If we have to '' a string we cannot escape the query it seems...
        do_not_escape = False
        if 'metric' in request.args:
            metric = request.args.get('metric', None)
            if metric and metric != 'all':
                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                query = "select id from metrics WHERE metric='%s'" % (metric)  # nosec
                try:
                    found_id = mysql_select(skyline_app, query)
                except:
                    logger.error('error :: failed to get app ids from db: %s' % traceback.format_exc())
                    found_id = None

                if found_id:
                    target_id = str(found_id[0][0])
                    if needs_and:
                        new_query_string = '%s AND metric_id=%s' % (query_string, target_id)
                    else:
                        new_query_string = '%s WHERE metric_id=%s' % (query_string, target_id)
                    query_string = new_query_string
                    needs_and = True

        if 'metric_like' in request.args:
            metric_like = request.args.get('metric_like', None)
            if metric_like and metric_like != 'all':
                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                query = 'select id from metrics WHERE metric LIKE \'%s\'' % (str(metric_like))  # nosec
                try:
                    rows = mysql_select(skyline_app, query)
                except:
                    logger.error('error :: failed to get metric ids from db: %s' % traceback.format_exc())
                    return False

                rows_returned = None
                try:
                    rows_returned = rows[0]
                    if ENABLE_WEBAPP_DEBUG:
                        logger.info('debug :: rows - rows[0] - %s' % str(rows[0]))
                except:
                    rows_returned = False
                    if ENABLE_WEBAPP_DEBUG:
                        logger.info('debug :: no rows returned')

                if rows_returned:
                    ids = get_ids_from_rows('metric', rows)
                    new_query_string = '%s WHERE metric_id IN (%s)' % (query_string, str(ids))
                else:
                    # Get nothing
                    new_query_string = '%s WHERE metric_id IN (0)' % (query_string)
                    if ENABLE_WEBAPP_DEBUG:
                        logger.info('debug :: no rows returned using new_query_string - %s' % new_query_string)

                query_string = new_query_string
                needs_and = True

        if 'count_by_metric' in request.args:
            count_by_metric = request.args.get('count_by_metric', None)
            if count_by_metric and count_by_metric != 'false':
                search_request = False
                count_request = True
                # query_string = 'SELECT metric_id, COUNT(*) FROM anomalies GROUP BY metric_id ORDER BY COUNT(*) DESC'
                query_string = 'SELECT metric_id, COUNT(*) FROM anomalies'
                needs_and = False

        if 'from_timestamp' in request.args:
            from_timestamp = request.args.get('from_timestamp', None)
            if from_timestamp and from_timestamp != 'all':

                if ":" in from_timestamp:
                    import time
                    import datetime
                    new_from_timestamp = time.mktime(datetime.datetime.strptime(from_timestamp, '%Y%m%d %H:%M').timetuple())
                    from_timestamp = str(int(new_from_timestamp))

                if needs_and:
                    new_query_string = '%s AND anomaly_timestamp >= %s' % (query_string, from_timestamp)
                    query_string = new_query_string
                    needs_and = True
                else:
                    new_query_string = '%s WHERE anomaly_timestamp >= %s' % (query_string, from_timestamp)
                    query_string = new_query_string
                    needs_and = True

        if 'until_timestamp' in request.args:
            until_timestamp = request.args.get('until_timestamp', None)
            if until_timestamp and until_timestamp != 'all':
                if ":" in until_timestamp:
                    import time
                    import datetime
                    new_until_timestamp = time.mktime(datetime.datetime.strptime(until_timestamp, '%Y%m%d %H:%M').timetuple())
                    until_timestamp = str(int(new_until_timestamp))

                if needs_and:
                    new_query_string = '%s AND anomaly_timestamp <= %s' % (query_string, until_timestamp)
                    query_string = new_query_string
                    needs_and = True
                else:
                    new_query_string = '%s WHERE anomaly_timestamp <= %s' % (query_string, until_timestamp)
                    query_string = new_query_string
                    needs_and = True

        if 'app' in request.args:
            app = request.args.get('app', None)
            if app and app != 'all':
                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                query = 'select id from apps WHERE app=\'%s\'' % (str(app))  # nosec
                try:
                    found_id = mysql_select(skyline_app, query)
                except:
                    logger.error('error :: failed to get app ids from db: %s' % traceback.format_exc())
                    found_id = None

                if found_id:
                    target_id = str(found_id[0][0])
                    if needs_and:
                        new_query_string = '%s AND app_id=%s' % (query_string, target_id)
                    else:
                        new_query_string = '%s WHERE app_id=%s' % (query_string, target_id)

                    query_string = new_query_string
                    needs_and = True

        if 'source' in request.args:
            source = request.args.get('source', None)
            if source and source != 'all':
                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                query = 'select id from sources WHERE source=\'%s\'' % (str(source))  # nosec
                try:
                    found_id = mysql_select(skyline_app, query)
                except:
                    logger.error('error :: failed to get source id from db: %s' % traceback.format_exc())
                    found_id = None

                if found_id:
                    target_id = str(found_id[0][0])
                    if needs_and:
                        new_query_string = '%s AND source_id=\'%s\'' % (query_string, target_id)
                    else:
                        new_query_string = '%s WHERE source_id=\'%s\'' % (query_string, target_id)

                    query_string = new_query_string
                    needs_and = True

        if 'algorithm' in request.args:
            algorithm = request.args.get('algorithm', None)

            # DISABLED as it is difficult match algorithm_id in the
            # triggered_algorithms csv list
            algorithm = 'all'
            if algorithm and algorithm != 'all':
                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                query = 'select id from algorithms WHERE algorithm LIKE \'%s\'' % (str(algorithm))  # nosec
                try:
                    rows = mysql_select(skyline_app, query)
                except:
                    logger.error('error :: failed to get algorithm ids from db: %s' % traceback.format_exc())
                    rows = []

                ids = get_ids_from_rows('algorithm', rows)

                if needs_and:
                    new_query_string = '%s AND algorithm_id IN (%s)' % (query_string, str(ids))
                else:
                    new_query_string = '%s WHERE algorithm_id IN (%s)' % (query_string, str(ids))
                query_string = new_query_string
                needs_and = True

        if 'host' in request.args:
            host = request.args.get('host', None)
            if host and host != 'all':
                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                query = 'select id from hosts WHERE host=\'%s\'' % (str(host))  # nosec
                try:
                    found_id = mysql_select(skyline_app, query)
                except:
                    logger.error('error :: failed to get host id from db: %s' % traceback.format_exc())
                    found_id = None

                if found_id:
                    target_id = str(found_id[0][0])
                    if needs_and:
                        new_query_string = '%s AND host_id=\'%s\'' % (query_string, target_id)
                    else:
                        new_query_string = '%s WHERE host_id=\'%s\'' % (query_string, target_id)
                    query_string = new_query_string
                    needs_and = True

        if 'limit' in request.args:
            limit = request.args.get('limit', '10')
        else:
            limit = '10'

        if 'order' in request.args:
            order = request.args.get('order', 'DESC')
        else:
            order = 'DESC'

        search_query = '%s ORDER BY id %s LIMIT %s' % (
            query_string, order, limit)

        if 'count_by_metric' in request.args:
            count_by_metric = request.args.get('count_by_metric', None)
            if count_by_metric and count_by_metric != 'false':
                # query_string = 'SELECT metric_id, COUNT(*) FROM anomalies GROUP BY metric_id ORDER BY COUNT(*) DESC'
                search_query = '%s GROUP BY metric_id ORDER BY COUNT(*) %s LIMIT %s' % (
                    query_string, order, limit)

        try:
            rows = mysql_select(skyline_app, search_query)
        except:
            logger.error('error :: failed to get anomalies from db: %s' % traceback.format_exc())
            rows = []

    anomalies = []
    anomalous_metrics = []

    if search_request:
        anomalies_json = path.abspath(path.join(path.dirname(__file__), '..', settings.ANOMALY_DUMP))
        panorama_json = string.replace(str(anomalies_json), 'anomalies.json', 'panorama.json')
        if ENABLE_WEBAPP_DEBUG:
            logger.info('debug ::  panorama_json - %s' % str(panorama_json))

    for row in rows:
        if search_request:
            anomaly_id = str(row[0])
            metric_id = str(row[1])
        if count_request:
            metric_id = str(row[0])
            anomaly_count = str(row[1])

        # @modified 20170913 - Task #2160: Test skyline with bandit
        # Added nosec to exclude from bandit tests
        query = 'select metric from metrics WHERE id=%s' % metric_id  # nosec
        try:
            result = mysql_select(skyline_app, query)
        except:
            logger.error('error :: failed to get id from db: %s' % traceback.format_exc())
            continue

        metric = str(result[0][0])
        if search_request:
            anomalous_datapoint = str(row[2])
            anomaly_timestamp = str(row[3])
            full_duration = str(row[4])
            created_timestamp = str(row[5])
            anomaly_data = (anomaly_id, metric, anomalous_datapoint, anomaly_timestamp, full_duration, created_timestamp)
            anomalies.append([int(anomaly_id), str(metric), anomalous_datapoint, anomaly_timestamp, full_duration, created_timestamp])
            anomalous_metrics.append(str(metric))
        if count_request:
            limit_argument = anomaly_count
            if int(anomaly_count) > 100:
                limit_argument = 100
            anomaly_data = (int(anomaly_count), metric, str(limit_argument))
            anomalies.append([int(anomaly_count), str(metric), str(limit_argument)])

    anomalies.sort(key=operator.itemgetter(int(0)))

    if search_request:
        with open(panorama_json, 'w') as fh:
            pass

        # Write anomalous_metrics to static webapp directory
        with open(panorama_json, 'a') as fh:
            # Make it JSONP with a handle_data() function
            fh.write('handle_data(%s)' % anomalies)

    if latest_anomalies:
        return anomalies
    else:
        return search_query, anomalies


def get_list(thing):
    """
    Get a list of names for things in a database table.

    :param thing: the thing, e.g. 'algorithm'
    :type thing: str
    :return: list
    :rtype: list

    """
    table = '%ss' % thing
    # @modified 20170913 - Task #2160: Test skyline with bandit
    # Added nosec to exclude from bandit tests
    query = 'select %s from %s' % (thing, table)  # nosec
    logger.info('select %s from %s' % (thing, table))  # nosec
    got_results = False
    try:
        results = mysql_select(skyline_app, query)
        got_results = True
    except:
        logger.error('error :: failed to get list of %ss from %s' % (thing, table))
        results = None

    things = []
    results_array_valid = False
    try:
        test_results = results[0]
        results_array_valid = True
    except:
        logger.error('error :: invalid results array for get list of %ss from %s' % (thing, table))

    if results_array_valid:
        logger.info('results: %s' % str(results))
        for result in results:
            things.append(str(result[0]))
        logger.info('things: %s' % str(things))

    return things


# @added 20180720 - Feature #2464: luminosity_remote_data
def luminosity_remote_data(anomaly_timestamp):
    """
    Gets all the unique_metrics from Redis and then mgets Redis data for all
    metrics.  The data is then preprocessed for the remote Skyline luminosity
    instance and only the relevant fragments of the time series are
    returned.  This return is then gzipped by the Flask Webapp response to
    ensure the minimum about of bandwidth is used.

    :param anomaly_timestamp: the anomaly timestamp
    :type anomaly_timestamp: int
    :return: list
    :rtype: list

    """

    message = 'luminosity_remote_data returned'
    success = False
    luminosity_data = []
    logger.info('luminosity_remote_data :: determining unique_metrics')
    unique_metrics = []
    # If you modify the values of 61 or 600 here, it must be modified in the
    # luminosity_remote_data function in
    # skyline/luminosity/process_correlations.py as well
    from_timestamp = int(anomaly_timestamp) - 600
    until_timestamp = int(anomaly_timestamp) + 61

    try:
        unique_metrics = list(REDIS_CONN.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
    except Exception as e:
        logger.error('error :: %s' % str(e))
        logger.error('error :: luminosity_remote_data :: could not determine unique_metrics from Redis set')
    if not unique_metrics:
        message = 'error :: luminosity_remote_data :: could not determine unique_metrics from Redis set'
        return luminosity_data, success, message
    logger.info('luminosity_remote_data :: %s unique_metrics' % str(len(unique_metrics)))

    # assigned metrics
    assigned_min = 0
    assigned_max = len(unique_metrics)
    assigned_keys = range(assigned_min, assigned_max)

    # Compile assigned metrics
    assigned_metrics = [unique_metrics[index] for index in assigned_keys]
    # Check if this process is unnecessary
    if len(assigned_metrics) == 0:
        message = 'error :: luminosity_remote_data :: assigned_metrics length is 0'
        logger.error(message)
        return luminosity_data, success, message

    # Multi get series
    raw_assigned_failed = True
    try:
        raw_assigned = REDIS_CONN.mget(assigned_metrics)
        raw_assigned_failed = False
    except:
        logger.info(traceback.format_exc())
        message = 'error :: luminosity_remote_data :: failed to mget raw_assigned'
        logger.error(message)
        return luminosity_data, success, message
    if raw_assigned_failed:
        message = 'error :: luminosity_remote_data :: failed to mget raw_assigned'
        logger.error(message)
        return luminosity_data, success, message

    # Distill timeseries strings into lists
    for i, metric_name in enumerate(assigned_metrics):
        timeseries = []
        try:
            raw_series = raw_assigned[i]
            unpacker = Unpacker(use_list=False)
            unpacker.feed(raw_series)
            timeseries = list(unpacker)
        except:
            timeseries = []

        if not timeseries:
            continue

        # Convert the time series if this is a known_derivative_metric
        base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
        known_derivative_metric = is_derivative_metric('webapp', base_name)
        if known_derivative_metric:
            try:
                derivative_timeseries = nonNegativeDerivative(timeseries)
                timeseries = derivative_timeseries
            except:
                logger.error('error :: nonNegativeDerivative failed')

        correlate_ts = []
        for ts, value in timeseries:
            if int(ts) < from_timestamp:
                continue
            if int(ts) <= anomaly_timestamp:
                correlate_ts.append((int(ts), value))
            if int(ts) > (anomaly_timestamp + until_timestamp):
                break
        if not correlate_ts:
            continue
        metric_data = [str(metric_name), correlate_ts]
        luminosity_data.append(metric_data)

    logger.info('luminosity_remote_data :: %s valid metric time series data preprocessed for the remote request' % str(len(luminosity_data)))

    return luminosity_data, success, message

# @added 20181119 - Feature #2714: webapp - now - preprocess time series
# Allow for the time series to be aggregated by median or sum per interval
# period
def aggregate_timeseries(base_name, timeseries, aggregate_by):
    """
    Allow for the time series to be aggregated by median or sum
    per minute so that webapp can return a reasonable number of data points for
    dyngraph to load and display in the browser without causing lag.  This is
    achieved by aggregating the time series using either the median of values
    per minute or the sum.

    :param base_name: the base_name of the metric
    :type base_name: str
    :param timeseries: the time series to aggregate
    :type timeseries: list
    :param aggregate_by: the method to be used to aggregate the time series by
    :type aggregate_by: str
    :return: (list, str, str)
    :rtype: tuple

    """

    import pandas as pd
    trace = 'none'
    fail_msg = 'none'

    aggregated_timeseries = []
    logger.info('aggregate_timeseries :: aggregating time series for %s by %s at a 60 second interval' % (
        base_name, aggregate_by))

    if len(timeseries) < 60:
        logger.info('aggregate_timeseries :: time series for %s less than 60 datapoints, too short' % base_name)
        trace = 'none'
        fail_msg = 'aggregate_timeseries :: time series for %s less than 60 datapoints, too short' % base_name
        return aggregated_timeseries, trace, fail_msg

    try:
        epoch_timeseries = []
        for ts, value in timeseries:
            epoch_timeseries.append((int(ts), value))
        del timeseries
        df = pd.DataFrame(epoch_timeseries)
        df.columns = ['ts', 'value']
        df['ts'] = pd.to_datetime(df['ts'],unit='s')
        aggregate_by = 'median'
        if aggregate_by == 'median':
            aggregated_df = df.resample('1T', on='ts').median()
        if aggregate_by == 'sum':
            aggregated_df = df.resample('1T', on='ts').sum()
        del df
        epoch_aggregated_df = aggregated_df.index.astype(np.int64) // 10 ** 9
        value_aggregated_df = aggregated_df['value'].to_frame()
        del aggregated_df
        timestamps = []
        for index, ts in epoch_aggregated_df.iterrows():
            timestamps.append(ts[0])
        del epoch_aggregated_df
        values = []
        for index, value in value_aggregated_df.iterrows():
            values.append(value[0])
        del value_aggregated_df
        aggregated_timeseries = []
        for ts, value in zip(timestamps, values):
            aggregated_timeseries.append((ts, value))
        ended_at = time.time()
        seconds_to_run = int(ended_at) - int(started_at)
        logger.info('aggregate_timeseries :: aggregated time series for %s in %s seconds' % (
            base_name, str(seconds_to_run)))
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('aggregate_timeseries :: failed to aggregate time series for %s' % base_name)
        fail_msg = 'aggregate_timeseries :: failed to aggregate time series for %s' % base_name
        return aggregated_timeseries, trace, fail_msg

    return aggregated_timeseries, fail_msg, trace
