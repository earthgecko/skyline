import logging
import traceback
from os import path
import string
import operator

from flask import request
# import mysql.connector
# from mysql.connector import errorcode

import settings
from skyline_functions import get_graphite_metric, mysql_select

import skyline_version
skyline_version = skyline_version.__absolute_version__

skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

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
                ]

# Converting one settings variable into a local variable, just because it is a
# long string otherwise.
try:
    ENABLE_WEBAPP_DEBUG = settings.ENABLE_PANORAMA_DEBUG
except:
    logger.error('error :: cannot determine ENABLE_PANORAMA_DEBUG from settings' % skyline_app)
    ENABLE_WEBAPP_DEBUG = False


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
        query = 'select id from metrics WHERE metric=\'%s\'' % metric
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
                query = "select id from metrics WHERE metric='%s'" % (metric)
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
                query = 'select id from metrics WHERE metric LIKE \'%s\'' % (str(metric_like))
                try:
                    rows = mysql_select(skyline_app, query)
                except:
                    logger.error('error :: failed to get metric ids from db: %s' % traceback.format_exc())
                    return False

                ids = get_ids_from_rows('metric', rows)
                new_query_string = '%s WHERE metric_id IN (%s)' % (query_string, str(ids))
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
                query = 'select id from apps WHERE app=\'%s\'' % (str(app))
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
                query = 'select id from sources WHERE source=\'%s\'' % (str(source))
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
                query = 'select id from algorithms WHERE algorithm LIKE \'%s\'' % (str(algorithm))
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
                query = 'select id from hosts WHERE host=\'%s\'' % (str(host))
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

        query = 'select metric from metrics WHERE id=%s' % metric_id
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
            if int(anomaly_count) > 200:
                limit_argument = 200
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
    :return: array
    :rtype: array, boolean

    """
    table = '%ss' % thing
    query = 'select %s from %s' % (thing, table)
    logger.info('select %s from %s' % (thing, table))
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
