import logging
from os import path
import operator
import time

import traceback

from ast import literal_eval

from flask import request
# import mysql.connector
# from mysql.connector import errorcode

# @added 20180720 - Feature #2464: luminosity_remote_data
# Added redis and msgpack
from redis import StrictRedis
from msgpack import Unpacker

# @added 20201103 - Feature #3824: get_cluster_data
import requests

# @added 20201125 - Feature #3850: webapp - yhat_values API endoint
import numpy as np

# @added 20210328 - Feature #3994: Panorama - mirage not anomalous
import pandas as pd

import settings
from skyline_functions import (
    mysql_select,
    # @added 20180720 - Feature #2464: luminosity_remote_data
    # nonNegativeDerivative, in_list, is_derivative_metric,
    # @added 20200507 - Feature #3532: Sort all time series
    # Added sort_timeseries and removed unused in_list
    nonNegativeDerivative, sort_timeseries,
    # @added 20201123 - Feature #3824: get_cluster_data
    #                   Feature #2464: luminosity_remote_data
    #                   Bug #3266: py3 Redis binary objects not strings
    #                   Branch #3262: py3
    get_redis_conn_decoded,
    # @added 20201125 - Feature #3850: webapp - yhat_values API endoint
    get_graphite_metric,
    # @added 20210328 - Feature #3994: Panorama - mirage not anomalous
    filesafe_metricname, mkdir_p)

# @added 20210420  - Task #4022: Move mysql_select calls to SQLAlchemy
# from database_queries import (
#    db_query_metric_id_from_base_name, db_query_latest_anomalies,
#    db_query_metric_ids_from_metric_like)
# @added 20210420  - Task #4022: Move mysql_select calls to SQLAlchemy
#                    Task #4030: refactoring
from functions.database.queries.metric_id_from_base_name import metric_id_from_base_name
from functions.database.queries.metric_ids_from_metric_like import metric_ids_from_metric_like
from functions.database.queries.latest_anomalies import latest_anomalies as db_latest_anomalies

# @added 20210617 - Feature #4144: webapp - stale_metrics API endpoint
#                   Feature #4076: CUSTOM_STALE_PERIOD
#                   Branch #1444: thunder
from functions.thunder.stale_metrics import thunder_stale_metrics

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
except Exception as e:
    logger.error('error :: cannot determine ENABLE_WEBAPP_DEBUG from settings - %s' % e)
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

        # @modified 20191014 - Task #3270: Deprecate string.replace for py3
        #                      Branch #3262: py3
        # ids_first = string.replace(str(found_ids), '[', '')
        # in_ids = string.replace(str(ids_first), ']', '')
        found_ids_str = str(found_ids)
        ids_first = found_ids_str.replace('[', '')
        in_ids = ids_first.replace(']', '')

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
    # @modified 20210504  - Task #4030: refactoring
    #                       Task #4022: Move mysql_select calls to SQLAlchemy
    # if metric
    #     logger.info('Getting db id for %s' % metric)
    #     # @modified 20170913 - Task #2160: Test skyline with bandit
    #     # Added nosec to exclude from bandit tests
    #     query = 'select id from metrics WHERE metric=\'%s\'' % metric  # nosec
    #     try:
    #         result = mysql_select(skyline_app, query)
    #     except:
    #         logger.error('error :: failed to get id from db: %s' % traceback.format_exc())
    #         result = 'metric id not found in database'
    #     return str(result[0][0])

    search_request = True
    count_request = False

    if latest_anomalies:
        logger.info('Getting latest anomalies')
        # @modified 20191108 - Feature #3306: Record the anomaly_end_timestamp
        #                      Branch #3262: py3
        # query = 'select id, metric_id, anomalous_datapoint, anomaly_timestamp, full_duration, created_timestamp from anomalies ORDER BY id DESC LIMIT 10'
        # @modified 20210420  - Task #4022: Move mysql_select calls to SQLAlchemy
        # query = 'select id, metric_id, anomalous_datapoint, anomaly_timestamp, full_duration, created_timestamp, anomaly_end_timestamp from anomalies ORDER BY id DESC LIMIT 10'
        # try:
        #     rows = mysql_select(skyline_app, query)
        # except:
        #     logger.error('error :: failed to get anomalies from db: %s' % traceback.format_exc())
        #     rows = []
        # @added 20210420  - Task #4022: Move mysql_select calls to SQLAlchemy
        rows = []
        try:
            rows = db_latest_anomalies(skyline_app)
        except:
            logger.error('error :: failed to get anomalies from db: %s' % traceback.format_exc())
            rows = []

    if not latest_anomalies:
        logger.info('Determining search parameters')
        # @modified 20191108 - Feature #3306: Record the end_timestamp of anomalies
        #                      Branch #3262: py3
        # query_string = 'select id, metric_id, anomalous_datapoint, anomaly_timestamp, full_duration, created_timestamp from anomalies'
        query_string = 'select id, metric_id, anomalous_datapoint, anomaly_timestamp, full_duration, created_timestamp, anomaly_end_timestamp from anomalies'

        needs_and = False

        # If we have to '' a string we cannot escape the query it seems...
        # do_not_escape = False
        if 'metric' in request.args:
            metric = request.args.get('metric', None)
            # if metric and metric != 'all':
            if isinstance(metric, str) and metric != 'all':
                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                # @modified 20210420  - Task #4022: Move mysql_select calls to SQLAlchemy
                # query = "select id from metrics WHERE metric='%s'" % (metric)  # nosec
                # try:
                #     found_id = mysql_select(skyline_app, query)
                # except:
                #     logger.error('error :: failed to get app ids from db: %s' % traceback.format_exc())
                #     found_id = None
                # @added 20210420  - Task #4022: Move mysql_select calls to SQLAlchemy
                found_id = None
                if metric.startswith(settings.FULL_NAMESPACE):
                    base_name = metric.replace(settings.FULL_NAMESPACE, '', 1)
                else:
                    base_name = str(metric)
                try:
                    # found_id = db_query_metric_id_from_base_name(skyline_app, base_name)
                    found_id = metric_id_from_base_name(skyline_app, base_name)
                except:
                    logger.error('error :: failed to get metric id from db: %s' % traceback.format_exc())
                    found_id = None
                if found_id:
                    # target_id = str(found_id[0][0])
                    target_id = str(found_id)
                    if needs_and:
                        new_query_string = '%s AND metric_id=%s' % (query_string, target_id)
                    else:
                        new_query_string = '%s WHERE metric_id=%s' % (query_string, target_id)
                    query_string = new_query_string
                    needs_and = True

        # in_ids_str = None
        if 'metric_like' in request.args:
            metric_like = request.args.get('metric_like', None)
            metrics_like_str = None
            # if metric_like and metric_like != 'all':
            if isinstance(metric_like, str) and metric_like != 'all':
                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                rows_returned = None
                # @modified 20210420  - Task #4022: Move mysql_select calls to SQLAlchemy
                # query = 'select id from metrics WHERE metric LIKE \'%s\'' % (str(metric_like))  # nosec
                # try:
                #     rows = mysql_select(skyline_app, query)
                # except:
                #     logger.error('error :: failed to get metric ids from db: %s' % traceback.format_exc())
                #     return False
                # rows_returned = None
                # try:
                #     rows_returned = rows[0]
                #     if ENABLE_WEBAPP_DEBUG:
                #         logger.info('debug :: rows - rows[0] - %s' % str(rows[0]))
                # except:
                #     rows_returned = False
                #     if ENABLE_WEBAPP_DEBUG:
                #         logger.info('debug :: no rows returned')

                # @added 20210420  - Task #4022: Move mysql_select calls to SQLAlchemy
                metrics_like_str = str(metric_like)
                db_metric_ids = None
                try:
                    db_metric_ids = metric_ids_from_metric_like(skyline_app, metrics_like_str)
                except Exception as e:
                    logger.error('error :: failed to get metric ids from db: %s' % e)
                    return False
                use_db_metric_ids = True
                if db_metric_ids and use_db_metric_ids:
                    rows_returned = False
                    ids = ''
                    for db_metric_id in db_metric_ids:
                        if ids == '':
                            ids = '%s' % str(db_metric_id)
                        else:
                            ids = '%s, %s' % (ids, str(db_metric_id))
                    new_query_string = '%s WHERE metric_id IN (%s)' % (query_string, str(ids))
                else:
                    # Get nothing
                    new_query_string = '%s WHERE metric_id IN (0)' % (query_string)
                    if ENABLE_WEBAPP_DEBUG:
                        logger.info('debug :: no rows returned using new_query_string - %s' % new_query_string)
                if not use_db_metric_ids:
                    if rows_returned:
                        ids = get_ids_from_rows('metric', rows)
                        new_query_string = '%s WHERE metric_id IN (%s)' % (query_string, str(ids))

                        logger.info('debug :: id is %s chars long after adding get_ids_from_rows, new_query_string: %s' % (
                            str(len(ids)), new_query_string))

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
                    # @modified 20211021 - handle multiple date formats
                    try:
                        new_from_timestamp = time.mktime(datetime.datetime.strptime(from_timestamp, '%Y%m%d %H:%M').timetuple())
                    except ValueError:
                        new_from_timestamp = time.mktime(datetime.datetime.strptime(from_timestamp, '%Y-%m-%d %H:%M').timetuple())
                    except Exception as err:
                        trace = traceback.format_exc()
                        logger.error('%s' % trace)
                        fail_msg = 'error :: panorama_request :: failed to unix timestamp from from_timestamp - %s' % str(err)
                        logger.error('%s' % fail_msg)
                        raise  # to webapp to return in the UI

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
                    # @modified 20211021 - handle multiple date formats
                    try:
                        new_until_timestamp = time.mktime(datetime.datetime.strptime(until_timestamp, '%Y%m%d %H:%M').timetuple())
                    except ValueError:
                        new_until_timestamp = time.mktime(datetime.datetime.strptime(until_timestamp, '%Y-%m-%d %H:%M').timetuple())
                    except Exception as err:
                        trace = traceback.format_exc()
                        logger.error('%s' % trace)
                        fail_msg = 'error :: panorama_request :: failed to unix timestamp from until_timestamp - %s' % str(err)
                        logger.error('%s' % fail_msg)
                        raise  # to webapp to return in the UI

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
                # @added 20210504  - Task #4030: refactoring
                #                    Task #4022: Move mysql_select calls to SQLAlchemy
                # Sanitise variable
                if isinstance(app, str):
                    for_app = str(app)
                else:
                    for_app = 'none'

                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                # @modified 20210504  - Task #4030: refactoring
                #                       Task #4022: Move mysql_select calls to SQLAlchemy
                # query = 'select id from apps WHERE app=\'%s\'' % (str(app))  # nosec
                query = 'select id from apps WHERE app=\'%s\'' % (str(for_app))
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

                # @added 20210504  - Task #4030: refactoring
                #                    Task #4022: Move mysql_select calls to SQLAlchemy
                # Sanitise variable
                if isinstance(source, str):
                    for_source = str(source)
                else:
                    for_source = 'none'

                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                # @modified 20210504  - Task #4030: refactoring
                #                       Task #4022: Move mysql_select calls to SQLAlchemy
                # query = 'select id from sources WHERE source=\'%s\'' % (str(source))  # nosec
                query = 'select id from sources WHERE source=\'%s\'' % (str(for_source))
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
            # @modified 20210421 - Task #4030: refactoring
            # semgrep - python.lang.correctness.useless-comparison.no-strings-as-booleans
            # if algorithm and algorithm != 'all':
            use_all_for_algorithm = True
            if use_all_for_algorithm and algorithm != 'all':

                # @added 20210504  - Task #4030: refactoring
                #                    Task #4022: Move mysql_select calls to SQLAlchemy
                # Sanitise variable
                if isinstance(algorithm, str):
                    for_algorithm = str(algorithm)
                else:
                    for_algorithm = 'none'

                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                # @modified 20210504  - Task #4030: refactoring
                #                       Task #4022: Move mysql_select calls to SQLAlchemy
                # query = 'select id from algorithms WHERE algorithm LIKE \'%s\'' % (str(algorithm))  # nosec
                query = 'select id from algorithms WHERE algorithm LIKE \'%s\'' % (str(for_algorithm))

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

                # @added 20210504  - Task #4030: refactoring
                #                    Task #4022: Move mysql_select calls to SQLAlchemy
                # Sanitise variable
                if isinstance(host, str):
                    for_host = str(host)
                else:
                    for_host = 'none'

                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                # @modified 20210504  - Task #4030: refactoring
                #                       Task #4022: Move mysql_select calls to SQLAlchemy
                # query = 'select id from hosts WHERE host=\'%s\'' % (str(host))  # nosec
                query = 'select id from hosts WHERE host=\'%s\'' % (str(for_host))
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
            # @modified 20210504  - Task #4030: refactoring
            #                       Task #4022: Move mysql_select calls to SQLAlchemy
            # limit = request.args.get('limit', '10')
            limit_str = request.args.get('limit', '10')
            try:
                limit = int(limit_str) + 0
            except Exception as e:
                logger.error('error :: limit parameter not an int: %s' % e)
                limit = 10
        else:
            limit = '10'

        if 'order' in request.args:
            # @modified 20210504  - Task #4030: refactoring
            #                       Task #4022: Move mysql_select calls to SQLAlchemy
            # order = request.args.get('order', 'DESC')
            order_str = request.args.get('order', 'DESC')
            if order_str == 'ASC':
                order = 'ASC'
            else:
                order = 'DESC'
        else:
            order = 'DESC'

        search_query = '%s ORDER BY id %s LIMIT %s' % (
            query_string, order, str(limit))

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
        # @modified 20191014 - Task #3270: Deprecate string.replace for py3
        #                      Branch #3262: py3
        anomalies_json = path.abspath(path.join(path.dirname(__file__), '..', settings.ANOMALY_DUMP))
        # panorama_json = string.replace(str(anomalies_json), 'anomalies.json', 'panorama.json')
        panorama_json = anomalies_json.replace('anomalies.json', 'panorama.json')
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
            anomaly_timestamp = str(row[3])
            full_duration = str(row[4])
            created_timestamp = str(row[5])
            # @modified 20191108 - Feature #3306: Record the anomaly_end_timestamp
            #                      Branch #3262: py3
            # anomaly_data = (anomaly_id, metric, anomalous_datapoint, anomaly_timestamp, full_duration, created_timestamp)
            # anomalies.append([int(anomaly_id), str(metric), anomalous_datapoint, anomaly_timestamp, full_duration, created_timestamp])
            anomaly_end_timestamp = str(row[6])
            # anomaly_data = (anomaly_id, metric, anomalous_datapoint, anomaly_timestamp, full_duration, created_timestamp, anomaly_end_timestamp)
            anomalies.append([int(anomaly_id), str(metric), anomalous_datapoint, anomaly_timestamp, full_duration, created_timestamp, anomaly_end_timestamp])
            anomalous_metrics.append(str(metric))

        if count_request:
            limit_argument = anomaly_count
            if int(anomaly_count) > 100:
                limit_argument = 100
            # anomaly_data = (int(anomaly_count), metric, str(limit_argument))
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
    logger.info('get_list :: select %s from %s' % (thing, table))  # nosec
    # got_results = False
    try:
        results = mysql_select(skyline_app, query)
        # got_results = True
    except:
        logger.error('error :: failed to get list of %ss from %s' % (thing, table))
        results = None

    things = []
    results_array_valid = False
    try:
        test_results = results[0]
        if test_results:
            results_array_valid = True
    except:
        logger.error('error :: invalid results array for get list of %ss from %s' % (thing, table))

    # @modified 20210415 - Feature #4014: Ionosphere - inference
    # Stop logging results in webapp

    if results_array_valid:
        # @modified 20210415 - Feature #4014: Ionosphere - inference
        # Stop logging results in webapp
        # logger.info('results: %s' % str(results))
        # for result in results:
        #     things.append(str(result[0]))
        # logger.info('things: %s' % str(things))
        logger.info('get_list :: returned valid result: %s' % str(results_array_valid))

    return things


# @added 20180720 - Feature #2464: luminosity_remote_data
# @modified 20201203 - Feature #3860: luminosity - handle low frequency data
# Add the metric resolution
# def luminosity_remote_data(anomaly_timestamp):
def luminosity_remote_data(anomaly_timestamp, resolution):
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
    # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
    # Use the metric resolution
    # from_timestamp = int(anomaly_timestamp) - 600
    # until_timestamp = int(anomaly_timestamp) + 61
    from_timestamp = int(anomaly_timestamp) - (resolution * 10)
    until_timestamp = int(anomaly_timestamp) + (resolution + 1)

    try:
        # @modified 20201123 - Feature #3824: get_cluster_data
        #                      Feature #2464: luminosity_remote_data
        #                      Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # unique_metrics = list(REDIS_CONN.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
        REDIS_CONN_DECODED = get_redis_conn_decoded(skyline_app)
        unique_metrics = list(REDIS_CONN_DECODED.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
    except Exception as e:
        logger.error('error :: %s' % str(e))
        logger.error('error :: luminosity_remote_data :: could not determine unique_metrics from Redis set')
    if not unique_metrics:
        message = 'error :: luminosity_remote_data :: could not determine unique_metrics from Redis set'
        return luminosity_data, success, message
    logger.info('luminosity_remote_data :: %s unique_metrics' % str(len(unique_metrics)))

    # @added 20210125 - Feature #3956: luminosity - motifs
    #                   Improve luminosity_remote_data performance
    # Although the is_derivative_metric function is appropriate in the below
    # loop here that is not the  most performant manner in which to determine if
    # the metrics are derivatives, as it needs to fire on every metric, so here
    # we just trust the Redis derivative_metrics list.  This increases
    # performance on 1267 metrics from 6.442009 seconds to 1.473067 seconds
    try:
        # @modified 20211012 - Feature #4280: aet.metrics_manager.derivative_metrics Redis hash
        # derivative_metrics = list(REDIS_CONN_DECODED.smembers('derivative_metrics'))
        derivative_metrics = list(REDIS_CONN_DECODED.smembers('aet.metrics_manager.derivative_metrics'))
    except:
        derivative_metrics = []

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

        # @added 20200507 - Feature #3532: Sort all time series
        # To ensure that there are no unordered timestamps in the time
        # series which are artefacts of the collector or carbon-relay, sort
        # all time series by timestamp before analysis.
        original_timeseries = timeseries
        if original_timeseries:
            timeseries = sort_timeseries(original_timeseries)
            del original_timeseries

        # Convert the time series if this is a known_derivative_metric
        # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
        # base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)

        # @added 20201117 - Feature #3824: get_cluster_data
        #                   Feature #2464: luminosity_remote_data
        #                   Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
        # Convert metric_name bytes to str
        metric_name = str(metric_name)

        # @modified 20210125 - Feature #3956: luminosity - motifs
        #                      Improve luminosity_remote_data performance
        # Although the is_derivative_metric function is appropriate here it is
        # not the most performant manner in which to determine if the metric
        # is a derivative in this case as it needs to fire on every metric, so
        # here we just trust the Redis derivative_metrics list. This increases
        # performance on 1267 metrics from 6.442009 seconds to 1.473067 seconds
        # if metric_name.startswith(settings.FULL_NAMESPACE):
        #     base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
        # else:
        #     base_name = metric_name
        # known_derivative_metric = is_derivative_metric('webapp', base_name)
        known_derivative_metric = False
        if metric_name in derivative_metrics:
            known_derivative_metric = True

        if known_derivative_metric:
            try:
                derivative_timeseries = nonNegativeDerivative(timeseries)
                timeseries = derivative_timeseries
            except:
                logger.error('error :: nonNegativeDerivative failed')

        # @modified 20210125 - Feature #3956: luminosity - motifs
        #                      Improve luminosity_remote_data performance
        # The list comprehension method halves the time to create the
        # correlate_ts from 0.0008357290644198656 to 0.0004676780663430691 seconds
        # correlate_ts = []
        # for ts, value in timeseries:
        #     if int(ts) < from_timestamp:
        #         continue
        #     if int(ts) <= anomaly_timestamp:
        #         correlate_ts.append((int(ts), value))
        #     if int(ts) > (anomaly_timestamp + until_timestamp):
        #         break
        correlate_ts = [x for x in timeseries if x[0] >= from_timestamp if x[0] <= until_timestamp]

        if not correlate_ts:
            continue
        metric_data = [str(metric_name), correlate_ts]
        luminosity_data.append(metric_data)

    logger.info('luminosity_remote_data :: %s valid metric time series data preprocessed for the remote request' % str(len(luminosity_data)))

    return luminosity_data, success, message


# @added 20200908 - Feature #3740: webapp - anomaly API endpoint
def panorama_anomaly_details(anomaly_id):
    """
    Gets the details for an anomaly from the database.
    """

    logger.info('panorama_anomaly_details - getting details for anomaly id %s' % str(anomaly_id))

    metric_id = 0
    # Added nosec to exclude from bandit tests
    query = 'select metric_id from anomalies WHERE id=\'%s\'' % str(anomaly_id)  # nosec
    try:
        result = mysql_select(skyline_app, query)
        metric_id = int(result[0][0])
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: panorama_anomaly_details - failed to get metric_id from db')
        return False
    if metric_id > 0:
        logger.info('panorama_anomaly_details - getting metric for metric_id - %s' % str(metric_id))
        # Added nosec to exclude from bandit tests
        query = 'select metric from metrics WHERE id=\'%s\'' % str(metric_id)  # nosec
        try:
            result = mysql_select(skyline_app, query)
            metric = str(result[0][0])
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: panorama_anomaly_details - failed to get metric from db')
            return False
    query = 'select id, metric_id, anomalous_datapoint, anomaly_timestamp, full_duration, created_timestamp, anomaly_end_timestamp from anomalies WHERE id=\'%s\'' % str(anomaly_id)  # nosec
    logger.info('panorama_anomaly_details - running query - %s' % str(query))
    try:
        rows = mysql_select(skyline_app, query)
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: panorama_anomaly_details - failed to get anomaly details from db')
        return False
    anomaly_data = None
    for row in rows:
        anomalous_datapoint = float(row[2])
        anomaly_timestamp = int(row[3])
        full_duration = int(row[4])
        created_timestamp = str(row[5])
        try:
            anomaly_end_timestamp = int(row[6])
        except:
            anomaly_end_timestamp = None
        anomaly_data = [int(anomaly_id), str(metric), anomalous_datapoint, anomaly_timestamp, full_duration, created_timestamp, anomaly_end_timestamp]
        break
    return anomaly_data


# @added 20201103 - Feature #3824: get_cluster_data
# @modified 20201127 - Feature #3824: get_cluster_data
#                      Feature #3820: HORIZON_SHARDS
# Allow to query only a single host in the cluster so that just the response
# can from a single host in the cluster can be evaluated
def get_cluster_data(api_endpoint, data_required, only_host='all', endpoint_params={}):
    """
    Gets data from the /api of REMOTE_SKYLINE_INSTANCES.  This allows the user
    to query a single Skyline webapp node in a cluster and the Skyline instance
    will respond with the concentated responses of all the
    REMOTE_SKYLINE_INSTANCES in one a single response.

    :param api_endpoint: the api endpoint to request data from the remote
        Skyline instances
    :param data_required: the element from the api json response that is
        required
    :param only_host: The remote Skyline host to query, if not passed all are
        queried.
    :param endpoint_params: A dictionary of any additional parameters that may
        be required
    :type api_endpoint: str
    :type data_required: str
    :type only_host: str
    :type endpoint_params: dict
    :return: list
    :rtype: list

    """
    try:
        connect_timeout = int(settings.GRAPHITE_CONNECT_TIMEOUT)
        read_timeout = int(settings.GRAPHITE_READ_TIMEOUT)
    except:
        connect_timeout = 5
        read_timeout = 10
    use_timeout = (int(connect_timeout), int(read_timeout))
    data = []

    if only_host != 'all':
        logger.info('get_cluster_data :: querying all remote hosts as only_host set to %s' % (
            str(only_host)))

    # @added 20220115 - Feature #4376: webapp - update_external_settings
    # Now determined based on endpoint_params if passed and handle GET
    # and POST
    normal_api = True
    method = 'GET'
    post_data = None
    if endpoint_params:
        if 'api_endpoint' in list(endpoint_params.keys()):
            api_endpoint = endpoint_params['api_endpoint']
            normal_api = False
            logger.info('get_cluster_data :: overriding api_endpoint with %s' % (
                str(api_endpoint)))
        if 'method' in list(endpoint_params.keys()):
            method = endpoint_params['method']
            logger.info('get_cluster_data :: overriding method with %s' % (
                str(method)))
        if 'post_data' in list(endpoint_params.keys()):
            post_data = endpoint_params['post_data']
            logger.info('get_cluster_data :: post_data was passed')

    for item in settings.REMOTE_SKYLINE_INSTANCES:
        r = None
        user = None
        password = None
        use_auth = False

        # @added 20201127 - Feature #3824: get_cluster_data
        #                   Feature #3820: HORIZON_SHARDS
        # Allow to query only a single host in the cluster so that just the response
        # can from a single host in the cluster can be evaluated
        if only_host != 'all':
            if only_host != str(item[0]):
                logger.info('get_cluster_data :: not querying %s as only_host set to %s' % (
                    str(item[0]), str(only_host)))
                continue
            logger.info('get_cluster_data :: querying %s as only_host set to %s' % (
                str(item[0]), str(only_host)))

        try:
            user = str(item[1])
            password = str(item[2])
            use_auth = True
        except:
            user = None
            password = None
        logger.info('get_cluster_data :: querying %s for %s on %s' % (
            str(item[0]), str(data_required), str(api_endpoint)))

        # @added 20220115 - Feature #4376: webapp - update_external_settings
        # Now determined based on endpoint_params if passed and handle GET
        # and POST
        url = '%s/api?%s' % (str(item[0]), api_endpoint)
        if not normal_api:
            url = '%s/%s' % (str(item[0]), str(api_endpoint))
        r = None

        try:
            # @modified 20220115 - Feature #4376: webapp - update_external_settings
            # Now determined based on endpoint_params if passed and handle GET
            # and POST
            # url = '%s/api?%s' % (str(item[0]), api_endpoint)
            if method == 'GET':
                if use_auth:
                    r = requests.get(url, timeout=use_timeout, auth=(user, password))
                else:
                    r = requests.get(url, timeout=use_timeout)
            if method == 'POST':
                connect_timeout = 5
                read_timeout = 10
                use_timeout = (int(connect_timeout), int(read_timeout))
                if use_auth:
                    r = requests.post(url, auth=(user, password), json=post_data, timeout=use_timeout, verify=settings.VERIFY_SSL)
                else:
                    r = requests.post(url, json=post_data, timeout=use_timeout, verify=settings.VERIFY_SSL)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: get_cluster_data :: failed to get %s from %s' % (
                api_endpoint, str(item)))
        if r:
            if r.status_code != 200:
                logger.error('error :: get_cluster_data :: %s from %s responded with status code %s and reason %s' % (
                    api_endpoint, str(item), str(r.status_code), str(r.reason)))
            js = None
            try:
                js = r.json()
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: get_cluster_data :: failed to get json from the response from %s on %s' % (
                    api_endpoint, str(item)))
            remote_data = []
            if js:
                logger.info('get_cluster_data :: got response for %s from %s' % (
                    str(data_required), str(item[0])))
                try:
                    remote_data = js['data'][data_required]
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: get_cluster_data :: failed to build remote_data from %s on %s' % (
                        str(data_required), str(item)))
            if remote_data:
                # @modified 20210617 - Feature #4144: webapp - stale_metrics API endpoint
                # Handle list and dic items
                if isinstance(remote_data, list):
                    logger.info('get_cluster_data :: got %s %s from %s' % (
                        str(len(remote_data)), str(data_required), str(item[0])))
                    data = data + remote_data
                if isinstance(remote_data, dict):
                    logger.info('get_cluster_data :: got %s %s from %s' % (
                        str(len(remote_data)), str(data_required), str(item[0])))
                    data.append(remote_data)
                if isinstance(remote_data, bool):
                    logger.info('get_cluster_data :: got %s %s from %s' % (
                        str(remote_data), str(data_required), str(item[0])))
                    data.append(remote_data)
        else:
            logger.error('error :: get_cluster_data :: failed to get response from %s on %s' % (
                api_endpoint, str(item)))

    return data


# @added 20201125 - Feature #3850: webapp - yhat_values API endoint
def get_yhat_values(
        metric, from_timestamp, until_timestamp, include_value, include_mean,
        include_yhat_real_lower, include_anomalous_periods):

    timeseries = []
    try:
        logger.info('get_yhat_values :: for %s from %s until %s' % (
            metric, str(from_timestamp), str(until_timestamp)))
        timeseries = get_graphite_metric('webapp', metric, from_timestamp, until_timestamp, 'list', 'object')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: get_yhat_values :: failed to get timeseries data for %s' % (
            metric))
        return None

    yhat_dict = {}
    logger.info('get_yhat_values :: %s values in timeseries for %s to calculate yhat values from' % (
        str(len(timeseries)), metric))

    # @added 20210126 - Task #3958: Handle secondary algorithms in yhat_values
    anomalous_periods_dict = {}
    if timeseries:
        metric_id = 0
        if metric:
            logger.info('get_yhat_values :: getting db id for metric - %s' % metric)
            query = 'select id from metrics WHERE metric=\'%s\'' % metric  # nosec
            try:
                result = mysql_select(skyline_app, query)
                metric_id = int(result[0][0])
            except:
                logger.error('error :: get_yhat_values :: failed to get id from db: %s' % traceback.format_exc())
        anomalies_at = []
        if metric_id:
            logger.info('get_yhat_values :: getting latest anomalies')
            query = 'select anomaly_timestamp, anomalous_datapoint, anomaly_end_timestamp from anomalies WHERE metric_id=%s AND anomaly_timestamp >= %s AND anomaly_timestamp <= %s' % (
                str(metric_id), str(from_timestamp), str(until_timestamp))
            try:
                rows = mysql_select(skyline_app, query)
                for row in rows:
                    a_timestamp = int(row[0])
                    a_value = float(row[1])
                    try:
                        a_end_timestamp = int(row[2])
                    except:
                        a_end_timestamp = 0
                    anomalies_at.append([a_timestamp, a_value, a_end_timestamp])
            except:
                logger.error('error :: get_yhat_values :: failed to get anomalies from db: %s' % traceback.format_exc())
                rows = []
        timeseries_ranges = []
        last_timestamp = None
        for index, item in enumerate(timeseries):
            if last_timestamp:
                t_range = list(range(last_timestamp, int(item[0])))
                timeseries_ranges.append([index, t_range, item])
            last_timestamp = int(item[0])
        t_range = list(range(last_timestamp, (int(item[0]) + 1)))
        timeseries_ranges.append([index, t_range, item])
        anomalies_index = []
        for index, time_range, item in timeseries_ranges:
            for a_timestamp, a_value, a_end_timestamp in anomalies_at:
                if a_timestamp in time_range:
                    anomalies_index.append([index, item])
        anomalous_period_indices = []
        anomalies_indices = [item[0] for item in anomalies_index]
        for index, item in enumerate(timeseries):
            for idx in anomalies_indices:
                anomaly_index_range = list(range((idx - 3), (idx + 5)))
                if index in anomaly_index_range:
                    for i in anomaly_index_range:
                        anomalous_period_indices.append(i)
        anomaly_timestamps_indices = []
        anomalies = []
        for item in anomalies_index:
            anomaly_timestamps_indices.append(item[0])
            anomalies.append(item[1])

    top = []
    bottom = []
    left = []
    right = []

    if timeseries:
        try:
            array_amin = np.amin([item[1] for item in timeseries])
            values = []

            # @added 20210126 - Task #3958: Handle secondary algorithms in yhat_values
            # last_value = None
            # start_anomalous_period = None
            # end_anomalous_period = None
            # sigma3_array = []
            # sigma3_values = []
            # extended_values = []
            last_breach = 0
            breach_for = 10
            last_breach_vector = 'positive'
            # last_used_extended = False
            # last_used_extended_value = None
            top = []
            bottom = []
            left = []
            right = []

            # @modified 20210126 - Task #3958: Handle secondary algorithms in yhat_values
            # for ts, value in timeseries:
            #     values.append(value)
            #     va = np.array(values)
            #     va_mean = va.mean()
            #    va_std_3 = 3 * va.std()
            for index, item in enumerate(timeseries):
                ts = item[0]
                value = item[1]
                values.append(value)
                va = np.array(values)
                va_mean = va.mean()
                va_std_3 = 3 * va.std()

                # @added 20210126 - Task #3958: Handle secondary algorithms in yhat_values
                anomalous_period = 0
                three_sigma_lower = va_mean - va_std_3
                three_sigma_upper = va_mean + va_std_3
                # sigma3_array.append([ts, value, va_mean, [three_sigma_lower, three_sigma_upper]])
                # sigma3_values.append([three_sigma_lower, three_sigma_upper])
                use_extended = False
                drop_expected_range = False
                if index not in anomaly_timestamps_indices:
                    use_extended = True
                    # if last_used_extended:
                    #     last_used_extended_value = None
                else:
                    drop_expected_range = True
                for anomaly_index in anomaly_timestamps_indices:
                    if index > anomaly_index:
                        # if index < (anomaly_index + 30):
                        if index < (anomaly_index + breach_for):
                            use_extended = False
                            anomalous_period = 1
                            break
                extended_lower = three_sigma_lower
                extended_upper = three_sigma_upper
                if use_extended:
                    if item[1] > three_sigma_upper:
                        extended_lower = three_sigma_lower
                        extended_upper = (item[1] + ((item[1] / 100) * 5))
                        last_breach = index
                        last_breach_vector = 'positive'
                    elif item[1] < three_sigma_lower:
                        extended_lower = (item[1] - ((item[1] / 100) * 5))
                        extended_upper = three_sigma_upper
                        last_breach = index
                        last_breach_vector = 'negative'
                    elif index < (last_breach + breach_for) and index > last_breach:
                        if last_breach_vector == 'positive':
                            extended_value = (item[1] + ((item[1] / 100) * 5))
                            three_sigma_value = three_sigma_upper
                            if three_sigma_value > extended_value:
                                extended_value = (three_sigma_value + ((three_sigma_value / 100) * 5))
                            extended_lower = three_sigma_lower
                            extended_upper = extended_value
                        else:
                            extended_lower = (item[1] - ((item[1] / 100) * 5))
                            extended_upper = three_sigma_upper
                        if drop_expected_range:
                            use_extended = False
                            if last_breach_vector == 'positive':
                                extended_lower = three_sigma_lower - (three_sigma_upper * 0.1)
                                extended_upper = item[1] - (item[1] * 0.1)
                            if last_breach_vector == 'negative':
                                extended_lower = three_sigma_lower - (three_sigma_lower * 0.1)
                                extended_upper = item[1] + (item[1] * 0.1)
                    else:
                        extended_lower = three_sigma_lower
                        extended_upper = three_sigma_upper
                        if drop_expected_range:
                            use_extended = False
                            if last_breach_vector == 'positive':
                                extended_lower = three_sigma_lower - (three_sigma_upper * 0.1)
                                extended_upper = item[1] - (item[1] * 0.1)
                            if last_breach_vector == 'negative':
                                extended_lower = three_sigma_lower - (three_sigma_lower * 0.1)
                                extended_upper = item[1] + (item[1] * 0.1)
                else:
                    extended_lower = three_sigma_lower
                    extended_upper = three_sigma_upper
                    if drop_expected_range:
                        use_extended = False
                        if last_breach_vector == 'positive':
                            extended_lower = three_sigma_lower - (three_sigma_upper * 0.1)
                            extended_upper = item[1] - (item[1] * 0.1)
                        if last_breach_vector == 'negative':
                            extended_lower = three_sigma_lower - (three_sigma_lower * 0.1)
                            extended_upper = item[1] + (item[1] * 0.1)
                # extended_values.append([extended_lower, extended_upper])
                lower = extended_lower
                upper = extended_upper
                if index in sorted(list(set(anomalous_period_indices))):
                    if index in anomalies_indices:
                        continue
                    for idx in anomaly_timestamps_indices:
                        if (index + 3) == idx:
                            a_top = extended_upper + (extended_upper * 0.1)
                            top.append(a_top)
                            a_bottom = extended_lower - (extended_lower * 0.1)
                            bottom.append(a_bottom)
                            a_left = item[0]
                            left.append(a_left)
                        if (index - 4) == idx:
                            a_right = item[0]
                            right.append(a_right)

                # @modified 20201126 - Feature #3850: webapp - yhat_values API endoint
                # Change dict key to int not float
                int_ts = int(ts)
                yhat_dict[int_ts] = {}
                if include_value:
                    yhat_dict[int_ts]['value'] = value
                if include_mean:
                    yhat_dict[int_ts]['mean'] = va_mean
                if include_mean:
                    yhat_dict[int_ts]['mean'] = va_mean

                # @modified 20210201 - Task #3958: Handle secondary algorithms in yhat_values
                # yhat_lower = va_mean - va_std_3
                yhat_lower = lower
                yhat_upper = upper

                if include_yhat_real_lower:
                    # @modified 20201202 - Feature #3850: webapp - yhat_values API endoint
                    # Set the yhat_real_lower correctly
                    # if yhat_lower < array_amin and array_amin == 0:
                    #     yhat_dict[int_ts]['yhat_real_lower'] = array_amin
                    if yhat_lower < 0 and array_amin > -0.0000000001:
                        yhat_dict[int_ts]['yhat_real_lower'] = 0
                    else:
                        yhat_dict[int_ts]['yhat_real_lower'] = yhat_lower
                yhat_dict[int_ts]['yhat_lower'] = yhat_lower
                # @modified 20210201 - Task #3958: Handle secondary algorithms in yhat_values
                yhat_dict[int_ts]['yhat_upper'] = va_mean + va_std_3
                yhat_dict[int_ts]['yhat_upper'] = upper
                # @added 20210201 - Task #3958: Handle secondary algorithms in yhat_values
                if use_extended:
                    if yhat_lower != three_sigma_lower:
                        yhat_dict[int_ts]['3sigma_lower'] = three_sigma_lower
                    if yhat_upper != three_sigma_upper:
                        yhat_dict[int_ts]['3sigma_upper'] = three_sigma_upper
                if include_anomalous_periods:
                    yhat_dict[int_ts]['anomalous_period'] = anomalous_period
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: get_yhat_values :: failed create yhat_dict for %s' % (
                metric))
            return None

    logger.info('get_yhat_values :: calculated yhat values for %s data points' % str(len(yhat_dict)))
    if yhat_dict:
        yhat_dict_cache_key = 'webapp.%s.%s.%s.%s.%s.%s' % (
            metric, str(from_timestamp), str(until_timestamp),
            str(include_value), str(include_mean),
            str(include_yhat_real_lower))
        logger.info('get_yhat_values :: saving yhat_dict to Redis key - %s' % yhat_dict_cache_key)
        try:
            REDIS_CONN.setex(yhat_dict_cache_key, 14400, str(yhat_dict))
            logger.info('get_yhat_values :: created Redis key - %s with 14400 TTL' % yhat_dict_cache_key)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: get_yhat_values :: failed to setex Redis key - %s' % yhat_dict_cache_key)

    # @added 20210126 - Task #3958: Handle secondary algorithms in yhat_values
    # Add rectangle coordinates that describe anomalous periods
    anomalous_periods_dict['rectangles'] = {}
    anomalous_periods_dict['rectangles']['top'] = top
    anomalous_periods_dict['rectangles']['bottom'] = bottom
    anomalous_periods_dict['rectangles']['left'] = left
    anomalous_periods_dict['rectangles']['right'] = right
    if anomalous_periods_dict:
        yhat_anomalous_periods_dict_cache_key = 'webapp.%s.%s.%s.%s.%s.%s.anomalous_periods' % (
            metric, str(from_timestamp), str(until_timestamp),
            str(include_value), str(include_mean),
            str(include_yhat_real_lower))
        logger.info('get_yhat_values :: saving yhat_dict to Redis key - %s' % yhat_anomalous_periods_dict_cache_key)
        try:
            REDIS_CONN.setex(yhat_anomalous_periods_dict_cache_key, 14400, str(yhat_anomalous_periods_dict_cache_key))
            logger.info('get_yhat_values :: created Redis key - %s with 14400 TTL' % yhat_anomalous_periods_dict_cache_key)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: get_yhat_values :: failed to setex Redis key - %s' % yhat_dict_cache_key)

    # @modified 20210201 - Task #3958: Handle secondary algorithms in yhat_values
    # return yhat_dict
    return yhat_dict, anomalous_periods_dict


# @added 20210326 - Feature #3994: Panorama - mirage not anomalous
def get_mirage_not_anomalous_metrics(
        metric=None, from_timestamp=None, until_timestamp=None,
        anomalies=False):
    """
    Determine mirage not anomalous metrics from mirage.panorama.not_anomalous_metrics
    and ionosphere.panorama.not_anomalous_metrics

    :param metric: base_name
    :param from_timestamp: the from_timestamp
    :param until_timestamp: the until_timestamp
    :param anomalies: whether to report anomalies as well
    :type metric: str
    :type from_timestamp: int
    :type until_timestamp: int
    :type anomalies: boolean
    :return: (dict, dict)
    :rtype: tuple

    """

    import datetime

    # fail_msg = None
    # trace = None

    current_date = datetime.datetime.now().date()
    current_date_str = '%s 00:00' % str(current_date)
    # from_timestamp_date_str = current_date_str
    # until_timestamp_date_str = current_date_str
    until_timestamp = str(int(time.time()))

    base_name = None
    if 'metric' in request.args:
        base_name = request.args.get('metric', None)
        if base_name == 'all':
            base_name = None
    if metric:
        base_name = metric

    if not from_timestamp and 'from_timestamp' in request.args:
        from_timestamp = request.args.get('from_timestamp', None)
        if from_timestamp == 'today':
            # from_timestamp_date_str = current_date_str
            # @modified 20211021 - handle multiple date formats
            try:
                new_from_timestamp = time.mktime(datetime.datetime.strptime(current_date_str, '%Y-%m-%d %H:%M').timetuple())
            except ValueError:
                new_from_timestamp = time.mktime(datetime.datetime.strptime(current_date_str, '%Y-%m-%d %H:%M').timetuple())
            except Exception as err:
                trace = traceback.format_exc()
                logger.error('%s' % trace)
                fail_msg = 'error :: panorama_request :: failed to unix timestamp from current_date_str - %s' % str(err)
                logger.error('%s' % fail_msg)
                raise  # to webapp to return in the UI

            from_timestamp = str(int(new_from_timestamp))
        if from_timestamp and from_timestamp != 'today':
            if ":" in from_timestamp:
                # try:
                #     datetime_object = datetime.datetime.strptime(from_timestamp, '%Y-%m-%d %H:%M')
                # except:
                #     # Handle old format
                #     datetime_object = datetime.datetime.strptime(from_timestamp, '%Y%m%d %H:%M')
                # from_timestamp_date_str = str(datetime_object.date())
                # @modified 20211021 - handle multiple date formats
                try:
                    new_from_timestamp = time.mktime(datetime.datetime.strptime(from_timestamp, '%Y-%m-%d %H:%M').timetuple())
                except ValueError:
                    new_from_timestamp = time.mktime(datetime.datetime.strptime(from_timestamp, '%Y%m%d %H:%M').timetuple())
                except Exception as err:
                    trace = traceback.format_exc()
                    logger.error('%s' % trace)
                    fail_msg = 'error :: panorama_request :: failed to unix timestamp from from_timestamp - %s' % str(err)
                    logger.error('%s' % fail_msg)
                    raise  # to webapp to return in the UI

                from_timestamp = str(int(new_from_timestamp))
    else:
        # from_timestamp_date_str = current_date_str
        # @modified 20211021 - handle multiple date formats
        try:
            new_from_timestamp = time.mktime(datetime.datetime.strptime(current_date_str, '%Y-%m-%d %H:%M').timetuple())
        except ValueError:
            new_from_timestamp = time.mktime(datetime.datetime.strptime(current_date_str, '%Y%m%d %H:%M').timetuple())
        except Exception as err:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: panorama_request :: failed to unix timestamp from current_date_str - %s' % str(err)
            logger.error('%s' % fail_msg)
            raise  # to webapp to return in the UI

        from_timestamp = str(int(new_from_timestamp))

    if not until_timestamp and 'until_timestamp' in request.args:
        until_timestamp = request.args.get('until_timestamp', None)
        if until_timestamp == 'all':
            # until_timestamp_date_str = current_date_str
            until_timestamp = str(int(time.time()))
        if until_timestamp and until_timestamp != 'all':
            if ":" in until_timestamp:
                # datetime_object = datetime.datetime.strptime(until_timestamp, '%Y-%m-%d %H:%M')
                # until_timestamp_date_str = str(datetime_object.date())
                # @modified 20211021 - handle multiple date formats
                try:
                    new_until_timestamp = time.mktime(datetime.datetime.strptime(until_timestamp, '%Y-%m-%d %H:%M').timetuple())
                except ValueError:
                    new_until_timestamp = time.mktime(datetime.datetime.strptime(until_timestamp, '%Y%m%d %H:%M').timetuple())
                except Exception as err:
                    trace = traceback.format_exc()
                    logger.error('%s' % trace)
                    fail_msg = 'error :: panorama_request :: failed to unix timestamp from until_timestamp - %s' % str(err)
                    logger.error('%s' % fail_msg)
                    raise  # to webapp to return in the UI

                until_timestamp = str(int(new_until_timestamp))
    else:
        # until_timestamp_date_str = current_date_str
        until_timestamp = str(int(time.time()))

    get_anomalies = False
    if 'anomalies' in request.args:
        anomalies_str = request.args.get('anomalies', 'false')
        if anomalies_str == 'true':
            get_anomalies = True
        logger.info(
            'get_mirage_not_anomalous_metrics - also determining anomalies for %s' % (
                str(base_name)))

    logger.info(
        'get_mirage_not_anomalous_metrics - base_name: %s, from_timestamp: %s, until_timestamp: %s' % (
            str(base_name), str(from_timestamp), str(until_timestamp)))

    redis_hash = 'mirage.panorama.not_anomalous_metrics'
    mirage_panorama_not_anomalous = {}
    try:
        REDIS_CONN_DECODED = get_redis_conn_decoded(skyline_app)
        mirage_panorama_not_anomalous = REDIS_CONN_DECODED.hgetall(redis_hash)
        logger.info('get_mirage_not_anomalous_metrics :: %s entries to check in the %s Redis hash key' % (
            str(len(mirage_panorama_not_anomalous)), redis_hash))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: get_mirage_not_anomalous_metrics :: failed to get Redis hash key %s' % redis_hash)
        mirage_panorama_not_anomalous = {}
    all_timestamp_float_strings = []
    if mirage_panorama_not_anomalous:
        all_timestamp_float_strings = list(mirage_panorama_not_anomalous.keys())
    timestamp_floats = []
    if all_timestamp_float_strings:
        for timestamp_float_string in all_timestamp_float_strings:
            if int(float(timestamp_float_string)) >= int(from_timestamp):
                if int(float(timestamp_float_string)) <= int(until_timestamp):
                    timestamp_floats.append(timestamp_float_string)

    not_anomalous_dict = {}
    not_anomalous_count = 0
    for timestamp_float_string in timestamp_floats:
        try:
            timestamp_float_dict = literal_eval(mirage_panorama_not_anomalous[timestamp_float_string])
            for i_metric in list(timestamp_float_dict.keys()):
                if base_name:
                    if base_name != i_metric:
                        continue
                try:
                    metric_dict = not_anomalous_dict[i_metric]
                except:
                    metric_dict = {}
                    not_anomalous_dict[i_metric] = {}
                    not_anomalous_dict[i_metric]['from'] = int(from_timestamp)
                    not_anomalous_dict[i_metric]['until'] = int(until_timestamp)
                    not_anomalous_dict[i_metric]['timestamps'] = {}
                metric_timestamp = timestamp_float_dict[i_metric]['timestamp']
                try:
                    metric_timestamp_dict = not_anomalous_dict[i_metric]['timestamps'][metric_timestamp]
                except:
                    not_anomalous_dict[i_metric]['timestamps'][metric_timestamp] = {}
                    metric_timestamp_dict = {}
                if not metric_timestamp_dict:
                    not_anomalous_dict[i_metric]['timestamps'][metric_timestamp]['value'] = timestamp_float_dict[i_metric]['value']
                    not_anomalous_dict[i_metric]['timestamps'][metric_timestamp]['hours_to_resolve'] = timestamp_float_dict[i_metric]['hours_to_resolve']
                    not_anomalous_count += 1
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: get_mirage_not_anomalous_metrics :: failed iterate mirage_panorama_not_anomalous entry')

    logger.info(
        'get_mirage_not_anomalous_metrics - not_anomalous_count: %s, for base_name: %s' % (
            str(not_anomalous_count), str(base_name)))

    # @added 20210429 - Feature #3994: Panorama - mirage not anomalous
    # A hash is added to the ionosphere.panorama.not_anomalous_metrics for
    # every metric that is found to be not anomalous.
    redis_hash = 'ionosphere.panorama.not_anomalous_metrics'
    ionosphere_panorama_not_anomalous = {}
    try:
        REDIS_CONN_DECODED = get_redis_conn_decoded(skyline_app)
        ionosphere_panorama_not_anomalous = REDIS_CONN_DECODED.hgetall(redis_hash)
        logger.info('get_mirage_not_anomalous_metrics :: %s entries to check in the %s Redis hash key' % (
            str(len(ionosphere_panorama_not_anomalous)), redis_hash))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: get_mirage_not_anomalous_metrics :: failed to get Redis hash key %s' % redis_hash)
        ionosphere_panorama_not_anomalous = {}
    ionosphere_all_timestamp_float_strings = []
    if ionosphere_panorama_not_anomalous:
        ionosphere_all_timestamp_float_strings = list(ionosphere_panorama_not_anomalous.keys())
    ionosphere_timestamp_floats = []
    if all_timestamp_float_strings:
        for timestamp_float_string in ionosphere_all_timestamp_float_strings:
            if int(float(timestamp_float_string)) >= int(from_timestamp):
                if int(float(timestamp_float_string)) <= int(until_timestamp):
                    ionosphere_timestamp_floats.append(timestamp_float_string)
    for timestamp_float_string in ionosphere_timestamp_floats:
        try:
            timestamp_float_dict = literal_eval(ionosphere_panorama_not_anomalous[timestamp_float_string])
            for i_metric in list(timestamp_float_dict.keys()):
                if base_name:
                    if base_name != i_metric:
                        continue
                try:
                    metric_dict = not_anomalous_dict[i_metric]
                except:
                    metric_dict = {}
                    not_anomalous_dict[i_metric] = {}
                    not_anomalous_dict[i_metric]['from'] = int(from_timestamp)
                    not_anomalous_dict[i_metric]['until'] = int(until_timestamp)
                    not_anomalous_dict[i_metric]['timestamps'] = {}
                del metric_dict
                metric_timestamp = timestamp_float_dict[i_metric]['timestamp']
                try:
                    metric_timestamp_dict = not_anomalous_dict[i_metric]['timestamps'][metric_timestamp]
                except:
                    not_anomalous_dict[i_metric]['timestamps'][metric_timestamp] = {}
                    metric_timestamp_dict = {}
                if not metric_timestamp_dict:
                    not_anomalous_dict[i_metric]['timestamps'][metric_timestamp]['value'] = timestamp_float_dict[i_metric]['value']
                    not_anomalous_dict[i_metric]['timestamps'][metric_timestamp]['hours_to_resolve'] = timestamp_float_dict[i_metric]['hours_to_resolve']
                    not_anomalous_count += 1
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: get_mirage_not_anomalous_metrics :: failed iterate ionosphere_panorama_not_anomalous entry')

    logger.info(
        'get_mirage_not_anomalous_metrics - not_anomalous_count: %s (with ionosphere), for base_name: %s' % (
            str(not_anomalous_count), str(base_name)))

    anomalies_dict = {}
    if get_anomalies:
        for i_metric in list(not_anomalous_dict.keys()):
            metric_id = None
            query = 'SELECT id FROM metrics WHERE metric=\'%s\'' % i_metric
            try:
                results = mysql_select(skyline_app, query)
                for item in results:
                    metric_id = int(item[0])
                    break
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: get_mirage_not_anomalous_metrics :: querying MySQL - %s' % query)
            query_start_str = 'SELECT anomaly_timestamp,anomalous_datapoint,anomaly_end_timestamp,full_duration FROM anomalies'
            if metric_id:
                query = '%s WHERE metric_id=%s AND anomaly_timestamp > %s AND anomaly_timestamp < %s' % (
                    query_start_str, metric_id, str(from_timestamp), str(until_timestamp))
            else:
                query = '%s WHERE anomaly_timestamp > %s AND anomaly_timestamp < %s' % (
                    query_start_str, str(from_timestamp), str(until_timestamp))
            anomalies = []
            try:
                results = mysql_select(skyline_app, query)
                for item in results:
                    anomalies.append([i_metric, int(item[0]), float(item[1]), float(item[2]), round(int(item[3]) / 3600)])
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: get_mirage_not_anomalous_metrics :: querying MySQL - %s' % query)

            anomalies_dict[i_metric] = {}
            anomalies_dict[i_metric]['from'] = int(from_timestamp)
            anomalies_dict[i_metric]['until'] = int(until_timestamp)
            anomalies_dict[i_metric]['timestamps'] = {}
            if anomalies:
                for a_metric, timestamp, value, anomaly_end_timestamp, hours_to_resolve in anomalies:
                    anomalies_dict[i_metric]['timestamps'][timestamp] = {}
                    anomalies_dict[i_metric]['timestamps'][timestamp]['value'] = value
                    anomalies_dict[i_metric]['timestamps'][timestamp]['hours_to_resolve'] = hours_to_resolve
                    anomalies_dict[i_metric]['timestamps'][timestamp]['end_timestamp'] = anomaly_end_timestamp

    # @added 20210328 - Feature #3994: Panorama - mirage not anomalous
    # Save key to use in not_anomalous_metric
    not_anomalous_dict_key = 'panorama.not_anomalous_dict.%s.%s' % (
        str(from_timestamp), str(until_timestamp))
    not_anomalous_dict_key_ttl = 600
    if base_name:
        not_anomalous_dict_key = 'panorama.not_anomalous_dict.%s.%s.%s' % (
            str(from_timestamp), str(until_timestamp), base_name)
        not_anomalous_dict_key_ttl = 600
    try:
        REDIS_CONN.setex(not_anomalous_dict_key, not_anomalous_dict_key_ttl, str(not_anomalous_dict))
        logger.info('get_mirage_not_anomalous_metrics :: created Redis key - %s with %s TTL' % (
            not_anomalous_dict_key, str(not_anomalous_dict_key_ttl)))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: get_mirage_not_anomalous_metrics :: failed to created Redis key - %s with %s TTL' % (
            not_anomalous_dict_key, str(not_anomalous_dict_key_ttl)))
    if not base_name:
        recent_not_anomalous_dict_key = 'panorama.not_anomalous_dict.recent'
        recent_not_anomalous_dict_key_ttl = 180
        try:
            REDIS_CONN.setex(recent_not_anomalous_dict_key, recent_not_anomalous_dict_key_ttl, str(not_anomalous_dict))
            logger.info('get_mirage_not_anomalous_metrics :: created Redis key - %s with %s TTL' % (
                recent_not_anomalous_dict_key, str(recent_not_anomalous_dict_key_ttl)))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: get_mirage_not_anomalous_metrics :: failed to created Redis key - %s with %s TTL' % (
                recent_not_anomalous_dict_key, str(recent_not_anomalous_dict_key_ttl)))

    anomalies_dict_key = 'panorama.anomalies_dict.%s.%s' % (
        str(from_timestamp), str(until_timestamp))
    anomalies_dict_key_ttl = 600
    if base_name:
        anomalies_dict_key = 'panorama.anomalies_dict.%s.%s.%s' % (
            str(from_timestamp), str(until_timestamp), base_name)
        anomalies_dict_key_ttl = 600
    try:
        REDIS_CONN.setex(anomalies_dict_key, anomalies_dict_key_ttl, str(anomalies_dict))
        logger.info('get_mirage_not_anomalous_metrics :: created Redis key - %s with %s TTL' % (
            anomalies_dict_key, str(anomalies_dict_key_ttl)))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: get_mirage_not_anomalous_metrics :: failed to created Redis key - %s with %s TTL' % (
            anomalies_dict_key, str(anomalies_dict_key_ttl)))
    if not base_name:
        recent_anomalies_dict_key = 'panorama.not_anomalous_dict.recent'
        recent_anomalies_dict_key_ttl = 180
        try:
            REDIS_CONN.setex(recent_anomalies_dict_key, recent_anomalies_dict_key_ttl, str(anomalies_dict))
            logger.info('get_mirage_not_anomalous_metrics :: created Redis key - %s with %s TTL' % (
                recent_anomalies_dict_key, str(recent_anomalies_dict_key_ttl)))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: get_mirage_not_anomalous_metrics :: failed to created Redis key - %s with %s TTL' % (
                recent_anomalies_dict_key, str(recent_anomalies_dict_key_ttl)))

    return not_anomalous_dict, anomalies_dict


# @added 20210328 - Feature #3994: Panorama - mirage not anomalous
def plot_not_anomalous_metric(not_anomalous_dict, anomalies_dict, plot_type):
    """
    Plot the metric not anomalous or anomalies graph and return the file path

    :param not_anomalous_dict: the dictionary of not anomalous events for the
        metric
    :param anomalies_dict: the dictionary of anomalous events for the
        metric
    :type not_anomalous_dict: dict
    :type anomalies_dict: dict
    :type plot_type: str ('not_anomalous' or 'anomalies')
    :return: path and filename
    :rtype: str

    """

    fail_msg = None
    trace = None

    metric = None
    from_timestamp = None
    until_timestamp = None

    try:
        metric = list(not_anomalous_dict.keys())[0]
        from_timestamp = not_anomalous_dict[metric]['from']
        until_timestamp = not_anomalous_dict[metric]['until']
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: plot_not_anomalous_metric :: failed to get details for plot from not_anomalous_dict'
        logger.error('%s' % fail_msg)
        raise  # to webapp to return in the UI
    if not metric or not from_timestamp or not until_timestamp or not plot_type:
        fail_msg = 'error :: plot_not_anomalous_metric :: failed to get details for plot'
        logger.error('%s' % fail_msg)
        raise  # to webapp to return in the UI

    try:
        timeseries = get_graphite_metric(
            skyline_app, metric, from_timestamp, until_timestamp, 'list',
            'object')
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: plot_not_anomalous_metric :: failed to get timeseries from Graphite for details for %s' % metric
        logger.error('%s' % fail_msg)
        raise  # to webapp to return in the UI

    if plot_type == 'not_anomalous':
        data_dict = not_anomalous_dict
    if plot_type == 'anomalies':
        data_dict = anomalies_dict

    plot_timestamps = list(data_dict[metric]['timestamps'].keys())

    logger.info('plot_not_anomalous_metric :: building not %s timeseries' % plot_type)
    plot_timeseries = []
    last_timestamp = None
    a_timestamps_done = []
    for timestamp, value in timeseries:
        anomaly = 0
        if not last_timestamp:
            last_timestamp = int(timestamp)
            plot_timeseries.append([int(timestamp), anomaly])
            continue
        for a_timestamp in plot_timestamps:
            if a_timestamp < last_timestamp:
                continue
            if a_timestamp > int(timestamp):
                continue
            if a_timestamp in a_timestamps_done:
                continue
            if a_timestamp in list(range(last_timestamp, int(timestamp))):
                anomaly = 1
                a_timestamps_done.append(a_timestamp)
        plot_timeseries.append([int(timestamp), anomaly])
    logger.info('plot_not_anomalous_metric :: created %s timeseries' % plot_type)

    logger.info('plot_not_anomalous_metric :: creating timeseries dataframe')
    try:
        df = pd.DataFrame(timeseries, columns=['date', 'value'])
        df['date'] = pd.to_datetime(df['date'], unit='s')
        datetime_index = pd.DatetimeIndex(df['date'].values)
        df = df.set_index(datetime_index)
        df.drop('date', axis=1, inplace=True)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: plot_not_anomalous_metric :: failed create timeseries dataframe to plot %s' % metric
        logger.error('%s' % fail_msg)
        raise  # to webapp to return in the UI

    logger.info('plot_not_anomalous_metric :: creating %s dataframe' % plot_type)
    try:
        plot_df = pd.DataFrame(plot_timeseries, columns=['date', 'value'])
        plot_df['date'] = pd.to_datetime(plot_df['date'], unit='s')
        datetime_index = pd.DatetimeIndex(plot_df['date'].values)
        plot_df = plot_df.set_index(datetime_index)
        plot_df.drop('date', axis=1, inplace=True)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: plot_not_anomalous_metric :: failed create not anomalous dataframe to plot %s' % metric
        logger.error('%s' % fail_msg)
        raise  # to webapp to return in the UI

    try:
        logger.info('plot_not_anomalous_metric :: loading plot from adtk.visualization')
        from adtk.visualization import plot
        sane_metricname = filesafe_metricname(str(metric))
        save_to_file = '%s/panorama/not_anomalous/%s/%s.%s.%s.%s.png' % (
            settings.SKYLINE_TMP_DIR, sane_metricname, plot_type,
            str(from_timestamp),
            str(until_timestamp), sane_metricname)
        save_to_path = path.dirname(save_to_file)
        if plot_type == 'not_anomalous':
            title = 'Not anomalous analysis\n%s' % metric
        if plot_type == 'anomalies':
            title = 'Anomalies\n%s' % metric

        if not path.exists(save_to_path):
            try:
                mkdir_p(save_to_path)
            except Exception as e:
                logger.error('error :: plot_not_anomalous_metric :: failed to create dir - %s - %s' % (
                    save_to_path, e))
        if path.exists(save_to_path):
            try:
                logger.info('plot_not_anomalous_metric :: plotting')
                if plot_type == 'not_anomalous':
                    plot(
                        df, anomaly=plot_df, anomaly_color='green', title=title,
                        ts_markersize=1, anomaly_alpha=0.4, legend=False,
                        save_to_file=save_to_file)
                if plot_type == 'anomalies':
                    plot(
                        df, anomaly=plot_df, anomaly_color='red', title=title,
                        ts_markersize=1, anomaly_alpha=1, legend=False,
                        save_to_file=save_to_file)
                logger.debug('debug :: plot_not_anomalous_metric :: plot saved to - %s' % (
                    save_to_file))
            except Exception as e:
                trace = traceback.format_exc()
                logger.error('%s' % trace)
                fail_msg = 'error :: plot_not_anomalous_metric :: failed to plot - %s: %s' % (str(metric), e)
                logger.error('%s' % fail_msg)
                raise  # to webapp to return in the UI
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: plot_not_anomalous_metric :: plotting %s for %s' % (str(metric), plot_type)
        logger.error('%s' % fail_msg)
        raise  # to webapp to return in the UI

    if not path.isfile(save_to_file):
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: plot_not_anomalous_metric :: plotting %s for %s to %s failed' % (
            str(metric), plot_type, save_to_file)
        logger.error('%s' % fail_msg)
        raise  # to webapp to return in the UI
    else:
        try:
            REDIS_CONN.hset('panorama.not_anomalous_plots', time.time(), save_to_file)
            logger.info('plot_not_anomalous_metric :: set Redis hash in panorama.not_anomalous_plots for clean up')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: plot_not_anomalous_metric :: failed to set save_to_file in Redis hash - panorama.not_anomalous_plots')

    return save_to_file


# @added 20210617 - Feature #4144: webapp - stale_metrics API endpoint
#                   Feature #4076: CUSTOM_STALE_PERIOD
#                   Branch #1444: thunder
def namespace_stale_metrics(namespace, cluster_data, exclude_sparsely_populated):
    """
    Plot the metric not anomalous or anomalies graph and return the file path

    :param not_anomalous_dict: the dictionary of not anomalous events for the
        metric
    :param anomalies_dict: the dictionary of anomalous events for the
        metric
    :type not_anomalous_dict: dict
    :type anomalies_dict: dict
    :type plot_type: str ('not_anomalous' or 'anomalies')
    :return: path and filename
    :rtype: str

    """

    fail_msg = None
    trace = None

    namespaces_namespace_stale_metrics_dict = {}
    namespaces_namespace_stale_metrics_dict['stale_metrics'] = {}

    unique_base_names = []
    try:
        REDIS_CONN_DECODED = get_redis_conn_decoded(skyline_app)
        unique_base_names = list(REDIS_CONN_DECODED.smembers('aet.analyzer.unique_base_names'))
        logger.info('%s namespaces checked for stale metrics discovered with thunder_stale_metrics' % (
            str(len(unique_base_names))))
    except Exception as e:
        fail_msg = 'error :: Webapp error with api?stale_metrics - %s' % e
        logger.error(fail_msg)
        raise

    now = int(time.time())
    namespace_stale_metrics_dict = {}
    namespace_recovered_metrics_dict = {}
    try:
        namespace_stale_metrics_dict, namespace_recovered_metrics_dict = thunder_stale_metrics(skyline_app, log=True)
    except Exception as e:
        fail_msg = 'error :: Webapp error with api?stale_metrics - %s' % e
        logger.error(fail_msg)
        raise
    logger.info('%s namespaces checked for stale metrics discovered with thunder_stale_metrics' % (
        str(len(namespace_stale_metrics_dict))))

    remote_stale_metrics_dicts = []
    if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
        exclude_sparsely_populated_str = 'false'
        if exclude_sparsely_populated:
            exclude_sparsely_populated_str = 'true'
        remote_namespaces_namespace_stale_metrics_dicts = []
        stale_metrics_uri = 'stale_metrics=true&namespace=%s&exclude_sparsely_populated=%s' % (
            str(namespace), str(exclude_sparsely_populated_str))
        try:
            remote_namespaces_namespace_stale_metrics_dicts = get_cluster_data(stale_metrics_uri, 'stale_metrics')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: Webapp could not get remote_namespaces_namespace_stale_metrics_dict from the remote Skyline instances')
        if remote_namespaces_namespace_stale_metrics_dicts:
            logger.info('got %s remote namespace_stale_metrics_dicts instances from the remote Skyline instances' % str(len(remote_namespaces_namespace_stale_metrics_dicts)))
            remote_stale_metrics_dicts = remote_namespaces_namespace_stale_metrics_dicts

    stale_metrics_count = 0
    total_metrics_count = len(unique_base_names)
    if namespace == 'all':
        namespaces_namespace_stale_metrics_dict['stale_metrics']['namespace'] = 'all'
        if remote_stale_metrics_dicts:
            for remote_stale_metrics_dict in remote_stale_metrics_dicts:
                total_metrics_count = total_metrics_count + remote_stale_metrics_dict['total_metrics_count']
        namespaces_namespace_stale_metrics_dict['stale_metrics']['total_metrics_count'] = total_metrics_count
        namespaces_namespace_stale_metrics_dict['stale_metrics']['stale_metrics'] = {}
        if namespace_stale_metrics_dict:
            for parent_namespace in list(namespace_stale_metrics_dict.keys()):
                for base_name in list(namespace_stale_metrics_dict[parent_namespace]['metrics'].keys()):
                    stale_metrics_count += 1
                    namespaces_namespace_stale_metrics_dict['stale_metrics']['stale_metrics'][base_name] = {}
                    last_timestamp = namespace_stale_metrics_dict[parent_namespace]['metrics'][base_name]
                    stale_for = now - int(float(last_timestamp))
                    namespaces_namespace_stale_metrics_dict['stale_metrics']['stale_metrics'][base_name]['last_timestamp'] = last_timestamp
                    namespaces_namespace_stale_metrics_dict['stale_metrics']['stale_metrics'][base_name]['stale_for'] = stale_for
        if remote_stale_metrics_dicts:
            for remote_stale_metrics_dict in remote_stale_metrics_dicts:
                for base_name in list(remote_stale_metrics_dict['stale_metrics'].keys()):
                    stale_metrics_count += 1
                    namespaces_namespace_stale_metrics_dict['stale_metrics']['stale_metrics'][base_name] = {}
                    last_timestamp = remote_stale_metrics_dict['stale_metrics'][base_name]['last_timestamp']
                    stale_for = remote_stale_metrics_dict['stale_metrics'][base_name]['stale_for']
                    namespaces_namespace_stale_metrics_dict['stale_metrics']['stale_metrics'][base_name]['last_timestamp'] = last_timestamp
                    namespaces_namespace_stale_metrics_dict['stale_metrics']['stale_metrics'][base_name]['stale_for'] = stale_for
        namespaces_namespace_stale_metrics_dict['stale_metrics']['stale_metrics_count'] = stale_metrics_count

    if namespace_stale_metrics_dict and namespace != 'all':
        namespaces_namespace_stale_metrics_dict['stale_metrics']['namespace'] = namespace
        total_metrics_count = len([base_name for base_name in unique_base_names if base_name.startswith(namespace)])
        if remote_stale_metrics_dicts:
            for remote_stale_metrics_dict in remote_stale_metrics_dicts:
                total_metrics_count = total_metrics_count + remote_stale_metrics_dict['total_metrics_count']
        namespaces_namespace_stale_metrics_dict['stale_metrics']['total_metrics_count'] = total_metrics_count
        namespaces_namespace_stale_metrics_dict['stale_metrics']['stale_metrics'] = {}
        top_level_namespace = namespace.split('.')[0]
        if namespace_stale_metrics_dict:
            for parent_namespace in list(namespace_stale_metrics_dict.keys()):
                if parent_namespace != top_level_namespace:
                    continue
                for base_name in list(namespace_stale_metrics_dict[parent_namespace]['metrics'].keys()):
                    if not base_name.startswith(namespace):
                        continue
                    stale_metrics_count += 1
                    namespaces_namespace_stale_metrics_dict['stale_metrics']['stale_metrics'][base_name] = {}
                    last_timestamp = namespace_stale_metrics_dict[parent_namespace]['metrics'][base_name]
                    stale_for = now - int(float(last_timestamp))
                    namespaces_namespace_stale_metrics_dict['stale_metrics']['stale_metrics'][base_name]['last_timestamp'] = last_timestamp
                    namespaces_namespace_stale_metrics_dict['stale_metrics']['stale_metrics'][base_name]['stale_for'] = stale_for
        if remote_stale_metrics_dicts:
            for remote_stale_metrics_dict in remote_stale_metrics_dicts:
                for base_name in list(remote_stale_metrics_dict['stale_metrics'].keys()):
                    stale_metrics_count += 1
                    namespaces_namespace_stale_metrics_dict['stale_metrics']['stale_metrics'][base_name] = {}
                    last_timestamp = remote_stale_metrics_dict['stale_metrics'][base_name]['last_timestamp']
                    stale_for = remote_stale_metrics_dict['stale_metrics'][base_name]['stale_for']
                    namespaces_namespace_stale_metrics_dict['stale_metrics']['stale_metrics'][base_name]['last_timestamp'] = last_timestamp
                    namespaces_namespace_stale_metrics_dict['stale_metrics']['stale_metrics'][base_name]['stale_for'] = stale_for
        namespaces_namespace_stale_metrics_dict['stale_metrics']['stale_metrics_count'] = stale_metrics_count

    return namespaces_namespace_stale_metrics_dict
