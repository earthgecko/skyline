from __future__ import division
import logging
from os import path
import time
from ast import literal_eval

import traceback
from flask import request
from sqlalchemy.sql import select
from sqlalchemy.sql import text

import settings
import skyline_version
from skyline_functions import (
    mkdir_p,
    get_redis_conn_decoded,
    get_redis_conn,
)
from database import (
    get_engine, engine_disposal, ionosphere_table_meta, metrics_table_meta,
    ionosphere_matched_table_meta,
    ionosphere_layers_matched_table_meta,
    anomalies_table_meta,
)

skyline_version = skyline_version.__absolute_version__
skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
try:
    ENABLE_WEBAPP_DEBUG = settings.ENABLE_WEBAPP_DEBUG
except EnvironmentError as e:
    logger.error('error :: cannot determine ENABLE_WEBAPP_DEBUG from settings - %s' % e)
    ENABLE_WEBAPP_DEBUG = False


# @added 20210107 - Feature #3934: ionosphere_performance
def get_ionosphere_performance(
        metric, metric_like, from_timestamp, until_timestamp, format,
        # @added 20210128 - Feature #3934: ionosphere_performance
        # Improve performance and pass arguments to get_ionosphere_performance
        # for cache key
        anomalies, new_fps, fps_matched_count, layers_matched_count,
        sum_matches, title, period, height, width, fp_type, timezone_str):
    """
    Analyse the performance of Ionosphere on a metric or metric namespace and
    create the graph resources or json data as required.

    :rtype:  dict

    """
    import datetime
    import pytz
    import pandas as pd

    dev_null = None
    ionosphere_performance_debug = False
    determine_start_timestamp = False
    redis_conn = None
    redis_conn_decoded = None

    # @added 20210202 - Feature #3934: ionosphere_performance
    # Handle user timezone
    tz_from_timestamp_datetime_obj = None
    tz_until_timestamp_datetime_obj = None
    utc_epoch_timestamp = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone(datetime.timedelta(seconds=0)))
    determine_timezone_start_date = False
    determine_timezone_end_date = False
    user_timezone = pytz.timezone(timezone_str)
    utc_timezone = pytz.timezone('UTC')

    # @added 20210203 - Feature #3934: ionosphere_performance
    # Add default timestamp
    start_timestamp = 0
    end_timestamp = 0

    if from_timestamp == 0:
        start_timestamp = 0
        determine_start_timestamp = True
    if from_timestamp != 0:
        if ":" in from_timestamp:
            # @modified 20210202 - Feature #3934: ionosphere_performance
            # Handle user timezone
            if timezone_str == 'UTC':
                new_from_timestamp = time.mktime(datetime.datetime.strptime(from_timestamp, '%Y%m%d %H:%M').timetuple())
                logger.info('get_ionosphere_performance - new_from_timestamp - %s' % str(new_from_timestamp))
            else:
                utc_from_timestamp = time.mktime(datetime.datetime.strptime(from_timestamp, '%Y%m%d %H:%M').timetuple())
                logger.info('get_ionosphere_performance - utc_from_timestamp - %s' % str(utc_from_timestamp))
                from_timestamp_datetime_obj = datetime.datetime.strptime(from_timestamp, '%Y%m%d %H:%M')
                logger.info('get_ionosphere_performance - from_timestamp_datetime_obj - %s' % str(from_timestamp_datetime_obj))
                tz_offset = pytz.timezone(timezone_str).localize(from_timestamp_datetime_obj).strftime('%z')
                tz_from_date = '%s:00 %s' % (from_timestamp, tz_offset)
                logger.info('get_ionosphere_performance - tz_from_date - %s' % str(tz_from_date))
                tz_from_timestamp_datetime_obj = datetime.datetime.strptime(tz_from_date, '%Y%m%d %H:%M:%S %z')
                tz_epoch_timestamp = int((tz_from_timestamp_datetime_obj - utc_epoch_timestamp).total_seconds())
                new_from_timestamp = tz_epoch_timestamp
                # new_from_timestamp = time.mktime(datetime.datetime.strptime(tz_from_timestamp, '%Y%m%d %H:%M:%S %z').timetuple())
                logger.info('get_ionosphere_performance - new_from_timestamp - %s' % str(new_from_timestamp))
                determine_timezone_start_date = True
            start_timestamp = int(new_from_timestamp)
        # @added 20210203 - Feature #3934: ionosphere_performance
        # Add default timestamp
        else:
            if from_timestamp == 'all':
                start_timestamp = 0
                determine_start_timestamp = True
            else:
                start_timestamp = int(from_timestamp)
        if from_timestamp == 'all':
            start_timestamp = 0
            determine_start_timestamp = True
    if from_timestamp == 'all':
        start_timestamp = 0
        determine_start_timestamp = True

    if until_timestamp and until_timestamp != 'all':
        if ":" in until_timestamp:
            if timezone_str == 'UTC':
                new_until_timestamp = time.mktime(datetime.datetime.strptime(until_timestamp, '%Y%m%d %H:%M').timetuple())
            else:
                until_timestamp_datetime_obj = datetime.datetime.strptime(until_timestamp, '%Y%m%d %H:%M')
                tz_offset = pytz.timezone(timezone_str).localize(until_timestamp_datetime_obj).strftime('%z')
                tz_until_date = '%s:00 %s' % (until_timestamp, tz_offset)
                logger.info('get_ionosphere_performance - tz_until_date - %s' % str(tz_until_date))
                tz_until_timestamp_datetime_obj = datetime.datetime.strptime(tz_until_date, '%Y%m%d %H:%M:%S %z')
                tz_epoch_timestamp = int((tz_until_timestamp_datetime_obj - utc_epoch_timestamp).total_seconds())
                new_from_timestamp = tz_epoch_timestamp
                # new_from_timestamp = time.mktime(datetime.datetime.strptime(tz_until_timestamp, '%Y%m%d %H:%M:%S %z').timetuple())
            end_timestamp = int(new_until_timestamp)
        # @added 20210203 - Feature #3934: ionosphere_performance
        # Add default timestamp
        else:
            if until_timestamp == 'all':
                end_timestamp = int(time.time())
            else:
                end_timestamp = int(until_timestamp)

    determine_timezone_end_date = False
    if until_timestamp == 'all':
        end_timestamp = int(time.time())
        determine_timezone_end_date = True
    if until_timestamp == 0:
        end_timestamp = int(time.time())
        determine_timezone_end_date = True

    start_timestamp_str = str(start_timestamp)
    # end_timestamp_str = str(end_timestamp)

    if timezone_str == 'UTC':
        begin_date = datetime.datetime.utcfromtimestamp(start_timestamp).strftime('%Y-%m-%d')
        end_date = datetime.datetime.utcfromtimestamp(end_timestamp).strftime('%Y-%m-%d')
    else:
        if determine_timezone_start_date:
            logger.info('get_ionosphere_performance - determine_timezone_start_date - True')
            # non_tz_start_datetime_object = datetime.datetime.utcfromtimestamp(start_timestamp)
            # logger.info('get_ionosphere_performance - non_tz_start_datetime_object - %s' % str(non_tz_start_datetime_object))
            # tz_start_datetime_object = utc_timezone.localize(non_tz_start_datetime_object).astimezone(user_timezone)
            # logger.info('get_ionosphere_performance - tz_end_datetime_object - %s' % str(tz_start_datetime_object))
            begin_date = tz_from_timestamp_datetime_obj.strftime('%Y-%m-%d')
            logger.info('get_ionosphere_performance - begin_date with %s timezone applied - %s' % (timezone_str, str(begin_date)))
        else:
            begin_date = datetime.datetime.utcfromtimestamp(start_timestamp).strftime('%Y-%m-%d')
        if determine_timezone_end_date:
            logger.info('get_ionosphere_performance - determine_timezone_end_date - True')
            non_tz_end_datetime_object = datetime.datetime.utcfromtimestamp(end_timestamp)
            logger.info('get_ionosphere_performance - non_tz_end_datetime_object - %s' % str(non_tz_end_datetime_object))
            tz_end_datetime_object = utc_timezone.localize(non_tz_end_datetime_object).astimezone(user_timezone)
            logger.info('get_ionosphere_performance - tz_end_datetime_object - %s' % str(tz_end_datetime_object))
            end_date = tz_end_datetime_object.strftime('%Y-%m-%d')
            logger.info('get_ionosphere_performance - end_date with %s timezone applied - %s' % (timezone_str, str(end_date)))
        else:
            logger.info('get_ionosphere_performance - determine_timezone_end_date - False')
            end_date = datetime.datetime.utcfromtimestamp(end_timestamp).strftime('%Y-%m-%d')

    original_begin_date = begin_date

    # Determine period
    frequency = 'D'
    if 'period' in request.args:
        period = request.args.get('period', 'daily')
        if period == 'daily':
            frequency = 'D'
            extended_end_timestamp = end_timestamp + 86400
        if period == 'weekly':
            frequency = 'W'
            extended_end_timestamp = end_timestamp + (86400 * 7)
        if period == 'monthly':
            frequency = 'M'
            extended_end_timestamp = end_timestamp + (86400 * 30)
    extended_end_date = datetime.datetime.utcfromtimestamp(extended_end_timestamp).strftime('%Y-%m-%d')

    remove_prefix = False
    try:
        remove_prefix_str = request.args.get('remove_prefix', 'false')
        if remove_prefix_str != 'false':
            remove_prefix = True
    except Exception as e:
        dev_null = e
    # Allow for the removal of a prefix from the metric name
    use_metric_name = metric
    if remove_prefix:
        try:
            if remove_prefix_str.endswith('.'):
                remove_prefix = '%s' % remove_prefix_str
            else:
                remove_prefix = '%s.' % remove_prefix_str
            use_metric_name = metric.replace(remove_prefix, '')
        except Exception as e:
            logger.error('error :: failed to remove prefix %s from %s - %s' % (str(remove_prefix_str), metric, e))

    # @added 20210129 - Feature #3934: ionosphere_performance
    # Improve performance and pass arguments to get_ionosphere_performance
    # for cache key
    yesterday_timestamp = end_timestamp - 86400
    yesterday_end_date = datetime.datetime.utcfromtimestamp(yesterday_timestamp).strftime('%Y-%m-%d')
    metric_like_str = str(metric_like)
    metric_like_wildcard = metric_like_str.replace('.%', '')
    # @modified 20210202 - Feature #3934: ionosphere_performance
    # Handle user timezone
    yesterday_data_cache_key = 'performance.%s.metric.%s.metric_like.%s.begin_date.%s.tz.%s.anomalies.%s.new_fps.%s.fps_matched_count.%s.layers_matched_count.%s.sum_matches.%s.period.%s.fp_type.%s' % (
        str(yesterday_end_date), str(metric), metric_like_wildcard, str(begin_date),
        str(timezone_str), str(anomalies), str(new_fps), str(fps_matched_count),
        str(layers_matched_count), str(sum_matches), str(period), str(fp_type))
    logger.info('get_ionosphere_performance - yesterday_data_cache_key - %s' % yesterday_data_cache_key)

    try:
        redis_conn_decoded = get_redis_conn_decoded(skyline_app)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error('error :: get_ionosphere_performance :: get_redis_conn_decoded failed')
        dev_null = e
    yesterday_data_raw = None
    try:
        yesterday_data_raw = redis_conn_decoded.get(yesterday_data_cache_key)
    except Exception as e:
        trace = traceback.format_exc()
        fail_msg = 'error :: get_ionosphere_performance - could not get Redis data for - %s' % yesterday_data_cache_key
        logger.error(trace)
        logger.error(fail_msg)
        dev_null = e
    yesterday_data = None
    if yesterday_data_raw:
        try:
            yesterday_data = literal_eval(yesterday_data_raw)
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: get_ionosphere_performance - could not get literal_eval Redis data from key - %s' % yesterday_data_cache_key
            logger.error(trace)
            logger.error(fail_msg)
            dev_null = e
    if yesterday_data:
        logger.info('get_ionosphere_performance - using cache data from yesterday with %s items' % str(len(yesterday_data)))
        new_from = '%s 23:59:59' % yesterday_end_date
        # @modified 20210202 - Feature #3934: ionosphere_performance
        # Handle user timezone
        if timezone_str == 'UTC':
            new_from_timestamp = time.mktime(datetime.datetime.strptime(new_from, '%Y-%m-%d %H:%M:%S').timetuple())
            start_timestamp = int(new_from_timestamp) + 1
            begin_date = datetime.datetime.utcfromtimestamp(start_timestamp).strftime('%Y-%m-%d')
        else:
            tz_new_from_timestamp_datetime_obj = datetime.datetime.strptime(new_from, '%Y-%m-%d %H:%M:%S')
            tz_offset = pytz.timezone(timezone_str).localize(tz_new_from_timestamp_datetime_obj).strftime('%z')
            tz_from_timestamp = '%s %s' % (new_from, tz_offset)
            new_from_timestamp = time.mktime(datetime.datetime.strptime(tz_from_timestamp, '%Y-%m-%d %H:%M:%S %z').timetuple())
            start_timestamp = int(new_from_timestamp) + 1
            begin_date = tz_new_from_timestamp_datetime_obj.strftime('%Y-%m-%d')

        logger.info('get_ionosphere_performance - using cache data from yesterday, set new start_timestamp: %s, begin_date: %s' % (
            str(start_timestamp), str(begin_date)))
        determine_start_timestamp = False
    else:
        logger.info('get_ionosphere_performance - no cache data for yesterday_data')

    try:
        engine, fail_msg, trace = get_engine(skyline_app)
        logger.info(fail_msg)
    except Exception as e:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('%s' % fail_msg)
        logger.error('error :: get_ionosphere_performance - could not get a MySQL engine')
        dev_null = e
        raise  # to webapp to return in the UI
    if not engine:
        trace = 'none'
        fail_msg = 'error :: get_ionosphere_performance - engine not obtained'
        logger.error(fail_msg)
        raise
    try:
        metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error('error :: get_ionosphere_performance - failed to get metrics_table meta')
        dev_null = e
        if engine:
            engine_disposal(skyline_app, engine)
        raise  # to webapp to return in the UI

    metric_id = None
    metric_ids = []
    if metric_like != 'all':
        metric_like_str = str(metric_like)
        logger.info('get_ionosphere_performance - metric_like - %s' % metric_like_str)
        metrics_like_query = text("""SELECT id FROM metrics WHERE metric LIKE :like_string""")
        metric_like_wildcard = metric_like_str.replace('.%', '')
        request_key = '%s.%s.%s.%s' % (metric_like_wildcard, begin_date, end_date, frequency)
        plot_title = '%s - %s' % (metric_like_wildcard, period)
        logger.info('get_ionosphere_performance - metric like query, cache key being generated from request key - %s' % request_key)
        try:
            connection = engine.connect()
            result = connection.execute(metrics_like_query, like_string=metric_like_str)
            connection.close()
            for row in result:
                m_id = row['id']
                metric_ids.append(int(m_id))
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            logger.error('error :: get_ionosphere_performance - could not determine ids from metrics table LIKE query - %s' % e)
            if engine:
                engine_disposal(skyline_app, engine)
            return {}

        start_timestamp_date = None
        # If the from_timestamp is 0 or all
        if determine_start_timestamp:
            created_dates = []
            try:
                connection = engine.connect()
                # stmt = select([metrics_table.c.created_timestamp], metrics_table.c.id.in_(metric_ids)).limit(1)
                stmt = select([metrics_table.c.created_timestamp], metrics_table.c.id.in_(metric_ids))
                result = connection.execute(stmt)
                for row in result:
                    # start_timestamp_date = row['created_timestamp']
                    created_dates.append(row['created_timestamp'])
                    # break
                connection.close()
                start_timestamp_date = sorted(created_dates)[0]

                if not start_timestamp_date:
                    logger.error('error :: get_ionosphere_performance - could not determine created_timestamp - returning empty')
                    if engine:
                        engine_disposal(skyline_app, engine)
                    return {}

                start_timestamp_str = str(start_timestamp_date)
                logger.info('get_ionosphere_performance - determined start_timestamp_str - %s' % start_timestamp_str)
                new_from_timestamp = time.mktime(datetime.datetime.strptime(start_timestamp_str, '%Y-%m-%d %H:%M:%S').timetuple())
                start_timestamp = int(new_from_timestamp)
                logger.info('get_ionosphere_performance - determined start_timestamp - %s' % str(start_timestamp))
                begin_date = datetime.datetime.utcfromtimestamp(start_timestamp).strftime('%Y-%m-%d')
                logger.info('get_ionosphere_performance - determined begin_date - %s' % str(begin_date))

                # @added 20210203 - Feature #3934: ionosphere_performance
                # Handle user timezone
                if timezone_str != 'UTC':
                    logger.info('get_ionosphere_performance - determining %s datetime from UTC start_timestamp_str - %s' % (timezone_str, str(start_timestamp_str)))
                    from_timestamp_datetime_obj = datetime.datetime.strptime(start_timestamp_str, '%Y-%m-%d %H:%M:%S')
                    logger.info('get_ionosphere_performance - from_timestamp_datetime_obj - %s' % str(from_timestamp_datetime_obj))
                    tz_offset = pytz.timezone(timezone_str).localize(from_timestamp_datetime_obj).strftime('%z')
                    tz_from_date = '%s %s' % (start_timestamp_str, tz_offset)
                    logger.info('get_ionosphere_performance - tz_from_date - %s' % str(tz_from_date))
                    tz_from_timestamp_datetime_obj = datetime.datetime.strptime(tz_from_date, '%Y-%m-%d %H:%M:%S %z')
                    begin_date = tz_from_timestamp_datetime_obj.strftime('%Y-%m-%d')
                    logger.info('get_ionosphere_performance - begin_date with %s timezone applied - %s' % (timezone_str, str(begin_date)))

                determine_start_timestamp = False
                request_key = '%s.%s.%s.%s' % (metric_like_wildcard, begin_date, end_date, frequency)
            except Exception as e:
                trace = traceback.format_exc()
                logger.error(trace)
                logger.error('error :: get_ionosphere_performance - could not determine ids from metrics table LIKE query - %s' % e)
                if engine:
                    engine_disposal(skyline_app, engine)
                return {}

    logger.info('get_ionosphere_performance - metric_ids length - %s' % str(len(metric_ids)))

    if not metric_ids:
        # stmt = select([metrics_table]).where(metrics_table.c.id > 0)
        if metric == 'all':
            request_key = 'all.%s.%s.%s' % (begin_date, end_date, frequency)
            plot_title = 'All metrics - %s' % period
            logger.info('get_ionosphere_performance - metric all query, cache key being generated from request key - %s' % request_key)
            # If the from_timestamp is 0 or all
            if determine_start_timestamp:
                try:
                    connection = engine.connect()
                    stmt = select([metrics_table.c.created_timestamp]).limit(1)
                    result = connection.execute(stmt)
                    for row in result:
                        start_timestamp_date = row['created_timestamp']
                        break
                    connection.close()
                    start_timestamp_str = str(start_timestamp_date)
                    logger.info('get_ionosphere_performance - determined start_timestamp_str - %s' % start_timestamp_str)
                    new_from_timestamp = time.mktime(datetime.datetime.strptime(start_timestamp_str, '%Y-%m-%d %H:%M:%S').timetuple())
                    start_timestamp = int(new_from_timestamp)
                    logger.info('get_ionosphere_performance - determined start_timestamp - %s' % str(start_timestamp))
                    begin_date = datetime.datetime.utcfromtimestamp(start_timestamp).strftime('%Y-%m-%d')
                    logger.info('get_ionosphere_performance - determined begin_date - %s' % str(begin_date))

                    # @added 20210203 - Feature #3934: ionosphere_performance
                    # Handle user timezone
                    if timezone_str != 'UTC':
                        logger.info('get_ionosphere_performance - determining %s datetime from UTC start_timestamp_str - %s' % (timezone_str, str(start_timestamp_str)))
                        from_timestamp_datetime_obj = datetime.datetime.strptime(start_timestamp_str, '%Y-%m-%d %H:%M:%S')
                        logger.info('get_ionosphere_performance - from_timestamp_datetime_obj - %s' % str(from_timestamp_datetime_obj))
                        tz_offset = pytz.timezone(timezone_str).localize(from_timestamp_datetime_obj).strftime('%z')
                        tz_from_date = '%s %s' % (start_timestamp_str, tz_offset)
                        logger.info('get_ionosphere_performance - tz_from_date - %s' % str(tz_from_date))
                        tz_from_timestamp_datetime_obj = datetime.datetime.strptime(tz_from_date, '%Y-%m-%d %H:%M:%S %z')
                        begin_date = tz_from_timestamp_datetime_obj.strftime('%Y-%m-%d')
                        logger.info('get_ionosphere_performance - begin_date with %s timezone applied - %s' % (timezone_str, str(begin_date)))

                    determine_start_timestamp = False
                    request_key = 'all.%s.%s.%s' % (begin_date, end_date, frequency)
                    logger.info('get_ionosphere_performance - metric all query, determine_start_timestamp cache key being generated from request key - %s' % request_key)
                except Exception as e:
                    trace = traceback.format_exc()
                    logger.error(trace)
                    logger.error('error :: get_ionosphere_performance - could not determine ids from metrics table LIKE query - %s' % e)
                    if engine:
                        engine_disposal(skyline_app, engine)
                    return {}
            try:
                request_key = '%s.%s.%s.%s' % (metric, begin_date, end_date, frequency)
                plot_title = '%s - %s' % (use_metric_name, period)
                logger.info('get_ionosphere_performance - metric all query, cache key being generated from request key - %s' % request_key)
                connection = engine.connect()
                stmt = select([metrics_table]).where(metrics_table.c.id > 0)
                result = connection.execute(stmt)
                for row in result:
                    metric_id_str = row['id']
                    r_metric_id = int(metric_id_str)
                    metric_ids.append(r_metric_id)
                connection.close()
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: get_ionosphere_performance - could not determine metric ids from metrics - %s' % e)
                if engine:
                    engine_disposal(skyline_app, engine)
                raise

        if metric != 'all':
            logger.info('get_ionosphere_performance - metric - %s' % metric)
            try:
                request_key = '%s.%s.%s.%s' % (metric, begin_date, end_date, frequency)
                plot_title = '%s - %s' % (use_metric_name, period)
                logger.info('get_ionosphere_performance - metric query, cache key being generated from request key - %s' % request_key)
                connection = engine.connect()
                stmt = select([metrics_table]).where(metrics_table.c.metric == str(metric))
                result = connection.execute(stmt)
                for row in result:
                    metric_id_str = row['id']
                    metric_id = int(metric_id_str)
                connection.close()
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: get_ionosphere_performance - could not determine metric id from metrics - %s' % e)
                if engine:
                    engine_disposal(skyline_app, engine)
                raise
            if determine_start_timestamp and metric_id:
                try:
                    connection = engine.connect()
                    stmt = select([metrics_table.c.created_timestamp]).where(metrics_table.c.metric == str(metric))
                    result = connection.execute(stmt)
                    for row in result:
                        start_timestamp_date = row['created_timestamp']
                        break
                    connection.close()
                    start_timestamp_str = str(start_timestamp_date)
                    logger.info('get_ionosphere_performance - determined start_timestamp_str - %s' % start_timestamp_str)
                    new_from_timestamp = time.mktime(datetime.datetime.strptime(start_timestamp_str, '%Y-%m-%d %H:%M:%S').timetuple())
                    start_timestamp = int(new_from_timestamp)
                    logger.info('get_ionosphere_performance - determined start_timestamp - %s' % str(start_timestamp))
                    begin_date = datetime.datetime.utcfromtimestamp(start_timestamp).strftime('%Y-%m-%d')
                    logger.info('get_ionosphere_performance - determined begin_date - %s' % str(begin_date))
                    request_key = '%s.%s.%s.%s' % (metric, begin_date, end_date, frequency)
                    logger.info('get_ionosphere_performance - metric query, determine_start_timestamp cache key being generated from request key - %s' % request_key)
                except Exception as e:
                    trace = traceback.format_exc()
                    logger.error(trace)
                    logger.error('error :: get_ionosphere_performance - could not determine ids from metrics table LIKE query - %s' % e)
                    if engine:
                        engine_disposal(skyline_app, engine)
                    return {}

            logger.info('get_ionosphere_performance - metric - %s' % str(metric))
    logger.info('get_ionosphere_performance - metric_id - %s' % str(metric_id))

    if metric != 'all':
        if not metric_ids and not metric_id:
            if engine:
                engine_disposal(skyline_app, engine)
            logger.info('get_ionosphere_performance - no metric_id or metric_ids, nothing to do')
            performance = {
                'performance': {'date': None, 'reason': 'no metric data found'},
                'request_key': request_key,
                'success': False,
                'reason': 'no data for metric/s',
                'plot': None,
                'csv': None,
            }
            return performance

    logger.info('get_ionosphere_performance - metric_id: %s, metric_ids length: %s' % (
        str(metric_id), str(len(metric_ids))))

    # Create request_key performance directory
    ionosphere_dir = path.dirname(settings.IONOSPHERE_DATA_FOLDER)
    performance_dir = '%s/performance/%s' % (ionosphere_dir, request_key)
    if not path.exists(performance_dir):
        mkdir_p(performance_dir)

    # Report anomalies
    report_anomalies = False
    if 'anomalies' in request.args:
        anomalies_str = request.args.get('performance', 'false')
        if anomalies_str == 'true':
            report_anomalies = True
    anomalies = []
    anomalies_ts = []
    if report_anomalies:
        try:
            anomalies_table, log_msg, trace = anomalies_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('anomalies_table OK')
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to get anomalies_table meta')
            dev_null = e
            if engine:
                engine_disposal(skyline_app, engine)
            raise  # to webapp to return in the UI
        try:
            connection = engine.connect()
            if metric_ids:
                # stmt = select([anomalies_table.c.id, anomalies_table.c.anomaly_timestamp], anomalies_table.c.metric_id.in_(metric_ids)).\
                stmt = select([anomalies_table.c.id, anomalies_table.c.metric_id, anomalies_table.c.anomaly_timestamp]).\
                    where(anomalies_table.c.anomaly_timestamp >= start_timestamp).\
                    where(anomalies_table.c.anomaly_timestamp <= end_timestamp)
                result = connection.execute(stmt)
            elif metric_id:
                stmt = select([anomalies_table.c.id, anomalies_table.c.metric_id, anomalies_table.c.anomaly_timestamp]).\
                    where(anomalies_table.c.metric_id == int(metric_id)).\
                    where(anomalies_table.c.anomaly_timestamp >= start_timestamp).\
                    where(anomalies_table.c.anomaly_timestamp <= end_timestamp)
                result = connection.execute(stmt)
            else:
                stmt = select([anomalies_table.c.id, anomalies_table.c.metric_id, anomalies_table.c.anomaly_timestamp]).\
                    where(anomalies_table.c.anomaly_timestamp >= start_timestamp).\
                    where(anomalies_table.c.anomaly_timestamp <= end_timestamp)
                result = connection.execute(stmt)
            for row in result:
                r_metric_id = row['metric_id']
                append_result = False
                if r_metric_id == metric_id:
                    append_result = True
                if not append_result:
                    if r_metric_id in metric_ids:
                        append_result = True
                if append_result:
                    anomaly_id = row['id']
                    anomaly_timestamp = row['anomaly_timestamp']
                    anomalies.append(int(anomaly_timestamp))
                    # anomalies_ts.append([datetime.datetime.fromtimestamp(int(anomaly_timestamp)), int(anomaly_id)])
                    anomalies_ts.append([int(anomaly_timestamp), int(anomaly_id)])
            connection.close()
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: could not determine anomaly ids')
            dev_null = e
            if engine:
                engine_disposal(skyline_app, engine)
            raise
        logger.info('get_ionosphere_performance - anomalies_ts length - %s' % str(len(anomalies_ts)))

    fp_type = 'all'
    if 'fp_type' in request.args:
        fp_type = request.args.get('fp_type', 'all')

    # Get fp_ids
    fp_ids = []
    fp_ids_ts = []

    fp_ids_cache_key = 'performance.%s.%s.fp_ids' % (request_key, timezone_str)
    fp_ids_ts_cache_key = 'performance.%s.%s.fp_ids_ts' % (request_key, timezone_str)

    if not redis_conn_decoded:
        try:
            redis_conn_decoded = get_redis_conn_decoded(skyline_app)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: get_ionosphere_performance :: get_redis_conn_decoded failed')
            dev_null = e
    try:
        fp_ids_raw = redis_conn_decoded.get(fp_ids_cache_key)
    except Exception as e:
        trace = traceback.format_exc()
        fail_msg = 'error :: get_ionosphere_performance - could not get Redis data for - %s' % fp_ids_cache_key
        logger.error(trace)
        logger.error(fail_msg)
        dev_null = e
    if fp_ids_raw:
        try:
            fp_ids = literal_eval(fp_ids_raw)
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: get_ionosphere_performance - could not get literal_eval Redis data from key - %s' % fp_ids_cache_key
            logger.error(trace)
            logger.error(fail_msg)
            dev_null = e
    if fp_ids:
        logger.info('get_ionosphere_performance - using fp_ids from cache')

    try:
        fp_ids_ts_raw = redis_conn_decoded.get(fp_ids_ts_cache_key)
    except Exception as e:
        trace = traceback.format_exc()
        fail_msg = 'error :: get_ionosphere_performance - could not get Redis data for - %s' % fp_ids_ts_cache_key
        logger.error(trace)
        logger.error(fail_msg)
        dev_null = e
    if fp_ids_ts_raw:
        try:
            fp_ids_ts = literal_eval(fp_ids_ts_raw)
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: get_ionosphere_performance - could not get literal_eval Redis data from key - %s' % fp_ids_ts_cache_key
            logger.error(trace)
            logger.error(fail_msg)
            dev_null = e
    if fp_ids_ts:
        logger.info('get_ionosphere_performance - using fp_ids_ts from cache')

    if not fp_ids or not fp_ids_ts:
        try:
            ionosphere_table, log_msg, trace = ionosphere_table_meta(skyline_app, engine)
            logger.info(log_msg)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: get_ionosphere_performance - failed to get ionosphere_table meta')
            dev_null = e
            if engine:
                engine_disposal(skyline_app, engine)
            raise  # to webapp to return in the UI
        try:
            logger.info('get_ionosphere_performance - determining fp ids of type %s' % fp_type)
            connection = engine.connect()
            if metric_ids:
                if fp_type == 'user':
                    # stmt = select([ionosphere_table.c.id, ionosphere_table.c.anomaly_timestamp], ionosphere_table.c.metric_id.in_(metric_ids)).\
                    stmt = select([ionosphere_table.c.id, ionosphere_table.c.metric_id, ionosphere_table.c.anomaly_timestamp]).\
                        where(ionosphere_table.c.enabled == 1).\
                        where(ionosphere_table.c.anomaly_timestamp <= end_timestamp).\
                        where(ionosphere_table.c.generation <= 1)
                elif fp_type == 'learnt':
                    # stmt = select([ionosphere_table.c.id, ionosphere_table.c.anomaly_timestamp], ionosphere_table.c.metric_id.in_(metric_ids)).\
                    stmt = select([ionosphere_table.c.id, ionosphere_table.c.metric_id, ionosphere_table.c.anomaly_timestamp]).\
                        where(ionosphere_table.c.enabled == 1).\
                        where(ionosphere_table.c.anomaly_timestamp <= end_timestamp).\
                        where(ionosphere_table.c.generation >= 2)
                else:
                    # stmt = select([ionosphere_table.c.id, ionosphere_table.c.anomaly_timestamp], ionosphere_table.c.metric_id.in_(metric_ids)).\
                    stmt = select([ionosphere_table.c.id, ionosphere_table.c.metric_id, ionosphere_table.c.anomaly_timestamp]).\
                        where(ionosphere_table.c.enabled == 1).\
                        where(ionosphere_table.c.anomaly_timestamp <= end_timestamp)
                logger.info('get_ionosphere_performance - determining fp ids of type %s for metric_ids' % fp_type)
                result = connection.execute(stmt)
            elif metric_id:
                if fp_type == 'user':
                    stmt = select([ionosphere_table.c.id, ionosphere_table.c.metric_id, ionosphere_table.c.anomaly_timestamp]).\
                        where(ionosphere_table.c.metric_id == int(metric_id)).\
                        where(ionosphere_table.c.enabled == 1).\
                        where(ionosphere_table.c.anomaly_timestamp <= end_timestamp).\
                        where(ionosphere_table.c.generation <= 1)
                elif fp_type == 'learnt':
                    stmt = select([ionosphere_table.c.id, ionosphere_table.c.metric_id, ionosphere_table.c.anomaly_timestamp]).\
                        where(ionosphere_table.c.metric_id == int(metric_id)).\
                        where(ionosphere_table.c.enabled == 1).\
                        where(ionosphere_table.c.anomaly_timestamp <= end_timestamp).\
                        where(ionosphere_table.c.generation >= 2)
                else:
                    stmt = select([ionosphere_table.c.id, ionosphere_table.c.metric_id, ionosphere_table.c.anomaly_timestamp]).\
                        where(ionosphere_table.c.metric_id == int(metric_id)).\
                        where(ionosphere_table.c.enabled == 1).\
                        where(ionosphere_table.c.anomaly_timestamp <= end_timestamp)
                logger.info('get_ionosphere_performance - determining fp ids for metric_id')
                result = connection.execute(stmt)
            else:
                if fp_type == 'user':
                    stmt = select([ionosphere_table.c.id, ionosphere_table.c.metric_id, ionosphere_table.c.anomaly_timestamp]).\
                        where(ionosphere_table.c.enabled == 1).\
                        where(ionosphere_table.c.anomaly_timestamp <= end_timestamp).\
                        where(ionosphere_table.c.generation <= 1)
                elif fp_type == 'learnt':
                    stmt = select([ionosphere_table.c.id, ionosphere_table.c.metric_id, ionosphere_table.c.anomaly_timestamp]).\
                        where(ionosphere_table.c.enabled == 1).\
                        where(ionosphere_table.c.anomaly_timestamp <= end_timestamp).\
                        where(ionosphere_table.c.generation >= 2)
                else:
                    stmt = select([ionosphere_table.c.id, ionosphere_table.c.metric_id, ionosphere_table.c.anomaly_timestamp]).\
                        where(ionosphere_table.c.enabled == 1).\
                        where(ionosphere_table.c.anomaly_timestamp <= end_timestamp)
                logger.info('get_ionosphere_performance - determining fp ids for all metrics')
                result = connection.execute(stmt)
            for row in result:
                r_metric_id = row['metric_id']
                append_result = False
                if r_metric_id == metric_id:
                    append_result = True
                if r_metric_id in metric_ids:
                    append_result = True
                if append_result:
                    fp_id = row['id']
                    anomaly_timestamp = row['anomaly_timestamp']
                    fp_ids.append(int(fp_id))
                    fp_ids_ts.append([int(anomaly_timestamp), int(fp_id)])
            connection.close()
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: get_ionosphere_performance - could not determine fp_ids')
            dev_null = e
            if engine:
                engine_disposal(skyline_app, engine)
            raise
        logger.info('get_ionosphere_performance - fp_ids_ts length - %s' % str(len(fp_ids_ts)))

        if fp_ids:
            if not redis_conn:
                try:
                    redis_conn = get_redis_conn(skyline_app)
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: get_redis_conn failed for get_ionosphere_performance')
                    dev_null = e
            if redis_conn:
                try:
                    logger.info('get_ionosphere_performance - setting Redis performance key with fp_ids containing %s items' % str(len(fp_ids)))
                    redis_conn.setex(fp_ids_cache_key, 600, str(fp_ids))
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: get_redis_conn failed to set - %s' % fp_ids_cache_key)
                    dev_null = e
        if fp_ids_ts:
            if not redis_conn:
                try:
                    redis_conn = get_redis_conn(skyline_app)
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: get_redis_conn failed for get_ionosphere_performance')
                    dev_null = e
            if redis_conn:
                try:
                    logger.info('get_ionosphere_performance - setting Redis performance key with fp_ids_ts containing %s items' % str(len(fp_ids_ts)))
                    redis_conn.setex(fp_ids_ts_cache_key, 600, str(fp_ids_ts))
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: get_redis_conn failed to set - %s' % fp_ids_ts_cache_key)
                    dev_null = e
    # Get fp matches
    try:
        ionosphere_matched_table, log_msg, trace = ionosphere_matched_table_meta(skyline_app, engine)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error('error :: get_ionosphere_performance - failed to get ionosphere_matched_table_meta_table meta')
        dev_null = e
        if engine:
            engine_disposal(skyline_app, engine)
        raise  # to webapp to return in the UI
    fps_matched_ts = []
    if fp_ids:
        try:
            connection = engine.connect()
            # stmt = select([ionosphere_matched_table.c.id, ionosphere_matched_table.c.metric_timestamp], ionosphere_matched_table.c.fp_id.in_(fp_ids)).\
            stmt = select([ionosphere_matched_table.c.id, ionosphere_matched_table.c.fp_id, ionosphere_matched_table.c.metric_timestamp]).\
                where(ionosphere_matched_table.c.metric_timestamp >= start_timestamp).\
                where(ionosphere_matched_table.c.metric_timestamp <= end_timestamp)
            result = connection.execute(stmt)
            for row in result:
                append_result = False
                if metric == 'all' and metric_like == 'all':
                    append_result = True
                if not append_result:
                    fp_id = row['fp_id']
                    if fp_id in fp_ids:
                        append_result = True
                if append_result:
                    matched_id = row['id']
                    metric_timestamp = row['metric_timestamp']
                    fps_matched_ts.append([int(metric_timestamp), int(matched_id)])
            connection.close()
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: get_ionosphere_performance - could not determine timestamps from ionosphere_matched')
            dev_null = e
            if engine:
                engine_disposal(skyline_app, engine)
            raise
    logger.info('get_ionosphere_performance - fps_matched_ts - %s' % str(len(fps_matched_ts)))

    # Get layers matches
    try:
        ionosphere_layers_matched_table, log_msg, trace = ionosphere_layers_matched_table_meta(skyline_app, engine)
    except Exception as e:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: get_ionosphere_performance - failed to get ionosphere_layers_matched_table meta'
        logger.error('%s' % fail_msg)
        dev_null = e
        if engine:
            engine_disposal(skyline_app, engine)
        raise  # to webapp to return in the UI
    layers_matched_ts = []
    if fp_ids:
        try:
            connection = engine.connect()
            # stmt = select([ionosphere_layers_matched_table.c.id, ionosphere_layers_matched_table.c.anomaly_timestamp], ionosphere_layers_matched_table.c.fp_id.in_(fp_ids)).\
            stmt = select([ionosphere_layers_matched_table.c.id, ionosphere_layers_matched_table.c.fp_id, ionosphere_layers_matched_table.c.anomaly_timestamp]).\
                where(ionosphere_layers_matched_table.c.anomaly_timestamp >= start_timestamp).\
                where(ionosphere_layers_matched_table.c.anomaly_timestamp <= end_timestamp)
            result = connection.execute(stmt)
            for row in result:
                append_result = False
                if metric == 'all' and metric_like == 'all':
                    append_result = True
                if not append_result:
                    fp_id = row['fp_id']
                    if fp_id in fp_ids:
                        append_result = True
                if append_result:
                    matched_layers_id = row['id']
                    matched_timestamp = row['anomaly_timestamp']
                    layers_matched_ts.append([int(matched_timestamp), int(matched_layers_id)])
            connection.close()
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: get_ionosphere_performance - could not determine timestamps from ionosphere_layers_matched'
            logger.error('%s' % fail_msg)
            dev_null = e
            if engine:
                engine_disposal(skyline_app, engine)
            raise  # to webapp to return in the UI
    logger.info('get_ionosphere_performance - layers_matched_ts - %s' % str(len(layers_matched_ts)))

    anomalies_df = []
    if anomalies_ts:
        try:
            anomalies_df = pd.DataFrame(anomalies_ts, columns=['date', 'id'])
            anomalies_df['date'] = pd.to_datetime(anomalies_df['date'], unit='s')
            # @added 20210202 - Feature #3934: ionosphere_performance
            # Handle user timezone
            if timezone_str != 'UTC':
                anomalies_df['date'] = anomalies_df['date'].dt.tz_localize('UTC').dt.tz_convert(user_timezone)

            anomalies_df = anomalies_df.set_index(pd.DatetimeIndex(anomalies_df['date']))
            anomalies_df = anomalies_df.resample(frequency).apply({'id': 'count'})
            anomalies_df.rename(columns={'id': 'anomaly_count'}, inplace=True)
            if ionosphere_performance_debug:
                fname_out = '%s/%s.anomalies_df.csv' % (settings.SKYLINE_TMP_DIR, request_key)
                anomalies_df.to_csv(fname_out)
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: get_ionosphere_performance - could not create anomalies_df'
            logger.error('%s' % fail_msg)
            dev_null = e
            if engine:
                engine_disposal(skyline_app, engine)
            raise  # to webapp to return in the UI
    fp_ids_df = []
    fps_total_df = []
    if fp_ids_ts:
        try:
            fp_ids_df = pd.DataFrame(fp_ids_ts, columns=['date', 'id'])
            fp_ids_df['date'] = pd.to_datetime(fp_ids_df['date'], unit='s')
            # @added 20210202 - Feature #3934: ionosphere_performance
            # Handle user timezone
            if timezone_str != 'UTC':
                fp_ids_df['date'] = fp_ids_df['date'].dt.tz_localize('UTC').dt.tz_convert(user_timezone)

            fp_ids_df = fp_ids_df.set_index(pd.DatetimeIndex(fp_ids_df['date']))
            fp_ids_df = fp_ids_df.resample(frequency).apply({'id': 'count'})
            fps_total_df = fp_ids_df.cumsum()
            fp_ids_df.rename(columns={'id': 'new_fps_count'}, inplace=True)
            fps_total_df.rename(columns={'id': 'fps_total_count'}, inplace=True)
            if ionosphere_performance_debug:
                fname_out = '%s/%s.fp_ids_df.csv' % (settings.SKYLINE_TMP_DIR, request_key)
                fp_ids_df.to_csv(fname_out)
                fname_out = '%s/%s.fps_total_df.csv' % (settings.SKYLINE_TMP_DIR, request_key)
                fps_total_df.to_csv(fname_out)
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: get_ionosphere_performance - could not create fp_ids_df'
            logger.error('%s' % fail_msg)
            dev_null = e
            if engine:
                engine_disposal(skyline_app, engine)
            raise  # to webapp to return in the UI
    fps_matched_df = []
    if fps_matched_ts:
        try:
            fps_matched_df = pd.DataFrame(fps_matched_ts, columns=['date', 'id'])
            fps_matched_df['date'] = pd.to_datetime(fps_matched_df['date'], unit='s')
            # @added 20210202 - Feature #3934: ionosphere_performance
            # Handle user timezone
            if timezone_str != 'UTC':
                fps_matched_df['date'] = fps_matched_df['date'].dt.tz_localize('UTC').dt.tz_convert(user_timezone)

            fps_matched_df = fps_matched_df.set_index(pd.DatetimeIndex(fps_matched_df['date']))
            fps_matched_df = fps_matched_df.resample(frequency).apply({'id': 'count'})
            fps_matched_df.rename(columns={'id': 'fps_matched_count'}, inplace=True)
            if ionosphere_performance_debug:
                fname_out = '%s/%s.fps_matched_df.csv' % (settings.SKYLINE_TMP_DIR, request_key)
                fps_matched_df.to_csv(fname_out)
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: get_ionosphere_performance - could not create fps_matched_df'
            logger.error('%s' % fail_msg)
            dev_null = e
            if engine:
                engine_disposal(skyline_app, engine)
            raise  # to webapp to return in the UI
    layers_matched_df = []
    if layers_matched_ts:
        try:
            layers_matched_df = pd.DataFrame(layers_matched_ts, columns=['date', 'id'])
            layers_matched_df['date'] = pd.to_datetime(layers_matched_df['date'], unit='s')
            # @added 20210202 - Feature #3934: ionosphere_performance
            # Handle user timezone
            if timezone_str != 'UTC':
                layers_matched_df['date'] = layers_matched_df['date'].dt.tz_localize('UTC').dt.tz_convert(user_timezone)

            layers_matched_df = layers_matched_df.set_index(pd.DatetimeIndex(layers_matched_df['date']))
            layers_matched_df = layers_matched_df.resample(frequency).apply({'id': 'count'})
            layers_matched_df.rename(columns={'id': 'layers_matched_count'}, inplace=True)
            if ionosphere_performance_debug:
                fname_out = '%s/%s.layers_matched_df.csv' % (settings.SKYLINE_TMP_DIR, request_key)
                layers_matched_df.to_csv(fname_out)
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: get_ionosphere_performance - could not create layers_matched_df'
            logger.error('%s' % fail_msg)
            dev_null = e
            if engine:
                engine_disposal(skyline_app, engine)
            raise  # to webapp to return in the UI

    date_list = pd.date_range(begin_date, end_date, freq=frequency)
    date_list = date_list.format(formatter=lambda x: x.strftime('%Y-%m-%d'))
    use_end_date = end_date
    if not date_list:
        date_list = pd.date_range(begin_date, extended_end_date, freq=frequency)
        date_list = date_list.format(formatter=lambda x: x.strftime('%Y-%m-%d'))
        use_end_date = extended_end_date
    # logger.debug('debug :: get_ionosphere_performance - date_list - %s' % str(date_list))

    # performance_df = pd.DataFrame(date_list, columns=['date'])
    performance_df = pd.DataFrame({'date': pd.date_range(begin_date, use_end_date, freq=frequency), 'day': date_list})
    # @added 20210202 - Feature #3934: ionosphere_performance
    # Handle user timezone
    if timezone_str != 'UTC':
        # It is already timezone aware in the begin_date and end_date, so I one
        # just reassign the same tzinfo to the date index?
        # Fuck yeah!!! (I sound like Colt Bennet)
        # performance_df['date'] = performance_df['date'].dt.tz_localize('UTC').dt.tz_convert(user_timezone)
        performance_df['date'] = performance_df['date'].dt.tz_localize(user_timezone).dt.tz_convert(user_timezone)

    # performance_df = performance_df.set_index(pd.DatetimeIndex(performance_df['date']))
    performance_df = performance_df.set_index(['date'])

    if len(anomalies_df) > 0 and report_anomalies:
        performance_df = pd.merge(performance_df, anomalies_df, how='outer', on='date')
        performance_df.sort_values('date')
        performance_df['anomaly_count'] = performance_df['anomaly_count'].fillna(0)
    if len(anomalies_df) == 0 and report_anomalies:
        performance_df['anomaly_count'] = 0

    # Report new fp count per day
    report_new_fps = False
    if 'new_fps' in request.args:
        new_fps_str = request.args.get('new_fps', 'false')
        if new_fps_str == 'true':
            report_new_fps = True

    if len(fp_ids_df) > 0 and report_new_fps:
        if yesterday_data:
            new_fp_ids_df = fp_ids_df.loc[yesterday_end_date:use_end_date]
            performance_df = pd.merge(performance_df, new_fp_ids_df, how='outer', on='date')
            del new_fp_ids_df
        else:
            performance_df = pd.merge(performance_df, fp_ids_df, how='outer', on='date')
        performance_df.sort_values('date')
        performance_df['new_fps_count'] = performance_df['new_fps_count'].fillna(0)
    # else:
    #    performance_df['new_fps_count'] = 0

    # Report running total fp count per day
    report_total_fps = False
    if 'total_fps' in request.args:
        total_fps_str = request.args.get('total_fps', 'false')
        if total_fps_str == 'true':
            report_total_fps = True
    if len(fps_total_df) > 0 and report_total_fps:
        if yesterday_data:
            new_fps_total = fps_total_df.loc[yesterday_end_date:use_end_date]
            performance_df = pd.merge(performance_df, new_fps_total, how='outer', on='date')
        else:
            performance_df = pd.merge(performance_df, fps_total_df, how='outer', on='date')
        performance_df.sort_values('date')
        performance_df['fps_total_count'].fillna(method='ffill', inplace=True)
        logger.info('get_ionosphere_performance - second step of creating performance_df complete, dataframe length - %s' % str(len(performance_df)))
    if len(fps_total_df) == 0 and report_total_fps:
        performance_df['fps_total_count'] = 0

    # Report fps_matched_count per day
    report_fps_matched_count = False
    if 'fps_matched_count' in request.args:
        fps_matched_count_str = request.args.get('fps_matched_count', 'false')
        if fps_matched_count_str == 'true':
            report_fps_matched_count = True
    # Report layers_matched_count per day
    report_layers_matched_count = False
    if 'layers_matched_count' in request.args:
        layers_matched_count_str = request.args.get('layers_matched_count', 'false')
        if layers_matched_count_str == 'true':
            report_layers_matched_count = True
    # Report sum_matches per day
    report_sum_matches = False
    if 'sum_matches' in request.args:
        sum_matches_str = request.args.get('sum_matches', 'false')
        if sum_matches_str == 'true':
            report_sum_matches = True

    if len(fps_matched_df) > 0 and report_fps_matched_count and not report_sum_matches:
        performance_df = pd.merge(performance_df, fps_matched_df, how='outer', on='date')
        performance_df.sort_values('date')
        performance_df['fps_matched_count'] = performance_df['fps_matched_count'].fillna(0)
        logger.info('get_ionosphere_performance - third step of creating performance_df complete, dataframe length - %s' % str(len(performance_df)))
    if len(fps_matched_df) == 0 and report_fps_matched_count and not report_sum_matches:
        performance_df['fps_matched_count'] = 0

    if len(layers_matched_df) > 0 and report_layers_matched_count and not report_sum_matches:
        performance_df = pd.merge(performance_df, layers_matched_df, how='outer', on='date')
        performance_df.sort_values('date')
        performance_df['layers_matched_count'] = performance_df['layers_matched_count'].fillna(0)
        logger.info('get_ionosphere_performance - fourth step of creating performance_df complete, dataframe length - %s' % str(len(performance_df)))
    if len(layers_matched_df) == 0 and report_layers_matched_count and not report_sum_matches:
        performance_df['layers_matched_count'] = 0

    if report_sum_matches:
        logger.info('get_ionosphere_performance - creating matches_sum_df to calculate totals and merge with performance_df')
        matches_sum_df = pd.DataFrame({'date': pd.date_range(begin_date, use_end_date, freq=frequency), 'day': date_list})
        if timezone_str != 'UTC':
            matches_sum_df['date'] = matches_sum_df['date'].dt.tz_localize('UTC').dt.tz_convert(user_timezone)

        matches_sum_df = matches_sum_df.set_index(['date'])
        if len(fps_matched_df) > 0:
            matches_sum_df = pd.merge(matches_sum_df, fps_matched_df, how='outer', on='date')
            matches_sum_df.sort_values('date')
            matches_sum_df['fps_matched_count'] = matches_sum_df['fps_matched_count'].fillna(0)
        if len(fps_matched_df) == 0:
            matches_sum_df['fps_matched_count'] = 0
        if len(layers_matched_df) > 0:
            matches_sum_df = pd.merge(matches_sum_df, layers_matched_df, how='outer', on='date')
            matches_sum_df.sort_values('date')
            matches_sum_df['layers_matched_count'] = matches_sum_df['layers_matched_count'].fillna(0)
        if len(layers_matched_df) == 0:
            matches_sum_df['layers_matched_count'] = 0
        matches_sum_df['total_matches'] = matches_sum_df['fps_matched_count'] + matches_sum_df['layers_matched_count']
        sum_df = matches_sum_df[['total_matches']].copy()
        logger.info('get_ionosphere_performance - sum_df has %s rows' % str(len(sum_df)))
        performance_df = pd.merge(performance_df, sum_df, how='outer', on='date')
        performance_df.sort_values('date')
        performance_df['total_matches'] = performance_df['total_matches'].fillna(0)

    if yesterday_data:
        ydf = pd.DataFrame(yesterday_data)
        ydf = ydf.transpose()

        # ydf['day'] = pd.to_datetime(ydf['day'], format='%Y-%m-%d')
        # ydf['day'] = ydf['day'].astype('datetime64')
        # ydf['date'] = ydf['day']
        # ydf = ydf.set_index(['date'])

        ydf['date'] = ydf['day']
        ydf['date'] = pd.to_datetime(ydf['date'], format='%Y-%m-%d')
        ydf['date'] = ydf['date'].astype('datetime64')
        # @added 20210202 - Feature #3934: ionosphere_performance
        # Handle user timezone
        if timezone_str != 'UTC':
            ydf['date'] = ydf['date'].dt.tz_localize('UTC').dt.tz_convert(user_timezone)

        ydf = ydf.set_index(['date'])

        logger.info('get_ionosphere_performance - yesterday_data dataframe has %s rows' % str(len(ydf)))
        logger.info('get_ionosphere_performance - performance_df dataframe has %s rows before merge' % str(len(performance_df)))
        # df_merged = reduce(lambda left, right: pd.merge(left, right, on=['date'], how='outer'), [ydf, performance_df])
        # df_merged = pd.merge(ydf, performance_df, on='day', how='outer')
        df_merged = pd.concat([ydf, performance_df])
        performance_df = df_merged
        logger.info('get_ionosphere_performance - performance_df dataframe has %s rows after merge' % str(len(performance_df)))

    if len(performance_df) > 0:
        logger.info('get_ionosphere_performance - final step of creating performance_df with %s columns' % str(len(performance_df.columns)))
        logger.info('get_ionosphere_performance - columns - %s' % str(performance_df.columns))
        # @modified 20210203 - Feature #3934: ionosphere_performance
        # Correct conditional operator
        # performance_df = performance_df[(performance_df.index > begin_date) & (performance_df.index <= end_date)]
        performance_df = performance_df[(performance_df.index >= original_begin_date) & (performance_df.index <= use_end_date)]
        performance_df = performance_df.dropna(how='any')

    plot_png = None
    if format != 'json':
        # Custom title
        if 'title' in request.args:
            title_str = request.args.get('title', 'default')
            if title_str != 'default':
                if title_str == 'none':
                    plot_title = ''
                else:
                    plot_title = title_str
        style = []
        if 'anomaly_count' in performance_df.columns:
            style.append('r')
        if 'new_fps_count' in performance_df.columns:
            style.append('orange')
        if 'fps_total_count' in performance_df.columns:
            style.append('brown')
        if 'fps_matched_count' in performance_df.columns:
            style.append('blue')
        if 'layers_matched_count' in performance_df.columns:
            style.append('deepskyblue')
        if 'total_matches' in performance_df.columns:
            style.append('green')

        # Custom size
        width = 14
        height = 7
        if 'width' in request.args:
            width_str = request.args.get('width', '14')
            if width_str:
                try:
                    width = int(width_str)
                except Exception as e:
                    logger.error('get_ionosphere_performance - width argument not int - %s' % str(width))
                    dev_null = e
        if 'height' in request.args:
            height_str = request.args.get('height', '7')
            if height_str:
                try:
                    height = int(height_str)
                except Exception as e:
                    logger.error('get_ionosphere_performance - height argument not int - %s' % str(height))
                    dev_null = e
        plot_png = '%s/%s.png' % (performance_dir, request_key)

        # @added 20210127 - Feature #3934: ionosphere_performance
        # Added secondary axis for fps_total_count
        plot_done = False
        if 'fps_total_count' in performance_df.columns and report_total_fps:
            import matplotlib.pyplot as plt

            # @added 20210203 - Feature #3934: ionosphere_performance
            # Correct axes labels
            ylabel = 'Anomalies and matches'
            if 'new_fps_count' in performance_df.columns:
                ylabel = 'Anomalies, new fps and matches'

            plt.figure()
            ax = performance_df.plot(
                secondary_y=['fps_total_count'],
                figsize=(width, height), title=plot_title, linewidth=1,
                style=style)
            # @modified 20210203 - Feature #3934: ionosphere_performance
            # Correct axes labels
            # ax.set_ylabel('Total fps')
            # # ax.right_ax.set_ylabel('Anomalies, new fps and matches')
            ax.set_ylabel(ylabel)
            ax.right_ax.set_ylabel('Total features profiles', rotation=-90)
            plt.savefig(plot_png)
            plot_done = True

        if not plot_done:
            fig = performance_df.plot(
                figsize=(width, height), title=plot_title, linewidth=1,
                style=style).get_figure()
            fig.savefig(plot_png)

    performance_dict = {}
    csv_file = '%s/%s.csv' % (performance_dir, request_key)

    if len(performance_df) > 0:
        new_performance_df = performance_df.reset_index(drop=True)
        new_performance_df.rename(columns={'day': 'date'}, inplace=True)

        logger.info('get_ionosphere_performance - created new_performance_df with %s columns' % str(new_performance_df.columns))
        new_performance_df.to_csv(csv_file, index=False)
        # performance_dict = new_performance_df.to_dict()
        performance_dict = new_performance_df.to_dict('records')
        # logger.debug('get_ionosphere_performance - performance_dict: %s' % str(performance_dict))

    yesterday_data_dict = {}
    yesterday_date_time_obj = datetime.datetime.strptime(yesterday_end_date, '%Y-%m-%d')
    # logger.debug('debug :: get_ionosphere_performance - performance_dict - %s' % str(performance_dict))
    for item in performance_dict:
        for key in item:
            if key == 'date':
                date_time_str = item['date']
                date_time_obj = datetime.datetime.strptime(date_time_str, '%Y-%m-%d')
                if date_time_obj <= yesterday_date_time_obj:
                    yesterday_data_dict[date_time_str] = {}
                    for a_key in item:
                        if a_key != 'date':
                            yesterday_data_dict[date_time_str][a_key] = item[a_key]
                        else:
                            yesterday_data_dict[date_time_str]['day'] = item[a_key]
    if yesterday_data_dict:
        if not yesterday_data:
            if not redis_conn:
                try:
                    redis_conn = get_redis_conn(skyline_app)
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: get_redis_conn failed for get_ionosphere_performance')
                    dev_null = e
            if redis_conn:
                try:
                    logger.info('get_ionosphere_performance - setting Redis performance key with yesterday_data containing %s items' % str(len(yesterday_data_dict)))
                    redis_conn.setex(yesterday_data_cache_key, 648000, str(yesterday_data_dict))
                    del yesterday_data_dict
                    del yesterday_data
                except Exception as e:
                    dev_null = e

    if engine:
        engine_disposal(skyline_app, engine)

    performance = {
        'performance': performance_dict,
        'request_key': request_key,
        'success': True,
        'reason': 'data found for metric/s',
        'plot': plot_png,
        'csv': csv_file,
    }

    # Clean up objects to free up memory
    try:
        del metric_ids
    except Exception as e:
        dev_null = e
    try:
        del anomalies
    except Exception as e:
        dev_null = e
    try:
        del anomalies_ts
    except Exception as e:
        dev_null = e
    try:
        del fp_ids
    except Exception as e:
        dev_null = e
    try:
        del fp_ids_ts
    except Exception as e:
        dev_null = e
    try:
        del fps_matched_ts
    except Exception as e:
        dev_null = e
    try:
        del layers_matched_ts
    except Exception as e:
        dev_null = e
    try:
        del anomalies_df
    except Exception as e:
        dev_null = e
    try:
        del fp_ids_df
    except Exception as e:
        dev_null = e
    try:
        del fps_total_df
    except Exception as e:
        dev_null = e
    try:
        del fps_matched_df
    except Exception as e:
        dev_null = e
    try:
        del layers_matched_df
    except Exception as e:
        dev_null = e
    try:
        del date_list
    except Exception as e:
        dev_null = e
    try:
        del performance_df
    except Exception as e:
        dev_null = e
    try:
        del matches_sum_df
    except Exception as e:
        dev_null = e
    try:
        del sum_df
    except Exception as e:
        dev_null = e
    try:
        del new_performance_df
    except Exception as e:
        dev_null = e
    try:
        del performance_dict
    except Exception as e:
        dev_null = e
    if dev_null:
        del dev_null
    return performance
