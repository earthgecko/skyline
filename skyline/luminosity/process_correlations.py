"""
process_correlations.py
"""
import logging
import traceback
# @modified 20191115 - Branch #3262: py3
# from math import ceil
from timeit import default_timer as timer
# @added 20180720 - Feature #2464: luminosity_remote_data
# Added requests and ast
from ast import literal_eval
# @added 20200428 - Feature #3510: Enable Luminosity to handle correlating namespaces only
#                   Feature #3500: webapp - crucible_process_metrics
#                   Feature #1448: Crucible web UI
# @modified 20230316 - Task #4872: Optimise luminosity for labelled_metrics
# Added strftime and gmtime to determin illuminance.all keys
from time import time, strftime, gmtime

# @added 20220722 - Task #4624: Change all dict copy to deepcopy
import copy

from redis import StrictRedis
from msgpack import Unpacker

from luminol.anomaly_detector import AnomalyDetector
from luminol.correlator import Correlator

# @added 20180720 - Feature #2464: luminosity_remote_data
# Added requests and ast
import requests

import settings
from skyline_functions import (
    # @modified 20230107 - Task #4022: Move mysql_select calls to SQLAlchemy
    #                      Task #4778: v4.0.0 - update dependencies
    # Deprecated mysql_select, use sqlalchemy rather than string-based query construction
    # mysql_select,
    is_derivative_metric, nonNegativeDerivative,
    # @added 20191030 - Bug #3266: py3 Redis binary objects not strings
    #                   Branch #3262: py3
    # Added a single functions to deal with Redis connection and the
    # charset='utf-8', decode_responses=True arguments required in py3
    get_redis_conn, get_redis_conn_decoded,
    # @added 20200506 - Feature #3532: Sort all time series
    sort_timeseries,
    # @added 20201207 - Feature #3858: skyline_functions - correlate_or_relate_with
    # @modified 20220504 - Task #4544: Handle EXTERNAL_SETTINGS in correlate_or_relate_with
    # Moved to new functions.metrics.correlate_or_relate_with
    # correlate_or_relate_with,
)

# @added 20210820 - Task #4030: refactoring
#                   Feature #4164: luminosity - cloudbursts
from functions.timeseries.determine_data_frequency import determine_data_frequency

# @added 20220225 - Feature #4000: EXTERNAL_SETTINGS
# Use correlation related settings from external settings
from functions.settings.get_external_settings import get_external_settings

# @added 20220504 - Task #4544: Handle EXTERNAL_SETTINGS in correlate_or_relate_with
#                   Feature #3858: skyline_functions - correlate_or_relate_with
# New function to handle external_settings
from functions.metrics.correlate_or_relate_with import correlate_or_relate_with

# @added 20200428 - Feature #3510: Enable Luminosity to handle correlating namespaces only
#                   Feature #3512: matched_or_regexed_in_list function
from matched_or_regexed_in_list import matched_or_regexed_in_list

from functions.metrics.get_base_name_from_labelled_metrics_name import get_base_name_from_labelled_metrics_name
from functions.metrics.get_labelled_metric_dict import get_labelled_metric_dict
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name

# @modified 20230107 - Task #4022: Move mysql_select calls to SQLAlchemy
#                      Task #4778: v4.0.0 - update dependencies
# Use sqlalchemy rather than string-based query construction
from sqlalchemy import select
from database import (
    get_engine, engine_disposal, anomalies_table_meta, metrics_table_meta)

try:
    # @modified 20200606 - Bug #3572: Apply list to settings import
    LUMINOSITY_CORRELATE_NAMESPACES_ONLY = list(settings.LUMINOSITY_CORRELATE_NAMESPACES_ONLY)
except:
    LUMINOSITY_CORRELATE_NAMESPACES_ONLY = []

# @added 20200924 - Feature #3756: LUMINOSITY_CORRELATION_MAPS
try:
    # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
    # LUMINOSITY_CORRELATION_MAPS = settings.LUMINOSITY_CORRELATION_MAPS.copy()
    LUMINOSITY_CORRELATION_MAPS = copy.deepcopy(settings.LUMINOSITY_CORRELATION_MAPS)
except:
    LUMINOSITY_CORRELATION_MAPS = {}

# @added 20210124 - Feature #3956: luminosity - motifs
# @modified 20230107 - Task #4778: v4.0.0 - update dependencies
# COMMENTED OUT ALL THE LUMINOSITY_ANALYSE_MOTIFS sections as these are WIP and
# TODO
# try:
#     LUMINOSITY_ANALYSE_MOTIFS = settings.LUMINOSITY_ANALYSE_MOTIFS
# except:
#     LUMINOSITY_ANALYSE_MOTIFS = True
# if LUMINOSITY_ANALYSE_MOTIFS:
#     from collections import Counter
#     # @modified 20230107 - Task #4786: Switch from matrixprofile to stumpy
#     # The matrix-profile-foundation/matrixprofile library is no longer maintained
#     # and this has been replaced with the stumpy matrixprofile implementation.
#     # A switch to the library used is done here and it falls back to matrixprofile
#     # if stumpy is not available, however as of v4.0.0 matrixprofile has been
#     # removed from the requirements.txt
#     # import matrixprofile as mp
#     stumpy_available = False
#     try:
#         from stumpy import stump
#     except:
#         stumpy_available = False
#     if not stumpy_available:
#         try:
#             import matrixprofile as mp
#         except:
#             mp = None
#
#     import numpy as np

# Database configuration
config = {'user': settings.PANORAMA_DBUSER,
          'password': settings.PANORAMA_DBUSERPASS,
          'host': settings.PANORAMA_DBHOST,
          'port': settings.PANORAMA_DBPORT,
          'database': settings.PANORAMA_DATABASE,
          'raise_on_warnings': True}

# @added 20180423 - Feature #2360: CORRELATE_ALERTS_ONLY
#                   Branch #2270: luminosity
# Only correlate metrics with an alert setting
try:
    CORRELATE_ALERTS_ONLY = settings.CORRELATE_ALERTS_ONLY
except:
    CORRELATE_ALERTS_ONLY = True

skyline_app = 'luminosity'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

# @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
# @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
#                      Branch #3262: py3
# Use get_redis_conn and get_redis_conn_decoded to use on Redis sets when the bytes
# types need to be decoded as utf-8 to str
# if settings.REDIS_PASSWORD:
#     redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
# else:
#     redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)

# @added 20191030 - Bug #3266: py3 Redis binary objects not strings
#                   Branch #3262: py3
# Added a single functions to deal with Redis connection and the
# charset='utf-8', decode_responses=True arguments required in py3
redis_conn = get_redis_conn(skyline_app)
redis_conn_decoded = get_redis_conn_decoded(skyline_app)


# @added 20201203 - Feature #3860: luminosity - handle low frequency data
# Handle varying metric resolutions
def determine_resolution(timeseries):
    """
    Determine the resolution of the timeseries data
    """
    resolution = 60
    try:
        ts_resolution = int(timeseries[-1][0]) - int(timeseries[-2][0])
        if ts_resolution != 60:
            resolution = ts_resolution
        # Handle data that has any big airgaps as best effort as possible by
        # checking the previous timestamps
        if ts_resolution > 3601:
            ts_resolution = int(timeseries[-3][0]) - int(timeseries[-4][0])
        if ts_resolution != 60:
            resolution = ts_resolution
    except:
        pass
    return resolution


def get_anomaly(request_type):
    """
    Query the database for the anomaly details
    """

    # logger = logging.getLogger(skyline_app_logger)

    if isinstance(request_type, int):
        latest = False
    else:
        latest = True

    # @added 20230107 - Task #4022: Move mysql_select calls to SQLAlchemy
    #                   Task #4778: v4.0.0 - update dependencies
    # Use sqlalchemy rather than string-based query construction
    engine = None
    try:
        engine, fail_msg, trace = get_engine(skyline_app)
        if fail_msg != 'got MySQL engine':
            logger.error('error :: get_anomaly :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            logger.error('error :: get_anomaly :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: get_anomaly :: could not get a MySQL engine - %s' % str(err))
    try:
        anomalies_table, fail_msg, trace = anomalies_table_meta(skyline_app, engine)
        if fail_msg != 'anomalies_table meta reflected OK':
            logger.error('error :: get_anomaly :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            logger.error('error :: get_anomaly :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: get_anomaly :: anomalies_table_meta - %s' % str(err))

    if latest:
        query = 'SELECT * FROM anomalies ORDER BY id DESC LIMIT 1'
    else:
        # @modified 20230107 - Task #4022: Move mysql_select calls to SQLAlchemy
        #                      Task #4778: v4.0.0 - update dependencies
        # Use sqlalchemy rather than string-based query construction
        # query = 'SELECT * FROM anomalies WHERE id=%s' % str(request_type)
        try:
            query = select(anomalies_table).where(anomalies_table.c.id == request_type)
        except Exception as err:
            logger.error('error :: get_anomaly :: failed to build anomaly query - %s')

    try:
        # @modified 20230107 - Task #4022: Move mysql_select calls to SQLAlchemy
        #                      Task #4778: v4.0.0 - update dependencies
        # Use sqlalchemy rather than string-based query construction
        # results = mysql_select(skyline_app, query)
        connection = engine.connect()
        results = connection.execute(query)
        for row in results:
            anomaly_id = int(row['id'])
            metric_id = int(row['metric_id'])
            anomaly_timestamp = int(row['anomaly_timestamp'])
            break
        connection.close()
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: get_anomaly :: failed to determine anomaly details from the database - %s')
        return (False, False, False, False)

    # @added 20230107 - Task #4022: Move mysql_select calls to SQLAlchemy
    #                   Task #4778: v4.0.0 - update dependencies
    # Use sqlalchemy rather than string-based query construction
    try:
        metrics_table, fail_msg, trace = metrics_table_meta(skyline_app, engine)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: get_anomaly :: failed to get metrics_table meta - %s' % err)

    try:
        # @modified 20230107 - Task #4022: Move mysql_select calls to SQLAlchemy
        #                      Task #4778: v4.0.0 - update dependencies
        # Use sqlalchemy rather than string-based query construction
        # Now interpolated above
        # anomaly_id = int(results[0][0])
        # metric_id = int(results[0][1])
        # anomaly_timestamp = int(results[0][5])

        # @modified 20230107 - Task #4022: Move mysql_select calls to SQLAlchemy
        #                      Task #4778: v4.0.0 - update dependencies
        # Use sqlalchemy rather than string-based query construction
        # query = 'SELECT metric FROM metrics WHERE id=%s' % str(metric_id)
        # results = mysql_select(skyline_app, query)
        # base_name = str(results[0][0])
        stmt = select(metrics_table.c.metric).where(metrics_table.c.id == metric_id)
        connection = engine.connect()
        results = connection.execute(stmt)
        for row in results:
            base_name = row['metric']
            break
        connection.close()
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: get_anomaly :: failed to determine base_name from database - %s' % err)
        return (False, False, False, False)

    # @added 20230107 - Task #4022: Move mysql_select calls to SQLAlchemy
    #                   Task #4778: v4.0.0 - update dependencies
    if engine:
        engine_disposal(skyline_app, engine)

    return (anomaly_id, metric_id, anomaly_timestamp, base_name)


def get_anomalous_ts(base_name, anomaly_timestamp):

    # logger = logging.getLogger(skyline_app_logger)

    # @modified 20230316 - Task #4872: Optimise luminosity for labelled_metrics
    # Added resolution to the return
    resolution = 0

    # @added 20180423 - Feature #2360: CORRELATE_ALERTS_ONLY
    #                   Branch #2270: luminosity
    # Only correlate metrics with an alert setting
    if CORRELATE_ALERTS_ONLY:
        try:
            # @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            # smtp_alerter_metrics = list(redis_conn.smembers('analyzer.smtp_alerter_metrics'))
            # @modified 20200421 - Feature #3306: Record anomaly_end_timestamp
            #                      Branch #2270: luminosity
            #                      Branch #3262: py3
            # Changed to use the aet Redis set, used to determine and record the
            # anomaly_end_timestamp, some transient sets need to copied so that
            # the data always exists, even if it is sourced from a transient set.
            # smtp_alerter_metrics = list(redis_conn_decoded.smembers('analyzer.smtp_alerter_metrics'))
            smtp_alerter_metrics = list(redis_conn_decoded.smembers('aet.analyzer.smtp_alerter_metrics'))
        except:
            smtp_alerter_metrics = []

        # @added 20220809 - Task #2732: Prometheus to Skyline
        #                   Branch #4300: prometheus
        labelled_metric_name = None
        use_base_name = str(base_name)
        if '_tenant_id="' in base_name:
            try:
                metric_id = get_metric_id_from_base_name(skyline_app, base_name)
                if metric_id:
                    labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
                    use_base_name = str(labelled_metric_name)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: get_anomalous_ts :: get_metric_id_from_base_name for %s failed - %s' % (
                    base_name, err))
        if base_name.startswith('labelled_metrics.') or labelled_metric_name:
            if not labelled_metric_name:
                labelled_metric_name = str(base_name)
            metric_id_str = labelled_metric_name.replace('labelled_metrics.', '', 1)
            metric_id = int(float(metric_id_str))
        if labelled_metric_name:
            smtp_alerter_enabled = False
            try:
                smtp_alerter_enabled = redis_conn_decoded.hget('metrics_manager.smtp_alerter_labelled_metrics', labelled_metric_name)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: get_anomalous_ts :: hget on metrics_manager.smtp_alerter_labelled_metrics for %s failed - %s' % (
                    base_name, err))
            if smtp_alerter_enabled:
                smtp_alerter_metrics.append(base_name)

        if base_name not in smtp_alerter_metrics:
            logger.error('%s has no alerter setting, not correlating' % base_name)
            # @modified 20230316 - Task #4872: Optimise luminosity for labelled_metrics
            # Added resolution to the return
            return [], resolution

    if not base_name or not anomaly_timestamp:
        # @modified 20230316 - Task #4872: Optimise luminosity for labelled_metrics
        # Added resolution to the return
        return [], resolution

    # from skyline_functions import nonNegativeDerivative
    anomalous_metric = '%s%s' % (settings.FULL_NAMESPACE, base_name)

    # @added 20220809 - Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    if base_name.startswith('labelled_metrics.') or labelled_metric_name:
        anomalous_metric = '%s' % str(use_base_name)

    unique_metrics = []
    if not anomalous_metric.startswith('labelled_metrics.'):
        try:
            # @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            # unique_metrics = list(redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
            unique_metrics = list(redis_conn_decoded.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: get_anomalous_ts :: no unique_metrics')
            # @modified 20230316 - Task #4872: Optimise luminosity for labelled_metrics
            # Added resolution to the return
            # @modified 20230316 - Task #4872: Optimise luminosity for labelled_metrics
            # Added resolution to the return
            return [], resolution

    # @added 20220809 - Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    if use_base_name.startswith('labelled_metrics.'):
        unique_labelled_metrics = []
        try:
            unique_labelled_metrics = list(redis_conn_decoded.smembers('labelled_metrics.unique_labelled_metrics'))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: get_anomalous_ts :: no unique_labelled_metrics')
            return []
        if unique_labelled_metrics:
            unique_metrics = unique_metrics + unique_labelled_metrics

    # @added 20180720 - Feature #2464: luminosity_remote_data
    # Ensure that Luminosity only processes it's own Redis metrics so that if
    # multiple Skyline instances are running, Luminosity does not process an
    # anomaly_id for a metric that is not local to itself.  This will stop the
    # call to the remote Redis with other_redis_conn below.  With the
    # introduction of the preprocessing luminosity_remote_data API endpoint for
    # remote Skyline instances, there is no further requirement for Skyline
    # instances to have direct access to Redis on another Skyline instance.
    # A much better solution and means all data is preprocessed and encrypted,
    # there is no need for iptables other than 443 (or custom https port).
    #
    if anomalous_metric in unique_metrics:
        logger.info('%s is a metric in Redis, processing on this Skyline instance' % base_name)
    else:
        logger.info('%s is not a metric in Redis, not processing on this Skyline instance' % base_name)
        # @modified 20230316 - Task #4872: Optimise luminosity for labelled_metrics
        # Added resolution to the return
        return [], resolution

    assigned_metrics = [anomalous_metric]
    # @modified 20180419 -
    raw_assigned = []

    # @added 20220809 - Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    if not base_name.startswith('labelled_metrics.') and not labelled_metric_name:
        try:
            raw_assigned = redis_conn.mget(assigned_metrics)
        except:
            raw_assigned = []
        if raw_assigned == [None]:
            logger.info('%s data not retrieved from local Redis' % (str(base_name)))
            raw_assigned = []

        # @modified 20180721 - Feature #2464: luminosity_remote_data
        # TO BE DEPRECATED settings.OTHER_SKYLINE_REDIS_INSTANCES
        # with the addition of the luminosity_remote_data API call and the above
        if not raw_assigned and settings.OTHER_SKYLINE_REDIS_INSTANCES:
            # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
            # for redis_ip, redis_port in settings.OTHER_SKYLINE_REDIS_INSTANCES:
            for redis_ip, redis_port, redis_password in settings.OTHER_SKYLINE_REDIS_INSTANCES:
                if not raw_assigned:
                    try:
                        if redis_password:
                            other_redis_conn = StrictRedis(host=str(redis_ip), port=int(redis_port), password=str(redis_password))
                        else:
                            other_redis_conn = StrictRedis(host=str(redis_ip), port=int(redis_port))
                        raw_assigned = other_redis_conn.mget(assigned_metrics)
                        if raw_assigned == [None]:
                            logger.info('%s data not retrieved from Redis at %s on port %s' % (str(base_name), str(redis_ip), str(redis_port)))
                            raw_assigned = []
                        if raw_assigned:
                            logger.info('%s data retrieved from Redis at %s on port %s' % (str(base_name), str(redis_ip), str(redis_port)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to connect to Redis at %s on port %s' % (str(redis_ip), str(redis_port)))
                        raw_assigned = []

        if not raw_assigned or raw_assigned == [None]:
            logger.info('%s data not retrieved' % (str(base_name)))
            # @modified 20230316 - Task #4872: Optimise luminosity for labelled_metrics
            # Added resolution to the return
            return [], resolution

        for i, metric_name in enumerate(assigned_metrics):
            try:
                raw_series = raw_assigned[i]
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)
            except:
                timeseries = []

    # @added 20220809 - Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    if base_name.startswith('labelled_metrics.') or labelled_metric_name:
        try:
            timeseries = redis_conn_decoded.ts().range(use_base_name, ((anomaly_timestamp - settings.FULL_DURATION) * 1000), (anomaly_timestamp * 1000))
        except Exception as err:
            logger.error('error :: get_anomalous_ts :: failed to get Redis timeseries for %s - %s' % (
                str(base_name), err))
            timeseries = []
        if timeseries:
            # Convert Redis millisecond timestamps to second timestamps
            timeseries = [[int(mts / 1000), value] for mts, value in timeseries]

    # @added 20200507 - Feature #3532: Sort all time series
    # To ensure that there are no unordered timestamps in the time
    # series which are artefacts of the collector or carbon-relay, sort
    # all time series by timestamp before analysis.
    original_timeseries = timeseries
    if original_timeseries:
        timeseries = sort_timeseries(original_timeseries)
        del original_timeseries

    # Convert the time series if this is a known_derivative_metric
    known_derivative_metric = is_derivative_metric(skyline_app, use_base_name)
    if known_derivative_metric:
        derivative_timeseries = nonNegativeDerivative(timeseries)
        timeseries = derivative_timeseries

    # @added 20201203 - Feature #3860: luminosity - handle low frequency data
    # Determine data resolution
    # @modified 20210820 - Feature #3860: luminosity - handle low frequency data
    #                      Task #4030: refactoring
    #                      Feature #4164: luminosity - cloudbursts
    # Use common determine_data_frequency function
    # resolution = determine_resolution(timeseries)
    resolution = determine_data_frequency(skyline_app, timeseries, False)

    # Sample the time series
    # @modified 20180720 - Feature #2464: luminosity_remote_data
    # Added note here - if you modify the value of 600 here, it must be
    # modified in the luminosity_remote_data function in
    # skyline/webapp/backend.py as well
    # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
    # from_timestamp = anomaly_timestamp - 600
    from_timestamp = anomaly_timestamp - (resolution * 10)

    anomaly_ts = []
    for ts, value in timeseries:
        if int(ts) < from_timestamp:
            continue
        if int(ts) <= anomaly_timestamp:
            anomaly_ts.append((int(ts), value))
        if int(ts) > anomaly_timestamp:
            break

    # @added 20190515 - Bug #3008: luminosity - do not analyse short time series
    # Only return a time series sample if the sample has sufficient data points
    # otherwise get_anomalies() will throw and error
    len_anomaly_ts = len(anomaly_ts)
    if len_anomaly_ts <= 9:
        logger.info('%s insufficient data not retrieved, only %s data points surfaced, not correlating' % (
            str(base_name), str(len_anomaly_ts)))
        # @modified 20230316 - Task #4872: Optimise luminosity for labelled_metrics
        # Added resolution to the return
        return [], resolution

    # @modified 20230316 - Task #4872: Optimise luminosity for labelled_metrics
    # Added resolution to the return
    return anomaly_ts, resolution 


def get_anoms(anomalous_ts):

    # logger = logging.getLogger(skyline_app_logger)

    if not anomalous_ts:
        logger.error('error :: get_anoms :: no anomalous_ts')
        return []

    anomalies = []
    try:
        anomaly_ts_dict = dict(anomalous_ts)
        my_detector = AnomalyDetector(anomaly_ts_dict, score_threshold=1.5)
        anomalies = my_detector.get_anomalies()
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: get_anoms :: AnomalyDetector')
    return anomalies


# @modified 20200506 - Feature #3510: Enable Luminosity to handle correlating namespaces only
# def get_assigned_metrics(i,):
# @modified 20230316 - Task #4872: Optimise luminosity for labelled_metrics
# Added anomaly_timestamp and resolution to filter out only metrics that are in the
# illuminance.all keys
def get_assigned_metrics(i, base_name, anomaly_timestamp, resolution):
    try:
        # @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # unique_metrics = list(redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
        unique_metrics = list(redis_conn_decoded.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
        # @added 20200924 - Feature #3756: LUMINOSITY_CORRELATION_MAPS
        original_unique_metrics = list(unique_metrics)
        logger.info('get_assigned_metrics :: created original_unique_metrics with %s metrics' % (
            str(len(original_unique_metrics))))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: get_assigned_metrics :: no unique_metrics')
        return []

    # @added 20220809 - Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    use_base_name = str(base_name)
    unique_labelled_metrics = []
    try:
        unique_labelled_metrics = list(redis_conn_decoded.smembers('labelled_metrics.unique_labelled_metrics'))
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: get_assigned_metrics :: smembers labelled_metrics.unique_labelled_metrics failed - %s' % err)
    active_labelled_ids_with_metric = {}
    if unique_labelled_metrics:
        unique_metrics = unique_metrics + unique_labelled_metrics
        try:
            active_labelled_ids_with_metric = redis_conn_decoded.hgetall('aet.metrics_manager.active_labelled_ids_with_metric')
        except Exception as err:
            logger.error('error :: get_assigned_metrics :: hgetall aet.metrics_manager.active_labelled_ids_with_metric failed - %s' % err)
    if base_name.startswith('labelled_metrics.'):
        try:
            use_base_name = get_base_name_from_labelled_metrics_name(skyline_app, base_name)
        except Exception as err:
            logger.error('error :: get_assigned_metrics :: get_base_name_from_labelled_metrics_name failed for %s - %s' % (
                base_name, err))

    # @added 20200506 - Feature #3510: Enable Luminosity to handle correlating namespaces only
    correlate_namespace_to = None

    # @added 20220225 - Feature #4000: EXTERNAL_SETTINGS
    # Use correlation related settings from external settings
    correlate_namespaces_only_override = None
    correlation_maps = {}
    parent_namespace = base_name.split('.')[0]

    # @added 20220810 - Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    if '_tenant_id="' in use_base_name:
        metric_dict = {}
        try:
            metric_dict = get_labelled_metric_dict(skyline_app, use_base_name)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: get_assigned_metrics :: get_labelled_metric_dict failed - %s' % err)
        if metric_dict:
            try:
                parent_namespace = metric_dict['labels']['_tenant_id']
            except Exception as err:
                logger.error('error :: get_assigned_metrics :: failed to determine parent_namespace from metric_dict - %s' % err)

    external_settings = {}
    try:
        external_settings = get_external_settings(skyline_app, parent_namespace, False)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error ::get_assigned_metrics :: get_external_settings failed - %s' % (
            err))
    if external_settings:
        config_id = list(external_settings.keys())[0]
        try:
            external_settings_correlate_namespaces_only = external_settings[config_id]['correlate_namespaces_only']
        except KeyError:
            external_settings_correlate_namespaces_only = None
        if isinstance(external_settings_correlate_namespaces_only, list):
            # Override the Skyline LUMINOSITY_CORRELATE_NAMESPACES_ONLY list
            # with the external_settings list
            correlate_namespaces_only_override = list(external_settings_correlate_namespaces_only)
        external_settings_correlation_maps = None
        try:
            external_settings_correlation_maps = external_settings[config_id]['correlation_maps']
        except KeyError:
            external_settings_correlation_maps = None
        if isinstance(external_settings_correlation_maps, dict):
            # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
            # correlation_maps = external_settings_correlation_maps.copy()
            correlation_maps = copy.deepcopy(external_settings_correlation_maps)

    # @added 20230315 - Task #4872: Optimise luminosity for labelled_metrics
    if correlation_maps:
        logger.debug('debug :: get_assigned_metrics :: external_settings correlation_maps: %s' % (
            str(correlation_maps)))

    number_of_labelled_metrics_iterated = 0

    # @modified 20220225 - Feature #4000: EXTERNAL_SETTINGS
    # Use correlation related settings from external settings if they exist
    # if correlate_namespaces_only:
    #     for correlate_namespace in correlate_namespaces_only:
    if LUMINOSITY_CORRELATE_NAMESPACES_ONLY or correlate_namespaces_only_override:
        if correlate_namespaces_only_override:
            correlate_namespaces_only_list = list(correlate_namespaces_only_override)
        else:
            correlate_namespaces_only_list = list(LUMINOSITY_CORRELATE_NAMESPACES_ONLY)

        # @added 20230315 - Task #4872: Optimise luminosity for labelled_metrics
        logger.debug('debug :: get_assigned_metrics :: correlate_namespaces_only_override: %s' % (
            str(correlate_namespaces_only_override)))

        # @added 20220225 - Feature #4000: EXTERNAL_SETTINGS
        # Allow for multiple namespaces to be declared and match the LONGEST
        # namespace first
        try:
            correlate_namespaces_only_list_len = []
            for item in correlate_namespaces_only_list:
                correlate_namespaces_only_list_len.append([item, len(item.split('.'))])
            sorted_correlate_namespaces_only_list_len = sorted(correlate_namespaces_only_list_len, key=lambda x: x[1], reverse=True)
            correlate_namespaces_only_list = [x[0] for x in sorted_correlate_namespaces_only_list_len]
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: get_assigned_metrics :: failed to sort correlate_namespaces_only_list by length - %s' % err)

        for correlate_namespace in correlate_namespaces_only_list:
            try:
                correlate_namespace_matched_by = None
                # correlate_namespace_to, correlate_namespace_matched_by = matched_or_regexed_in_list(skyline_app, base_name, [correlate_namespace])
                correlate_namespace_to, correlate_namespace_matched_by = matched_or_regexed_in_list(skyline_app, use_base_name, [correlate_namespace])
                if correlate_namespace_to:
                    break
            except:
                pass

    # @modified 20220225 - Feature #4000: EXTERNAL_SETTINGS
    # Use correlation related settings from external settings if they exist
    # if correlate_namespaces_only:
    errors = []
    if LUMINOSITY_CORRELATE_NAMESPACES_ONLY or correlate_namespaces_only_override:
        if not correlate_namespace_to:
            correlate_with_metrics = []
            for metric_name in unique_metrics:
                # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
                # metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                if metric_name.startswith(settings.FULL_NAMESPACE):
                    metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                else:
                    metric_base_name = metric_name

                # @added 20220810 - Task #2732: Prometheus to Skyline
                #                   Branch #4300: prometheus
                if metric_name.startswith('labelled_metrics.'):
                    try:
                        metric_id_str = metric_name.replace('labelled_metrics.', '', 1)
                        metric_base_name = active_labelled_ids_with_metric[metric_id_str]
                    except Exception as err:
                        errors.append([metric_name, 'not found in active_labelled_ids_with_metric', err])

                # @modified 20220225 - Feature #4000: EXTERNAL_SETTINGS
                # add_metric, add_metric_matched_by = matched_or_regexed_in_list(skyline_app, metric_base_name, correlate_namespaces_only)
                add_metric, add_metric_matched_by = matched_or_regexed_in_list(skyline_app, metric_base_name, correlate_namespaces_only_list)

                if not add_metric:
                    correlate_with_metrics.append(metric_name)
            if correlate_with_metrics:
                logger.info('get_assigned_metrics :: replaced %s unique_metrics with %s correlate_with_metrics, excluding metrics from LUMINOSITY_CORRELATE_NAMESPACES_ONLY' % (
                    str(len(unique_metrics)), str(len(correlate_with_metrics))))
                unique_metrics = correlate_with_metrics
    if correlate_namespace_to:
        correlate_with_metrics = []
        correlate_with_namespace = correlate_namespace_matched_by['matched_namespace']
        if correlate_with_namespace:
            for metric_name in unique_metrics:
                # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
                # metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                if metric_name.startswith(settings.FULL_NAMESPACE):
                    metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                else:
                    metric_base_name = metric_name

                # @added 20220810 - Task #2732: Prometheus to Skyline
                #                   Branch #4300: prometheus
                if metric_name.startswith('labelled_metrics.'):
                    try:
                        metric_id_str = metric_name.replace('labelled_metrics.', '', 1)
                        metric_base_name = active_labelled_ids_with_metric[metric_id_str]
                    except Exception as err:
                        errors.append([metric_name, 'not found in active_labelled_ids_with_metric', err])

                add_metric, add_metric_matched_by = matched_or_regexed_in_list(skyline_app, metric_base_name, [correlate_with_namespace])
                if add_metric:
                    correlate_with_metrics.append(metric_name)
        if correlate_with_metrics:
            logger.info('get_assigned_metrics :: replaced %s unique_metrics with %s correlate_with_metrics' % (
                str(len(unique_metrics)), str(len(correlate_with_metrics))))
            unique_metrics = correlate_with_metrics

    # @added 20200924 - Feature #3756: LUMINOSITY_CORRELATION_MAPS
    # WIP not tested or fully POCed
    # @modified 20220225 - Feature #4000: EXTERNAL_SETTINGS
    # Use correlation related settings from external settings if they exist
    # if LUMINOSITY_CORRELATION_MAPS:
    if LUMINOSITY_CORRELATION_MAPS or correlation_maps:

        # @added 20230315 - Task #4872: Optimise luminosity for labelled_metrics
        logger.debug('debug :: get_assigned_metrics :: processing LUMINOSITY_CORRELATION_MAPS or correlation_maps')

        # @added 20220225 - Feature #4000: EXTERNAL_SETTINGS
        # Replace Skyline LUMINOSITY_CORRELATION_MAPS with external settings
        # correlation_maps if they exist
        if correlation_maps and LUMINOSITY_CORRELATION_MAPS:
            for c_map in list(LUMINOSITY_CORRELATION_MAPS.keys()):
                try:
                    del LUMINOSITY_CORRELATION_MAPS[c_map]
                except:
                    pass
        for c_map in list(correlation_maps.keys()):
            # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
            # LUMINOSITY_CORRELATION_MAPS[c_map] = correlation_maps[c_map].copy()
            LUMINOSITY_CORRELATION_MAPS[c_map] = copy.deepcopy(correlation_maps[c_map])

        also_correlate = []
        if metric_name.startswith(settings.FULL_NAMESPACE):
            metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
        else:
            metric_base_name = metric_name

        # @added 20220810 - Task #2732: Prometheus to Skyline
        #                   Branch #4300: prometheus
        if metric_name.startswith('labelled_metrics.'):
            try:
                metric_id_str = metric_name.replace('labelled_metrics.', '', 1)
                metric_base_name = active_labelled_ids_with_metric[metric_id_str]
            except Exception as err:
                errors.append([metric_name, 'not found in active_labelled_ids_with_metric', err])

        for correlation_map in LUMINOSITY_CORRELATION_MAPS:
            metric_in_map = False
            for i_map in LUMINOSITY_CORRELATION_MAPS[correlation_map]:
                if i_map == metric_base_name:
                    metric_in_map = True
                    break
            if metric_in_map:
                for i_map in LUMINOSITY_CORRELATION_MAPS[correlation_map]:
                    if i_map != metric_base_name:
                        also_correlate.append(i_map)
        if also_correlate:
            for i_metric_base_name in also_correlate:
                if '_tenant_id="' not in i_metric_base_name:
                    i_metric_name = '%s%s' % (settings.FULL_NAMESPACE, i_metric_base_name)

                if i_metric_base_name.startswith('labelled_metrics.'):
                    i_metric_name = '%s' % str(i_metric_base_name)

                if i_metric_name not in unique_metrics:
                    unique_metrics.append(i_metric_name)
            logger.info('get_assigned_metrics :: appended %s metrics to unique_metrics to be correlated from correlation_maps' % (
                str(len(also_correlate))))

    # @added 20230316 - Task #4872: Optimise luminosity for labelled_metrics
    # Filter out only unique_metrics that are present in the
    # analyzer.illuminance.all.YYYY-MM-DD set/s this takes the number of
    # metrics to be correlated down dramatically.  The logic here is that
    # if a metric has not triggered even a single algorithm in analyzer or
    # analyzer_labelled_metrics the chances of it correlating are very
    # low given that the metric has not experienced any, even a slight,
    # significant change, therefore these metrics will not correlate so
    # they are removed.  The differences to timing and performance between
    # surfacing 5846 metrics and 16 or 948 or 154 is significant, especially
    # when a cluster of anomalies occurs in a large namespace.
    aligned_anomaly_timestamp = int(int(anomaly_timestamp) // resolution * resolution)
    first_aligned_timestamp = aligned_anomaly_timestamp - (resolution * 3)
    illuminance_all_timestamp_keys = [first_aligned_timestamp]
    last_aligned_timestamp = aligned_anomaly_timestamp + (resolution * 2)
    while illuminance_all_timestamp_keys[-1] != last_aligned_timestamp:
        last_aligned_ts = illuminance_all_timestamp_keys[-1]
        illuminance_all_timestamp_keys.append(last_aligned_ts + resolution)
    date_string = str(strftime('%Y-%m-%d', gmtime(anomaly_timestamp)))
    illuminance_key = 'analyzer.illuminance.all.%s' % date_string
    illuminance_all_keys = [illuminance_key]
    # If the day has just rolled over query yesterday illuminance.all key as well
    hour_min_string = str(strftime('%H:%M', gmtime(anomaly_timestamp)))
    if '00:0' in hour_min_string:
        date_string = str(strftime('%Y-%m-%d', gmtime(anomaly_timestamp - 86400)))
        illuminance_key = 'analyzer.illuminance.all.%s' % date_string
        illuminance_all_keys.append(illuminance_key)
    logger.info('get_assigned_metrics :: determining recently active metrics from %s for timestamps %s' % (
        str(illuminance_all_keys), str(illuminance_all_timestamp_keys)))
    recently_active_metric_ids = []
    for illuminance_all_key in illuminance_all_keys:
        for ts in illuminance_all_timestamp_keys:
            illuminance_dict_str = None
            try:
                illuminance_dict_str = redis_conn_decoded.hget(illuminance_all_key, str(ts))
            except Exception as err:
                errors.append([illuminance_all_key, ts, 'could not query not illuminance_all_key for ts', err])
                illuminance_dict_str = None
            illuminance_dict = {}
            if illuminance_dict_str:
                try:
                    illuminance_dict = literal_eval(illuminance_dict_str)
                except Exception as err:
                    errors.append([illuminance_all_key, ts, 'could not literal_eval data from ts in illuminance_all_key', err])
                    illuminance_dict = {}
            recent_metric_ids = []
            if illuminance_dict:
                recent_metric_ids = [int(id_str) for id_str in list(illuminance_dict.keys())]
            recently_active_metric_ids = recently_active_metric_ids + recent_metric_ids
    recently_active_metric_ids = list(set(recently_active_metric_ids))
    ids_with_metric_names = {}
    if recently_active_metric_ids:
        ids_with_metric_names = redis_conn_decoded.hgetall('aet.metrics_manager.ids_with_metric_names')
    recently_active_metrics = []
    logger.info('get_assigned_metrics :: filtering on %s recently active metrics from illuminance.all from the %s metrics determined to correlate' % (
        str(len(recently_active_metric_ids)), str(len(unique_metrics))))
    for recently_active_metric_id in recently_active_metric_ids:
        try:
            id_str = str(recently_active_metric_id)
            metric_base_name = ids_with_metric_names[id_str]
            if metric_base_name == base_name:
                continue
            if '_tenant_id="' in metric_base_name:
                use_metric_name = 'labelled_metrics.%s' % id_str
            else:
                use_metric_name = '%s%s' % (settings.FULL_NAMESPACE, metric_base_name)
            recently_active_metrics.append(use_metric_name)
        except KeyError:
            continue
        except Exception as err:
            errors.append([recently_active_metric_id, 'could not find base_name for recently_active_metric_id in ids_with_metric_names', err])
    if recently_active_metrics:
        logger.info('get_assigned_metrics :: filtering out only recently active metrics')
        unique_metrics = list(set(unique_metrics) & set(recently_active_metrics))
    else:
        logger.info('get_assigned_metrics :: there are no recently active metrics')
        unique_metrics = []
    logger.info('get_assigned_metrics :: %s metrics to correlate after filtering only recently active metrics' % str(len(unique_metrics)))

    # Discover assigned metrics
    # @modified 20180720 - Feature #2464: luminosity_remote_data
    # Use just 1 processor
    # keys_per_processor = int(ceil(float(len(unique_metrics)) / float(settings.LUMINOSITY_PROCESSES)))
    # if i == settings.LUMINOSITY_PROCESSES:
    #     assigned_max = len(unique_metrics)
    # else:
    #     assigned_max = min(len(unique_metrics), i * keys_per_processor)
    # assigned_min = (i - 1) * keys_per_processor
    assigned_min = 0
    assigned_max = len(unique_metrics)
    assigned_keys = range(assigned_min, assigned_max)
    # Compile assigned metrics
    assigned_metrics = [unique_metrics[index] for index in assigned_keys]
    logger.info('get_assigned_metrics :: filtered %s metrics from the found metrics in local Redis' % str(len(unique_metrics)))
    return assigned_metrics


# @added 20180720 - Feature #2464: luminosity_remote_data
# @modified 20201203 - Feature #3860: luminosity - handle low frequency data
# Added the metric resolution
# def get_remote_assigned(anomaly_timestamp):
# @modified 20230409 - Task #4872: Optimise luminosity for labelled_metrics
# Added namespace
# def get_remote_assigned(anomaly_timestamp, resolution):
def get_remote_assigned(anomaly_timestamp, resolution, namespace=None):
    remote_assigned = []
    # @modified 20201215 - Feature #3890: metrics_manager - sync_cluster_files
    # for remote_url, remote_user, remote_password in settings.REMOTE_SKYLINE_INSTANCES:
    for remote_url, remote_user, remote_password, hostname in settings.REMOTE_SKYLINE_INSTANCES:
        # @modified 20180722 - Feature #2464: luminosity_remote_data
        # Use a gzipped response - deprecated the raw unprocessed time series
        # method
        # response_ok = False
        # url = '%s/api?luminosity_remote_data=true&anomaly_timestamp=%s' % (remote_url, str(anomaly_timestamp))
        # try:
        #     r = requests.get(url, timeout=15, auth=(remote_user, remote_password))
        #     if int(r.status_code) == 200:
        #         logger.info('get_remote_assigned :: time series data retrieved from %s' % remote_url)
        #         response_ok = True
        #     else:
        #         logger.error('get_remote_assigned :: time series data not retrieved from %s, status code %s returned' % (remote_url, str(r.status_code)))
        # except:
        #     logger.error(traceback.format_exc())
        #     logger.error('error :: get_remote_assigned :: failed to get time series data from %s' % str(url))
        # if response_ok:
        #     data = literal_eval(r.text)
        #     ts_data = data['results']
        #     # remote_assigned.append(ts_data)
        #     remote_assigned = ts_data
        #     logger.info('get_remote_assigned :: %s metrics retrieved from %s' % (str(len(ts_data)), remote_url))

        # @added 20180721 - Feature #2464: luminosity_remote_data
        # Use a gzipped response as the response as raw unprocessed time series
        # can be mulitple megabytes
        # @modified 20201117 - Feature #3824: get_cluster_data
        # Correct url with api for get_cluster_data
        # url = '%s/luminosity_remote_data?anomaly_timestamp=%s' % (remote_url, str(anomaly_timestamp))
        # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
        # Added the metric resolution
        # url = '%s/api?luminosity_remote_data&anomaly_timestamp=%s' % (remote_url, str(anomaly_timestamp))
        url = '%s/api?luminosity_remote_data&anomaly_timestamp=%s&resolution=%s' % (remote_url, str(anomaly_timestamp), str(resolution))

        # @added 20230409 - Task #4872: Optimise luminosity for labelled_metrics
        # Added namespace
        if namespace:
            url = '%s&namespace=%s' % (url, str(namespace))

        response_ok = False

        # @added 20190519 - Branch #3002: docker
        # Handle self signed certificate on Docker
        verify_ssl = True
        try:
            running_on_docker = settings.DOCKER
        except:
            running_on_docker = False
        if running_on_docker:
            verify_ssl = False

        try:
            # @modified 20190519 - Branch #3002: docker
            # r = requests.get(url, timeout=15, auth=(remote_user, remote_password))
            # @modified 20201222 - Feature #3824: get_cluster_data
            # Allow for longer response
            # r = requests.get(url, timeout=15, auth=(remote_user, remote_password), verify=verify_ssl)
            # @modified 20201222 - Feature #3824: get_cluster_data
            # Allow for longer cluster responses with large metric populations
            # r = requests.get(url, timeout=15, auth=(remote_user, remote_password), verify=verify_ssl)
            r = requests.get(url, timeout=65, auth=(remote_user, remote_password), verify=verify_ssl)
            if int(r.status_code) == 200:
                logger.info('get_remote_assigned :: time series data retrieved from %s' % remote_url)
                response_ok = True
            else:
                logger.error('get_remote_assigned :: time series data not retrieved from %s, status code %s returned' % (remote_url, str(r.status_code)))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: get_remote_assigned :: failed to get time series data from %s' % str(url))
        if response_ok:
            # import gzip
            # import StringIO
            data = None
            try:
                # @modified 20180722 - Feature #2464: luminosity_remote_data
                # First method is failing.  Trying requests automatic gzip
                # decoding below.
                # Binary Response Content - You can also access the response body as bytes, for non-text requests:
                # >>> r.content
                # b'[{"repository":{"open_issues":0,"url":"https://github.com/...
                # The gzip and deflate transfer-encodings are automatically decoded for you.
                # fakefile = StringIO.StringIO(r.content)  # fakefile is now a file-like object thta can be passed to gzip.GzipFile:
                # try:
                #     uncompressed = gzip.GzipFile(fileobj=fakefile, mode='r')
                #    decompressed_data = uncompressed.read()
                # @modified 20201204 - Feature #2464: luminosity_remote_data
                #                      Feature #3820: HORIZON_SHARDS
                # decompressed_data = (r.content)
                # data = literal_eval(decompressed_data)
                data = literal_eval(r.text)
                logger.info('get_remote_assigned :: response data decompressed with native requests gzip decoding')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: get_remote_assigned :: failed to decompress gzipped response with native requests gzip decoding')

            if data:
                ts_data = data['results']
                logger.info('get_remote_assigned :: %s metrics retrieved compressed from %s' % (str(len(ts_data)), remote_url))
                if remote_assigned:
                    # @modified 20201204 - Feature #2464: luminosity_remote_data
                    #                      Feature #3820: HORIZON_SHARDS
                    # remote_assigned.append(ts_data)
                    for item in ts_data:
                        remote_assigned.append(item)
                else:
                    remote_assigned = ts_data

    logger.info('get_remote_assigned :: %s metrics retrieved from remote Skyline instances' % (str(len(remote_assigned))))
    return remote_assigned


# @modified 20180720 - Feature #2464: luminosity_remote_data
# def get_correlations(base_name, anomaly_timestamp, anomalous_ts, assigned_metrics, raw_assigned, anomalies):
# @modified 20230315 - Task #4872: Optimise luminosity for labelled_metrics
def get_correlations(
    base_name, anomaly_timestamp, anomalous_ts, assigned_metrics, raw_assigned,
# @modified 20230315 - Task #4872: Optimise luminosity for labelled_metrics
# Added processing_time
        remote_assigned, anomalies, processing_time,
# @modified 20230316 - Task #4872: Optimise luminosity for labelled_metrics
# Added resolution as already determined and no need to determine again
        resolution,):

    # logger = logging.getLogger(skyline_app_logger)

    # Distill timeseries strings into lists
    start = timer()
    count = 0
    metrics_checked_for_correlation = 0

    # @added 20201203 - Feature #3860: luminosity - handle low frequency data
    # Determine data resolution
    # @modified 20210820 - Feature #3860: luminosity - handle low frequency data
    #                      Task #4030: refactoring
    #                      Feature #4164: luminosity - cloudbursts
    # Use common determine_data_frequency function
    # resolution = determine_resolution(anomalous_ts)
    # @modified 20230316 - Task #4872: Optimise luminosity for labelled_metrics
    # Added resolution as an argument as already determined and no need to determine again
    # resolution = determine_data_frequency(skyline_app, anomalous_ts, False)

    # Sample the time series
    # @modified 20180720 - Feature #2464: luminosity_remote_data
    # Added note here - if you modify the value of 600 here, it must be
    # modified in the luminosity_remote_data function in
    # skyline/webapp/backend.py as well
    # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
    # from_timestamp = anomaly_timestamp - 600
    from_timestamp = anomaly_timestamp - (resolution * 10)

    correlated_metrics = []
    correlations = []
    no_data = False
    if not anomalous_ts:
        no_data = True
        logger.info('get_correlations :: no anomalous_ts')
    # @modified 20230409 - Task #4872: Optimise luminosity for labelled_metrics
    # if not assigned_metrics:
    if not assigned_metrics and not remote_assigned:
        no_data = True
        logger.info('get_correlations :: no assigned_metrics')
    # @modified 20230409 - Task #4872: Optimise luminosity for labelled_metrics
    # if not raw_assigned:
    if not raw_assigned and not remote_assigned:
        if '_tenant_id="' not in base_name:
            no_data = True
            logger.info('get_correlations :: no raw_assigned')
    if not anomalies:
        no_data = True
        logger.info('get_correlations :: no anomalies')
    if no_data:
        logger.error('error :: get_correlations :: no data')
        # return (correlated_metrics, correlations)
        return (correlated_metrics, correlations, [], 0)

    # @added 20200428 - Feature #3510: Enable Luminosity to handle correlating namespaces only
    #                   Feature #3500: webapp - crucible_process_metrics
    #                   Feature #1448: Crucible web UI
    # Discard the check if the anomaly_timestamp is not in FULL_DURATION as it
    # will have been added via the Crucible or webapp/crucible route
    start_timestamp_of_full_duration_data = int(time() - settings.FULL_DURATION)
    if anomaly_timestamp < (start_timestamp_of_full_duration_data + 2000):
        logger.info('get_correlations :: the anomaly_timestamp is too old not correlating')
        # @added 20230321 - Feature #4874: luminosity - related_metrics - labelled_metrics
        # If timestamps are old do not sleep
        try:
            redis_conn_decoded.setex('luminosity.process_correlations.old_timestamp', 10, int(time()))
        except Exception as err:
            logger.error('error :: failed to setex Redis key luminosity.process_correlations.old_timestamp - %s' % err)

        # return (correlated_metrics, correlations)
        return (correlated_metrics, correlations, [], 0)

    start_local_correlations = timer()

    local_redis_metrics_checked_count = 0
    local_redis_metrics_correlations_count = 0

    logger.info('get_correlations :: the local Redis metric count is %s' % str(len(assigned_metrics)))

    # @added 20200428 - Feature #3510: Enable Luminosity to handle correlating namespaces only
    # Removed here and handled in get_assigned_metrics

    # @added 20230315 - Task #4872: Optimise luminosity for labelled_metrics
    labelled_metrics_fetched = 0
    sorts_took = 0.0
    timestamps_iterated = 0
    timestamp_iterations_took = 0

    for i, metric_name in enumerate(assigned_metrics):
        count += 1
        # print(metric_name)
        # @modified 20180719 - Branch #2270: luminosity
        # Removed test limiting that was errorneously left in
        # if count > 1000:
        #     break

        # @added 20230315 - Task #4872: Optimise luminosity for labelled_metrics
        if (count % 1000) == 0:
            logger.debug('debug :: get_correlations :: checked %s metrics so far and %s labelled_metrics fetched' % (
                str(count), str(labelled_metrics_fetched)))
        allow_to_run_for = 60
        if settings.REMOTE_SKYLINE_INSTANCES:
            allow_to_run_for = 90
        now = int(time())
        if now >= (processing_time + (allow_to_run_for - 5)):
            logger.info('get_correlations :: stopping as approach max runtime - checked %s metrics of %s' % (
                str((count - 1)), str(len(assigned_metrics))))
            break

        correlated = None
        # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
        # metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
        if metric_name.startswith(settings.FULL_NAMESPACE):
            metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
        else:
            metric_base_name = metric_name

        if str(metric_base_name) == str(base_name):
            continue

        # @modified 20220810 - Task #2732: Prometheus to Skyline
        #                      Branch #4300: prometheus
        # Handle labelled_metrics
        timeseries = []
        if not metric_base_name.startswith('labelled_metrics.'):
            try:
                raw_series = raw_assigned[i]
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)
            except:
                timeseries = []
        else:
            try:
                timeseries = redis_conn_decoded.ts().range(metric_base_name, (from_timestamp * 1000), ((anomaly_timestamp + (resolution + 1)) * 1000))
                if timeseries:
                    # Convert Redis millisecond timestamps to second timestamps
                    timeseries = [[int(mts / 1000), value] for mts, value in timeseries]
            except:
                timeseries = []
            # @added 20230315 - Task #4872: Optimise luminosity for labelled_metrics
            labelled_metrics_fetched += 1

        if not timeseries:
            # print('no time series data for %s' % base_name)
            continue

        # @added 20200507 - Feature #3532: Sort all time series
        # To ensure that there are no unordered timestamps in the time
        # series which are artefacts of the collector or carbon-relay, sort
        # all time series by timestamp before analysis.
        original_timeseries = timeseries
        if original_timeseries:
            sort_start = time()
            timeseries = sort_timeseries(original_timeseries)
            del original_timeseries
            sorts_took += (time() - sort_start)

        # Convert the time series if this is a known_derivative_metric
        known_derivative_metric = is_derivative_metric(skyline_app, metric_base_name)
        if known_derivative_metric:
            try:
                derivative_timeseries = nonNegativeDerivative(timeseries)
                timeseries = derivative_timeseries
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: nonNegativeDerivative')

        correlate_ts = []

        # @added 20201203 - Feature #3860: luminosity - handle low frequency data
        # Determine data resolution
        # @modified 20210820 - Feature #3860: luminosity - handle low frequency data
        #                      Task #4030: refactoring
        #                      Feature #4164: luminosity - cloudbursts
        # Use common determine_data_frequency function
        # resolution = determine_resolution(timeseries)
        resolution = determine_data_frequency(skyline_app, timeseries, False)

        timestamp_iterations_start = time()
        # @modified 20230316 - Task #4872: Optimise luminosity for labelled_metrics
        # Only grab the last 60 datapoints from the timeseries rather than iterating
        # the whole thing
        # for ts, value in timeseries:
        for ts, value in timeseries[-60:]:
            timestamps_iterated += 1
            if int(ts) < from_timestamp:
                continue
            if int(ts) <= anomaly_timestamp:
                correlate_ts.append((int(ts), value))
            # @modified 20180720 - Feature #2464: luminosity_remote_data
            # Added note here - if you modify the value of 61 here, it must be
            # modified in the luminosity_remote_data function in
            # skyline/webapp/backend.py as well
            # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
            # Handle varying metric resolutions
            # if int(ts) > (anomaly_timestamp + 61):
            # @modified 20211226 - Feature #3860: luminosity - handle low frequency data
            # Only if resolution is known
            if resolution:
                if int(ts) > (anomaly_timestamp + (resolution + 1)):
                    break
        timestamp_iterations_took += (time() - timestamp_iterations_start)

        if not correlate_ts:
            continue

        local_redis_metrics_checked_count += 1
        anomaly_ts_dict = dict(anomalous_ts)
        correlate_ts_dict = dict(correlate_ts)

        for a in anomalies:
            try:
                # @modified 20180720 - Feature #2464: luminosity_remote_data
                # Added note here - if you modify the value of 120 here, it must be
                # modified in the luminosity_remote_data function in
                # skyline/webapp/backend.py as well
                # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
                # Handle varying metric resolutions
                # if int(a.exact_timestamp) < int(anomaly_timestamp - 120):
                #     continue
                # if int(a.exact_timestamp) > int(anomaly_timestamp + 120):
                #     continue
                if int(a.exact_timestamp) < int(anomaly_timestamp - (resolution * 2)):
                    continue
                if int(a.exact_timestamp) > int(anomaly_timestamp + (resolution * 2)):
                    continue

            except:
                continue
            try:
                # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
                # Handle varying metric resolutions
                # time_period = (int(anomaly_timestamp - 120), int(anomaly_timestamp + 120))
                time_period = (int(anomaly_timestamp - (resolution * 2)), int(anomaly_timestamp + (resolution * 2)))

                my_correlator = Correlator(anomaly_ts_dict, correlate_ts_dict, time_period)
                # For better correlation use 0.9 instead of 0.8 for the threshold
                # @modified 20180524 - Feature #2360: CORRELATE_ALERTS_ONLY
                #                      Branch #2270: luminosity
                #                      Feature #2378: Add redis auth to Skyline and rebrow
                # Added this to setting.py
                # if my_correlator.is_correlated(threshold=0.9):
                try:
                    cross_correlation_threshold = settings.LUMINOL_CROSS_CORRELATION_THRESHOLD
                    metrics_checked_for_correlation += 1
                except:
                    cross_correlation_threshold = 0.9
                if my_correlator.is_correlated(threshold=cross_correlation_threshold):
                    correlation = my_correlator.get_correlation_result()
                    correlated = True
                    correlations.append([metric_base_name, correlation.coefficient, correlation.shift, correlation.shifted_coefficient])
                    local_redis_metrics_correlations_count += 1
            except:
                pass
        if correlated:
            correlated_metrics.append(metric_base_name)

    # @added 20230315 - Task #4872: Optimise luminosity for labelled_metrics
    logger.debug('debug :: get_correlations :: sorts_took: %s, timestamps_iterated: %s, timestamp_iterations_took: %s, labelled_metrics_fetched: %s' % (
        str(sorts_took), str(timestamps_iterated), str(timestamp_iterations_took), str(labelled_metrics_fetched)))

    # @added 20180720 - Feature #2464: luminosity_remote_data
    # Added the correlation of preprocessed remote data
    end_local_correlations = timer()
    logger.info('get_correlations :: checked - local_redis_metrics_checked_count is %s' % str(local_redis_metrics_checked_count))
    logger.info('get_correlations :: correlated - local_redis_metrics_correlations_count is %s' % str(local_redis_metrics_correlations_count))
    logger.info('get_correlations :: processed %s correlations on local_redis_metrics_checked_count %s local metrics in %.6f seconds' % (
        str(local_redis_metrics_correlations_count),
        str(local_redis_metrics_checked_count),
        (end_local_correlations - start_local_correlations)))

    # @added 20201207 - Feature #3858: skyline_functions - correlate_or_relate_with
    do_not_correlate_with = []

    # @added 20220504 - Task #4544: Handle EXTERNAL_SETTINGS in correlate_or_relate_with
    #                   Feature #3858: skyline_functions - correlate_or_relate_with
    external_settings = {}
    if settings.EXTERNAL_SETTINGS and remote_assigned:
        try:
            external_settings = get_external_settings(skyline_app, None, False)
        except Exception as err:
            logger.error('error :: get_external_settings failed - %s' % (
                err))

    remote_metrics_count = 0
    remote_correlations_check_count = 0
    remote_correlations_count = 0
    logger.info('get_correlations :: remote_assigned count %s' % str(len(remote_assigned)))

    start_remote_correlations = timer()
    for ts_data in remote_assigned:
        remote_metrics_count += 1
        correlated = None
        metric_name = str(ts_data[0])
        # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
        # metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
        if metric_name.startswith(settings.FULL_NAMESPACE):
            metric_base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
        else:
            metric_base_name = metric_name

        if str(metric_base_name) == str(base_name):
            continue

        # @added 20201207 - Feature #3858: skyline_functions - correlate_or_relate_with
        try:
            # @modified 20220504 - Task #4544: Handle EXTERNAL_SETTINGS in correlate_or_relate_with
            #                   Feature #3858: skyline_functions - correlate_or_relate_with
            # Added external_settings
            correlate_or_relate = correlate_or_relate_with(skyline_app, base_name, metric_base_name, external_settings)
            if not correlate_or_relate:
                do_not_correlate_with.append(metric_base_name)
                continue
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: get_remote_assigned :: failed to evaluate correlate_or_relate_with')

        timeseries = []
        try:
            timeseries = ts_data[1]
        except:
            timeseries = []
        if not timeseries:
            continue

        # @added 20201203 - Feature #3860: luminosity - handle low frequency data
        # Determine data resolution
        # @modified 20210820 - Feature #3860: luminosity - handle low frequency data
        #                      Task #4030: refactoring
        #                      Feature #4164: luminosity - cloudbursts
        # Use common determine_data_frequency function
        # resolution = determine_resolution(timeseries)
        resolution = determine_data_frequency(skyline_app, timeseries, False)

        correlate_ts = []
        for ts, value in timeseries:
            if int(ts) < from_timestamp:
                continue
            if int(ts) <= anomaly_timestamp:
                correlate_ts.append((int(ts), value))
            # @modified 20180720 - Feature #2464: luminosity_remote_data
            # Added note here - if you modify the value of 61 here, it must be
            # modified in the luminosity_remote_data function in
            # skyline/webapp/backend.py as well
            # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
            # Handle varying metric resolutions
            # if int(ts) > (anomaly_timestamp + 61):
            if int(ts) > (anomaly_timestamp + (resolution + 1)):
                break
        if not correlate_ts:
            continue

        anomaly_ts_dict = dict(anomalous_ts)
        correlate_ts_dict = dict(correlate_ts)

        for a in anomalies:
            try:
                # @modified 20180720 - Feature #2464: luminosity_remote_data
                # Added note here - if you modify the value of 120 here, it must be
                # modified in the luminosity_remote_data function in
                # skyline/webapp/backend.py as well
                # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
                # Handle varying metric resolutions
                # if int(a.exact_timestamp) < int(anomaly_timestamp - 120):
                #     continue
                # if int(a.exact_timestamp) > int(anomaly_timestamp + 120):
                #     continue
                if int(a.exact_timestamp) < int(anomaly_timestamp - (resolution * 2)):
                    continue
                if int(a.exact_timestamp) > int(anomaly_timestamp + (resolution * 2)):
                    continue
            except:
                continue
            try:
                # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
                # Handle varying metric resolutions
                # time_period = (int(anomaly_timestamp - 120), int(anomaly_timestamp + 120))
                time_period = (int(anomaly_timestamp - (resolution * 2)), int(anomaly_timestamp + (resolution * 2)))

                my_correlator = Correlator(anomaly_ts_dict, correlate_ts_dict, time_period)
                metrics_checked_for_correlation += 1
                remote_correlations_check_count += 1
                try:
                    cross_correlation_threshold = settings.LUMINOL_CROSS_CORRELATION_THRESHOLD
                except:
                    cross_correlation_threshold = 0.9
                if my_correlator.is_correlated(threshold=cross_correlation_threshold):
                    correlation = my_correlator.get_correlation_result()
                    correlated = True
                    correlations.append([metric_base_name, correlation.coefficient, correlation.shift, correlation.shifted_coefficient])
                    remote_correlations_count += 1
            except:
                pass
        if correlated:
            correlated_metrics.append(metric_base_name)

    end_remote_correlations = timer()

    # @added 20201207 - Feature #3858: skyline_functions - correlate_or_relate_with
    if len(do_not_correlate_with) > 0:
        logger.info('get_correlations :: discarded %s remote assigned metrics as not in a correlation group with %s' % (
            str(len(do_not_correlate_with)), base_name))

    logger.info('get_correlations :: checked - remote_correlations_check_count is %s' % str(remote_correlations_check_count))
    logger.info('get_correlations :: correlated - remote_correlations_count is %s' % str(remote_correlations_count))
    logger.info('get_correlations :: processed remote correlations on remote_metrics_count %s local metric in %.6f seconds' % (
        str(remote_metrics_count),
        (end_remote_correlations - start_remote_correlations)))

    end = timer()
    logger.info('get_correlations :: checked a total of %s metrics and correlated %s metrics to %s anomaly, processed in %.6f seconds' % (
        str(metrics_checked_for_correlation), str(len(correlated_metrics)),
        base_name, (end - start)))
    # @added 20170720 - Task #2462: Implement useful metrics for Luminosity
    # Added runtime to calculate avg_runtime Graphite metric
    runtime = '%.6f' % (end - start)

    # @added 20210124 - Feature #3956: luminosity - motifs
    # @modified 20230107 - Task #4778: v4.0.0 - update dependencies
    # COMMENTED OUT ALL THE LUMINOSITY_ANALYSE_MOTIFS sections as these are WIP and
    # TODO
    # if LUMINOSITY_ANALYSE_MOTIFS:
    #     # DO stuff...
    #     # def skyline_matrixprofile(current_skyline_app, parent_pid, timeseries, algorithm_parameters):
    #     todo = True

    return (correlated_metrics, correlations, metrics_checked_for_correlation, runtime)


def process_correlations(i, anomaly_id):

    # logger = logging.getLogger(skyline_app_logger)

    # @added 20230315 - Task #4872: Optimise luminosity for labelled_metrics
    processing_start = int(time())
    metrics_checked_for_correlation = 0
    runtime = 0

    anomalies = []
    correlated_metrics = []
    correlations = []
    sorted_correlations = []
    # @added 20210124 - Feature #3956: luminosity - motifs
    motifs = []

    start_process_correlations = timer()

    anomaly_id, metric_id, anomaly_timestamp, base_name = get_anomaly(anomaly_id)

    # @added 20230315 - Task #4872: Optimise luminosity for labelled_metrics
    try:
        if (int(anomaly_timestamp) + 900) < processing_start:
            logger.info('process_correlations :: the anomaly_timestamp %s is too old not correlating' % str(anomaly_timestamp))
            return (base_name, anomaly_timestamp, anomalies, correlated_metrics, correlations, sorted_correlations, metrics_checked_for_correlation, runtime, motifs)
    except Exception as err:
        logger.error('error :: process_correlations :: failed to determine age of the anomaly - %s' % str(err))

    # @added 20200428 - Feature #3510: Enable Luminosity to handle correlating namespaces only
    #                   Feature #3500: webapp - crucible_process_metrics
    #                   Feature #1448: Crucible web UI
    # Discard the check if the anomaly_timestamp is not in FULL_DURATION as it
    # will have been added via the Crucible or webapp/crucible route
    start_timestamp_of_full_duration_data = int(time() - settings.FULL_DURATION)
    if anomaly_timestamp < (start_timestamp_of_full_duration_data + 2000):
        logger.info('process_correlations :: the anomaly_timestamp %s is too old not correlating' % str(anomaly_timestamp))
        metrics_checked_for_correlation = 0
        runtime = 0
        # @added 20210124 - Feature #3956: luminosity - motifs
        # Added motifs
        return (base_name, anomaly_timestamp, anomalies, correlated_metrics, correlations, sorted_correlations, metrics_checked_for_correlation, runtime, motifs)

    # @modified 20230316 - Task #4872: Optimise luminosity for labelled_metrics
    # Added resolution to the return
    anomalous_ts, resolution = get_anomalous_ts(base_name, anomaly_timestamp)

    if not anomalous_ts:
        metrics_checked_for_correlation = 0
        runtime = 0
        logger.info('process_correlations :: no timeseries data available in Redis for %s, nothing to correlate' % str(base_name))
        # @added 20210124 - Feature #3956: luminosity - motifs
        # Added motifs
        return (base_name, anomaly_timestamp, anomalies, correlated_metrics, correlations, sorted_correlations, metrics_checked_for_correlation, runtime, motifs)
    anomalies = get_anoms(anomalous_ts)
    if not anomalies:
        metrics_checked_for_correlation = 0
        runtime = 0
        logger.info('process_correlations :: no anomalies found for %s, nothing to correlate' % str(base_name))
        # @added 20210124 - Feature #3956: luminosity - motifs
        # Added motifs
        return (base_name, anomaly_timestamp, anomalies, correlated_metrics, correlations, sorted_correlations, metrics_checked_for_correlation, runtime, motifs)

    # @modified 20200506 - Feature #3510: Enable Luminosity to handle correlating namespaces only
    # assigned_metrics = get_assigned_metrics(i)
    # @modified 20230316 - Task #4872: Optimise luminosity for labelled_metrics
    # Added anomaly_timestamp and resolution
    assigned_metrics = get_assigned_metrics(i, base_name, anomaly_timestamp, resolution)

    # @added 20201203 - Feature #3860: luminosity - handle low frequency data
    # Determine data resolution
    # @modified 20210820 - Feature #3860: luminosity - handle low frequency data
    #                      Task #4030: refactoring
    #                      Feature #4164: luminosity - cloudbursts
    # Use common determine_data_frequency function
    # resolution = determine_resolution(anomalous_ts)
    # @modified 20230316 - Task #4872: Optimise luminosity for labelled_metrics
    # Added resolution to the return in get_anomalous_ts
    # resolution = determine_data_frequency(skyline_app, anomalous_ts, False)

    # @added 20220810 - Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    # Separate the normal Redis metric from labelled_metrics and reconstruct the
    # assigned_metrics
    redis_assigned_metrics = [i_metric for i_metric in assigned_metrics if not i_metric.startswith('labelled_metrics.')]
    assigned_labelled_metrics = [i_metric for i_metric in assigned_metrics if i_metric.startswith('labelled_metrics.')]
    assigned_metrics = redis_assigned_metrics + assigned_labelled_metrics

    # @modified 20220810 - Task #2732: Prometheus to Skyline
    #                      Branch #4300: prometheus
    # raw_assigned = redis_conn.mget(assigned_metrics)
    # @modified 20230411 - Task #4872: Optimise luminosity for labelled_metrics
    # raw_assigned = redis_conn.mget(redis_assigned_metrics)
    if redis_assigned_metrics:
        try:
            raw_assigned = redis_conn.mget(redis_assigned_metrics)
        except Exception as err:
            logger.error('error :: process_correlations :: failed to mget redis_assigned_metrics - %s' % str(err))
    else:
        raw_assigned = []

    # @added 20180720 - Feature #2464: luminosity_remote_data
    remote_assigned = []
    if settings.REMOTE_SKYLINE_INSTANCES:
        # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
        # Add the metric resolution
        # remote_assigned = get_remote_assigned(anomaly_timestamp)
        try:
            remote_assigned = get_remote_assigned(anomaly_timestamp, resolution)
        except Exception as err:
            logger.error('error :: process_correlations :: get_remote_assigned failed - %s' % str(err))


    # @added 20230315 - Task #4872: Optimise luminosity for labelled_metrics
    processing_start = int(time())
    get_correlations_start = int(time())
    # @modified 20180720 - Feature #2464: luminosity_remote_data
    # correlated_metrics, correlations = get_correlations(base_name, anomaly_timestamp, anomalous_ts, assigned_metrics, raw_assigned, anomalies)
    # @modified 20230315 - Task #4872: Optimise luminosity for labelled_metrics
    # correlated_metrics, correlations, metrics_checked_for_correlation, runtime = get_correlations(base_name, anomaly_timestamp, anomalous_ts, assigned_metrics, raw_assigned, remote_assigned, anomalies)
    correlated_metrics, correlations, metrics_checked_for_correlation, runtime = get_correlations(base_name, anomaly_timestamp, anomalous_ts, assigned_metrics, raw_assigned, remote_assigned, anomalies, processing_start, resolution)
    sorted_correlations = sorted(correlations, key=lambda x: x[1], reverse=True)
    end_process_correlations = timer()

    runtime = end_process_correlations - start_process_correlations

    logger.info('process_correlations :: took %.6f seconds' % (
        (end_process_correlations - start_process_correlations)))

    # @modified 20210124 - Feature #3956: luminosity - motifs
    # Added motifs
    return (base_name, anomaly_timestamp, anomalies, correlated_metrics, correlations, sorted_correlations, metrics_checked_for_correlation, runtime, motifs)
