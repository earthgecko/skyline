from __future__ import division
import logging
import traceback
from sqlalchemy.sql import select

from database import (
    get_engine, engine_disposal, metrics_table_meta, anomalies_table_meta,
)

LOCAL_DEBUG = False


# @added 20210420  - Task #4022: Move mysql_select calls to SQLAlchemy
# Add a global method to query the DB for a metric id from a base_name
def db_query_metric_id_from_base_name(current_skyline_app, base_name):
    """
    Given a base name, return the metric_id
    """
    metric_id = 0
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: db_query_metric_id_from_base_name :: could not get a MySQL engine'
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace

    try:
        metrics_table, fail_msg, trace = metrics_table_meta(current_skyline_app, engine)
        current_logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: db_query_metric_id_from_base_name :: failed to get metrics_table meta for %s' % base_name
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return False, fail_msg, trace

    try:
        connection = engine.connect()
        stmt = select([metrics_table]).where(metrics_table.c.metric == base_name)
        result = connection.execute(stmt)
        for row in result:
            metric_id = int(row['id'])
            break
        connection.close()
        current_logger.info('db_query_metric_id_from_base_name :: determined db metric id: %s' % str(metric_id))
    except:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: db_query_metric_id_from_base_name :: could not determine id of metric from DB for  %s' % base_name
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return False, fail_msg, trace
    if engine:
        engine_disposal(current_skyline_app, engine)

    if not metric_id:
        current_logger.error('error :: db_query_metric_id_from_base_name :: no id for metric in the DB - %s' % base_name)
    return metric_id


def db_query_metric_ids_from_metric_like(current_skyline_app, metrics_like_str):
    """
    Given a SQL metric name wildcard, return a list of metric_ids
    """
    metric_ids = []
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: db_query_metric_ids_from_metric_like :: could not get a MySQL engine'
        current_logger.error('%s' % fail_msg)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return False

    try:
        metrics_table, fail_msg, trace = metrics_table_meta(current_skyline_app, engine)
        current_logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: db_query_metric_ids_from_metric_like :: failed to get metrics_table meta'
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return False, fail_msg, trace
    try:
        connection = engine.connect()
        stmt = select([metrics_table.c.id]).where(metrics_table.c.metric.ilike(metrics_like_str))
        result = connection.execute(stmt)
        for row in result:
            metric_ids.append(int(row['id']))
        connection.close()
        current_logger.info('db_query_metric_ids_from_metric_like :: determined %s metric_ids for metric_like_str: %s' % (
            str(len(metric_ids)), metrics_like_str))
    except:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: db_query_metric_ids_from_metric_like :: could not determine ids of metrics_like_str: %s' % str(metrics_like_str)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return False, fail_msg, trace
    if engine:
        engine_disposal(current_skyline_app, engine)
    if not metric_ids:
        current_logger.error('error :: db_query_metric_ids_from_metric_like :: no ids for metrics_like_str: %s' % str(metrics_like_str))
    return metric_ids


def db_query_latest_anomalies(current_skyline_app):
    """
    Return the latest anomalies as a list of tuples, each tuple a DB row.
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    anomalies = []
    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: db_query_latest_anomalies :: could not get a MySQL engine'
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return anomalies

    try:
        anomalies_table, fail_msg, trace = anomalies_table_meta(current_skyline_app, engine)
        current_logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: db_query_latest_anomalies :: failed to get metrics_table meta'
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return anomalies

    try:
        connection = engine.connect()
        # Replacing panorama query
        # query = 'select id, metric_id, anomalous_datapoint, anomaly_timestamp, full_duration, created_timestamp, anomaly_end_timestamp from anomalies ORDER BY id DESC LIMIT 10'
        stmt = select([anomalies_table.c.id, anomalies_table.c.metric_id,
                       anomalies_table.c.anomalous_datapoint,
                       anomalies_table.c.anomaly_timestamp,
                       anomalies_table.c.full_duration,
                       anomalies_table.c.created_timestamp,
                       anomalies_table.c.anomaly_end_timestamp]).\
            where(anomalies_table.c.id > 0).order_by(anomalies_table.c.id.desc()).\
            limit(10)
        results = connection.execute(stmt)
        anomalies = []
        if results is not None:
            for row in results:
                if row is not None:
                    anomalies.append(row)
        if not anomalies:
            anomalies = []
        connection.close()
        current_logger.info('db_query_latest_anomalies :: determined %s latest anomalies' % str(len(anomalies)))
        current_logger.info('db_query_latest_anomalies :: anomalies: %s' % str(anomalies))
    except:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: db_query_latest_anomalies :: could not determine latest anomalies'
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return anomalies
    if engine:
        engine_disposal(current_skyline_app, engine)
    return anomalies


def db_query_anomalies(
    current_skyline_app, base_name, metrics_like, ):
    """
    Return the latest anomalies as a list of tuples, each tuple a DB row.
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    anomalies = []
    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: db_query_latest_anomalies :: could not get a MySQL engine'
        current_logger.error('%s' % fail_msg)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return anomalies

    try:
        anomalies_table, fail_msg, trace = anomalies_table_meta(current_skyline_app, engine)
        current_logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: db_query_latest_anomalies :: failed to get metrics_table meta'
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return anomalies

    try:
        connection = engine.connect()
        # Replacing panorama query
        # query = 'select id, metric_id, anomalous_datapoint, anomaly_timestamp, full_duration, created_timestamp, anomaly_end_timestamp from anomalies ORDER BY id DESC LIMIT 10'
        stmt = select([anomalies_table.c.id, anomalies_table.c.metric_id,
                       anomalies_table.c.anomalous_datapoint,
                       anomalies_table.c.anomaly_timestamp,
                       anomalies_table.c.full_duration,
                       anomalies_table.c.created_timestamp,
                       anomalies_table.c.anomaly_end_timestamp]).\
            where(anomalies_table.c.id > 0).order_by(anomalies_table.c.id.desc()).\
            limit(10)
        results = connection.execute(stmt)
        anomalies = []
        if results is not None:
            for row in results:
                if row is not None:
                    anomalies.append(row)
        if not anomalies:
            anomalies = []
        connection.close()
        current_logger.info('db_query_latest_anomalies :: determined %s latest anomalies' % str(len(anomalies)))
        current_logger.info('db_query_latest_anomalies :: anomalies: %s' % str(anomalies))
    except:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: db_query_latest_anomalies :: could not determine latest anomalies'
        current_logger.error('%s' % fail_msg)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return anomalies
    return anomalies
