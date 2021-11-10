"""
Get anomalies for a metric id
"""
import logging
import traceback
from sqlalchemy.sql import select

from database import (
    get_engine, engine_disposal, anomalies_table_meta,
)


def get_anomalies(current_skyline_app, metric_id, params={'latest': False}):
    """
    Given a metric_id, return the anomalies for a metric or the latest one if
    latest is passed as True
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    anomalies = {}

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        if fail_msg != 'got MySQL engine':
            current_logger.error('error :: get_anomalies :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            current_logger.error('error :: get_anomalies :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_anomalies :: could not get a MySQL engine - %s' % str(err))

    latest = False
    if params:
        try:
            latest = params['latest']
        except KeyError:
            latest = False

    if engine:
        try:
            anomalies_table, fail_msg, trace = anomalies_table_meta(current_skyline_app, engine)
            if fail_msg != 'anomalies_table meta reflected OK':
                current_logger.error('error :: get_anomalies :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
            if trace != 'none':
                current_logger.error('error :: get_anomalies :: could not get a MySQL engine trace - %s' % str(trace))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_anomalies :: anomalies_table_meta - %s' % str(err))
        try:
            connection = engine.connect()
            if latest:
                stmt = select([anomalies_table]).where(anomalies_table.c.metric_id == metric_id).order_by(anomalies_table.c.id.desc()).limit(1)
            else:
                stmt = select([anomalies_table]).where(anomalies_table.c.metric_id == metric_id).order_by(anomalies_table.c.id.desc())
            results = connection.execute(stmt)
            for row in results:
                anomaly_id = row['id']
                anomalies[anomaly_id] = dict(row)
            connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_anomalies :: failed to build anomalies dict - %s' % str(err))

    if engine:
        engine_disposal(current_skyline_app, engine)

    return anomalies


def get_anomalies_from_timestamp(current_skyline_app, metric_id, from_timestamp):
    """
    Given a metric_id and timestamp return the anomalies for a metric or all
    metrics from the given timestamp.
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    anomalies = {}

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        if fail_msg != 'got MySQL engine':
            current_logger.error('error :: get_anomalies_from_timestamp :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            current_logger.error('error :: get_anomalies_from_timestamp :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_anomalies_from_timestamp :: could not get a MySQL engine - %s' % str(err))

    if engine:
        try:
            anomalies_table, fail_msg, trace = anomalies_table_meta(current_skyline_app, engine)
            if fail_msg != 'anomalies_table meta reflected OK':
                current_logger.error('error :: get_anomalies_from_timestamp :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
            if trace != 'none':
                current_logger.error('error :: get_anomalies_from_timestamp :: could not get a MySQL engine trace - %s' % str(trace))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_anomalies_from_timestamp :: anomalies_table_meta - %s' % str(err))
        try:
            connection = engine.connect()
            if metric_id:
                stmt = select([anomalies_table]).\
                    where(anomalies_table.c.metric_id == metric_id).\
                    where(anomalies_table.c.anomaly_timestamp >= from_timestamp).\
                    order_by(anomalies_table.c.id.desc())
            else:
                stmt = select([anomalies_table]).\
                    where(anomalies_table.c.anomaly_timestamp >= from_timestamp).\
                    order_by(anomalies_table.c.id.desc())
            results = connection.execute(stmt)
            for row in results:
                anomaly_id = row['id']
                anomalies[anomaly_id] = dict(row)
            connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_anomalies_from_timestamp :: failed to build anomalies dict - %s' % str(err))

    if engine:
        engine_disposal(current_skyline_app, engine)

    return anomalies
