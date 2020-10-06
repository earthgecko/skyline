from __future__ import division
import logging

import traceback
import settings
import skyline_version

from sqlalchemy.sql import select

from database import (
    get_engine, snab_table_meta, metrics_table_meta, anomalies_table_meta)

skyline_version = skyline_version.__absolute_version__
skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
try:
    ENABLE_WEBAPP_DEBUG = settings.ENABLE_WEBAPP_DEBUG
except EnvironmentError as err:
    logger.error('error :: cannot determine ENABLE_WEBAPP_DEBUG from settings')
    ENABLE_WEBAPP_DEBUG = False


def get_snab_engine():

    try:
        engine, fail_msg, trace = get_engine(skyline_app)
        return engine, fail_msg, trace
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: update_snab_result :: failed to get MySQL engine for snab table'
        logger.error('%s' % fail_msg)
        # return None, fail_msg, trace
        raise  # to webapp to return in the UI


def snab_engine_disposal(engine):
    if engine:
        try:
            engine.dispose()
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: update_snab_result :: calling engine.dispose()')
    return


def update_snab_result(snab_id, anomaly_id, snab_result):
    """
    Update the relevant field in the snab table.

    :param snab_id: the snab table id
    :param anomaly_id: the anomaly id
    :param snab_result: a selected result
    :type snab_id: int
    :type anomaly_id: int
    :type result: str
    :return: snab_result_updated, base_name, anomaly_timestamp
    :rtype: tuple

    """

    snab_result_updated = False
    base_name = None
    anomaly_timestamp = None
    connection = None

    logger.info(
        'update_snab_result :: for snab id %s with anomaly id %s and result %s' % (
            str(snab_id), str(anomaly_id), str(snab_result)))
    logger.info('getting MySQL engine')
    try:
        engine, fail_msg, trace = get_snab_engine()
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('%s' % fail_msg)
        logger.error('error :: update_snab_result :: could not get a MySQL engine to get update snab table')
        raise  # to webapp to return in the UI
    if not engine:
        trace = 'none'
        fail_msg = 'error :: update_snab_result :: engine not obtained'
        logger.error(fail_msg)
        raise
    try:
        snab_table, log_msg, trace = snab_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('snab_table OK')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: update_snab_result :: failed to get snab_table meta')
        if engine:
            snab_engine_disposal(engine)
        raise  # to webapp to return in the UI

    # @modified 20201004 - Task #3748: POC SNAB
    #                   Branch #3068: SNAB
    # Allow results to be changed
    determined_result = False
    existing_result = None
    try:
        if not connection:
            connection = engine.connect()
        stmt = select([snab_table]).\
            where(snab_table.c.id == snab_id)
        result = connection.execute(stmt)
        for row in result:
            tP_set = row['tP']
            if tP_set:
                existing_result = 'tP'
            fP_set = row['fP']
            if fP_set:
                existing_result = 'fP'
            tN_set = row['tN']
            if tN_set:
                existing_result = 'tN'
            fN_set = row['fN']
            if fN_set:
                existing_result = 'fN'
            unsure_set = row['unsure']
            if unsure_set:
                existing_result = 'unsure'
            determined_result = True
            break
        logger.info('update_snab_result :: current result values to snab id %s - tP: %s, fP: %s, tN: %s, fN: %s, unsure: %s' % (
            str(snab_id), str(tP_set), str(fP_set), str(tN_set), str(fN_set),
            str(unsure_set)))
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('error :: update_snab_result :: could not determine current result values for snab id %s' % (
            str(snab_id)))
        fail_msg = 'error :: update_snab_result :: could not determine current result values for snab id %s' % (
            str(snab_id))
        if engine:
            try:
                connection.close()
            except:
                pass
            snab_engine_disposal(engine)
        raise

    if not determined_result:
        logger.error('error :: update_snab_result :: did not determine current result values for snab id %s' % (
            str(snab_id)))
    else:
        logger.info('update_snab_result :: current existing result for snab id %s - %s' % (
            str(snab_id), str(existing_result)))

    try:
        if not connection:
            connection = engine.connect()
        if snab_result == 'tP':
            stmt = snab_table.update().\
                values(tP=1, fP=None, tN=None, fN=None, unsure=None).\
                where(snab_table.c.id == int(snab_id)).\
                where(snab_table.c.anomaly_id == int(anomaly_id))
        if snab_result == 'fP':
            stmt = snab_table.update().\
                values(tP=None, fP=1, tN=None, fN=None, unsure=None).\
                where(snab_table.c.id == int(snab_id)).\
                where(snab_table.c.anomaly_id == int(anomaly_id))
        if snab_result == 'tN':
            stmt = snab_table.update().\
                values(tP=None, fP=None, tN=1, fN=None, unsure=None).\
                where(snab_table.c.id == int(snab_id)).\
                where(snab_table.c.anomaly_id == int(anomaly_id))
        if snab_result == 'fN':
            stmt = snab_table.update().\
                values(tP=None, fP=None, tN=None, fN=1, unsure=None).\
                where(snab_table.c.id == int(snab_id)).\
                where(snab_table.c.anomaly_id == int(anomaly_id))
        if snab_result == 'unsure':
            stmt = snab_table.update().\
                values(tP=None, fP=None, tN=None, fN=None, unsure=1).\
                where(snab_table.c.id == int(snab_id)).\
                where(snab_table.c.anomaly_id == int(anomaly_id))
        if snab_result == 'NULL':
            stmt = snab_table.update().\
                values(tP=None, fP=None, tN=None, fN=None, unsure=None).\
                where(snab_table.c.id == int(snab_id)).\
                where(snab_table.c.anomaly_id == int(anomaly_id))
        connection.execute(stmt)
        snab_result_updated = True
        logger.info('update_snab_result :: update result for snab id %s with anomaly id %s and result %s' % (
            str(snab_id), str(anomaly_id), str(snab_result)))
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('error :: update_snab_result :: could not update result for snab id %s with anomaly id %s and result %s' % (
            str(snab_id), str(anomaly_id), str(result)))
        fail_msg = 'error :: update_snab_result :: could not update result for snab id %s with anomaly id %s and result %s' % (
            str(snab_id), str(anomaly_id), str(result))
        if engine:
            try:
                connection.close()
            except:
                pass
            snab_engine_disposal(engine)
        raise
    try:
        metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('update_snab_result :: metrics_table OK')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: update_snab_result :: failed to get metrics_table meta for %s' % base_name)
        if engine:
            try:
                connection.close()
            except:
                pass
            snab_engine_disposal(engine)
        raise  # to webapp to return in the UI
    try:
        anomalies_table, log_msg, trace = anomalies_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('update_snab_result :: anomalies_table OK')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: update_snab_result :: failed to get anomalies_table meta')
        if engine:
            try:
                connection.close()
            except:
                pass
            snab_engine_disposal(engine)
        raise  # to webapp to return in the UI
    metric_id = None
    try:
        if not connection:
            connection = engine.connect()
        stmt = select([anomalies_table]).\
            where(anomalies_table.c.id == anomaly_id)
        result = connection.execute(stmt)
        for row in result:
            metric_id = row['metric_id']
            anomaly_timestamp = row['anomaly_timestamp']
            break
        logger.info('update_snab_result :: determined anomaly_timestamp %s for metric id %s for anomaly id %s' % (
            str(anomaly_timestamp), str(metric_id), str(anomaly_id)))
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: update_snab_result :: could not determine anomaly timestamp or metric id from DB for anomaly id %s' % (
            str(anomaly_id))
        logger.error('%s' % fail_msg)
        if engine:
            try:
                connection.close()
            except:
                pass
            snab_engine_disposal(engine)
        raise  # to webapp to return in the UI
    if metric_id:
        try:
            if not connection:
                connection = engine.connect()
            stmt = select([metrics_table]).where(metrics_table.c.id == int(metric_id))
            result = connection.execute(stmt)
            for row in result:
                base_name = row['metric']
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: could not determine metric id from metrics table'
            if engine:
                try:
                    connection.close()
                except:
                    pass
                snab_engine_disposal(engine)
            raise

    if connection:
        try:
            connection.close()
        except:
            pass

    if engine:
        snab_engine_disposal(engine)
    return snab_result_updated, base_name, anomaly_timestamp, existing_result
