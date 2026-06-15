"""
update_anomaly.py
"""
import logging
import traceback

from time import time

from sqlalchemy.sql import select

from database import (
    get_engine, engine_disposal, anomalies_table_meta,
    anomalies_updated_table_meta)

# @added 20240618 - Feature #5370: anomalies_updated
#                    Feature #5372: vista - bq_update
# To record updates made to an anomalies
def update_anomaly(current_skyline_app, anomaly_id, field, new_value):
    """
    Update the anomalies table row field with the passed value and create a
    record in the anomalies_updated table to record the previous value and the
    new value
    """
    function_str = 'functions.database.queries.update_anomaly'
    log_msg = None
    trace = None
    updated = {
        'anomaly_id': anomaly_id, 'field': field, 'new_value': new_value,
        'metric_id': None, 'previous_value': None, 'anomalies_updated_id': None,
    }

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    current_logger.debug('debug :: %s :: passed parameters: %s' % (
        function_str, str(updated)))

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine, err: %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return updated

    try:
        anomalies_table, log_msg, trace = anomalies_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get anomalies_table meta for anomaly id %s, err: %s' % (
            function_str, str(anomaly_id), err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        return updated

    try:
        anomalies_updated_table, log_msg, trace = anomalies_updated_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get anomalies_updated_table meta for anomaly id %s, err: %s' % (
            function_str, str(anomaly_id), err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        return updated

    original_anomaly = {}
    metric_id = None
    original_value = None
    try:
        #connection = engine.connect()
        # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #stmt = select([anomalies_table]).where(anomalies_table.c.id == anomaly_id)
        stmt = select(anomalies_table).where(anomalies_table.c.id == anomaly_id)
        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #results = connection.execute(stmt)
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]

        for row in results:
            original_anomaly = dict(row)
            break
        #connection.close()
        metric_id = original_anomaly['metric_id']
        updated['metric_id'] = int(metric_id)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could select row for anomaly id %s, err: %s' % (
            function_str, str(anomaly_id), err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        return updated
    try:
        original_value_type_str = str(type(original_anomaly[field]))
        if original_value_type_str == "<class 'decimal.Decimal'>":
            original_value = float(original_anomaly[field])
        else:
            original_value = original_anomaly[field]
        updated['previous_value'] = original_value
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to determine original_value for %s for anomaly id %s, err: %s' % (
            function_str, field, str(anomaly_id), err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        return updated

    try:
        values_dict = {anomalies_table.columns[field]: new_value}
        # @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #connection = engine.connect()
        #stmt = anomalies_table.update().values(values_dict).\
        #    where(anomalies_table.c.id == int(anomaly_id))
        ## stmt = anomalies_table.update().values(values_dict).\
        ##stmt = anomalies_table.update().values({field: new_value}).\
        ##    where(anomalies_table.c.id == int(anomaly_id))
        ##current_logger.debug('debug :: %s :: update 2 stmt: %s' % (
        ##    function_str, str(stmt)))
        #result = connection.execute(stmt)
        #connection.close()
        stmt = anomalies_table.update().\
            where(anomalies_table.c.id == int(anomaly_id)).values(values_dict)
        current_logger.debug('debug :: %s :: update 1 stmt: %s' % (
            function_str, str(stmt)))
        with engine.begin() as connection:
            result = connection.execute(stmt)

        current_logger.info('%s :: updated anomaly_id: %s with %s' % (
            function_str, str(anomaly_id), str(values_dict)))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not update anomalies for anomaly id %s, err: %s' % (
            function_str, str(anomaly_id), err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        return updated

    try:
        #connection = engine.connect()
        ins = anomalies_updated_table.insert().values(
            anomaly_id=int(anomaly_id), metric_id=int(metric_id),
            changed_timestamp=int(time()), column=field,
            previous_value=original_value, new_value=new_value)

        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #result = connection.execute(ins)
        #connection.close()
        #new_updated_id = result.inserted_primary_key[0]
        with engine.begin() as connection:
            result = connection.execute(ins)
            new_updated_id = result.inserted_primary_key[0]

        current_logger.info('%s :: new changed id: %s, previous_value: %s, new_value: %s' % (
            function_str, str(new_updated_id), str(original_value),
            str(new_value)))
        updated['anomalies_updated_id'] = new_updated_id
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not insert into anomalies_updated for anomaly id %s, err: %s' % (
            function_str, str(anomaly_id), err))

    if engine:
        engine_disposal(current_skyline_app, engine)
    if log_msg:
        del log_msg
    if trace:
        del trace
    return updated
