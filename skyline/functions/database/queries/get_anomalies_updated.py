"""
get_anomalies_updated_updated.py
"""
import logging
import traceback

from sqlalchemy.sql import select

from database import get_engine, engine_disposal, anomalies_updated_table_meta


# @added 20240625 - Feature #5370: anomalies_updated
#                   Feature #5372: vista - bq_update
def get_anomalies_updated(current_skyline_app, anomaly_ids):
    """
    Given a list of anomaly ids, return the anomalies that have been updated.
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    updated_anomalies = {}

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        if fail_msg != 'got MySQL engine':
            current_logger.error('error :: get_anomalies_updated :: could not get a MySQL engine, fail_msg: %s' % str(fail_msg))
        if trace != 'none':
            current_logger.error('error :: get_anomalies_updated :: could not get a MySQL engine, trace: %s' % str(trace))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_anomalies_updated :: could not get a MySQL engine, err: %s' % str(err))

    if engine:
        try:
            anomalies_updated_table, fail_msg, trace = anomalies_updated_table_meta(current_skyline_app, engine)
            if fail_msg != 'anomalies_updated meta reflected OK':
                current_logger.error('error :: get_anomalies_updated :: could not get a MySQL engine, fail_msg: %s' % str(fail_msg))
            if trace != 'none':
                current_logger.error('error :: get_anomalies_updated :: could not get a MySQL engine, trace: %s' % str(trace))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_anomalies_updated :: anomalies_updated_table_meta, err: %s' % str(err))
        try:
            connection = engine.connect()
            stmt = select([anomalies_updated_table], anomalies_updated_table.c.anomaly_id.in_(anomaly_ids))
            results = connection.execute(stmt)
            for row in results:
                anomaly_id = row['anomaly_id']
                updated_anomalies[anomaly_id] = dict(row)
                # Coerce str to float
                try:
                    new_value = float(row['new_value'])
                    updated_anomalies[anomaly_id]['new_value'] = new_value
                except:
                    pass
                try:
                    previous_value = float(row['previous_value'])
                    updated_anomalies[anomaly_id]['previous_value'] = previous_value
                except:
                    pass
            connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_anomalies_updated :: failed to build updated_anomalies dict, err: %s' % str(err))

    if engine:
        engine_disposal(current_skyline_app, engine)

    return updated_anomalies
