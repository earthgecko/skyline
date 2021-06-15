from __future__ import division
import logging
import traceback
from sqlalchemy.sql import select

from database import get_engine, engine_disposal, anomalies_table_meta


# @added 20210420  - Task #4022: Move mysql_select calls to SQLAlchemy
# Add a global method to query the DB for the latest_anomalies
def latest_anomalies(current_skyline_app):
    """
    Return the latest anomalies as a list of tuples, each tuple a DB row.
    """
    function_str = 'database_queries.latest_anomalies'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    anomalies = []
    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as e:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine - %s' % (function_str, e)
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
    except Exception as e:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to get metrics_table meta - %s' % (function_str, e)
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
        current_logger.info('%s :: determined %s latest anomalies' % (
            function_str, str(len(anomalies))))
    except Exception as e:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not determine latest anomalies - %s' % (
            function_str, e)
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
