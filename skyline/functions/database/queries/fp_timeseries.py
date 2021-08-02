from __future__ import division
import logging
import traceback
from timeit import default_timer as timer

from database import get_engine, engine_disposal


# @added 20210420  - Task #4022: Move mysql_select calls to SQLAlchemy
# Add a global method to query the DB for the latest_anomalies
def get_db_fp_timeseries(current_skyline_app, metric_id, fp_id):
    """
    Return a features profile timeseries from the database as a list
    """
    function_str = 'functions.database.queries.fp_timeseries'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    timeseries = []

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
        return timeseries

    try:
        start_db_query = timer()
        metric_fp_ts_table = 'z_ts_%s' % str(metric_id)
        stmt = 'SELECT timestamp, value FROM %s WHERE fp_id=%s' % (metric_fp_ts_table, str(fp_id))
        connection = engine.connect()
        for row in engine.execute(stmt):
            fp_id_ts_timestamp = int(row['timestamp'])
            fp_id_ts_value = float(row['value'])
            if fp_id_ts_timestamp and fp_id_ts_value:
                timeseries.append([fp_id_ts_timestamp, fp_id_ts_value])
        connection.close()
        end_db_query = timer()
        current_logger.info('%s :: determined %s values for the fp_id %s time series in %6f seconds' % (
            function_str, str(len(timeseries)), str(fp_id),
            (end_db_query - start_db_query)))
    except Exception as e:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not determine timestamps and values from %s - %s' % (
            function_str, metric_fp_ts_table, e))
    if engine:
        engine_disposal(current_skyline_app, engine)
    return timeseries
