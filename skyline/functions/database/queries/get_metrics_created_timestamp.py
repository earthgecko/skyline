import logging
import traceback
import datetime
import time

from sqlalchemy.sql import select

from database import get_engine, engine_disposal, metrics_table_meta


def get_metrics_created_timestamp(current_skyline_app, metric_ids):
    """
    Return a dict of metric_ids and the created timestamp of each metric id.
    """
    metrics_created_timestamp = {}
    function_str = 'database.queries.get_all_active_db_metric_names'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine - %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
        return metrics_created_timestamp, fail_msg, trace

    try:
        metrics_table, fail_msg, trace = metrics_table_meta(current_skyline_app, engine)
        current_logger.info(fail_msg)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to get metrics_table meta - %s' % (
            function_str, err)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return metrics_created_timestamp, fail_msg, trace

    try:
        connection = engine.connect()
        if metric_ids:
            stmt = select([metrics_table.c.id, metrics_table.c.created_timestamp], metrics_table.c.id.in_(metric_ids))
        else:
            stmt = select([metrics_table.c.id, metrics_table.c.created_timestamp])
        result = connection.execute(stmt)
        for row in result:
            metric_id = row['id']
            created_timestamp_datetime = row['created_timestamp']
            metrics_created_timestamp[metric_id] = int(time.mktime(created_timestamp_datetime.timetuple()))
        connection.close()
        current_logger.info('%s :: determined %s created_timestamps from the db' % (
            function_str, str(len(metrics_created_timestamp))))
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not determine created_timestamps from DB for - %s' % (
            function_str, err)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return False, fail_msg, trace
    if engine:
        engine_disposal(current_skyline_app, engine)

    if not metrics_created_timestamp:
        current_logger.error('error :: %s :: no metrics_created_timestamp returned from the DB' % (
            function_str))

    return metrics_created_timestamp
