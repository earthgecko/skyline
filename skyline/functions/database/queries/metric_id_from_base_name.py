from __future__ import division
import logging
import traceback
from sqlalchemy.sql import select

from database import get_engine, engine_disposal, metrics_table_meta


# @added 20210420  - Task #4022: Move mysql_select calls to SQLAlchemy
# Add a global method to query the DB for a metric id from a base_name
def metric_id_from_base_name(current_skyline_app, base_name):
    """
    Given a base name, return the metric_id
    """
    metric_id = 0
    function_str = 'database_queries.metric_id_from_base_name'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as e:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine - %s' % (function_str, e)
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace

    try:
        metrics_table, fail_msg, trace = metrics_table_meta(current_skyline_app, engine)
        current_logger.info(fail_msg)
    except Exception as e:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to get metrics_table meta for %s- %s' % (
            function_str, base_name, e)
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
        current_logger.info('%s :: determined db metric id: %s' % (
            function_str, str(metric_id)))
    except Exception as e:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not determine id of metric from DB for  %s - %s' % (
            function_str, base_name, e)
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
        current_logger.error('error :: %s :: no id for metric in the DB - %s' % (
            function_str, base_name))
    return metric_id
