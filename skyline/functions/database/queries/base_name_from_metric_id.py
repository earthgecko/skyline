from __future__ import division
import logging
import traceback
from sqlalchemy.sql import select

from database import get_engine, engine_disposal, metrics_table_meta


# @added 20210706 - Feature #4164: luminosity - cloudbursts
# Add a global method to query the DB for a base_name from a metric id
def base_name_from_metric_id(current_skyline_app, metric_id, log=False):
    """
    Given a metric id, return the base_name
    """
    base_name = None
    function_str = 'database_queries.base_name_from_metric_id'

    def get_log(current_skyline_app):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        return current_logger

    if log:
        current_logger = get_log(current_skyline_app)

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as e:
        if not log:
            current_logger = get_log(current_skyline_app)
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine - %s' % (function_str, e)
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace

    try:
        metrics_table, fail_msg, trace = metrics_table_meta(current_skyline_app, engine)
        if log:
            current_logger.info(fail_msg)
    except Exception as e:
        if not log:
            current_logger = get_log(current_skyline_app)
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
        stmt = select([metrics_table.c.metric]).where(metrics_table.c.id == metric_id)
        result = connection.execute(stmt)
        for row in result:
            base_name = row['metric']
            break
        connection.close()
        if log:
            current_logger.info('%s :: determined metric with id %s base_name: %s' % (
                function_str, str(metric_id), base_name))
    except Exception as e:
        if not log:
            current_logger = get_log(current_skyline_app)
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

    if not base_name:
        current_logger.error('error :: %s :: no base_name for metric in the DB with id %s' % (
            function_str, str(metric_id)))
    return base_name
