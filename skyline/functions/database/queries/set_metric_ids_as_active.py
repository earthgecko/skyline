"""
set_metric_ids_as_active.py
"""
import logging
import traceback
from time import time

from sqlalchemy.sql import select

from database import get_engine, engine_disposal, metrics_table_meta


# @added 20230123 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
#                   Feature #4444: webapp - inactive_metrics
# Set the passed metric ids to active again
def set_metric_ids_as_active(current_skyline_app, metric_ids):
    """
    Set all the passed metric ids as active
    """

    function_str = 'database.queries.set_metric_ids_as_active'
    set_metrics_as_active_count = 0

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    current_logger.info('%s :: attempting to set %s metrics as active' % (
        function_str, str(len(metric_ids))))

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine - %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
        return set_metrics_as_active_count

    try:
        metrics_table, fail_msg, trace = metrics_table_meta(current_skyline_app, engine)
        current_logger.info(fail_msg)
    except Exception as e:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to get metrics_table meta - %s' % (
            function_str, e)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return set_metrics_as_active_count

    try:
        connection = engine.connect()
        stmt = metrics_table.update().values(
            inactive=0, inactive_at=int(time())).\
            where(metrics_table.c.id.in_(metric_ids))
        connection.execute(stmt)
        result = connection.execute(stmt)
        set_metrics_as_active_count = int(result.rowcount)
        connection.close()
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not set inactive to 0 in DB - %s' % (
            function_str, err)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return set_metrics_as_active_count
    if engine:
        engine_disposal(current_skyline_app, engine)

    current_logger.info('%s :: set %s metrics as active' % (
        function_str, str(set_metrics_as_active_count)))

    return set_metrics_as_active_count
