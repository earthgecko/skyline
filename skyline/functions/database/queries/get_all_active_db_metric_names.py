from __future__ import division
import logging
import traceback
from sqlalchemy.sql import select

from database import get_engine, engine_disposal, metrics_table_meta


# @added 20220302 - Feature #4444: webapp - inactive_metrics
#                   Feature #3828: Add inactive columns to the metrics DB table
#                   Feature #4468: flux - remove_namespace_quota_metrics
def get_all_active_db_metric_names(current_skyline_app, with_ids=False):
    """
    Return all active metric names from the database as a list and optionally
    with a dict of metric_names and ids
    """
    metric_names = []
    metric_names_with_ids = {}
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
        return False, fail_msg, trace

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
        return False, fail_msg, trace

    try:
        connection = engine.connect()
        if with_ids:
            stmt = select([metrics_table.c.id, metrics_table.c.metric]).where(metrics_table.c.inactive == 0)
        else:
            stmt = select([metrics_table.c.metric]).where(metrics_table.c.inactive == 0)
        result = connection.execute(stmt)
        for row in result:
            base_name = row['metric']
            metric_names.append(base_name)
            if with_ids:
                metric_names_with_ids[base_name] = row['id']
        connection.close()
        current_logger.info('%s :: determined metric names from the db: %s' % (
            function_str, str(len(metric_names))))
    except Exception as e:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not determine metric names from DB for - %s' % (
            function_str, e)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return False, fail_msg, trace
    if engine:
        engine_disposal(current_skyline_app, engine)

    if not metric_names:
        current_logger.error('error :: %s :: no metric names returned from the DB' % (
            function_str))

    if with_ids:
        return metric_names, metric_names_with_ids

    return metric_names
