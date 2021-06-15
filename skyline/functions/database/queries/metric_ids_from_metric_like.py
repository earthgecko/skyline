from __future__ import division
import logging
import traceback
from sqlalchemy.sql import select

from database import get_engine, engine_disposal, metrics_table_meta


# @added 20210420  - Task #4022: Move mysql_select calls to SQLAlchemy
# Add a global method to query the DB for a metric ids from a wildcard
def metric_ids_from_metric_like(current_skyline_app, metrics_like_str):
    """
    Given a SQL metric name wildcard, return a list of metric_ids
    """
    metric_ids = []
    function_str = 'database_queries.metric_ids_from_metric_like'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as e:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine - %s' % (function_str, e)
        current_logger.error('%s' % fail_msg)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return False

    try:
        metrics_table, fail_msg, trace = metrics_table_meta(current_skyline_app, engine)
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
        return False, fail_msg, trace
    try:
        connection = engine.connect()
        stmt = select([metrics_table.c.id]).where(metrics_table.c.metric.ilike(metrics_like_str))
        result = connection.execute(stmt)
        for row in result:
            metric_ids.append(int(row['id']))
        connection.close()
        current_logger.info('%s :: determined %s metric_ids for metric_like_str: %s' % (
            function_str, str(len(metric_ids)), metrics_like_str))
    except Exception as e:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not determine ids of metrics_like_str: %s -%s' % (
            function_str, str(metrics_like_str), e)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return False, fail_msg, trace
    if engine:
        engine_disposal(current_skyline_app, engine)
    if not metric_ids:
        current_logger.error('error :: %s :: no ids for metrics_like_str: %s' % str(metrics_like_str))
    return metric_ids
