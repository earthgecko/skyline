from __future__ import division
import logging
import traceback
from sqlalchemy.sql import select

from database import get_engine, engine_disposal, metrics_table_meta


# @added 20210620  - Task #3596: POC inference
#                    Branch #3590: inference
# Add a global method to query the DB for all untrained metrics
def get_untrained_metrics(current_skyline_app, log=True):
    """
    Return a list of all untrained metrics.
    """
    untrained_metrics = []
    function_str = 'database_queries.get_untrained_metrics'

    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine - %s' % (function_str, e)
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace

    try:
        metrics_table, fail_msg, trace = metrics_table_meta(current_skyline_app, engine)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
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
        stmt = select([metrics_table.c.metric]).where(metrics_table.c.ionosphere_enabled == 0)
        result = connection.execute(stmt)
        for row in result:
            untrained_metrics.append(row['metric'])
        connection.close()
        if log:
            current_logger.info('%s :: determined %s untrained metrics' % (
                function_str, str(len(untrained_metrics))))
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not determine untrained metrics from the DB - %s' % (
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

    if not untrained_metrics:
        if log:
            current_logger.info('%s :: determined there are not untrained metrics - %s' % (
                function_str, str(len(untrained_metrics))))
    return untrained_metrics
