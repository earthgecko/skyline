import logging
import traceback
import datetime
from sqlalchemy.sql import select
from time import time

from database import get_engine, engine_disposal, metrics_table_meta
from settings import FULL_DURATION


# @added 20220824 - Feature #3870: metrics_manager - check_data_sparsity
def get_new_metrics(current_skyline_app, from_timestamp=None):
    """
    Return all newly created metrics in the last 24 hours (or from_timestamp)
    """
    new_metrics = {}
    function_str = 'database.queries.get_new_metrics'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine - %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
        return new_metrics, fail_msg, trace

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
        return new_metrics, fail_msg, trace

    current_time = datetime.datetime.utcnow()
    full_duration_period = current_time - datetime.timedelta(seconds=FULL_DURATION)

    # @added 20230510 - Feature #4902: Prevent training on metrics newer than 7 days
    if from_timestamp:
        current_timestamp = int(time())
        from_seconds = current_timestamp - from_timestamp
        full_duration_period = current_time - datetime.timedelta(seconds=from_seconds)

    try:
        connection = engine.connect()
        stmt = select([metrics_table.c.metric, metrics_table.c.id]).where(metrics_table.c.created_timestamp > full_duration_period)
        result = connection.execute(stmt)
        for row in result:
            new_metrics[row['metric']] = row['id']
        connection.close()
        current_logger.info('%s :: determined %s new metrics from the db' % (
            function_str, str(len(new_metrics))))
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

    return new_metrics
