"""
update_metric_tenant_id.py
"""
import logging
import traceback

# @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
#import settings

from database import get_engine, engine_disposal, metrics_table_meta


# @added 20250122 - Feature #5592: tenant_id column in DB tables
def update_metric_tenant_id(current_skyline_app, metric, tenant_id):
    """
    Update the metric tenant_id in the metrics table
    """
    function_str = 'database.queries.update_metric_tenant_id'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    set_tenant_id = None

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine - %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
        return set_tenant_id

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
        return set_tenant_id

    try:
        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #connection = engine.connect()
        #ins = metrics_table.update().values(
        #    tenant_id=tenant_id).\
        #    where(metrics_table.c.metric == metric)
        #result = connection.execute(ins)
        #connection.close()
        ins = metrics_table.update().\
            where(metrics_table.c.metric == metric).values(
            tenant_id=tenant_id)
        with engine.begin() as connection:
            connection.execute(ins)

        set_tenant_id = tenant_id
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not update metric: %s into DB - %s' % (
            function_str, str(metric), err)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return 0, fail_msg, trace
    if engine:
        engine_disposal(current_skyline_app, engine)

    return set_tenant_id
