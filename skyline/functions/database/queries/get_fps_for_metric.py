import logging
import traceback

from sqlalchemy.sql import select

from database import get_engine, engine_disposal, ionosphere_table_meta


# @added 20220519 - Feature #4326: webapp - panorama_plot_anomalies
#                   Task #4582 - POC ARTime
def get_fps_for_metric(current_skyline_app, metric_id):
    """
    Return a dictionary of all the fps for a metric.
    """
    function_str = 'functions.database.queries.get_fps_for_metric'
    log_msg = None
    trace = None

    fps_dict = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
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
        return fps_dict

    try:
        ionosphere_table, log_msg, trace = ionosphere_table_meta(current_skyline_app, engine)
    except Exception as e:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get ionosphere_table meta for metric id %s - %s' % (
            function_str, str(metric_id), e))
        if engine:
            engine_disposal(engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return fps_dict

    try:
        connection = engine.connect()
        stmt = select([ionosphere_table]).where(ionosphere_table.c.metric_id == int(metric_id))
        results = connection.execute(stmt)
        if results:
            for row in results:
                fp_id = row['id']
                fps_dict[fp_id] = dict(row)
        connection.close()
    except Exception as e:
        trace = traceback.format_exc()
        current_logger.error(trace)
        try:
            connection.close()
        except Exception as err:
            current_logger.error('error :: %s :: failed to close connection - %s' % (
                function_str, err))
        fail_msg = 'error :: %s :: could not convert db ionosphere rows to dict - %s' % (
            function_str, e)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return fps_dict

    if engine:
        engine_disposal(current_skyline_app, engine)
    if log_msg:
        del log_msg
    if trace:
        del trace
    return fps_dict
