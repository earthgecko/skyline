import logging
import traceback

from sqlalchemy.sql import select

from database import get_engine, engine_disposal, ionosphere_table_meta


# @added 20210426  - Feature #4014: Ionosphere - inference
# Add a global method to query the DB for enabled (or disabled) features
# profiles from the ionosphere table row
def get_ionosphere_fp_ids_for_full_duration(
        current_skyline_app, metric_id, full_duration=0, enabled=True):
    """
    Return the ionosphere table database rows as a dict
    """
    function_str = 'functions.database.queries.get_ionosphere_fp_ids_for_full_duration'
    log_msg = None
    trace = None
    if enabled:
        enabled = 1
    else:
        enabled = 0

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    fp_ids_full_duration = {}
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
        return fp_ids_full_duration

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
        return fp_ids_full_duration

    try:
        connection = engine.connect()
        if full_duration:
            stmt = select([ionosphere_table]).\
                where(ionosphere_table.c.metric_id == int(metric_id)).\
                where(ionosphere_table.c.full_duration == int(full_duration)).\
                where(ionosphere_table.c.enabled == enabled)
        else:
            stmt = select([ionosphere_table]).\
                where(ionosphere_table.c.metric_id == int(metric_id)).\
                where(ionosphere_table.c.enabled == enabled)
        results = connection.execute(stmt)
        if results:
            for row in results:
                fp_id = row['id']
                fp_ids_full_duration[fp_id] = row
        connection.close()
    except Exception as e:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not get ionosphere rows for metric id %s - %s' % (
            function_str, str(metric_id), e))
        if engine:
            engine_disposal(engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return fp_ids_full_duration

    if engine:
        engine_disposal(current_skyline_app, engine)
    if log_msg:
        del log_msg
    if trace:
        del trace
    return fp_ids_full_duration
