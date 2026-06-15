"""
update_ionosphere_fp_resolution.py
"""
import logging
import traceback

from sqlalchemy.sql import select

from database import get_engine, engine_disposal, ionosphere_table_meta


# @added 20240103 - Feature #4672: ionosphere_downsampled
#                   Task #5178: Build and test skyline v4.1.0
def update_ionosphere_fp_resolution(current_skyline_app, fp_id, resolution):
    """
    Update the ionosphere table fp row with the passed resolution
    """
    function_str = 'functions.database.queries.update_ionosphere_fp_resolution'
    log_msg = None
    trace = None
    updated = False

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # DISABLED FOR NOW
    disabled = True
    if disabled:
        current_logger.info('%s :: update_ionosphere_fp_resolution :: DISABLED TBD' % (
            function_str))
        return updated

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine, err: %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return updated

    try:
        ionosphere_table, log_msg, trace = ionosphere_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get ionosphere_table meta for fp id %s, err: %s' % (
            function_str, str(fp_id), err))
        if engine:
            engine_disposal(engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return updated

    try:
        # @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #connection = engine.connect()
        #stmt = ionosphere_table.update().values(
        #    resolution=int(resolution)).\
        #    where(ionosphere_table.c.id == int(fp_id))
        #result = connection.execute(stmt)
        #connection.close()
        stmt = ionosphere_table.update().\
            where(ionosphere_table.c.id == int(fp_id)).values(
            resolution=int(resolution))
        with engine.begin() as connection:
            connection.execute(stmt)

        current_logger.info('%s :: updated fp_id: %s with resolution: %s' % (
            function_str, str(fp_id), str(resolution)))
        updated = True
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not update ionosphere row for fp id %s, err: %s' % (
            function_str, str(fp_id), err))
        if engine:
            engine_disposal(engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return updated

    if engine:
        engine_disposal(current_skyline_app, engine)
    if log_msg:
        del log_msg
    if trace:
        del trace
    return updated
