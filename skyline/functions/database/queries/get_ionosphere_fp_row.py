import logging
import traceback

from sqlalchemy.sql import select

from database import get_engine, engine_disposal, ionosphere_table_meta


# @added 20210425  - Task #4030: refactoring
# Add a global method to query the DB for an ionosphere table row
def get_ionosphere_fp_db_row(current_skyline_app, fp_id):
    """
    Return the ionosphere table database row as a dict
    """
    function_str = 'functions.database.queries.get_ionosphere_fp_db_row'
    log_msg = None
    trace = None

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    fp_id_row = {}
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
        return fp_id_row

    try:
        ionosphere_table, log_msg, trace = ionosphere_table_meta(current_skyline_app, engine)
    except Exception as e:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get ionosphere_table meta for fp id %s - %s' % (
            function_str, str(fp_id), e))
        if engine:
            engine_disposal(engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return fp_id_row

    try:
        connection = engine.connect()
        stmt = select([ionosphere_table]).where(ionosphere_table.c.id == int(fp_id))
        result = connection.execute(stmt)
        row = result.fetchone()
        try:
            fp_id_row = dict(row)
        except Exception as e:
            trace = traceback.format_exc()
            connection.close()
            current_logger.error(trace)
            fail_msg = 'error :: %s :: could not convert db ionosphere row to dict for fp id %s - %s' % (
                function_str, str(fp_id), e)
            current_logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            if current_skyline_app == 'webapp':
                # Raise to webapp
                raise
            return fp_id_row
        connection.close()
    except Exception as e:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not get ionosphere row for fp id %s - %s' % (
            function_str, str(fp_id), e))
        if engine:
            engine_disposal(engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return fp_id_row

    if engine:
        engine_disposal(current_skyline_app, engine)
    if log_msg:
        del log_msg
    if trace:
        del trace
    return fp_id_row
