import logging
import traceback

from sqlalchemy.sql import select

from database import get_engine, engine_disposal, cloudburst_table_meta


def get_cloudburst_row(current_skyline_app, cloudburst_id):
    """
    Return the cloudburst table database row as a dict
    """
    function_str = 'functions.database.queries.get_cloudburst_row'

    current_logger = logging.getLogger(current_skyline_app + 'Log')
    cloudburst_dict = {}
    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine - %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return cloudburst_dict

    try:
        cloudburst_table, log_msg, trace = cloudburst_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get cloudburst_table meta for cloudburst id %s - %s' % (
            function_str, str(cloudburst_id), err))
        if engine:
            engine_disposal(engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return cloudburst_dict

    try:
        connection = engine.connect()
        stmt = select([cloudburst_table]).where(cloudburst_table.c.id == cloudburst_id)
        result = connection.execute(stmt)
        row = result.fetchone()
        cloudburst_dict = dict(row)
        connection.close()
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not get cloudburst row for cloudburst id %s - %s' % (
            function_str, str(cloudburst_id), err))
        if engine:
            engine_disposal(engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return cloudburst_dict

    if engine:
        engine_disposal(current_skyline_app, engine)

    return cloudburst_dict
