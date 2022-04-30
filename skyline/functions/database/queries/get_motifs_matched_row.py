"""
get_motifs_matched_row.py
"""
import logging
import traceback

from sqlalchemy.sql import select

from database import get_engine, engine_disposal, motifs_matched_table_meta


# @added 20220317 - Feature #4540: Plot matched timeseries
#                   Feature #4014: Ionosphere - inference
def get_motifs_matched_row(current_skyline_app, motifs_matched_id):
    """
    Return the motifs_matched table database row as a dict
    """
    function_str = 'functions.database.queries.get_motifs_matched_row'
    log_msg = None
    trace = None

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    motifs_matched_row = {}
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
        return motifs_matched_row

    try:
        motifs_matched_table, log_msg, trace = motifs_matched_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get ionosphere_table meta for motifs_matched id %s - %s' % (
            function_str, str(motifs_matched_id), err))
        if engine:
            engine_disposal(engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return motifs_matched_row

    try:
        connection = engine.connect()
        stmt = select([motifs_matched_table]).where(motifs_matched_table.c.id == int(motifs_matched_id))
        result = connection.execute(stmt)
        row = result.fetchone()
        try:
            motifs_matched_row = dict(row)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: could not convert db motifs_matched row to dict for motifs_matched id %s - %s' % (
                function_str, str(motifs_matched_id), err))
            if engine:
                engine_disposal(engine)
            if current_skyline_app == 'webapp':
                # Raise to webapp
                raise
            return motifs_matched_row
        connection.close()
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not get motifs_matched row for motifs_matched id %s - %s' % (
            function_str, str(motifs_matched_id), err))
        if engine:
            engine_disposal(engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return motifs_matched_row

    if engine:
        engine_disposal(current_skyline_app, engine)
    if log_msg:
        del log_msg
    if trace:
        del trace
    return motifs_matched_row
