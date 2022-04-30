"""
get_ionosphere_fp_row_from_match_id.py
"""
import logging
import traceback

from sqlalchemy.sql import select

from database import (
    get_engine, engine_disposal, ionosphere_matched_table_meta,
    ionosphere_layers_matched_table_meta)
from functions.database.queries.get_ionosphere_fp_row import get_ionosphere_fp_db_row

# @added 20220317 - Feature #4540: Plot matched timeseries
#                   Feature #4014: Ionosphere - inference
def get_ionosphere_fp_row_from_match_id(current_skyline_app, match_id, layers_match_id):
    """
    Return the ionosphere_matched and ionosphere table database rows as a dict
    for the match id
    """
    function_str = 'functions.database.queries.get_ionosphere_fp_row_from_match_id'
    log_msg = None
    trace = None

    table = 'ionosphere_matched'
    if layers_match_id:
        table = 'ionosphere_layers_matched'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    matched = {}
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
        return matched

    try:
        if match_id:
            ionosphere_matched_table, log_msg, trace = ionosphere_matched_table_meta(current_skyline_app, engine)
        if layers_match_id:
            ionosphere_layers_matched_table, log_msg, trace = ionosphere_layers_matched_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get %s meta for match id %s - %s' % (
            function_str, table, str(match_id), err))
        if engine:
            engine_disposal(engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return matched

    try:
        connection = engine.connect()
        if match_id:
            stmt = select([ionosphere_matched_table]).where(ionosphere_matched_table.c.id == int(match_id))
        if layers_match_id:
            stmt = select([ionosphere_layers_matched_table]).where(ionosphere_layers_matched_table.c.id == int(layers_match_id))
        result = connection.execute(stmt)
        row = result.fetchone()
        if row:
            try:
                matched['match'] = dict(row)
            except Exception as err:
                trace = traceback.format_exc()
                connection.close()
                current_logger.error(trace)
                fail_msg = 'error :: %s :: could not convert db %s row to dict for match id %s - %s' % (
                    function_str, table, str(match_id), err)
                current_logger.error('%s' % fail_msg)
                if engine:
                    engine_disposal(current_skyline_app, engine)
                if current_skyline_app == 'webapp':
                    # Raise to webapp
                    raise
                return matched
        else:
            matched['match'] = {}
            current_logger.warning('warning :: %s :: failed to get matched DB row for match id %s' % (
                function_str, str(match_id)))

        connection.close()
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not get %s row for match id %s - %s' % (
            function_str, table, str(match_id), err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return matched
    if engine:
        engine_disposal(current_skyline_app, engine)

    if not matched['match']:
        return matched

    fp_id = 0
    try:
        fp_id = matched['match']['fp_id']
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not determine fp_id from matched[\'match\'][\'fp_id\'] - %s' % (
            function_str, err))
    if fp_id:
        try:
            matched['fp'] = get_ionosphere_fp_db_row(current_skyline_app, int(fp_id))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: could not get ionosphere row for fp id %s - %s' % (
                function_str, str(fp_id), err))

    if log_msg:
        del log_msg
    if trace:
        del trace

    return matched
