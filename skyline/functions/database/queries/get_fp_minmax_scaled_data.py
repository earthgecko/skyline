"""
get_fp_minmax_scaled_data.py
"""
import logging
import traceback

from sqlalchemy.sql import select

from database import get_engine, engine_disposal, ionosphere_minmax_table_meta


# @added 20221026 - Feature #4708: ionosphere - store and cache fp minmax data
def get_fp_minmax_scaled_data(current_skyline_app, fp_id):
    """
    Get the fp minmax scaled data from the ionosphere_minmax table, e.g.:

    fp_minmax_scaled_dict = {
        'id': 123,
        'min_fp_value': 0.100004,
        'max_fp_value': 4.678987,
        'values_count': 1009,
        'features_count': 140,
        'features_sum': 3787.6024799786837,
        'tsfresh_version': '0.17.9',
        'calc_time': 1.186837,
    }

    :param current_skyline_app: the app calling the function
    :param fp_id: the featrues profile id
    :type current_skyline_app: str
    :type fp_id: int
    :return: success
    :rtype: boolean

    """
    function_str = 'database.queries.get_fp_minmax_scaled_data'
    fp_minmax_scaled_dict = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not get a MySQL engine - %s' % (function_str, err))
        return fp_minmax_scaled_dict

    try:
        ionosphere_minmax_table, fail_msg, trace = ionosphere_minmax_table_meta(current_skyline_app, engine)
        current_logger.info(fail_msg)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('%s' % trace)
        current_logger.error('error :: %s :: failed to get ionosphere_minmax_table_meta - %s' % (
            function_str, err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        return fp_minmax_scaled_dict

    try:
        connection = engine.connect()
        stmt = select([ionosphere_minmax_table]).where(ionosphere_minmax_table.c.fp_id == fp_id)
        result = connection.execute(stmt)
        row = result.fetchone()
        if row:
            fp_minmax_scaled_dict = dict(row)
        connection.close()
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not get ionosphere_minmax row for fp id %s - %s' % (
            function_str, str(fp_id), err))

    if engine:
        engine_disposal(current_skyline_app, engine)

    if fp_minmax_scaled_dict:
        current_logger.info('%s :: got fp %s minmax details from the DB' % (
            function_str, str(fp_id)))
    else:
        current_logger.info('%s :: fp %s minmax details not found in the DB' % (
            function_str, str(fp_id)))

    return fp_minmax_scaled_dict
