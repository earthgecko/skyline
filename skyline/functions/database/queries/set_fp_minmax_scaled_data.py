"""
set_fp_minmax_scaled_data.py
"""
import logging
import traceback

from sqlalchemy.sql import select

from database import get_engine, engine_disposal, ionosphere_minmax_table_meta


# @added 20221026 - Feature #4708: ionosphere - store and cache fp minmax data
def set_fp_minmax_scaled_data(current_skyline_app, fp_minmax_scaled_dict):
    """
    Set the fp minmax scaled data in the ionosphere_minmax table, e.g.:

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
    :param fp_minmax_scaled_dict: a list of metric ids (as in int(id), as ints)
    :type current_skyline_app: str
    :type fp_minmax_scaled_dict: list
    :return: success
    :rtype: boolean

    """
    function_str = 'database.queries.set_fp_minmax_scaled_data'
    success = False

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    current_logger.info('%s :: called with %s' % (
        function_str, str(fp_minmax_scaled_dict)))

    try:
        fp_id = fp_minmax_scaled_dict['id']
        min_fp_value = fp_minmax_scaled_dict['min_fp_value']
        max_fp_value = fp_minmax_scaled_dict['max_fp_value']
        values_count = fp_minmax_scaled_dict['values_count']
        features_count = fp_minmax_scaled_dict['features_count']
        features_sum = fp_minmax_scaled_dict['features_sum']
        tsfresh_version = fp_minmax_scaled_dict['tsfresh_version']
        calc_time = fp_minmax_scaled_dict['calc_time']
    except Exception as err:
        current_logger.error('error :: %s :: failed to determine minmax scaled details from %s - %s' % (
            function_str, str(fp_minmax_scaled_dict), err))
        return success

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not get a MySQL engine - %s' % (function_str, err))
        return success

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
        return success

    connection = None
    try:
        connection = engine.connect()
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not connect to DB - %s' % (
            function_str, err))
        if engine:
            engine_disposal(engine)
        return success

    update_row = False
    db_fp_minmax_scaled_dict = {}
    try:
        stmt = select([ionosphere_minmax_table]).where(ionosphere_minmax_table.c.fp_id == fp_id)
        result = connection.execute(stmt)
        row = result.fetchone()
        if row:
            db_fp_minmax_scaled_dict = dict(row)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not get ionosphere_minmax row for fp id %s - %s' % (
            function_str, str(fp_id), err))
        if connection:
            connection.close()
        if engine:
            engine_disposal(engine)
        return success

    db_method = 'insert'
    if 'fp_id' in db_fp_minmax_scaled_dict:
        update_row = True
        db_method = 'update'

    try:
        if update_row:
            current_logger.info('%s :: updating with new values, existing DB row data: %s' % (
                function_str, str(db_fp_minmax_scaled_dict)))
            stmt = ionosphere_minmax_table.update().values(
                minmax_min=min_fp_value,
                minmax_max=max_fp_value,
                values_count=values_count,
                features_count=features_count,
                features_sum=features_sum,
                tsfresh_version=tsfresh_version,
                calc_time=calc_time).\
                where(ionosphere_minmax_table.c.fp_id == fp_id)
            result = connection.execute(stmt)
            if result.rowcount == 1:
                success = True
            connection.close()
        else:
            current_logger.info('%s :: %s fp %s values into the DB' % (
                function_str, db_method, str(fp_id)))
            inserted_fp_id = None
            connection = engine.connect()
            ins = ionosphere_minmax_table.insert().values(
                fp_id=int(fp_id),
                minmax_min=min_fp_value,
                minmax_max=max_fp_value,
                values_count=values_count,
                features_count=features_count,
                features_sum=features_sum,
                tsfresh_version=tsfresh_version,
                calc_time=calc_time)
            result = connection.execute(ins)
            inserted_fp_id = result.inserted_primary_key[0]
            connection.close()
            if inserted_fp_id:
                success = True
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to %s data for fp_id: %s - %s' % (
            function_str, db_method, str(fp_id), str(err)))
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return success

    if engine:
        engine_disposal(current_skyline_app, engine)

    if success:
        current_logger.info('%s :: %s fp %s minmax values run - %s' % (
            function_str, db_method, str(fp_id), str(success)))
    else:
        current_logger.warning('warning :: %s :: failed to %s fp %s minmax values in the DB' % (
            function_str, db_method, str(fp_id)))

    return success
