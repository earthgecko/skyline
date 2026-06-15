import logging
import traceback

from sqlalchemy.sql import select

from database import get_engine, engine_disposal, ionosphere_table_meta


# @added 20240201 - Task #5250: Optimise ionosphere_backend.get_fp_matches
# Added a hash of all echo fp ids specifically for webapp
# ionosphere_backend.get_fp_matches so that the /api&anomaly
# request does not need to make a SQL query it get all the echo
# fp ids and it is fast.
def get_ionosphere_echo_fps(current_skyline_app, last_id=0):
    """
    Return a dict of the echo fp ids and metric ids > last_id
    """
    function_str = 'functions.database.queries.get_ionosphere_echo_fps'
    log_msg = None
    trace = None

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    echo_fps = {}
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
        return echo_fps

    try:
        ionosphere_table, log_msg, trace = ionosphere_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get ionosphere_table meta, err: %s' % (
            function_str, err))
        if engine:
            engine_disposal(engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return echo_fps

    errors = []
    try:
        #connection = engine.connect()
        # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #stmt = select([ionosphere_table.c.id,ionosphere_table.c.metric_id]).\
        stmt = select(ionosphere_table.c.id,ionosphere_table.c.metric_id).\
            where(ionosphere_table.c.id > int(last_id)).\
            where(ionosphere_table.c.echo_fp == 1)

        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #results = connection.execute(stmt)
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]
        for row in results:
            try:
                echo_fps[row['id']] = row['metric_id']
            except Exception as err:
                errors.append(['err adding to echo_fps', err])
        #connection.close()
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not get echo fps, err: %s' % (
            function_str, err))
        if engine:
            engine_disposal(engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return echo_fps

    if engine:
        engine_disposal(current_skyline_app, engine)
    if log_msg:
        del log_msg
    if trace:
        del trace
    return echo_fps
