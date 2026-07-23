import logging
import traceback

from sqlalchemy.sql import select

from database import get_engine, engine_disposal, ionosphere_table_meta
from skyline_functions import get_redis_conn_decoded


# @added 20260708 - Feature #5764: get_ionosphere_disabled_fp_ids
#                   Feature #5572: get_all_fps
#                   Feature #3890: metrics_manager - sync_cluster_files
def get_ionosphere_disabled_fp_ids(current_skyline_app):
    """
    Return the list of fp_ids that are disabled in the database.
    """
    function_str = 'functions.database.queries.get_ionosphere_disabled_fp_ids'
    log_msg = None
    trace = None

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    disabled_fp_ids = []

    current_logger.info('%s :: determining disabled fp_ids from the DB' % (
        function_str))

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
        return disabled_fp_ids

    try:
        ionosphere_table, log_msg, trace = ionosphere_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get ionosphere_table meta, err: %s' % (
            function_str, err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return disabled_fp_ids

    results = []
    try:
        stmt = select(ionosphere_table.c.id).\
            where(ionosphere_table.c.enabled == 0)
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not get disabled fp_ids, err: %s' % (
            function_str, err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return disabled_fp_ids

    errors = []
    for row in results:
        try:
            disabled_fp_ids.append(row['id'])
        except Exception as err:
            errors.append([err])
    if errors:
        current_logger.error('error :: %s :: errors reported in determining disabled fp_ids, errors[0]: %s' % (
            function_str, str(errors[0])))

    current_logger.info('%s :: determined %s disabled fp_ids from the DB' % (
        function_str, str(len(disabled_fp_ids))))

    # Create the latest.ionosphere.disabled.fp_ids Redis set which expires every
    # 30 minutes.
    if disabled_fp_ids:
        try:
            redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
        except Exception as err:
            trace = traceback.format_exc()
            current_logger.error(trace)
            fail_msg = 'error :: %s :: get_redis_conn_decoded failed, err: %s' % (function_str, err)
            current_logger.error('%s' % fail_msg)
        try:
            redis_conn_decoded.sadd('latest.ionosphere.disabled.fp_ids', *(disabled_fp_ids))
            redis_conn_decoded.expire('latest.ionosphere.disabled.fp_ids', 1800)
        except Exception as err:
            trace = traceback.format_exc()
            current_logger.error(trace)
            fail_msg = 'error :: %s :: get_redis_conn_decoded failed, err: %s' % (function_str, err)
            current_logger.error('%s' % fail_msg)


    if engine:
        engine_disposal(current_skyline_app, engine)
    if log_msg:
        del log_msg
    if trace:
        del trace
    return disabled_fp_ids
