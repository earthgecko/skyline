"""
create_alias_fps_for_metrics.py
"""
import copy
import logging
import traceback

from sqlalchemy.sql import select

from database import get_engine, engine_disposal, alias_features_profile_table_meta
# @added 20241018 - Feature #5481: ionosphere.copy_features_profile
from functions.ionosphere.copy_features_profile import copy_features_profile


# @added 20241009 - Feature #5479: ionosphere.alias_features_profile
def create_alias_fps_for_metrics(current_skyline_app, alias_fps_to_create):
    """
    Return a dictionary of all the aliased fps created.

    :param current_skyline_app: the Skyline app executing the function.
    :param metric_ids: a list of metric ids to determine aliases fps for.
    :type current_skyline_app: str
    :type metric_ids: list
    :return: metric_ids_alias_fps_dict
    :rtype: dict


    """
    function_str = 'functions.database.queries.create_alias_fps_for_metrics'
    log_msg = None
    trace = None

    alias_fps_created = {}
    # @added 20241018 - Feature #5481: ionosphere.copy_features_profile
    fps_copied = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    current_logger.info('%s :: requested to create %s alias fp records' % (
        function_str, str(len(alias_fps_to_create))))
    existing_alias_fp_count = 0

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
        return alias_fps_created, fps_copied
    try:
        alias_features_profile_table, log_msg, trace = alias_features_profile_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get ionosphere_table meta, err: %s' % (
            function_str, err))
        if engine:
            engine_disposal(engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return alias_fps_created, fps_copied

    for fp_id in alias_fps_to_create:
        alias_id = False
        # Existing alias fp_ids for metrics should not be passed through but
        # to ensure idempotency a check is made.
        try:
            metric_id = alias_fps_to_create[fp_id]['metric_id']
            original_metric_id = alias_fps_to_create[fp_id]['original_metric_id']
            label = alias_fps_to_create[fp_id]['label']
            user_id = alias_fps_to_create[fp_id]['user_id']
        except KeyError:
            continue
        try:
            connection = engine.connect()
            stmt = select([alias_features_profile_table]).\
                where(alias_features_profile_table.c.metric_id == metric_id).\
                where(alias_features_profile_table.c.fp_id == int(fp_id))
            result = connection.execute(stmt)
            for row in result:
                alias_id = row['id']
                break
            connection.close()
        except Exception as err:
            trace = traceback.format_exc()
            current_logger.error(trace)
            try:
                connection.close()
            except Exception as err2:
                current_logger.error('error :: %s :: failed to close connection, err: %s' % (
                    function_str, err2))
            fail_msg = 'error :: %s :: could not select rows, err: %s' % (
                function_str, err)
            current_logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            if current_skyline_app == 'webapp':
                # Raise to webapp
                raise
            return alias_fps_created, fps_copied
        if alias_id:
            existing_alias_fp_count += 1
            continue
        try:
            connection = engine.connect()
            ins = alias_features_profile_table.insert().values(
                metric_id=int(metric_id), original_metric_id=int(original_metric_id),
                fp_id=int(fp_id), label=str(label), user_id=user_id)
            result = connection.execute(ins)
            new_alias_fp_id = result.inserted_primary_key[0]
            connection.close()
            alias_fps_created[fp_id] = copy.deepcopy(alias_fps_to_create[fp_id])
            alias_fps_created[fp_id]['alias_id'] = new_alias_fp_id
        except Exception as err:
            trace = traceback.format_exc()
            current_logger.error(trace)
            try:
                connection.close()
            except Exception as err2:
                current_logger.error('error :: %s :: failed to close connection, err: %s' % (
                    function_str, err2))
            fail_msg = 'error :: %s :: could not insert alias fp record, err: %s' % (
                function_str, err)
            current_logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(current_skyline_app, engine)
            if current_skyline_app == 'webapp':
                # Raise to webapp
                raise
            return alias_fps_created, fps_copied

    try:
        connection.close()
    except Exception as err:
        current_logger.error('error :: %s :: failed to close connection after inserts, err: %s' % (
            function_str, err))

    # @added 20241011 - Feature #5481: ionosphere.copy_features_profile
    #                   Feature #5479: ionosphere.alias_features_profile
    # COPY features_profile data to a new features_profile
    # create new fp in ionosphere table
    # create metric_fp_ts_table = 'z_ts_%s' % str(metric_id)
    # copy and rename /opt/skyline/ionosphere/features_profiles resources
    for fp_id, alias_fp_dict in alias_fps_created.items():
        fp_created_via_copy = {}       
        try:
            fp_created_via_copy = copy_features_profile(current_skyline_app, alias_fp_dict)
        except Exception as err:
            trace = traceback.format_exc()
            current_logger.error(trace)
            current_logger.error('error :: %s :: copy_features_profile failed with alias_fp_dict: %s, err: %s' % (
                function_str, str(alias_fp_dict), err))
        if len(fp_created_via_copy) > 0:
            try:
                new_fp_id = fp_created_via_copy['id']
                fps_copied[new_fp_id] = copy.deepcopy(fp_created_via_copy)
            except Exception as err:
                trace = traceback.format_exc()
                current_logger.error(trace)
                current_logger.error('error :: %s :: failed to add fp_created_via_copy to fps_copied, fp_created_via_copy: %s, err: %s' % (
                    function_str, str(fp_created_via_copy), err))

    if engine:
        engine_disposal(current_skyline_app, engine)

    if log_msg:
        del log_msg
    if trace:
        del trace
    current_logger.info('%s :: skipped creating %s alias fps that already exist' % (
        function_str, str(existing_alias_fp_count)))
    current_logger.info('%s :: created %s alias fp records' % (
        function_str, str(len(alias_fps_created))))

    return alias_fps_created, fps_copied
