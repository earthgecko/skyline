import logging
import traceback

from sqlalchemy.sql import select
# @added 20241010 - Feature #5479: ionosphere.alias_features_profile
from sqlalchemy import or_

from database import get_engine, engine_disposal, ionosphere_table_meta
# @added 20241010 - Feature #5479: ionosphere.alias_features_profile
from functions.database.queries.get_alias_fps_for_metrics import get_alias_fps_for_metrics

# @added 20210426  - Feature #4014: Ionosphere - inference
# Add a global method to query the DB for enabled (or disabled) features
# profiles from the ionosphere table row
def get_ionosphere_fp_ids_for_full_duration(
        current_skyline_app, metric_id, full_duration=0, enabled=True,
        # @added 20260209 - Feature #5705: ionosphere and inference limit
        by_latest_matches=False):
    """
    Return the ionosphere table database rows as a dict
    """
    function_str = 'functions.database.queries.get_ionosphere_fp_ids_for_full_duration'
    log_msg = None
    trace = None
    if enabled:
        enabled = 1
    else:
        enabled = 0

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    fp_ids_full_duration = {}
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
        return fp_ids_full_duration

    try:
        ionosphere_table, log_msg, trace = ionosphere_table_meta(current_skyline_app, engine)
    except Exception as e:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get ionosphere_table meta for metric id %s - %s' % (
            function_str, str(metric_id), e))
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return fp_ids_full_duration

    # @added 20241010 - Feature #5479: ionosphere.alias_features_profile
    metric_ids_alias_fps_dict = {}
    try:
        metric_ids_alias_fps_dict = get_alias_fps_for_metrics(current_skyline_app, [metric_id])
    except Exception as err:
        current_logger.error('error :: %s :: get_alias_fps_for_metrics failed, err: %s' % (
            function_str, err))
    alias_fp_ids = []
    if len(metric_ids_alias_fps_dict) > 0:
        alias_fp_ids = list(metric_ids_alias_fps_dict[metric_id])

    try:
        #connection = engine.connect()
        if full_duration:
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([ionosphere_table]).\
            stmt = select(ionosphere_table).\
                where(ionosphere_table.c.metric_id == int(metric_id)).\
                where(ionosphere_table.c.full_duration == int(full_duration)).\
                where(ionosphere_table.c.enabled == enabled)
            # @added 20241010 - Feature #5479: ionosphere.alias_features_profile
            if alias_fp_ids:
                # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
                #                      Task #5628: Build v5.0.0 and test
                #stmt = select([ionosphere_table]).where(
                stmt = select(ionosphere_table).where(
                    or_(
                        ionosphere_table.c.metric_id == int(metric_id),
                        ionosphere_table.c.id.in_(alias_fp_ids)
                    )
                ).\
                where(ionosphere_table.c.full_duration == int(full_duration)).\
                where(ionosphere_table.c.enabled == enabled)
        else:
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([ionosphere_table]).\
            stmt = select(ionosphere_table).\
                where(ionosphere_table.c.metric_id == int(metric_id)).\
                where(ionosphere_table.c.enabled == enabled)
            # @added 20241010 - Feature #5479: ionosphere.alias_features_profile
            if alias_fp_ids:
                # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
                #                      Task #5628: Build v5.0.0 and test
                #stmt = select([ionosphere_table]).where(
                stmt = select(ionosphere_table).where(
                    or_(
                        ionosphere_table.c.metric_id == int(metric_id),
                        ionosphere_table.c.id.in_(alias_fp_ids)
                    )
                ).\
                where(ionosphere_table.c.enabled == enabled)

        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #results = connection.execute(stmt)
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]

        if results:
            for row in results:
                fp_id = row['id']
                fp_ids_full_duration[fp_id] = dict(row)
        #connection.close()
    except Exception as e:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not get ionosphere rows for metric id %s - %s' % (
            function_str, str(metric_id), e))
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return fp_ids_full_duration

    # @added 20260209 - Feature #5705: ionosphere and inference limit
    if by_latest_matches:
        # Sort the dictionary by fps with latest matches first followed by id
        sorted_data = dict(sorted(
            fp_ids_full_duration.items(),
            key=lambda item: (
                -max(item[1]['last_matched'], item[1]['motif_last_matched']),  # Latest match first (negated for descending)
                -item[1]['id']  # Then by id descending
            )
        ))
        fp_ids_full_duration = sorted_data

    if len(alias_fp_ids) > 0:
        for fp_id in list(fp_ids_full_duration.keys()):
            if fp_id in alias_fp_ids:
                fp_ids_full_duration[fp_id]['alias_fp_id'] = metric_ids_alias_fps_dict[metric_id][fp_id]['id']
                fp_ids_full_duration[fp_id]['original_metric_id'] = metric_ids_alias_fps_dict[metric_id][fp_id]['original_metric_id']
                # @modified 20250811 - Feature #5479: ionosphere.alias_features_profile
                # There is no alias_metric_id key it should be metric_id
                #fp_ids_full_duration[fp_id]['alias_metric_id'] = metric_ids_alias_fps_dict[metric_id][fp_id]['alias_metric_id']
                try:
                    fp_ids_full_duration[fp_id]['alias_metric_id'] = metric_ids_alias_fps_dict[metric_id][fp_id]['metric_id']
                except KeyError:
                    pass

    if engine:
        engine_disposal(current_skyline_app, engine)
    if log_msg:
        del log_msg
    if trace:
        del trace
    return fp_ids_full_duration
