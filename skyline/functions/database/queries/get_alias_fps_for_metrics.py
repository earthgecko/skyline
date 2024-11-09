import logging
import traceback

from sqlalchemy.sql import select

from database import get_engine, engine_disposal, alias_features_profile_table_meta
from functions.database.queries.get_all_db_metric_names import get_all_db_metric_names


# @added 20241007 - Feature #5479: ionosphere.alias_features_profile
def get_alias_fps_for_metrics(current_skyline_app, metric_ids):
    """
    Return a dictionary of all the aliased fps for metrics.

    :param current_skyline_app: the Skyline app executing the function.
    :param metric_ids: a list of metric ids to determine aliases fps for.
    :type current_skyline_app: str
    :type metric_ids: list
    :return: metric_ids_alias_fps_dict
    :rtype: dict


    """
    function_str = 'functions.database.queries.get_alias_fps_for_metrics'
    log_msg = None
    trace = None

    metric_ids_alias_fps_dict = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
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
        return metric_ids_alias_fps_dict

    try:
        alias_features_profile_table, log_msg, trace = alias_features_profile_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get ionosphere_table meta, err: %s' % (
            function_str, err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return metric_ids_alias_fps_dict

    try:
        connection = engine.connect()
        stmt = select([alias_features_profile_table], alias_features_profile_table.c.metric_id.in_(metric_ids))
        results = connection.execute(stmt)
        if results:
            for row in results:
                metric_id = row['metric_id']
                fp_id = row['fp_id']
                if metric_id not in metric_ids_alias_fps_dict.keys():
                    metric_ids_alias_fps_dict[metric_id] = {}
                metric_ids_alias_fps_dict[metric_id][fp_id] = dict(row)
        connection.close()
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        try:
            connection.close()
        except Exception as err2:
            current_logger.error('error :: %s :: failed to close connection, err: %s' % (
                function_str, err2))
        fail_msg = 'error :: %s :: could not convert db ionosphere rows to dict, err: %s' % (
            function_str, err)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return metric_ids_alias_fps_dict

    if engine:
        engine_disposal(current_skyline_app, engine)

    # Add the metric name and original metric from their ids
    if len(metric_ids_alias_fps_dict) > 0:
        metric_names_with_ids = {}
        with_ids = True
        try:
            metric_names, metric_names_with_ids = get_all_db_metric_names(current_skyline_app, with_ids)
        except Exception as err:
            current_logger.error('error :: %s :: get_all_db_metric_names failed, err: %s' % (
                function_str, err))
        metric_ids_with_names = {}
        for metric_name, metric_id in metric_names_with_ids.items():
            metric_ids_with_names[metric_id] = metric_name
    alias_metric_ids = list(metric_ids_alias_fps_dict.keys())
    for alias_metric_id in alias_metric_ids:
        alias_metric = 'unknown'
        try:
            alias_metric = metric_ids_with_names[alias_metric_id]
        except:
            alias_metric = 'unknown'
        fp_ids = list(metric_ids_alias_fps_dict[alias_metric_id].keys())
        for fp_id in fp_ids:
            original_metric = 'unknown'
            try:
                original_metric_id = metric_ids_alias_fps_dict[alias_metric_id][fp_id]['original_metric_id']
                original_metric = metric_ids_with_names[original_metric_id]
            except:
                original_metric = 'unknown'
            metric_ids_alias_fps_dict[alias_metric_id][fp_id]['metric'] = alias_metric
            metric_ids_alias_fps_dict[alias_metric_id][fp_id]['original_metric'] = original_metric

    if log_msg:
        del log_msg
    if trace:
        del trace
    return metric_ids_alias_fps_dict
