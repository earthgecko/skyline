"""
insert_new_metric.py
"""
import logging
import traceback

import settings
from database import get_engine, engine_disposal, metrics_table_meta
from ionosphere_functions import get_ionosphere_learn_details


# @added 20221130 - Feature #4734: mirage_vortex
def insert_new_metric(current_skyline_app, metric):
    """
    Insert a new metric into the metrics table
    """
    function_str = 'database.queries.insert_new_metric'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine - %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return 0, fail_msg, trace

    try:
        metrics_table, fail_msg, trace = metrics_table_meta(current_skyline_app, engine)
        current_logger.info(fail_msg)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to get metrics_table meta - %s' % (
            function_str, err)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return 0, fail_msg, trace

    # Set defaults
    learn_full_duration_days = int(settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS)
    valid_learning_duration = int(settings.IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS)
    max_generations = int(settings.IONOSPHERE_LEARN_DEFAULT_MAX_GENERATIONS)
    max_percent_diff_from_origin = float(settings.IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN)
    try:
        use_full_duration, valid_learning_duration, use_full_duration_days, max_generations, max_percent_diff_from_origin = get_ionosphere_learn_details(current_skyline_app, metric)
        learn_full_duration_days = use_full_duration_days
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: failed to get_ionosphere_learn_details for %s' % metric)

    new_metric_id = 0
    try:
        connection = engine.connect()
        ins = metrics_table.insert().values(
            metric=metric,
            learn_full_duration_days=int(learn_full_duration_days),
            learn_valid_ts_older_than=int(valid_learning_duration),
            max_generations=int(max_generations),
            max_percent_diff_from_origin=float(max_percent_diff_from_origin))
        result = connection.execute(ins)
        connection.close()
        new_metric_id = result.inserted_primary_key[0]
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not insert metric: %s into DB - %s' % (
            function_str, str(metric), err)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return 0, fail_msg, trace
    if engine:
        engine_disposal(current_skyline_app, engine)
    if new_metric_id:
        current_logger.info('%s :: inserted new metric with id: %s, metric: %s' % (
            function_str, str(new_metric_id), str(metric)))
    else:
        current_logger.error('error :: %s :: failed to inserted new metric, no id returned, metric: %s' % (
            function_str, str(metric)))

    return new_metric_id
