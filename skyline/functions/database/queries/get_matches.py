"""
Get anomalies for a metric id
"""
import logging
import traceback
from sqlalchemy.sql import select

from database import (
    get_engine, engine_disposal, ionosphere_matched_table_meta,
    # @added 20220910 - Feature #4658: ionosphere.learn_repetitive_patterns
    # Added layers matches
    ionosphere_layers_matched_table_meta,
)

from functions.database.queries.get_fps_for_metric import get_fps_for_metric


# @added 20220519 - Feature #4326: webapp - panorama_plot_anomalies
#                   Task #4582 - POC ARTime
def get_matches(current_skyline_app, metric_id, from_timestamp, until_timestamp):
    """
    Given a metric_id and timestamps return the matches for a metric.
    """

    function_str = 'functions.database.queries.get_ionosphere_fps_for_metric'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    matches = {}

    fps_dict = {}
    fp_ids = []
    try:
        fps_dict = get_fps_for_metric(current_skyline_app, metric_id)
    except Exception as err:
        current_logger.error('error :: %s :: could not get a MySQL engine - %s' % (
            function_str, str(err)))

    if fps_dict:
        fp_ids = list(fps_dict.keys())

    if not fp_ids:
        current_logger.info('%s :: no fps found for metric id - %s' % (function_str, str(metric_id)))
        return matches

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        if fail_msg != 'got MySQL engine':
            current_logger.error('error :: %s :: could not get a MySQL engine fail_msg - %s' % (
                function_str, str(fail_msg)))
        if trace != 'none':
            current_logger.error('error :: %s :: could not get a MySQL engine trace - %s' % (
                function_str, str(trace)))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: could not get a MySQL engine - %s' % (
            function_str, str(err)))

    if engine:
        try:
            ionosphere_matched_table, fail_msg, trace = ionosphere_matched_table_meta(current_skyline_app, engine)
            if fail_msg != 'ionosphere_matched_table meta reflected OK':
                current_logger.error('error :: %s :: could not get a MySQL engine fail_msg - %s' % (
                    function_str, str(fail_msg)))
            if trace != 'none':
                current_logger.error('error :: %s :: could not get a MySQL engine trace - %s' % (
                    function_str, str(trace)))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: ionosphere_matched_table_meta - %s' % (
                function_str, str(err)))
        try:
            connection = engine.connect()
            if from_timestamp and until_timestamp:
                stmt = select([ionosphere_matched_table], ionosphere_matched_table.c.fp_id.in_(fp_ids)).\
                    where(ionosphere_matched_table.c.metric_timestamp >= from_timestamp).\
                    where(ionosphere_matched_table.c.metric_timestamp <= until_timestamp)
            else:
                stmt = select([ionosphere_matched_table], ionosphere_matched_table.c.fp_id.in_(fp_ids))
            results = connection.execute(stmt)
            for row in results:
                match_id = row['id']
                matches[match_id] = dict(row)
            connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to build matches dict - %s' % (
                function_str, str(err)))

        # @added 20220910 - Feature #4658: ionosphere.learn_repetitive_patterns
        # Added layers matches
        try:
            ionosphere_layers_matched_table, fail_msg, trace = ionosphere_layers_matched_table_meta(current_skyline_app, engine)
            if fail_msg != 'ionosphere_layers_matched_table meta reflected OK':
                current_logger.error('error :: %s :: could not get a MySQL engine fail_msg - %s' % (
                    function_str, str(fail_msg)))
            if trace != 'none':
                current_logger.error('error :: %s :: could not get a MySQL engine trace - %s' % (
                    function_str, str(trace)))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: ionosphere_layers_matched_table_meta - %s' % (
                function_str, str(err)))
        try:
            connection = engine.connect()
            if from_timestamp and until_timestamp:
                stmt = select([ionosphere_layers_matched_table], ionosphere_layers_matched_table.c.fp_id.in_(fp_ids)).\
                    where(ionosphere_layers_matched_table.c.anomaly_timestamp >= from_timestamp).\
                    where(ionosphere_layers_matched_table.c.anomaly_timestamp <= until_timestamp)
            else:
                stmt = select([ionosphere_layers_matched_table], ionosphere_matched_table.c.fp_id.in_(fp_ids))
            results = connection.execute(stmt)
            for row in results:
                match_id = row['id']
                matches[match_id] = dict(row)
            connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to build matches dict - %s' % (
                function_str, str(err)))

    if engine:
        engine_disposal(current_skyline_app, engine)

    return matches
