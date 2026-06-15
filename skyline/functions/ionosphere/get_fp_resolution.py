"""
get_fp_resolution.py
"""
import logging
import traceback

from functions.database.queries.fp_timeseries import get_db_fp_timeseries
from functions.database.queries.get_ionosphere_fp_row import get_ionosphere_fp_db_row
from functions.database.queries.update_ionosphere_fp_resolution import update_ionosphere_fp_resolution
from functions.timeseries.determine_data_frequency import determine_data_frequency
from skyline_functions import get_graphite_metric, get_redis_conn_decoded


# @added 20240103 - Feature #4672: ionosphere_downsampled
#                   Task #5178: Build and test skyline v4.1.0
def get_fp_resolution(current_skyline_app, metric_id, fp_id, fp_id_dict={}):
    """
    Return the resolution of a fp timeseries.  If the resolution is not
    recorded in the database, calculate it and record it in the database and in
    Redis.

    :param current_skyline_app: the app calling the function
    :param metric_id: the metric id
    :param fp_id: the features profile id
    :type current_skyline_app: str
    :type metric_id: int
    :type fp_id: int
    :return: resolution
    :rtype: int

    """

    function_str = 'functions.ionosphere.get_fp_resolution'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    current_logger.info('%s :: getting resolution for fp id: %s' % (
        function_str, str(fp_id)))

    try:
        fp_id_row = get_ionosphere_fp_db_row(current_skyline_app, fp_id)
    except Exception as err:
        current_logger.error('error :: %s :: failed to get_ionosphere_fp_db_row for fp_id %s - %s' % (
            function_str, str(fp_id), err))

    resolution = 0
    use_fp_timeseries = False
    update_resolution_in_db = True
    try:
        resolution = int(fp_id_row['resolution'])
        if resolution:
            update_resolution_in_db = False
    except KeyError:
        use_fp_timeseries = True
    except Exception as err:
        current_logger.error('%s :: resolution not determined from fp_id_row, err: %s' % (
            function_str, err))

    fp_timeseries = []
    if not resolution and use_fp_timeseries:
        try:
            fp_timeseries = get_db_fp_timeseries(current_skyline_app, metric_id, fp_id)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: get_db_fp_timeseries failed for fp id %s - %s' % (
                function_str, str(fp_id), err))
    if fp_timeseries:
        try:
            resolution = determine_data_frequency(current_skyline_app, fp_timeseries, False)
            if resolution:
                current_logger.info('%s :: fp id: %s, fp_timeseries has resolution: %s' % (
                    function_str, str(fp_id), str(resolution)))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: determine_data_frequency failed for fp id %s - %s' % (
                function_str, str(fp_id), err))

    if resolution:
        redis_key = 'skyline.ionosphere.fp_resolutions'
        try:
            redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
            redis_conn_decoded.hset(redis_key, str(fp_id), str(resolution))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to set Redis key %s - %s' % (
                function_str, redis_key, err))

    if update_resolution_in_db and resolution:
        current_logger.info('%s :: updating resolution in ionosphere table, fp id: %s, resolution: %s' % (
            function_str, str(fp_id), str(resolution)))
        try:
            updated_resolution = update_ionosphere_fp_resolution(current_skyline_app, fp_id, resolution)
            if updated_resolution:
                current_logger.info('%s :: updated resolution in the database' % function_str)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: update_ionosphere_fp_resolution failed, err: %s' % (
                function_str, err))

    return resolution
