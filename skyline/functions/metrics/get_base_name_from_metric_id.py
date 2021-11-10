import logging
import traceback

from skyline_functions import get_redis_conn_decoded
from functions.database.queries.base_name_from_metric_id import base_name_from_metric_id


def get_base_name_from_metric_id(current_skyline_app, metric_id):
    """
    Returns a metric id for a base_name from the
    aet.metrics_manager.ids_with_metric_names Redis hash or the DB if not found
    in Redis.

    :param current_skyline_app: the app calling the function
    :param metric_id: the metric id to lookup the base_name for.
    :type current_skyline_app: str
    :type metric_id: int
    :return: base_name
    :rtype: str

    """

    redis_key = 'aet.metrics_manager.ids_with_metric_names'
    function_str = 'functions.metrics.get_base_name_from_metric_id'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as e:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: get_redis_conn_decoded failed - %s' % (
            current_skyline_app, function_str, e))
        return metric_id

    base_name = None
    try:
        base_name = redis_conn_decoded.hget(redis_key, metric_id)
        # DEBUG
        current_logger.info('debug :: %s :: %s :: hget(%s, %s)' % (
            current_skyline_app, function_str, redis_key, str(metric_id)))
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: failed to get base_name for %s: %s' % (
            current_skyline_app, function_str, str(metric_id), str(err)))

    if not base_name:
        try:
            base_name = base_name_from_metric_id(current_skyline_app, metric_id, False)
        except Exception as err:
            current_logger.error('error :: %s :: %s :: base_name_from_metric_id falied to determine base_name from metric_id: %s - %s' % (
                current_skyline_app, function_str, str(metric_id), str(err)))

    return base_name
