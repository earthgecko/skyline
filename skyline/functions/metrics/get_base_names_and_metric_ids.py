import logging
import traceback

from skyline_functions import get_redis_conn_decoded


def get_base_names_and_metric_ids(current_skyline_app):
    """
    Returns a dict of base_names with their metric id from the
    aet.metrics_manager.ids_with_metric_names Redis hash.

    :param current_skyline_app: the app calling the function
    :param metric_id: the metric id to lookup the base_name for.
    :type current_skyline_app: str
    :type metric_id: int
    :return: base_name
    :rtype: str

    """

    base_names_with_ids = {}

    redis_key = 'aet.metrics_manager.metric_names_with_ids'
    function_str = 'functions.metrics.get_base_name_from_metric_id'

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as e:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: get_redis_conn_decoded failed - %s' % (
            current_skyline_app, function_str, e))
        return base_names_with_ids

    try:
        base_names_with_ids = redis_conn_decoded.hgetall(redis_key)
        if base_names_with_ids:
            # Format cast the id str as an int
            for base_name in list(base_names_with_ids.keys()):
                metric_id = int(str(base_names_with_ids[base_name]))
                base_names_with_ids[base_name] = metric_id
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: get_redis_conn_decoded failed - %s' % (
            current_skyline_app, function_str, str(err)))

    return base_names_with_ids
