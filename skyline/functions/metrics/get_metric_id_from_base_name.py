import logging
import traceback

from skyline_functions import get_redis_conn_decoded


def get_metric_id_from_base_name(current_skyline_app, base_name):
    """
    Returns a metric id for a base_name.

    :param current_skyline_app: the app calling the function
    :param base_name: the base_name of the metric to determine the latest
        anomaly for.  Can be None if metric_id is passed as a positive int
    :return: metric_id
    :rtype: int

    """

    metric_id = 0
    redis_key = 'aet.metrics_manager.metric_names_with_ids'
    function_str = 'functions.metrics.get_metric_id_from_base_name'

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

    metric_id_str = None
    try:
        metric_id_str = redis_conn_decoded.hget(redis_key, base_name)
        # DEBUG
        current_logger.info('debug :: %s :: %s :: hget(%s, %s)' % (
            current_skyline_app, function_str, redis_key, str(base_name)))
        if metric_id_str:
            metric_id = int(str(metric_id_str))
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: failed to get metric_id for %s: %s' % (
            current_skyline_app, function_str, base_name, str(err)))

    return metric_id
