import logging
import traceback
from ast import literal_eval

from skyline_functions import get_redis_conn_decoded
# from functions.database.queries.base_name_from_metric_id import base_name_from_metric_id
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from functions.panorama.update_metric_latest_anomaly import update_metric_latest_anomaly


def get_metric_latest_anomaly(current_skyline_app, base_name, metric_id, log=False):
    """
    Return a dict of the latest anomaly for a metric.  This function uses the
    panorama.metrics.latest_anomaly Redis hash key.  If a no data is found in
    the Redis hash for a metric the skyline/functions/panorama/update_metric_latest_anomaly.py
    is called to try and populate the Redis hash key from the database data.

    :param current_skyline_app: the app calling the function
    :param base_name: the base_name of the metric to determine the latest
        anomaly for.  Can be None if metric_id is passed as a positive int
    :param metric_id: the metric id to determine the latest anomaly for.  Can be
        None or 0 if base_name passed.
    :param log: whether to log or not, optional, defaults to False
    :type current_skyline_app: str
    :type log: boolean
    :return: latest_anomaly
    :rtype: dict

    """

    latest_anomaly = {}
    function_str = 'functions.metrics.get_metric_latest_anomaly_timestamp'
    redis_key = 'panorama.metrics.latest_anomaly'

    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.info('%s :: %s :: determining latest anomaly timestamp' % (
            current_skyline_app, function_str))
    else:
        current_logger = None

    if not base_name:
        if not metric_id:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: %s :: no base_name or metric_id passed' % (
                current_skyline_app, function_str))
        return latest_anomaly

    if not base_name:
        try:
            # base_name = base_name_from_metric_id(current_skyline_app, metric_id, False)
            base_name = get_base_name_from_metric_id(current_skyline_app, metric_id)
        except Exception as err:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: %s :: base_name_from_metric_id falied to determine base_name from metric_id: %s - %s' % (
                current_skyline_app, function_str, str(metric_id), str(err)))

    if base_name:
        latest_anomaly_str = None
        try:
            redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
            latest_anomaly_str = redis_conn_decoded.hget(redis_key, base_name)
            # DEBUG
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.info('debug :: %s :: %s :: hget(%s, %s)' % (
                current_skyline_app, function_str, redis_key, base_name))

            if latest_anomaly_str:
                latest_anomaly = literal_eval(str(latest_anomaly_str))
                if log:
                    current_logger.info('%s :: %s :: got %s latest_anomaly: %s' % (
                        current_skyline_app, function_str, base_name,
                        str(latest_anomaly)))
        except Exception as err:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: %s :: failed to get latest_anomaly for %s from Redis hash %s: %s' % (
                current_skyline_app, function_str, base_name, redis_key, str(err)))

    if not latest_anomaly:
        try:
            latest_anomaly = update_metric_latest_anomaly(current_skyline_app, base_name, metric_id)
        except Exception as err:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: %s :: update_metric_latest_anomaly failed for %s - %s' % (
                current_skyline_app, function_str, base_name, str(err)))
            latest_anomaly = {}

    return latest_anomaly
