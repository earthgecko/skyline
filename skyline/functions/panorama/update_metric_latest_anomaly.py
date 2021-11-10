import logging
import traceback

from skyline_functions import get_redis_conn_decoded
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.database.queries.query_anomalies import get_anomalies


def update_metric_latest_anomaly(current_skyline_app, base_name, metric_id=0):
    """
    Update the panorama.metrics.latest_anomaly Redis hash with the latest
    anomaly for a metric from the database and return a dict of the latest
    anomaly.

    :param current_skyline_app: the app calling the function
    :param base_name: the base_name of the metric to determine the latest
        anomaly for.  Can be None if metric_id is passed as a positive int
    :param metric_id: the metric id to determine the latest anomaly for.  Can be
        None or 0 if base_name passed.
    :type current_skyline_app: str
    :type base_name: str
    :type metric_id: int
    :return: latest_anomaly
    :rtype: dict

    """

    latest_anomaly = {}
    redis_key = 'panorama.metrics.latest_anomaly'

    function_str = 'functions.panorama.update_metric_latest_anomaly'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    current_logger.info('%s :: %s :: determining latest anomaly timestamp' % (
        current_skyline_app, function_str))

    if not base_name:
        if not metric_id:
            current_logger.error('error :: %s :: %s :: no base_name or metric_id passed' % (
                current_skyline_app, function_str))
            return latest_anomaly

    if not metric_id and base_name:
        try:
            metric_id = get_metric_id_from_base_name(current_skyline_app, base_name)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: %s :: get_metric_id_from_base_name failed for %s - %s' % (
                current_skyline_app, function_str, base_name, str(err)))

    if not metric_id:
        current_logger.error('error :: %s :: %s :: no metric_id was found for %s' % (
            current_skyline_app, function_str, base_name))
        return latest_anomaly

    params = {'latest': True}
    try:
        latest_anomaly = get_anomalies(current_skyline_app, metric_id, params)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: get_anomalies failed no latest_anomaly was found for %s - %s' % (
            current_skyline_app, function_str, base_name, str(err)))

    if not latest_anomaly:
        current_logger.info('%s :: %s :: no latest_anomaly was found for %s metric_id: %s' % (
            current_skyline_app, function_str, base_name, str(metric_id)))
        return latest_anomaly

    anomaly_data_dict = {}
    # Format the results from their Python mysql types
    try:
        anomaly_ids_list = list(latest_anomaly.keys())
        anomaly_id = anomaly_ids_list[0]
        anomaly_data_dict = latest_anomaly[anomaly_id]
        for key in list(anomaly_data_dict.keys()):
            if 'decimal.Decimal' in str(type(anomaly_data_dict[key])):
                anomaly_data_dict[key] = float(anomaly_data_dict[key])
            if 'datetime.datetime' in str(type(anomaly_data_dict[key])):
                anomaly_data_dict[key] = str(anomaly_data_dict[key])
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: failed to created formatted anomaly_data_dict for %s - %s' % (
            current_skyline_app, function_str, base_name, str(err)))

    if not anomaly_data_dict:
        current_logger.error('%s :: %s :: no formatted anomaly_data_dict was created for %s metric_id: %s' % (
            current_skyline_app, function_str, base_name, str(metric_id)))
        return {}

    latest_anomaly = anomaly_data_dict.copy()
    latest_anomaly['metric'] = base_name

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
        set_key = redis_conn_decoded.hset(redis_key, base_name, str(latest_anomaly))
        if set_key:
            current_logger.info('%s :: %s :: updated %s for %s with latest_anomaly: %s' % (
                current_skyline_app, function_str, redis_key, base_name,
                str(latest_anomaly)))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: failed to update Redis key %s with %s - %s' % (
            current_skyline_app, function_str, redis_key, str(latest_anomaly),
            str(err)))

    return latest_anomaly
