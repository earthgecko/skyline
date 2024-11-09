"""
api_get_metric_anomalies.py
"""
import logging

from flask import request

from functions.metrics.get_metric_anomalies import get_metric_anomalies

# @added 20231124 - Feature #5144: web - api get_metric_anomalies
#                   Feature #5142: web - api get_anomaly_details
#                   Feature #5104: boundary - external_settings
def api_get_metric_anomalies(current_skyline_app):
    """
    Return a dict with the anomaly DB objects for a metric in the given period.

    :param current_skyline_app: the app calling the function
    :param anomaly_id: the anomaly id
    :type current_skyline_app: str
    :type anomaly_id: int
    :return: anomaly_dict
    :rtype: dict

    """
    function_str = 'api_get_metric_anomalies'
    anomalies_dict = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    metric = None
    try:
        metric = request.args.get('metric')
        current_logger.info('api_get_metric_anomalies :: with metric: %s' % str(metric))
    except Exception as err:
        metric = None
    metric_id = None
    try:
        metric_id = int(request.args.get('metric'))
        current_logger.info('api_get_metric_anomalies :: with metric_id: %s' % str(metric_id))
    except Exception as err:
        metric_id = None
    if not metric and not metric_id:
        current_logger.error('error :: api_get_metric_anomalies request no metric or metric_id argument passed - %s' % err)
        return 400

    from_timestamp = None
    try:
        from_timestamp = int(request.args.get('from_timestamp'))
        current_logger.info('api_get_metric_anomalies :: with from_timestamp: %s' % str(from_timestamp))
    except Exception as err:
        current_logger.error('error :: api_get_metric_anomalies request no from_timestamp argument - %s' % err)
        return 400
    until_timestamp = None
    try:
        until_timestamp = int(request.args.get('until_timestamp'))
        current_logger.info('api_get_metric_anomalies :: with until_timestamp: %s' % str(until_timestamp))
    except Exception as err:
        current_logger.error('error :: aapi_get_metric_anomalies request no until_timestamp argument - %s' % err)
        return 400

    try:
        anomalies_dict = get_metric_anomalies(current_skyline_app, from_timestamp, until_timestamp, metric=metric, metric_id=metric_id)
    except Exception as err:
        current_logger.error('error :: %s :: get_metric_anomalies failed for metric: %s, metric_id %s - %s' % (
            function_str, str(metric), str(metric_id), err))
    current_logger.info('%s :: %s :: got %s anomalies for metric: %s, metric_id %s' % (
        current_skyline_app, function_str, str(len(anomalies_dict)),
        str(metric), str(metric_id)))
    current_logger.info('%s :: returning %s anomalies for %s' % (
        function_str, str(len(anomalies_dict)), metric))
    return (anomalies_dict, metric, from_timestamp, until_timestamp)
