"""
get_metric_anomalies.py
"""
import logging

from functions.database.queries.get_apps import get_apps
from functions.database.queries.query_anomalies import get_anomalies_for_period
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name


# @added 20231124 - Feature #5144: web - api get_metric_anomalies
#                   Feature #5142: web - api get_anomaly_details
#                   Feature #5104: boundary - external_settings
def get_metric_anomalies(
        current_skyline_app, from_timestamp, until_timestamp, metric=None, 
        metric_id=None):
    """
    Return a dict with the anomaly DB objects for a metric in the given period.

    :param current_skyline_app: the app calling the function
    :param metric: the metric
    :param from_timestamp: the from_timestamp
    :param until_timestamp: the until_timestamp
    :type current_skyline_app: str
    :type metric: str
    :type from_timestamp: int
    :type until_timestamp: int
    :return: anomalies_dict
    :rtype: dict

    """
    function_str = 'get_metric_anomalies'
    anomalies_dict = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if not metric_id and metric:
        try:
            metric_id = get_metric_id_from_base_name(current_skyline_app, metric)
        except Exception as err:
            current_logger.error('error :: get_metric_anomalies :: get_metric_id_from_base_name failed with metric: %s - %s' % (
                str(metric), err))
            return anomalies_dict
    if not metric and metric_id:
        try:
            metric = get_base_name_from_metric_id(current_skyline_app, metric_id)
        except Exception as err:
            current_logger.error('error :: get_metric_anomalies :: get_base_name_from_metric_id failed with metric_id: %s - %s' % (
                str(metric_id), err))
            return anomalies_dict

    try:
        anomalies_dict = get_anomalies_for_period(current_skyline_app, [metric_id], from_timestamp, until_timestamp)
    except Exception as err:
        current_logger.error('error :: %s :: get_anomalies_for_period failed for metric_id %s - %s' % (
            function_str, str(metric_id), err))
    current_logger.info('%s :: %s :: got %s anomalies for %s' % (
        current_skyline_app, function_str, str(len(anomalies_dict)), metric))

    apps = {}
    if len(anomalies_dict) > 0:
        try:
            apps = get_apps(current_skyline_app)
        except Exception as err:
            current_logger.error('error :: %s :: get_apps failed - %s' % (
                function_str, err))
    for anomaly_id in list(anomalies_dict.keys()):
        anomalies_dict[anomaly_id]['metric'] = metric
        app_id = None
        try:
            app_id = anomalies_dict[anomaly_id]['app_id']
        except:
            app_id = None
        app = None
        if app_id and apps:
            for key, value in apps.items():
                if value == app_id:
                    app = key
                    break
        if app:
            anomalies_dict[anomaly_id]['app'] = app
    current_logger.info('%s :: returning %s anomalies for %s' % (
        function_str, str(len(anomalies_dict)), metric))
    return anomalies_dict
