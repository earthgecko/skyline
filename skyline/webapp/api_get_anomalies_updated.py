"""
api_get_anomalies_updated.py
"""
import logging

from flask import request

from functions.database.queries.get_anomalies_updated import get_anomalies_updated

# @added 20240625 - Feature #5370: anomalies_updated
#                   Feature #5372: vista - bq_update
def api_get_anomalies_updated(current_skyline_app):
    """
    Return a dict with the anomaly DB objects for a metric in the given period.

    :param current_skyline_app: the app calling the function
    :param anomaly_id: the anomaly id
    :type current_skyline_app: str
    :type anomaly_id: int
    :return: anomaly_dict
    :rtype: dict

    """
    function_str = 'api_get_anomalies_updated'
    updated_anomalies = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    anomaly_ids = []
    try:
        anomaly_ids_str = request.args.get('anomaly_ids')
        if anomaly_ids_str:
            anomaly_ids = anomaly_ids_str.split(',')
        current_logger.info('api_get_anomalies_updated :: with anomaly_ids: %s' % str(anomaly_ids))
    except Exception as err:
        anomaly_ids = []

    if len(anomaly_ids) > 0:
        try:
            updated_anomalies = get_anomalies_updated(current_skyline_app, anomaly_ids)
        except Exception as err:
            current_logger.error('error :: %s :: get_anomalies_updated failed for anomaly_ids: %s, err: %s' % (
                function_str, str(anomaly_ids), err))
    current_logger.info('%s :: got %s updated anomalies' % (
        function_str, str(len(updated_anomalies))))
    return updated_anomalies
