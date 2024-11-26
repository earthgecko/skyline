"""
api_get_anomaly.py
"""
import logging

from functions.database.queries.get_apps import get_apps
from functions.database.queries.query_anomalies import get_anomaly
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id


# @added 20231123 - Feature #5142: web - api get_anomaly_details
#                   Feature #5104: boundary - external_settings
def api_get_anomaly(current_skyline_app, anomaly_id):
    """
    Return a dict with the anomaly DB object and the addtion of ids interpolated
    to apps, etc where relevant.  Although there are multiple methods to get
    anomaly details via the webapp, none simply return the actually DB anomaly
    object, they return less or more, more being correlations, matches, etc
    which are expensive.  This is to allow external apps to look up an anomaly
    quickly and determine if it is a boundary or mirage anomaly, etc.

    :param current_skyline_app: the app calling the function
    :param anomaly_id: the anomaly id
    :type current_skyline_app: str
    :type anomaly_id: int
    :return: anomaly_dict
    :rtype: dict

    """
    function_str = 'api_get_anomaly'
    anomaly_dict = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        anomaly_dict = get_anomaly(current_skyline_app, anomaly_id)
    except Exception as err:
        current_logger.error('error ::  %s :: get_anomaly failed - %s' % (
            function_str, err))

    # Simply for reference
    current_anomaly_dict_keys = [
        'metric_id',
        'host_id',
        'app_id',
        'source_id',
        'anomaly_timestamp',
        'anomaly_end_timestamp',
        'anomalous_datapoint',
        'full_duration',
        'algorithms_run',
        'triggered_algorithms',
        'created_timestamp',
        'slack_thread_ts',
        'label',
        'user_id',
        'alert'
    ]

    if len(anomaly_dict) > 0:
        metric_id = None
        try:
            metric_id = anomaly_dict['metric_id']
        except:
            metric_id = None
        base_name = None
        if metric_id:
            try:
                base_name = get_base_name_from_metric_id(current_skyline_app, int(metric_id))
            except Exception as err:
                current_logger.error('error :: %s :: get_base_name_from_metric_id failed for metric_id: %s - %s' % (
                    function_str, str(metric_id), err))
        if base_name:
            anomaly_dict['metric'] = base_name
        app_id = None
        try:
            app_id = anomaly_dict['app_id']
        except:
            app_id = None
        app = None
        if app_id:
            apps = {}
            try:
                apps = get_apps(current_skyline_app)
            except Exception as err:
                current_logger.error('error :: %s :: get_apps failed - %s' % (
                    function_str, err))
            if apps:
                for key, value in apps.items():
                    if value == app_id:
                       app = key
                       break
        if app:
            anomaly_dict['app'] = app
        current_logger.info('%s :: created anomaly_dict: %s' % (
            function_str, str(anomaly_dict)))
    else:
        current_logger.info('warning :: %s :: no anomaly data to created anomaly_dict' % function_str)

    return anomaly_dict
