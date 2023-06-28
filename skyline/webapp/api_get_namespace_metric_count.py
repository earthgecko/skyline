"""
api_get_namespace_metric_count.py
"""
import logging
from flask import request

from functions.metrics.get_namespace_metric_count import get_namespace_metric_count


# @added 20230131 - Feature #4838: functions.metrics.get_namespace_metric.count
#                   Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
def api_get_namespace_metric_count(current_skyline_app):
    """
    Return a dict with the number of metrics in the namespace with totals for
    total_metrics, inactive_metrics and active_metrics.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: namespace_metric_count
    :rtype: dict

    """
    namespace_metric_count = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    namespace = None
    try:
        namespace = request.args.get('namespace')
        current_logger.info('api_get_namespace_metric_count :: with namespace: %s' % str(namespace))
    except Exception as err:
        current_logger.error('error :: api_get_namespace_metric_count :: no namespace argument - %s' % err)
        return 400

    # @added 20230202 - Feature #4838: functions.metrics.get_namespace_metric.count
    # Declare a primary_namespace
    primary_namespace = False
    try:
        if 'primary_namespace' in request.args:
            primary_namespace_str = request.args.get('primary_namespace')
            if primary_namespace_str == 'true':
                primary_namespace = True
                current_logger.info('api_get_namespace_metric_count :: with primary_namespace: %s' % str(primary_namespace_str))
    except Exception as err:
        current_logger.error('error :: api_get_namespace_metric_count :: determining primary_namespace argument - %s' % err)

    try:
        namespace_metric_count = get_namespace_metric_count(current_skyline_app, namespace, primary_namespace=primary_namespace)
    except Exception as err:
        current_logger.error('error :: api_get_namespace_metric_count :: get_namespace_metric_count failed - %s' % (
            err))
    current_logger.info('api_get_namespace_metric_count :: namespace_metric_count: %s' % str(namespace_metric_count))
    return namespace_metric_count
