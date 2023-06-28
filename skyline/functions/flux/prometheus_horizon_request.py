"""
prometheus_horizon_request.py
"""
import logging
import traceback

from skyline_functions import get_redis_conn_decoded
from matched_or_regexed_in_list import matched_or_regexed_in_list
from functions.database.queries.set_metrics_as_inactive import set_metrics_as_inactive
from functions.redis.remove_metrics_from_redis import remove_metrics_from_redis


# @added 20220617 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
def prometheus_horizon_request(
        current_skyline_app, now, post_data):
    """
    Parse the metric entries in a flux prometheus horizon-shard request

    :param current_skyline_app: the app calling the function
    :param now: time string
    :param post_data: the post_data dict
    :type current_skyline_app: str
    :type now: str
    :type post_data: dict
    :return: horizon_prometheus_data
    :rtype: dict

    """
    horizon_prometheus_data = {
        'prometheus_metrics_list': [],
        'metric_namespaces': [],
        'prometheus_metrics': {},
    }
    prometheus_metrics_list = []
    metric_namespaces = []
    prometheus_metrics = {}
    function_str = 'prometheus :: %s :: prometheus_horizon_request' % str(now)
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    metrics = []
    try:
        metrics = post_data['metrics']
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get metrics from post_data[\'metrics\'] - %s' % (
            function_str, err))
        metrics = []
    current_logger.info('%s :: processing %s prometheus metrics from horizon-shard request' % (
        function_str, str(len(metrics))))

    error_logged = False
    for metric_data_dict in metrics:
        try:
            metric = metric_data_dict['metric']
            metric_list_data = '%s %s %s %s %s' % (
                str(metric_data_dict['tenant_id']),
                str(metric_data_dict['server_id']),
                str(metric_data_dict['timestamp']),
                str(metric_data_dict['value']),
                metric)
            prometheus_metrics_list.append(metric_list_data)
            metric_name_elements = metric.split('{', 1)
            metric_namespace = metric_name_elements[0]
            metric_namespaces.append(metric_namespace)
            prometheus_metrics[metric] = {}
            prometheus_metrics[metric]['metric_namespace'] = metric_namespace
        except Exception as err:
            if not error_logged:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to interpolate metric_list_data from get metrics from %s - %s' % (
                    function_str, str(metric_data_dict), err))
                error_logged = True
    metric_namespaces = list(set(metric_namespaces))
    current_logger.info('%s :: prometheus_metrics_list has %s entries with %s metric_namespaces' % (
        function_str, str(len(prometheus_metrics_list)),
        str(len(metric_namespaces))))

    horizon_prometheus_data = {
        'prometheus_metrics_list': prometheus_metrics_list,
        'metric_namespaces': metric_namespaces,
        'prometheus_metrics': prometheus_metrics,
    }

    return horizon_prometheus_data
