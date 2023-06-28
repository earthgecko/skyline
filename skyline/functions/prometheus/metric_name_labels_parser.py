"""
Parse a Prometheus metric and determine metric name and labels
"""
import logging

from prometheus_client.parser import _parse_labels as parse_labels


# @added 20220616 - Branch #4300: prometheus
#                   Task #2732: Prometheus to Skyline
def metric_name_labels_parser(current_skyline_app, metric):
    """
    Given a Prometheus metric string return a dict of the metric name and labels.

    :param current_skyline_app: the app calling the function
    :param metric: the prometheus metric
    :type current_skyline_app: str
    :type metric: str
    :return: metric_dict
    :rtype: dict

    """

    metric_dict = {}

    function_str = '%s :: functions.prometheus.metric_name_labels_parser' % current_skyline_app

    metric_name = None
    try:
        metric_elements = metric.split('{', 1)
        metric_name = metric_elements[0]
        metric_labels_str = metric_elements[1]
        metric_labels = metric_labels_str.rstrip('}')
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to parse metric %s - %s' % (
            function_str, str(metric), str(err)))
        return metric_dict

    labels = {}
    try:
        labels = parse_labels(metric_labels)
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to parse labels %s - %s' % (
            function_str, str(metric), str(err)))
        return metric_dict

    metric_dict['metric'] = metric_name
    metric_dict['labels'] = labels

    return metric_dict
