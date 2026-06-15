"""
Parse a Prometheus metric and determine metric name and labels
"""
import logging

# @modified 20250617 - Task #5627: v5.0.0 update dependencies
# _parse_labels changed to parse_labels in v0.22.0
try:
    from prometheus_client.parser import _parse_labels as parse_labels
except:
    from prometheus_client.parser import parse_labels


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

    # @added 20260427 - Task #5713: Test CentOS Stream 10
    # parse_labels hangs if the labels do not end in ", occurs in while loop
    # and something is obviously not handled promptly.
    # https://github.com/prometheus/client_python/blob/482656c8c07b78668adb5f3171dfe71ccc2396b2/prometheus_client/parser.py#L61
    if not metric_labels.endswith('"'):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: invalid metric_labels to parse, does not end with a ", metric_labels: %s, metric: %s' % (
            function_str, str(metric_labels), str(metric)))
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
