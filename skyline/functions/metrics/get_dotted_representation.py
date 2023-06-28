"""
dotted_representation.py
"""
import logging
import traceback

from functions.prometheus.metric_name_labels_parser import metric_name_labels_parser


# @added 20220830 - Feature #4652: http_alerter - dotted_representation
#                   Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
def get_dotted_representation(current_skyline_app, metric):
    """
    Given a prometheus_metricname or dotted_metricname determine the translated
    metric name

    :param current_skyline_app: the Skyline app calling the function
    :param prometheus_metricname: the prometheus metric name
    :param dotted_metricname: the translated dotted prometheus metric name
    :type current_skyline_app: str
    :type prometheus_metricname: str
    :type dotted_metricname: str
    :return: translated_metric_name
    :rtype: str

    """
    function_str = 'functions.metrics.get_dotted_representation'
    dotted_representation = None
    metric_dict = {}
    try:
        metric_dict = metric_name_labels_parser(current_skyline_app, metric)
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: metric_name_labels_parser failed - %s' % (
            current_skyline_app, function_str, err))
    metric_elements = []
    if metric_dict:
        metric_elements.append(metric_dict['metric'])
        for label in metric_dict['labels']:
            metric_elements.append(label)
            metric_elements.append(metric_dict['labels'][label])
    if metric_elements:
        metric_elements = list(set(metric_elements))
        metric_elements.sort()
    for element in metric_elements:
        if '.' in element:
            element = element.replace('.', '\\.')
        if dotted_representation:
            dotted_representation = '%s.%s' % (dotted_representation, element)
        else:
            dotted_representation = '%s' % element

    return dotted_representation
