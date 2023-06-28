"""
get_metrics_in_namespace.py
# THIS IS A WORK IN PROGRESS - DOES NOT HAVE DESIRED EFFECT ON ALL METRICS
"""
import logging
import traceback
from ast import literal_eval

from functions.database.queries.get_all_active_db_metric_names import get_all_active_db_metric_names
from functions.prometheus.metric_name_labels_parser import metric_name_labels_parser

function_str = 'functions.metrics.get_metrics_in_namespace'


# @added 20221211 - Task #4762: Group metrics based on names and labels
# THIS IS A WORK IN PROGRESS - DOES NOT HAVE DESIRED EFFECT ON ALL METRICS
def increment_count(element_counts, element):
    try:
        element_counts[element] += 1
    except:
        element_counts[element] = 1
    return element_counts


def get_metric_elements(current_skyline_app, metrics):
    metric_elements = []
    metric_namespace_elements = {}
    element_counts = {}
    try:
        for metric in metrics:
            metric_dict = {}
            metric_name_elements_list = []
            if '_tenant_id="' in metric:
                try:
                    metric_dict = metric_name_labels_parser(current_skyline_app, metric)
                except:
                    pass
                metric_namespace = metric_dict['metric']
                element_counts = increment_count(element_counts, metric_namespace)
                metric_elements.append(metric_namespace)
                for label in list(metric_dict['labels']):
                    metric_elements.append(label)
                    metric_name_elements_list.append(label)
                    element_counts = increment_count(element_counts, label)
                    metric_elements.append(metric_dict['labels'][label])
                    metric_name_elements_list.append(metric_dict['labels'][label])
                    element_counts = increment_count(element_counts, metric_dict['labels'][label])
            else:
                metric_name_elements = metric.split('.')
                metric_namespace = metric_name_elements[0]
                element_counts = increment_count(element_counts, metric_namespace)
                metric_name_elements_list.append(metric_namespace)
                for i in metric_name_elements[1:]:
                    metric_name_elements_list.append(i)
                    element_counts = increment_count(element_counts, i)
            try:
                metric_namespace_elements_list = metric_namespace_elements[metric_namespace]
            except:
                metric_namespace_elements_list = []
            metric_namespace_elements_list = list(set(metric_namespace_elements_list + metric_name_elements_list))
            metric_namespace_elements[metric_namespace] = metric_namespace_elements_list
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_metric_elements failed - %s' % (
            function_str, str(err)))

    unique_metric_elements = list(set(metric_elements))
    element_counts = dict(sorted(element_counts.items(), key=lambda item: item[1], reverse=True))

    return metric_elements, metric_namespace_elements, element_counts


def get_significant_elements(current_skyline_app, metric, element_counts):
    significant_elements = {}
    metric_dict = {}
    metric_elements = []
    try:
        if '_tenant_id="' in metric:
            try:
                metric_dict = metric_name_labels_parser(current_skyline_app, metric)
            except:
                pass
            metric_namespace = metric_dict['metric']
            metric_elements.append(metric_namespace)
            for label in list(metric_dict['labels']):
                metric_elements.append(label)
                metric_elements.append(metric_dict['labels'][label])
        else:
            metric_elements = metric.split('.')
            metric_dict['metric'] = metric_elements[0]
            metric_dict['labels'] = {}
            for index, element in enumerate(metric_elements):
                if index == (len(metric_elements) - 1):
                    break
                metric_dict['labels'][element] = metric_elements[index + 1]
        for element in metric_elements:
            significant_elements[element] = int(element_counts[element])
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_metric_elements failed - %s' % (
            function_str, str(err)))

    significant_elements = dict(sorted(significant_elements.items(), key=lambda item: item[1], reverse=True))
    return significant_elements, metric_dict, metric_elements


def get_label_values(current_skyline_app, metrics_with_required_elements):
    label_values = {}
    try:
        for i_metric in metrics_with_required_elements:
            i_metric_dict = {}
            metric_elements = []
            if '_tenant_id="' in i_metric:
                try:
                    i_metric_dict = metric_name_labels_parser(current_skyline_app, i_metric)
                except:
                    pass
                metric_namespace = i_metric_dict['metric']
                metric_elements.append(metric_namespace)
                for label in list(i_metric_dict['labels']):
                    try:
                        values_list = label_values[label]
                    except:
                        values_list = []
                    values_list.append(i_metric_dict['labels'][label])
                    label_values[label] = list(set(values_list))
            else:
                metric_elements = i_metric.split('.')
                for index, element in enumerate(metric_elements):
                    if index == (len(metric_elements) - 1):
                        break
                    try:
                        values_list = label_values[element]
                    except:
                        values_list = []
                    values_list.append(metric_elements[index + 1])
                    label_values[element] = list(set(values_list))
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_label_values failed - %s' % (
            function_str, str(err)))
    return label_values


def get_unique_labels(current_skyline_app, label_values, metric_dict, metrics_with_required_elements):
    unique_labels = []
    significant_labels = {}
    common_labels = {}
    try:
        for label in list(label_values.keys()):
            if len(label_values[label]) > 1:
                significant_labels[label] = list(label_values[label])
            else:
                common_labels[label] = label_values[label][0]
        significant_label_counts = {}
        labels_used = {}
        for label in list(significant_labels.keys()):
            if label not in metric_dict['labels']:
                continue
            label_value = metric_dict['labels'][label]
            labels_used[label] = label_value
            number_of_metrics = 0
            for i_metric in metrics_with_required_elements:
                if label_value not in i_metric:
                    continue
                for i_label in list(labels_used.keys()):
                    if labels_used[i_label] not in i_metric:
                        break
                    number_of_metrics += 1
            if number_of_metrics:
                significant_label_counts[str(list(labels_used.keys()))] = number_of_metrics
        significant_label_counts = dict(sorted(significant_label_counts.items(), key=lambda item: item[1]))
        if significant_label_counts:
            unique_labels = literal_eval(list(significant_label_counts.keys())[0])
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_unique_labels failed - %s' % (
            function_str, str(err)))

    return unique_labels


# @added 20221211 - Task #4762: Group metrics based on names and labels
# THIS IS A WORK IN PROGRESS - DOES NOT HAVE DESIRED EFFECT ON ALL METRICS
def get_metrics_in_namespace(current_skyline_app, base_name):
    """
    Return a list of the metrics that belong to the same namespace.

    THIS IS A WORK IN PROGRESS - DOES NOT HAVE DESIRED EFFECT ON ALL METRICS

    :param current_skyline_app: the app calling the function
    :param base_name: the metric base_name
    :type current_skyline_app: str
    :type base_name: str
    :return: related_metrics
    :rtype: list

    """

    # THIS IS A WORK IN PROGRESS - DOES NOT HAVE DESIRED EFFECT ON ALL METRICS

    metrics_in_namespace = []

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    current_logger.info('%s :: determining metrics in namespace of %s' % (
        function_str, base_name))

    metrics = []
    try:
        metrics = get_all_active_db_metric_names(current_skyline_app)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_all_active_db_metric_names failed - %s' % (
            function_str, str(err)))
        return metrics_in_namespace

    metric_elements = []
    metric_namespace_elements = {}
    element_counts = {}
    try:
        metric_elements, metric_namespace_elements, element_counts = get_metric_elements(current_skyline_app, metrics)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_metric_elements failed - %s' % (
            function_str, str(err)))
        return metrics_in_namespace

    significant_elements = {}
    metric_dict = {}
    metric_elements = []
    try:
        significant_elements, metric_dict, metric_elements = get_significant_elements(current_skyline_app, base_name, element_counts)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_significant_elements failed - %s' % (
            function_str, str(err)))
        return metrics_in_namespace

    metrics_with_required_elements = []
    try:
        least_element = list(significant_elements.keys())[-1]
        most_element = list(significant_elements.keys())[0]
        if '_tenant_id="' not in base_name:
            most_element = '%s.' % metric_elements[0]
            for key in sorted(list(significant_elements.keys()), reverse=True):
                if significant_elements[key] > 1:
                    least_element = '.%s' % key
                    break
        for i_metric in metrics:
            if least_element in i_metric and most_element in i_metric:
                metrics_with_required_elements.append(i_metric)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to determine metrics_with_required_elements - %s' % (
            function_str, str(err)))
        return metrics_in_namespace

    label_values = {}
    try:
        label_values = get_label_values(current_skyline_app, metrics_with_required_elements)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_label_values failed - %s' % (
            function_str, str(err)))
        return metrics_in_namespace

    unique_labels = []
    try:
        unique_labels = get_unique_labels(current_skyline_app, label_values, metric_dict, metrics_with_required_elements)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_unique_labels failed - %s' % (
            function_str, str(err)))
        return metrics_in_namespace

    try:
        for i_metric in metrics:
            related = True
            for label in unique_labels:
                if label not in i_metric:
                    related = False
                    break
                if metric_dict['labels'][label] not in i_metric:
                    related = False
                    break
            if related:
                metrics_in_namespace.append(i_metric)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: iteratiing metrics failed - %s' % (
            function_str, str(err)))
        return metrics_in_namespace
    if not metrics_in_namespace or len(metrics_in_namespace) == 1:
        if metrics_with_required_elements:
            metrics_in_namespace = metrics_with_required_elements
    if '_tenant_id="' not in base_name:
        metrics_in_namespace = [metric for metric in metrics_in_namespace if metric.startswith(most_element)]

    return metrics_in_namespace
