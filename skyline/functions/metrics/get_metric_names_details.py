"""
get_metric_names_details.py
"""
import logging
from time import time

from settings import FULL_NAMESPACE

# @added 20220823 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
# Return namespaces, labels, values and elements with the response
from prometheus_client.parser import _parse_labels as parse_labels


from matched_or_regexed_in_list import matched_or_regexed_in_list


# @added 20230203 - Feature #4838: functions.metrics.get_namespace_metric.count
#                   Feature #4842: webapp - api - get_flux_test_metrics
# Added in webapp on 20220823 for - Task #2732: Prometheus to Skyline
#                                   Branch #4300: prometheus
# Return namespaces, labels, values and elements with the response
# request_time pre returning namespaces, labels, values and elements - 0.2227020263671875
# request_time post returning namespaces, labels, values and elements - 0.33968615531921387
# request_time with cache_key - 0.03976845741271973
# @added 20230131 - Feature #4838: functions.metrics.get_namespace_metric.count
#                   Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
# Just added this comment for #4838 a cluster with 15420 metrics returning
# filtered namespaces, labels, values and elements - request_time
# 19.590527296066284 seconds.  On the cluster this has a high time
# complexity, because the cluster_data=true is calling it on all the
# cluster nodes.
def get_metric_names_details(current_skyline_app, metrics, strip_labels=[]):
    """
    Return a count of the metrics that belong to a namespace.
    The data is determined from the database and not from Redis keys because
    on clusters the database is source of truth and not the Redis keys which
    may only have metrics for that shard.

    :param current_skyline_app: the app calling the function
    :param metrics: a list of metrics
    :param strip_labels: a list of labels to strip
    :type current_skyline_app: str
    :type metrics: list
    :type strip_labels: list
    :return: metric_names_details
    :rtype: dict

    """
    function_str = 'get_metric_names_details'
    start = time()

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    current_logger.info('%s :: determining metric details %s metrics' % (
        function_str, str(len(metrics))))

    metric_names_details = {}
    namespaces_dict = {}
    labels_dict = {}
    values_dict = {}
    elements = []
    metrics_and_labels = {}

    for metric_name in metrics:
        base_name = str(metric_name)
        if metric_name.startswith(FULL_NAMESPACE):
            base_name = metric_name.replace(FULL_NAMESPACE, '', 1)
        if '_tenant_id' in base_name:
            metric_labels = None
            namespace = None
            try:
                metric_elements = base_name.split('{', 1)
                namespace = metric_elements[0]
                metric_labels_str = metric_elements[1]
                metric_labels = metric_labels_str.rstrip('}')
            except Exception as err:
                current_logger.error('error :: %s :: failed to parse metric %s - %s' % (
                    function_str, str(base_name), str(err)))
            metrics_and_labels[base_name] = {}
            metrics_and_labels[base_name]['namespaces'] = [namespace]
            metrics_and_labels[base_name]['labels'] = []
            metrics_and_labels[base_name]['values'] = []
            namespaces_dict_defined = None
            try:
                namespaces_dict_defined = namespaces_dict[namespace]
            except:
                namespaces_dict[namespace] = {}
                namespaces_dict[namespace]['labels'] = []
                namespaces_dict[namespace]['values'] = []
            labels = {}
            if metric_labels:
                try:
                    labels = parse_labels(metric_labels)
                except Exception as err:
                    current_logger.error('error :: %s :: failed to parse labels %s - %s' % (
                        function_str, str(base_name), str(err)))
            for label in list(labels.keys()):
                if label in strip_labels:
                    continue
                labels_dict_defined = None
                try:
                    labels_dict_defined = labels_dict[label]
                except:
                    labels_dict[label] = {}
                    labels_dict[label]['namespaces'] = []
                    labels_dict[label]['values'] = []
                value = labels[label]
                values_dict_defined = None
                try:
                    values_dict_defined = values_dict[value]
                except:
                    values_dict[value] = {}
                    values_dict[value]['namespaces'] = []
                    values_dict[value]['labels'] = []
                namespaces_dict[namespace]['labels'].append(label)
                namespaces_dict[namespace]['values'].append(value)
                metrics_and_labels[base_name]['labels'].append(label)
                metrics_and_labels[base_name]['values'].append(value)
                labels_dict[label]['namespaces'].append(namespace)
                labels_dict[label]['values'].append(value)
                values_dict[value]['namespaces'].append(namespace)
                values_dict[value]['labels'].append(label)
        else:
            namespace_elements = base_name.split('.')
            elements = elements + namespace_elements

        for namespace in list(namespaces_dict.keys()):
            namespaces_dict[namespace]['labels'] = list(set(namespaces_dict[namespace]['labels']))
            namespaces_dict[namespace]['values'] = list(set(namespaces_dict[namespace]['values']))
        for label in list(labels_dict.keys()):
            labels_dict[label]['namespaces'] = list(set(labels_dict[label]['namespaces']))
            labels_dict[label]['values'] = list(set(labels_dict[label]['values']))
        for value in list(values_dict.keys()):
            values_dict[value]['namespaces'] = list(set(values_dict[value]['namespaces']))
            values_dict[value]['labels'] = list(set(values_dict[value]['labels']))

    current_logger.info('%s :: determined metric details for %s metrics in %s seconds' % (
        function_str, str(len(metrics)), str(time() - start)))

    metric_names_details = {
        'metrics': metrics,
        # @added 20220823 - Task #2732: Prometheus to Skyline
        #                   Branch #4300: prometheus
        # Return namespaces, labels, values and elements with the response
        'namespace_elements': elements,
        'namespaces': namespaces_dict,
        'labels': labels_dict,
        'values': values_dict,
        'metrics_and_labels': metrics_and_labels,
        'time_complexity': 'high',
    }
    return metric_names_details
