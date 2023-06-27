"""
skyline_prometheus_metric.py
"""
import logging
from os import uname

from skyline_functions import get_redis_conn_decoded
from functions.prometheus.skyline_prometheus_metric_names import skyline_prometheus_metric_names

this_host = str(uname()[1])

process_namespaces = [
    'skyline_horizon_worker', 'skyline_flux_listen', 'skyline_flux_worker',
    'skyline_luminosity', 'skyline_vista_worker',
    'skyline_analyzer_labelled_metrics']
kpi_metrics = [
    'run_time', 'avg_runtime', 'metrics_manager_run_time', 'logged_errors'
    'Stale', 'total_anomalies',
]
LOCAL_DEBUG = False


def get_metric_namespace(common_metric_namespaces, data):
    """
    Determine the metric namespace
    """
    elements_used_in_namespace = []
    metric_namespace = None
    data['metric'] = metric_namespace
    for common_metric_namespace in list(common_metric_namespaces.keys()):
        if metric_namespace:
            break
        namespace_elements = common_metric_namespaces[common_metric_namespace]['namespace_elements']
        for namespace_element in namespace_elements:
            if isinstance(namespace_element, list):
                element_count = len(namespace_element)
                elements_found = 0
                for n_element in namespace_element:
                    for element in data['metric_elements']:
                        if element == n_element:
                            elements_found += 1
                if elements_found == element_count:
                    metric_namespace = common_metric_namespace
                    for n_element in namespace_element:
                        if n_element in metric_namespace:
                            elements_used_in_namespace.append(n_element)
                    break
                if metric_namespace:
                    break
            else:
                if namespace_element in data['metric_elements']:
                    metric_namespace = common_metric_namespace
                    if namespace_element in metric_namespace:
                        elements_used_in_namespace.append(namespace_element)
                    break
        if metric_namespace:
            data['metric'] = metric_namespace
            data['help'] = common_metric_namespaces[common_metric_namespace]['help']
            data['type'] = common_metric_namespaces[common_metric_namespace]['type']
            data['elements_used_in_namespace'] = elements_used_in_namespace
            break
    return data


def get_metric_name(app_processes, data):
    """
    Determine the metric name with labels
    """
    data['labels'] = {}
    prometheus_metric = '%s' % data['metric']
    used_indices = []
    for index, element in enumerate(data['metric_elements']):
        if index == 0:
            used_indices.append(index)
        if index == 1 and element == data['skyline_app']:
            data['labels']['app'] = str(element)
            used_indices.append(index)
        if element == this_host:
            data['labels']['host'] = str(element)
            used_indices.append(index)

    if 'labelled_metrics' in data['metric_elements']:
        data['labels']['process'] = 'labelled_metrics'

    # The logged_errors metrics are submitted by the thunder app for all apps
    # so replace the app label
    if 'logged_errors' in data['metric_elements']:
        origin_app = data['metric_elements'][1]
        data['labels']['app'] = origin_app

    key = None
    for index, element in enumerate(data['metric_elements']):
        if index in used_indices:
            continue
        if element in app_processes:
            data['labels']['process'] = str(element)
            continue
        if not key:
            key = str(element)
            continue
        if key:
            if key == 'anomaly_breakdown':
                key = 'algorithm'
            data['labels'][key] = str(element)
            key = None
    kpi = False
    if data['graphite_metric'].startswith('skyline.mirage.'):
        if 'checks' in data['metric_elements']:
            if data['graphite_metric'].endswith('.checks.pending'):
                kpi = True
            if kpi:
                data['labels']['kpi'] = 'true'
            else:
                data['labels']['kpi'] = 'false'
    for kpi_metric in kpi_metrics:
        if kpi_metric in data['metric_elements']:
            kpi = True
            break

    if kpi:
        data['labels']['kpi'] = 'true'

    last_label = None
    if key:
        # if key not in data['elements_used_in_namespace']:
        #     last_label = str(key)
        last_label = str(key)

    if 'process' not in list(data['labels'].keys()):
        data['labels']['process'] = 'parent'

    if data['metric'] == 'skyline_analyzer_labelled_metrics':
        try:
            del data['labels']['http_alerter']
        except:
            pass

    # The logged_errors metrics are submitted by the thunder app for all apps
    # so replace the app label
    if 'logged_errors' in data['metric_elements']:
        data['labels']['app'] = origin_app
        try:
            del data['labels'][origin_app]
        except:
            pass

    ordered_labels = {}
    for label in sorted(list(data['labels'].keys())):
        ordered_labels[label] = data['labels'][label]
    data['labels'] = ordered_labels

    for index, key in enumerate(list(data['labels'].keys())):
        if key == last_label:
            continue
        if index == 0:
            join_str = '{'
        else:
            join_str = ','
        prometheus_metric = '%s%s%s="%s"' % (prometheus_metric, join_str, str(key), str(data['labels'][key]))
    if isinstance(last_label, str):
        if data['metric'] == 'skyline_run_time':
            data['labels']['seconds'] = last_label
            prometheus_metric = '%s,seconds="%s"' % (prometheus_metric, str(last_label))
        if data['metric'] == 'skyline_timings':
            data['labels']['seconds'] = last_label
            prometheus_metric = '%s,seconds="%s"' % (prometheus_metric, str(last_label))
        if data['metric'] in process_namespaces:
            data['labels']['metric'] = last_label
            prometheus_metric = '%s,metric="%s"' % (prometheus_metric, str(last_label))
    if data['metric'] == 'skyline_vista_worker_fetcher_metrics_json':
        if 'vista="fetcher",metrics="json"' in prometheus_metric:
            try:
                del data['labels']['vista']
                del data['labels']['metrics']
            except:
                pass
            data['labels']['metric'] = 'fetcher_metrics_in_json'
            prometheus_metric = prometheus_metric.replace('vista="fetcher",metrics="json"', 'metric="fetcher_metrics_in_json"')
    prometheus_metric = '%s}' % prometheus_metric
    data['full_metric_name'] = prometheus_metric
    return data


# @added 20220722 - Branch #4300: prometheus
#                   Task #2732: Prometheus to Skyline
# Add a metric to be exposed to prometheus
def skyline_prometheus_metric(current_skyline_app, metric, timestamp, value):
    """
    Determine the prometheus metric name and add to the
    skyline.expose.prometheus.metrics Redis hash

    :param current_skyline_app: the app calling the function
    :param metric: the Skyline Graphite metric
    :param timestamp: the metric timestamp
    :param value: the metric value
    :type current_skyline_app: str
    :type metric: str
    :type timestamp: int
    :type value: float
    :return: prometheus_metric
    :rtype: str

    """

    function_str = '%s :: functions.prometheus.skyline_prometheus_metric' % current_skyline_app
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    data = {}
    data['graphite_metric'] = metric
    data['timestamp'] = timestamp
    data['skyline_app'] = current_skyline_app
    data['value'] = float(value)
    data['metric_elements'] = metric.split('.')

    skyline_prometheus_metric_names_dict = {}
    common_metric_namespaces = {}
    app_processes = []
    try:
        skyline_prometheus_metric_names_dict = skyline_prometheus_metric_names(current_skyline_app)
        common_metric_namespaces = skyline_prometheus_metric_names_dict['common_metric_namespaces']
        app_processes = skyline_prometheus_metric_names_dict['app_processes']
    except:
        pass

    try:
        data = get_metric_namespace(common_metric_namespaces, data)
    except Exception as err:
        current_logger.error('error :: get_metric_namespace failed with: %s - %s' % (str(data), err))

    if not data['metric']:
        return None

    try:
        data = get_metric_name(app_processes, data)
    except Exception as err:
        current_logger.error('error :: get_metric_name failed with: %s - %s' % (str(data), err))

    prometheus_metric = data['full_metric_name']
    if LOCAL_DEBUG:
        current_logger.debug('debug :: %s :: submitting %s' % (
            function_str, str(prometheus_metric)))

    if prometheus_metric == '':
        return None

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
        key = '%s.%s' % (str(timestamp), prometheus_metric)
        redis_conn_decoded.hset('skyline.expose.prometheus.metrics', key, str(data))
    except Exception as err:
        current_logger.error('error :: %s :: hset failed on skyline.expose.prometheus.metrics: %s - %s' % (
            function_str, str(data), err))

    return prometheus_metric
