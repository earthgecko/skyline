"""
generate_prometheus_metrics.py
"""
import logging
from os import uname
import copy
from ast import literal_eval
from time import time

from prometheus_client import Gauge

from skyline_functions import get_redis_conn_decoded

this_host = str(uname()[1])


# @added 20220724 - Branch #4300: prometheus
#                   Task #2732: Prometheus to Skyline
# Generate prometheus metrics
def generate_prometheus_metrics(current_skyline_app):
    """
    Generate the metrics to be scrape by Prometheus from the
    skyline.expose.prometheus.metrics Redis hash.
    """

    function_str = 'functions.prometheus.generate_prometheus_metrics'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    current_timestamp = int(time())
    current_aligned_timestamp = int(current_timestamp // 60 * 60)

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_redis_conn_decoded failed - %s' % (
            function_str, err))

    submitted_metrics = {}
    try:
        submitted_metrics = redis_conn_decoded.hgetall('skyline.expose.prometheus.metrics')
        redis_conn_decoded.delete('skyline.expose.prometheus.metrics')
    except Exception as err:
        current_logger.error('error :: %s :: failed to get Redis hash skyline.expose.prometheus.metrics - %s' % (
            function_str, err))

    generated_metrics = 0
    dropped_old_datapoints = 0
    metrics = []
    for key in list(submitted_metrics.keys()):
        # split 2 dots because the timestamp are as 1658817858.685164
        key_list = key.split('.', 2)
        # Drop old data
        if int(key_list[0]) < (current_aligned_timestamp - 60):
            continue
        metrics.append(key_list[2])
    metrics = list(set(metrics))
    current_logger.info('%s :: %s metrics to generate' % (
        function_str, str(len(metrics))))

    metrics_to_submit = {}
    for metric in metrics:
        metric_data = {}
        metric_values = []
        for key in list(submitted_metrics.keys()):
            key_list = key.split('.', 2)
            if key_list[2] != metric:
                continue
            # Drop old data
            if int(key_list[0]) < (current_aligned_timestamp - 60):
                dropped_old_datapoints += 1
                continue
            current_data = {}
            try:
                current_data = literal_eval(submitted_metrics[key])
                if not metric_data and current_data:
                    metric_data = copy.deepcopy(current_data)
            except Exception as err:
                current_logger.error('error :: %s :: failed to literal_eval %s: %s - %s' % (
                    function_str, key, str(submitted_metrics[key]), err))
                continue

            try:
                metric_values.append(float(current_data['value']))
            except Exception as err:
                current_logger.error('error :: %s :: failed to determine metric value from %s: %s - %s' % (
                    function_str, key, str(submitted_metrics[key]), err))
                continue

        if not metric_values:
            current_logger.error('error :: %s :: failed to determine metric values for %s' % (
                function_str, metric))
            continue
        current_data['metric_values'] = metric_values
        try:
            metric_name = current_data['metric']
            metric_help = current_data['help']
            metric_type = current_data['type']
            metric_labels = current_data['labels']
        except Exception as err:
            current_logger.error('error :: %s :: failed to determine variables from current_data for %s - %s' % (
                function_str, metric, err))
            continue
        try:
            metrics_to_submit[metric_name][metric] = current_data
        except:
            metrics_to_submit[metric_name] = {}
            metrics_to_submit[metric_name][metric] = current_data

    for metric_name in list(set(list(metrics_to_submit.keys()))):
        metric_name_labels = []
        for metric in list(metrics_to_submit[metric_name].keys()):
            metric_type = metrics_to_submit[metric_name][metric]['type']
            metric_help = metrics_to_submit[metric_name][metric]['help']
            for label in list(metrics_to_submit[metric_name][metric]['labels'].keys()):
                metric_name_labels.append(label)
        metric_name_labels = list(set(metric_name_labels))

        if metric_type == 'gauge':
            try:
                g = Gauge(metric_name, metric_help, metric_name_labels)
            except Exception as err:
                if 'Duplicated timeseries in CollectorRegistry' in str(err):
                    continue
                if 'Incorrect label names' in str(err):
                    current_logger.warning('warning :: %s :: failed to create gauge for metric_name: %s, metric_name_labels: %s, metric_labels: %s - %s' % (
                        function_str, metric_name, str(metric_name_labels),
                        str(metric_labels), err))
                    continue
                current_logger.error('error :: %s :: failed to create gauge for metric_name: %s, metric_name_labels: %s, metric_labels: %s - %s' % (
                    function_str, metric_name, str(metric_name_labels),
                    str(metric_labels), err))
                continue
            for metric in list(metrics_to_submit[metric_name].keys()):
                try:
                    metric_values = metrics_to_submit[metric_name][metric]['metric_values']
                    metric_labels = metrics_to_submit[metric_name][metric]['labels']
                    g.labels(**metric_labels).set(sum(metric_values))
                    generated_metrics += 1
                except Exception as err:
                    if 'Incorrect label names' in str(err):
                        current_logger.warning('warning :: %s :: failed to create gauge for metric_name: %s, metric_name_labels: %s, metric_labels: %s - %s' % (
                            function_str, metric_name, str(metric_name_labels),
                            str(metric_labels), err))
                        continue
                    current_logger.error('error :: %s :: failed to record gauge for metric_name: %s, metric: %s, metric_name_labels: %s, metric_labels: %s - %s' % (
                        function_str, metric_name, metric, str(metric_name_labels),
                        str(metric_labels), err))
                    continue

    current_logger.info('%s :: generated %s metrics and dropped %s old data points' % (
        function_str, str(generated_metrics), str(dropped_old_datapoints)))
    return
