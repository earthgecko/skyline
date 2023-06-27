"""
skyline_prometheus_metric_names.py
"""
import logging
from os import uname

this_host = str(uname()[1])


# @added 20220722 - Branch #4300: prometheus
#                   Task #2732: Prometheus to Skyline
# Add a metric to be exposed to prometheus
def skyline_prometheus_metric_names(current_skyline_app):
    """
    Return a dict of the Skyline Prometheus metric names.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: skyline_prometheus_metric_names_dict
    :rtype: dict

    """

    skyline_prometheus_metric_names_dict = {}
    app_processes = ['labelled_metrics', 'worker', 'listen', 'fetcher']
    skyline_prometheus_metric_names_dict['app_processes'] = app_processes

    common_metric_namespaces = {
        'skyline_logged_errors': {
            'namespace_elements': ['logged_errors'],
            'type': 'gauge',
            'help': 'The number errors logged. KPI',
        },
        'skyline_run_time': {
            'namespace_elements': [
                'run_time', 'avg_runtime', 'metrics_manager_run_time',
            ],
            'type': 'gauge',
            'help': 'app run time in seconds. KPI',
        },
        'skyline_timings': {
            'namespace_elements': [
                'features_calculation_time', 'time_to_fetch', 'duration',
                'projected', ['labelled_metrics', '3sigma_timings'],
                ['labelled_metrics', 'downsample_timings'],
                ['labelled_metrics', 'mad_timings'],
                ['labelled_metrics', 'redis_timings'],
                ['labelled_metrics', 'stationary_timing'],
                ['labelled_metrics', 'redis_full_duration_timings'],
            ],
            'type': 'gauge',
            'help': 'The operation run time in seconds.',
        },
        'skyline_algorithm_breakdown_timing': {
            'namespace_elements': [
                ['algorithm_breakdown', 'timing', 'total_time'],
                ['algorithm_breakdown', 'timing', 'median_time']
            ],
            'type': 'gauge',
            'help': 'The algorithm timings in seconds.',
        },
        'skyline_algorithm_breakdown_times_run': {
            'namespace_elements': [['algorithm_breakdown', 'timing', 'times_run']],
            'type': 'gauge',
            'help': 'The number of times the algorithm was run.',
        },
        'skyline_exceptions': {
            'namespace_elements': ['exceptions'],
            'type': 'gauge',
            'help': 'The number of metrics that reported the exception class.',
        },
        'skyline_flux_listen_discarded': {
            'namespace_elements': [['flux', 'listen', 'discarded']],
            'type': 'gauge',
            'help': 'The number of metrics discarded due to the reported reason.',
        },
        'skyline_flux_listen': {
            'namespace_elements': [['flux', 'listen']],
            'type': 'gauge',
            'help': 'The number of metrics reporter for the various flux listen processes.',
        },
        'skyline_flux_worker_discarded': {
            'namespace_elements': [['flux', 'worker', 'discarded']],
            'type': 'gauge',
            'help': 'The number of metrics discarded by the flux worker processes.',
        },
        'skyline_flux_worker_sent_to_graphite': {
            'namespace_elements': [['flux', 'worker', 'metrics_sent_to_graphite']],
            'type': 'gauge',
            'help': 'The number of metrics sent to Graphite by the flux worker processes.',
        },
        'skyline_flux_worker_httpMetricDataQueue': {
            'namespace_elements': [['flux', 'worker', 'httpMetricDataQueue']],
            'type': 'gauge',
            'help': 'The number of attempts in the httpMetricDataQueue reported by the primary flux worker processes.',
        },
        'skyline_flux_worker': {
            'namespace_elements': [['flux', 'worker']],
            'type': 'gauge',
            'help': 'The number of metrics operated on by the flux worker processes.',
        },
        'skyline_flux_aggregator': {
            'namespace_elements': [['flux', 'aggregator']],
            'type': 'gauge',
            'help': 'The metrics for the flux aggregator processes.',
        },
        'skyline_flux_populate_metric_worker': {
            'namespace_elements': [['flux', 'populate_metric_worker']],
            'type': 'gauge',
            'help': 'The metrics for the flux populate_metric_worker processes.',
        },
        'skyline_anomalies': {
            'namespace_elements': ['total_anomalies'],
            'type': 'gauge',
            'help': 'The number of anomalies detected by the app.',
        },
        'skyline_analysed': {
            'namespace_elements': ['total_analyzed'],
            'type': 'gauge',
            'help': 'The number of metrics analysed by the app.',
        },
        'skyline_total_metrics': {
            'namespace_elements': ['total_metrics'],
            'type': 'gauge',
            'help': 'The total number of metrics known to the app.',
        },
        'skyline_anomaly_breakdown': {
            'namespace_elements': ['anomaly_breakdown'],
            'type': 'gauge',
            'help': 'The number of metrics that triggered the algorithm per app.',
        },
        'skyline_analyzer_labelled_metrics': {
            'namespace_elements': [['analyzer', 'labelled_metrics']],
            'type': 'gauge',
            'help': 'The analyzer labelled_metrics metrics.',
        },
        'skyline_vista_worker_fetcher_metrics_json': {
            'namespace_elements': [['worker', 'fetcher', 'metrics', 'json']],
            'type': 'gauge',
            'help': 'The number of metrics in vista worker fetch json.',
        },
        'skyline_vista_fetcher': {
            'namespace_elements': [['vista', 'fetcher']],
            'type': 'gauge',
            'help': 'The number of metrics operated on by vista fetcher.',
        },
        'skyline_vista_worker': {
            'namespace_elements': [['vista', 'worker']],
            'type': 'gauge',
            'help': 'The number of metrics operated on by vista worker.',
        },
        'skyline_metrics_sparsity': {
            'namespace_elements': ['metrics_sparsity'],
            'type': 'gauge',
            'help': 'The sparsity of metric population.',
        },
        'skyline_http_alerter_queue': {
            'namespace_elements': [['http_alerter', 'queue']],
            'type': 'gauge',
            'help': 'The number of alert in the http_alerter queues.',
        },
        'skyline_low_priority_metrics': {
            'namespace_elements': ['low_priority_metrics'],
            'type': 'gauge',
            'help': 'The metrics relating to low_priority_metrics.',
        },
        'skyline_mirage_checks': {
            'namespace_elements': [['mirage', 'checks']],
            'type': 'gauge',
            'help': 'The metrics relating to mirage checks.',
        },
        'skyline_snab_checks': {
            'namespace_elements': [['snab', 'checks']],
            'type': 'gauge',
            'help': 'The metrics relating to snab checks.',
        },
        'skyline_horizon_worker': {
            'namespace_elements': [['horizon', 'worker']],
            'type': 'gauge',
            'help': 'The metrics relating to the horizon worker.',
        },
        'skyline_luminosity_classify_metrics': {
            'namespace_elements': [['luminosity', 'classify_metrics']],
            'type': 'gauge',
            'help': 'luminosity classification metrics.',
        },
        'skyline_luminosity': {
            'namespace_elements': ['luminosity'],
            'type': 'gauge',
            'help': 'luminosity metrics.',
        },
        'skyline_sent_to_mirage': {
            'namespace_elements': ['sent_to_mirage'],
            'type': 'gauge',
            'help': 'The number of metrics sent to mirage by the app.',
        },
        'skyline_sent_to_ionosphere': {
            'namespace_elements': ['sent_to_ionosphere'],
            'type': 'gauge',
            'help': 'The number of metrics sent to ionosphere by the app.',
        },
        'skyline_sent_to_panorama': {
            'namespace_elements': ['sent_to_panorama'],
            'type': 'gauge',
            'help': 'The number of metrics sent to panorama by the app.',
        },
        'skyline_analyzer_mirage_periodic_checks': {
            'namespace_elements': [['analyzer', 'mirage_periodic_checks']],
            'type': 'gauge',
            'help': 'The number of metrics sent to mirage for periodic checks.',
        },
        'skyline_analyzer_mirage_metrics': {
            'namespace_elements': [['analyzer', 'mirage_metrics']],
            'type': 'gauge',
            'help': 'The number of metrics classified as mirage metrics.',
        },
        'skyline_analyzer_ionosphere_metrics': {
            'namespace_elements': [['analyzer', 'ionosphere_metrics']],
            'type': 'gauge',
            'help': 'The number of metrics classified as ionosphere metrics.',
        },
        'skyline_thunder_alerts_sent': {
            'namespace_elements': [['thunder', 'alerts', 'sent']],
            'type': 'gauge',
            'help': 'The number of alerts sent out by thunder.',
        },
        'skyline_ionosphere': {
            'namespace_elements': [
                ['ionosphere', 'not_anomalous'],
                ['ionosphere', 'training_metrics'],
                ['ionosphere', 'fps_checked'],
                ['ionosphere', 'layers_checked'],
            ],
            'type': 'gauge',
            'help': 'The number of alerts sent out by thunder.',
        },
    }
    skyline_prometheus_metric_names_dict['common_metric_namespaces'] = common_metric_namespaces
    return skyline_prometheus_metric_names_dict
