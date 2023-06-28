"""
get_labelled_metric_dict.py
"""
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from functions.prometheus.metric_name_labels_parser import metric_name_labels_parser


# @added 20220808 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
def get_labelled_metric_dict(current_skyline_app, metric):
    """
    Returns the metric dict for a labelled_metrics name

    :param current_skyline_app: the app calling the function
    :param metric: the labelled_metrics name to lookup the base_name for.
    :type current_skyline_app: str
    :type metric: str
    :return: metric_dict
    :rtype: dict

    """
    metric_dict = {}
    base_name = str(metric)
    if metric.startswith('labelled_metrics.'):
        metric_id_str = metric.replace('labelled_metrics.', '', 1)
        try:
            metric_id = int(metric_id_str)
        except:
            metric_id = 0
        if metric_id:
            try:
                base_name = get_base_name_from_metric_id(current_skyline_app, int(metric_id))
            except:
                base_name = str(metric)
    try:
        metric_dict = metric_name_labels_parser(current_skyline_app, base_name)
    except:
        metric_dict = {}

    return metric_dict
