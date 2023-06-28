import logging
import traceback

from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id


# @added 20220722 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
def get_base_name_from_labelled_metrics_name(current_skyline_app, labelled_metrics_name):
    """
    Returns base_name for a labelled_metrics name

    :param current_skyline_app: the app calling the function
    :param labelled_metrics_name: the labelled_metrics name to lookup the base_name for.
    :type current_skyline_app: str
    :type labelled_metrics_name: str
    :return: base_name
    :rtype: str

    """

    base_name = None
    if labelled_metrics_name.startswith('labelled_metrics.'):
        metric_id_str = labelled_metrics_name.replace('labelled_metrics.', '', 1)
        try:
            base_name = get_base_name_from_metric_id(current_skyline_app, int(metric_id_str))
        except:
            pass

    return base_name
