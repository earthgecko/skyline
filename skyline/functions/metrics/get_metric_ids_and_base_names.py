"""
get_metric_ids_and_base_names.py
"""
import logging
import traceback

from functions.metrics.get_base_names_and_metric_ids import get_base_names_and_metric_ids


def get_metric_ids_and_base_names(current_skyline_app):
    """
    Returns a dict of metric ids and their base_name.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: ids_with_base_names
    :rtype: dict

    """

    base_names_with_ids = {}
    ids_with_base_names = {}

    function_str = 'functions.metrics.get_base_name_from_metric_id'

    try:
        base_names_with_ids = get_base_names_and_metric_ids(current_skyline_app)
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: get_base_names_and_metric_ids failed - %s' % (
            current_skyline_app, function_str, str(err)))
    if base_names_with_ids:
# @modified 20240131 - Task #5248: Optimise ionosphere_functions.get_related
# Yak shaving. Using items() rather than a for loop is 0.0004088878631591797
# seconds faster.  loop took 0.019259214401245117 seconds, items() took
# 0.018850326538085938 seconds
#        for base_name in list(base_names_with_ids.keys()):
#            metric_id = int(str(base_names_with_ids[base_name]))
#            ids_with_base_names[metric_id] = base_name
        for base_name, metric_id in base_names_with_ids.items():
            ids_with_base_names[int(metric_id)] = base_name

    return ids_with_base_names
