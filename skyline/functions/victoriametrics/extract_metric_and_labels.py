"""
extract_metric_and_labels.py
"""
import re


# @added 20241227 - Feature #5585: boundary - functions - tsdb_function
def extract_metric_and_labels(query_string):
    """
    Given a query string extract the metric from the metricsql string and return
    the metric name.

    :param current_skyline_app: the skyline app using this function
    :param query_string: the query string
    :type current_skyline_app: str
    :type query_string: str
    :return: metric
    :rtype: str

    """
    pattern = r"([a-zA-Z_][a-zA-Z0-9_]*\{.*?\})"
    match = re.search(pattern, query_string)
    if match:
        return match.group(1)
    else:
        return None

