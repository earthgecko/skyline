"""
skip_metric.py
"""


def skip_metric(base_name, skip_list, do_not_skip_list):
    """
    Returns a boolean whether to skip the base_name or not.

    :param base_name: the base_name of the metric to determine the latest
        anomaly for.  Can be None if metric_id is passed as a positive int
    :type base_name: str
    :type skip_list: list
    :type do_not_skip_list: list
    :return: skip
    :rtype: boolean
    """
    skip = False
    metric_namespace_elements = base_name.split('.')
    for to_skip in skip_list:
        if to_skip in base_name:
            skip = True
            break
        to_skip_namespace_elements = to_skip.split('.')
        elements_matched = set(metric_namespace_elements) & set(to_skip_namespace_elements)
        if len(elements_matched) == len(to_skip_namespace_elements):
            skip = True
            break
    if skip:
        for do_not_skip in do_not_skip_list:
            if do_not_skip in base_name:
                skip = False
                break
            do_not_skip_namespace_elements = do_not_skip.split('.')
            elements_matched = set(metric_namespace_elements) & set(do_not_skip_namespace_elements)
            if len(elements_matched) == len(do_not_skip_namespace_elements):
                skip = False
                break
    return skip
