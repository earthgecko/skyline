"""
get_namespace_metric_count.py
"""
import logging

from functions.database.queries.get_all_db_metric_names import get_all_db_metric_names
from functions.database.queries.get_all_active_db_metric_names import get_all_active_db_metric_names
from matched_or_regexed_in_list import matched_or_regexed_in_list


# @added 20230131 - Feature #4838: functions.metrics.get_namespace_metric.count
#                   Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
def get_namespace_metric_count(current_skyline_app, namespace, primary_namespace=False):
    """
    Return a count of the metrics that belong to a namespace.
    The data is determined from the database and not from Redis keys because
    on clusters the database is source of truth and not the Redis keys which
    may only have metrics for that shard.

    :param current_skyline_app: the app calling the function
    :param namespace: the namespace or regex of the namespace
    :param primary_namespace: whether this is a primary namespace
    :type current_skyline_app: str
    :type namespace: str
    :type primary_namespace: bool
    :return: namespace_metric_count
    :rtype: dict

    """
    function_str = 'get_namespace_metric_count'

    total_metrics = 0
    active_metrics_count = 0
    inactive_metrics_count = 0
    namespace_metric_count = {
        'namespace': namespace,
        'primary_namespace': primary_namespace,
    }

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    current_logger.info('%s :: determining metric count for namespace: %s' % (
        function_str, namespace))

    namespaces = [namespace]
    labelled_metrics_namespace = None

    # @added 20230202 - Feature #4838: functions.metrics.get_namespace_metric.count
    # Declare a primary_namespace
    if primary_namespace:
        # This handles specific internal use cases
        if namespace.startswith('org_'):
            labelled_metrics_namespace = '.*_tenant_id="%s",.*' % str(namespace)
            namespace_str = '^%s\\..*' % str(namespace)
            namespaces = [namespace_str, labelled_metrics_namespace]
    namespace_metric_count['namespaces'] = namespaces

    all_metric_names_with_ids = {}
    with_ids = True
    try:
        all_metric_names, all_metric_names_with_ids = get_all_db_metric_names(current_skyline_app, with_ids)
    except Exception as err:
        current_logger.error('error :: %s :: get_all_db_metric_names failed - %s' % (
            function_str, err))

    active_metric_names_with_ids = {}
    try:
        active_metric_names, active_metric_names_with_ids = get_all_active_db_metric_names(current_skyline_app, with_ids)
    except Exception as err:
        current_logger.error('error :: %s :: get_all_active_db_metric_names failed - %s' % (
            function_str, err))

    if all_metric_names_with_ids and active_metric_names_with_ids:
        all_metric_ids_set = set(list(all_metric_names_with_ids.values()))
        active_metric_ids_set = set(list(active_metric_names_with_ids.values()))
        set_difference = all_metric_ids_set.difference(active_metric_ids_set)
        inactive_metric_ids = list(set_difference)

    pattern_match_errors = []
    for base_name in list(all_metric_names_with_ids.keys()):
        pattern_match = False
        try:
            pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, base_name, namespaces)
            del metric_matched_by
        except Exception as err:
            pattern_match_errors.append([base_name, namespaces, err])
        if not pattern_match:
            continue
        total_metrics += 1
        metric_id = all_metric_names_with_ids[base_name]
        if metric_id in inactive_metric_ids:
            inactive_metrics_count += 1
            continue
        active_metrics_count += 1

    if pattern_match_errors:
        current_logger.error('error :: %s :: creating inactive_metric list, matched_or_regexed_in_list reports %s errors, last 3 reported: %s' % (
            function_str, str(len(pattern_match_errors)),
            str(pattern_match_errors[-3:])))

    namespace_metric_count['active_metrics'] = active_metrics_count
    namespace_metric_count['inactive_metrics'] = inactive_metrics_count
    namespace_metric_count['total_metrics'] = total_metrics

    return namespace_metric_count
