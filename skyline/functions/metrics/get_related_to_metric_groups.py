"""
Get metric_groups that a metric exists in
"""
import logging
import traceback

from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.database.queries.related_to_metric_groups import related_to_metric_groups
from functions.metrics.get_related_metrics import get_related_metrics


def get_related_to_metric_groups(current_skyline_app, base_name, metric_id):
    """
    Returns a dict of the metric_groups that a metric is related to.

    :param current_skyline_app: the app calling the function
    :param base_name: the base_name of the metric to determine the latest
        anomaly for.  Can be None if metric_id is passed as a positive int
    :param metric_id: the metric id if base_name is passed as None.
    :type current_skyline_app: str
    :type base_name: str
    :type metric_id: int
    :return: related_to_metric_groups_dict
    :rtype: dict

    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    related_to_metrics_dict = {}

    if not base_name and metric_id:
        base_name = None
        try:
            base_name = get_base_name_from_metric_id(current_skyline_app, metric_id)
        except Exception as err:
            current_logger.error('error :: get_related_to_metric_groups :: base_name_from_metric_id failed to determine base_name from metric_id: %s - %s' % (
                str(metric_id), str(err)))

    if not metric_id and base_name:
        metric_id = 0
        try:
            metric_id = get_metric_id_from_base_name(current_skyline_app, base_name)
        except Exception as err:
            current_logger.error('error :: get_related_to_metric_groups :: get_metric_id_from_base_name failed to determine metric_id from base_name: %s - %s' % (
                str(base_name), str(err)))

    if not metric_id:
        current_logger.error('error :: get_related_to_metric_groups :: get_metric_id_from_base_name failed to determine metric_id from base_name: %s' % (
            str(base_name)))
        return related_to_metrics_dict

    related_to_metrics_dict[base_name] = {}
    related_to_metrics_dict[base_name]['metric_id'] = metric_id
    related_to_metrics_dict[base_name]['related_to_metrics'] = {}

    try:
        related_to_metric_groups_dict = related_to_metric_groups(current_skyline_app, base_name, metric_id)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_related_to_metric_groups :: related_to_metric_groups failed - %s' % str(err))

    if related_to_metric_groups_dict:
        for related_to_metric in list(related_to_metric_groups_dict['related_to_metrics'].keys()):
            related_to_metric_id = related_to_metric_groups_dict['related_to_metrics'][related_to_metric]['related_to_metric_id']
            # related_to_metrics_dict['related_to_metrics'][related_to_metric]['metric'] = related_to_metric
            cluster_data = False
            full_details = True
            related_metrics = {}
            try:
                related_metrics = get_related_metrics(current_skyline_app, cluster_data, full_details, related_to_metric, metric_id=related_to_metric_id)
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_related_to_metric_groups :: related_to_metric_groups failed - %s' % str(err))
            if related_metrics:
                related_to_metrics_dict[base_name]['related_to_metrics'][related_to_metric] = related_metrics['related_metrics'][base_name]

    # Sort related_to_metric_groups_dict by confidence then by avg_coefficient
    confidence_related_to_metrics_list = []
    confidence_related_to_metrics = {}
    related_to_metric_keys = []
    for related_to_metric in list(related_to_metrics_dict[base_name]['related_to_metrics'].keys()):
        related_to_metric_data = []
        if not related_to_metric_keys:
            related_to_metric_keys.append('metric')
            for index, key in enumerate(list(related_to_metrics_dict[base_name]['related_to_metrics'][related_to_metric].keys())):
                related_to_metric_keys.append(key)
                if key == 'confidence':
                    confidence_index = index + 1
                if key == 'avg_coefficient':
                    avg_coefficient_index = index + 1
        related_to_metric_data.append(related_to_metric)
        for key in related_to_metric_keys:
            if key == 'metric':
                continue
            related_to_metric_data.append(related_to_metrics_dict[base_name]['related_to_metrics'][related_to_metric][key])
        confidence_related_to_metrics_list.append(related_to_metric_data)
    sorted_confidence_related_to_metrics_list = sorted(confidence_related_to_metrics_list, key=lambda x: (x[confidence_index], x[avg_coefficient_index]), reverse=True)
    for item in sorted_confidence_related_to_metrics_list:
        related_to_metric = item[0]
        confidence_related_to_metrics[related_to_metric] = {}
        for index, key in enumerate(related_to_metric_keys):
            if key == 'metric':
                continue
            if key == 'metric_id':
                confidence_related_to_metrics[related_to_metric][key] = item[index]
            if key == 'confidence':
                confidence_related_to_metrics[related_to_metric][key] = item[index]
        for index, key in enumerate(related_to_metric_keys):
            if key in ['metric', 'metric_id', 'confidence']:
                continue
            confidence_related_to_metrics[related_to_metric][key] = item[index]
        original_metric_id = confidence_related_to_metrics[related_to_metric]['metric_id']
        original_related_metric_id = confidence_related_to_metrics[related_to_metric]['related_to_metric_id']
        confidence_related_to_metrics[related_to_metric]['metric_id'] = original_related_metric_id
        confidence_related_to_metrics[related_to_metric]['related_to_metric_id'] = original_metric_id

    related_to_metrics_dict[base_name]['related_to_metrics'] = confidence_related_to_metrics

    return related_to_metrics_dict
