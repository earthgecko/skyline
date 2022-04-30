"""
non_derivative_metrics.py
"""
import logging
import traceback

import settings
from matched_or_regexed_in_list import matched_or_regexed_in_list

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20220414 - Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
#                   Task #3868: POC MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
def non_derivative_metrics(self, external_settings, unique_base_names):
    """
    Returns a list of base_names that are metrics that defined in
    NON_DERIVATIVE_NAMESPACES

    :param current_skyline_app: the app calling the function
    :param external_settings: the external_settings dict
    :param unique_base_names: the unique_base_names list
    :type current_skyline_app: str
    :type external_settings: dict
    :type unique_base_names: list
    :return: non_derivative_metrics_list
    :rtype: list

    """
    non_derivative_metrics_list = []

    non_derivative_metric_namespaces = []
    try:
        non_derivative_metric_namespaces = list(settings.NON_DERIVATIVE_MONOTONIC_METRICS)
    except AttributeError:
        non_derivative_metric_namespaces = []

    for config_id in list(external_settings):
        try:
            external_settings_non_derivative_metric_namespaces = list(external_settings[config_id]['non_derivative_namespaces'])
            if external_settings_non_derivative_metric_namespaces:
                non_derivative_metric_namespaces = non_derivative_metric_namespaces + external_settings_non_derivative_metric_namespaces
        except KeyError:
            continue

    for i_base_name in unique_base_names:
        if not non_derivative_metric_namespaces:
            break
        pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, i_base_name, non_derivative_metric_namespaces)
        if pattern_match:
            non_derivative_metrics_list.append(i_base_name)
        del metric_matched_by

    non_derivative_metrics_list = list(set(non_derivative_metrics_list))
    logger.info('metrics_manager :: determined %s non_derivative_metrics' % str(len(non_derivative_metrics_list)))

    if non_derivative_metrics_list:
        try:
            self.redis_conn.sadd('metrics_manager.non_derivative_metrics', *set(non_derivative_metrics_list))
            logger.info('metrics_manager :: added %s metrics to the metrics_manager.non_derivative_metrics set' % str(len(non_derivative_metrics_list)))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: failed to add multiple members to the metrics_manager.non_derivative_metrics Redis set - %s' % err)
    try:
        metrics_manager_non_derivative_metrics = list(self.redis_conn_decoded.smembers('metrics_manager.non_derivative_metrics'))
    except Exception as err:
        logger.error('error :: metrics_manager :: failed to get metrics_manager.non_derivative_metrics Redis set - %s' % str(err))
        metrics_manager_non_derivative_metrics = []
    non_derivative_metrics_to_remove = list(set(metrics_manager_non_derivative_metrics).difference(set(non_derivative_metrics_list)))
    if non_derivative_metrics_to_remove:
        try:
            self.redis_conn_decoded.srem('metrics_manager.non_derivative_metrics', *set(non_derivative_metrics_to_remove))
        except Exception as err:
            logger.error('metrics_manager :: failed to remove metrics from metrics_manager.non_derivative_metrics Redis set - %s' % str(err))
    logger.info('metrics_manager :: removed %s metrics to the metrics_manager.non_derivative_metrics set' % str(len(non_derivative_metrics_to_remove)))

    return non_derivative_metrics_list
