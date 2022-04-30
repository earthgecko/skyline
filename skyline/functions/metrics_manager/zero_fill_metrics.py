"""
zero_fill_metrics
"""
import logging
import traceback

import settings
from matched_or_regexed_in_list import matched_or_regexed_in_list

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20220406 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
#                   Feature #4520: settings - ZERO_FILL_NAMESPACES
def zero_fill_metrics(self, external_settings, unique_base_names):
    """
    Returns a list of base_names that are metrics that defined in
    LAST_KNOWN_VALUE_NAMESPACES

    :param current_skyline_app: the app calling the function
    :param external_settings: the external_settings dict
    :param unique_base_names: the unique_base_names list
    :type current_skyline_app: str
    :type external_settings: dict
    :type unique_base_names: list
    :return: zero_fill_metrics_list
    :rtype: list

    """
    zero_fill_metrics_list = []

    zero_fill_metric_namespaces = []
    try:
        zero_fill_metric_namespaces = list(settings.ZERO_FILL_NAMESPACES)
    except AttributeError:
        zero_fill_metric_namespaces = []

    for config_id in list(external_settings):
        try:
            external_settings_zero_fill_metric_namespaces = list(external_settings[config_id]['zero_fill_namespaces'])
            if external_settings_zero_fill_metric_namespaces:
                zero_fill_metric_namespaces = zero_fill_metric_namespaces + external_settings_zero_fill_metric_namespaces
        except KeyError:
            continue

    for i_base_name in unique_base_names:
        if not zero_fill_metric_namespaces:
            break
        pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, i_base_name, zero_fill_metric_namespaces)
        if pattern_match:
            zero_fill_metrics_list.append(i_base_name)
        del metric_matched_by

    zero_fill_metrics_list = list(set(zero_fill_metrics_list))
    logger.info('metrics_manager :: determined %s zero_fill_metrics' % str(len(zero_fill_metrics_list)))

    if zero_fill_metrics_list:
        try:
            self.redis_conn.sadd('metrics_manager.zero_fill_metrics', *set(zero_fill_metrics_list))
            logger.info('metrics_manager :: added %s metrics to the metrics_manager.zero_fill_metrics set' % str(len(zero_fill_metrics_list)))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: failed to add multiple members to the metrics_manager.zero_fill_metrics Redis set - %s' % err)
    try:
        metrics_manager_zero_fill_metrics = list(self.redis_conn_decoded.smembers('metrics_manager.zero_fill_metrics'))
    except Exception as err:
        logger.error('error :: metrics_manager :: failed to get metrics_manager.zero_fill_metrics Redis set - %s' % str(err))
        metrics_manager_zero_fill_metrics = []
    zero_fill_metrics_to_remove = list(set(metrics_manager_zero_fill_metrics).difference(set(zero_fill_metrics_list)))
    if zero_fill_metrics_to_remove:
        try:
            self.redis_conn_decoded.srem('metrics_manager.zero_fill_metrics', *set(zero_fill_metrics_to_remove))
        except Exception as err:
            logger.error('metrics_manager :: failed to remove metrics from metrics_manager.zero_fill_metrics Redis set - %s' % str(err))
    logger.info('metrics_manager :: removed %s metrics to the metrics_manager.zero_fill_metrics set' % str(len(zero_fill_metrics_to_remove)))

    return zero_fill_metrics_list
