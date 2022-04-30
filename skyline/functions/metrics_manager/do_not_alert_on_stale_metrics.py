"""
do_not_alert_on_stale_metrics
"""
import logging
import traceback

import settings
from matched_or_regexed_in_list import matched_or_regexed_in_list

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20220410 - Task #4514: Integrate opentelemetry
#                   Feature #4516: flux - opentelemetry traces
def do_not_alert_on_stale_metrics(self, external_settings, unique_base_names):
    """
    Returns a list of base_names that are metrics that defined in
    DO_NOT_ALERT_ON_STALE_METRICS

    :param current_skyline_app: the app calling the function
    :param external_settings: the external_settings dict
    :param unique_base_names: the unique_base_names list
    :type current_skyline_app: str
    :type external_settings: dict
    :type unique_base_names: list
    :return: do_not_alert_on_stale_metrics_list
    :rtype: list

    """
    do_not_alert_on_stale_metrics_list = []

    do_not_alert_on_stale_metric_namespaces = []
    try:
        do_not_alert_on_stale_metric_namespaces = list(settings.DO_NOT_ALERT_ON_STALE_METRICS)
    except AttributeError:
        do_not_alert_on_stale_metric_namespaces = []

    for config_id in list(external_settings.keys()):
        external_settings_namespace = None
        alert_on_stale_metrics = False
        try:
            alert_on_stale_metrics = external_settings[config_id]['alert_on_stale_metrics']['enabled']
            if alert_on_stale_metrics:
                external_settings_namespace = external_settings[config_id]['namespace']
        except KeyError:
            continue
        if external_settings_namespace:
            do_not_alert_on_stale_metric_namespaces.append(external_settings_namespace)

    do_not_alert_on_stale_metric_namespaces = list(set(do_not_alert_on_stale_metric_namespaces))
    for i_base_name in unique_base_names:
        if not do_not_alert_on_stale_metric_namespaces:
            break
        pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, i_base_name, do_not_alert_on_stale_metric_namespaces)
        if pattern_match:
            do_not_alert_on_stale_metrics_list.append(i_base_name)
        del metric_matched_by

    do_not_alert_on_stale_metrics_list = list(set(do_not_alert_on_stale_metrics_list))
    logger.info('metrics_manager :: determined %s do_not_alert_on_stale_metrics' % str(len(do_not_alert_on_stale_metrics_list)))

    if do_not_alert_on_stale_metrics_list:
        try:
            self.redis_conn.sadd('metrics_manager.do_not_alert_on_stale_metrics', *set(do_not_alert_on_stale_metrics_list))
            logger.info('metrics_manager :: added %s metrics to the metrics_manager.do_not_alert_on_stale_metrics set' % str(len(do_not_alert_on_stale_metrics_list)))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: failed to add multiple members to the metrics_manager.do_not_alert_on_stale_metrics Redis set - %s' % err)
    try:
        metrics_manager_do_not_alert_on_stale_metrics = list(self.redis_conn_decoded.smembers('metrics_manager.do_not_alert_on_stale_metrics'))
    except Exception as err:
        logger.error('error :: metrics_manager :: failed to get metrics_manager.do_not_alert_on_stale_metrics Redis set - %s' % str(err))
        metrics_manager_do_not_alert_on_stale_metrics = []
    do_not_alert_on_stale_metrics_to_remove = list(set(metrics_manager_do_not_alert_on_stale_metrics).difference(set(do_not_alert_on_stale_metrics_list)))
    if do_not_alert_on_stale_metrics_to_remove:
        try:
            self.redis_conn_decoded.srem('metrics_manager.do_not_alert_on_stale_metrics', *set(do_not_alert_on_stale_metrics_to_remove))
        except Exception as err:
            logger.error('metrics_manager :: failed to remove metrics from metrics_manager.do_not_alert_on_stale_metrics Redis set - %s' % str(err))
    logger.info('metrics_manager :: removed %s metrics to the metrics_manager.do_not_alert_on_stale_metrics set' % str(len(do_not_alert_on_stale_metrics_to_remove)))

    return do_not_alert_on_stale_metrics_list
