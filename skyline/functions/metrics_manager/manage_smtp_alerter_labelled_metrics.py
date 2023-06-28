"""
smtp_alerter_labelled_metrics.py
"""
import logging
import traceback
from time import time

import settings
from external_alert_configs import get_external_alert_configs
from matched_or_regexed_in_list import matched_or_regexed_in_list

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20220809 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
def manage_smtp_alerter_labelled_metrics(self, ids_with_metric_names):
    """

    Create and manage the metrics_manager.smtp_alerter_labelled_metrics Redis
    hash of metrics that are smtp alert enabled.

    :param self: the self object
    :type self: object
    :return: smtp_alerter_labelled_metrics
    :rtype: dict

    """
    smtp_alert_metrics = []
    logger.info('metrics_manager :: manage_smtp_alerter_labelled_metrics :: managing metrics_manager.smtp_alerter_labelled_metrics Redis hash')

    timestamp_str = str(int(time()))
    unique_labelled_metrics = []
    try:
        unique_labelled_metrics = list(self.redis_conn_decoded.smembers('labelled_metrics.unique_labelled_metrics'))
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: metrics_manager :: manage_smtp_alerter_labelled_metrics :: smembers labelled_metrics.unique_labelled_metrics failed - %s' % (
            err))
    if not unique_labelled_metrics:
        logger.info('metrics_manager :: manage_smtp_alerter_labelled_metrics :: no labelled_metrics found')
        return smtp_alert_metrics
    known_smtp_alerter_labelled_metrics = {}
    try:
        known_smtp_alerter_labelled_metrics = self.redis_conn_decoded.hgetall('metrics_manager.smtp_alerter_labelled_metrics')
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: metrics_manager :: manage_smtp_alerter_labelled_metrics :: hgetall on metrics_manager.smtp_alerter_labelled_metrics failed - %s' % (
            err))

    external_alerts = {}
    external_from_cache = None
    internal_alerts = {}
    internal_from_cache = None
    all_alerts = list(settings.ALERTS)
    all_from_cache = None
    if settings.EXTERNAL_ALERTS:
        try:
            external_alerts, external_from_cache, internal_alerts, internal_from_cache, all_alerts, all_from_cache = get_external_alert_configs(skyline_app)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: manage_smtp_alerter_labelled_metrics :: could not determine external alert configs')
        logger.info('metrics_manager :: manage_smtp_alerter_labelled_metrics :: retrieved %s external_alerts configurations from_cache %s, %s internal_alerts from_cache %s and %s all_alerts from_cache %s' % (
            str(len(external_alerts)), str(external_from_cache),
            str(len(internal_alerts)), str(internal_from_cache),
            str(len(all_alerts)), str(all_from_cache)))
        if not all_alerts:
            logger.error('error :: metrics_manager :: manage_smtp_alerter_labelled_metrics :: all_alerts is not set, so creating from settings.ALERTS')
            all_alerts = list(settings.ALERTS)

    errors = []
    warns = []
    smtp_alert_patterns = [item[0] for item in all_alerts if item[1] == 'smtp']
    for labelled_metric in unique_labelled_metrics:
        metric_id = 0
        try:
            metric_id_str = labelled_metric.replace('labelled_metrics.', '', 1)
            metric_id = int(float(metric_id_str))
        except Exception as err:
            errors.append([labelled_metric, 'failed to determine metric id', str(err)])
            continue
        base_name = None
        try:
            base_name = ids_with_metric_names[metric_id]
        except Exception as err:
            warns.append([labelled_metric, 'failed to determine base_name from ids_with_metric_names', str(err)])
            continue
        try:
            pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, smtp_alert_patterns)
            try:
                del metric_matched_by
            except:
                pass
        except Exception as err:
            errors.append([labelled_metric, 'pattern_match', str(err)])
            pattern_match = False
        if pattern_match:
            smtp_alert_metrics.append(labelled_metric)
    if errors:
        logger.error('error :: metrics_manager :: manage_smtp_alerter_labelled_metrics :: %s errors encountered in determining smtp_alert_metrics, last entry sample: %s' % (
            str(len(errors)), str(errors[-1])))
        redis_set = 'metrics_manager.manage_smtp_alerter_labelled_metrics.errors.%s' % timestamp_str
        logger.warning('warning :: metrics_manager :: manage_smtp_alerter_labelled_metrics :: %s errors reported being added to Redis set %s' % (
            str(len(errors)), redis_set))
        for error in errors:
            try:
                self.redis_conn_decoded.sadd(redis_set, str(error))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: manage_smtp_alerter_labelled_metrics :: sadd on %s - %s' % (
                    redis_set, err))
        try:
            self.redis_conn_decoded.expire(redis_set, 3600)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: manage_smtp_alerter_labelled_metrics :: expire on %s - %s' % (
                redis_set, err))
        errors = []

    if warns:
        logger.warning('warning :: metrics_manager :: manage_smtp_alerter_labelled_metrics :: %s warnings encountered in determining smtp_alert_metrics, last entry sample: %s' % (
            str(len(warns)), str(warns[-1])))
        redis_set = 'metrics_manager.manage_smtp_alerter_labelled_metrics.warnings.%s' % timestamp_str
        logger.warning('warning :: metrics_manager :: manage_smtp_alerter_labelled_metrics :: %s warnings reported being added to Redis set %s' % (
            str(len(warns)), redis_set))
        for warn in warns:
            try:
                self.redis_conn_decoded.sadd(redis_set, str(warn))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: manage_smtp_alerter_labelled_metrics :: sadd on %s - %s' % (
                    redis_set, err))
        try:
            self.redis_conn_decoded.expire(redis_set, 3600)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: manage_smtp_alerter_labelled_metrics :: expire on %s - %s' % (
                redis_set, err))
        warns = []

    metrics_to_remove = []
    metrics_to_add = []
    if known_smtp_alerter_labelled_metrics and smtp_alert_metrics:
        smtp_alert_metrics_set = set(smtp_alert_metrics)
        known_smtp_alerter_labelled_metrics_set = set(list(known_smtp_alerter_labelled_metrics.keys()))
        set_difference = known_smtp_alerter_labelled_metrics_set.difference(smtp_alert_metrics_set)
        metrics_to_remove = list(set_difference)
        set_difference = smtp_alert_metrics_set.difference(known_smtp_alerter_labelled_metrics_set)
        metrics_to_add = list(set_difference)
    if not known_smtp_alerter_labelled_metrics:
        metrics_to_add = list(set(smtp_alert_metrics))
    if metrics_to_remove:
        logger.info('metrics_manager :: manage_smtp_alerter_labelled_metrics :: %s metrics to remove from metrics_manager.smtp_alerter_labelled_metrics' % str(len(metrics_to_remove)))
        try:
            removed_metrics = self.redis_conn_decoded.hdel('metrics_manager.smtp_alerter_labelled_metrics', *metrics_to_remove)
            logger.info('metrics_manager :: manage_smtp_alerter_labelled_metrics :: removed %s metrics from metrics_manager.smtp_alerter_labelled_metrics' % str(removed_metrics))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: manage_smtp_alerter_labelled_metrics :: hdel on metrics_manager.smtp_alerter_labelled_metrics failed - %s' % (
                err))
    if metrics_to_add:
        metrics_to_add_dict = {}
        for labelled_metric in metrics_to_add:
            metrics_to_add_dict[labelled_metric] = timestamp_str
        logger.info('metrics_manager :: manage_smtp_alerter_labelled_metrics :: %s metrics to add to metrics_manager.smtp_alerter_labelled_metrics' % str(len(metrics_to_add)))
        try:
            added_metrics = self.redis_conn_decoded.hset('metrics_manager.smtp_alerter_labelled_metrics', mapping=metrics_to_add_dict)
            logger.info('metrics_manager :: manage_smtp_alerter_labelled_metrics :: added %s metrics to metrics_manager.smtp_alerter_labelled_metrics' % str(added_metrics))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: manage_smtp_alerter_labelled_metrics :: hdel on metrics_manager.smtp_alerter_labelled_metrics failed - %s' % (
                err))

    logger.info('metrics_manager :: manage_smtp_alerter_labelled_metrics :: all done')
    return smtp_alert_metrics
