"""
manage_skyline_feedback_metrics.py
"""
import logging
from time import time

from matched_or_regexed_in_list import matched_or_regexed_in_list

from settings import SKYLINE_FEEDBACK_NAMESPACES, DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20240214 - Feature #5272: analyzer - load_shedding - SKYLINE_FEEDBACK_NAMESPACES
def manage_skyline_feedback_metrics(self, unique_base_names):
    """

    Create and manage the metrics_manager.skyline_feedback_metrics Redis set

    :param self: the self object
    :type self: object
    :return: skyline_feedback_metrics
    :rtype: set

    """
    start = time()
    skyline_feedback_metrics = []

    logger.info('metrics_manager :: skyline_feedback_metrics - determining feedback metrics from %s unique_base_names' % (
        str(len(unique_base_names))))

    errors = []
    for base_name in set(unique_base_names):
        try:
            feedback_metric = False
            metric_namespace_elements = base_name.split('.')
            for to_skip in SKYLINE_FEEDBACK_NAMESPACES:
                if to_skip in base_name:
                    feedback_metric = True
                    break
                to_skip_namespace_elements = to_skip.split('.')
                elements_matched = set(metric_namespace_elements) & set(to_skip_namespace_elements)
                if len(elements_matched) == len(to_skip_namespace_elements):
                    feedback_metric = True
                    break
            if feedback_metric:
                pattern_match = False
                try:
                    pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES)
                    del metric_matched_by
                except Exception as err:
                    errors.append(['matched_or_regexed_in_list DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES', base_name, err])
                    pattern_match = False
                if pattern_match:
                    feedback_metric = False
            if feedback_metric:
                skyline_feedback_metrics.append(base_name)
        except Exception as err:
            errors.append([base_name, err])

    if skyline_feedback_metrics:
        logger.info('metrics_manager :: skyline_feedback_metrics - adding %s feedback metrics to metrics_manager.analyzer.skyline_feedback_metrics' % (
            str(len(skyline_feedback_metrics))))
        try:
            self.redis_conn_decoded.sadd('metrics_manager.analyzer.skyline_feedback_metrics', *set(skyline_feedback_metrics))
            logger.info('metrics_manager :: skyline_feedback_metrics - added %s feedback metrics to metrics_manager.analyzer.skyline_feedback_metrics' % (
                str(len(skyline_feedback_metrics))))
        except Exception as err:
            logger.error('error :: metrics_manager :: skyline_feedback_metrics :: failed to sadd metrics_manager.analyzer.skyline_feedback_metrics, err: %s' % (
                err))

    # @added 20240303 - Feature #5272: analyzer - load_shedding - SKYLINE_FEEDBACK_NAMESPACES
    # Remove metrics as well
    feedback_metrics_to_remove = []
    if skyline_feedback_metrics:
        skyline_feedback_metrics_redis_set = []
        try:
            skyline_feedback_metrics_redis_set = self.redis_conn_decoded.smembers('metrics_manager.analyzer.skyline_feedback_metrics')
            logger.info('metrics_manager :: skyline_feedback_metrics - got %s feedback metrics from metrics_manager.analyzer.skyline_feedback_metrics' % (
                str(len(skyline_feedback_metrics_redis_set))))
        except Exception as err:
            logger.error('error :: metrics_manager :: skyline_feedback_metrics :: failed to smembers metrics_manager.analyzer.skyline_feedback_metrics, err: %s' % (
                err))
        skyline_feedback_metrics_set = set(skyline_feedback_metrics)
        if isinstance(skyline_feedback_metrics_redis_set, set):
            logger.info('metrics_manager :: skyline_feedback_metrics - comparing skyline_feedback_metrics_set with %s items and skyline_feedback_metrics_redis_set with %s items' % (
                str(len(skyline_feedback_metrics_set)), str(len(skyline_feedback_metrics_redis_set))))
            try:
                set_difference = skyline_feedback_metrics_redis_set.difference(skyline_feedback_metrics_set)
                feedback_metrics_to_remove = list(set_difference)
            except Exception as err:
                logger.error('error :: metrics_manager :: skyline_feedback_metrics :: failed to difference between skyline_feedback_metrics_set and skyline_feedback_metrics_redis_set, err: %s' % (
                    err))
        logger.info('metrics_manager :: skyline_feedback_metrics - there are %s feedback metrics to remove from metrics_manager.analyzer.skyline_feedback_metrics' % (
            str(len(feedback_metrics_to_remove))))
        if feedback_metrics_to_remove:
            logger.info('metrics_manager :: skyline_feedback_metrics - sample feedback_metrics_to_remove[0]: %s, sample feedback_metrics_to_remove[-1]: %s' % (
                str(feedback_metrics_to_remove[0]), str(feedback_metrics_to_remove[-1])))
            try:
                removed = self.redis_conn_decoded.srem('metrics_manager.analyzer.skyline_feedback_metrics', *set(feedback_metrics_to_remove))
                logger.info('metrics_manager :: skyline_feedback_metrics - removed %s feedback metrics for metrics_manager.analyzer.skyline_feedback_metrics' % (
                    str(removed)))
            except Exception as err:
                logger.error('error :: metrics_manager :: skyline_feedback_metrics :: failed to srem metrics_manager.analyzer.skyline_feedback_metrics, err: %s' % (
                    err))
            try:
                self.redis_conn_decoded.sadd('metrics_manager.analyzer.skyline_feedback_metrics.removed', *set(feedback_metrics_to_remove))
                self.redis_conn_decoded.expire('metrics_manager.analyzer.skyline_feedback_metrics.removed', 300)
                logger.info('metrics_manager :: skyline_feedback_metrics - added %s feedback metrics to metrics_manager.analyzer.skyline_feedback_metrics.removed and set expire to 300' % (
                    str(len(feedback_metrics_to_remove))))
            except Exception as err:
                logger.error('error :: metrics_manager :: skyline_feedback_metrics :: failed to sadd to metrics_manager.analyzer.skyline_feedback_metrics.removed, err: %s' % (
                    err))

    logger.info('metrics_manager :: skyline_feedback_metrics took %s seconds' % (
        str(time() - start)))

    return len(skyline_feedback_metrics)