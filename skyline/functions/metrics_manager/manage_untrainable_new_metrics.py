"""
manage_untrainable_new_metrics.py
"""
import logging
import traceback
from time import time

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20230510 - Feature #4902: Prevent training on metrics newer than 7 days
def manage_untrainable_new_metrics(self, new_metrics):
    """

    Create and manage the metrics_manager.untrainable_new_metrics Redis hash

    :param self: the self object
    :param new_metrics: the new metrics
    :type self: object
    :type new_metrics: dict
    :return: managed_new_metrics_dict
    :rtype: dict

    """
    managed_new_metrics_dict = {'added': None, 'removed': None}

    now = int(time())
    logger.info('metrics_manager :: manage_untrainable_new_metrics - managing metrics_manager.untrainable_new_metrics')

    untrainable_new_metrics_dict = {}
    try:
        untrainable_new_metrics_dict = self.redis_conn_decoded.hgetall('metrics_manager.untrainable_new_metrics')
    except Exception as err:
        logger.error('error :: metrics_manager :: manage_untrainable_new_metrics :: failed to hgetall metrics_manager.untrainable_new_metrics - %s' % (
            err))
    untrainable_new_metrics = list(untrainable_new_metrics_dict.keys())
    logger.info('metrics_manager :: manage_untrainable_new_metrics - metrics_manager.untrainable_new_metrics has %s items' % str(len(untrainable_new_metrics)))

    new_metrics_to_add = {}
    for metric in list(new_metrics.keys()):
        if metric not in untrainable_new_metrics:
            new_metrics_to_add[metric] = str(now)
    if new_metrics_to_add:
        logger.info('metrics_manager :: manage_untrainable_new_metrics - adding %s new metrics to metrics_manager.untrainable_new_metrics' % str(len(new_metrics_to_add)))
        try:
            added = self.redis_conn_decoded.hset('metrics_manager.untrainable_new_metrics', mapping=new_metrics_to_add)
            logger.info('metrics_manager :: manage_untrainable_new_metrics - added %s new metrics to metrics_manager.untrainable_new_metrics' % str(added))
        except Exception as err:
            logger.error('error :: metrics_manager :: manage_untrainable_new_metrics :: failed to hset metrics_manager.untrainable_new_metrics with new_metrics_to_add - %s' % (
                err))
    managed_new_metrics_dict['added'] = len(new_metrics_to_add)

    metrics_to_remove = []
    for metric in untrainable_new_metrics:
        try:
            added_at = int(untrainable_new_metrics_dict[metric])
            if (added_at + (86400 * 7)) < now:
                 continue
            metrics_to_remove.append(metric)
        except Exception as err:
            logger.error('error :: metrics_manager :: manage_untrainable_new_metrics :: failed to determine added at from untrainable_new_metrics_dict - %s' % (
                err))
    logger.info('metrics_manager :: manage_untrainable_new_metrics - there are %s metrics to remove from metrics_manager.untrainable_new_metrics' % str(len(metrics_to_remove)))
    if metrics_to_remove:
        try:
            removed = self.redis_conn_decoded.hdel('metrics_manager.untrainable_new_metrics', *set(metrics_to_remove))
            logger.info('metrics_manager :: manage_untrainable_new_metrics - removed %s metrics from metrics_manager.untrainable_new_metrics' % str(removed))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: failed to hdel from metrics_manager.untrainable_new_metrics - %s' % (
                err))
    managed_new_metrics_dict['removed'] = len(metrics_to_remove)

    return managed_new_metrics_dict
