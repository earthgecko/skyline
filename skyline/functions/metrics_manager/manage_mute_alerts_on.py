"""
manage_mute_alerts_on.py
"""
import logging
from os import uname
from time import time

from settings import REMOTE_SKYLINE_INSTANCES
from slack_functions import slack_post_message

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20230605 - Feature #4932: mute_alerts_on
def manage_mute_alerts_on(self):
    """

    Manage the metrics_manager.mute_alerts_on Redis hash

    :param self: the self object
    :type self: object
    :return: removed_count
    :rtype: int

    """
    removed = 0
    now = int(time())
    logger.info('metrics_manager :: manage_mute_alerts_on - managing metrics_manager.mute_alerts_on')
    mute_alerts_on_dict = {}
    try:
        mute_alerts_on_dict = self.redis_conn_decoded.hgetall('metrics_manager.mute_alerts_on')
    except Exception as err:
        logger.error('error :: metrics_manager :: manage_mute_alerts_on :: failed to hgetall metrics_manager.mute_alerts_on - %s' % (
            err))
    mute_alerts_on = list(mute_alerts_on_dict.keys())
    logger.info('metrics_manager :: manage_mute_alerts_on :: metrics_manager.mute_alerts_on has %s items' % str(len(mute_alerts_on)))

    metrics_to_remove = []
    feedback_labelled_metric_ids_to_remove = []
    for metric in mute_alerts_on:
        timestamp = int(mute_alerts_on_dict[metric])
        if now >= timestamp:
            metrics_to_remove.append(metric)
            del mute_alerts_on_dict[metric]
            if metric.startswith('labelled_metrics.'):
                metric_id_str = metric.split('.')[-1]
                feedback_labelled_metric_ids_to_remove.append(metric_id_str)
    if metrics_to_remove:
        logger.info('metrics_manager :: manage_mute_alerts_on - removing %s metrics from metrics_manager.mute_alerts_on' % str(len(metrics_to_remove)))
        try:
            removed = self.redis_conn_decoded.hdel('metrics_manager.mute_alerts_on', *set(metrics_to_remove))
            logger.info('metrics_manager :: manage_mute_alerts_on - removed %s metrics from metrics_manager.mute_alerts_on' % str(removed))
        except Exception as err:
            logger.error('error :: metrics_manager :: manage_mute_alerts_on :: failed to hdel metrics_manager.mute_alerts_on with metrics_to_remove - %s' % (
                err))
    else:
        logger.info('metrics_manager :: manage_mute_alerts_on - no metrics to remove from metrics_manager.mute_alerts_on')
    if feedback_labelled_metric_ids_to_remove:
        try:
            self.redis_conn.srem('metrics_manager.feedback.labelled_metric_ids', *set(feedback_labelled_metric_ids_to_remove))
        except Exception as err:
            logger.error('error :: metrics_manager :: failed to srem from metrics_manager.feedback.labelled_metric_ids Redis set - %s' % err)

    if removed:
        try:
            alert_slack_channel = None
            slack_message = '*Skyline - NOTICE* - alerting muting has expired and been *removed* from %s metrics' % (
                str(removed))
            slack_message = '%s - on %s' % (slack_message, str(uname()[1]))
            slack_post = slack_post_message(skyline_app, alert_slack_channel, None, slack_message)
            logger.info('metrics_manager :: manage_mute_alerts_on :: posted notice to slack - %s' % slack_message)
        except Exception as err:
            logger.error('error :: metrics_manager :: manage_mute_alerts_on :: slack_post_message failed - %s' % (
                err))

    return mute_alerts_on_dict, removed
