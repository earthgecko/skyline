"""
prune_expose_prometheus_metrics.py
"""
import logging
from time import time

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20220727 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
def prune_expose_prometheus_metrics(self):
    """

    Prune any entries in the skyline.expose.prometheus.metrics Redis hash that
    are older than 5 minutes.  This ensures that if the metrics stop being
    scraped by Prometheus/victoriametrics, et al, the Redis hash does not
    continue to grow.

    :param self: the self object
    :type self: object
    :return: pruned_count
    :rtype: int

    """

    pruned_count = 0
    function_str = 'metrics_manager :: functions.metrics_manager.prune_expose_prometheus_metrics'

    current_timestamp = int(time())
    current_aligned_timestamp = int(current_timestamp // 60 * 60)

    logger.info('%s :: pruning entries older than 5 minutes from skyline.expose.prometheus.metrics Redis hash' % function_str)

    submitted_metrics = {}
    try:
        submitted_metrics = self.redis_conn_decoded.hgetall('skyline.expose.prometheus.metrics')
    except Exception as err:
        logger.error('error :: %s :: failed to get Redis hash skyline.expose.prometheus.metrics - %s' % (
            function_str, err))

    for key in list(submitted_metrics.keys()):
        # split 2 dots because the timestamp are as 1658817858.685164
        key_list = key.split('.', 2)
        # Drop old data
        if int(key_list[0]) < (current_aligned_timestamp - 300):
            try:
                self.redis_conn_decoded.hdel('skyline.expose.prometheus.metrics', key)
                pruned_count += 1
            except Exception as err:
                logger.error('error :: %s :: failed to hdel %s - %s' % (
                    function_str, key, err))

    return pruned_count
