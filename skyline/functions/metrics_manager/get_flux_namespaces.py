"""
get_flux_namespaces.py
"""
import logging
from time import time

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20220427 - Feature #4536: Handle Redis failure
def get_flux_namespaces(self):
    """

    Create and manage the metrics_manager.flux.namespaces Redis hash

    :param self: the self object
    :type self: object
    :return: flux_namespaces
    :rtype: list

    """

    logger.info('metrics_manager :: get_flux_namespaces :: getting latest flux namespaces from aet.flux.workers.metrics_sent')
    flux_namespaces = []
    current_timestamp = int(time())
    aet_flux_workers_metrics_sent = []
    try:
        aet_flux_workers_metrics_sent = list(self.redis_conn_decoded.smembers('aet.flux.workers.metrics_sent'))
    except Exception as err:
        logger.error('error :: metrics_manager :: get_flux_namespaces :: failed to smembers aet.flux.workers.metrics_sent Redis set - %s' % (
            err))
    flux_namespaces = []
    if aet_flux_workers_metrics_sent:
        for metric in aet_flux_workers_metrics_sent:
            flux_namespaces.append(metric.split('.')[0])
        flux_namespaces = list(set(flux_namespaces))
    logger.info('metrics_manager :: get_flux_namespaces :: determined %s top level namespaces from aet.flux.workers.metrics_sent' % str(len(flux_namespaces)))
    flux_namespaces_dict = {}
    if flux_namespaces:
        for namespace in flux_namespaces:
            flux_namespaces_dict[namespace] = current_timestamp
    if flux_namespaces_dict:
        try:
            self.redis_conn_decoded.hset('metrics_manager.flux.namespaces', mapping=flux_namespaces_dict)
        except Exception as err:
            logger.error('error :: metrics_manager :: get_flux_namespaces :: failed to hset metrics_manager.flux.namespaces - %s' % str(err))

    metrics_manager_flux_namespaces_dict = {}
    try:
        metrics_manager_flux_namespaces_dict = self.redis_conn_decoded.hgetall('metrics_manager.flux.namespaces')
    except Exception as err:
        logger.error('error :: metrics_manager :: get_flux_namespaces :: failed to hgetall metrics_manager.flux.namespaces - %s' % str(err))
    remove_stale_namespaces = []
    if metrics_manager_flux_namespaces_dict:
        for namespace in list(metrics_manager_flux_namespaces_dict.keys()):
            try:
                last_timestamp = int(float(metrics_manager_flux_namespaces_dict[namespace]))
                age = current_timestamp - last_timestamp
                if age > 86400:
                    logger.info('metrics_manager :: get_flux_namespaces :: removing %s from metrics_manager.flux.namespaces as no data for %s seconds' % str(age))
                    remove_stale_namespaces.append(namespace)
            except Exception as err:
                logger.error('error :: metrics_manager :: get_flux_namespaces :: failed to determine last data time for %s - %s' % (
                    namespace, str(err)))
    logger.info('metrics_manager :: get_flux_namespaces :: %s stale namespaces to remove from metrics_manager.flux.namespaces' % str(len(remove_stale_namespaces)))
    if remove_stale_namespaces:
        try:
            self.redis_conn_decoded.hdel('metrics_manager.flux.namespaces', *set(remove_stale_namespaces))
        except Exception as err:
            logger.error('error :: metrics_manager :: get_flux_namespaces :: failed to hdel remove_stale_namespaces from metrics_manager.flux.namespaces - %s' % str(err))
        try:
            metrics_manager_flux_namespaces_dict = self.redis_conn_decoded.hgetall('metrics_manager.flux.namespaces')
        except Exception as err:
            logger.error('error :: metrics_manager :: get_flux_namespaces :: failed to hgetall updated metrics_manager.flux.namespaces - %s' % str(err))

    if metrics_manager_flux_namespaces_dict:
        flux_namespaces = list(metrics_manager_flux_namespaces_dict.keys())
    logger.info('metrics_manager :: get_flux_namespaces :: %s flux namespaces found' % str(len(flux_namespaces)))

    return flux_namespaces
