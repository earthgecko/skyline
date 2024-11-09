"""
manage_batch_processing_namespaces.py
"""
import copy
import logging

import settings
from functions.settings.get_bq_accounts_settings import get_bq_accounts_settings

try:
    BATCH_PROCESSING_NAMESPACES = settings.BATCH_PROCESSING_NAMESPACES
except:
    BATCH_PROCESSING_NAMESPACES = []

# @added 20240522 - Feature #5352: vista - bigquery
try:
    BATCH_METRICS_CUSTOM_FULL_DURATIONS = copy.deepcopy(settings.BATCH_METRICS_CUSTOM_FULL_DURATIONS)
except:
    BATCH_METRICS_CUSTOM_FULL_DURATIONS = {}
try:
    CUSTOM_STALE_PERIOD = copy.deepcopy(settings.CUSTOM_STALE_PERIOD)
except:
    CUSTOM_STALE_PERIOD = {}

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20240518 - Feature #5356: get_batch_processing_namespaces
#                   Feature #5352: vista - bigquery
#                   Feature #3480: batch_processing 
def manage_batch_processing_namespaces(self):
    """

    Create and manage the metrics_manager.batch_processing_namespaces.

    :param self: the self object
    :type self: object
    :return: batch_processing_namespaces
    :rtype: list

    """

    logger.info('metrics_manager :: manage_batch_processing_namespaces :: managing metrics_manager.manage_batch_processing_namespaces Redis set')
    batch_processing_namespaces = list(BATCH_PROCESSING_NAMESPACES)

    # @added 20240522 - Feature #5352: vista - bigquery
    namespace_custom_full_durations = copy.deepcopy(BATCH_METRICS_CUSTOM_FULL_DURATIONS)
    namespace_custom_stale_period = copy.deepcopy(CUSTOM_STALE_PERIOD)

    vista_bq_accounts = {}
    try:
        vista_bq_accounts = get_bq_accounts_settings(skyline_app)
    except Exception as err:
        logger.error('error :: metrics_manager :: manage_batch_processing_namespaces :: get_bq_accounts_settings failed, err: %s' % err)
    for bq_account in vista_bq_accounts:
        if 'batch_processing_namespace' in vista_bq_accounts[bq_account].keys():
            if isinstance(vista_bq_accounts[bq_account]['batch_processing_namespace'], str):
                batch_processing_namespace = vista_bq_accounts[bq_account]['batch_processing_namespace']
                try:
                    batch_processing_namespaces.append(batch_processing_namespace)
                except Exception as err:
                    logger.error('error :: metrics_manager :: manage_batch_processing_namespaces :: failed to add vista bq_account %s batch_processing_namespace, err: %s' % (
                        bq_account, err))
            # @added 20240522 - Feature #5352: vista - bigquery
            if 'full_duration' in vista_bq_accounts[bq_account].keys():
                if isinstance(vista_bq_accounts[bq_account]['full_duration'], int):
                    try:
                        namespace_custom_full_durations[batch_processing_namespace] = vista_bq_accounts[bq_account]['full_duration']
                    except Exception as err:
                        logger.error('error :: metrics_manager :: manage_batch_processing_namespaces :: failed to add vista bq_account %s full_duration to namespace_custom_full_durations, err: %s' % (
                            bq_account, err))
            if 'stale_period' in vista_bq_accounts[bq_account].keys():
                if isinstance(vista_bq_accounts[bq_account]['stale_period'], int):
                    try:
                        namespace_custom_stale_period[batch_processing_namespace] = vista_bq_accounts[bq_account]['stale_period']
                    except Exception as err:
                        logger.error('error :: metrics_manager :: manage_batch_processing_namespaces :: failed to add vista bq_account %s stale_period to namespace_custom_full_durations, err: %s' % (
                            bq_account, err))

    current_batch_processing_namespaces = []
    try:
        current_batch_processing_namespaces = self.redis_conn_decoded.smembers('metrics_manager.batch_processing_namespaces')
    except Exception as err:
        logger.error('error :: metrics_manager :: manage_batch_processing_namespaces :: smembers of metrics_manager.batch_processing_namespaces failed, err: %s' % err)

    if batch_processing_namespaces:
        add_namespaces = [namespace for namespace in batch_processing_namespaces if namespace not in current_batch_processing_namespaces]
        if add_namespaces:
            logger.info('metrics_manager :: manage_batch_processing_namespaces :: %s namespaces to add to metrics_manager.batch_processing_namespaces, %s' % (
                str(len(add_namespaces)), str(add_namespaces)))
            try:
                self.redis_conn_decoded.sadd('metrics_manager.batch_processing_namespaces', *set(add_namespaces))
                logger.info('metrics_manager :: manage_batch_processing_namespaces :: created the metrics_manager.batch_processing_namespaces Redis set with %s namespace' % (
                    str(len(batch_processing_namespaces))))
            except Exception as err:
                logger.error('metrics_manager :: manage_batch_processing_namespaces :: failed to create set metrics_manager.batch_processing_namespaces, err: %s' % (
                    err))
        remove_namespaces = [namespace for namespace in current_batch_processing_namespaces if namespace not in batch_processing_namespaces]
        if remove_namespaces:
            logger.info('metrics_manager :: manage_batch_processing_namespaces :: %s namespaces to add to metrics_manager.batch_processing_namespaces, %s' % (
                str(len(remove_namespaces)), str(remove_namespaces)))
            try:
                self.redis_conn_decoded.sadd('metrics_manager.batch_processing_namespaces', *set(add_namespaces))
                logger.info('metrics_manager :: manage_batch_processing_namespaces :: created the metrics_manager.batch_processing_namespaces Redis set with %s namespace' % (
                    str(len(batch_processing_namespaces))))
            except Exception as err:
                logger.error('metrics_manager :: manage_batch_processing_namespaces :: failed to create set metrics_manager.batch_processing_namespaces, err: %s' % (
                    err))

    # @added 20240522 - Feature #5352: vista - bigquery
    if len(namespace_custom_full_durations) > 0:
        logger.info('metrics_manager :: manage_batch_processing_namespaces :: %s namespaces to add to metrics_manager.batch_processing_namespaces_full_durations, %s' % (
            str(len(namespace_custom_full_durations)), str(namespace_custom_full_durations)))
        try:
            self.redis_conn_decoded.hset('metrics_manager.batch_processing_namespaces_full_durations', mapping=namespace_custom_full_durations)
        except Exception as err:
            logger.error('metrics_manager :: manage_batch_processing_namespaces :: failed to hset metrics_manager.batch_processing_namespaces_full_durations, err: %s' % (
                err))
        remove_keys = []
        batch_processing_namespaces_full_durations_keys = []
        try:
            batch_processing_namespaces_full_durations_keys = self.redis_conn_decoded.hkeys('metrics_manager.batch_processing_namespaces_full_durations')
        except Exception as err:
            logger.error('metrics_manager :: manage_batch_processing_namespaces :: hkeys failed on metrics_manager.batch_processing_namespaces_full_durations, err: %s' % (
                err))
        for namespace in batch_processing_namespaces_full_durations_keys:
            if namespace not in list(namespace_custom_full_durations.keys()):
                remove_keys.append(namespace)
        if remove_keys:
            logger.info('metrics_manager :: manage_batch_processing_namespaces :: removing %s namespaces keys from metrics_manager.batch_processing_namespaces_full_durations, %s' % (
                str(len(remove_keys)), str(remove_keys)))
            for key in remove_keys:
                try:
                    self.redis_conn_decoded.hdel('metrics_manager.batch_processing_namespaces_full_durations', key)
                except Exception as err:
                    logger.error('metrics_manager :: manage_batch_processing_namespaces :: hdel failed on %s from metrics_manager.batch_processing_namespaces_full_durations, err: %s' % (
                        key, err))
    if len(namespace_custom_stale_period) > 0:
        logger.info('metrics_manager :: manage_batch_processing_namespaces :: %s namespaces to add to metrics_manager.batch_processing_namespaces_custom_stale_period, %s' % (
            str(len(namespace_custom_stale_period)), str(namespace_custom_stale_period)))
        try:
            self.redis_conn_decoded.hset('metrics_manager.batch_processing_namespaces_custom_stale_period', mapping=namespace_custom_stale_period)
        except Exception as err:
            logger.error('metrics_manager :: manage_batch_processing_namespaces :: failed to hset metrics_manager.batch_processing_namespaces_custom_stale_period, err: %s' % (
                err))
        remove_keys = []
        batch_processing_namespaces_stale_period_keys = []
        try:
            batch_processing_namespaces_stale_period_keys = self.redis_conn_decoded.hkeys('metrics_manager.batch_processing_namespaces_custom_stale_period')
        except Exception as err:
            logger.error('metrics_manager :: manage_batch_processing_namespaces :: hkeys failed on metrics_manager.batch_processing_namespaces_custom_stale_period, err: %s' % (
                err))
        for namespace in batch_processing_namespaces_stale_period_keys:
            if namespace not in list(namespace_custom_stale_period.keys()):
                remove_keys.append(namespace)
        if remove_keys:
            logger.info('metrics_manager :: manage_batch_processing_namespaces :: removing %s namespaces keys from metrics_manager.batch_processing_namespaces_custom_stale_period, %s' % (
                str(len(remove_keys)), str(remove_keys)))
            for key in remove_keys:
                try:
                    self.redis_conn_decoded.hdel('metrics_manager.batch_processing_namespaces_custom_stale_period', key)
                except Exception as err:
                    logger.error('metrics_manager :: manage_batch_processing_namespaces :: hdel failed on %s from metrics_manager.batch_processing_namespaces_custom_stale_period, err: %s' % (
                        key, err))

    logger.info('metrics_manager :: manage_batch_processing_namespaces :: all done')
    return batch_processing_namespaces
