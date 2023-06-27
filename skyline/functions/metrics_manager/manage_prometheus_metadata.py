"""
manage_prometheus_metadata.py
"""
import logging
import traceback
from ast import literal_eval

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20220620 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
def manage_prometheus_metadata(self):
    """

    Create and manage the metrics_manager.prometheus.metadata.* Redis keys and
    the metrics_manager.prometheus.metrics_type.* Redis hashes per
    tenant_id.server_id from the flux.prometheus_metadata Redis hash

    :param self: the self object
    :type self: object
    :return: (prometheus_metadata_dict, prometheus_metrics_type_dict)
    :rtype: tuple

    """
    prometheus_metadata_dict = {}
    prometheus_metrics_type_dict = {}

    logger.info('metrics_manager :: manage_prometheus_metadata - determining metric types for tenant servers')

    flux_prometheus_metadata_dict = {}
    try:
        flux_prometheus_metadata_dict = self.redis_conn_decoded.hgetall('flux.prometheus_metadata')
        self.redis_conn_decoded.delete('flux.prometheus_metadata')
    except Exception as err:
        logger.error('error :: metrics_manager :: manage_prometheus_metadata :: failed to hgetall flux.prometheus_metadata - %s' % (
            err))

    for timestamp_str in list(flux_prometheus_metadata_dict.keys()):
        try:
            prometheus_metadata = literal_eval(flux_prometheus_metadata_dict[timestamp_str])
            tenant_id = prometheus_metadata['tenant_id']
            server_id = prometheus_metadata['server_id']
            server_url = prometheus_metadata['server_url']
            metadata_list = prometheus_metadata['metadata']
            metadata_key = 'metrics_manager.prometheus.metadata.%s.%s' % (
                str(tenant_id), str(server_id))
            metrics_type_key = 'metrics_manager.prometheus.metrics_type.%s.%s' % (
                str(tenant_id), str(server_id))
            try:
                metadata = prometheus_metadata_dict[metadata_key]['metadata']
            except:
                prometheus_metadata_dict[metadata_key] = {}
                prometheus_metadata_dict[metadata_key]['metadata'] = {
                    'tenant_id': tenant_id, 'server_id': server_id,
                    'server_url': server_url
                }
                prometheus_metadata_dict[metadata_key]['metrics'] = {}
                prometheus_metrics_type_dict[metrics_type_key] = {}
            for metricFamilyName_dict in metadata_list:
                try:
                    metric_type = prometheus_metadata_dict[metadata_key]['metrics'][metricFamilyName_dict['metricFamilyName']]['type']
                except:
                    try:
                        prometheus_metadata_dict[metadata_key]['metrics'][metricFamilyName_dict['metricFamilyName']] = {}
                        prometheus_metrics_type_dict[metrics_type_key][metricFamilyName_dict['metricFamilyName']] = metricFamilyName_dict['type']
                        prometheus_metadata_dict[metadata_key]['metrics'][metricFamilyName_dict['metricFamilyName']]['type'] = metricFamilyName_dict['type']
                        prometheus_metadata_dict[metadata_key]['metrics'][metricFamilyName_dict['metricFamilyName']]['help'] = metricFamilyName_dict['help']
                    except:
                        pass
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: manage_prometheus_metadata :: failed to evaluate flux_prometheus_metadata_dict data - %s' % (
                err))

    for metadata_key in list(prometheus_metadata_dict.keys()):
        try:
            self.redis_conn_decoded.set(metadata_key, str(prometheus_metadata_dict[metadata_key]))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: manage_prometheus_metadata :: failed to set Redis key %s - %s' % (
                metadata_key, err))

    for metrics_type_key in list(prometheus_metrics_type_dict.keys()):
        try:
            self.redis_conn_decoded.hset(metrics_type_key, mapping=prometheus_metrics_type_dict[metrics_type_key])
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: manage_prometheus_metadata :: failed to hset Redis key %s - %s' % (
                metrics_type_key, err))

    logger.info('metrics_manager :: manage_prometheus_metadata - determine metric types for %s tenant servers' % (
        str(len(prometheus_metrics_type_dict))))

    return (prometheus_metadata_dict, prometheus_metrics_type_dict)
