"""
get_labelled_metric_type.py
"""
import logging
import traceback

from skyline_functions import get_redis_conn_decoded
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.metrics.get_labelled_metric_dict import get_labelled_metric_dict

# In Skyline a metric is either a counter (derivative) or a gauge
skyline_metric_types = {'COUNTER': 1, 'GAUGE': 0}
skyline_metric_types_by_id = {}
for key in list(skyline_metric_types.keys()):
    skyline_metric_types_by_id[skyline_metric_types[key]] = key


# @added 20220801 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
def get_labelled_metric_type(current_skyline_app, metric):
    """
    Returns the mtric type for a labelled_metrics name

    :param current_skyline_app: the app calling the function
    :param labelled_metric_name: the labelled_metrics name to lookup the base_name for.
    :type current_skyline_app: str
    :type labelled_metrics_name: str
    :return: type
    :rtype: str

    """
    function_str = 'functions.metrics.get_labelled_metric_type'
    current_logger = None
    metric_type = None

    metric_id = 0
    base_name = str(metric)
    if metric.startswith('labelled_metrics.'):
        metric_id_str = metric.replace('labelled_metrics.', '', 1)
        try:
            metric_id = int(metric_id_str)
        except:
            metric_id = 0
    if not metric_id:
        try:
            metric_id = get_metric_id_from_base_name(current_skyline_app, metric)
        except:
            pass

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to connect to Redis to get type for %s - %s' % (
            function_str, metric, err))
        return metric_type

    metric_type_str = None
    if metric_id:
        try:
            metric_type_str = redis_conn_decoded.hget('skyline.labelled_metrics.id.type', str(metric_id))
        except Exception as err:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: hget skyline.labelled_metrics.id.type failed for %s - %s' % (
                function_str, str(metric_id), err))
    if metric_type_str:
        metric_type = skyline_metric_types_by_id[int(metric_type_str)]

    if not metric_type:
        metric_dict = {}
        try:
            metric_dict = get_labelled_metric_dict(current_skyline_app, base_name)
        except Exception as err:
            if not current_logger:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: get_labelled_metric_dict failed for %s - %s' % (
                function_str, str(metric_id), err))
        metric_type_redis_key = None
        metric_name = None
        try:
            metric_name = metric_dict['metric']
            tenant_id = metric_dict['labels']['_tenant_id']
            server_id = metric_dict['labels']['_server_id']
            metric_type_redis_key = 'metrics_manager.prometheus.metrics_type.%s.%s' % (str(tenant_id), str(server_id))
        except Exception as err:
            if not current_logger:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: determining metric_type_redis_key failed from metric_dict: %s - %s' % (
                function_str, str(metric_dict), err))
        if metric_type_redis_key:
            try:
                metric_type = redis_conn_decoded.hget(metric_type_redis_key, metric_name)
            except Exception as err:
                if not current_logger:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error('error :: %s :: failed to determining metric_type failed from %s for %s - %s' % (
                    function_str, str(metric_type_redis_key), str(metric_name),
                    err))

    return metric_type
