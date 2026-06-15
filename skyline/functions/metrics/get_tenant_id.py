"""
get_tenant_id.py
"""
import logging
import traceback

from skyline_functions import get_redis_conn_decoded
from functions.database.queries.update_metric_tenant_id import update_metric_tenant_id
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.settings.get_external_settings import get_external_settings

try:
    from settings import EXTERNAL_SETTINGS
except:
    EXTERNAL_SETTINGS = {}

# @added 20250122 - Feature #5592: tenant_id column in DB tables
def get_tenant_id(
        current_skyline_app, metric_id=0, base_name=None,
        new_metric=False, log=False):
    """
    Returns a tenant_id for a metric_id or base_name.

    :param current_skyline_app: the app calling the function
    :param metric_id: the metric id to lookup the base_name for
    :param base_name: the base_name of the metric
    :param new_metric: whether this is a new metric that is not expected to have
        a metric_id yet
    :param log:  whether the function should log or not
    :type current_skyline_app: str
    :type metric_id: int
    :type base_name: str
    :type new_metric: boolean
    :type log: boolean
    :return: tenant_id
    :rtype: int

    """
    tenant_id = 0

    # So they are stored in the DB
    # Also stored in Redis
    # Determined from settings and external_settings

    function_str = 'functions.metrics.get_tenant_id'

    # tenant_id is only applied if they come from external settings
    if not EXTERNAL_SETTINGS:
        return tenant_id
    external_settings_active = False
    for external_setting, external_settings_dict in EXTERNAL_SETTINGS.items():
        if external_setting == 'mock_api_external_settings':
            continue
        try:
            if 'mock_api?test_external_settings' in external_settings_dict['endpoint']:
                continue
            else:
                external_settings_active = True
                break
        except:
            pass
    if not external_settings_active:
        return tenant_id

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = None
    if log:
        current_logger = logging.getLogger(current_skyline_app_logger)

    if not metric_id and base_name and not new_metric:
        try:
            metric_id = get_metric_id_from_base_name(current_skyline_app, base_name)
        except Exception as err:
            if not current_logger:
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: get_metric_id_from_base_name failed with base_name: %s, err: %s' % (
                function_str, str(base_name), err))

    if metric_id and not base_name:
        try:
            base_name = get_base_name_from_metric_id(current_skyline_app, metric_id)
        except Exception as err:
            if not current_logger:
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: get_metric_id_from_base_name failed with base_name: %s, err: %s' % (
                function_str, str(metric_id), err))

    if not metric_id and not new_metric:
        if log:
            current_logger.info('%s :: failed to determine metric_id for base_name: %s' % (
                function_str, str(base_name)))
        return tenant_id
    if not base_name:
        if log:
            current_logger.info('%s :: failed to determine base_name for metric_id: %s' % (
                function_str, str(metric_id)))
        return tenant_id

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        if not current_logger:
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_redis_conn_decoded failed - %s' % (
            function_str, err))
        return tenant_id

    if metric_id:
        try:
            tenant_id = redis_conn_decoded.hget('panorama.metric_ids_with_tenant_id', metric_id)
            if tenant_id:
                tenant_id = int(tenant_id)
                return tenant_id
            else:
                tenant_id = 0
        except Exception as err:
            if not current_logger:
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: get_redis_conn_decoded failed - %s' % (
                function_str, err))
            return tenant_id

    no_tenant_id = 0
    if metric_id:
        try:
            no_tenant_id = redis_conn_decoded.sismember('panorama.metric_ids_with_no_tenant_id', metric_id)
        except Exception as err:
            if not current_logger:
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: sismember failed, err: %s' % (
                function_str, err))
            return tenant_id
        if no_tenant_id:
            return tenant_id

    external_settings = {}
    try:
        external_settings = get_external_settings(current_skyline_app, namespace=None, log=False)
    except Exception as err:
        if not current_logger:
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: get_external_settings failed, err: %s' % (
            function_str, err))
        external_settings = {}
    for external_setting, data_dict in external_settings.items():
        try:
            if external_setting.startswith('external-'):
                i_tenant_id = data_dict['id']
                namespace = data_dict['namespace']
                if namespace not in base_name:
                    continue
                namespace_dot = '%s.' % namespace
                if namespace_dot in base_name:
                    tenant_id = int(i_tenant_id)
                    break
                namespace_label = '="%s"' % namespace
                if namespace_label in base_name:
                    tenant_id = int(i_tenant_id)
                    break
        except Exception as err:
            if not current_logger:
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: iterating external_setting failed, err: %s' % (
                function_str, err))

    if tenant_id:
        set_tenant_id = None
        if not new_metric:
            try:
                set_tenant_id = update_metric_tenant_id(current_skyline_app, base_name, tenant_id=tenant_id)
            except Exception as err:
                if not current_logger:
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: update_metric_tenant_id failed, err: %s' % (
                    function_str, err))
        if metric_id:
            try:
                redis_conn_decoded.hset('panorama.metric_ids_with_tenant_id', metric_id, tenant_id)
            except Exception as err:
                if not current_logger:
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: hset on panorama.metric_ids_with_tenant_id failed, err: %s' % (
                    function_str, err))

    if not tenant_id and metric_id:
        try:
            redis_conn_decoded.sadd('panorama.metric_ids_with_no_tenant_id', metric_id)
        except Exception as err:
            if not current_logger:
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: sadd on panorama.metric_ids_with_no_tenant_id failed, err: %s' % (
                function_str, err))

    return tenant_id
