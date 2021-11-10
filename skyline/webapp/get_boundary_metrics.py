import logging
import traceback

import settings
from skyline_functions import get_redis_conn_decoded
from matched_or_regexed_in_list import matched_or_regexed_in_list
from backend import get_cluster_data


# @added 20210720 - Feature #4188: metrics_manager.boundary_metrics
def get_boundary_metrics(
        current_skyline_app, metrics, namespaces, cluster_data=False,
        log=False):
    """
    Determine all the boundary metrics and return a dictionary of them and
    their algorithms.

    :param current_skyline_app: the app calling the function
    :param metrics: a list of base_names
    :param namespaces: a list of namespace pattern to match
    :param cluster_data: whether this is a cluster_data request, optional,
        defaults to False
    :param log: whether to log or not, optional, defaults to False
    :type current_skyline_app: str
    :type metrics: list
    :type namespace: list
    :type cluster_data: boolean
    :type log: boolean
    :return: boundary_metrics
    :rtype: dict

    """

    boundary_metrics = {}
    function_str = 'get_boundary_metrics'

    filter_by_metrics = []

    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.info('%s :: %s :: determining boundary_metrics' % (
            current_skyline_app, function_str))
    else:
        current_logger = None

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: get_redis_conn_decoded failed - %s' % (
            current_skyline_app, function_str, e))
        raise

    boundary_metrics_redis_dict = {}
    try:
        boundary_metrics_redis_dict = redis_conn_decoded.hgetall('metrics_manager.boundary_metrics')
        if log:
            current_logger.info('%s :: %s :: got %s boundary metrics from metrics_manager.boundary_metrics' % (
                current_skyline_app, function_str,
                str(len(boundary_metrics_redis_dict))))
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get Redis hash key metrics_manager.boundary_metrics - %s' % (function_str, e))
        raise

    boundary_metrics = boundary_metrics_redis_dict.copy()

    remote_boundary_metrics = []
    if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
        boundary_metrics_uri = 'boundary_metrics'
        try:
            remote_boundary_metrics = get_cluster_data(boundary_metrics_uri, 'boundary_metrics')
        except Exception as e:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to get boundary_metrics from remote instances - %s' % (function_str, e))
            raise
        if remote_boundary_metrics:
            if log:
                current_logger.info('got %s remote boundary_metrics from the remote Skyline instances' % str(len(remote_boundary_metrics)))
            for remote_data in remote_boundary_metrics:
                for base_name in list(remote_data.keys()):
                    boundary_metrics[base_name] = remote_data[base_name]

    if metrics:
        for metric in metrics:
            filter_by_metrics.append(metric)

    unique_base_names = []
    if namespaces:
        redis_key = 'analyzer.metrics_manager.db.metric_names'
        try:
            unique_base_names = list(redis_conn_decoded.smembers(redis_key))
            if unique_base_names:
                if log:
                    current_logger.info('%s :: %s :: got %s unique_base_names' % (
                        current_skyline_app, function_str,
                        str(len(unique_base_names))))
        except Exception as e:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: %s :: failed to get Redis key %s - %s' % (
                current_skyline_app, function_str, redis_key, e))
            raise
        for base_name in unique_base_names:
            try:
                pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, base_name, namespaces)
                if pattern_match:
                    filter_by_metrics.append(base_name)
            except Exception as e:
                if not log:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: %s :: failed to get Redis key %s - %s' % (
                    current_skyline_app, function_str, redis_key, e))

    if filter_by_metrics:
        if log:
            current_logger.info('%s :: %s :: filtering on %s metrics' % (
                current_skyline_app, function_str,
                str(len(filter_by_metrics))))

        filtered_boundary_metrics = {}
        for base_name in list(set(filter_by_metrics)):
            boundary_metric_dict = None
            try:
                boundary_metric_dict = boundary_metrics_redis_dict[base_name]
            except:
                continue
            if boundary_metric_dict:
                filtered_boundary_metrics[base_name] = boundary_metric_dict
        if filtered_boundary_metrics:
            boundary_metrics = filtered_boundary_metrics.copy()
            if log:
                current_logger.info('%s :: %s :: filtered %s boundary_metrics' % (
                    current_skyline_app, function_str,
                    str(len(boundary_metrics))))

    return boundary_metrics
