"""
api_get_namespace_metric_count.py
"""
import logging
from flask import request

from settings import REMOTE_SKYLINE_INSTANCES
from backend import get_cluster_data
from functions.flux.get_test_metrics import get_test_metrics
from functions.metrics.get_metric_names_details import get_metric_names_details


# @added 20230202 - Feature #4842: webapp - api - get_flux_test_metrics
#                   Feature #4840: flux - prometheus - x-test-only header
def api_get_flux_test_metrics(current_skyline_app, cluster_data):
    """
    Return a dict with the number of metrics in the namespace with totals for
    total_metrics, inactive_metrics and active_metrics.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: namespace_metric_count
    :rtype: dict

    """
    flux_test_data = {}

    function_str = 'api_get_flux_test_metrics'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    namespace = None
    try:
        namespace = request.args.get('namespace')
        current_logger.info('api_get_flux_test_metrics :: with namespace: %s' % str(namespace))
    except Exception as err:
        current_logger.error('error :: api_get_flux_test_metrics no namespace argument - %s' % err)
        return 400

    try:
        flux_test_data = get_test_metrics(current_skyline_app, namespace)
    except Exception as err:
        current_logger.error('error :: %s :: get_test_metrics failed - %s' % (
            function_str, err))
    if flux_test_data:
        if 'errors' not in list(flux_test_data.keys()):
            current_logger.info('api_get_flux_test_metrics :: returning flux_test_data')
            # return flux_test_data
    else:
        current_logger.warning('warning :: api_get_flux_test_metrics :: failed to determine test metrics')

    remote_flux_test_data = []
    if REMOTE_SKYLINE_INSTANCES and cluster_data:
        current_logger.info('api_get_flux_test_metrics :: determining flux_test_data from other cluster nodes')
        api_uri = 'get_flux_test_metrics=true&namespace=%s&cluster_call=true' % namespace

        try:
            remote_flux_test_data = get_cluster_data(api_uri, 'flux_test_metrics')
        except Exception as err:
            current_logger.error('error :: %s :: failed to get_flux_test_metrics from remote instances - %s' % (
                function_str, err))
            raise
        current_logger.info('%s :: determined %s data sets from remote instances' % (
            function_str, str(len(remote_flux_test_data))))

    if remote_flux_test_data:
        current_logger.info('%s :: got remote remote_flux_test_data from the remote Skyline instances' % (
            function_str))
        for flux_test_data_dict in remote_flux_test_data:
            if 'metrics' in flux_test_data_dict:
                flux_test_data = flux_test_data_dict
                break
        if 'metrics' not in flux_test_data:
            current_logger.info('%s :: no flux_test_data from the remote Skyline instances' % (
                function_str))
    if 'metrics' in flux_test_data:
        metric_names_details = {}
        current_logger.info('%s :: getting metric_names_details for %s metrics' % (
            function_str, str(len(flux_test_data['metrics']))))
        try:
            metric_names_details = get_metric_names_details(current_skyline_app, flux_test_data['metrics'], strip_labels=[])
        except Exception as err:
            current_logger.error('error :: %s :: get_metric_names_details failed - %s' % (
                function_str, err))
        if metric_names_details:
            current_logger.info('%s :: got metric_names_details for %s metrics' % (
                function_str, str(len(flux_test_data['metrics']))))
            flux_test_data['metric_details'] = metric_names_details

    return flux_test_data
