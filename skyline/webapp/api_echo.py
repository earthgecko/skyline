"""
api_echo.py
"""
import logging
from time import time
from flask import request

from settings import REMOTE_SKYLINE_INSTANCES
from backend import get_cluster_data
from matched_or_regexed_in_list import matched_or_regexed_in_list
from functions.metrics.get_metric_ids_and_base_names import get_metric_ids_and_base_names
from skyline_functions import get_redis_conn_decoded

# @added 20250522 - Feature #5626: skylark.echo
# Thanks to my friend Pip for the naming inspiration, things that carry
# tunes and messages.
def api_echo(current_skyline_app, cluster_data=False):
    """
    Return a dict with the last analysed metrics with their timestamp and value.

    :param current_skyline_app: the app calling the function
    :param cluster_data: the cluster_data parameter from the request
    :type current_skyline_app: str
    :type cluster_data: bool
    :return: analysed_events
    :rtype: dict

    """
    function_str = 'api_echo'
    analysed_metrics_dict = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    namespaces = None
    try:
        namespaces_str = request.args.get('namespaces')
        if namespaces_str:
            namespaces = namespaces_str.split(',')
        current_logger.info('api_echo :: with namespaces: %s' % str(namespaces))
    except:
        namespaces = None

    if not namespaces:
        post_data = {}
        try:
            post_data = request.get_json()
        except Exception as err:
            current_logger.error('error :: api_echo - no POST data - %s' % (
                err))
            current_logger.info('api_echo :: return 400 no POST data')
            analysed_metrics_dict['status_code'] = 400
            analysed_metrics_dict['error'] = 'no post data'
            return analysed_metrics_dict

        if post_data:
            current_logger.info('api_echo :: post_data: %s' % str(post_data))
        try:
            namespaces = post_data['data']['namespaces']
        except KeyError:
            namespaces = None
        except Exception as err:
            namespaces = None
            current_logger.error('error :: api_echo - evaluation of  post_data[\'data\'][\'namespaces\'] failed - %s' % (
                err))

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_redis_conn_decoded failed - %s' % (
            function_str, err))
        raise

    skylark_metrics_value_dict = {}
    skylark_metrics_timestamp_dict = {}
    metric_ids_with_base_names = {}

    last_aligned_ts = int(int(time()) // 60 * 60) - 60
    skylark_values_key = 'skylark.echo.metrics.values.%s' % str(last_aligned_ts)
    skylark_timestamps_key = 'skylark.echo.metrics.timestamps.%s' % str(last_aligned_ts)
    try:
        skylark_metrics_value_dict = redis_conn_decoded.hgetall(skylark_values_key)
    except Exception as err:
        current_logger.error('error :: %s :: failed to hgetall %s, err: %s' % (
            function_str, skylark_values_key, err))
        raise
    try:
        skylark_metrics_timestamp_dict = redis_conn_decoded.hgetall(skylark_timestamps_key)
    except Exception as err:
        current_logger.error('error :: %s :: failed to hgetall %s, err: %s' % (
            function_str, skylark_timestamps_key, err))
        raise

    # @added 20250611 - Feature #5626: skylark.echo
    # Add metric_id
    try:
        metric_ids_with_base_names = get_metric_ids_and_base_names(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_metric_ids_and_base_names failed, err: %s' % (
            function_str, err))
        raise
    base_names_with_metric_ids = {base_name: metric_id for metric_id, base_name in metric_ids_with_base_names.items()}

    errors = []
    for metric, value in skylark_metrics_value_dict.items():

        # @added 20250611 - Feature #5626: skylark.echo
        # Add metric_id
        metric_id = 0
        try:
            metric_id = base_names_with_metric_ids[metric]
        except Exception as err:
            errors.append([metric, err])

        try:
            analysed_metrics_dict[metric] = {
                'timestamp': int(skylark_metrics_timestamp_dict[metric]),
                'value': float(value),
                # @added 20250611 - Feature #5626: skylark.echo
                # Add metric_id
                'id': metric_id,
            }
        except Exception as err:
            errors.append([metric, err])

    all_metrics = list(analysed_metrics_dict.keys())
    remove_metrics = []
    pattern_match_errors = []
    metric_ids_to_lookup_errors = []
    if namespaces:
        for metric in all_metrics:
            use_base_name = str(metric)
            if metric.startswith('labelled_metrics.'):
                if not metric_ids_with_base_names:
                    try:
                        metric_ids_with_base_names = get_metric_ids_and_base_names(current_skyline_app)
                    except Exception as err:
                        current_logger.error('error :: %s :: get_metric_ids_and_base_names failed, err: %s' % (
                            function_str, err))
                        raise
                try:
                    metric_id_str = metric.replace('labelled_metrics.', '', 1)
                    metric_id = int(metric_id_str)
                    use_base_name = metric_ids_with_base_names[metric_id]
                except Exception as err:
                    metric_ids_to_lookup_errors.append(['error metric_name lookup', metric, err])
                    continue
            pattern_match = None
            metric_matched_by = None
            try:
                pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, use_base_name, namespaces)
            except Exception as err:
                pattern_match_errors.append(['error matched_or_regexed_in_list', str(use_base_name), err])
            if pattern_match:
                continue
            else:
                remove_metrics.append(metric)
    if remove_metrics:
        for metric in remove_metrics:
            try:
                del analysed_metrics_dict[metric]
            except:
                pass

    # TODO - cluster similar to api_get_analysed_events.py

    current_logger.info('%s :: returning %s metrics in analysed_metrics_dict' % (
        function_str, str(len(analysed_metrics_dict))))
    return analysed_metrics_dict
