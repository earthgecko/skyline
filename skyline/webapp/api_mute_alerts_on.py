"""
api_mute_alerts_on.py
"""
# from ast import literal_eval
import datetime
import logging
from os import uname
from time import time, strftime, gmtime

from flask import request, jsonify

from backend import get_cluster_data
from functions.metrics.get_base_names_and_metric_ids import get_base_names_and_metric_ids
from matched_or_regexed_in_list import matched_or_regexed_in_list
from settings import FLUX_SELF_API_KEY, REMOTE_SKYLINE_INSTANCES, SERVER_PYTZ_TIMEZONE 
from skyline_functions import get_redis_conn_decoded
from slack_functions import slack_post_message


# @added 20230605 - Feature #4932: mute_alerts_on
def api_mute_alerts_on(current_skyline_app, cluster_data, namespace, metric_patterns, expiry, list_muted_metrics):
    """
    Add the muted metrics and return a dict with the muted metrics.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: muted_metrics
    :rtype: dict

    """
    now = time()
    muted_metrics = {}
    match_errs = []

    function_str = 'api_mute_alerts_on'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    current_logger.info('api_mute_alerts_on :: with namespace: %s' % str(namespace))
    current_logger.info('api_mute_alerts_on :: with list_muted_metrics: %s' % str(list_muted_metrics))
    current_logger.info('api_mute_alerts_on :: with metric_patterns: %s' % str(metric_patterns))
    current_logger.info('api_mute_alerts_on :: with expiry: %s' % str(expiry))
    
    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_redis_conn_decoded failed - %s' % (
            function_str, err))
        raise

    muted_metrics_str = None
    if list_muted_metrics:
        try:
            muted_metrics = redis_conn_decoded.hgetall('metrics_manager.mute_alerts_on')
        except Exception as err:
            current_logger.error('error :: %s :: hgetall on metrics_manager.mute_alerts_on failed - %s' % (
                function_str, err))
            raise
        muted_metrics_dict = {}
        for metric in list(muted_metrics.keys()):
            if metric.startswith('labelled_metrics.'):
                continue
            if namespace:
                try:
                    pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, metric, [namespace])
                    if not pattern_match:
                        continue
                except Exception as err:
                    match_errs.append(['matched_or_regexed_in_list error', base_name], err)
            if metric_patterns:
                try:
                    pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, metric, metric_patterns)
                    if not pattern_match:
                        continue
                except Exception as err:
                    match_errs.append(['matched_or_regexed_in_list error', base_name], err)
            timestamp = int(muted_metrics[metric])
            until = datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            muted_metrics_dict[metric] = {'timestamp': timestamp, 'until': until}
        if muted_metrics_dict:
            muted_metrics = muted_metrics_dict.copy()
        return muted_metrics

    add_metrics = []

    base_names_with_ids = {}
    try:
        base_names_with_ids = get_base_names_and_metric_ids(current_skyline_app)
    except Exception as err:
        current_logger.error('error ::%s :: get_base_names_and_metric_ids failed - %s' % (
            function_str, str(err)))
        raise

    current_logger.info('api_mute_alerts_on :: running filters on %s metrics' % str(len(base_names_with_ids)))

    muting = []
    cluster_muting = []

    # Filter on namespace first if passed
    namespace_filtered_metrics = list(base_names_with_ids.keys())
    if namespace == 'None':
        namespace = None
    if namespace:
        namespace_filtered_metrics = []
        for base_name in list(base_names_with_ids.keys()):
            try:
                pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, base_name, [namespace])
                if pattern_match:
                    namespace_filtered_metrics.append(base_name)
            except Exception as err:
                match_errs.append(['matched_or_regexed_in_list error', base_name], err)
        current_logger.info('api_mute_alerts_on :: determined %s metrics that match namespace: %s' % (
            str(len(namespace_filtered_metrics)), str(namespace)))

    # Filter on metric_patterns if passed
    if metric_patterns:
        current_logger.info('api_mute_alerts_on :: filtering %s metrics on mertic_patterns: %s' % (
            str(len(namespace_filtered_metrics)), str(metric_patterns)))
        for base_name in namespace_filtered_metrics:
            try:
                pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, base_name, metric_patterns)
                if pattern_match:
                    add_metrics.append(base_name)
            except Exception as err:
                match_errs.append(['matched_or_regexed_in_list error', base_name], err)
    else:
        for base_name in namespace_filtered_metrics:
            add_metrics.append(base_name)

    # Add labelled_metrics. names as well
    for base_name in add_metrics:
        if not base_name.startswith('labelled_metrics.'):
            muting.append(base_name)
            cluster_muting.append(base_name)
        if '_tenant_id="' in base_name:
            labelled_metric = 'labelled_metrics.%s' % str(base_names_with_ids[base_name])
            add_metrics.append(labelled_metric)

    current_logger.info('api_mute_alerts_on :: determined %s metrics to mute' % str(len(muting)))

    add_metrics_dict = {}
    debug_redis_hash = 'skyline.mute_alerts_on.%s' % str(now)
    if add_metrics:
        expire_at = int(now) + expiry
        expire_at_str = str(expire_at)
        add_metrics_dict = {}
        for metric in add_metrics:
            add_metrics_dict[metric] = expire_at_str
        current_logger.info('api_mute_alerts_on :: adding %s metrics to metrics_manager.mute_alerts_on' % str(len(add_metrics_dict)))
        if add_metrics_dict:
            try:
                redis_conn_decoded.hset('metrics_manager.mute_alerts_on', mapping=add_metrics_dict)
            except Exception as err:
                current_logger.error('error :: %s :: failed to hset metrics_manager.mute_alerts_on - %s' % (
                    function_str, err))
                raise
            try:
                redis_conn_decoded.hset(debug_redis_hash, mapping=add_metrics_dict)
                redis_conn_decoded.expire(debug_redis_hash, 86400)
            except Exception as err:
                current_logger.error('error :: %s :: failed to hset metrics_manager.mute_alerts_on - %s' % (
                    function_str, err))
                raise
        muted_metrics = add_metrics_dict.copy()

    remote_muted_metrics = []
    if REMOTE_SKYLINE_INSTANCES and cluster_data:
        current_logger.info('api_mute_alerts_on :: calling /mute_alerts_on on other cluster nodes')
        endpoint_params = {
            'api_endpoint': 'mute_alerts_on',
            'data_required': 'metrics',
            'only_host': 'all',
            'method': 'POST',
            'post_data': {
                'data': {
                    'key': FLUX_SELF_API_KEY,
                    'cluster_data': False,
                    'cluster_call': True,
                    'namespace': namespace,
                    'metric_patterns': metric_patterns,
                    'expiry': expiry,
                }
            }
        }
        try:
            remote_muted_metrics = get_cluster_data('mute_alerts_on', 'metrics', only_host='all', endpoint_params=endpoint_params)
        except Exception as err:
            current_logger.error('error :: api_mute_alerts_on :: could not get mute_alerts_on from the remote Skyline instances - %s')
        if remote_muted_metrics:
            current_logger.info('api_mute_alerts_on :: got %s lists of muted_metrics from the other cluster nodes' % str(len(remote_muted_metrics)))
            for item in remote_muted_metrics:
                current_logger.info('api_mute_alerts_on :: a remote list with %s muted_metrics from another cluster nodes' % str(len(item)))

    if not add_metrics_dict:
        try:
            muted_metrics = redis_conn_decoded.hgetall('metrics_manager.mute_alerts_on')
        except Exception as err:
            current_logger.error('error :: %s :: hgetall on metrics_manager.mute_alerts_on failed - %s' % (
                function_str, err))
            raise

    host_muted_metrics = list(muted_metrics.keys())
    multiple_hosts_muted_metrics = {}
    # Add the remote muted metrics
    if remote_muted_metrics:
        for item in remote_muted_metrics:
            for metric in list(item.keys()):
                if metric in host_muted_metrics:
                    try:
                        multiple_hosts_muted_metrics[metric] += 1
                    except:
                        multiple_hosts_muted_metrics[metric] = 2
                muted_metrics[metric] = item[metric]['timestamp']
                if not metric.startswith('labelled_metrics.'):
                    cluster_muting.append(metric)

    if multiple_hosts_muted_metrics:
        current_logger.debug('debug api_mute_alerts_on :: multiple_hosts_muted_metrics: %s' % str(multiple_hosts_muted_metrics))

    muted_metrics_dict = {}
    for metric in list(muted_metrics.keys()):
        if metric.startswith('labelled_metrics.'):
            continue
        timestamp = int(muted_metrics[metric])
        until = datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        muted_metrics_dict[metric] = {'timestamp': timestamp, 'until': until}
    if muted_metrics_dict:
        muted_metrics = muted_metrics_dict.copy()

    if muting:
        try:
            alert_slack_channel = None
            slack_message = '*Skyline - NOTICE* - alerting muted on %s metrics for %s seconds until %s (%s).  For namespace: %s and metric_patterns: %s\ndebug_redis_hash: %s' % (
                str(len(list(set(muting)))), str(expiry), str(until), str(SERVER_PYTZ_TIMEZONE),
                str(namespace), str(metric_patterns), debug_redis_hash)
            if REMOTE_SKYLINE_INSTANCES:
                slack_message = '%s - on %s' % (slack_message, str(uname()[1]))
            if REMOTE_SKYLINE_INSTANCES and cluster_data:
                slack_message = '%s. Total metrics muted on cluster: %s' % (
                    slack_message, str(len(list(set(cluster_muting)))))
            slack_post = slack_post_message(current_skyline_app, alert_slack_channel, None, slack_message)
            current_logger.info('%s :: posted notice to slack - %s' % (function_str, slack_message))
        except Exception as err:
            current_logger.error('error :: %s :: slack_post_message failed - %s' % (
                function_str, err))

    return muted_metrics
