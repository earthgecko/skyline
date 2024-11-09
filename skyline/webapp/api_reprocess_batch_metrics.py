"""
api_reprocess_batch_metrics.py
"""
import copy
import logging
import os

import requests
from flask import request

import settings
from functions.cluster.is_shard_metric import is_shard_metric
from functions.metrics.get_base_names_and_metric_ids import get_base_names_and_metric_ids
from functions.database.queries.get_all_db_metric_names import get_all_db_metric_names
from functions.database.queries.get_all_inactive_db_metric_names import get_all_inactive_db_metric_names
from functions.database.queries.set_metric_ids_as_active import set_metric_ids_as_active
from matched_or_regexed_in_list import matched_or_regexed_in_list
from skyline_functions import get_redis_conn_decoded

try:
    HORIZON_SHARDS = copy.deepcopy(settings.HORIZON_SHARDS)
except:
    HORIZON_SHARDS = {}
number_of_horizon_shards = 0
this_host = str(os.uname()[1])
HORIZON_SHARD = 0
if HORIZON_SHARDS:
    number_of_horizon_shards = len(HORIZON_SHARDS)
    HORIZON_SHARD = HORIZON_SHARDS[this_host]

# @added 20240602 - Feature #5368: analyzer_batch - reprocess
#                   Feature #5352: vista - bigquery
def api_reprocess_batch_metrics(current_skyline_app):
    """
    Return a dict with the timeseries.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: bq_backfill_job_dict
    :rtype: dict

    """
    function_str = 'api_reprocess_batch_metrics'
    response_dict = {'namespaces': [], 'metrics': [], 'success': False, 'status_code': 200}
    work_dict = {}
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    redis_reprocess_hash = 'analyzer.batch.reprocess_work'

    # Test whether form or json POST
    form_data = False
    try:
        namespaces = request.form['namespaces']
        if namespaces:
            form_data = True
            current_logger.info('api_reprocess_batch_metrics, form POST')
            current_logger.info('api_reprocess_batch_metrics - namespaces passed: %s' % str(namespaces))
    except:
        current_logger.info('api_reprocess_batch_metrics no form data trying json')

    post_data = {}
    if not form_data:
        try:
            post_data = request.get_json()
        except Exception as err:
            current_logger.error('error :: %s - no POST data - %s' % (
                function_str, err))
            current_logger.info('%s, return 400 no POST data' % function_str)
            response_dict['status_code'] = 400
            response_dict['error'] = 'no post data'
            return response_dict
        try:
            namespaces = post_data['data']['namespaces']
            current_logger.info('api_reprocess_batch_metrics - namespaces passed: %s' % str(namespaces))
        except Exception as err:
            current_logger.error('error :: %s :: no namespaces in POST data, err: %s' % (
                function_str, err))
            response_dict['status_code'] = 400
            response_dict['error'] = 'no namespaces passed in post data'
            return response_dict

    if isinstance(namespaces, str):
        namespaces = namespaces.split(',')
    response_dict['namespaces'] = list(namespaces)
    work_dict['namespaces'] = list(namespaces)

    # Allow for a list of metrics to be sent in post data rather than namespaces
    metrics = []
    if not form_data:
        try:
            metrics = post_data['data']['metrics']
            current_logger.info('api_reprocess_batch_metrics - %s metrics passed' % str(len(metrics)))
        except:
            metrics = []
    work_dict['metrics'] = list(metrics)

    from_timestamp = None
    try:
        if form_data:
            from_timestamp = int(request.form['from_timestamp'])
        else:
            from_timestamp = int(post_data['data']['from_timestamp'])
        current_logger.info('%s :: with from_timestamp: %s' % (function_str, str(from_timestamp)))
        response_dict['from_timestamp'] = from_timestamp
        work_dict['from_timestamp'] = int(from_timestamp)
    except Exception as err:
        current_logger.error('error :: %s :: no valid from_timestamp argument, err: %s' % (function_str, err))
        response_dict['status_code'] = 400
        response_dict['error'] = 'no valid from_timestamp argument'
        return response_dict

    until_timestamp = None
    try:
        if form_data:
            until_timestamp = int(request.form['until_timestamp'])
        else:
            until_timestamp = int(post_data['data']['until_timestamp'])
        current_logger.info('%s :: with until_timestamp: %s' % (function_str, str(until_timestamp)))
        response_dict['until_timestamp'] = until_timestamp
        work_dict['until_timestamp'] = int(until_timestamp)
    except Exception as err:
        current_logger.error('error :: %s :: no valid until_timestamp argument, err: %s' % (function_str, err))
        response_dict['status_code'] = 400
        response_dict['error'] = 'no valid until_timestamp argument'
        return response_dict

    dry_run = True
    try:
        dry_run_str = None
        if form_data:
            dry_run_str = request.form['dry_run']
        else:
            dry_run_str = post_data['data']['dry_run']
        if str(dry_run_str) == 'false' or str(dry_run_str) == 'False':
            dry_run = False
        current_logger.info('%s :: with dry_run: %s' % (function_str, str(dry_run)))
        response_dict['dry_run'] = dry_run
        work_dict['dry_run'] = dry_run
    except Exception as err:
        current_logger.error('error :: %s :: no valid until_timestamp argument, err: %s' % (function_str, err))
        response_dict['status_code'] = 400
        response_dict['error'] = 'no valid until_timestamp argument'
        return response_dict

    # @added 20240607 - Feature #5368: analyzer_batch - reprocess
    # Allow for oneshot analysis
    oneshot = False
    try:
        oneshot_str = None
        if form_data:
            oneshot_str = request.form['oneshot']
        else:
            oneshot_str = post_data['data']['oneshot']
        if str(oneshot_str) == 'true' or str(oneshot_str) == 'True':
            oneshot = True
        current_logger.info('%s :: with oneshot: %s' % (function_str, str(oneshot)))
        response_dict['oneshot'] = oneshot
        work_dict['oneshot'] = oneshot
    except Exception as err:
        current_logger.error('error :: %s :: no valid oneshot argument, err: %s' % (function_str, err))
        response_dict['status_code'] = 400
        response_dict['error'] = 'no valid oneshot argument'
        return response_dict

    cluster_call = False
    try:
        cluster_call_str = None
        if form_data:
            cluster_call_str = request.form['cluster_call']
        else:
            cluster_call_str = post_data['data']['cluster_call']
        if str(cluster_call_str) == 'true' or str(cluster_call_str) == 'True':
            cluster_call = True
        current_logger.info('%s :: with cluster_call: %s' % (function_str, str(cluster_call_str)))
        response_dict['cluster_call'] = cluster_call
    except Exception as err:
        cluster_call = False

    base_names_with_ids = {}
    try:
        base_names_with_ids = get_base_names_and_metric_ids(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_base_names_and_metric_ids failed - %s' % (
            function_str, err))

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_redis_conn_decoded failed - %s' % (
            function_str, err))
        raise

    # Resources to determine what metric ids need to be set to active again
    metric_ids_to_set_as_active_again = []
    all_db_metric_ids_with_names = {}
    try:
        all_db_metric_ids_with_names = redis_conn_decoded.hgetall('metrics_manager.all_db_metric_ids_with_names')
    except Exception as err:
        current_logger.error('error :: %s :: hgetall failed on metrics_manager.all_db_metric_ids_with_names, err: %s' % (
            function_str, err))
    all_base_names_with_ids = {}
    all_ids_with_base_names = {}
    for id_str, i_base_name in all_db_metric_ids_with_names.items():
        metric_id = int(float(id_str))
        all_base_names_with_ids[i_base_name] = metric_id
        all_ids_with_base_names[metric_id] = i_base_name
    # get inactive metric_ids
    inactive_metric_names_with_id = {}
    try:
        inactive_metric_names, inactive_metric_names_with_id = get_all_inactive_db_metric_names(current_skyline_app, with_ids=True)
    except Exception as err:
        current_logger.error('error :: %s :: get_all_inactive_db_metric_names failed, err: %s' % (
            function_str, err))
    inactive_metric_ids = set(list(inactive_metric_names_with_id.values()))

    metrics_to_reprocess = []
    all_base_names = list(all_base_names_with_ids.keys())
    if metrics:
        for base_name in metrics:
            metrics_to_reprocess.append(base_name)
            metric_id = None
            try:
                metric_id = all_base_names_with_ids[base_name]
            except:
                pass
            if metric_id:
                if metric_id in inactive_metric_ids:
                    metric_ids_to_set_as_active_again.append(metric_id)

    if not metrics_to_reprocess:
        for base_name in all_base_names:
            try:
                matched, matched_by = matched_or_regexed_in_list(current_skyline_app, base_name, namespaces)
                del matched_by
                if matched:
                    metrics_to_reprocess.append(base_name)
                    metric_id = None
                    try:
                        metric_id = all_base_names_with_ids[base_name]
                    except:
                        pass
                    if metric_id:
                        if metric_id in inactive_metric_ids:
                            metric_ids_to_set_as_active_again.append(metric_id)
            except:
                pass

    current_logger.info('%s :: %s metrics to reprocess' % (
        function_str, str(len(metrics_to_reprocess))))
    response_dict['metrics_to_reprocess_count'] = len(metrics_to_reprocess)

    if len(metric_ids_to_set_as_active_again):
        current_logger.info('%s :: %s metrics to set as active' % (
            function_str, str(len(metric_ids_to_set_as_active_again))))
        set_metrics_as_active_count = None
        try:
            set_metrics_as_active_count = set_metric_ids_as_active(current_skyline_app, metric_ids_to_set_as_active_again)
        except Exception as err:
            current_logger.error('error :: %s :: set_metric_ids_as_active failed - %s' % (
                function_str, str(err)))
        current_logger.info('%s :: %s metrics set as active' % (
            function_str, str(set_metrics_as_active_count)))

    local_metrics_to_reprocess = []
    call_cluster_nodes = False
    if HORIZON_SHARDS:
        for base_name in metrics_to_reprocess:
            shard_metric = is_shard_metric(base_name)
            if shard_metric:
                local_metrics_to_reprocess.append(base_name)
            else:
                call_cluster_nodes = True
        current_logger.info('%s :: %s local metrics to process' % (
            function_str, str(len(local_metrics_to_reprocess))))
        metrics_to_reprocess = list(local_metrics_to_reprocess)

    metrics_to_reprocess_dict = {}
    for base_name in metrics_to_reprocess:
        data_dict = {
            'from_timestamp': from_timestamp, 'until_timestamp': until_timestamp,
            'dry_run': dry_run, 'oneshot': oneshot
        }
        metrics_to_reprocess_dict[base_name] = str(data_dict)

    added_count = 0
    if len(metrics_to_reprocess_dict) > 0:
        try:
            added_count = redis_conn_decoded.hset(redis_reprocess_hash, mapping=metrics_to_reprocess_dict)
        except Exception as err:
            current_logger.error('error :: %s :: hset failed on %s on %s, err: %s' % (
                function_str, redis_reprocess_hash, this_host, err))
            response_dict['status_code'] = 500
            err_msg = 'failed to add to process on %s' % this_host
            response_dict['error'] = err_msg
    current_logger.info('%s :: added %s items to %s' % (
        function_str, str(added_count), redis_reprocess_hash))
    response_dict['added_count'] = added_count
    response_dict['metrics'] = list(metrics_to_reprocess)
    if call_cluster_nodes:
        response_dict['cluster_nodes'] = {}
        response_dict['cluster_nodes'][this_host] = {'added_count': added_count, 'metrics': metrics_to_reprocess}

    if cluster_call:
        call_cluster_nodes = False

    if call_cluster_nodes:
        for remote_url, remote_user, remote_password, shostname in settings.REMOTE_SKYLINE_INSTANCES:
            if shostname == this_host:
                continue
            try:
                # Although a POST request query parameters are added for tracking
                # and debugging in the log
                shard_url = '%s/reprocess_batch_metrics' % remote_url
            except Exception as err:
                current_logger.error('error :: %s :: failed to construct the shard_url for %s, err: %s' % (
                    function_str, shostname, err))
            data_dict = {
                'data': {
                'namespaces': namespaces, 'metrics': metrics,
                'from_timestamp': from_timestamp,
                'until_timestamp': until_timestamp, 'dry_run': dry_run,
                'oneshot': oneshot, 'verify_ssl': settings.VERIFY_SSL,
                'cluster_call': True,
                }
            }
            r = None
            try:
                headers = {"content-type": "application/json"}
                r = requests.post(shard_url, auth=(remote_user, remote_password), json=data_dict, headers=headers, timeout=30, verify=settings.VERIFY_SSL)
                current_logger.info('%s :: %s returned status_code: %s' % (
                    function_str, shostname, str(r.status_code)))
            except Exception as err:
                current_logger.error('error :: %s :: request failed on %s, err: %s' % (
                    function_str, str(shard_url), err))
            if r:
                try:
                    response_json = r.json()
                    added_metrics = response_json['data']['metrics']
                    added_count = response_json['data']['added_count']
                    response_dict['added_count'] += added_count
                    for metric in added_metrics:
                        response_dict['metrics'].append(metric)
                    response_dict['cluster_nodes'][shostname] = {'added_count': added_count, 'metrics': added_metrics}
                except Exception as err:
                    current_logger.error('error :: %s :: failed to determine details from %s response, err: %s' % (
                        function_str, shostname, err))

    response_dict['success'] = True

    return response_dict
