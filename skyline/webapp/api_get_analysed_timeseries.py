"""
api_get_analysed_timeseries.py
"""
import logging
import os
from flask import request

from settings import HORIZON_SHARDS, IONOSPHERE_DATA_FOLDER
from backend import get_cluster_data
from functions.metrics.get_authoritative_node import get_authoritative_node
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.metrics.get_base_name_from_labelled_metrics_name import get_base_name_from_labelled_metrics_name
from functions.timeseries.load_timeseries_json import load_timeseries_json

# @added 20240125 - Feature #5240: webapp - api_get_analysed_timeseries
def get_analysed_timeseries(current_skyline_app, cluster_data=False):
    """
    Return a list with the Mirage analysed Graphite and Redis downsampled and
    merged data timeseries if it exists.

    :param current_skyline_app: the app calling the function
    :param metric: the metric name
    :param anomaly_timestamp: the anomaly timestamp
    :param cluster_data: the cluster_data parameter from the request
    :type current_skyline_app: str
    :type metric: str
    :type anomaly_timestamp: int
    :type cluster_data: bool
    :return: analysed_timeseries
    :rtype: list

    """
    function_str = 'api_get_analysed_timeseries'
    analysed_timeseries = []

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    metric = None
    try:
        metric = request.args.get('metric')
        current_logger.info('api_get_analysed_timeseries :: with metric: %s' % str(metric))
    except Exception as err:
        current_logger.error('error :: api_get_analysed_timeseries :: no metric argument, err: %s' % err)
        return 400

    anomaly_timestamp = None
    try:
        if 'anomaly_timestamp' in request.args:
            anomaly_timestamp = int(request.args.get('anomaly_timestamp'))
        else:
            anomaly_timestamp = int(request.args.get('timestamp'))
        current_logger.info('api_get_analysed_timeseries :: with anomaly_timestamp: %s' % str(anomaly_timestamp))
    except Exception as err:
        current_logger.error('error :: api_get_analysed_timeseries :: no valid anomaly_timestamp argument, err: %s' % err)
        return 400

    # Determine if it exists locally
    labelled_metric_name = None
    base_name = str(metric)
    use_base_name = str(metric)
    if '{' in base_name and '}' in base_name and '_tenant_id="' in base_name:
        metric_id = 0
        try:
            metric_id = get_metric_id_from_base_name(current_skyline_app, base_name)
        except Exception as err:
            current_logger.error('error :: api_get_analysed_timeseries :: get_metric_id_from_base_name failed with base_name: %s, err: %s' % (str(base_name), err))
        if metric_id:
            labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
    if base_name.startswith('labelled_metrics.'):
        labelled_metric_name = str(base_name)
        try:
            metric_name = get_base_name_from_labelled_metrics_name(current_skyline_app, base_name)
            if metric_name:
                base_name = str(metric_name)
        except Exception as err:
            current_logger.error('error :: api_get_analysed_timeseries :: get_base_name_from_labelled_metrics_name failed for %s, err: %s' % (
                base_name, err))
    if labelled_metric_name:
        use_base_name = str(labelled_metric_name)

    timeseries_dir = use_base_name.replace('.', '/')
    metric_data_dir = '%s/%s/%s' % (IONOSPHERE_DATA_FOLDER, str(anomaly_timestamp), timeseries_dir)
    anomaly_json = '%s/%s.json' % (metric_data_dir, use_base_name)
    if labelled_metric_name:
        anomaly_json = '%s/%s.json' % (metric_data_dir, labelled_metric_name)
    if os.path.isfile(anomaly_json):
        current_logger.info('analysed data ts json available - %s' % (anomaly_json))
        try:
            analysed_timeseries = load_timeseries_json(current_skyline_app, anomaly_json)
        except Exception as err:
            current_logger.error('error :: api_get_analysed_timeseries :: load_timeseries_json failed on %s, err: %s' % (
                anomaly_json, err))

    if analysed_timeseries:
        return analysed_timeseries

    metric_shard_url = None
    if not analysed_timeseries and HORIZON_SHARDS and cluster_data:
        try:
            metric_shard_url = get_authoritative_node(current_skyline_app, base_name, return_fqdn=True)
        except Exception as err:
            current_logger.error('error :: api_get_analysed_timeseries :: get_metric_shard_host failed for %s, err: %s' % (
                base_name, err))
    remote_analysed_timeseries = {}
    if metric_shard_url:
        current_logger.info('api_get_analysed_timeseries checking %s for analysed_timeseries' % str(metric_shard_url))
        api_uri = 'get_analysed_timeseries=true&metric=%s&anomaly_timestamp=%s&only_host=%s&cluster_data=false&cluster_call=true' % (
            str(metric), str(anomaly_timestamp), str(metric_shard_url))
        try:
            remote_analysed_timeseries = get_cluster_data(api_uri, 'analysed_timeseries')
        except Exception as err:
            current_logger.error('error :: %s :: failed to get analysed_timeseries from remote instance, err: %s' % (
                function_str, err))
            remote_analysed_timeseries = []
    if remote_analysed_timeseries:
        try:
            analysed_timeseries = remote_analysed_timeseries['data']['analysed_timeseries']
        except Exception as err:
            current_logger.error('error :: %s :: failed to get analysed_timeseries from remote_analysed_timeseries, err: %s' % (
                function_str, err))

    current_logger.info('%s :: returning analysed_timeseries of length: %s' % (
        function_str, str(len(analysed_timeseries))))
    return analysed_timeseries
