"""
api_create_training_data.py
"""
import os
import logging
import os
from time import time

from flask import request

import settings
# from backend import get_cluster_data
from skyline_functions import (
    get_graphite_metric, mkdir_p, write_data_to_file,
)
from functions.database.queries.get_algorithms import get_algorithms
from functions.database.queries.get_apps import get_apps
from functions.database.queries.query_anomalies import get_anomaly
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric


# @added 20231003 - Feature #5092: ionosphere - create_training_data
def api_create_training_data(current_skyline_app, cluster_data=False):
    """
    Create training data for an anomaly if it does not exist.  No echo features
    profile will be created.

    :param current_skyline_app: the app calling the function
    :param cluster_data: the cluster_data parameter from the request
    :type current_skyline_app: str
    :type cluster_data: bool
    :return: training_data_url
    :rtype: str

    """
    function_str = 'api_create_training_data'
    training_data = {
        'anomaly_id': None,
        'status': 500,
        'error': None,
        'training_data_url': None,
    }

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    anomaly_id = None
    try:
        anomaly_id_str = request.args.get('anomaly_id')
        anomaly_id = int(anomaly_id_str)
        current_logger.info('api_create_training_data :: with anomaly_id: %s' % str(anomaly_id))
    except Exception as err:
        error_reported = 'invalid anomaly id passed - %s' % err
        current_logger.error('error :: api_create_training_data ::  %s' % error_reported)
        training_data['status'] = 400
        training_data['error'] = error_reported
        return training_data
    training_data['anomaly_id'] = anomaly_id
    anomaly = {}
    try:
        anomaly = get_anomaly(current_skyline_app, anomaly_id)
    except Exception as err:
        error_reported = 'invalid anomaly id passed, no anomaly found - %s' % err
        current_logger.error('error :: api_create_training_data :: %s' % error_reported)
        training_data['status'] = 400
        training_data['error'] = error_reported
        return training_data
    if not anomaly:
        error_reported = 'invalid anomaly id passed, no anomaly found - %s' % err
        current_logger.error('error :: api_create_training_data :: %s' % error_reported)
        training_data['status'] = 400
        training_data['error'] = error_reported
        return training_data

    training_data['anomaly'] = anomaly
    metric_id = training_data['anomaly']['metric_id']
    metric = None
    try:
        metric = get_base_name_from_metric_id(current_skyline_app, metric_id)
    except Exception as err:
        current_logger.error('error :: get_base_name_from_metric_id failed to determine metric from metric_id: %s - %s' % (
            str(metric_id), str(err)))
        error_reported = 'no metric determined related to the anomaly - %s' % err
        current_logger.error('error :: api_create_training_data :: %s' % error_reported)
        training_data['status'] = 400
        training_data['error'] = error_reported
        return training_data

    training_data['metric'] = metric
    try:
        anomaly_timestamp = training_data['anomaly']['anomaly_timestamp']
        full_duration = training_data['anomaly']['full_duration']
        value = training_data['anomaly']['anomalous_datapoint']
        app_id = training_data['anomaly']['app_id']
    except Exception as err:
        current_logger.error('error :: failed to determine anomaly details - %s' % (
            str(err)))
        error_reported = 'failed to determine the required anomaly details - %s' % err
        training_data['status'] = 400
        training_data['error'] = error_reported
        return training_data

    from_timestamp = anomaly_timestamp - full_duration
    if anomaly_timestamp < (time() - (86400 * 3)):
        error_reported = 'the anomaly is too old to train on'
        training_data['status'] = 400
        training_data['error'] = error_reported
        return training_data

    use_base_name = str(metric)
    if '_tenant_id="' in metric:
        use_base_name = 'labelled_metrics.%s' % str(metric_id)

    # Create training data dir
    metric_timeseries_dir = use_base_name.replace('.', '/')
    training_data_dir = '%s/%s/%s' % (
        settings.IONOSPHERE_DATA_FOLDER, str(anomaly_timestamp),
        metric_timeseries_dir)

    training_data_url = '%s/ionosphere?timestamp=%s&metric=%s&requested_timestamp=%s' % (
        settings.SKYLINE_URL, str(anomaly_timestamp), use_base_name,
        str(anomaly_timestamp))

    metric_vars_file = '%s/%s.txt' % (training_data_dir, use_base_name)
    timeseries_json = '%s/%s.json' % (training_data_dir, use_base_name)

    algorithms_run_ids = []
    try:
        algorithms_run_ids = training_data['anomaly']['algorithms_run'].split(',')
        triggered_algorithms_ids = training_data['anomaly']['triggered_algorithms'].split(',')
        algorithms = {}
        all_algorithms_by_id = {}
        try:
            algorithms, all_algorithms_by_id = get_algorithms(current_skyline_app, return_all_algorithms_by_id=True)
        except Exception as err:
            current_logger.error('error :: api_create_training_data :: get_algorithms failed - %s' % err)
        all_algorithms_by_name = {}
        for id in list(all_algorithms_by_id.keys()):
            all_algorithms_by_name[all_algorithms_by_id[id]] = id
    except Exception as err:
        current_logger.error('error :: api_create_training_data :: failed to determine algorithms - %s' % (
            err))

    algorithms_run = []
    for aid in algorithms_run_ids:
        algorithms_run.append(all_algorithms_by_id[int(aid)])

    triggered_algorithms = []
    for aid in triggered_algorithms_ids:
        triggered_algorithms.append(all_algorithms_by_id[int(aid)])

    added_by = None
    try:
        apps_dict = get_apps(current_skyline_app)
        for app in list(apps_dict.keys()):
            if apps_dict[app] == app_id:
                added_by = str(app)
                break
    except Exception as err:
        current_logger.error('error :: api_create_training_data :: get_apps failed - %s' % err)

    # Make sure that boundary and crucible things are not trained on
    if added_by in ['boundary', 'crucible']:
        error_reported = 'the anomaly is a threshold type and cannot be train on'
        training_data['status'] = 400
        training_data['error'] = error_reported
        return training_data

    graphite_metric = True
    if use_base_name.startswith('labelled_metrics.'):
        graphite_metric = False
    parent_id = 0

    # Note:
    # The values are enclosed is single quoted intentionally
    # as the imp.load_source used in crucible results in a
    # shift in the decimal position when double quoted, e.g.
    # value = "5622.0" gets imported as
    # 2016-03-02 12:53:26 :: 28569 :: metric variable - value - 562.2
    # single quoting results in the desired,
    # 2016-03-02 13:16:17 :: 1515 :: metric variable - value - 5622.0
    anomaly_data = None
    try:
        anomaly_data = 'metric = \'%s\'\n' \
                    'value = \'%s\'\n' \
                    'from_timestamp = \'%s\'\n' \
                    'metric_timestamp = \'%s\'\n' \
                    'algorithms = %s\n' \
                    'triggered_algorithms = %s\n' \
                    'anomaly_dir = \'%s\'\n' \
                    'graphite_metric = %s\n' \
                    'run_crucible_tests = False\n' \
                    'added_by = \'%s\'\n' \
                    'added_at = \'%s\'\n' \
                    'full_duration = \'%s\'\n' \
                    'ionosphere_parent_id = \'%s\'\n' \
                    'algorithms_run = %s\n' \
            % (str(use_base_name), str(value), str(from_timestamp),
                str(anomaly_timestamp), str(algorithms_run),
                str(triggered_algorithms), training_data_dir,
                str(graphite_metric), str(added_by),
                str(anomaly_timestamp), str(full_duration), str(parent_id),
                algorithms_run)
    except Exception as err:
        current_logger.error('error :: api_create_training_data :: failed to construct anomaly_data - %s' % (err))
        training_data['error'] = 'failed to construct anomaly_data for training'


    if os.path.exists(timeseries_json) and os.path.exists(metric_vars_file):
        training_data['status'] = 200
        training_data['training_data_url'] = training_data_url
        del training_data['error']
        return training_data

    if not os.path.exists(training_data_dir):
        mkdir_p(training_data_dir)
        current_logger.info('api_create_training_data :: created dir - %s' % training_data_dir)

    metric_json_file_saved = False
    if use_base_name.startswith('labelled_metrics.'):
        try:
            # get_victoriametrics_metric automatically applies the rate and
            # step required no downsampling or nonNegativeDerivative is
            # required.
            metric_json_file_saved = get_victoriametrics_metric(
                current_skyline_app, metric, from_timestamp, anomaly_timestamp,
                'json', timeseries_json, metric_data={})
            if metric_json_file_saved:
                current_logger.info('api_create_training_data :: %s time series data saved to %s' % (use_base_name, metric_json_file_saved))
        except Exception as err:
            current_logger.error('error :: api_create_training_data :: get_victoriametrics_metric failed to surface data for %s - %s' % (
                str(metric), err))
    else:
        try:
            metric_json_file_saved = get_graphite_metric(
                current_skyline_app, metric, from_timestamp, anomaly_timestamp,
                'json', timeseries_json)
            if metric_json_file_saved:
                current_logger.info('api_create_training_data :: %s time series data saved' % metric)
        except Exception as err:
            current_logger.error('error :: api_create_training_data :: get_graphite_metric failed to surface data for %s - %s' % (
                str(metric), err))

    if not metric_json_file_saved:
        error_reported = 'failed to surface data to train with'
        training_data['status'] = 500
        training_data['error'] = error_reported
        return training_data

    if anomaly_data:
        try:
            write_data_to_file(str(current_skyline_app), metric_vars_file, 'w', anomaly_data)
            current_logger.info(
                'api_create_training_data :: added metric_vars_file :: %s' % (
                    metric_vars_file))
        except Exception as err:
            current_logger.error('error :: api_create_training_data :: write_data_to_file failed - %s' % (err))
            training_data['error'] = 'failed to write anomaly_data for training'

    if os.path.exists(timeseries_json) and os.path.exists(metric_vars_file):
        training_data['status'] = 200
        training_data['training_data_url'] = training_data_url
        del training_data['error']

    return training_data
