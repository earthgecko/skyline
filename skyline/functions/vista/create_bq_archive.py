"""
create_bq_archive.py
"""
import copy
import logging
import os

from datetime import datetime, timezone
from time import time

import numpy as np
import pandas as pd

import settings
from skyline_functions import (
    get_redis_conn_decoded, mkdir_p, write_data_to_file)
from functions.graphite.get_metrics_timeseries import get_metrics_timeseries
from functions.metrics.get_base_names_and_metric_ids import get_base_names_and_metric_ids
from functions.settings.get_bq_accounts_settings import get_bq_accounts_settings
from functions.cluster.is_shard_metric import is_shard_metric
from functions.redis.get_redis_metrics_timeseries import get_redis_metrics_timeseries

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


# @added 20240612 - Feature #5374: vista - create_bq_archive
def create_bq_archive(current_skyline_app, vista_bq_account_key, for_timestamp):
    """
    Create a bq archive csv file for a date if one does not exist using data
    from Redis for local metrics and data from Graphite for remote metrics.

    :param current_skyline_app: the app calling the function
    :param vista_bq_account: the Vista BQ account
    :param for_timestamp: the from unix timestamp for the archive to be created
        this will be used to create the archive filename using the date_format
        from the account settings
    :type current_skyline_app: str
    :type vista_bq_account: str
    :return: metrics_data, archive_file
    :rtype: tuple (dict, str)

    """

    start = time()

    metrics_data = {}
    archive_file = None

    function_str = 'functions.vista.create_bq_archive'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    vista_bq_accounts = {}
    try:
        vista_bq_accounts = get_bq_accounts_settings(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_bq_accounts_settings failed, err: %s' % (
            function_str, err))

    bq_account_settings = {}
    try:
        bq_account_settings = copy.deepcopy(vista_bq_accounts[vista_bq_account_key])
    except Exception as err:
        current_logger.error('error :: %s :: failed to determine settings for %s, err: %s' % (
            function_str, vista_bq_account_key, err))

    if not bq_account_settings:
        err_msg = 'failed to determine settings for %s' % vista_bq_account_key
        current_logger.error('error :: %s :: %s' % (function_str, err_msg))
        return metrics_data, archive_file

    batch_processing_namespace = None
    try:
        batch_processing_namespace = bq_account_settings['batch_processing_namespace']
    except Exception as err:
        current_logger.error('error :: %s :: failed to determine batch_processing_namespace from bq_account_settings: %s, err: %s' % (
            function_str, str(bq_account_settings), err))
        return metrics_data, archive_file

    base_names_with_ids = {}
    try:
        base_names_with_ids = get_base_names_and_metric_ids(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_base_names_and_metric_ids failed, err: %s' % (
            function_str, err))

    metrics = [base_name for base_name in list(base_names_with_ids.keys()) if batch_processing_namespace in base_name]

    redis_metrics = list(metrics)
    graphite_metrics = []
    if HORIZON_SHARDS:
        redis_metrics = []
        graphite_metrics = []
        for base_name in metrics:
            shard_metric = is_shard_metric(base_name)
            if shard_metric:
                redis_metrics.append(base_name)
            else:
                graphite_metrics.append(base_name)
    current_logger.info('%s :: %s - %s Redis metrics to fetch and %s Graphite metrics to fetch, total metrics to fetch: %s' % (
        function_str, vista_bq_account_key, str(len(redis_metrics)),
        str(len(graphite_metrics)), str(len(metrics))))

    metrics_data = {}
    # Split the metrics into chunks of 300
    errors = []
    for i in range(0, len(redis_metrics), 300):
        metrics_timeseries = {}
        try:
            assigned_metrics = redis_metrics[i:(i + 300)]
            metrics_timeseries = get_redis_metrics_timeseries(current_skyline_app, assigned_metrics, log=False)
        except Exception as err:
            current_logger.error('error :: %s :: get_redis_metrics_timeseries failed - %s' % (
                function_str, err))
        for base_name in list(metrics_timeseries.keys()):
            timeseries = []
            try:
                timeseries = metrics_timeseries[base_name]
            except Exception as err:
                errors.append([base_name, 'no time series from Redis', err])
                graphite_metrics.append(base_name)
                continue
            reversed_timeseries = list(reversed(timeseries))
            for timestamp, value in reversed_timeseries:
                if int(timestamp) == for_timestamp:
                    if isinstance(value, float):
                        metrics_data[base_name] = {'timestamp': int(timestamp), 'value': value, 'source': 'redis'}
                        break
                if int(timestamp) < for_timestamp:
                    break
    current_logger.info('%s :: data for %s metrics at %s fetched from Redis' % (
        function_str, str(len(metrics_data)), str(for_timestamp)))

    # Split the metrics into chunks of 300
    if len(graphite_metrics):
        fetched_from_graphite = 0
        for i in range(0, len(graphite_metrics), 300):
            metrics_timeseries = {}
            assigned_metrics = graphite_metrics[i:(i + 300)]
            metrics_functions = {}
            for base_name in assigned_metrics:
                metrics_functions[base_name] = {}
                metrics_functions[base_name]['functions'] = None
            current_logger.info('%s :: fetching time series data for %s metrics from Graphite' % (
                function_str, str(len(metrics_functions))))
            try:
                metrics_timeseries = get_metrics_timeseries(current_skyline_app, metrics_functions, (for_timestamp - 1), (for_timestamp + 1), log=True)
            except Exception as err:
                current_logger.error('error :: %s :: get_metrics_timeseries failed, err: %s' % (
                    function_str, err))
            for base_name in list(metrics_timeseries.keys()):
                timeseries = []
                try:
                    timeseries = metrics_timeseries[base_name]['timeseries']
                    if timeseries is None:
                        err_msg = 'timeseries from Graphite is None, from_timestamp: %s, until_timestamp: %s' % (
                           str(for_timestamp - 1), str(for_timestamp + 1))
                        errors.append([base_name, err_msg])
                        continue
                except Exception as err:
                    err_msg = 'no time series from Graphite, from_timestamp: %s, until_timestamp: %s' % (
                        str(for_timestamp - 1), str(for_timestamp + 1))
                    errors.append([base_name, err_msg, err])
                    continue
                if len(timeseries) > 0:
                    reversed_timeseries = list(reversed(timeseries))
                    for timestamp, value in reversed_timeseries:
                        if int(timestamp) == for_timestamp:
                            if isinstance(value, float):
                                metrics_data[base_name] = {'timestamp': int(timestamp), 'value': value, 'source': 'graphite'}
                                fetched_from_graphite += 1
                                break
        current_logger.info('%s :: data for %s metrics at %s fetched from Graphite' % (
            function_str, str(fetched_from_graphite), str(for_timestamp)))

    if len(errors) > 0:
        current_logger.debug('debug :: %s :: %s errors reported, errors[-3:]: %s' % (
            function_str, str(len(errors)), str(errors[-3:])))

    current_logger.info('%s :: data for %s metrics at %s fetched from Graphite' % (
        function_str, str(fetched_from_graphite), str(for_timestamp)))

    # Create DataFrame metrics
    metric_prefix = bq_account_settings['metric_prefix']
    primary_dimension = bq_account_settings['primary_dimension']
    metric_dimensions = bq_account_settings['metric_dimensions']
    date_field = bq_account_settings['date_field']
    metric_format = bq_account_settings['metric_format']
    date_field = bq_account_settings['date_field']
    date_field_format = bq_account_settings['date_field_format']

    date_field_value = datetime.fromtimestamp(for_timestamp, tz=timezone.utc).strftime(date_field_format)
    assigned_metrics = list(metrics_data.keys())
    primary_dimension_data = {}
    for base_name in list(metrics_data.keys()):
        use_base_name = str(base_name)
        # Strip up to the metric_prefix
        if not base_name.startswith(metric_prefix):
            metricname_start = base_name.find(metric_prefix)
            if metricname_start != -1:
                use_base_name = base_name[metricname_start:]
            else:
                use_base_name = str(base_name)
        primary_dimension_value = use_base_name.split('.')[-2]

        if primary_dimension_value not in list(primary_dimension_data.keys()):
            primary_dimension_data[primary_dimension_value] = {}
        for metric_dimension in metric_dimensions:
            if metric_dimension in use_base_name:
                primary_dimension_data[primary_dimension_value][metric_dimension] = metrics_data[base_name]['value']
            else:
                primary_dimension_data[primary_dimension_value][metric_dimension] = np.nan

    df_data_list = []
    for primary_dimension_element in list(primary_dimension_data.keys()):
        metric_data = [date_field_value, primary_dimension_element]
        for metric_dimension in primary_dimension_data[primary_dimension_element].keys():
            value = np.nan
            if metric_dimension in primary_dimension_data[primary_dimension_element].keys():
                value = primary_dimension_data[primary_dimension_element][metric_dimension]
            metric_data.append(value)
        df_data_list.append(metric_data)

    current_logger.info('%s :: creating DataFrame with %s rows' % (
        function_str, str(len(df_data_list))))

    df = []
    if len(df_data_list) > 0:
        try:
            columns = [date_field, primary_dimension]
            for metric_dimension in metric_dimensions:
                columns.append(metric_dimension)
            df = pd.DataFrame(df_data_list, columns=columns)
            current_logger.info('%s :: created df, len(df): %s' % (
                function_str, str(len(df))))
        except Exception as err:
            current_logger.error('error :: %s :: failed to create df, err: %s' % (
                function_str, err))
            df = []

    date_folder_ts = int(start)
    date_folder = datetime.fromtimestamp(date_folder_ts, tz=timezone.utc).strftime('%Y%m%d%H%M%S')
    archive_data_path = '%s/vista/bq_archives/%s/%s' % (
        settings.SKYLINE_DIR, vista_bq_account_key, date_folder)
    archive_csv_filename = '%s.%s.csv' % (vista_bq_account_key, str(date_field_value))
    archive_csv = '%s/%s' % (str(archive_data_path), archive_csv_filename)

    if len(df) > 0:
        if not os.path.isdir(archive_data_path):
            try:
                mkdir_p(archive_data_path)
            except Exception as err:
                err_msg = 'failed to create archive_data_path: %s' % archive_data_path
                current_logger.error('error :: %s :: %s, err: %s' % (
                    function_str, err_msg, err))
        if not os.path.isfile(archive_csv):
            current_logger.info('%s :: archiving data for %s to %s' % (
                function_str, str(date_field_value), archive_csv))
            try:
                df.to_csv(archive_csv, index=False)
            except Exception as err:
                err_msg = 'df.to_csv failed to create archive_csv: %s' % archive_csv
                current_logger.error('error :: %s :: %s, err: %s' % (
                    function_str, err_msg, err))
            if os.path.isfile(archive_csv):
                archive_file = str(archive_csv)
                current_logger.info('%s :: archived data for %s to %s' % (
                    function_str, str(date_field_value), archive_csv))
            else:
                current_logger.error('error :: %s :: df.to_csv failed to create archive_csv: %s' % (
                    function_str, archive_csv))

    # Create archive csv
    current_logger.info('%s :: returning data for %s metrics at %s' % (
        function_str, str(len(metrics_data)), str(for_timestamp)))

    return metrics_data, archive_file
