from __future__ import division
import logging
from os import path, walk, listdir, remove
# import string
import operator
import time
import re
# import csv
# import datetime
import shutil
import glob
from ast import literal_eval
from sys import version_info

import traceback
from flask import request
import requests
# from redis import StrictRedis

# from sqlalchemy import (
#    create_engine, Column, Table, Integer, String, MetaData, DateTime)
# from sqlalchemy.dialects.mysql import DOUBLE, TINYINT
from sqlalchemy.sql import select
# import json
# from tsfresh import __version__ as tsfresh_version

# @added 20170916 - Feature #1996: Ionosphere - matches page
from pymemcache.client.base import Client as pymemcache_Client

# @added 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
#                   Bug #2818: Mutliple SQL Injection Security Vulnerabilities
from sqlalchemy.sql import text

import settings
import skyline_version
# from skyline_functions import (
#    RepresentsInt, mkdir_p, write_data_to_file, get_graphite_metric)
from skyline_functions import (
    mkdir_p, get_graphite_metric, write_data_to_file,
    # @added 20201213 - Feature #3890: metrics_manager - sync_cluster_files
    get_redis_conn_decoded,
    # @added 20210413 - Feature #4014: Ionosphere - inference
    #                   Branch #3590: inference
    mysql_select,
)

# @added 20200813 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
from skyline_functions import historical_data_dir_exists

# from tsfresh_feature_names import TSFRESH_FEATURES

from database import (
    get_engine, ionosphere_table_meta, metrics_table_meta,
    ionosphere_matched_table_meta,
    # @added 20170305 - Feature #1960: ionosphere_layers
    ionosphere_layers_table_meta, layers_algorithms_table_meta,
    # @added 20170307 - Feature #1960: ionosphere_layers
    # To present matched layers Graphite graphs
    ionosphere_layers_matched_table_meta,
    # @added 20190502 - Branch #2646: slack
    anomalies_table_meta,
    # @added 20200929 - Task #3748: POC SNAB
    #                   Branch #3068: SNAB
    snab_table_meta,
    # @added 20210413 - Feature #4014: Ionosphere - inference
    #                   Branch #3590: inference
    motifs_matched_table_meta,
)
from motif_match_types import motif_match_types_dict
from functions.numpy.percent_different import get_percent_different

# @added 20190502 - Branch #2646: slack
from slack_functions import slack_post_message, slack_post_reaction

# @added 20200512 - Bug #2534: Ionosphere - fluid approximation - IONOSPHERE_MINMAX_SCALING_RANGE_TOLERANCE on low ranges
#                   Feature #2404: Ionosphere - fluid approximation
from ionosphere_functions import create_fp_ts_graph

# @added 20210414 - Feature #4014: Ionosphere - inference
#                   Branch #3590: inference
from motif_plots import plot_motif_match

# @added 20210430 - Feature #4014: Ionosphere - inference
from functions.database.queries.fp_timeseries import get_db_fp_timeseries
from functions.database.queries.metric_id_from_base_name import metric_id_from_base_name
from functions.database.queries.metric_ids_from_metric_like import metric_ids_from_metric_like

skyline_version = skyline_version.__absolute_version__
skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
try:
    ENABLE_WEBAPP_DEBUG = settings.ENABLE_WEBAPP_DEBUG
except EnvironmentError as e:
    logger.error('error :: cannot determine ENABLE_WEBAPP_DEBUG from settings - %s' % e)
    ENABLE_WEBAPP_DEBUG = False

try:
    full_duration_seconds = int(settings.FULL_DURATION)
except:
    full_duration_seconds = 86400

full_duration_in_hours = full_duration_seconds / 60 / 60
exclude_redis_json = 'redis.%sh.json' % str(int(full_duration_in_hours))

# @added 20190502 - Branch #2646: slack
channel_id = None
try:
    SLACK_ENABLED = settings.SLACK_ENABLED
except:
    SLACK_ENABLED = False
try:
    slack_thread_updates = settings.SLACK_OPTS['thread_updates']
except:
    slack_thread_updates = False
if slack_thread_updates and SLACK_ENABLED:
    update_slack_thread = True
else:
    update_slack_thread = False
if update_slack_thread:
    try:
        channel = settings.SLACK_OPTS['default_channel']
        channel_id = settings.SLACK_OPTS['default_channel_id']
    except:
        channel = False
        channel_id = False
    throw_exception_on_default_channel = False
    if channel == 'YOUR_default_slack_channel':
        throw_exception_on_default_channel = True
    if channel_id == 'YOUR_default_slack_channel_id':
        throw_exception_on_default_channel = True

# @added 20191029 - Branch #3262: py3
# Allow for the use of self signed SSL certificates even if not running on
# docker.
try:
    overall_verify_ssl = settings.VERIFY_SSL
except:
    overall_verify_ssl = True
if overall_verify_ssl:
    logging.captureWarnings(True)

# @added 20200731 - Feature #3654: IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE
try:
    IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE = settings.IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE
except:
    IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE = False

# @added 20200813 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
try:
    IONOSPHERE_HISTORICAL_DATA_FOLDER = settings.IONOSPHERE_HISTORICAL_DATA_FOLDER
except:
    IONOSPHERE_HISTORICAL_DATA_FOLDER = '/opt/skyline/ionosphere/historical_data'
try:
    IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR = settings.IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
except:
    IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR = []
# @added 20210421 - Feature #4014: Ionosphere - inference
try:
    IONOSPHERE_INFERENCE_MOTIFS_ENABLED = settings.IONOSPHERE_INFERENCE_MOTIFS_ENABLED
except:
    IONOSPHERE_INFERENCE_MOTIFS_ENABLED = True
# @added 20210419 - Feature #4014: Ionosphere - inference
# Only store motif data in the database if specifically enabled
try:
    IONOSPHERE_INFERENCE_STORE_MATCHED_MOTIFS = settings.IONOSPHERE_INFERENCE_STORE_MATCHED_MOTIFS
except:
    IONOSPHERE_INFERENCE_STORE_MATCHED_MOTIFS = False


def ionosphere_get_metrics_dir(requested_timestamp, context):
    """
    Get a list of all the metrics in timestamp training data or features profile
    folder

    :param requested_timestamp: the training data timestamp
    :param context: the request context, training_data or features_profiles
    :type requested_timestamp: str
    :type context: str
    :return: tuple of lists
    :rtype:  (list, list, list, list)

    """
    if context == 'training_data':
        log_context = 'training data'
    if context == 'features_profiles':
        log_context = 'features profile data'
    logger.info(
        'Metrics requested for timestamp %s dir %s' % (
            log_context, str(requested_timestamp)))
    if context == 'training_data':
        data_dir = '%s' % settings.IONOSPHERE_DATA_FOLDER
    if context == 'features_profiles':
        data_dir = '%s' % (settings.IONOSPHERE_PROFILES_FOLDER)
        # @added 20160113 - Feature #1858: Ionosphere - autobuild features_profiles dir
        if settings.IONOSPHERE_AUTOBUILD:
            # TODO: see ionosphere docs page.  Create any deleted/missing
            #       features_profiles dir with best effort with the data that is
            #       available and DB data on-demand
            # Build the expected features_profiles dirs from the DB and auto
            # provision any that are not present
            if not path.exists(data_dir):
                # provision features_profiles image resources
                mkdir_p(data_dir)

    metric_paths = []
    metrics = []
    timestamps = []
    human_dates = []
    for root, dirs, files in walk(data_dir):
        for file in files:
            if file.endswith('.json'):
                data_file = True
                if re.search(exclude_redis_json, file):
                    data_file = False
                if re.search('mirage.redis.json', file):
                    data_file = False
                if re.search(requested_timestamp, root) and data_file:
                    metric_name = file.replace('.json', '')
                    add_metric = True
                else:
                    add_metric = False
                if add_metric:
                    metric_paths.append([metric_name, root])
                    metrics.append(metric_name)
                    if context == 'training_data':
                        timestamp = int(root.split('/')[5])
                    if context == 'features_profiles':
                        timestamp = int(path.split(root)[1])
                    timestamps.append(timestamp)

    # @added 20200814 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
    # Return historical metrics as well if enabled
    if IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR and context == 'training_data':
        for root, dirs, files in walk(IONOSPHERE_HISTORICAL_DATA_FOLDER):
            for file in files:
                if file.endswith('.json'):
                    data_file = True
                    if re.search(exclude_redis_json, file):
                        data_file = False
                    if re.search('mirage.redis.json', file):
                        data_file = False
                    if re.search(requested_timestamp, root) and data_file:
                        metric_name = file.replace('.json', '')
                        for historical_metric_namespace in IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR:
                            if historical_metric_namespace in metric_name:
                                add_metric = True
                    else:
                        add_metric = False
                    if add_metric:
                        metric_paths.append([metric_name, root])
                        metrics.append(metric_name)
                        if context == 'training_data':
                            timestamp = int(root.split('/')[5])
                        if context == 'features_profiles':
                            timestamp = int(path.split(root)[1])
                        timestamps.append(timestamp)

    set_unique_metrics = set(metrics)
    unique_metrics = list(set_unique_metrics)
    unique_metrics.sort()
    set_unique_timestamps = set(timestamps)
    unique_timestamps = list(set_unique_timestamps)
    unique_timestamps.sort()
    for i_ts in unique_timestamps:
        human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(int(i_ts)))
        human_dates.append(human_date)

    return (metric_paths, unique_metrics, unique_timestamps, human_dates)


def ionosphere_data(requested_timestamp, data_for_metric, context):
    """
    Get a list of all training data or profiles folders and metrics

    :param requested_timestamp: the training data or profile timestamp
    :param data_for_metric: the metric base_name
    :param context: the request context, training_data or features_profiles
    :type requested_timestamp: str
    :type data_for_metric: str
    :type context: str
    :return: tuple of lists
    :rtype:  (list, list, list, list)

    """
    # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
    # base_name = data_for_metric.replace(settings.FULL_NAMESPACE, '', 1)
    if data_for_metric.startswith(settings.FULL_NAMESPACE):
        base_name = data_for_metric.replace(settings.FULL_NAMESPACE, '', 1)
    else:
        base_name = data_for_metric

    if context == 'training_data':
        log_context = 'training data'
    if context == 'features_profiles':
        log_context = 'features profile data'
    logger.info(
        '%s requested for %s at timestamp %s' %
        (log_context, str(base_name), str(requested_timestamp)))
    if requested_timestamp:
        timeseries_dir = base_name.replace('.', '/')
        if context == 'training_data':
            data_dir = '%s/%s/%s' % (
                settings.IONOSPHERE_DATA_FOLDER, requested_timestamp,
                timeseries_dir)
        if context == 'features_profiles':
            data_dir = '%s/%s/%s' % (
                settings.IONOSPHERE_PROFILES_FOLDER, timeseries_dir,
                requested_timestamp)
    else:
        if context == 'training_data':
            data_dir = '%s' % settings.IONOSPHERE_DATA_FOLDER
        if context == 'features_profiles':
            data_dir = '%s' % (settings.IONOSPHERE_PROFILES_FOLDER)

    metric_paths = []
    metrics = []
    timestamps = []
    human_dates = []
    if context == 'training_data':
        data_dir = '%s' % settings.IONOSPHERE_DATA_FOLDER
    if context == 'features_profiles':
        data_dir = '%s' % settings.IONOSPHERE_PROFILES_FOLDER
    for root, dirs, files in walk(data_dir):
        for file in files:
            if file.endswith('.json'):
                data_file = True
                if re.search(exclude_redis_json, file):
                    data_file = False
                if re.search('mirage.redis.json', file):
                    data_file = False
                if re.search('\\d{10}', root) and data_file:
                    metric_name = file.replace('.json', '')
                    if data_for_metric != 'all':
                        add_metric = False
                        if metric_name == base_name:
                            add_metric = True
                        if requested_timestamp:
                            if re.search(requested_timestamp, file):
                                add_metric = True
                            else:
                                add_metric = False
                        if add_metric:
                            metric_paths.append([metric_name, root])
                            metrics.append(metric_name)
                            if context == 'training_data':
                                timestamp = int(root.split('/')[5])
                            if context == 'features_profiles':
                                timestamp = int(path.split(root)[1])
                            timestamps.append(timestamp)
                    else:
                        metric_paths.append([metric_name, root])
                        metrics.append(metric_name)
                        if context == 'training_data':
                            timestamp = int(root.split('/')[5])
                        if context == 'features_profiles':
                            timestamp = int(path.split(root)[1])
                        timestamps.append(timestamp)

    # @added 20200813 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
    # If the IONOSPHERE_HISTORICAL_DATA_FOLDER dir exist iterate it and
    # and historical training data to the list.
    if IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR and context == 'training_data':
        check_for_historical_training_data = False
        for historical_metric_namespace in IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR:
            if historical_metric_namespace in base_name:
                check_for_historical_training_data = True

        # @added 20200814 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
        # Return historical metrics if this is an 'all' request to list_by date
        # or metric (if no training data exists for the metric)
        if base_name in 'all':
            check_for_historical_training_data = True

        historical_data_path_exists = False
        if check_for_historical_training_data:
            if path.exists(IONOSPHERE_HISTORICAL_DATA_FOLDER):
                historical_data_path_exists = True
        if check_for_historical_training_data and historical_data_path_exists:
            for root, dirs, files in walk(IONOSPHERE_HISTORICAL_DATA_FOLDER):
                for file in files:
                    if file.endswith('.json'):
                        data_file = True
                        if re.search(exclude_redis_json, file):
                            data_file = False
                        if re.search('mirage.redis.json', file):
                            data_file = False
                        if re.search('\\d{10}', root) and data_file:
                            metric_name = file.replace('.json', '')
                            if data_for_metric != 'all':
                                add_metric = False
                                if metric_name == base_name:
                                    add_metric = True
                                if requested_timestamp:
                                    if re.search(requested_timestamp, file):
                                        add_metric = True
                                    else:
                                        add_metric = False
                                if add_metric:
                                    metric_paths.append([metric_name, root])
                                    metrics.append(metric_name)
                                    if context == 'training_data':
                                        timestamp = int(root.split('/')[5])
                                    if context == 'features_profiles':
                                        timestamp = int(path.split(root)[1])
                                    timestamps.append(timestamp)
                            else:
                                metric_paths.append([metric_name, root])
                                metrics.append(metric_name)
                                if context == 'training_data':
                                    timestamp = int(root.split('/')[5])
                                if context == 'features_profiles':
                                    timestamp = int(path.split(root)[1])
                                timestamps.append(timestamp)

    set_unique_metrics = set(metrics)
    unique_metrics = list(set_unique_metrics)
    unique_metrics.sort()
    set_unique_timestamps = set(timestamps)
    unique_timestamps = list(set_unique_timestamps)
    unique_timestamps.sort()
    for i_ts in unique_timestamps:
        human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(i_ts)))
        human_dates.append(human_date)

    return (metric_paths, unique_metrics, unique_timestamps, human_dates)


def get_an_engine():

    try:
        engine, fail_msg, trace = get_engine(skyline_app)
        return engine, fail_msg, trace
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to get MySQL engine for'
        logger.error('%s' % fail_msg)
        # return None, fail_msg, trace
        raise  # to webapp to return in the UI


def engine_disposal(engine):
    if engine:
        try:
            engine.dispose()
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: calling engine.dispose()')
    return


def ionosphere_metric_data(requested_timestamp, data_for_metric, context, fp_id):
    """
    Get a list of all training data folders and metrics
    """

    # @added 20170104 - Feature #1842: Ionosphere - Graphite now graphs
    #                   Feature #1830: Ionosphere alerts
    # Use the new_load_metric_vars method
    def new_load_metric_vars(metric_vars_file):
        """
        Load the metric variables for a check from a metric check variables file

        :param metric_vars_file: the path and filename to the metric variables files
        :type metric_vars_file: str
        :return: the metric_vars module object or ``False``
        :rtype: list

        """
        if path.isfile(metric_vars_file):
            logger.info(
                'loading metric variables from metric_check_file - %s' % (
                    str(metric_vars_file)))
        else:
            logger.error(
                'error :: loading metric variables from metric_check_file - file not found - %s' % (
                    str(metric_vars_file)))
            return False

        metric_vars = []
        with open(metric_vars_file) as f:
            for line in f:
                no_new_line = line.replace('\n', '')
                no_equal_line = no_new_line.replace(' = ', ',')
                array = str(no_equal_line.split(',', 1))
                add_line = literal_eval(array)
                metric_vars.append(add_line)

        string_keys = ['metric', 'anomaly_dir', 'added_by', 'app', 'source']
        float_keys = ['value']
        int_keys = ['from_timestamp', 'metric_timestamp', 'added_at', 'full_duration']
        array_keys = ['algorithms', 'triggered_algorithms', 'algorithms_run']
        boolean_keys = ['graphite_metric', 'run_crucible_tests']

        metric_vars_array = []
        for var_array in metric_vars:
            key = None
            value = None
            if var_array[0] in string_keys:
                key = var_array[0]
                value_str = str(var_array[1]).replace("'", '')
                value = str(value_str)
                if var_array[0] == 'metric':
                    metric = value
            if var_array[0] in float_keys:
                key = var_array[0]
                value_str = str(var_array[1]).replace("'", '')
                value = float(value_str)
            if var_array[0] in int_keys:
                key = var_array[0]
                value_str = str(var_array[1]).replace("'", '')
                value = int(value_str)
            if var_array[0] in array_keys:
                key = var_array[0]
                value = literal_eval(str(var_array[1]))
            if var_array[0] in boolean_keys:
                key = var_array[0]
                if str(var_array[1]) == 'True':
                    value = True
                else:
                    value = False
            if key:
                metric_vars_array.append([key, value])

            if len(metric_vars_array) == 0:
                logger.error(
                    'error :: loading metric variables - none found' % (
                        str(metric_vars_file)))
                return False

            if settings.ENABLE_DEBUG:
                logger.info(
                    'debug :: metric_vars determined - metric variable - metric - %s' % str(metric_vars.metric))

        # @added 20170113 - Feature #1842: Ionosphere - Graphite now graphs
        # Handle features profiles that were created pre the addition of
        # full_duration
        full_duration_present = False
        for key, value in metric_vars_array:
            if key == 'full_duration':
                full_duration_present = True
        if not full_duration_present:
            try:
                for key, value in metric_vars_array:
                    if key == 'from_timestamp':
                        value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                        use_from_timestamp = int(value_list[0])
                    if key == 'metric_timestamp':
                        value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                        use_metric_timestamp = int(value_list[0])
                round_full_duration_days = int((use_metric_timestamp - use_from_timestamp) / 86400)
                round_full_duration = int(round_full_duration_days) * 86400
                logger.info('debug :: calculated missing full_duration')
                metric_vars_array.append(['full_duration', round_full_duration])
            except:
                logger.error('error :: could not calculate missing full_duration')
                metric_vars_array.append(['full_duration', 'unknown'])

        logger.info('debug :: metric_vars for %s' % str(metric))
        logger.info('debug :: %s' % str(metric_vars_array))

        return metric_vars_array

    # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
    # base_name = data_for_metric.replace(settings.FULL_NAMESPACE, '', 1)
    if data_for_metric.startswith(settings.FULL_NAMESPACE):
        base_name = data_for_metric.replace(settings.FULL_NAMESPACE, '', 1)
    else:
        base_name = data_for_metric

    logger.info('%s requested for %s at %s' % (
        context, str(base_name), str(requested_timestamp)))
    metric_paths = []
    images = []
    timeseries_dir = base_name.replace('.', '/')

    if context == 'training_data':
        metric_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, str(requested_timestamp),
            timeseries_dir)

        # @added 20200813 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
        metric_data_dir_does_not_exist = False
        if not path.exists(metric_data_dir):
            metric_data_dir_does_not_exist = True
        if IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR and metric_data_dir_does_not_exist:
            logger.info('ionosphere_metric_data :: checking for historical training data for %s' % metric_data_dir)
            try:
                historical_data, metric_data_dir = historical_data_dir_exists('webapp', metric_data_dir)
                if historical_data:
                    logger.info('ionosphere_metric_data :: using historical training data - %s' % metric_data_dir)
                else:
                    logger.info('ionosphere_metric_data :: no historical training data found for %s' % metric_data_dir)
            except:
                trace = traceback.format_exc()
                logger.error(trace)
                fail_msg = 'error :: ionosphere_metric_data :: failed to determine whether this is historical training data'
                logger.error('%s' % fail_msg)
                if context == 'training_data':
                    # Raise to webbapp I believe to provide traceback to user in UI
                    raise
                else:
                    return False, False, False, fail_msg, trace

    if context == 'features_profiles':
        metric_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_PROFILES_FOLDER, timeseries_dir,
            str(requested_timestamp))
        # @added 20160113 - Feature #1858: Ionosphere - autobuild features_profiles dir
        if settings.IONOSPHERE_AUTOBUILD:
            # TODO: see ionosphere docs page.  Create any deleted/missing
            #       features_profiles dir with best effort with the data that is
            #       available and DB data on-demand
            if not path.exists(metric_data_dir):
                # provision features_profiles image resources
                mkdir_p(metric_data_dir)

    # @added 20170617 - Feature #2054: ionosphere.save.training_data
    if context == 'saved_training_data':
        metric_data_dir = '%s_saved/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, str(requested_timestamp),
            timeseries_dir)

    human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(requested_timestamp)))

    # @added 20190328 - Feature #2484: FULL_DURATION feature profiles
    # For ionosphere_echo features profiles
    fp_anomaly_timestamp = int(requested_timestamp)

    metric_var_filename = '%s.txt' % str(base_name)
    metric_vars_file = False
    ts_json_filename = '%s.json' % str(base_name)
    ts_json_file = 'none'

    # @added 20170309 - Feature #1960: ionosphere_layers
    # Also return the Analyzer FULL_DURATION timeseries if available in a Mirage
    # based features profile
    full_duration_in_hours = int(settings.FULL_DURATION) / 3600
    ionosphere_json_filename = '%s.mirage.redis.%sh.json' % (
        base_name, str(int(full_duration_in_hours)))
    ionosphere_json_file = 'none'

    # @added 20170308 - Feature #1960: ionosphere_layers
    layers_id_matched_file = False
    layers_id_matched = None

    # @added 20170331 - Task #1988: Review - Ionosphere layers - always show layers
    #                   Feature #1960: ionosphere_layers
    fp_id_matched_file = None
    fp_id_matched = None
    # @added 20170401 - Task #1988: Review - Ionosphere layers - added fp_details_list
    #                   Feature #1960: ionosphere_layers
    fp_created_file = None
    fp_details_list = []

    td_files = listdir(metric_data_dir)
    for i_file in td_files:
        metric_file = path.join(metric_data_dir, i_file)
        metric_paths.append([i_file, metric_file])
        if i_file.endswith('.png'):
            # @modified 20170106 - Feature #1842: Ionosphere - Graphite now graphs
            # Exclude any graphite_now png files from the images lists
            append_image = True
            if '.graphite_now.' in i_file:
                append_image = False
            # @added 20170107 - Feature #1852: Ionosphere - features_profile matched graphite graphs
            # Exclude any matched.fp-id images
            if '.matched.fp_id' in i_file:
                append_image = False
            # @added 20170308 - Feature #1960: ionosphere_layers
            #                   Feature #1852: Ionosphere - features_profile matched graphite graphs
            # Exclude any matched.fp-id images
            if '.matched.layers.fp_id' in i_file:
                append_image = False

            # @added 20210419 - Feature #4014: Ionosphere - inference
            # Plot the macthed motif
            if '.matched_motif.' in i_file:
                append_image = False

            if append_image:
                images.append(str(metric_file))
        if i_file == metric_var_filename:
            metric_vars_file = str(metric_file)
        if i_file == ts_json_filename:
            ts_json_file = str(metric_file)
        # @added 20170308 - Feature #1960: ionosphere_layers
        if '.layers_id_matched.layers_id' in i_file:
            layers_id_matched_file = str(metric_file)
        # @added 20170331 - Task #1988: Review - Ionosphere layers - always show layers
        #                   Feature #1960: ionosphere_layers
        # Added mirror functionality of the layers_id_matched_file
        # for feature profile matches too as it has proved useful
        # in the frontend with regards to training data sets being
        # matched by layers and can do the same for in the frontend
        # training data for feature profile matches too.
        if '.profile_id_matched.fp_id' in i_file:
            fp_id_matched_file = str(metric_file)
        # @added 20170401 - Task #1988: Review - Ionosphere layers - added fp_details_list
        #                   Feature #1960: ionosphere_layers
        if '.fp.created.txt' in i_file:
            fp_created_file = str(metric_file)

        # @added 20170309 - Feature #1960: ionosphere_layers
        if i_file == ionosphere_json_filename:
            ionosphere_json_file = str(metric_file)

    metric_vars_ok = False
    metric_vars = ['error: could not read metrics vars file', metric_vars_file]

    # @added 20181114 - Bug #2684: ionosphere_backend.py - metric_vars_file not set
    # Handle if the metrics_var_file has not been set and is still False so
    # that the path.isfile does not error with
    # TypeError: coercing to Unicode: need string or buffer, bool found
    metric_vars_file_exists = False
    if metric_vars_file:
        try:
            if path.isfile(metric_vars_file):
                metric_vars_file_exists = True
        except:
            logger.error('error :: metric_vars_file %s was not found' % str(metric_vars_file))

    # @modified 20181114 - Bug #2684: ionosphere_backend.py - metric_vars_file not set
    # if path.isfile(metric_vars_file):
    if metric_vars_file_exists:
        try:
            # @modified 20170104 - Feature #1842: Ionosphere - Graphite now graphs
            #                      Feature #1830: Ionosphere alerts
            # Use the new_load_metric_vars method
            # metric_vars = []
            # with open(metric_vars_file) as f:
            #     for line in f:
            #         add_line = line.replace('\n', '')
            #         metric_vars.append(add_line)
            metric_vars = new_load_metric_vars(metric_vars_file)
            metric_vars_ok = True
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            metric_vars_ok = False
            # logger.error(traceback.format_exc())
            fail_msg = metric_vars
            logger.error('%s' % fail_msg)
            logger.error('error :: failed to load metric_vars from: %s' % str(metric_vars_file))
            raise  # to webapp to return in the UI

    # TODO
    # Make a sample ts for lite frontend

    ts_json_ok = False
    # @modified 20190314 - Bug #2870: webapp incorrectly reporting no timeseries json file
    # ts_json = ['error: no timeseries json file', ts_json_file]
    ts_json = []

    if path.isfile(ts_json_file):
        try:
            # ts_json = []
            with open(ts_json_file) as f:
                for line in f:
                    ts_json.append(line)
            ts_json_ok = True

            with open((ts_json_file), 'r') as f:
                raw_timeseries = f.read()
            timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
            ts_json = literal_eval(timeseries_array_str)
        except:
            ts_json_ok = False
            # @added 20190314 - Bug #2870: webapp incorrectly reporting no timeseries json file
            ts_json = ['error: could not read the time series from the json file', ts_json_file]
            logger.error('error :: could not read the time series from the json file - %s' % str(ts_json_file))
    else:
        # @added 20190314 - Bug #2870: webapp incorrectly reporting no timeseries json file
        ts_json = ['error: no timeseries json file', ts_json_file]
        logger.error('error :: no timeseries json file - %s' % str(ts_json_file))

    # @added 20170309 - Feature #1960: ionosphere_layers
    # Also return the Analyzer FULL_DURATION timeseries if available in a Mirage
    # based features profile
    ionosphere_json = False
    ionosphere_json = []

    # @added 20170331 - Task #1988: Review - Ionosphere layers - always show layers
    #                   Feature #1960: ionosphere_layers
    # Return the anomalous_timeseries as an array to sample
    anomalous_timeseries = []

    if path.isfile(ionosphere_json_file):
        try:
            with open(ionosphere_json_file) as f:
                for line in f:
                    ionosphere_json.append(line)
            # @added 20170331 - Task #1988: Review - Ionosphere layers - always show layers
            #                   Feature #1960: ionosphere_layers
            # Return the anomalous_timeseries as an array to sample
            with open((ionosphere_json_file), 'r') as f:
                raw_timeseries = f.read()
            timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
            anomalous_timeseries = literal_eval(timeseries_array_str)
        except:
            logger.error('error :: failed to get time series from ionosphere_json_file - %s' % str(ionosphere_json_file))
    # @added 20171130 - Task #1988: Review - Ionosphere layers - always show layers
    #                   Feature #1960: ionosphere_layers
    # Return the anomalous_timeseries as an array to sample and just use the
    # ts_json file if there is no ionosphere_json_file
    if not anomalous_timeseries:
        # @modified 20191018 - Bug #3286: ionosphere_backend.py fallback to available ts_json
        # Wrapped in try except
        try:
            with open((ts_json_file), 'r') as f:
                raw_timeseries = f.read()
            timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
            anomalous_timeseries = literal_eval(timeseries_array_str)
        except:
            logger.info('ionosphere_backend :: no time series json found at %s' % ts_json_file)

    # @added 20191018 - Bug #3286: ionosphere_backend.py fallback to available ts_json
    # If no ts_json file has been found, use metric.analyzer.redis.FULL_DURATION.json
    # if it is available.  This is the fallback if no metric.json or
    # metric.mirage.redis.FULL_DURATION.json exists in the case of Analyzer only
    # metrics and raise if no anomalous_timeseries
    if not anomalous_timeseries:
        full_duration_in_hours = int(settings.FULL_DURATION) / 3600
        fallback_json_filename = '%s.analyzer.redis.%sh.json' % (
            base_name, str(int(full_duration_in_hours)))
        fallback_json_file = 'none'
        fallback_json_file = '%s/%s' % (metric_data_dir, fallback_json_filename)
        logger.info('ionosphere_backend :: trying fallback_json_file - %s' % fallback_json_file)
        if path.isfile(fallback_json_file):
            try:
                with open((fallback_json_file), 'r') as f:
                    raw_timeseries = f.read()
                timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
                anomalous_timeseries = literal_eval(timeseries_array_str)
                logger.info('ionosphere_backend :: loaded data from fallback_json_file - %s' % fallback_json_file)
            except:
                trace = traceback.format_exc()
                logger.error(trace)
                fail_msg = 'error :: ionosphere_backend :: failed to load any time series data after trying to load the fallback_json_file'
                logger.error('%s' % fail_msg)
                raise  # to webapp to return in the UI
    if not anomalous_timeseries:
        fail_msg = 'error :: ionosphere_backend :: failed to load any time series data form json files'
        logger.error('%s' % fail_msg)
        raise  # to webapp to return in the UI

    # @added 20170308 - Feature #1960: ionosphere_layers
    if layers_id_matched_file:
        if path.isfile(layers_id_matched_file):
            try:
                with open(layers_id_matched_file) as f:
                    output = f.read()
                layers_id_matched = int(output)
            except:
                layers_id_matched = False

    # @added 20170331 - Task #1988: Review - Ionosphere layers - always show layers
    #                   Feature #1960: ionosphere_layers
    # Added mirror functionality of the layers_id_matched_file
    # for feature profile matches too as it has proved useful
    # in the frontend with regards to training data sets being
    # matched by layers and can do the same for in the frontend
    # training data for feature profile matches too.
    if fp_id_matched_file:
        if path.isfile(fp_id_matched_file):
            try:
                with open(fp_id_matched_file) as f:
                    output = f.read()
                fp_id_matched = int(output)
            except:
                fp_id_matched = False

    # @added 20170401 - Task #1988: Review - Ionosphere layers - added fp_id_created
    #                   Feature #1960: ionosphere_layers
    if fp_created_file:
        if path.isfile(fp_created_file):
            try:
                with open(fp_created_file) as f:
                    output = f.read()
                fp_details_list = literal_eval(output)
            except:
                fp_details_list = None

    ts_full_duration = None
    if metric_vars_ok and ts_json_ok:
        for key, value in metric_vars:
            if key == 'full_duration':
                ts_full_duration = value

    data_to_process = False
    if metric_vars_ok and ts_json_ok:
        data_to_process = True
    panorama_anomaly_id = False
    # @modified 20180608 - Bug #2406: Ionosphere - panorama anomaly id lag
    # Time shift the requested_timestamp by 120 seconds either way on the
    # from_timestamp and until_timestamp parameter to account for any lag in the
    # insertion of the anomaly by Panorama in terms Panorama only running every
    # 60 second and Analyzer to Mirage to Ionosphere and back introduce
    # additional lags.  Panorama will not add multiple anomalies from the same
    # metric in the time window so there is no need to consider the possibility
    # of there being multiple anomaly ids being returned.
    # url = '%s/panorama?metric=%s&from_timestamp=%s&until_timestamp=%s&panorama_anomaly_id=true' % (settings.SKYLINE_URL, str(base_name), str(requested_timestamp), str(requested_timestamp))
    grace_from_timestamp = int(requested_timestamp) - 120
    grace_until_timestamp = int(requested_timestamp) + 120
    url = '%s/panorama?metric=%s&from_timestamp=%s&until_timestamp=%s&panorama_anomaly_id=true' % (settings.SKYLINE_URL, str(base_name), str(grace_from_timestamp), str(grace_until_timestamp))
    panorama_resp = None
    logger.info('getting anomaly id from panorama: %s' % str(url))

    # @added 20190519 - Branch #3002: docker
    # Handle self signed certificate on Docker
    verify_ssl = True
    try:
        running_on_docker = settings.DOCKER
    except:
        running_on_docker = False
    if running_on_docker:
        verify_ssl = False

    # @added 20191029 - Branch #3262: py3
    # Allow for the use of self signed SSL certificates even if not running on
    # docker.
    try:
        overall_verify_ssl = settings.VERIFY_SSL
    except:
        overall_verify_ssl = True
    if not overall_verify_ssl:
        verify_ssl = False

    if settings.WEBAPP_AUTH_ENABLED:
        user = str(settings.WEBAPP_AUTH_USER)
        password = str(settings.WEBAPP_AUTH_USER_PASSWORD)
    try:
        if settings.WEBAPP_AUTH_ENABLED:
            # @modified 20181106 - Bug #2668: Increase timeout on requests panorama id
            # r = requests.get(url, timeout=2, auth=(user, password))
            # @modified 20190519 - Branch #3002: docker
            # Handle self signed certificate on Docker
            # r = requests.get(url, timeout=settings.GRAPHITE_READ_TIMEOUT, auth=(user, password))
            r = requests.get(url, timeout=settings.GRAPHITE_READ_TIMEOUT, auth=(user, password), verify=verify_ssl)
        else:
            # @modified 20181106 - Bug #2668: Increase timeout on requests panorama id
            # r = requests.get(url, timeout=2)
            # @modified 20190519 - Branch #3002: docker
            # Handle self signed certificate on Docker
            # r = requests.get(url, timeout=settings.GRAPHITE_READ_TIMEOUT)
            r = requests.get(url, timeout=settings.GRAPHITE_READ_TIMEOUT, verify=verify_ssl)
        panorama_resp = True
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: failed to get anomaly id from panorama: %s' % str(url))

    if panorama_resp:
        try:
            data = literal_eval(r.text)
            if str(data) == '[]':
                panorama_anomaly_id = None
                logger.debug('debug :: panorama anomlay data: %s' % str(data))
            else:
                panorama_anomaly_id = int(data[0][0])
                logger.debug('debug :: panorama anomlay data: %s' % str(data))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to get anomaly id from panorama response: %s' % str(r.text))

    # @added 20170106 - Feature #1842: Ionosphere - Graphite now graphs
    # Graphite now graphs at TARGET_HOURS, 24h, 7d, 30d to fully inform the
    # operator about the metric.
    graphite_now_images = []
    graphite_now = int(time.time())

    # @added 20200731 - Feature #3654: IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE
    # Allow to override the Graphite NOW graphs with Graphite THEN graphs
    if IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE:
        graphite_now = int(requested_timestamp)

    graph_resolutions = []
    # @modified 20170116 - Feature #1854: Ionosphere learn - generations
    #                      Feature #1842: Ionosphere - Graphite now graphs
    # Also include the Graphite NOW graphs in the features_profile page as
    # graphs WHEN CREATED
    # if context == 'training_data':
    if context == 'training_data' or context == 'features_profiles' or context == 'saved_training_data':
        graph_resolutions = [int(settings.TARGET_HOURS), 24, 168, 720]
        # @modified 20170107 - Feature #1842: Ionosphere - Graphite now graphs
        # Exclude if matches TARGET_HOURS - unique only
        _graph_resolutions = sorted(set(graph_resolutions))
        graph_resolutions = _graph_resolutions

    for target_hours in graph_resolutions:
        graph_image = False
        try:
            graph_image_file = '%s/%s.graphite_now.%sh.png' % (metric_data_dir, base_name, str(target_hours))
            # These are NOW graphs, so if the graph_image_file exists, remove it
            # @modified 20170116 - Feature #1854: Ionosphere learn - generations
            #                      Feature #1842: Ionosphere - Graphite now graphs
            # Only remove if this is the training_data context and match on the
            # graph_image_file rather than graph_image response
            if context == 'training_data':
                target_seconds = int((target_hours * 60) * 60)
                from_timestamp = str(graphite_now - target_seconds)
                until_timestamp = str(graphite_now)
                if path.isfile(graph_image_file):
                    try:
                        remove(str(graph_image_file))
                        logger.info('graph_image_file removed - %s' % str(graph_image_file))
                    except OSError:
                        pass
                logger.info('getting Graphite graph for %s hours - from_timestamp - %s, until_timestamp - %s' % (str(target_hours), str(from_timestamp), str(until_timestamp)))
                graph_image = get_graphite_metric(
                    skyline_app, base_name, from_timestamp, until_timestamp, 'image',
                    graph_image_file)

            # if graph_image:
            if path.isfile(graph_image_file):
                graphite_now_images.append(graph_image_file)

            # @added 20170106 - Feature #1842: Ionosphere - Graphite now graphs
            # TODO: Un/fortunately there is no simple method by which to annotate
            # these Graphite NOW graphs at the anomaly timestamp, if these were
            # from Grafana, yes but we cannot add Grafana as a dep :)  It would
            # be possible to add these using the dygraph js method ala now, then
            # and Panorama, but that is BEYOND the scope of js I want to have to
            # deal with.  I think we can leave this up to the operator's
            # neocortex to do the processing.  Which may be a valid point as
            # sticking a single red line vertical line in the graphs ala Etsy
            # deployments https://codeascraft.com/2010/12/08/track-every-release/
            # or how @andymckay does it https://blog.mozilla.org/webdev/2012/04/05/tracking-deployments-in-graphite/
            # would arguably introduce a bias in this context.  The neocortex
            # should be able to handle this timeshifting fairly simply with a
            # little practice.
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to get Graphite graph at %s hours for %s' % (str(target_hours), base_name))

    # @added 20170107 - Feature #1852: Ionosphere - features_profile matched graphite graphs
    # Get the last 9 matched timestamps for the metric and get graphite graphs
    # for them
    graphite_matched_images = []
    matched_count = 0
    if context == 'features_profiles':
        logger.info('getting MySQL engine')
        try:
            engine, fail_msg, trace = get_an_engine()
            logger.info(fail_msg)
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            logger.error('%s' % fail_msg)
            logger.error('error :: could not get a MySQL engine to get fp_ids')
            raise  # to webapp to return in the UI

        if not engine:
            trace = 'none'
            fail_msg = 'error :: engine not obtained'
            logger.error(fail_msg)
            raise

        try:
            ionosphere_matched_table, log_msg, trace = ionosphere_matched_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('ionosphere_matched_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to get ionosphere_checked_table meta for %s' % base_name)
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI

        # @added 20210421 - Feature #4014: Ionosphere - inference
        matched_timestamps_motif_ids_dict = {}

        matched_timestamps = []

        # @added 20170107 - Feature #1852: Ionosphere - features_profile matched graphite graphs
        # Added details of match anomalies for verification added to tsfresh_version
        # all_calc_features_sum = None
        # all_calc_features_count = None
        # sum_common_values = None
        # common_features_count = None
        # That is more than it looks...

        try:
            connection = engine.connect()
            stmt = select([ionosphere_matched_table]).where(ionosphere_matched_table.c.fp_id == int(fp_id))
            result = connection.execute(stmt)
            for row in result:
                matched_timestamp = row['metric_timestamp']
                matched_timestamps.append(int(matched_timestamp))
                # @added 20210421 - Feature #4014: Ionosphere - inference
                motifs_matched_id = 0
                try:
                    motifs_matched_id = row['motifs_matched_id']
                    if motifs_matched_id:
                        logger.info('found motif_id from the DB - %s' % (str(motifs_matched_id)))
                        matched_timestamps_motif_ids_dict[int(matched_timestamp)] = motifs_matched_id
                except KeyError:
                    motifs_matched_id = 0
            connection.close()
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: could not determine timestamps from ionosphere_matched for fp_id %s' % str(fp_id))
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal and raise
            if engine:
                engine_disposal(engine)
            raise

        len_matched_timestamps = len(matched_timestamps)
        matched_count = len_matched_timestamps
        logger.info('determined %s matched timestamps for fp_id %s' % (str(len_matched_timestamps), str(fp_id)))

        last_matched_timestamps = []
        if len_matched_timestamps > 0:
            last_graph_timestamp = int(time.time())
            # skip_if_last_graph_timestamp_less_than = 600
            sorted_matched_timestamps = sorted(matched_timestamps)
#            get_matched_timestamps = sorted_matched_timestamps[-4:]
            get_matched_timestamps = sorted_matched_timestamps[-20:]
            # Order newest first
            for ts in get_matched_timestamps[::-1]:
                if len(get_matched_timestamps) > 4:
                    graph_time_diff = int(last_graph_timestamp) - int(ts)
                    if graph_time_diff > 600:
                        last_matched_timestamps.append(ts)
                else:
                    last_matched_timestamps.append(ts)
                last_graph_timestamp = int(ts)

        # @added 20190327 - Feature #2484: FULL_DURATION feature profiles
        fp_db_full_duration = None
        try:
            ionosphere_table, log_msg, trace = ionosphere_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('ionosphere_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to get ionosphere_table meta for fp id %s' % str(fp_id))
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI

        try:
            connection = engine.connect()
            stmt = select([ionosphere_table]).where(ionosphere_table.c.id == int(fp_id))
            result = connection.execute(stmt)
            for row in result:
                fp_db_full_duration = int(row['full_duration'])
                logger.info('found fp id full_duration from the DB - %s' % (str(fp_db_full_duration)))
                # @added 20190328 - Feature #2484: FULL_DURATION feature profiles
                # For ionosphere_echo features profiles
                fp_anomaly_timestamp = int(row['anomaly_timestamp'])
                logger.info('found fp anomaly_timestamp from the DB - %s' % (str(fp_anomaly_timestamp)))
            connection.close()
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: could not determine full_duration from ionosphere for fp_id %s' % str(fp_id))
            if engine:
                engine_disposal(engine)
            raise

        for matched_timestamp in last_matched_timestamps:
            # Get Graphite images
            graph_image = False
            try:
                key = 'full_duration'
                value_list = [var_array[1] for var_array in metric_vars if var_array[0] == key]
                full_duration = int(value_list[0])
                if fp_db_full_duration:
                    full_duration = fp_db_full_duration
                from_timestamp = str(int(matched_timestamp) - int(full_duration))
                until_timestamp = str(matched_timestamp)
                graph_image_file = '%s/%s.matched.fp_id-%s.%s.png' % (metric_data_dir, base_name, str(fp_id), str(matched_timestamp))
                # @added 20210421 - Feature #4014: Ionosphere - inference
                try:
                    motifs_matched_id = matched_timestamps_motif_ids_dict[int(matched_timestamp)]
                except KeyError:
                    motifs_matched_id = 0
                if motifs_matched_id:
                    graph_image_file = '%s/%s.matched.fp_id-%s.motif_id-%s.%s.png' % (
                        metric_data_dir, base_name, str(fp_id),
                        str(motifs_matched_id), str(matched_timestamp))
                if not path.isfile(graph_image_file):
                    logger.info('getting Graphite graph for fp_id %s matched timeseries from_timestamp - %s, until_timestamp - %s' % (str(fp_id), str(from_timestamp), str(until_timestamp)))
                    graph_image = get_graphite_metric(
                        skyline_app, base_name, from_timestamp, until_timestamp, 'image',
                        graph_image_file)
                else:
                    graph_image = True
                    logger.info('not getting Graphite graph as exists - %s' % (graph_image_file))
                if graph_image:
                    graphite_matched_images.append(graph_image_file)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get Graphite graph for fp_id %s at %s' % (str(fp_id), str(matched_timestamp)))

    # @added 20170308 - Feature #1960: ionosphere_layers
    # Added matched layers Graphite graphs
    graphite_layers_matched_images = []
    layers_matched_count = 0
    if context == 'features_profiles':
        if not engine:
            fail_msg = 'error :: no engine obtained for ionosphere_layers_matched_table'
            logger.error('%s' % fail_msg)
            raise  # to webapp to return in the UI

        try:
            ionosphere_layers_matched_table, log_msg, trace = ionosphere_layers_matched_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('ionosphere_layers_matched_table OK')
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: failed to get ionosphere_layers_matched_table meta for %s' % base_name
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI

        layers_id_matched = []
        try:
            connection = engine.connect()
            stmt = select([ionosphere_layers_matched_table]).where(ionosphere_layers_matched_table.c.fp_id == int(fp_id))
            result = connection.execute(stmt)
            for row in result:
                matched_layers_id = row['layer_id']
                matched_timestamp = row['anomaly_timestamp']
                layers_id_matched.append([int(matched_timestamp), int(matched_layers_id)])
                # logger.info('found matched_timestamp %s' % (str(matched_timestamp)))
            connection.close()
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: could not determine timestamps from ionosphere_matched for fp_id %s' % str(fp_id)
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI

        layers_matched_count = len(layers_id_matched)
        logger.info('determined %s matched layers timestamps for fp_id %s' % (str(layers_matched_count), str(fp_id)))

        last_matched_layers = []
        if layers_matched_count > 0:
            last_graph_timestamp = int(time.time())
            # skip_if_last_graph_timestamp_less_than = 600
            sorted_matched_layers = sorted(layers_id_matched)
            get_matched_layers = sorted_matched_layers[-20:]
            # Order newest first
            for matched_layer in get_matched_layers[::-1]:
                if len(get_matched_layers) > 4:
                    graph_time_diff = int(last_graph_timestamp) - int(matched_layer[0])
                    if graph_time_diff > 600:
                        last_matched_layers.append(matched_layer)
                else:
                    last_matched_layers.append(matched_layer)
                last_graph_timestamp = int(matched_layer[0])

        logger.info('determined %s matched layers timestamps for graphs for fp_id %s' % (str(len(last_matched_layers)), str(fp_id)))

        for matched_layer in last_matched_layers:
            # Get Graphite images
            graph_image = False
            matched_layer_id = None
            try:
                full_duration = int(settings.FULL_DURATION)
                from_timestamp = str(int(matched_layer[0]) - int(full_duration))
                until_timestamp = str(matched_layer[0])
                matched_layer_id = str(matched_layer[1])
                graph_image_file = '%s/%s.layers_id-%s.matched.layers.fp_id-%s.%s.png' % (
                    metric_data_dir, base_name, str(matched_layer_id),
                    str(fp_id), str(matched_layer[0]))
                if not path.isfile(graph_image_file):
                    logger.info(
                        'getting Graphite graph for fp_id %s layer_id %s matched timeseries from_timestamp - %s, until_timestamp - %s' % (
                            str(fp_id), str(matched_layer_id), str(from_timestamp),
                            str(until_timestamp)))
                    graph_image = get_graphite_metric(
                        skyline_app, base_name, from_timestamp, until_timestamp, 'image',
                        graph_image_file)
                else:
                    graph_image = True
                    logger.info('not getting Graphite graph as exists - %s' % (graph_image_file))
                if graph_image:
                    graphite_layers_matched_images.append(graph_image_file)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get Graphite graph for fp_id %s at %s' % (str(fp_id), str(matched_timestamp)))

        if engine:
            engine_disposal(engine)

    return (
        metric_paths, images, human_date, metric_vars, ts_json, data_to_process,
        panorama_anomaly_id, graphite_now_images, graphite_matched_images,
        matched_count,
        # @added 20170308 - Feature #1960: ionosphere_layers
        # Show the latest matched layers graphs as well and the matched layers_id_matched
        # in the training_data page if there has been one.
        graphite_layers_matched_images, layers_id_matched, ts_full_duration,
        # @added 20170309 - Feature #1960: ionosphere_layers
        # Also return the Analyzer FULL_DURATION timeseries if available in a Mirage
        # based features profile
        ionosphere_json,
        # @added 20170331 - Task #1988: Review - Ionosphere layers - always show layers
        #                   Feature #1960: ionosphere_layers
        # Return the anomalous_timeseries as an array to sample and fp_id_matched
        anomalous_timeseries, fp_id_matched,
        # @added 20170401 - Task #1988: Review - Ionosphere layers - added fp_details_list
        #                   Feature #1960: ionosphere_layers
        fp_details_list,
        # @added 20190328 - Feature #2484: FULL_DURATION feature profiles
        # For ionosphere_echo features profiles
        fp_anomaly_timestamp)


# @modified 20170114 - Feature #1854: Ionosphere learn
# DEPRECATED create_features_profile here as this function has been migrated in
# order to decouple the creation of features profiles from the webapp as
# ionosphere/learn now requires access to this function as well.  Moved to a
# shared function in ionosphere_functions.py
# REMOVED
# def create_features_profile(requested_timestamp, data_for_metric, context):
def features_profile_details(fp_id):
    """
    Get the Ionosphere details of a fetures profile

    :param fp_id: the features profile id
    :type fp_id: str
    :return: tuple
    :rtype:  (str, boolean, str, str)

    """
    logger = logging.getLogger(skyline_app_logger)

    function_str = 'ionoshere_backend.py :: features_profile_details'

    trace = 'none'
    fail_msg = 'none'
    fp_details = None

    logger.info('%s :: getting MySQL engine' % function_str)
    try:
        engine, fail_msg, trace = get_an_engine()
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not get a MySQL engine'
        logger.error('%s' % fail_msg)
        # return False, False, fail_msg, trace, False
        raise  # to webapp to return in the UI

    if not engine:
        trace = 'none'
        fail_msg = 'error :: engine not obtained'
        logger.error(fail_msg)
        # return False, False, fail_msg, trace, False
        raise  # to webapp to return in the UI
    ionosphere_table = None
    try:
        ionosphere_table, fail_msg, trace = ionosphere_table_meta(skyline_app, engine)
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to get ionosphere_table meta for fp_id %s details' % str(fp_id)
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        # return False, False, fail_msg, trace, False
        raise  # to webapp to return in the UI

    logger.info('%s :: ionosphere_table OK' % function_str)

    try:
        connection = engine.connect()
        stmt = select([ionosphere_table]).where(ionosphere_table.c.id == int(fp_id))
        result = connection.execute(stmt)
        row = result.fetchone()
        # fp_details_object = row
        fp_details_object = dict(row)
        connection.close()
        try:
            tsfresh_version = row['tsfresh_version']
        except:
            tsfresh_version = 'unknown'
        try:
            calc_time = row['calc_time']
        except:
            calc_time = 'unknown'
        full_duration = row['full_duration']
        features_count = row['features_count']
        features_sum = row['features_sum']
        deleted = row['deleted']
        matched_count = row['matched_count']
        last_matched = row['last_matched']
        if str(last_matched) == '0':
            human_date = 'never matched'
        else:
            human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(last_matched)))
        created_timestamp = row['created_timestamp']
        full_duration = row['full_duration']
        # @modified 20161229 - Feature #1830: Ionosphere alerts
        # Added checked_count and last_checked
        last_checked = row['last_checked']
        if str(last_checked) == '0':
            checked_human_date = 'never checked'
        else:
            checked_human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(last_checked)))
        checked_count = row['checked_count']
        # @modified 20170114 - Feature #1854: Ionosphere learn
        # Added parent_id and generation
        parent_id = row['parent_id']
        generation = row['generation']
        # @added 20170402 - Feature #2000: Ionosphere - validated
        validated = row['validated']
        # @added 20170305 - Feature #1960: ionosphere_layers
        layers_id = row['layers_id']
        # @added 20190922 - Feature #2516: Add label to features profile
        label = row['label']

        # @added 20210421 - Feature #4014: Ionosphere - inference
        motif_checked_count = row['motif_checked_count']
        motif_last_checked = row['motif_last_checked']
        motif_matched_count = row['motif_matched_count']
        motif_last_matched = row['motif_last_matched']
        if str(motif_last_checked) == '0':
            motif_checked_human_date = 'never checked'
        else:
            motif_checked_human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(motif_last_checked)))
        if str(motif_last_matched) == '0':
            motif_matched_human_date = 'never checked'
        else:
            motif_matched_human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(motif_last_matched)))

        fp_details = '''
tsfresh_version     :: %s | calc_time :: %s
features_count      :: %s
features_sum        :: %s
deleted             :: %s
matched_count       :: %s
last_matched        :: %s | human_date :: %s
checked_count       :: %s
last_checked        :: %s | human_date :: %s
motif_matched_count :: %s
motif_last_matched  :: %s | human_date :: %s
motif_checked_count :: %s
motif_last_checked  :: %s | human_date :: %s
created_timestamp   :: %s
full_duration       :: %s
parent_id           :: %s | generation :: %s | validated :: %s
layers_id           :: %s
label               :: %s
''' % (str(tsfresh_version), str(calc_time), str(features_count),
            str(features_sum), str(deleted), str(matched_count),
            str(last_matched), str(human_date),
            str(checked_count),
            str(last_checked), str(checked_human_date),
            str(motif_matched_count),
            str(motif_last_matched), str(motif_matched_human_date),
            str(motif_checked_count),
            str(motif_last_checked), str(motif_checked_human_date),
            str(created_timestamp),
            str(full_duration),
            str(parent_id), str(generation),
            str(validated), str(layers_id), str(label))
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not get fp_id %s details from ionosphere DB table' % str(fp_id)
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        # return False, False, fail_msg, trace, False
        raise  # to webapp to return in the UI

    if engine:
        engine_disposal(engine)

    # @modified 20170114 -  Feature #1854: Ionosphere learn - generations
    # Return the fp_details_object so that webapp can pass the parent_id and
    # generation to the templates
    # return fp_details, True, fail_msg, trace
    return fp_details, True, fail_msg, trace, fp_details_object


# @added 20170118 - Feature #1862: Ionosphere features profiles search page
# Added fp_search parameter
# @modified 20170220 - Feature #1862: Ionosphere features profiles search page
def ionosphere_search(default_query, search_query):
    """
    Gets the details features profiles from the database, using the URL arguments
    that are passed in by the :obj:`request.args` to build the MySQL select
    query string and queries the database, parse the results and creates an
    array of the features profiles that matched the query.

    :param None: determined from :obj:`request.args`
    :return: array
    :rtype: array

    """
    logger = logging.getLogger(skyline_app_logger)
    import time
    import datetime

    function_str = 'ionoshere_backend.py :: ionosphere_search'

    trace = 'none'
    fail_msg = 'none'

    full_duration_list = []
    enabled_list = []
    tsfresh_version_list = []
    generation_list = []
    features_profiles = []
    features_profiles_count = []

    # possible_options = [
    #     'full_duration', 'enabled', 'tsfresh_version', 'generation', 'count']

    # @modified 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
    # added some additional logging to debug built queries
    # logger.info('determining search parameters')
    logger.info('determining search parameters - with default_query: %s, search_query - %s' % (
        str(default_query), str(search_query)))

    query_string = 'SELECT * FROM ionosphere'

    # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
    logger.info('starting with default query_string of "%s"' % query_string)

# id, metric_id, full_duration, anomaly_timestamp, enabled, tsfresh_version,
# calc_time, features_sum, matched_count, last_matched, created_timestamp,
# last_checked, checked_count, parent_id, generation
    needs_and = False

    count_request = False
    matched_count = None
    checked_count = None
    generation_count = None

    count_by_metric = None
    if 'count_by_metric' in request.args:
        count_by_metric = request.args.get('count_by_metric', None)
        if count_by_metric and count_by_metric != 'false':
            count_request = True
            count_by_metric = True
            features_profiles_count = []
            query_string = 'SELECT COUNT(*), metric_id FROM ionosphere GROUP BY metric_id'
            # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
            logger.info('query_string now "%s"' % query_string)
        else:
            count_by_metric = False

    count_by_matched = None
    if 'count_by_matched' in request.args:
        count_by_matched = request.args.get('count_by_matched', None)
        if count_by_matched and count_by_matched != 'false':
            count_request = True
            count_by_matched = True
            matched_count = []
#            query_string = 'SELECT COUNT(*), id FROM ionosphere GROUP BY matched_count ORDER BY COUNT(*)'
            query_string = 'SELECT matched_count, id FROM ionosphere ORDER BY matched_count'
            # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
            logger.info('query_string now "%s"' % query_string)
        else:
            count_by_matched = False

    count_by_checked = None
    if 'count_by_checked' in request.args:
        count_by_checked = request.args.get('count_by_checked', None)
        if count_by_checked and count_by_checked != 'false':
            count_request = True
            count_by_checked = True
            checked_count = []
            query_string = 'SELECT COUNT(*), id FROM ionosphere GROUP BY checked_count ORDER BY COUNT(*)'
            query_string = 'SELECT checked_count, id FROM ionosphere ORDER BY checked_count'
            # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
            logger.info('query_string now "%s"' % query_string)
        else:
            count_by_checked = False

    count_by_generation = None
    if 'count_by_generation' in request.args:
        count_by_generation = request.args.get('count_by_generation', None)
        if count_by_generation and count_by_generation != 'false':
            count_request = True
            count_by_generation = True
            generation_count = []
            query_string = 'SELECT COUNT(*), generation FROM ionosphere GROUP BY generation ORDER BY COUNT(*)'
            # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
            logger.info('query_string now "%s"' % query_string)
        else:
            count_by_generation = False

    get_metric_profiles = None
    metric = None
    if 'metric' in request.args:
        metric = request.args.get('metric', None)
        if metric and metric != 'all' and metric != '*':
            # A count_request always takes preference over a metric
            if not count_request:
                get_metric_profiles = True
                query_string = 'SELECT * FROM ionosphere WHERE metric_id=REPLACE_WITH_METRIC_ID'
                needs_and = True
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)
            else:
                new_query_string = 'SELECT * FROM ionosphere WHERE metric_id=REPLACE_WITH_METRIC_ID'
                query_string = new_query_string
                needs_and = True
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)

    if 'from_timestamp' in request.args:
        from_timestamp = request.args.get('from_timestamp', None)
        if from_timestamp and from_timestamp != 'all':
            if ":" in from_timestamp:
                new_from_timestamp = time.mktime(datetime.datetime.strptime(from_timestamp, '%Y%m%d %H:%M').timetuple())
                from_timestamp = str(int(new_from_timestamp))

            # @added 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
            #                   Bug #2818: Mutliple SQL Injection Security Vulnerabilities
            # Validate from_timestamp
            try:
                validate_from_timestamp = int(from_timestamp) + 1
                int_from_timestamp = validate_from_timestamp - 1
                validated_from_timestamp = str(int_from_timestamp)
            except:
                trace = traceback.format_exc()
                logger.error(trace)
                fail_msg = 'error :: could not validate from_timestamp'
                logger.error('%s' % fail_msg)
                raise

            if needs_and:
                # @modified 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
                #                      Bug #2818: Mutliple SQL Injection Security Vulnerabilities
                # Use validated variable
                # new_query_string = '%s AND anomaly_timestamp >= %s' % (query_string, from_timestamp)
                new_query_string = '%s AND anomaly_timestamp >= %s' % (query_string, validate_from_timestamp)
                query_string = new_query_string
                needs_and = True
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)
            else:
                # @modified 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
                #                      Bug #2818: Mutliple SQL Injection Security Vulnerabilities
                # Use validated variable
                # new_query_string = '%s WHERE anomaly_timestamp >= %s' % (query_string, from_timestamp)
                new_query_string = '%s WHERE anomaly_timestamp >= %s' % (query_string, validated_from_timestamp)
                query_string = new_query_string
                needs_and = True
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)

    if 'until_timestamp' in request.args:
        until_timestamp = request.args.get('until_timestamp', None)
        if until_timestamp and until_timestamp != 'all':
            if ":" in until_timestamp:
                new_until_timestamp = time.mktime(datetime.datetime.strptime(until_timestamp, '%Y%m%d %H:%M').timetuple())
                until_timestamp = str(int(new_until_timestamp))

            # @added 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
            #                   Bug #2818: Mutliple SQL Injection Security Vulnerabilities
            # Validate until_timestamp
            try:
                validate_until_timestamp = int(until_timestamp) + 1
                int_until_timestamp = validate_until_timestamp - 1
                validated_until_timestamp = str(int_until_timestamp)
            except:
                trace = traceback.format_exc()
                logger.error(trace)
                fail_msg = 'error :: could not validate until_timestamp'
                logger.error('%s' % fail_msg)
                raise

            if needs_and:
                # @modified 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
                #                      Bug #2818: Mutliple SQL Injection Security Vulnerabilities
                # Use validated variable
                # new_query_string = '%s AND anomaly_timestamp <= %s' % (query_string, until_timestamp)
                new_query_string = '%s AND anomaly_timestamp <= %s' % (query_string, validated_until_timestamp)
                query_string = new_query_string
                needs_and = True
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)
            else:
                # @modified 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
                #                      Bug #2818: Mutliple SQL Injection Security Vulnerabilities
                # Use validated variable
                # new_query_string = '%s WHERE anomaly_timestamp <= %s' % (query_string, until_timestamp)
                new_query_string = '%s WHERE anomaly_timestamp <= %s' % (query_string, validated_until_timestamp)
                query_string = new_query_string
                needs_and = True
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)

    if 'generation_greater_than' in request.args:
        generation_greater_than = request.args.get('generation_greater_than', None)
        if generation_greater_than and generation_greater_than != '0':
            # @added 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
            #                   Bug #2818: Mutliple SQL Injection Security Vulnerabilities
            # Validate generation_greater_than
            try:
                validate_generation_greater_than = int(generation_greater_than) + 1
                int_generation_greater_than = validate_generation_greater_than - 1
                validated_generation_greater_than = str(int_generation_greater_than)
            except:
                trace = traceback.format_exc()
                logger.error(trace)
                fail_msg = 'error :: could not validate generation_greater_than'
                logger.error('%s' % fail_msg)
                raise

            if needs_and:
                # @modified 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
                #                      Bug #2818: Mutliple SQL Injection Security Vulnerabilities
                # Use validated variable
                # new_query_string = '%s AND generation > %s' % (query_string, generation_greater_than)
                new_query_string = '%s AND generation > %s' % (query_string, validated_generation_greater_than)
                query_string = new_query_string
                needs_and = True
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)
            else:
                # @modified 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
                #                      Bug #2818: Mutliple SQL Injection Security Vulnerabilities
                # Use validated variable
                # new_query_string = '%s WHERE generation > %s' % (query_string, generation_greater_than)
                new_query_string = '%s WHERE generation > %s' % (query_string, validated_generation_greater_than)
                query_string = new_query_string
                needs_and = True
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)

    # @added 20170315 - Feature #1960: ionosphere_layers
    if 'layers_id_greater_than' in request.args:
        layers_id_greater_than = request.args.get('layers_id_greater_than', None)
        if layers_id_greater_than and layers_id_greater_than != '0':
            # @added 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
            #                   Bug #2818: Mutliple SQL Injection Security Vulnerabilities
            # Validate layers_id_greater_than
            try:
                validate_layers_id_greater_than = int(layers_id_greater_than) + 1
                int_layers_id_greater_than = validate_layers_id_greater_than - 1
                validated_layers_id_greater_than = str(int_layers_id_greater_than)
            except:
                trace = traceback.format_exc()
                logger.error(trace)
                fail_msg = 'error :: could not validate layers_id_greater_than'
                logger.error('%s' % fail_msg)
                raise

            if needs_and:
                # @modified 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
                #                      Bug #2818: Mutliple SQL Injection Security Vulnerabilities
                # Use validated variable
                # new_query_string = '%s AND layers_id > %s' % (query_string, layers_id_greater_than)
                new_query_string = '%s AND layers_id > %s' % (query_string, validated_layers_id_greater_than)
                query_string = new_query_string
                needs_and = True
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)
            else:
                # @modified 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
                #                      Bug #2818: Mutliple SQL Injection Security Vulnerabilities
                # Use validated variable
                # new_query_string = '%s WHERE layers_id > %s' % (query_string, layers_id_greater_than)
                new_query_string = '%s WHERE layers_id > %s' % (query_string, validated_layers_id_greater_than)
                query_string = new_query_string
                needs_and = True
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)

    # @added 20170402 - Feature #2000: Ionosphere - validated
    if 'validated_equals' in request.args:
        validated_equals = request.args.get('validated_equals', 'any')
        if validated_equals == 'true':
            validate_string = 'validated = 1'
        if validated_equals == 'false':
            validate_string = 'validated = 0'
        if validated_equals != 'any':
            if needs_and:
                new_query_string = '%s AND %s' % (query_string, validate_string)
                query_string = new_query_string
                needs_and = True
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)
            else:
                new_query_string = '%s WHERE %s' % (query_string, validate_string)
                query_string = new_query_string
                needs_and = True
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)

    # @added 20170518 - Feature #1996: Ionosphere - matches page - matched_greater_than
    if 'matched_greater_than' in request.args:
        matched_greater_than = request.args.get('matched_greater_than', None)
        if matched_greater_than and matched_greater_than != '0':
            # @added 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
            #                   Bug #2818: Mutliple SQL Injection Security Vulnerabilities
            # Validate matched_greater_than
            try:
                validate_matched_greater_than = int(matched_greater_than) + 1
                int_matched_greater_than = validate_matched_greater_than - 1
                validated_matched_greater_than = str(int_matched_greater_than)
            except:
                trace = traceback.format_exc()
                logger.error(trace)
                fail_msg = 'error :: could not validate matched_greater_than'
                logger.error('%s' % fail_msg)
                raise

            if needs_and:
                # @modified 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
                #                      Bug #2818: Mutliple SQL Injection Security Vulnerabilities
                # Use validated variable
                # new_query_string = '%s AND matched_count > %s' % (query_string, matched_greater_than)
                new_query_string = '%s AND matched_count > %s' % (query_string, validated_matched_greater_than)
                query_string = new_query_string
                needs_and = True
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)
            else:
                # @modified 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
                #                      Bug #2818: Mutliple SQL Injection Security Vulnerabilities
                # Use validated variable
                # new_query_string = '%s WHERE matched_count > %s' % (query_string, matched_greater_than)
                new_query_string = '%s WHERE matched_count > %s' % (query_string, validated_matched_greater_than)
                query_string = new_query_string
                needs_and = True
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)

    # @added 20170913 - Feature #2056: ionosphere - disabled_features_profiles
    # Added enabled query modifier to search and display enabled or disabled
    # profiles in the search_features_profiles page results.
    if 'enabled' in request.args:
        enabled = request.args.get('enabled', None)
        enabled_query = False
        enabled_query_value = 1
        if enabled:
            if str(enabled) == 'all':
                enabled_query = False
            if str(enabled) == 'true':
                enabled_query = True
            if str(enabled) == 'false':
                enabled_query = True
                enabled_query_value = 0
        if enabled_query:
            if needs_and:
                new_query_string = '%s AND enabled = %s' % (query_string, str(enabled_query_value))
                query_string = new_query_string
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)
            else:
                new_query_string = '%s WHERE enabled = %s' % (query_string, str(enabled_query_value))
                query_string = new_query_string
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)
            needs_and = True

    # @modified 20180414 - Feature #1862: Ionosphere features profiles search page
    #                      Branch #2270: luminosity
    # Moved from being just above metrics = [] below as required to determine
    # metric_like queries
    engine_needed = True
    engine = None
    if engine_needed:
        logger.info('%s :: getting MySQL engine' % function_str)
        try:
            engine, fail_msg, trace = get_an_engine()
            logger.info(fail_msg)
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: could not get a MySQL engine'
            logger.error('%s' % fail_msg)
            raise

        if not engine:
            trace = 'none'
            fail_msg = 'error :: engine not obtained'
            logger.error(fail_msg)
            raise

        try:
            metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('metrics_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to get metrics_table meta')

            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI

    # @added 20180414 - Feature #1862: Ionosphere features profiles search page
    #                   Branch #2270: luminosity
    if 'metric_like' in request.args:
        metric_like_str = request.args.get('metric_like', 'all')
        if metric_like_str != 'all':
            # SQLAlchemy requires the MySQL wildcard % to be %% to prevent
            # interpreting the % as a printf-like format character
            # python_escaped_metric_like = metric_like_str.replace('%', '%%')
            # @modified 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
            #                      Bug #2818: Mutliple SQL Injection Security Vulnerabilities
            # Change the query
            # nosec to exclude from bandit tests
            # metrics_like_query = 'SELECT id FROM metrics WHERE metric LIKE \'%s\'' % (str(python_escaped_metric_like))  # nosec
            # logger.info('executing metrics_like_query - %s' % metrics_like_query)
            # like_string_var = str(metric_like_str)
            metrics_like_query = text("""SELECT id FROM metrics WHERE metric LIKE :like_string""")
            # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
            logger.info('metrics_like_query - "%s"' % metrics_like_query)

            metric_ids = ''
            try:
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('metrics_like_query running query')

                connection = engine.connect()
                # @modified 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
                #                      Bug #2818: Mutliple SQL Injection Security Vulnerabilities
                # results = connection.execute(metrics_like_query)
                results = connection.execute(metrics_like_query, like_string=metric_like_str)
                connection.close()
                for row in results:
                    metric_id = str(row[0])
                    if metric_ids == '':
                        metric_ids = '%s' % (metric_id)
                    else:
                        new_metric_ids = '%s, %s' % (metric_ids, metric_id)
                        metric_ids = new_metric_ids
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('metrics_like_query done')

            except:
                trace = traceback.format_exc()
                logger.error(trace)
                logger.error('error :: could not determine ids from metrics table')
                # Disposal and return False, fail_msg, trace for Bug #2130: MySQL - Aborted_clients
                if engine:
                    engine_disposal(engine)
                return False, fail_msg, trace
            if needs_and:
                new_query_string = '%s AND metric_id IN (%s)' % (query_string, str(metric_ids))
                query_string = new_query_string
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)
            else:
                new_query_string = '%s WHERE metric_id IN (%s)' % (query_string, str(metric_ids))
                # @modified 20200708 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                # Correct query
                if 'SELECT COUNT(*), metric_id FROM ionosphere GROUP BY metric_id' in query_string:
                    new_select = query_string.replace('GROUP BY metric_id', '')
                    new_query_string = '%s WHERE metric_id IN (%s) GROUP BY metric_id' % (new_select, str(metric_ids))
                query_string = new_query_string
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)
            needs_and = True

    ordered_by = None
    if 'order' in request.args:
        order = request.args.get('order', 'DESC')
        if str(order) == 'DESC':
            ordered_by = 'DESC'
        if str(order) == 'ASC':
            ordered_by = 'ASC'

    if ordered_by:
        if count_request and search_query:
            new_query_string = '%s %s' % (query_string, ordered_by)
        else:
            new_query_string = '%s ORDER BY id %s' % (query_string, ordered_by)
        query_string = new_query_string
        # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
        logger.info('query_string now "%s"' % query_string)

    if 'limit' in request.args:
        limit = request.args.get('limit', '30')
        try:
            validate_limit = int(limit) + 0
            if int(limit) != 0:
                # @modified 20190116 - Mutliple SQL Injection Security Vulnerabilities #86
                #                      Bug #2818: Mutliple SQL Injection Security Vulnerabilities
                # new_query_string = '%s LIMIT %s' % (query_string, str(limit))
                new_query_string = '%s LIMIT %s' % (query_string, str(validate_limit))
                query_string = new_query_string
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('query_string now "%s"' % query_string)
        except:
            logger.error('error :: limit is not an integer - %s' % str(limit))

    # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
    # To ensure that not all metrics and features profiles are queried on a
    # default Ionosphere search, identify this requests and do not query all
    # metrics and features profiles.
    # TODO: determine what other contexts are using this increasingly inefficient
    # methods and fix those to as the more metrics and more features profiles
    # that are added increases the size of these objects and iterations over
    # them.  This method is not scaleable with 100s of 1000s of metrics or
    # features profiles
    default_ionosphere_search = False
    if 'SELECT * FROM ionosphere ORDER BY id DESC LIMIT' in query_string:
        default_ionosphere_search = True

    metrics = []

    try:
        connection = engine.connect()
        stmt = select([metrics_table]).where(metrics_table.c.id != 0)
        # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
        logger.info('selecting all metrics with id > 0')
        if default_ionosphere_search:
            stmt = select([metrics_table]).where(metrics_table.c.id == 0)
            logger.info('defult Ionosphere search some not selecting all metrics')

        result = connection.execute(stmt)
        for row in result:
            metric_id = int(row['id'])
            metric_name = str(row['metric'])
            metrics.append([metric_id, metric_name])
        connection.close()
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: could not determine metrics from metrics table'
        logger.error('%s' % fail_msg)
        # @added 20170806 - Bug #2130: MySQL - Aborted_clients
        # Added missing disposal and raise
        if engine:
            engine_disposal(engine)
        raise
    # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
    logger.info('selected all metrics with id > 0')

    if get_metric_profiles:
        metrics_id = None
        for metric_obj in metrics:
            if metrics_id:
                break
            if metric == str(metric_obj[1]):
                metrics_id = str(metric_obj[0])
        new_query_string = query_string.replace('REPLACE_WITH_METRIC_ID', metrics_id)
        query_string = new_query_string
        # @modified 20200715 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
        # logger.debug('debug :: query_string - %s' % query_string)
        logger.info('get_metric_profiles - query_string now - %s' % query_string)

    ionosphere_table = None
    try:
        ionosphere_table, fail_msg, trace = ionosphere_table_meta(skyline_app, engine)
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to get ionosphere_table meta for options'
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise
    logger.info('%s :: ionosphere_table OK' % function_str)

    # @added 20200505 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
    apply_metric_id = False
    if get_metric_profiles:
        try:
            apply_metric_id = metrics_id
        except:
            apply_metric_id = False

    # @added 20200505 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
    find_unvalidated = False
    if 'fp_validate' in request.args:
        fp_validate_arg = request.args.get('fp_validate', 'false')
        if fp_validate_arg == 'true':
            if 'metric' in request.args:
                metric_arg = request.args.get('metric', 'none')
                if metric_arg == 'all':
                    find_unvalidated = True

    if get_metric_profiles:
        try:
            apply_metric_id = metrics_id
        except:
            apply_metric_id = False

    all_fps = []
    try:
        connection = engine.connect()
        # @modified 20200505 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
        # apply_metric_id if known
        if not apply_metric_id:
            stmt = select([ionosphere_table]).where(ionosphere_table.c.id != 0)
            # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
            logger.info('selecting all features profiles with id > 0')
            if default_ionosphere_search:
                stmt = select([ionosphere_table]).where(ionosphere_table.c.id == 0)
                logger.info('default Ionosphere search so not selecting all features profiles')
        else:
            # @added 20200505 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
            logger.info('optimised - selecting all features profiles with metric_id == %s' % str(apply_metric_id))
            stmt = select([ionosphere_table]).where(ionosphere_table.c.metric_id == apply_metric_id)
        # @added 20200506 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
        if find_unvalidated:
            logger.info('optimised - selecting all features profiles with validated == 0')
            stmt = select([ionosphere_table]).where(ionosphere_table.c.validated == 0)

        result = connection.execute(stmt)
        for row in result:
            try:
                fp_id = int(row['id'])
                fp_metric_id = int(row['metric_id'])
                for metric_obj in metrics:
                    if fp_metric_id == int(metric_obj[0]):
                        fp_metric = metric_obj[1]
                        break
                full_duration = int(row['full_duration'])
                anomaly_timestamp = int(row['anomaly_timestamp'])
                tsfresh_version = str(row['tsfresh_version'])
                # These handle MySQL NULL
                try:
                    calc_time = float(row['calc_time'])
                except:
                    calc_time = 0
                try:
                    features_count = int(row['features_count'])
                except:
                    features_count = 0
                try:
                    features_sum = float(row['features_sum'])
                except:
                    features_sum = 0
                try:
                    deleted = int(row['deleted'])
                except:
                    deleted = 0
                fp_matched_count = int(row['matched_count'])
                last_matched = int(row['last_matched'])
                if str(last_matched) == '0':
                    human_date = 'never matched'
                else:
                    human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(last_matched)))
                created_timestamp = str(row['created_timestamp'])
                last_checked = int(row['last_checked'])
                if str(last_checked) == '0':
                    checked_human_date = 'never checked'
                else:
                    checked_human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(last_checked)))
                fp_checked_count = int(row['checked_count'])
                fp_parent_id = int(row['parent_id'])
                fp_generation = int(row['generation'])
                # @added 20170402 - Feature #2000: Ionosphere - validated
                fp_validated = int(row['validated'])
                # @added 20210415 - Feature #4014: Ionosphere - inference
                #                   Branch #3590: inference
                motif_matched_count = int(row['motif_matched_count'])
                motif_last_matched = int(row['motif_last_matched'])
                motif_last_checked = int(row['motif_last_checked'])
                motif_checked_count = int(row['motif_checked_count'])
                if str(motif_last_checked) == '0':
                    motif_last_checked_human_date = 'never checked'
                else:
                    motif_last_checked_human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(motif_last_checked)))
                if str(motif_last_matched) == '0':
                    motif_last_matched_human_date = 'never matched'
                else:
                    motif_last_matched_human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(motif_last_matched)))

                # @modifed 20210415 - Feature #4014: Ionosphere - inference
                #                     Branch #3590: inference
                # all_fps.append([fp_id, fp_metric_id, str(fp_metric), full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated])
                all_fps.append([fp_id, fp_metric_id, str(fp_metric), full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, motif_matched_count, motif_last_matched_human_date, motif_last_checked_human_date, motif_checked_count])
                # logger.info('%s :: %s feature profiles found' % (function_str, str(len(all_fps))))
            except:
                trace = traceback.format_exc()
                logger.error('%s' % trace)
                logger.error('error :: bad row data')
        connection.close()
        all_fps.sort(key=operator.itemgetter(int(0)))
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        logger.error('error :: bad row data')
        raise

    # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
    # @modified 20200506 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
    # apply_metric_id if known
    if not apply_metric_id:
        logger.info('selected all features profiles with id > 0')
    else:
        # @modified 20200715 - Task #3648: webapp - fp_search - do not use Redis metric check
        # logger.info('selected all features profiles with metric_id == %s' % str(apply_metric_id))
        logger.info('selected all %s features profiles with metric_id == %s' % (str(len(all_fps)), str(apply_metric_id)))

    if count_request and search_query:
        features_profiles = None
        features_profiles_count = None
        full_duration_list = None
        enabled_list = None
        tsfresh_version_list = None
        generation_list = None

    if count_by_metric and search_query:
        features_profiles_count = []
        if engine_needed and engine:
            try:
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('count_by_metric and search_query running query - "%s"' % query_string)

                stmt = query_string
                connection = engine.connect()
                for row in engine.execute(stmt):
                    fp_count = int(row[0])
                    fp_metric_id = int(row['metric_id'])
                    for metric_obj in metrics:
                        if fp_metric_id == metric_obj[0]:
                            fp_metric = metric_obj[1]
                            break
                    features_profiles_count.append([fp_count, fp_metric_id, str(fp_metric)])
                connection.close()
                logger.info('%s :: features_profiles_count %s' % (function_str, str(len(features_profiles_count))))
            except:
                trace = traceback.format_exc()
                logger.error('%s' % trace)
                fail_msg = 'error :: failed to count features profiles'
                logger.error('%s' % fail_msg)
                if engine:
                    engine_disposal(engine)
                raise
        features_profiles_count.sort(key=operator.itemgetter(int(0)))

    if count_request and search_query:
        if not count_by_metric:
            if engine_needed and engine:
                try:
                    # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                    logger.info('count_request and search_query -  running query - "%s"' % query_string)
                    stmt = query_string
                    connection = engine.connect()
                    for row in engine.execute(stmt):
                        item_count = int(row[0])
                        item_id = int(row[1])
                        if count_by_matched or count_by_checked:
                            for fp_obj in all_fps:
                                if item_id == fp_obj[0]:
                                    metric_name = fp_obj[2]
                                    break
                        if count_by_matched:
                            matched_count.append([item_count, item_id, metric_name])
                        if count_by_checked:
                            checked_count.append([item_count, item_id, metric_name])
                        if count_by_generation:
                            generation_count.append([item_count, item_id])
                    connection.close()
                except:
                    trace = traceback.format_exc()
                    logger.error('%s' % trace)
                    fail_msg = 'error :: failed to get ionosphere_table meta for options'
                    logger.error('%s' % fail_msg)
                    if engine:
                        engine_disposal(engine)
                    raise

    if count_request and search_query:
        if engine:
            engine_disposal(engine)

        # @modified 20170809 - Bug #2136: Analyzer stalling on no metrics
        # Added except to all del methods to prevent stalling if any object does
        # not exist
        try:
            del all_fps
        except:
            logger.error('error :: failed to del all_fps')
        try:
            del metrics
        except:
            logger.error('error :: failed to del metrics')
        search_success = True
        return (features_profiles, features_profiles_count, matched_count,
                checked_count, generation_count, full_duration_list,
                enabled_list, tsfresh_version_list, generation_list,
                search_success, fail_msg, trace)

    features_profiles = []
    # @added 20170322 - Feature #1960: ionosphere_layers
    # Added layers information to the features_profiles items
    layers_present = False

    if engine_needed and engine and search_query:
        try:
            connection = engine.connect()
            if get_metric_profiles:
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('engine_needed and engine and search_query - get_metric_profiles - running query')

                # stmt = select([ionosphere_table]).where(ionosphere_table.c.metric_id == int(metric_id))
                stmt = select([ionosphere_table]).where(ionosphere_table.c.metric_id == int(metrics_id))
                logger.debug('debug :: stmt - is abstracted - for metric_id: %s' % str(int(metrics_id)))
            else:

                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                # Only queries the metrics with ids returned by the default LIMIT
                # search, instead of raising all the metrics in the metrics query
                # further above, just inefficient
                if default_ionosphere_search:
                    stmt = query_string
                    logger.debug('debug :: stmt - %s' % stmt)
                    default_search_metric_ids = []
                    try:
                        result = connection.execute(stmt)
                    except:
                        trace = traceback.format_exc()
                        logger.error('%s' % trace)
                        fail_msg = 'error :: MySQL query failed'
                        logger.error('%s' % fail_msg)
                        if engine:
                            engine_disposal(engine)
                        raise
                    for row in result:
                        try:
                            metric_id = int(row['metric_id'])
                            default_search_metric_ids.append(metric_id)
                        except:
                            trace = traceback.format_exc()
                            logger.error('%s' % trace)
                            logger.error('error :: bad row data')
                    logger.info('engine_needed and engine and search_query - determined %s metric ids for the default Ionosphere search' % str(len(default_search_metric_ids)))
                    metrics = []
                    select_metric_ids = ''
                    for default_search_metric_id in default_search_metric_ids:
                        if select_metric_ids == '':
                            select_metric_ids = '%s' % str(default_search_metric_id)
                        else:
                            select_metric_ids = '%s, %s' % (str(select_metric_ids), str(default_search_metric_id))
                    select_metric_ids_stmt = 'SELECT * FROM metrics WHERE id in (%s)' % select_metric_ids

                    # @modified 20201122 - Bug #3844: Handle no features profile in webapp
                    # When there are no features profiles in the ionosphere
                    # table, e.g. new install, the default webapp ionosphere
                    # features profiles page throws an error
                    # sqlalchemy.exc.ProgrammingError: (mysql.connector.errors.ProgrammingError) 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MariaDB server version for the right syntax to use near ')' at line 1
                    # [SQL: SELECT * FROM metrics WHERE id in ()]
                    if select_metric_ids == '':
                        select_metric_ids_stmt = 'SELECT * FROM metrics WHERE id=0'
                        logger.warning('warn :: determined 0 metrics with features profiles for the default Ionosphere search')

                    try:
                        metrics_result = connection.execute(select_metric_ids_stmt)
                        for row in metrics_result:
                            metric_id = int(row['id'])
                            metric_name = str(row['metric'])
                            metrics.append([metric_id, metric_name])
                        logger.info('engine_needed and engine and search_query - determined %s metric objects for the default Ionosphere search' % str(len(metrics)))
                    except:
                        trace = traceback.format_exc()
                        logger.error('%s' % trace)
                        fail_msg = 'error :: MySQL query failed - %s' % select_metric_ids_stmt
                        logger.error('%s' % fail_msg)
                        if engine:
                            engine_disposal(engine)
                        raise

                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('engine_needed and engine and search_query - not get_metric_profiles - running query')

                stmt = query_string
                logger.debug('debug :: stmt - %s' % stmt)

            # @added 20200506 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
            if find_unvalidated:
                logger.info('optimised - selecting all features profiles with validated == 0')
                stmt = select([ionosphere_table]).where(ionosphere_table.c.validated == 0)

            try:
                result = connection.execute(stmt)
            except:
                trace = traceback.format_exc()
                logger.error('%s' % trace)
                fail_msg = 'error :: MySQL query failed'
                logger.error('%s' % fail_msg)
                if engine:
                    engine_disposal(engine)
                raise
            for row in result:
                try:
                    fp_id = int(row['id'])
                    metric_id = int(row['metric_id'])
                    for metric_obj in metrics:
                        if metric_id == int(metric_obj[0]):
                            metric = metric_obj[1]
                            break
                    full_duration = int(row['full_duration'])
                    anomaly_timestamp = int(row['anomaly_timestamp'])
                    tsfresh_version = str(row['tsfresh_version'])
                    # These handle MySQL NULL
                    try:
                        calc_time = float(row['calc_time'])
                    except:
                        calc_time = 0
                    try:
                        features_count = int(row['features_count'])
                    except:
                        features_count = 0
                    try:
                        features_sum = float(row['features_sum'])
                    except:
                        features_sum = 0
                    try:
                        deleted = int(row['deleted'])
                    except:
                        deleted = 0
                    fp_matched_count = int(row['matched_count'])
                    last_matched = int(row['last_matched'])
                    if str(last_matched) == '0':
                        human_date = 'never matched'
                    else:
                        human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(last_matched)))
                    created_timestamp = str(row['created_timestamp'])
                    last_checked = int(row['last_checked'])
                    if str(last_checked) == '0':
                        checked_human_date = 'never checked'
                    else:
                        checked_human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(last_checked)))
                    fp_checked_count = int(row['checked_count'])
                    fp_parent_id = int(row['parent_id'])
                    fp_generation = int(row['generation'])
                    # @added 20170402 - Feature #2000: Ionosphere - validated
                    fp_validated = int(row['validated'])

                    fp_layers_id = int(row['layers_id'])
                    # @added 20170322 - Feature #1960: ionosphere_layers
                    # Added layers information to the features_profiles items
                    if fp_layers_id > 0:
                        layers_present = True

                    # @added 20210415 - Feature #4014: Ionosphere - inference
                    #                   Branch #3590: inference
                    motif_matched_count = int(row['motif_matched_count'])
                    motif_last_matched = int(row['motif_last_matched'])
                    motif_last_checked = int(row['motif_last_checked'])
                    motif_checked_count = int(row['motif_checked_count'])

                    # @modified 20180812 - Feature #2430: Ionosphere validate learnt features profiles page
                    # Fix bug and make this function output useable to
                    # get_features_profiles_to_validate
                    append_to_features_profile_list = True
                    if 'validated_equals' in request.args:
                        validated_equals = request.args.get('validated_equals', 'any')
                    else:
                        validated_equals = 'any'
                    if validated_equals == 'false':
                        if fp_validated == 1:
                            append_to_features_profile_list = False
                    if append_to_features_profile_list:
                        # @modifed 20210415 - Feature #4014: Ionosphere - inference
                        #                     Branch #3590: inference
                        # features_profiles.append([fp_id, metric_id, str(metric), full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id])
                        if str(motif_last_checked) == '0':
                            motif_last_checked_human_date = 'never checked'
                        else:
                            motif_last_checked_human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(motif_last_checked)))
                        if str(motif_last_matched) == '0':
                            motif_last_matched_human_date = 'never matched'
                        else:
                            motif_last_matched_human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(motif_last_matched)))
                        features_profiles.append([fp_id, metric_id, str(metric), full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id, motif_matched_count, motif_last_matched_human_date, motif_last_checked_human_date, motif_checked_count])
                    # @added 20170912 - Feature #2056: ionosphere - disabled_features_profiles
                    features_profile_enabled = int(row['enabled'])
                    if features_profile_enabled == 1:
                        enabled_list.append(fp_id)
                except:
                    trace = traceback.format_exc()
                    logger.error('%s' % trace)
                    logger.error('error :: bad row data')
            connection.close()
            features_profiles.sort(key=operator.itemgetter(int(0)))
            logger.debug('debug :: features_profiles length - %s' % str(len(features_profiles)))
            # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
            logger.info('engine_needed and engine and search_query - got features_profiles')
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: failed to get ionosphere_table data'
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            raise

    # @added 20170322 - Feature #1960: ionosphere_layers
    # Added layers information to the features_profiles items
    features_profiles_layers = []
    if features_profiles and layers_present:
        try:
            ionosphere_layers_table, log_msg, trace = ionosphere_layers_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('features_profiles and layers_present - ionosphere_layers OK')
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: features_profiles and layers_present - failed to get ionosphere_layers meta'
            logger.error('%s' % fail_msg)
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI
        try:
            connection = engine.connect()
            if get_metric_profiles:
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('features_profiles and layers_present - get_metric_profiles - running query')
                stmt = select([ionosphere_layers_table]).where(ionosphere_layers_table.c.metric_id == int(metrics_id))
                # logger.debug('debug :: stmt - is abstracted')
            else:
                layers_query_string = 'SELECT * FROM ionosphere_layers'
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('features_profiles and layers_present - not get_metric_profiles - running query - "%s"' % layers_query_string)
                stmt = layers_query_string
                # logger.debug('debug :: stmt - %s' % stmt)
            result = connection.execute(stmt)
            for row in result:
                try:
                    layer_id = int(row['id'])
                    fp_id = int(row['fp_id'])
                    layer_matched_count = int(row['matched_count'])
                    layer_last_matched = int(row['last_matched'])
                    if str(layer_last_matched) == '0':
                        layer_human_date = 'never matched'
                    else:
                        layer_human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(layer_last_matched)))
                    layer_last_checked = int(row['last_checked'])
                    # @modified 20170924 - Feature #2170: Ionosphere - validated matches
                    # Fixed variable typo which resulted in layer last checked
                    # field showing 1970-01-01 00:00:00 UTC (Thursday)
                    # if str(last_checked) == '0':
                    if str(layer_last_checked) == '0':
                        layer_checked_human_date = 'never checked'
                    else:
                        layer_checked_human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(layer_last_checked)))
                    layer_check_count = int(row['check_count'])
                    layer_label = str(row['label'])
                    features_profiles_layers.append([layer_id, fp_id, layer_matched_count, layer_human_date, layer_check_count, layer_checked_human_date, layer_label])
                except:
                    trace = traceback.format_exc()
                    logger.error('%s' % trace)
                    logger.error('error :: bad row data')
            connection.close()
            features_profiles_layers.sort(key=operator.itemgetter(int(0)))
            logger.debug('debug :: features_profiles and layers_present - features_profiles length - %s' % str(len(features_profiles)))
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: failed to get ionosphere_table data'
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            raise

    # Add the layers information to the features_profiles list
    features_profiles_and_layers = []
    if features_profiles:
        # @modified 20170402 - Feature #2000: Ionosphere - validated
        # @modifed 20210415 - Feature #4014: Ionosphere - inference
        #                     Branch #3590: inference
        # for fp_id, metric_id, metric, full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id in features_profiles:
        for fp_id, metric_id, metric, full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id, motif_matched_count, motif_last_matched, motif_last_checked, motif_checked_count in features_profiles:
            default_values = True
            # @modified 20180816 - Feature #2430: Ionosphere validate learnt features profiles page
            # Moved default_values to before the evalution as it was found
            # that sometimes the features_profiles had 19 elements if a
            # features profile had no layer or 23 elements if there was a
            # layer
            if default_values:
                layer_id = 0
                layer_matched_count = 0
                layer_human_date = 'none'
                layer_check_count = 0
                layer_checked_human_date = 'none'
                layer_label = 'none'
            if int(fp_layers_id) > 0:
                for layer_id, layer_fp_id, layer_matched_count, layer_human_date, layer_check_count, layer_checked_human_date, layer_label in features_profiles_layers:
                    if int(fp_layers_id) == int(layer_id):
                        default_values = False
                        break
            # @modifed 20210415 - Feature #4014: Ionosphere - inference
            #                     Branch #3590: inference
            # features_profiles_and_layers.append([fp_id, metric_id, metric, full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id, layer_matched_count, layer_human_date, layer_check_count, layer_checked_human_date, layer_label])
            features_profiles_and_layers.append([fp_id, metric_id, metric, full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id, layer_matched_count, layer_human_date, layer_check_count, layer_checked_human_date, layer_label, motif_matched_count, motif_last_matched, motif_last_checked, motif_checked_count])

        # old_features_profile_list = features_profiles
        features_profiles = features_profiles_and_layers

        full_duration_list = None
        # @modified 20170912 - Feature #2056: ionosphere - disabled_features_profiles
        # enabled_list = None
        if not enabled_list:
            # @modified 20190816 - Bug #3190: webapp error - search_features_profiles_block when all features profiles are disabled
            # If all features profiles have been disabled this needs to be
            # passed as a empty list to be iterated in search_features_profiles
            # template because type 'NoneType' is not iterable
            # enabled_list = None
            enabled_list = []
        tsfresh_version_list = None
        generation_list = None
        if engine:
            engine_disposal(engine)
        try:
            del all_fps
        except:
            logger.error('error :: failed to del all_fps')
        try:
            del metrics
        except:
            logger.error('error :: failed to del metrics')
        search_success = True
        return (features_profiles, features_profiles_count, matched_count,
                checked_count, generation_count, full_duration_list,
                enabled_list, tsfresh_version_list, generation_list,
                search_success, fail_msg, trace)

    get_options = [
        'full_duration', 'enabled', 'tsfresh_version', 'generation']

    if engine_needed and engine and default_query:
        for required_option in get_options:
            all_list = []
#            required_option = 'full_duration'
            try:
                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                stmt = 'SELECT %s FROM ionosphere WHERE enabled=1' % str(required_option)  # nosec
                # @added 20200404 - Task #3464: Optimise ionosphere_backend ionosphere_search queries
                logger.info('engine_needed and engine and default_query - running query - "%s"' % stmt)
                connection = engine.connect()
                for row in engine.execute(stmt):
                    value = row[str(required_option)]
                    all_list.append(value)
                connection.close()
            except:
                trace = traceback.format_exc()
                logger.error('%s' % trace)
                fail_msg = 'error :: failed to get ionosphere_table meta for options'
                logger.error('%s' % fail_msg)
                if engine:
                    engine_disposal(engine)
                raise

            if required_option == 'full_duration':
                full_duration_list = set(all_list)
            if required_option == 'enabled':
                enabled_list = set(all_list)
            if required_option == 'tsfresh_version':
                tsfresh_version_list = set(all_list)
            if required_option == 'generation':
                generation_list = set(all_list)

    if engine:
        engine_disposal(engine)
    try:
        del all_fps
    except:
        logger.error('error :: failed to del all_fps')
    try:
        del metrics
    except:
        logger.error('error :: failed to del metrics')

    search_success = True
    return (features_profiles, features_profiles_count, matched_count,
            checked_count, generation_count, full_duration_list,
            enabled_list, tsfresh_version_list, generation_list, search_success,
            fail_msg, trace)


# @added 20170305 - Feature #1960: ionosphere_layers
def create_ionosphere_layers(base_name, fp_id, requested_timestamp):
    """
    Create a layers profile.

    :param None: determined from :obj:`request.args`
    :return: array
    :rtype: array

    """

    function_str = 'ionoshere_backend.py :: create_ionosphere_layers'

    trace = 'none'
    fail_msg = 'none'
    layers_algorithms = None
    layers_added = None

    value_conditions = ['<', '>', '==', '!=', '<=', '>=']
    conditions = ['<', '>', '==', '!=', '<=', '>=', 'in', 'not in']

    if 'd_condition' in request.args:
        d_condition = request.args.get('d_condition', '==')
    else:
        logger.error('no d_condition argument passed')
        fail_msg = 'error :: no d_condition argument passed'
        return False, False, layers_algorithms, layers_added, fail_msg, trace

    if not str(d_condition) in conditions:
        logger.error('d_condition not a valid conditon - %s' % str(d_condition))
        fail_msg = 'error :: d_condition not a valid conditon - %s' % str(d_condition)
        return False, False, layers_algorithms, layers_added, fail_msg, trace

    if 'd_boundary_limit' in request.args:
        d_boundary_limit = request.args.get('d_boundary_limit', '0')
    else:
        logger.error('no d_boundary_limit argument passed')
        fail_msg = 'error :: no d_boundary_limit argument passed'
        return False, False, layers_algorithms, layers_added, fail_msg, trace

    try:
        # @modified 20170317 - Feature #1960: ionosphere_layers - allow for floats
        # test_d_boundary_limit = int(d_boundary_limit) + 1
        test_d_boundary_limit = float(d_boundary_limit) + 1
        logger.info('d_boundary_limit tested OK with %s' % str(test_d_boundary_limit))
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: d_boundary_limit is not an int'
        return False, False, layers_algorithms, layers_added, fail_msg, trace

    # @modified 20160315 - Feature #1972: ionosphere_layers - use D layer boundary for upper limit
    # Added d_boundary_times
    if 'd_boundary_times' in request.args:
        d_boundary_times = request.args.get('d_boundary_times', '1')
    else:
        logger.error('no d_boundary_times argument passed')
        fail_msg = 'error :: no d_boundary_times argument passed'
        return False, False, layers_algorithms, layers_added, fail_msg, trace
    try:
        test_d_boundary_times = int(d_boundary_times) + 1
        logger.info('d_boundary_times tested OK with %s' % str(test_d_boundary_times))
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: d_boundary_times is not an int'
        return False, False, layers_algorithms, layers_added, fail_msg, trace

    # @added 20170616 - Feature #2048: D1 ionosphere layer
    if 'd1_condition' in request.args:
        d1_condition = request.args.get('d1_condition', 'none')
    else:
        logger.error('no d1_condition argument passed')
        fail_msg = 'error :: no d1_condition argument passed'
        return False, False, layers_algorithms, layers_added, fail_msg, trace
    if str(d1_condition) == 'none':
        d1_condition = 'none'
        d1_boundary_limit = 0
        d1_boundary_times = 0
    else:
        if not str(d1_condition) in conditions:
            logger.error('d1_condition not a valid conditon - %s' % str(d1_condition))
            fail_msg = 'error :: d1_condition not a valid conditon - %s' % str(d1_condition)
            return False, False, layers_algorithms, layers_added, fail_msg, trace

        if 'd1_boundary_limit' in request.args:
            d1_boundary_limit = request.args.get('d1_boundary_limit', '0')
        else:
            logger.error('no d1_boundary_limit argument passed')
            fail_msg = 'error :: no d1_boundary_limit argument passed'
            return False, False, layers_algorithms, layers_added, fail_msg, trace
        try:
            test_d1_boundary_limit = float(d1_boundary_limit) + 1
            logger.info('d1_boundary_limit tested OK with %s' % str(test_d1_boundary_limit))
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: d1_boundary_limit is not an int'
            return False, False, layers_algorithms, layers_added, fail_msg, trace
        if 'd1_boundary_times' in request.args:
            d1_boundary_times = request.args.get('d1_boundary_times', '1')
        else:
            logger.error('no d1_boundary_times argument passed')
            fail_msg = 'error :: no d1_boundary_times argument passed'
            return False, False, layers_algorithms, layers_added, fail_msg, trace
        try:
            test_d1_boundary_times = int(d1_boundary_times) + 1
            logger.info('d1_boundary_times tested OK with %s' % str(test_d1_boundary_times))
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: d1_boundary_times is not an int'
            return False, False, layers_algorithms, layers_added, fail_msg, trace

    if 'e_condition' in request.args:
        e_condition = request.args.get('e_condition', None)
    else:
        logger.error('no e_condition argument passed')
        fail_msg = 'error :: no e_condition argument passed'
        return False, False, layers_algorithms, layers_added, fail_msg, trace

    if not str(e_condition) in value_conditions:
        logger.error('e_condition not a valid value conditon - %s' % str(e_condition))
        fail_msg = 'error :: e_condition not a valid value conditon - %s' % str(e_condition)
        return False, False, layers_algorithms, layers_added, fail_msg, trace

    if 'e_boundary_limit' in request.args:
        e_boundary_limit = request.args.get('e_boundary_limit')
    else:
        logger.error('no e_boundary_limit argument passed')
        fail_msg = 'error :: no e_boundary_limit argument passed'
        return False, False, layers_algorithms, layers_added, fail_msg, trace

    try:
        # @modified 20170317 - Feature #1960: ionosphere_layers - allow for floats
        # test_e_boundary_limit = int(e_boundary_limit) + 1
        test_e_boundary_limit = float(e_boundary_limit) + 1
        logger.info('test_e_boundary_limit tested OK with %s' % str(test_e_boundary_limit))
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: e_boundary_limit is not an int'
        return False, False, layers_algorithms, layers_added, fail_msg, trace

    if 'e_boundary_times' in request.args:
        e_boundary_times = request.args.get('e_boundary_times')
    else:
        logger.error('no e_boundary_times argument passed')
        fail_msg = 'error :: no e_boundary_times argument passed'
        return False, False, layers_algorithms, layers_added, fail_msg, trace

    try:
        test_e_boundary_times = int(e_boundary_times) + 1
        logger.info('test_e_boundary_times tested OK with %s' % str(test_e_boundary_times))
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: e_boundary_times is not an int'
        return False, False, layers_algorithms, layers_added, fail_msg, trace

    es_layer = False
    if 'es_layer' in request.args:
        es_layer_arg = request.args.get('es_layer')
        if es_layer_arg == 'true':
            es_layer = True
    if es_layer:
        es_day = None
        if 'es_day' in request.args:
            es_day = request.args.get('es_day')
        else:
            logger.error('no es_day argument passed')
            fail_msg = 'error :: no es_day argument passed'
            return False, False, layers_algorithms, layers_added, fail_msg, trace

    f1_layer = False
    if 'f1_layer' in request.args:
        f1_layer_arg = request.args.get('f1_layer')
        if f1_layer_arg == 'true':
            f1_layer = True
    if f1_layer:
        from_time = None
        valid_f1_from_time = False
        if 'from_time' in request.args:
            from_time = request.args.get('from_time')
        if from_time:
            values_valid = True
            if len(from_time) == 4:
                for digit in from_time:
                    try:
                        int(digit) + 1
                    except:
                        values_valid = False
            if values_valid:
                if int(from_time) < 2400:
                    valid_f1_from_time = True
        if not valid_f1_from_time:
            logger.error('no valid f1_layer from_time argument passed - %s' % str(from_time))
            fail_msg = 'error :: no valid f1_layer from_time argument passed - %s' % str(from_time)
            return False, False, layers_algorithms, layers_added, fail_msg, trace

    f2_layer = False
    if 'f2_layer' in request.args:
        f2_layer_arg = request.args.get('f2_layer')
        if f2_layer_arg == 'true':
            f2_layer = True
    if f2_layer:
        until_time = None
        valid_f2_until_time = False
        if 'until_time' in request.args:
            until_time = request.args.get('until_time')
        if until_time:
            values_valid = True
            if len(until_time) == 4:
                for digit in until_time:
                    try:
                        int(digit) + 1
                    except:
                        values_valid = False
            if values_valid:
                if int(until_time) < 2400:
                    valid_f2_until_time = True
        if not valid_f2_until_time:
            logger.error('no valid f2_layer until_time argument passed - %s' % str(until_time))
            fail_msg = 'error :: no valid f2_layer until_time argument passed - %s' % str(until_time)
            return False, False, layers_algorithms, layers_added, fail_msg, trace

    label = False
    if 'fp_layer_label' in request.args:
        label_arg = request.args.get('fp_layer_label')
        label = label_arg[:255]

    engine_needed = True
    engine = None
    if engine_needed:
        logger.info('%s :: getting MySQL engine' % function_str)
        try:
            engine, fail_msg, trace = get_an_engine()
            logger.info(fail_msg)
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: could not get a MySQL engine'
            logger.error('%s' % fail_msg)
            raise

        if not engine:
            trace = 'none'
            fail_msg = 'error :: engine not obtained'
            logger.error(fail_msg)
            raise

        try:
            metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('metrics_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to get metrics_table meta')
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI

    metrics_id = 0
    try:
        connection = engine.connect()
        stmt = select([metrics_table]).where(metrics_table.c.metric == base_name)
        result = connection.execute(stmt)
        for row in result:
            metrics_id = int(row['id'])
        connection.close()
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not determine metric id from metrics table'
        if engine:
            engine_disposal(engine)
        raise

    # Create layer profile
    ionosphere_layers_table = None
    try:
        ionosphere_layers_table, fail_msg, trace = ionosphere_layers_table_meta(skyline_app, engine)
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: ionosphere_backend :: failed to get ionosphere_layers_table meta for %s' % base_name
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise

    layer_id = 0
    try:
        connection = engine.connect()
        stmt = select([ionosphere_layers_table]).where(ionosphere_layers_table.c.fp_id == fp_id)
        result = connection.execute(stmt)
        for row in result:
            layer_id = int(row['id'])
        connection.close()
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not determine id from ionosphere_layers_table'
        if engine:
            engine_disposal(engine)
        raise

    if layer_id > 0:
        return layer_id, True, None, None, fail_msg, trace

    new_layer_id = False
    try:
        connection = engine.connect()
        ins = ionosphere_layers_table.insert().values(
            fp_id=fp_id, metric_id=int(metrics_id), enabled=1, label=label)
        result = connection.execute(ins)
        connection.close()
        new_layer_id = result.inserted_primary_key[0]
        logger.info('new ionosphere layer_id: %s' % str(new_layer_id))
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to insert a new record into the ionosphere_layers table for %s' % base_name
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise

    # Create layer profile
    layers_algorithms_table = None
    try:
        layers_algorithms_table, fail_msg, trace = layers_algorithms_table_meta(skyline_app, engine)
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: ionosphere_backend :: failed to get layers_algorithms_table meta for %s' % base_name
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise

    new_layer_algorithm_ids = []
    layers_added = []
    # D layer
    try:
        connection = engine.connect()
        ins = layers_algorithms_table.insert().values(
            layer_id=new_layer_id, fp_id=fp_id, metric_id=int(metrics_id),
            layer='D', type='value', condition=d_condition,
            # @modified 20170317 - Feature #1960: ionosphere_layers - allow for floats
            # layer_boundary=int(d_boundary_limit),
            layer_boundary=str(d_boundary_limit),
            # @modified 20160315 - Feature #1972: ionosphere_layers - use D layer boundary for upper limit
            # Added d_boundary_times
            times_in_row=int(d_boundary_times))
        result = connection.execute(ins)
        connection.close()
        new_layer_algorithm_id = result.inserted_primary_key[0]
        logger.info('new ionosphere_algorithms D layer id: %s' % str(new_layer_algorithm_id))
        new_layer_algorithm_ids.append(new_layer_algorithm_id)
        layers_added.append('D')
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to insert a new D layer record into the layers_algorithms table for %s' % base_name
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise

    # E layer
    try:
        connection = engine.connect()
        ins = layers_algorithms_table.insert().values(
            layer_id=new_layer_id, fp_id=fp_id, metric_id=int(metrics_id),
            layer='E', type='value', condition=e_condition,
            # @modified 20170317 - Feature #1960: ionosphere_layers - allow for floats
            # layer_boundary=int(e_boundary_limit),
            layer_boundary=str(e_boundary_limit),
            times_in_row=int(e_boundary_times))
        result = connection.execute(ins)
        connection.close()
        new_layer_algorithm_id = result.inserted_primary_key[0]
        logger.info('new ionosphere_algorithms E layer id: %s' % str(new_layer_algorithm_id))
        new_layer_algorithm_ids.append(new_layer_algorithm_id)
        layers_added.append('E')
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to insert a new E layer record into the layers_algorithms table for %s' % base_name
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise

    # @added 20170616 - Feature #2048: D1 ionosphere layer
    # This must be the third created algorithm layer as in the frontend list
    # D is [0], E is [1], so D1 has to be [2]
    if d1_condition:
        try:
            connection = engine.connect()
            ins = layers_algorithms_table.insert().values(
                layer_id=new_layer_id, fp_id=fp_id, metric_id=int(metrics_id),
                layer='D1', type='value', condition=d1_condition,
                layer_boundary=str(d1_boundary_limit),
                times_in_row=int(d1_boundary_times))
            result = connection.execute(ins)
            connection.close()
            new_layer_algorithm_id = result.inserted_primary_key[0]
            logger.info('new ionosphere_algorithms D1 layer id: %s' % str(new_layer_algorithm_id))
            new_layer_algorithm_ids.append(new_layer_algorithm_id)
            layers_added.append('D1')
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: failed to insert a new D1 layer record into the layers_algorithms table for %s' % base_name
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            raise

    # Es layer
    if es_layer:
        try:
            connection = engine.connect()
            ins = layers_algorithms_table.insert().values(
                layer_id=new_layer_id, fp_id=fp_id, metric_id=int(metrics_id),
                layer='Es', type='day', condition='in', layer_boundary=es_day)
            result = connection.execute(ins)
            connection.close()
            new_layer_algorithm_id = result.inserted_primary_key[0]
            logger.info('new ionosphere_algorithms Es layer id: %s' % str(new_layer_algorithm_id))
            new_layer_algorithm_ids.append(new_layer_algorithm_id)
            layers_added.append('Es')
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: failed to insert a new Es layer record into the layers_algorithms table for %s' % base_name
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            raise

    # F1 layer
    if f1_layer:
        try:
            connection = engine.connect()
            ins = layers_algorithms_table.insert().values(
                layer_id=new_layer_id, fp_id=fp_id, metric_id=int(metrics_id),
                layer='F1', type='time', condition='>',
                layer_boundary=str(from_time))
            result = connection.execute(ins)
            connection.close()
            new_layer_algorithm_id = result.inserted_primary_key[0]
            logger.info('new ionosphere_algorithms F1 layer id: %s' % str(new_layer_algorithm_id))
            new_layer_algorithm_ids.append(new_layer_algorithm_id)
            layers_added.append('F1')
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: failed to insert a new F1 layer record into the layers_algorithms table for %s' % base_name
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            raise

    # F2 layer
    if f2_layer:
        try:
            connection = engine.connect()
            ins = layers_algorithms_table.insert().values(
                layer_id=new_layer_id, fp_id=fp_id, metric_id=int(metrics_id),
                layer='F2', type='time', condition='<',
                layer_boundary=str(until_time))
            result = connection.execute(ins)
            connection.close()
            new_layer_algorithm_id = result.inserted_primary_key[0]
            logger.info('new ionosphere_algorithms F2 layer id: %s' % str(new_layer_algorithm_id))
            new_layer_algorithm_ids.append(new_layer_algorithm_id)
            layers_added.append('F2')
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: failed to insert a new F2 layer record into the layers_algorithms table for %s' % base_name
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            raise

    ionosphere_table = None
    try:
        ionosphere_table, fail_msg, trace = ionosphere_table_meta(skyline_app, engine)
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to get ionosphere_table meta for options'
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise
    logger.info('%s :: ionosphere_table OK' % function_str)

    try:
        connection = engine.connect()
        connection.execute(
            ionosphere_table.update(
                ionosphere_table.c.id == fp_id).
            values(layers_id=new_layer_id))
        connection.close()
        logger.info('updated layers_id for %s' % str(fp_id))
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: could not update layers_id for %s ' % str(fp_id)
        logger.error(fail_msg)
        # @added 20170806 - Bug #2130: MySQL - Aborted_clients
        # Added missing disposal
        if engine:
            engine_disposal(engine)
        raise

    if engine:
        engine_disposal(engine)

    # @added 20190502 -

    return new_layer_id, True, layers_added, new_layer_algorithm_ids, fail_msg, trace


def feature_profile_layers_detail(fp_layers_id):
    """
    Get the Ionosphere layers details of a fetures profile

    :param fp_layers_id: the features profile layers_id
    :type fp_id: str
    :return: tuple
    :rtype:  (str, boolean, str, str, object)

    """
    logger = logging.getLogger(skyline_app_logger)

    function_str = 'ionoshere_backend.py :: features_profile_layers_details'

    trace = 'none'
    fail_msg = 'none'
    # fp_details = None

    logger.info('%s :: getting MySQL engine' % function_str)
    try:
        engine, fail_msg, trace = get_an_engine()
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not get a MySQL engine'
        logger.error('%s' % fail_msg)
        # return False, False, fail_msg, trace, False
        raise  # to webapp to return in the UI

    if not engine:
        trace = 'none'
        fail_msg = 'error :: engine not obtained'
        logger.error(fail_msg)
        # return False, False, fail_msg, trace, False
        raise  # to webapp to return in the UI

    ionosphere_layers_table = None
    try:
        ionosphere_layers_table, fail_msg, trace = ionosphere_layers_table_meta(skyline_app, engine)
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to get ionosphere_layers_table meta for fp_id %s details' % str(fp_layers_id)
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        # return False, False, fail_msg, trace, False
        raise  # to webapp to return in the UI

    logger.info('%s :: ionosphere_layers_table OK' % function_str)

    try:
        connection = engine.connect()
        stmt = select([ionosphere_layers_table]).where(ionosphere_layers_table.c.id == int(fp_layers_id))
        result = connection.execute(stmt)
        row = result.fetchone()
        layer_details_object = row
        connection.close()
        feature_profile_id = row['fp_id']
        metric_id = row['metric_id']
        enabled = row['enabled']
        deleted = row['deleted']
        matched_count = row['matched_count']
        last_matched = row['last_matched']
        if str(last_matched) == '0':
            human_date = 'never matched'
        else:
            human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(last_matched)))
        created_timestamp = row['created_timestamp']
        last_checked = row['last_checked']
        if str(last_checked) == '0':
            checked_human_date = 'never checked'
        else:
            checked_human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(last_checked)))
        check_count = row['check_count']
        label = row['label']
        layer_details = '''
fp_id             :: %s | metric_id :: %s
enabled           :: %s
deleted           :: %s
matched_count     :: %s
last_matched      :: %s | human_date :: %s
created_timestamp :: %s
checked_count     :: %s
last_checked      :: %s | human_date :: %s
label             :: %s
''' % (str(feature_profile_id), str(metric_id), str(enabled), str(deleted),
            str(matched_count), str(last_matched), str(human_date),
            str(created_timestamp), str(check_count),
            str(last_checked), str(checked_human_date), str(label))
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not get layers_id %s details from ionosphere_layers DB table' % str(fp_layers_id)
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    if engine:
        engine_disposal(engine)

    return layer_details, True, fail_msg, trace, layer_details_object


def feature_profile_layer_alogrithms(fp_layers_id):
    """
    Get the Ionosphere layer algorithm details of a layer

    :param fp_layers_id: the features profile layers_id
    :type fp_id: str
    :return: tuple
    :rtype:  (str, boolean, str, str)

    """
    logger = logging.getLogger(skyline_app_logger)

    function_str = 'ionoshere_backend.py :: features_profile_layer_algorithms'

    trace = 'none'
    fail_msg = 'none'
    # fp_details = None

    logger.info('%s :: getting MySQL engine' % function_str)
    try:
        engine, fail_msg, trace = get_an_engine()
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not get a MySQL engine'
        logger.error('%s' % fail_msg)
        # return False, False, fail_msg, trace, False
        raise  # to webapp to return in the UI

    if not engine:
        trace = 'none'
        fail_msg = 'error :: engine not obtained'
        logger.error(fail_msg)
        # return False, False, fail_msg, trace, False
        raise  # to webapp to return in the UI
    layers_algorithms_table = None
    try:
        layers_algorithms_table, fail_msg, trace = layers_algorithms_table_meta(skyline_app, engine)
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to get layers_algorithms_table meta for fp_id %s details' % str(fp_layers_id)
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        # return False, False, fail_msg, trace, False
        raise  # to webapp to return in the UI

    logger.info('%s :: layers_algorithms_table OK' % function_str)
    es_condition = None
    es_day = None
    es_layer = ' [\'NOT ACTIVE - Es layer not created\']'
    f1_from_time = None
    f1_layer = ' [\'NOT ACTIVE - F1 layer not created\']'
    f2_until_time = None
    f2_layer = ' [\'NOT ACTIVE - F2 layer not created\']'
    # @added 20170616 - Feature #2048: D1 ionosphere layer
    d1_layer = ' [\'NOT ACTIVE - D1 layer not created\']'
    d1_condition = 'none'
    d1_boundary_limit = 'none'
    d1_boundary_times = 'none'

    try:
        connection = engine.connect()
        stmt = select([layers_algorithms_table]).where(layers_algorithms_table.c.layer_id == int(fp_layers_id))
        result = connection.execute(stmt)
        connection.close()
        layer_algorithms_details_object = result
        layer_active = '[\'ACTIVE\']'
        for row in result:
            layer = row['layer']
            if layer == 'D':
                d_condition = row['condition']
                d_boundary_limit = row['layer_boundary']
            # @added 20170616 - Feature #2048: D1 ionosphere layer
            if layer == 'D1':
                d1_condition = row['condition']
                if str(d1_condition) != 'none':
                    d1_condition = row['condition']
                    d1_layer = ' [\'ACTIVE\']'
                    d1_boundary_limit = row['layer_boundary']
                    d1_boundary_times = row['times_in_row']
                else:
                    d1_condition = 'none'
            if layer == 'E':
                e_condition = row['condition']
                e_boundary_limit = row['layer_boundary']
                e_boundary_times = row['times_in_row']
            if layer == 'Es':
                es_condition = row['condition']
                es_day = row['layer_boundary']
                es_layer = layer_active
            if layer == 'F1':
                f1_from_time = row['layer_boundary']
                f1_layer = layer_active
            if layer == 'F2':
                f2_until_time = row['layer_boundary']
                f2_layer = layer_active

        layer_algorithms_details = '''
D layer  :: if value %s %s                    :: [do not check]  ::  ['ACTIVE']
D1 layer :: if value %s %s in last %s values  :: [do not check]  ::  %s
E layer  :: if value %s %s in last %s values  :: [not_anomalous, if active Es, F1 and F2 layers match]  ::  ['ACTIVE']
Es layer :: if day %s %s                 :: [not_anomalous, if active F1 and F2 layers match]  ::  %s
F1 layer :: if from_time > %s              :: [not_anomalous, if active F2 layer matchs]  ::  %s
F2 layer :: if until_time < %s             :: [not_anomalous]  ::  %s
''' % (str(d_condition), str(d_boundary_limit), str(d1_condition),
            str(d1_boundary_limit), str(d1_boundary_times), str(d1_layer),
            str(e_condition), str(e_boundary_limit), str(e_boundary_times),
            str(es_condition), str(es_day),
            str(es_layer), str(f1_from_time), str(f1_layer), str(f2_until_time),
            str(f2_layer))
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not get layers_algorithms for layer_id %s from layers_algorithms DB table' % str(fp_layers_id)
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    if engine:
        engine_disposal(engine)

    return layer_algorithms_details, True, fail_msg, trace, layer_algorithms_details_object


# @added 20170308 - Feature #1960: ionosphere_layers
# To present the operator with the existing layers and algorithms for the metric
def metric_layers_alogrithms(base_name):
    """
    Get the Ionosphere layer algorithm details of a metric

    :param base_name: the metric base_name
    :type base_name: str
    :return: tuple
    :rtype:  (str, boolean, str, str)

    """
    logger = logging.getLogger(skyline_app_logger)

    function_str = 'ionoshere_backend.py :: metric_layers_alogrithms'

    trace = 'none'
    fail_msg = 'none'
    metric_layers_algorithm_details = None

    logger.info('%s :: getting MySQL engine' % function_str)
    try:
        engine, fail_msg, trace = get_an_engine()
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not get a MySQL engine'
        logger.error('%s' % fail_msg)
        raise  # to webapp to return in the UI

    if not engine:
        trace = 'none'
        fail_msg = 'error :: engine not obtained'
        logger.error(fail_msg)
        raise  # to webapp to return in the UI

    try:
        metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('metrics_table OK')
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: failed to get metrics_table meta'
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    metric_id = 0
    try:
        connection = engine.connect()
        stmt = select([metrics_table]).where(metrics_table.c.metric == base_name)
        result = connection.execute(stmt)
        connection.close()
        for row in result:
            metric_id = int(row['id'])
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: failed to get id for %s from metrics table' % str(base_name)
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    if not metric_id:
        # @added 20181024 - Bug #2638: anomalies db table - anomalous_datapoint greater than DECIMAL
        # For debugging
        trace = traceback.format_exc()
        logger.error(trace)

        fail_msg = 'error :: no id for %s' % str(base_name)
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    ionosphere_layers_table = None
    try:
        ionosphere_layers_table, fail_msg, trace = ionosphere_layers_table_meta(skyline_app, engine)
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to get ionosphere_layers_table meta for %s details' % str(base_name)
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    metric_layers_details = []
    metric_layers_count = 0
    metric_layers_matched_count = 0
    try:
        connection = engine.connect()
        stmt = select([ionosphere_layers_table]).where(ionosphere_layers_table.c.metric_id == metric_id)
        result = connection.execute(stmt)
        connection.close()
        for row in result:
            try:
                l_id = row['id']
                l_fp_id = row['fp_id']
                l_metric_id = row['metric_id']
                l_matched_count = row['matched_count']
                l_check_count = row['check_count']
                l_label = str(row['label'])
                metric_layers_details.append([l_id, l_fp_id, l_metric_id, l_matched_count, l_check_count, l_label])
                metric_layers_count += 1
                metric_layers_matched_count += int(l_matched_count)
                logger.info('%s :: added layer id %s to layer count' % (function_str, str(l_id)))
            except:
                metric_layers_count += 0
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not get layers ids for metric_id %s from ionosphere_layers DB table' % str(metric_id)
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    layers_algorithms_table = None
    try:
        layers_algorithms_table, fail_msg, trace = layers_algorithms_table_meta(skyline_app, engine)
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to get layers_algorithms_table meta for base_name %s details' % str(base_name)
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    metric_layers_algorithm_details = []
    logger.info('%s :: layers_algorithms_table OK' % function_str)
    try:
        connection = engine.connect()
        stmt = select([layers_algorithms_table]).where(layers_algorithms_table.c.metric_id == metric_id)
        result = connection.execute(stmt)
        connection.close()
        for row in result:
            la_id = row['id']
            la_layer_id = row['layer_id']
            la_fp_id = row['fp_id']
            la_metric_id = row['metric_id']
            la_layer = str(row['layer'])
            la_type = str(row['type'])
            la_condition = str(row['condition'])
            la_layer_boundary = str(row['layer_boundary'])
            la_times_in_a_row = row['times_in_row']
            metric_layers_algorithm_details.append([la_id, la_layer_id, la_fp_id, la_metric_id, la_layer, la_type, la_condition, la_layer_boundary, la_times_in_a_row])
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not get layers_algorithms for metric_id %s from layers_algorithms DB table' % str(metric_id)
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    if engine:
        engine_disposal(engine)

    logger.info('metric_layers_details :: %s' % str(metric_layers_details))
    logger.info('metric_layers_algorithm_details :: %s' % str(metric_layers_algorithm_details))

    return metric_layers_details, metric_layers_algorithm_details, metric_layers_count, metric_layers_matched_count, True, fail_msg, trace


# @added 20170327 - Feature #2004: Ionosphere layers - edit_layers
#                   Task #2002: Review and correct incorrectly defined layers
def edit_ionosphere_layers(layers_id):
    """
    Edit a layers profile.

    :param layers_id: the layer id to edit
    :return: array
    :rtype: array

    """
    logger = logging.getLogger(skyline_app_logger)

    function_str = 'ionoshere_backend.py :: edit_ionosphere_layers'

    logger.info('updating layers for %s' % str(layers_id))

    trace = 'none'
    fail_msg = 'none'

    value_conditions = ['<', '>', '==', '!=', '<=', '>=']
    conditions = ['<', '>', '==', '!=', '<=', '>=', 'in', 'not in']

    if 'd_condition' in request.args:
        d_condition = request.args.get('d_condition', '==')
    else:
        logger.error('no d_condition argument passed')
        fail_msg = 'error :: no d_condition argument passed'
        return False, fail_msg, trace

    if not str(d_condition) in conditions:
        logger.error('d_condition not a valid conditon - %s' % str(d_condition))
        fail_msg = 'error :: d_condition not a valid conditon - %s' % str(d_condition)
        return False, fail_msg, trace

    if 'd_boundary_limit' in request.args:
        d_boundary_limit = request.args.get('d_boundary_limit', '0')
    else:
        logger.error('no d_boundary_limit argument passed')
        fail_msg = 'error :: no d_boundary_limit argument passed'
        return False, fail_msg, trace

    try:
        test_d_boundary_limit = float(d_boundary_limit) + 1
        logger.info('test_d_boundary_limit tested OK with %s' % str(test_d_boundary_limit))
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: d_boundary_limit is not an int'
        return False, fail_msg, trace

    if 'd_boundary_times' in request.args:
        d_boundary_times = request.args.get('d_boundary_times', '1')
    else:
        logger.error('no d_boundary_times argument passed')
        fail_msg = 'error :: no d_boundary_times argument passed'
        return False, fail_msg, trace
    try:
        test_d_boundary_times = int(d_boundary_times) + 1
        logger.info('test_d_boundary_times tested OK with %s' % str(test_d_boundary_times))
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: d_boundary_times is not an int'
        return False, fail_msg, trace

    # @added 20170616 - Feature #2048: D1 ionosphere layer
    d1_condition = None
    if 'd1_condition' in request.args:
        d1_condition = request.args.get('d1_condition', 'none')
    else:
        logger.error('no d1_condition argument passed')
        fail_msg = 'error :: no d1_condition argument passed'
        return False, fail_msg, trace
    if str(d1_condition) == 'none':
        d1_condition = None
    else:
        if not str(d1_condition) in conditions:
            logger.error('d1_condition not a valid conditon - %s' % str(d1_condition))
            fail_msg = 'error :: d1_condition not a valid conditon - %s' % str(d1_condition)
            return False, fail_msg, trace

        if 'd1_boundary_limit' in request.args:
            d1_boundary_limit = request.args.get('d1_boundary_limit', '0')
        else:
            logger.error('no d1_boundary_limit argument passed')
            fail_msg = 'error :: no d1_boundary_limit argument passed'
            return False, fail_msg, trace
        try:
            test_d1_boundary_limit = float(d1_boundary_limit) + 1
            logger.info('test_d1_boundary_limit tested OK with %s' % str(test_d1_boundary_limit))
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: d1_boundary_limit is not an int'
            return False, fail_msg, trace
        if 'd1_boundary_times' in request.args:
            d1_boundary_times = request.args.get('d1_boundary_times', '1')
        else:
            logger.error('no d1_boundary_times argument passed')
            fail_msg = 'error :: no d1_boundary_times argument passed'
            return False, fail_msg, trace
        try:
            test_d1_boundary_times = int(d1_boundary_times) + 1
            logger.info('test_d1_boundary_times tested OK with %s' % str(test_d1_boundary_times))
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: d1_boundary_times is not an int'
            return False, fail_msg, trace

    if 'e_condition' in request.args:
        e_condition = request.args.get('e_condition', None)
    else:
        logger.error('no e_condition argument passed')
        fail_msg = 'error :: no e_condition argument passed'
        return False, fail_msg, trace

    if not str(e_condition) in value_conditions:
        logger.error('e_condition not a valid value conditon - %s' % str(e_condition))
        fail_msg = 'error :: e_condition not a valid value conditon - %s' % str(e_condition)
        return False, fail_msg, trace

    if 'e_boundary_limit' in request.args:
        e_boundary_limit = request.args.get('e_boundary_limit')
    else:
        logger.error('no e_boundary_limit argument passed')
        fail_msg = 'error :: no e_boundary_limit argument passed'
        return False, fail_msg, trace

    try:
        test_e_boundary_limit = float(e_boundary_limit) + 1
        logger.info('test_e_boundary_limit tested OK with %s' % str(test_e_boundary_limit))
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: e_boundary_limit is not an int'
        return False, fail_msg, trace

    if 'e_boundary_times' in request.args:
        e_boundary_times = request.args.get('e_boundary_times')
    else:
        logger.error('no e_boundary_times argument passed')
        fail_msg = 'error :: no e_boundary_times argument passed'
        return False, fail_msg, trace

    try:
        test_e_boundary_times = int(e_boundary_times) + 1
        logger.info('test_e_boundary_times tested OK with %s' % str(test_e_boundary_times))
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: e_boundary_times is not an int'
        return False, fail_msg, trace

    # NOT IMPLEMENTED YET
    # es_layer = False
    # f1_layer = False
    # f2_layer = False

    update_label = False
    if 'fp_layer_label' in request.args:
        label_arg = request.args.get('fp_layer_label')
        update_label = label_arg[:255]

    engine_needed = True
    engine = None
    ionosphere_layers_table = None
    layers_algorithms_table = None

    if engine_needed:
        logger.info('%s :: getting MySQL engine' % function_str)
        try:
            engine, fail_msg, trace = get_an_engine()
            logger.info(fail_msg)
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: could not get a MySQL engine'
            logger.error('%s' % fail_msg)
            raise

        if not engine:
            trace = 'none'
            fail_msg = 'error :: engine not obtained'
            logger.error(fail_msg)
            raise

        try:
            ionosphere_layers_table, fail_msg, trace = ionosphere_layers_table_meta(skyline_app, engine)
            logger.info(fail_msg)
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: ionosphere_backend :: failed to get ionosphere_layers_table meta for layers_id %s' % (str(layers_id))
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI

        try:
            layers_algorithms_table, fail_msg, trace = layers_algorithms_table_meta(skyline_app, engine)
            logger.info(fail_msg)
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: ionosphere_backend :: failed to get layers_algorithms_table meta for layers_id %s' % (str(layers_id))
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI

    if update_label:
        # Update layers_id label
        try:
            connection = engine.connect()
            connection.execute(
                ionosphere_layers_table.update(
                    ionosphere_layers_table.c.id == layers_id).
                values(label=update_label))
            connection.close()
            logger.info('updated label for %s - %s' % (str(layers_id), str(update_label)))
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            logger.error('error :: could not update label for layers_id %s ' % str(layers_id))
            fail_msg = 'error :: could not update label for layers_id %s ' % str(layers_id)
            if engine:
                engine_disposal(engine)
            raise

    layers_algorithms = []
    try:
        connection = engine.connect()
        stmt = select([layers_algorithms_table]).where(layers_algorithms_table.c.layer_id == layers_id)
        result = connection.execute(stmt)
        connection.close()
        for row in result:
            la_id = row['id']
            la_layer = str(row['layer'])
            layers_algorithms.append([la_id, la_layer])
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not get layers_algorithms for layer id %s from layers_algorithms DB table' % str(layers_id)
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    # Remake D and E layers as defined by arguments
    for algorithm_id, layer_name in layers_algorithms:
        # D layer
        if layer_name == 'D':
            try:
                connection = engine.connect()
                connection.execute(
                    layers_algorithms_table.update(
                        layers_algorithms_table.c.id == algorithm_id).values(
                            condition=d_condition, layer_boundary=d_boundary_limit,
                            times_in_row=d_boundary_times))
                connection.close()
                logger.info('updated D layer for %s - %s, %s, %s' % (
                    str(layers_id), str(d_condition), str(d_boundary_limit),
                    str(d_boundary_times)))
            except:
                trace = traceback.format_exc()
                logger.error('%s' % trace)
                fail_msg = 'error :: failed to update D layer record into the layers_algorithms table for %s' % str(layers_id)
                logger.error('%s' % fail_msg)
                if engine:
                    engine_disposal(engine)
                raise

        # @added 20170616 - Feature #2048: D1 ionosphere layer
        if d1_condition and layer_name == 'D1':
            try:
                connection = engine.connect()
                connection.execute(
                    layers_algorithms_table.update(
                        layers_algorithms_table.c.id == algorithm_id).values(
                            condition=d1_condition, layer_boundary=d1_boundary_limit,
                            times_in_row=d1_boundary_times))
                connection.close()
                logger.info('updated D1 layer for %s - %s, %s, %s' % (
                    str(layers_id), str(d1_condition), str(d1_boundary_limit),
                    str(d1_boundary_times)))
            except:
                trace = traceback.format_exc()
                logger.error('%s' % trace)
                fail_msg = 'error :: failed to update D1 layer record into the layers_algorithms table for %s' % str(layers_id)
                logger.error('%s' % fail_msg)
                if engine:
                    engine_disposal(engine)
                raise

        # E layer
        if layer_name == 'E':
            try:
                connection = engine.connect()
                connection.execute(
                    layers_algorithms_table.update(
                        layers_algorithms_table.c.id == algorithm_id).values(
                            condition=e_condition, layer_boundary=e_boundary_limit,
                            times_in_row=e_boundary_times))
                connection.close()
                logger.info('updated E layer for %s - %s, %s, %s' % (
                    str(layers_id), str(e_condition), str(e_boundary_limit),
                    str(e_boundary_times)))
            except:
                trace = traceback.format_exc()
                logger.error('%s' % trace)
                fail_msg = 'error :: failed to update E layer record into the layers_algorithms table for %s' % str(layers_id)
                logger.error('%s' % fail_msg)
                if engine:
                    engine_disposal(engine)
                raise

    if engine:
        engine_disposal(engine)

    return True, fail_msg, trace


# @added 20170402 - Feature #2000: Ionosphere - validated
# @modified 20181013 - Feature #2430: Ionosphere validate learnt features profiles page
# Extended the validate_fp function to validate a single fp id or all the unvalidated,
# enabled features profiles for a metric_id
# def validate_fp(fp_id):
# @modified 20190919 - Feature #3230: users DB table
#                      Ideas #2476: Label and relate anomalies
#                      Feature #2516: Add label to features profile
# Added user
# def validate_fp(update_id, id_column_name):
def validate_fp(update_id, id_column_name, user_id):
    """
    Validate a single features profile or validate all enabled, unvalidated
    features profiles for a metric_id.

    :param update_id: the features profile id or metric_id to validate
    :type update_id: int
    :param id_column_name: the column name to select where on, e.g. id or metric_id
    :param user_id: the user id of the user that is validating
    :type update_id: int
    :type id_column_name: str
    :type user_id: int
    :return: tuple
    :rtype:  (boolean, str, str)

    """
    logger = logging.getLogger(skyline_app_logger)

    function_str = 'ionoshere_backend.py :: validate_fp'

    # @added 20181013 - Feature #2430: Ionosphere validate learnt features profiles page
    fp_id = update_id

    # @modified 20181013 - Feature #2430: Ionosphere validate learnt features profiles page
    if id_column_name == 'id':
        logger.info('%s validating fp_id %s' % (function_str, str(fp_id)))
    if id_column_name == 'metric_id':
        logger.info('%s validating all enabled and unvalidated features profiles for metric_id - %s' % (function_str, str(update_id)))

    trace = 'none'
    fail_msg = 'none'

    logger.info('%s :: getting MySQL engine' % function_str)
    try:
        engine, fail_msg, trace = get_an_engine()
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not get a MySQL engine'
        logger.error('%s' % fail_msg)
        raise

    if not engine:
        trace = 'none'
        fail_msg = 'error :: engine not obtained'
        logger.error(fail_msg)
        raise

    try:
        ionosphere_table, fail_msg, trace = ionosphere_table_meta(skyline_app, engine)
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        # @modified 20181013 - Feature #2430: Ionosphere validate learnt features profiles page
        # fail_msg = 'error :: ionosphere_backend :: failed to get ionosphere_table meta for fp_id %s' % (str(fp_id))
        if id_column_name == 'id':
            fail_msg = 'error :: ionosphere_backend :: %s :: failed to get ionosphere_table meta for fp_id %s' % (function_str, str(fp_id))
        if id_column_name == 'metric_id':
            fail_msg = 'error :: ionosphere_backend ::  %s :: failed to get ionosphere_table meta for metric_id - %s' % (function_str, str(update_id))
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    try:
        connection = engine.connect()
        # @modified 20181013 - Feature #2430: Ionosphere validate learnt features profiles page
        # fail_msg = 'error :: ionosphere_backend :: failed to get ionosphere_table meta for fp_id %s' % (str(fp_id))
        # @modified 20190919 - Feature #3230: users DB table
        #                      Ideas #2476: Label and relate anomalies
        #                      Feature #2516: Add label to features profile
        # Added user_id
        if id_column_name == 'id':
            connection.execute(
                ionosphere_table.update(
                    ionosphere_table.c.id == int(fp_id)).
                values(validated=1, user_id=user_id))
        if id_column_name == 'metric_id':
            stmt = ionosphere_table.update().\
                values(validated=1, user_id=user_id).\
                where(ionosphere_table.c.metric_id == int(update_id)).\
                where(ionosphere_table.c.validated == 0).\
                where(ionosphere_table.c.enabled == 1)
            connection.execute(stmt)
        connection.close()
        if id_column_name == 'id':
            logger.info('updated validated for %s' % (str(fp_id)))
        if id_column_name == 'metric_id':
            logger.info('updated validated for all enabled, unvalidated features profiles for metric_id - %s' % (str(update_id)))
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        if id_column_name == 'id':
            logger.error('error :: could not update validated for fp_id %s ' % str(fp_id))
            fail_msg = 'error :: could not update validated label for fp_id %s ' % str(fp_id)
        if id_column_name == 'metric_id':
            logger.error('error :: could not update validated for all enabled, unvalidated features profiles for metric_id - %s ' % str(update_id))
            fail_msg = 'error :: could not update validated labels for all enabled, unvalidated features profiles for metric_id - %s ' % str(update_id)
        if engine:
            engine_disposal(engine)
        raise

    # @added 20170806 - Bug #2130: MySQL - Aborted_clients
    # Added missing disposal
    if engine:
        engine_disposal(engine)

    if id_column_name == 'id':
        return True, fail_msg, trace
    if id_column_name == 'metric_id':
        return True, fail_msg, trace


# @added 20170617 - Feature #2054: ionosphere.save.training_data
def save_training_data_dir(timestamp, base_name, label, hdate):
    """
    Save training_data and return details or just return details if exists

    :param timestamp: the Ionosphere training_data metric timestamp
    :param base_name: metric base_name
    :param label: the saved training_data label
    :param hdate: human date for the saved training_data
    :type timestamp: str
    :type base_name: str
    :type label: str
    :type hdate: str
    :return: saved_successful, details, fail_msg, trace
    :rtype: boolean, list, str, str

    """
    logger = logging.getLogger(skyline_app_logger)

    function_str = 'ionoshere_backend.py :: save_training_data'

    trace = 'none'
    fail_msg = 'none'
    training_data_saved = True

    logger.info(
        '%s :: Saving training_data for %s.%s' % (
            function_str, (timestamp), str(base_name)))
    metric_timeseries_dir = base_name.replace('.', '/')
    metric_training_data_dir = '%s/%s/%s' % (
        settings.IONOSPHERE_DATA_FOLDER, str(timestamp),
        metric_timeseries_dir)
    saved_metric_training_data_dir = '%s_saved/%s/%s' % (
        settings.IONOSPHERE_DATA_FOLDER, str(timestamp),
        metric_timeseries_dir)
    details_file = '%s/%s.%s.saved_training_data_label.txt' % (saved_metric_training_data_dir, str(timestamp), base_name)

    if path.isfile(details_file):
        logger.info(
            '%s :: Saved training_data for %s.%s already exists' % (
                function_str, (timestamp), str(base_name)))
        saved_training_data_details = []
        try:
            with open(details_file) as f:
                for line in f:
                    saved_training_data_details.append(line)
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = '%s :: error :: failed to read details file %s' % (function_str, details_file)
            logger.error('%s' % fail_msg)
            raise
        return True, saved_training_data_details, fail_msg, trace

    if not path.exists(saved_metric_training_data_dir):
        try:
            mkdir_p(saved_metric_training_data_dir)
            logger.info(
                '%s :: created %s' % (function_str, saved_metric_training_data_dir))
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = '%s :: error :: failed to create %s' % (function_str, saved_metric_training_data_dir)
            logger.error('%s' % fail_msg)
            training_data_saved = False

    if training_data_saved:

        # @modified 20210419 - Feature #4014: Ionosphere - inference
        # Include motif data in saved_training_data, use os.walk instead of
        # glob
        use_old_glob_method = False
        if use_old_glob_method:
            save_data_files = []
            try:
                glob_path = '%s/*.*' % metric_training_data_dir
                save_data_files = glob.glob(glob_path)
            except:
                trace = traceback.format_exc()
                logger.error(trace)
                logger.error(
                    '%s :: error :: glob %s - training data not copied to %s' % (
                        function_str, metric_training_data_dir, saved_metric_training_data_dir))
                fail_msg = 'error :: glob failed to copy'
                logger.error('%s' % fail_msg)
                training_data_saved = False

        # @added 20210419 - Feature #4014: Ionosphere - inference
        # Include motif data in saved_training_data
        save_data_files = []
        for dir_path, folders, files in walk(metric_training_data_dir):
            try:
                if files:
                    for i in files:
                        path_and_file = '%s/%s' % (dir_path, i)
                        save_data_files.append(path_and_file)
            except:
                trace = traceback.format_exc()
                logger.error(trace)
                logger.error('error :: %s :: training data not found from %s' % (
                    str(function_str), str(metric_training_data_dir)))
                fail_msg = 'error :: walk failed to find training_data files'
                logger.error('%s' % fail_msg)
                training_data_saved = False

    if not training_data_saved:
        raise

    for i_file in save_data_files:
        try:
            # @added 20210419 - Feature #4014: Ionosphere - inference
            # Include motif data in saved_training_data
            i_dir = path.dirname(i_file)
            saved_data_dir = '%s_saved' % settings.IONOSPHERE_DATA_FOLDER
            use_saved_metric_training_data_dir = i_dir.replace(settings.IONOSPHERE_DATA_FOLDER, saved_data_dir)
            if not path.exists(use_saved_metric_training_data_dir):
                mkdir_p(use_saved_metric_training_data_dir)

            # @modified 20210419 - Feature #4014: Ionosphere - inference
            # shutil.copy(i_file, saved_metric_training_data_dir)
            shutil.copy(i_file, use_saved_metric_training_data_dir)
            logger.info(
                '%s :: training data copied to %s/%s' % (
                    # function_str, saved_metric_training_data_dir, i_file))
                    function_str, use_saved_metric_training_data_dir, i_file))
        except shutil.Error as e:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            logger.error(
                '%s :: error :: shutil error - %s - not copied to %s' % (
                    function_str, i_file, saved_metric_training_data_dir))
            logger.error('%s :: error :: %s' % (function_str, e))
            training_data_saved = False
            fail_msg = 'error :: shutil error'
        # Any error saying that the directory doesn't exist
        except OSError as e:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            logger.error(
                '%s :: error :: OSError error %s - training data not copied to %s' % (
                    function_str, metric_training_data_dir, saved_metric_training_data_dir))
            logger.error(
                '%s :: error :: %s' % (function_str, e))
            training_data_saved = False
            fail_msg = 'error :: shutil error'

    if not training_data_saved:
        raise

    # Create a label file
    try:
        # @modified 20210727 - Feature #4206: webapp - saved_training_data page
        # Make it a list of dicts so literal_eval can be applied to it. It was
        # only ever created to use as plaintext in HTML <code>, this change
        # makes no difference to its original usage.
        # saved_training_data_details = '[[label: \'%s\'], [saved_date: \'%s\']]' % (str(label), str(hdate))
        saved_training_data_details = "[{'label': '%s'}, {'saved_date': '%s'}]" % (str(label), str(hdate))
        write_data_to_file(skyline_app, details_file, 'w', saved_training_data_details)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = '%s :: error :: failed to write label file' % (function_str)
        logger.error('%s' % fail_msg)

    return True, False, fail_msg, trace


# added 20170908 - Feature #2056: ionosphere - disabled_features_profiles
def features_profile_family_tree(fp_id):
    """
    Returns the all features profile ids of the related progeny features
    profiles, the whole family tree.

    :param fp_id: the features profile id
    :return: array
    :rtype: array

    """
    logger = logging.getLogger(skyline_app_logger)

    function_str = 'ionoshere_backend.py :: features_profile_progeny'

    logger.info('%s getting the features profile ids of the progeny of fp_id %s' % (function_str, str(fp_id)))

    trace = 'none'
    fail_msg = 'none'
    current_fp_id = int(fp_id)
    family_tree_fp_ids = [current_fp_id]

    logger.info('%s :: getting MySQL engine' % function_str)
    try:
        engine, fail_msg, trace = get_an_engine()
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not get a MySQL engine'
        logger.error('%s' % fail_msg)
        raise

    if not engine:
        trace = 'none'
        fail_msg = 'error :: engine not obtained'
        logger.error(fail_msg)
        raise

    try:
        ionosphere_table, fail_msg, trace = ionosphere_table_meta(skyline_app, engine)
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: ionosphere_backend :: failed to get ionosphere_table meta for fp_id %s' % (str(fp_id))
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    row = current_fp_id
    while row:
        try:
            connection = engine.connect()
            stmt = select([ionosphere_table]).where(ionosphere_table.c.parent_id == current_fp_id)
            result = connection.execute(stmt)
            connection.close()
            row = None
            for row in result:
                progeny_id = row['id']
                family_tree_fp_ids.append(int(progeny_id))
                current_fp_id = progeny_id
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: could not get id for %s' % str(current_fp_id)
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI

    if engine:
        engine_disposal(engine)

    return family_tree_fp_ids, fail_msg, trace


# added 20170908 - Feature #2056: ionosphere - disabled_features_profiles
def disable_features_profile_family_tree(fp_ids):
    """
    Disable a features profile and all related progeny features profiles

    :param fp_ids: a list of the the features profile ids to disable
    :return: array
    :rtype: array

    """
    logger = logging.getLogger(skyline_app_logger)

    function_str = 'ionoshere_backend.py :: disable_features_profile_and_progeny'

    logger.info('%s disabling fp ids - %s' % (function_str, str(fp_ids)))

    trace = 'none'
    fail_msg = 'none'

    logger.info('%s :: getting MySQL engine' % function_str)
    try:
        engine, fail_msg, trace = get_an_engine()
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not get a MySQL engine'
        logger.error('%s' % fail_msg)
        raise

    if not engine:
        trace = 'none'
        fail_msg = 'error :: engine not obtained'
        logger.error(fail_msg)
        raise

    try:
        ionosphere_table, fail_msg, trace = ionosphere_table_meta(skyline_app, engine)
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: ionosphere_backend :: failed to get ionosphere_table meta for disable_features_profile_family_tree'
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    # @added 20200516 - Bug #3546: Change ionosphere_enabled if all features profiles are disabled
    # Disable any related layers as well
    try:
        ionosphere_layers_table, fail_msg, trace = ionosphere_layers_table_meta(skyline_app, engine)
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: ionosphere_backend :: failed to get ionosphere_layers_table meta for disable_features_profile_family_tree'
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    # @added 20190503 - Branch #2646: slack
    message_on_features_profile_disabled = False
    if slack_thread_updates and SLACK_ENABLED:
        try:
            message_on_features_profile_disabled = settings.SLACK_OPTS['message_on_features_profile_disabled']
        except:
            message_on_features_profile_disabled = False
    if message_on_features_profile_disabled:
        try:
            metrics_table, fail_msg, trace = metrics_table_meta(skyline_app, engine)
            logger.info(fail_msg)
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: ionosphere_backend :: failed to get metrics_table meta for disable_features_profile_family_tree'
            logger.error('%s' % fail_msg)
        if channel:
            try:
                anomalies_table, fail_msg, trace = anomalies_table_meta(skyline_app, engine)
                logger.info(fail_msg)
            except:
                trace = traceback.format_exc()
                logger.error('%s' % trace)
                fail_msg = 'error :: ionosphere_backend :: failed to get anomalies_table meta for disable_features_profile_family_tree'
                logger.error('%s' % fail_msg)

    for fp_id in fp_ids:

        # @added 20200516 - Bug #3546: Change ionosphere_enabled if all features profiles are disabled
        # Disable any related layers as well
        layers_disabled = 0
        layer_ids = []
        try:
            connection = engine.connect()
            stmt = select([ionosphere_layers_table]).where(ionosphere_layers_table.c.fp_id == int(fp_id))
            result = connection.execute(stmt)
            for row in result:
                layer_id = int(row['id'])
                layer_ids.append(layer_id)
                logger.info('disable_features_profile_family_tree :: found layer id %s for fp id %s to disabled' % (
                    str(layer_id), str(fp_id)))
            connection.close()
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            logger.error('error :: could not determine layers to disable for fp_id %s ' % str(fp_id))
            fail_msg = 'error :: could not determine layers to disable for fp_id %s ' % str(fp_id)
            if engine:
                engine_disposal(engine)
            raise

        try:
            connection = engine.connect()
            connection.execute(
                ionosphere_table.update(
                    ionosphere_table.c.id == int(fp_id)).
                values(enabled=0))
            connection.close()
            logger.info('updated enabled for %s to 0' % (str(fp_id)))
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            logger.error('error :: could not update enabled for fp_id %s ' % str(fp_id))
            fail_msg = 'error :: could not update enabled for fp_id %s ' % str(fp_id)
            if engine:
                engine_disposal(engine)
            raise

        # @added 20200516 - Bug #3546: Change ionosphere_enabled if all features profiles are disabled
        # Disable any related layers as well
        if layer_ids:
            for layer_id in layer_ids:
                try:
                    connection = engine.connect()
                    connection.execute(
                        ionosphere_layers_table.update(
                            ionosphere_layers_table.c.id == int(layer_id)).
                        values(enabled=0))
                    connection.close()
                    logger.info('updated enabled for layer id %s to 0' % (str(layer_id)))
                    layers_disabled += 1
                except:
                    trace = traceback.format_exc()
                    logger.error(trace)
                    logger.error('error :: could not update enabled for layer id %s ' % str(layer_id))
                    fail_msg = 'error :: could not update enabled for layer id %s ' % str(layer_id)
                    if engine:
                        engine_disposal(engine)
                    raise

        # @added 20190503 - Branch #2646: slack
        if message_on_features_profile_disabled:
            # TODO
            # To generate a slack message with a link to the features profile,
            # the metric name and anomaly_timestamp has to be determined
            fp_metric_id = 0
            fp_anomaly_timestamp = 0
            try:
                connection = engine.connect()
                stmt = select([ionosphere_table]).where(ionosphere_table.c.id == int(fp_id))
                result = connection.execute(stmt)
                for row in result:
                    fp_metric_id = int(row['metric_id'])
                    fp_anomaly_timestamp = int(row['anomaly_timestamp'])
                    logger.info('disable_features_profile_family_tree :: found fp metric_id %s and anomaly_timestamp %s from the DB to message disabled to slack' % (
                        str(fp_metric_id), str(fp_anomaly_timestamp)))
                connection.close()
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: disable_features_profile_family_tree :: could not determine metric_id and anomaly_timestamp from ionosphere for fp_id %s to message disabled to slack' % str(fp_id))
                continue
            if fp_metric_id and fp_anomaly_timestamp:
                base_name = None
                try:
                    connection = engine.connect()
                    stmt = select([metrics_table]).where(metrics_table.c.id == fp_metric_id)
                    result = connection.execute(stmt)
                    for row in result:
                        base_name = row['metric']
                    connection.close()
                except:
                    trace = traceback.format_exc()
                    logger.error(trace)
                    fail_msg = 'error :: disable_features_profile_family_tree :: could not determine metric from metrics table to message disabled fp id %s to slack' % str(fp_id)
                    continue
            if base_name:
                ionosphere_link = '%s/ionosphere?fp_view=true&fp_id=%s&metric=%s' % (
                    settings.SKYLINE_URL, str(fp_id), base_name)
                # @added 20200516 - Bug #3546: Change ionosphere_enabled if all features profiles are disabled
                # Disable any related layers as well
                # message = '*DISABLED* - features profile id %s was disabled for %s via %s - %s' % (
                #     str(fp_id), str(base_name), skyline_app, ionosphere_link)
                message = '*DISABLED* - features profile id %s and %s related layers were disabled for %s via %s - %s' % (
                    str(fp_id), str(layers_disabled), str(base_name),
                    skyline_app, ionosphere_link)

                if throw_exception_on_default_channel:
                    fail_msg = 'error :: disable_features_profile_family_tree :: the default_channel or default_channel_id from settings.SLACK_OPTS is set to the default, please replace these with your channel details or set SLACK_ENABLED or SLACK_OPTS[\'thread_updates\'] to False and restart webapp'
                    logger.error('%s' % fail_msg)
                    raise  # to webapp to return in the UI
                if channel_id:
                    slack_response = {'ok': False}
                    try:
                        slack_response = slack_post_message(skyline_app, channel_id, None, message)
                    except:
                        trace = traceback.format_exc()
                        logger.error(trace)
                        fail_msg = 'error :: disable_features_profile_family_tree :: failed to slack_post_message - %s' % message
                        logger.error('%s' % fail_msg)
                    if not slack_response['ok']:
                        fail_msg = 'error :: disable_features_profile_family_tree :: failed to slack_post_message, slack dict output follows'
                        logger.error('%s' % fail_msg)
                        logger.error('%s' % str(slack_response))
                    else:
                        logger.info('disable_features_profile_family_tree :: posted slack update to %s, fp id %s disabled' % (
                            channel_id, str(fp_id)))
                    # Post a X reaction if there is a slack_thread_ts that is not
                    # too old
                    anomaly_id = None
                    slack_thread_ts = 0
                    try:
                        connection = engine.connect()
                        stmt = select([anomalies_table]).\
                            where(anomalies_table.c.metric_id == fp_metric_id).\
                            where(anomalies_table.c.anomaly_timestamp == int(fp_anomaly_timestamp))
                        result = connection.execute(stmt)
                        for row in result:
                            anomaly_id = row['id']
                            slack_thread_ts = row['slack_thread_ts']
                            break
                        connection.close()
                        logger.info('disable_features_profile_family_tree :: determined anomaly id %s for metric id %s at anomaly_timestamp %s with slack_thread_ts %s' % (
                            str(anomaly_id), str(fp_metric_id),
                            str(fp_anomaly_timestamp), str(slack_thread_ts)))
                    except:
                        trace = traceback.format_exc()
                        logger.error(trace)
                        fail_msg = 'error :: disable_features_profile_family_tree :: could not determine id of anomaly from DB for metric id %s at anomaly_timestamp %s' % (
                            str(fp_metric_id), str(fp_anomaly_timestamp))
                        logger.error('%s' % fail_msg)
                    if float(slack_thread_ts) == 0:
                        slack_thread_ts = None
                        logger.info('disable_features_profile_family_tree :: slack_thread_ts is %s, so uknown, not posting reaction' % (
                            str(slack_thread_ts)))
                        continue
                    if slack_thread_ts:
                        slack_thread_ts_int = int(float(slack_thread_ts))
                        time_now = int(time.time())
                        oldest_ts = time_now - 604800
                        if slack_thread_ts_int < oldest_ts:
                            logger.info('disable_features_profile_family_tree :: slack_thread_ts %s is older than a week not posting reaction' % (
                                str(slack_thread_ts)))
                            continue
                        slack_response = {'ok': False}
                        try:
                            slack_response = slack_post_message(skyline_app, channel_id, str(slack_thread_ts), message)
                        except:
                            trace = traceback.format_exc()
                            logger.error(trace)
                            fail_msg = 'error :: disable_features_profile_family_tree :: failed to slack_post_message - %s' % message
                            logger.error('%s' % fail_msg)
                        if not slack_response['ok']:
                            fail_msg = 'error :: disable_features_profile_family_tree :: failed to slack_post_message, slack dict output follows'
                            logger.error('%s' % fail_msg)
                            logger.error('%s' % str(slack_response))
                        else:
                            logger.info('disable_features_profile_family_tree :: posted slack update to %s, fp id %s disabled' % (
                                channel_id, str(fp_id)))
                        try:
                            reaction_emoji = settings.SLACK_OPTS['message_on_features_profile_disabled_reaction_emoji']
                        except:
                            reaction_emoji = 'x'
                        slack_response = {'ok': False}
                        try:
                            slack_response = slack_post_reaction(skyline_app, channel_id, str(slack_thread_ts), reaction_emoji)
                        except:
                            trace = traceback.format_exc()
                            logger.error(trace)
                            fail_msg = 'error :: webapp_update_slack_thread :: failed to slack_post_reaction to channel id %s and slack_thread_ts %s with reaction %s' % (
                                str(channel_id), str(slack_thread_ts), str(reaction_emoji))
                            logger.error('%s' % fail_msg)

    if engine:
        engine_disposal(engine)

    return True, fail_msg, trace


# @added 20170915 - Feature #1996: Ionosphere - matches page
def get_fp_matches(metric, metric_like, get_fp_id, get_layer_id, from_timestamp, until_timestamp, limit, sort):
    """
    Get all the matches.

    :param metric: all or the metric name
    :param metric_like: False or the metric MySQL like string e.g statsd.%
    :param get_fp_id: None or int
    :param get_layer_id: None or int
    :param from_timestamp: timestamp or None
    :param until_timestamp: timestamp or None
    :param limit: None or number to limit to
    :param sort: DESC or ASC
    :return: list
    :rtype: list

    """
    logger = logging.getLogger(skyline_app_logger)

    function_str = 'ionoshere_backend.py :: get_fp_matches'

    logger.info('%s getting matches' % (function_str))
    logger.info('arguments :: %s, %s, %s, %s, %s, %s, %s, %s' % (
        str(metric), str(metric_like), str(get_fp_id), str(get_layer_id),
        str(from_timestamp), str(until_timestamp), str(limit),
        str(sort)))
    trace = 'none'
    fail_msg = 'none'

    # @added 20200113 - Feature #3390: luminosity related anomalies
    #                   Branch #2270: luminosity
    get_related_matches = False
    if metric == 'get_related_matches':
        get_related_matches = True
        metric = 'all'
        related_from_timestamp = from_timestamp
        related_until_timestamp = until_timestamp

    if settings.MEMCACHE_ENABLED:
        memcache_client = pymemcache_Client((settings.MEMCACHED_SERVER_IP, settings.MEMCACHED_SERVER_PORT), connect_timeout=0.1, timeout=0.2)
    else:
        memcache_client = None

    logger.info('%s :: getting MySQL engine' % function_str)
    try:
        engine, fail_msg, trace = get_an_engine()
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not get a MySQL engine'
        logger.error('%s' % fail_msg)
        return False, fail_msg, trace

    if not engine:
        trace = 'none'
        fail_msg = 'error :: engine not obtained'
        logger.error(fail_msg)
        return False, fail_msg, trace

    query_string = 'SELECT * FROM ionosphere_matched'
    needs_and = False

    if metric and metric != 'all':
        metric_id_stmt = 'SELECT id FROM metrics WHERE metric=\'%s\'' % str(metric)
        metric_id = None
        logger.info('metric set to %s' % str(metric))
        try:
            connection = engine.connect()
            result = connection.execute(metric_id_stmt)
            connection.close()
            for row in result:
                if not metric_id:
                    metric_id = int(row[0])
            logger.info('metric_id set to %s' % str(metric_id))
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            logger.error('error :: could not determine id from metrics table')
            # Disposal and return False, fail_msg, trace for Bug #2130: MySQL - Aborted_clients
            if engine:
                engine_disposal(engine)
            return False, fail_msg, trace

        fp_ids_stmt = 'SELECT id FROM ionosphere WHERE metric_id=%s' % str(metric_id)
        fp_ids = ''
        try:
            connection = engine.connect()
            results = connection.execute(fp_ids_stmt)
            connection.close()
            for row in results:
                fp_id = str(row[0])
                if fp_ids == '':
                    fp_ids = '%s' % (fp_id)
                else:
                    new_fp_ids = '%s, %s' % (fp_ids, fp_id)
                    fp_ids = new_fp_ids
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            logger.error('error :: could not determine id from metrics table')
            # Disposal and return False, fail_msg, trace for Bug #2130: MySQL - Aborted_clients
            if engine:
                engine_disposal(engine)
            return False, fail_msg, trace
        logger.info('fp_ids set to %s' % str(fp_ids))
        query_string = 'SELECT * FROM ionosphere_matched WHERE fp_id in (%s)' % str(fp_ids)
        needs_and = True

#    if 'metric_like' in request.args:
    if metric_like:
        if metric_like and metric_like != 'all':
            # SQLAlchemy requires the MySQL wildcard % to be %% to prevent
            # interpreting the % as a printf-like format character
            python_escaped_metric_like = metric_like.replace('%', '%%')
            # nosec to exclude from bandit tests
            metrics_like_query = 'SELECT id FROM metrics WHERE metric LIKE \'%s\'' % (str(python_escaped_metric_like))  # nosec
            logger.info('executing metrics_like_query - %s' % metrics_like_query)
            metric_ids = ''
            try:
                connection = engine.connect()
                results = connection.execute(metrics_like_query)
                connection.close()
                for row in results:
                    metric_id = str(row[0])
                    if metric_ids == '':
                        metric_ids = '%s' % (metric_id)
                    else:
                        new_metric_ids = '%s, %s' % (metric_ids, metric_id)
                        metric_ids = new_metric_ids
            except:
                trace = traceback.format_exc()
                logger.error(trace)
                logger.error('error :: could not determine ids from metrics table')
                # Disposal and return False, fail_msg, trace for Bug #2130: MySQL - Aborted_clients
                if engine:
                    engine_disposal(engine)
                return False, fail_msg, trace

            fp_ids_stmt = 'SELECT id FROM ionosphere WHERE metric_id IN (%s)' % str(metric_ids)
            fp_ids = ''
            try:
                connection = engine.connect()
                results = connection.execute(fp_ids_stmt)
                connection.close()
                for row in results:
                    fp_id = str(row[0])
                    if fp_ids == '':
                        fp_ids = '%s' % (fp_id)
                    else:
                        new_fp_ids = '%s, %s' % (fp_ids, fp_id)
                        fp_ids = new_fp_ids
            except:
                trace = traceback.format_exc()
                logger.error(trace)
                logger.error('error :: could not determine id from metrics table')
                # Disposal and return False, fail_msg, trace for Bug #2130: MySQL - Aborted_clients
                if engine:
                    engine_disposal(engine)
                return False, fail_msg, trace
            query_string = 'SELECT * FROM ionosphere_matched WHERE fp_id in (%s)' % str(fp_ids)
            needs_and = True

    # @added 20170917 - Feature #1996: Ionosphere - matches page
    # Added by fp_id or layer_id as well
    get_features_profiles_matched = True
    get_layers_matched = True
    if get_fp_id or get_layer_id:
        if get_fp_id:
            logger.info('get_fp_id set to %s' % str(get_fp_id))
            if get_fp_id != '0':
                get_layers_matched = False
                query_string = 'SELECT * FROM ionosphere_matched WHERE fp_id=%s' % str(get_fp_id)
        if get_layer_id:
            logger.info('get_layer_id set to %s' % str(get_layer_id))
            if get_layer_id != '0':
                get_features_profiles_matched = False
                query_string = 'SELECT * FROM ionosphere_layers_matched WHERE layer_id=%s' % str(get_layer_id)
                fp_id_query_string = 'SELECT fp_id FROM ionosphere_layers WHERE id=%s' % str(get_layer_id)
                fp_id = None
                try:
                    connection = engine.connect()
                    result = connection.execute(fp_id_query_string)
                    connection.close()
                    for row in result:
                        if not fp_id:
                            fp_id = int(row[0])
                except:
                    trace = traceback.format_exc()
                    logger.error(trace)
                    logger.error('error :: could not determine id from metrics table')
                    # Disposal and return False, fail_msg, trace for Bug #2130: MySQL - Aborted_clients
                    if engine:
                        engine_disposal(engine)
                    return False, fail_msg, trace
        needs_and = True
        # @added 20190524 - Branch #3002: docker
        if str(get_fp_id) == '0':
            needs_and = False

    if 'from_timestamp' in request.args:
        from_timestamp = request.args.get('from_timestamp', None)
        if from_timestamp and from_timestamp != 'all':
            if ":" in from_timestamp:
                import datetime
                new_from_timestamp = time.mktime(datetime.datetime.strptime(from_timestamp, '%Y%m%d %H:%M').timetuple())
                from_timestamp = str(int(new_from_timestamp))

            # @added 20190530 - Branch #3002: docker
            # Fix search so that if timestamps are passed and there is a list of
            # fp ids the SQL does not break
            try:
                if len(fp_ids) > 0:
                    needs_and = True
            except:
                logger.info('fp_ids length unknown, OK')

            if needs_and:
                new_query_string = '%s AND metric_timestamp >= %s' % (query_string, from_timestamp)
                query_string = new_query_string
                needs_and = True
            else:
                new_query_string = '%s WHERE metric_timestamp >= %s' % (query_string, from_timestamp)
                query_string = new_query_string
                needs_and = True

    if 'until_timestamp' in request.args:
        until_timestamp = request.args.get('until_timestamp', None)
        if until_timestamp and until_timestamp != 'all':
            if ":" in until_timestamp:
                import datetime
                new_until_timestamp = time.mktime(datetime.datetime.strptime(until_timestamp, '%Y%m%d %H:%M').timetuple())
                until_timestamp = str(int(new_until_timestamp))

            if needs_and:
                new_query_string = '%s AND metric_timestamp <= %s' % (query_string, until_timestamp)
                query_string = new_query_string
                needs_and = True
            else:
                new_query_string = '%s WHERE metric_timestamp <= %s' % (query_string, until_timestamp)
                query_string = new_query_string
                needs_and = True

    if 'validated_equals' in request.args:
        filter_matches = False
        validated_equals = request.args.get('validated_equals', None)
        if validated_equals == 'any':
            filter_matches = False
        if validated_equals == 'true':
            filter_match_validation = 1
            filter_matches = True
        if validated_equals == 'false':
            filter_match_validation = 0
            filter_matches = True
        if validated_equals == 'invalid':
            filter_match_validation = 2
            filter_matches = True
        if filter_matches:
            if needs_and:
                new_query_string = '%s AND validated = %s' % (query_string, str(filter_match_validation))
                query_string = new_query_string
                needs_and = True
            else:
                new_query_string = '%s WHERE validated = %s' % (query_string, str(filter_match_validation))
                query_string = new_query_string
                needs_and = True

    ordered_by = None
    if 'order' in request.args:
        order = request.args.get('order', 'DESC')
        if str(order) == 'DESC':
            ordered_by = 'DESC'
        if str(order) == 'ASC':
            ordered_by = 'ASC'

    if ordered_by:
        new_query_string = '%s ORDER BY id %s' % (query_string, ordered_by)
        query_string = new_query_string

    if 'limit' in request.args:
        limit = request.args.get('limit', '30')
        try:
            test_limit = int(limit) + 0
            if int(limit) != 0:
                new_query_string = '%s LIMIT %s' % (query_string, str(limit))
                query_string = new_query_string
                logger.info('test_limit tested OK with %s' % str(test_limit))
        except:
            logger.error('error :: limit is not an integer - %s' % str(limit))

    # @added 20200113 - Feature #3390: luminosity related anomalies
    #                   Branch #2270: luminosity
    if get_related_matches:
        try:
            query_string = 'SELECT * FROM ionosphere_matched WHERE metric_timestamp > %s AND metric_timestamp <= %s' % (
                str(related_from_timestamp), str(related_until_timestamp))
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            logger.error('error :: could not generate query_string for get_related_matches')
            if engine:
                engine_disposal(engine)
            return False, fail_msg, trace

    # Get ionosphere_summary memcache object from which metric names will be
    # determined
    memcache_result = None
    ionosphere_summary_list = None
    if settings.MEMCACHE_ENABLED:
        try:

            # @modified 20191029 - Task #3304: py3 - handle pymemcache bytes not str
            # memcache_result = memcache_client.get('ionosphere_summary_list')
            python_version = int(version_info[0])
            if python_version == 2:
                memcache_result = memcache_client.get('ionosphere_summary_list')
            else:
                memcache_result = memcache_client.get('ionosphere_summary_list').decode('utf-8')
        except:
            # @modified 20200507 - stop reporting this as an error
            # it can be expected to happen from time to time
            # logger.error('error :: failed to get ionosphere_summary_list from memcache')
            logger.info('failed to get ionosphere_summary_list from memcache, will query DB')
        try:
            memcache_client.close()
        # Added nosec to exclude from bandit tests
        except:  # nosec
            pass

    if memcache_result:
        try:
            logger.info('using memcache ionosphere_summary_list key data')
            ionosphere_summary_list = literal_eval(memcache_result)
        except:
            logger.error('error :: failed to process data from memcache key - ionosphere_summary_list')
            ionosphere_summary_list = False

    if not ionosphere_summary_list:
        stmt = "SELECT ionosphere.id, ionosphere.metric_id, metrics.metric FROM ionosphere INNER JOIN metrics ON ionosphere.metric_id=metrics.id"
        try:
            connection = engine.connect()
            results = connection.execute(stmt)
            connection.close()
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            logger.error('error :: could not determine metrics from metrics table')
            # Disposal and raise for Bug #2130: MySQL - Aborted_clients
            if engine:
                engine_disposal(engine)
            return False, fail_msg, trace
        if results:
            # Because the each row in the results is a dict and all the rows are
            # being used, these are being converted into a list and stored in
            # memcache as a list
            ionosphere_summary_list = []
            for row in results:
                ionosphere_summary_list.append([int(row['id']), int(row['metric_id']), str(row['metric'])])
            if settings.MEMCACHE_ENABLED:
                try:
                    memcache_client.set('ionosphere_summary_list', ionosphere_summary_list, expire=600)
                    logger.info('set memcache ionosphere_summary_list key with DB results')
                except:
                    logger.error('error :: failed to get ionosphere_summary_list from memcache')
                try:
                    memcache_client.close()
                # Added nosec to exclude from bandit tests
                except:  # nosec
                    pass

    # @added 20190328 - Feature #2484: FULL_DURATION feature profiles
    # Added ionosphere_echo, get the ids of all echo_fp features profiles
    echo_fp_ids = []
    if settings.IONOSPHERE_ECHO_ENABLED:
        echo_fp_ids_stmt = 'SELECT id FROM ionosphere WHERE echo_fp=1'
        try:
            connection = engine.connect()
            results = connection.execute(echo_fp_ids_stmt)
            connection.close()
            for row in results:
                fp_id = int(row[0])
                echo_fp_ids.append(fp_id)
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            logger.error('error :: could not determine echo fp ids from ionosphere table')
            if engine:
                engine_disposal(engine)
            return False, fail_msg, trace
        logger.info('Determined %s echo_fp fp ids' % str(len(echo_fp_ids)))

# ionosphere_matched table layout
# | id    | fp_id | metric_timestamp | all_calc_features_sum | all_calc_features_count | sum_common_values | common_features_count | tsfresh_version |
# | 39793 |   782 |       1505560867 |      9856.36758282061 |                     210 |  9813.63277426169 |                   150 | 0.4.0           |
# @modified 20180620 - Feature #2404: Ionosphere - fluid approximation
# Added minmax scaling
# | id    | fp_id | metric_timestamp | all_calc_features_sum | all_calc_features_count | sum_common_values | common_features_count | tsfresh_version | minmax | minmax_fp_features_sum | minmax_fp_features_count | minmax_anomalous_features_sum | minmax_anomalous_features_count |
# | 68071 |  3352 |       1529490602 |      383311386.647846 |                     210 |  383283135.786868 |                   150 | 0.4.0           |      1 |        4085.7427786846 |                      210 |              4048.14642205812 |                             210 |
# ionosphere_layers_matched table layout
# | id    | layer_id | fp_id | metric_id | anomaly_timestamp | anomalous_datapoint | full_duration |
# | 25069 |       24 |  1108 |       195 |        1505561823 |            2.000000 |         86400 |
# @modified 20190601 - Feature #3084: Ionosphere - validated matches
# Added validated
# ionosphere_matched table layout
# | id    | fp_id | metric_timestamp | all_calc_features_sum | all_calc_features_count | sum_common_values | common_features_count | tsfresh_version | minmax | minmax_fp_features_sum | minmax_fp_features_count | minmax_anomalous_features_sum | minmax_anomalous_features_count | fp_count | fp_checked | validated |
# | 26946 |  2925 |       1529664909 |      -214679719261.15 |                     210 | -214680153882.557 |                   150 | 0.4.0           |      1 |       4162.60829454032 |                      210 |              4196.51947426028 |                             210 |        0 |          0 |         0 |
# ionosphere_layers_matched table layout
# | id | layer_id | fp_id | metric_id | anomaly_timestamp | anomalous_datapoint | full_duration | layers_count | layers_checked | approx_close | validated |
# |  1 |       27 |  1114 |        34 |        1488971471 |           10.000000 |         86400 |            0 |              0 |            0 |         0 |

    matches = []
# matches list elements - where id is the ionosphere_matched or the
# ionosphere_layers_matched table id for the match being processed
# [metric_timestamp, id, matched_by, fp_id, layer_id, metric, uri_to_matched_page]
# e.g.
# [[1505560867, 39793, 'features_profile', 782, 'None', 'stats.skyline-dev-3-40g-gra1.vda.ioInProgress', 'ionosphere?fp_matched=true...'],
# [1505561823, 25069, 'layers', 1108, 24, 'stats.controller-dev-3-40g-sbg1.apache.sending', 'ionosphere?fp_matched=true...']]

    metric_list = []

    if get_features_profiles_matched:
        try:
            connection = engine.connect()
            stmt = query_string
            logger.info('executing %s' % stmt)
            results = connection.execute(stmt)
            connection.close()
        except:
            trace = traceback.format_exc()
            logger.error(traceback.format_exc())
            logger.error('error :: could not determine metrics from metrics table')
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal and raise
            if engine:
                engine_disposal(engine)
            return False, fail_msg, trace

        for row in results:
            metric_timestamp = int(row['metric_timestamp'])
            metric_human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(metric_timestamp)))
            match_id = int(row['id'])
            # @modified 20180620 - Feature #2404: Ionosphere - fluid approximation
            # Added minmax scaling
            # matched_by = 'features profile'
            minmax = int(row['minmax'])
            if minmax == 0:
                matched_by = 'features profile'
            else:
                matched_by = 'features profile - minmax'
            fp_id = int(row['fp_id'])

            # @added 20190601 - Feature #3084: Ionosphere - validated matches
            validated = int(row['validated'])

            # @added 20190328 - Feature #2484: FULL_DURATION feature profiles
            # Added ionosphere_echo
            if settings.IONOSPHERE_ECHO_ENABLED:
                for echo_fp_id in echo_fp_ids:
                    if echo_fp_id == fp_id:
                        new_matched_by = '%s - echo' % matched_by
                        matched_by = new_matched_by
                        break

            layer_id = 'None'
            # Get metric name, first get metric id from the features profile
            # record
            try:
                metric_list = [row[2] for row in ionosphere_summary_list if row[0] == fp_id]
                metric = metric_list[0]
            except:
                metric = 'UNKNOWN'
            uri_to_matched_page = 'None'

            # @added 20210413 - Feature #4014: Ionosphere - inference
            #                   Branch #3590: inference
            motifs_matched_id = 0
            try:
                motifs_matched_id = int(row['motifs_matched_id'])
                if motifs_matched_id:
                    matched_by = 'motif'
            except:
                motifs_matched_id = 0

            # @modified 20190601 - Feature #3084: Ionosphere - validated matches
            # Added validated
            # matches.append([metric_human_date, match_id, matched_by, fp_id, layer_id, metric, uri_to_matched_page])
            # @added 20200908 - Feature #3740: webapp - anomaly API endpoint
            # Added metric_timestamp
            # matches.append([metric_human_date, match_id, matched_by, fp_id, layer_id, metric, uri_to_matched_page, validated])
            # @modified 20210413 - Feature #4014: Ionosphere - inference
            #                   Branch #3590: inference
            # Added motifs_matched_id
            matches.append([metric_human_date, match_id, matched_by, fp_id, layer_id, metric, uri_to_matched_page, validated, metric_timestamp, motifs_matched_id])

    if get_layers_matched:
        # layers matches
        new_query_string = query_string.replace('ionosphere_matched', 'ionosphere_layers_matched')
        query_string = new_query_string
        new_query_string = query_string.replace('metric_timestamp', 'anomaly_timestamp')
        query_string = new_query_string
        try:
            connection = engine.connect()
            stmt = query_string
            logger.info('executing %s' % stmt)
            results = connection.execute(stmt)
            connection.close()
        except:
            trace = traceback.format_exc()
            logger.error(traceback.format_exc())
            logger.error('error :: could not determine metrics from metrics table')
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal and raise
            if engine:
                engine_disposal(engine)
            return False, fail_msg, trace

        for row in results:
            anomaly_timestamp = int(row['anomaly_timestamp'])
            metric_human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(anomaly_timestamp)))
            match_id = int(row['id'])
            # @modified 20180921 - Feature #2558: Ionosphere - fluid approximation - approximately_close on layers
            # matched_by = 'layers'
            try:
                approx_close = int(row['approx_close'])
            except:
                approx_close = 0
            if approx_close == 0:
                matched_by = 'layers'
            else:
                matched_by = 'layers - approx_close'

            # @added 20190601 - Feature #3084: Ionosphere - validated matches
            validated = int(row['validated'])

            fp_id = int(row['fp_id'])
            layer_id = int(row['layer_id'])
            # Get metric name, first get metric id from the features profile
            # record
            try:
                metric_list = [row[2] for row in ionosphere_summary_list if row[0] == fp_id]
                metric = metric_list[0]
            except:
                metric = 'UNKNOWN'
            uri_to_matched_page = 'None'

            # @modified 20190601 - Feature #3084: Ionosphere - validated matches
            # Added validated
            # matches.append([metric_human_date, match_id, matched_by, fp_id, layer_id, metric, uri_to_matched_page])
            # @added 20200908 - Feature #3740: webapp - anomaly API endpoint
            # Added anomaly_timestamp
            # matches.append([metric_human_date, match_id, matched_by, fp_id, layer_id, metric, uri_to_matched_page, validated])
            # @modified 20210413 - Feature #4014: Ionosphere - inference
            #                      Branch #3590: inference
            # Added motifs_matched_id
            motifs_matched_id = 0
            matches.append([metric_human_date, match_id, matched_by, fp_id, layer_id, metric, uri_to_matched_page, validated, anomaly_timestamp, motifs_matched_id])

    sorted_matches = sorted(matches, key=lambda x: x[0])
    matches = sorted_matches

    if engine:
        engine_disposal(engine)
    try:
        del metric_list
    except:
        logger.error('error :: failed to del metrics_list')

    # @added 20180809 - Bug #2496: error reported on no matches found
    #                   https://github.com/earthgecko/skyline/issues/64
    # If there are no matches return this information in matches to prevent
    # webapp from reporting an error
    if not matches:
        # [[1505560867, 39793, 'features_profile', 782, 'None', 'stats.skyline-dev-3-40g-gra1.vda.ioInProgress', 'ionosphere?fp_matched=true...'],
        # @modified 20180921 - Feature #2558: Ionosphere - fluid approximation - approximately_close on layers
        # matches = [['None', 'None', 'no matches were found', 'None', 'None', 'no matches were found', 'None']]
        # @added 20200908 - Feature #3740: webapp - anomaly API endpoint
        # Added anomaly_timestamp
        # matches = [['None', 'None', 'no matches were found', 'None', 'None', 'no matches were found', 'None', 'None']]
        # @modified 20210413 - Feature #4014: Ionosphere - inference
        #                      Branch #3590: inference
        # Added motifs_matched_id
        matches = [['None', 'None', 'no matches were found', 'None', 'None', 'no matches were found', 'None', 'None', 'None', 'None']]

    return matches, fail_msg, trace


# @added 20170917 - Feature #1996: Ionosphere - matches page
def get_matched_id_resources(matched_id, matched_by, metric, requested_timestamp):
    """
    Get the Ionosphere matched details of a features profile or layer

    :param matched_id: the matched id
    :type id: int
    :param matched_by: either features_profile, layers or motif
    :type id: str
    :param metric: metric base_name
    :type id: str
    :param requested_timestamp: the timestamp of the features profile
    :type id: int
    :return: tuple
    :rtype:  (str, boolean, str, str)

    """
    logger = logging.getLogger(skyline_app_logger)

    function_str = 'ionoshere_backend.py :: get_matched_id_resources'

    trace = 'none'
    fail_msg = 'none'
    matched_details = None
    matched_details_object = None

    use_table = 'ionosphere_matched'
    if matched_by == 'layers':
        use_table = 'ionosphere_layers_matched'
    # @added 20210413 - Feature #4014: Ionosphere - inference
    #                   Branch #3590: inference
    if matched_by == 'motif':
        use_table = 'motifs_matched'

    logger.info('%s :: getting MySQL engine' % function_str)
    try:
        engine, fail_msg, trace = get_an_engine()
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not get a MySQL engine'
        logger.error('%s' % fail_msg)
        # return False, False, fail_msg, trace, False
        raise  # to webapp to return in the UI

    if not engine:
        trace = 'none'
        fail_msg = 'error :: engine not obtained'
        logger.error(fail_msg)
        # return False, False, fail_msg, trace, False
        raise  # to webapp to return in the UI

    if matched_by == 'features_profile':
        ionosphere_matched_table = None
        try:
            ionosphere_matched_table, fail_msg, trace = ionosphere_matched_table_meta(skyline_app, engine)
            logger.info(fail_msg)
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)

    if matched_by == 'layers':
        ionosphere_layers_matched_table = None
        try:
            ionosphere_layers_matched_table, fail_msg, trace = ionosphere_layers_matched_table_meta(skyline_app, engine)
            logger.info(fail_msg)
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)

    # @added 20210413 - Feature #4014: Ionosphere - inference
    #                   Branch #3590: inference
    if matched_by == 'motif':
        motifs_matched_table = None
        try:
            motifs_matched_table, fail_msg, trace = motifs_matched_table_meta(skyline_app, engine)
            logger.info(fail_msg)
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)

    if trace != 'none':
        fail_msg = 'error :: failed to get %s table for matched id %s' % (use_table, str(matched_id))
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        # return False, False, fail_msg, trace, False
        raise  # to webapp to return in the UI

    logger.info('%s :: %s table OK' % (function_str, use_table))

    if matched_by == 'features_profile':
        stmt = select([ionosphere_matched_table]).where(ionosphere_matched_table.c.id == int(matched_id))
    if matched_by == 'layers':
        stmt = select([ionosphere_layers_matched_table]).where(ionosphere_layers_matched_table.c.id == int(matched_id))

    # @added 20210413 - Feature #4014: Ionosphere - inference
    #                   Branch #3590: inference
    if matched_by == 'motif':
        stmt = select([motifs_matched_table]).where(motifs_matched_table.c.id == int(matched_id))

    try:
        connection = engine.connect()
        # stmt = select([ionosphere_matched_table]).where(ionosphere_matched_table.c.id == int(matched_id))
        result = connection.execute(stmt)
        row = result.fetchone()
        # @modified 20210415 - Feature #4014: Ionosphere - inference
        # TypeError: 'RowProxy' object does not support item assignment
        # Convert RowProxy to dict to enable adding elements to the
        # matched_details_object object, like motif_period_minutes
        # matched_details_object = row
        # @modified 20210416 - Feature #4014: Ionosphere - inference
        # Check data was returned from the query because if a not existent motif
        # id is passed ...
        #     matched_details_object = dict(row)
        # TypeError: 'NoneType' object is not iterable
        # matched_details_object = dict(row)
        try:
            matched_details_object = dict(row)
        except Exception as e:
            trace = traceback.format_exc()
            connection.close()
            logger.error(trace)
            fail_msg = 'error :: could not get matched_id %s details from %s DB table row - %s' % (str(matched_id), use_table, e)
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI
        connection.close()
    except Exception as e:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not get matched_id %s details from %s DB table - %s' % (str(matched_id), use_table, e)
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        # return False, False, fail_msg, trace, False
        raise  # to webapp to return in the UI

    if matched_by == 'features_profile':
        try:
            fp_id = row['fp_id']
            metric_timestamp = row['metric_timestamp']
            all_calc_features_sum = row['all_calc_features_sum']
            all_calc_features_count = row['all_calc_features_count']
            sum_common_values = row['sum_common_values']
            common_features_count = row['common_features_count']
            tsfresh_version = row['tsfresh_version']
            matched_human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(metric_timestamp)))
            # @added 20180620 - Feature #2404: Ionosphere - fluid approximation
            # Added minmax scaling
            minmax = int(row['minmax'])
            minmax_fp_features_sum = row['minmax_fp_features_sum']
            minmax_fp_features_count = row['minmax_fp_features_count']
            minmax_anomalous_features_sum = row['minmax_anomalous_features_sum']
            minmax_anomalous_features_count = row['minmax_anomalous_features_count']
            matched_details = '''
tsfresh_version       :: %s
all_calc_features_sum :: %s     | all_calc_features_count :: %s
sum_common_values     :: %s     | common_features_count :: %s
metric_timestamp      :: %s               | human_date :: %s
minmax_scaled         :: %s
minmax_fp_features_sum        :: %s  | minmax_fp_features_count :: %s
minmax_anomalous_features_sum :: %s  | minmax_anomalous_features_count :: %s
''' % (str(tsfresh_version), str(all_calc_features_sum),
                str(all_calc_features_count), str(sum_common_values),
                str(common_features_count), str(metric_timestamp),
                str(matched_human_date), str(minmax),
                str(minmax_fp_features_sum), str(minmax_fp_features_count),
                str(minmax_anomalous_features_sum),
                str(minmax_anomalous_features_count))
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: could not get details for matched id %s - %s' % (str(matched_id), e)
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            # return False, False, fail_msg, trace, False
            raise  # to webapp to return in the UI
        full_duration_stmt = 'SELECT full_duration FROM ionosphere WHERE id=%s' % str(fp_id)
        full_duration = None
        try:
            connection = engine.connect()
            result = connection.execute(full_duration_stmt)
            connection.close()
            for row in result:
                if not full_duration:
                    full_duration = int(row[0])
            logger.info('full_duration for matched determined as %s' % (str(full_duration)))
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            logger.error('error :: could not determine full_duration from ionosphere table - %s' % e)
            # Disposal and return False, fail_msg, trace for Bug #2130: MySQL - Aborted_clients
            if engine:
                engine_disposal(engine)
            return False, fail_msg, trace

    if matched_by == 'layers':
        try:
            layer_id = row['layer_id']
            fp_id = row['fp_id']
            metric_timestamp = row['anomaly_timestamp']
            anomalous_datapoint = row['anomalous_datapoint']
            full_duration = row['full_duration']
            matched_human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(metric_timestamp)))
            matched_details = '''
layer_id            :: %s
anomalous_datapoint :: %s
full_duration       :: %s
metric_timestamp    :: %s     | human_date :: %s
''' % (str(layer_id), str(anomalous_datapoint), str(full_duration),
                str(metric_timestamp), str(matched_human_date))
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: could not get details for matched id %s - %s' % (str(matched_id), e)
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            # return False, False, fail_msg, trace, False
            raise  # to webapp to return in the UI

    # @added 20210413 - Feature #4014: Ionosphere - inference
    #                   Branch #3590: inference
    if matched_by == 'motif':
        motif_match_types = motif_match_types_dict()
        try:
            motif_id = row['id']
            metric_id = row['metric_id']
            fp_id = row['fp_id']
            metric_timestamp = row['metric_timestamp']
            primary_match = row['primary_match']
            index = row['index']
            size = row['size']
            distance = row['distance']
            type_id = row['type_id']
            for match_type in motif_match_types:
                if type_id == motif_match_types[match_type]:
                    break
            validated = row['validated']
            matched_human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(metric_timestamp)))
            if str(distance) == '0E-10':
                use_distance = 0.0
            else:
                use_distance = distance
            if str(primary_match) == '0':
                primary_match_str = 'False'
            else:
                primary_match_str = 'True'

            # @added 20210424 -
            try:
                motif_area = row['motif_area']
                fp_motif_area = row['fp_motif_area']
                area_percent_diff = row['area_percent_diff']
            except KeyError:
                motif_area = None
                fp_motif_area = None
                area_percent_diff = None
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: could not get details for motif matched id %s - %s' % (str(matched_id), e)
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            # return False, False, fail_msg, trace, False
            raise  # to webapp to return in the UI
        full_duration = size * 60
        generation = 0
        try:
            query = 'SELECT generation FROM ionosphere WHERE id=%s' % (str(fp_id))
            results = mysql_select(skyline_app, query)
            for result in results:
                generation = int(result[0])
        except Exception as e:
            logger.error('error :: get_matched_id_resources :: failed to get generation from the database for fp_id %s from ionoshere table - %s' % (
                str(fp_id), e))
        if generation == 0:
            generation_str = 'trained'
        else:
            generation_str = 'LEARNT'

        # @added 20210424 -
        # matched_id, matched_by, metric, requested_timestamp
        # /opt/skyline/ionosphere/data/1619267040/telegraf/ssdnodes-26820/sda1/diskio/iops_in_progress/1619267040.telegraf.ssdnodes-26820.sda1.diskio.iops_in_progress.inference.matched_motifs.dict
        metric_timeseries_dir = metric.replace('.', '/')
        inference_file = '%s/%s/%s/%s.%s.inference.matched_motifs.dict' % (
            settings.IONOSPHERE_DATA_FOLDER, str(metric_timestamp),
            metric_timeseries_dir, str(metric_timestamp), metric)
        if path.isfile(inference_file):
            if not motif_area or fp_motif_area:
                matched_motif_dict = {}
                try:
                    with open(inference_file, 'r') as f:
                        matched_motif_dict_str = f.read()
                    matched_motif_dict = literal_eval(matched_motif_dict_str)
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to evaluate matched_motifs_dict from %s - %s' % (inference_file, e))
                    matched_motif_dict = {}
                    matched_motif = None
                if matched_motif_dict:
                    try:
                        matched_motif = list(matched_motif_dict.keys())[0]
                        motif_area = matched_motif_dict[matched_motif]['motif_area']
                        fp_motif_area = matched_motif_dict[matched_motif]['fp_motif_area']
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to evaluate matched_motifs_dict from %s - %s' % (inference_file, e))
                    try:
                        area_percent_diff = matched_motif_dict[matched_motif]['area_percent_diff']
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to evaluate matched_motifs_dict from %s - %s' % (inference_file, e))
                        percent_different = None
                        if motif_area and fp_motif_area:
                            try:
                                percent_different = get_percent_different(fp_motif_area, motif_area, True)
                                logger.info('percent_different between fp_motif_area and motif_area - %s' % str(percent_different))
                            except Exception as e:
                                logger.error('error :: failed to calculate percent_different - %s' % e)

        try:
            matched_details = '''
motif_id            :: %s     | fp_id         :: %s (%s)
size                :: %s     | index         :: %s
distance            :: %s
motif_area          :: %s
fp_motif_area       :: %s
area_diff           :: %s %%
match_type          :: %s     | type_id       :: %s
validated           :: %s     | primary_match :: %s
metric_timestamp    :: %s
human_date          :: %s
''' % (str(motif_id), str(fp_id), generation_str, str(size), str(index),
                str(use_distance),
                str(motif_area),
                str(fp_motif_area),
                str(area_percent_diff),
                str(match_type), str(type_id),
                str(validated), primary_match_str,
                str(metric_timestamp), str(matched_human_date))
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: could not create matched_details for motif matched id %s - %s' % (str(matched_id), e)
            logger.error('%s' % fail_msg)
            matched_details = '''
%s
%s
''' % (fail_msg, trace)

    if engine:
        engine_disposal(engine)

    # Create a Graphite image
    from_timestamp = str(int(metric_timestamp) - int(full_duration))
    until_timestamp = str(metric_timestamp)
    timeseries_dir = metric.replace('.', '/')
    metric_data_dir = '%s/%s/%s' % (
        settings.IONOSPHERE_PROFILES_FOLDER, timeseries_dir,
        str(requested_timestamp))

    if matched_by == 'features_profile':
        graph_image_file = '%s/%s.matched.fp_id-%s.%s.png' % (
            metric_data_dir, metric, str(fp_id), str(metric_timestamp))
    if matched_by == 'layers':
        graph_image_file = '%s/%s.layers_id-%s.matched.layers.fp_id-%s.%s.png' % (
            metric_data_dir, metric, str(matched_id),
            str(fp_id), str(layer_id))

    # @added 20210413 - Feature #4014: Ionosphere - inference
    #                   Branch #3590: inference
    if matched_by == 'motif':
        graph_image_file = '%s/%s.matched.motif_id-%s.%s.png' % (
            metric_data_dir, metric, str(motif_id), str(metric_timestamp))
        # Create the fp motif
        fp_timeseries = []
        # metric_fp_ts_table = 'z_ts_%s' % str(metric_id)
        try:
            # query = 'SELECT timestamp,value FROM %s WHERE fp_id=%s' % (metric_fp_ts_table, str(fp_id))
            # results = mysql_select(skyline_app, query)
            # for result in results:
            #     fp_timeseries.append([int(result[0]), result[1]])
            int_fp_id = int(fp_id) + 0
            fp_timeseries = get_db_fp_timeseries(skyline_app, metric_id, int_fp_id)
        except Exception as e:
            # logger.error('error :: get_matched_id_resources :: failed to get timeseries from the database for fp_id %s from %s table - %s' % (
            #     str(fp_id), metric_fp_ts_table, e))
            logger.error('error :: get_matched_id_resources :: failed to get timeseries with get_db_fp_timeseries - %s' % (
                e))
        fp_motif = [item[1] for item in fp_timeseries[index:(index + size)]]
        last_fp_timeseries_index = len(fp_timeseries) - 1
        if last_fp_timeseries_index < (index + size):
            logger.info('get_matched_id_resources :: adjusting index for fp_motif sequence because (index + size) > last_fp_timeseries_index')
            index_diff = (index + size) - last_fp_timeseries_index
            use_index = index - index_diff
            fp_motif = [item[1] for item in fp_timeseries[use_index:last_fp_timeseries_index]]

        # Create the not anomalous motif that was matched with the fp_motif
        not_anomalous_motif_sequence = []
        if IONOSPHERE_INFERENCE_STORE_MATCHED_MOTIFS:
            try:
                query = 'SELECT timestamp,value FROM not_anomalous_motifs WHERE motif_id=%s' % (str(motif_id))
                results = mysql_select(skyline_app, query)
                for result in results:
                    not_anomalous_motif_sequence.append([int(result[0]), result[1]])
            except Exception as e:
                logger.error('error :: get_matched_id_resources :: failed to get motif sequence from not_anomalous_motifs for motif_id %s - %s' % (
                    str(motif_id), e))
            if not not_anomalous_motif_sequence:
                logger.error('error :: get_matched_id_resources :: failed to get not_anomalous_motif_sequence from not_anomalous_motifs table for motif_id %s' % (
                    str(motif_id)))

        # If the timeseries was not found in the DB try and load it from the
        # training data, this should only ever be needed for testing
        if not not_anomalous_motif_sequence:
            logger.info('get_matched_id_resources :: trying to get not_anomalous_motif_sequence from training data for motif_id %s' % (
                str(motif_id)))
            metric_dir = metric.replace('.', '/')
            metric_training_dir = '%s/%s/%s' % (
                settings.IONOSPHERE_DATA_FOLDER, str(metric_timestamp), metric_dir)
            timeseries_json = '%s/%s.json' % (metric_training_dir, metric)

            # @modified 20210419 -
            # Properly interpolate the FULL_DURATION hours
            # full_duration_timeseries_json = '%s/%s.mirage.redis.24h.json' % (metric_training_dir, metric)
            full_duration_in_hours = int(settings.FULL_DURATION / 60 / 60)
            full_duration_timeseries_json = '%s/%s.mirage.redis.%sh.json' % (
                metric_training_dir, metric, str(full_duration_in_hours))

            full_duration = 0
            try:
                query = 'SELECT full_duration from ionosphere WHERE id=%s' % (str(fp_id))
                results = mysql_select(skyline_app, query)
                for result in results:
                    full_duration = int(result[0])
            except Exception as e:
                logger.error('error :: get_matched_id_resources :: failed to get full_duration of fp id %s via mysql_select - %s' % (str(fp_id), e))
            if full_duration == settings.FULL_DURATION:
                timeseries_json_file = full_duration_timeseries_json
            else:
                timeseries_json_file = timeseries_json
            timeseries = []
            if path.isfile(timeseries_json_file):
                try:
                    with open((timeseries_json_file), 'r') as f:
                        raw_timeseries = f.read()
                    timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
                    del raw_timeseries
                    timeseries = literal_eval(timeseries_array_str)
                    del timeseries_array_str
                except Exception as e:
                    logger.error('error :: get_matched_id_resources :: failed to load timeseries from %s - %s' % (metric, e))
            if timeseries:
                not_anomalous_motif_sequence = timeseries[-size:]
        if not not_anomalous_motif_sequence:
            logger.error('error :: get_matched_id_resources :: failed to load any timeseries data to created matched_motif plot from %s' % (timeseries_json_file))
            graph_image_file = None
            return matched_details, False, fail_msg, trace, matched_details_object, graph_image_file

        matched_details_object['motif_timeseries'] = not_anomalous_motif_sequence
        matched_details_object['motif_sequence'] = [item[1] for item in not_anomalous_motif_sequence]
        # @added 20210415 - Feature #4014: Ionosphere - inference
        # I realised looking through the graphs with Sab that users of Skyline are
        # normally used to 7 days graps mostly, 24hour graphs and 30d graphs.
        # They are not used to minutes.
        # Make the user aware of the specific resolution they are viewing, a new
        # UI resolution for Skyline (LAST X MINUTES).  Thank you my love.
        # For everything.
        not_anomalous_timestamp = int(not_anomalous_motif_sequence[-1][0])
        graph_period_seconds = not_anomalous_timestamp - int(not_anomalous_motif_sequence[0][0])
        matched_details_object['motif_period_seconds'] = graph_period_seconds
        matched_details_object['motif_period_minutes'] = round(graph_period_seconds / 60)

        plotted_image = False
        if not path.isfile(graph_image_file):
            on_demand_motif_analysis = True
            logger.info('fp_motif length: %s' % (str(len(fp_motif))))

            plotted_image, plotted_image_file = plot_motif_match(
                skyline_app, metric, metric_timestamp, fp_id, full_duration,
                generation_str, motif_id, index, size, distance, type_id,
                fp_motif, not_anomalous_motif_sequence, graph_image_file,
                on_demand_motif_analysis)
        else:
            plotted_image = True
        if not plotted_image:
            logger.error('failed to plot motif match plot')
            graph_image_file = None
        return matched_details, True, fail_msg, trace, matched_details_object, graph_image_file

    if not path.isfile(graph_image_file):
        logger.info('getting Graphite graph for match - from_timestamp - %s, until_timestamp - %s' % (str(from_timestamp), str(until_timestamp)))
        graph_image = get_graphite_metric(
            skyline_app, metric, from_timestamp, until_timestamp, 'image',
            graph_image_file)
        if not graph_image:
            logger.error('failed getting Graphite graph')
            graph_image_file = None

    # @added 20200512 - Bug #2534: Ionosphere - fluid approximation - IONOSPHERE_MINMAX_SCALING_RANGE_TOLERANCE on low ranges
    #                   Feature #2404: Ionosphere - fluid approximation
    # Due to the loss of resolution in the Graphite graph images due
    # to their size, create a matplotlib graph for the DB fp time
    # series data for more accurate validation
    fp_ts_graph_file = '%s/%s.fp_id_ts.%s.matplotlib.png' % (
        metric_data_dir, metric, str(fp_id))
    if not path.isfile(fp_ts_graph_file):
        try:
            no_timeseries = []
            created_fp_ts_graph = create_fp_ts_graph('webapp', metric_data_dir, metric, int(fp_id), int(metric_timestamp), no_timeseries)
            if created_fp_ts_graph:
                logger.info('get_matched_id_resources :: created_fp_ts_graph for %s fp id %s' % (metric, str(fp_id)))
        except:
            trace = traceback.format_exc()
            logger.error(traceback.format_exc())
            logger.error('error :: get_matched_id_resources :: failed to created_fp_ts_graph for %s fp id %s' % (metric, str(fp_id)))

    return matched_details, True, fail_msg, trace, matched_details_object, graph_image_file


# @added 20180812 - Feature #2430: Ionosphere validate learnt features profiles page
# @modified 20190601 - Task #3082 - Ionosphere - validate matched table - add generation
def get_features_profiles_to_validate(base_name):
    """
    Get the details for Ionosphere features profiles that need to be validated
    for a metric and returns a list of the details for each of the features
    profile including the ionosphere_image API URIs for all the relevant graph
    images for the weabpp Ionosphere validate_features_profiles page.
    For example::

        [[  fp_id, metric_id, metric, fp_full_duration, anomaly_timestamp,
            fp_parent_id, parent_full_duration, parent_anomaly_timestamp, fp_date,
            fp_graph_uri, parent_fp_date, parent_fp_graph_uri, parent_parent_fp_id,
            fp_learn_graph_uri, parent_fp_learn_graph_uri, minimum_full_duration,
            maximum_full_duration, generation]]

    :param base_name: metric base_name
    :type base_name: str
    :return: list of lists
    :rtype:  [[int, int, str, int, int, int, int, int, str, str, str, str, int, str, str, int, int]]

    """
    logger = logging.getLogger(skyline_app_logger)

    function_str = 'ionoshere_backend.py :: get_feature_profiles_validate'

    trace = 'none'
    fail_msg = 'none'

    # Query the ionosphere_functions function for base_name, validated == false
    # and get the details for each features profile that needs to be validated
    features_profiles_to_validate = []
    search_success = False
    fps = []
    try:
        fps, fps_count, mc, cc, gc, full_duration_list, enabled_list, tsfresh_version_list, generation_list, search_success, fail_msg, trace = ionosphere_search(False, True)
        logger.info('fp object :: %s' % str(fps))
    except:
        trace = traceback.format_exc()
        fail_msg = 'error :: %s :: error with search_ionosphere' % function_str
        logger.error(fail_msg)
        return (features_profiles_to_validate, fail_msg, trace)
    if not search_success:
        trace = traceback.format_exc()
        fail_msg = 'error :: %s :: Webapp error with search_ionosphere' % function_str
        logger.error(fail_msg)
        return (features_profiles_to_validate, fail_msg, trace)

    # Determine the minimum and maximum full durations from the returned fps so
    # this can be used later to determine what class of features profile is
    # being dealt with in terms of whether the features profile is a
    # full_duration LEARNT features profile or a settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS
    # LEARNT features profile.  This allows for determining the correct other
    # resolution ionosphere_image URIs which are interpolated for display in the
    # HTML table on the validate_features_profiles page.
    minimum_full_duration = None
    maximum_full_duration = None
    # [fp_id, metric_id, metric, full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id, layer_matched_count, layer_human_date, layer_check_count, layer_checked_human_date, layer_label]
    # [4029, 157, 'stats.skyline-dev-3.vda1.ioTime', 604800, 1534001973, '0.4.0', 0.841248, 210, 70108436036.9, 0, 0, 'never matched', '2018-08-11 16:41:04', 0, 'never checked', 3865, 6, 0, 0]
    # for fp_id, metric_id, metric, fp_full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id in fps:
    # @modified 20210425 - Feature #4014: Ionosphere - inference
    # Added motif_matched_count, motif_last_matched, motif_last_checked, motif_checked_count
    for fp_id, metric_id, metric, fp_full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id, layer_matched_count, layer_human_date, layer_check_count, layer_checked_human_date, layer_label, motif_matched_count, motif_last_matched, motif_last_checked, motif_checked_count in fps:
        if not minimum_full_duration:
            minimum_full_duration = int(fp_full_duration)
        else:
            if int(fp_full_duration) < int(minimum_full_duration):
                minimum_full_duration = int(fp_full_duration)
        if not maximum_full_duration:
            maximum_full_duration = int(fp_full_duration)
        else:
            if int(fp_full_duration) > int(maximum_full_duration):
                maximum_full_duration = int(fp_full_duration)

    # Get the features profile parent details (or parent parent if needed) to
    # determine the correct arguments for the ionosphere_image URIs for the
    # graph images of the parent, from which the fp being evaluated this was
    # learn for side-by-side visual comparison to inform the user and all for
    # them to
    # [fp_id, metric_id, metric, full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id, layer_matched_count, layer_human_date, layer_check_count, layer_checked_human_date, layer_label]
    # for fp_id, metric_id, metric, fp_full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id in fps:
    # @modified 20210425 - Feature #4014: Ionosphere - inference
    # Added motif_matched_count, motif_last_matched, motif_last_checked, motif_checked_count
    for fp_id, metric_id, metric, fp_full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id, layer_matched_count, layer_human_date, layer_check_count, layer_checked_human_date, layer_label, motif_matched_count, motif_last_matched, motif_last_checked, motif_checked_count in fps:
        if int(fp_parent_id) == 0:
            continue
        if int(fp_validated) == 1:
            continue

        # @added 20181013 - Feature #2430: Ionosphere validate learnt features profiles page
        if fp_id not in enabled_list:
            continue

        parent_fp_details_object = None
        parent_parent_fp_id = None
        try:
            parent_fp_details, success, fail_msg, trace, parent_fp_details_object = features_profile_details(fp_parent_id)
        except:
            trace = traceback.format_exc()
            fail_msg = 'error :: %s :: failed to get parent_fp_details_object from features_profile_details for parent fp_id %s' % (
                function_str, str(fp_parent_id))
            logger.error(fail_msg)
            return (features_profiles_to_validate, fail_msg, trace)
        if not parent_fp_details_object:
            trace = traceback.format_exc()
            fail_msg = 'error :: %s :: no parent_fp_details_object from features_profile_details for parent fp_id %s' % (
                function_str, str(fp_parent_id))
            logger.error(fail_msg)
            return (features_profiles_to_validate, fail_msg, trace)
        parent_full_duration = parent_fp_details_object['full_duration']

        # If the features profile is learnt at a full_duration of
        # settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS (aka
        # maximum_full_duration), the graphs of the parent's parent fp ip are
        # required.  This is because a features profile that is LEARNT in the
        # learn full duration in days context, will essentially have the same
        # graphs as it's parent.  Therefore the graphs of the parent's parent
        # are required to allow for the side-by-side visual comparsion.
        get_parent_parent = False
        if int(fp_full_duration) > int(minimum_full_duration):
            get_parent_parent = True

        logger.debug('debug :: ionosphere_backend :: get_features_profiles_to_validate :: get_parent_parent: %s' % str(get_parent_parent))

        try:
            parent_parent_fp_id = parent_fp_details_object['parent_id']
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: failed to get parent_parent_fp_id from parent_fp_details_object[\'parent_id\'] - %s' % (
                function_str, err))
            parent_parent_fp_id = 0

        logger.debug('debug :: ionosphere_backend :: get_features_profiles_to_validate :: parent_parent_fp_id: %s' % str(parent_parent_fp_id))

        if int(parent_parent_fp_id) == 0:
            get_parent_parent = False

        parent_parent_fp_details_object = None
        if get_parent_parent:
            try:
                parent_parent_fp_id = parent_fp_details_object['parent_id']
                parent_parent_fp_details, success, fail_msg, trace, parent_parent_fp_details_object = features_profile_details(parent_parent_fp_id)
                parent_parent_full_duration = parent_parent_fp_details_object['full_duration']
                parent_parent_anomaly_timestamp = parent_parent_fp_details_object['anomaly_timestamp']
                logger.debug('debug :: ionosphere_backend :: get_features_profiles_to_validate :: parent_parent_full_duration: %s' % str(parent_parent_full_duration))
            except:
                trace = traceback.format_exc()
                fail_msg = 'error :: %s :: failed to get parent_parent_fp_details_object from features_profile_details for parent parent fp_id %s' % (
                    function_str, str(parent_parent_fp_id))
                logger.error(fail_msg)
                return (features_profiles_to_validate, fail_msg, trace)
        if not parent_fp_details_object:
            trace = traceback.format_exc()
            fail_msg = 'error :: %s :: no parent_fp_details_object from features_profile_details for parent fp_id %s' % (
                function_str, str(fp_parent_id))
            logger.error(fail_msg)
            return (features_profiles_to_validate, fail_msg, trace)
        parent_full_duration = parent_fp_details_object['full_duration']
        parent_anomaly_timestamp = parent_fp_details_object['anomaly_timestamp']

        metric_timeseries_dir = base_name.replace('.', '/')
        # https://skyline.example.com/ionosphere_images?image=/opt/skyline/ionosphere/features_profiles/stats/skyline-1/io/received/1526312070/stats.skyline-1.io.received.graphite_now.168h.png
        # Existing image URLs are namespaced and available via the API from:
        # ionosphere_images?image=/opt/skyline/ionosphere/features_profiles/stats/<base_name>/io/received/<timestamp>/<graphite_metric_namespace>.graphite_now.<full_duration_in_hours>h.png
        fp_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_PROFILES_FOLDER, metric_timeseries_dir,
            str(anomaly_timestamp))
        full_duration_in_hours = fp_full_duration / 60 / 60
        fp_date = time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(int(anomaly_timestamp)))
        fp_graph_uri = 'ionosphere_images?image=%s/%s.graphite_now.%sh.png' % (
            str(fp_data_dir), base_name, str(int(full_duration_in_hours)))
        if int(fp_full_duration) < maximum_full_duration:
            fp_hours = int(maximum_full_duration / 60 / 60)
            get_hours = str(fp_hours)
        else:
            fp_hours = int(minimum_full_duration / 60 / 60)
            get_hours = str(fp_hours)
        fp_learn_graph_uri = 'ionosphere_images?image=%s/%s.graphite_now.%sh.png' % (
            str(fp_data_dir), base_name, get_hours)

        # For this is a LEARNT feature profile at settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS
        # the we want to compare the graph to the parent's parent graph at
        # settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS
        parent_fp_date_str = time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(int(parent_anomaly_timestamp)))
        parent_fp_date = '%s - using parent fp id %s' % (str(parent_fp_date_str), str(int(fp_parent_id)))
        if get_parent_parent and parent_parent_fp_details_object:
            parent_fp_data_dir = '%s/%s/%s' % (
                settings.IONOSPHERE_PROFILES_FOLDER, metric_timeseries_dir,
                str(parent_parent_anomaly_timestamp))
            if parent_parent_full_duration < maximum_full_duration:
                parent_full_duration_in_hours = int(minimum_full_duration) / 60 / 60
            else:
                parent_full_duration_in_hours = int(parent_parent_full_duration) / 60 / 60
            parent_parent_fp_date_str = time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(int(parent_parent_anomaly_timestamp)))
            parent_fp_date = '%s - using parent\'s parent fp id %s' % (str(parent_parent_fp_date_str), str(int(parent_parent_fp_id)))
        else:
            parent_fp_data_dir = '%s/%s/%s' % (
                settings.IONOSPHERE_PROFILES_FOLDER, metric_timeseries_dir,
                str(parent_anomaly_timestamp))
            if parent_full_duration > fp_full_duration:
                parent_full_duration_in_hours = int(fp_full_duration) / 60 / 60
            else:
                parent_full_duration_in_hours = int(parent_full_duration) / 60 / 60
        parent_fp_graph_uri = 'ionosphere_images?image=%s/%s.graphite_now.%sh.png' % (
            str(parent_fp_data_dir), base_name, str(int(parent_full_duration_in_hours)))
        if int(fp_full_duration) == maximum_full_duration:
            fp_hours = int(minimum_full_duration / 60 / 60)
            get_hours = str(fp_hours)
        else:
            fp_hours = int(maximum_full_duration / 60 / 60)
            get_hours = str(fp_hours)
        parent_fp_learn_graph_uri = 'ionosphere_images?image=%s/%s.graphite_now.%sh.png' % (
            # str(parent_fp_data_dir), base_name, str(int(parent_full_duration_in_hours)))
            str(parent_fp_data_dir), base_name, get_hours)

        # @modified 20190601 - Task #3082 - Ionosphere - validate matched table - add generation
        # Added parent_fp_generation
        parent_fp_generation = parent_fp_details_object['generation']

        # @modified 20181013 - Feature #2430: Ionosphere validate learnt features profiles page
        # Only add to features_profiles_to_validate if fp_id in enabled_list
        # @modified 20190601 - Task #3082 - Ionosphere - validate matched table - add generation
        # Added fp_generation and parent_fp_generation
        if fp_id in enabled_list:
            features_profiles_to_validate.append([fp_id, metric_id, metric, fp_full_duration, anomaly_timestamp, fp_parent_id, parent_full_duration, parent_anomaly_timestamp, fp_date, fp_graph_uri, parent_fp_date, parent_fp_graph_uri, parent_parent_fp_id, fp_learn_graph_uri, parent_fp_learn_graph_uri, minimum_full_duration, maximum_full_duration, fp_generation, parent_fp_generation])

    logger.info('%s :: features_profiles_to_validate - %s' % (
        function_str, str(features_profiles_to_validate)))
    return (features_profiles_to_validate, fail_msg, trace)


# @added 20180815 - Feature #2430: Ionosphere validate learnt features profiles page
def get_metrics_with_features_profiles_to_validate():
    """
    Get the metrics with Ionosphere features profiles that need to be validated
    and return a list of the details for each metric.
    [[metric_id, metric, fps_to_validate_count]]

    :return: list of lists
    :rtype:  [[int, str, int]]

    """
    logger = logging.getLogger(skyline_app_logger)

    function_str = 'ionoshere_backend.py :: get_metrics_with_features_profiles_to_validate'

    trace = 'none'
    fail_msg = 'none'

    # Query the ionosphere_functions function for base_name, validated == false
    # and get the details for each features profile that needs to be validated
    metrics_with_features_profiles_to_validate = []
    search_success = False
    fps = []
    try:
        fps, fps_count, mc, cc, gc, full_duration_list, enabled_list, tsfresh_version_list, generation_list, search_success, fail_msg, trace = ionosphere_search(False, True)
    except:
        trace = traceback.format_exc()
        fail_msg = 'error :: %s :: error with search_ionosphere' % function_str
        logger.error(fail_msg)
        return (metrics_with_features_profiles_to_validate, fail_msg, trace)
    if not search_success:
        trace = traceback.format_exc()
        fail_msg = 'error :: %s :: Webapp error with search_ionosphere' % function_str
        logger.error(fail_msg)
        return (metrics_with_features_profiles_to_validate, fail_msg, trace)

    # Determine the minimum and maximum full durations from the returned fps so
    # this can be used later to determine what class of features profile is
    # being dealt with in terms of whether the features profile is a
    # full_duration LEARNT features profile or a settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS
    # LEARNT features profile.  This allows for determining the correct other
    # resolution ionosphere_image URIs which are interpolated for display in the
    # HTML table on the validate_features_profiles page.
    # [fp_id, metric_id, metric, full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id, layer_matched_count, layer_human_date, layer_check_count, layer_checked_human_date, layer_label]
    metric_ids_with_fps_to_validate = []
    # for fp_id, metric_id, metric, fp_full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id in fps:
    # @modified 20210425 - Feature #4014: Ionosphere - inference
    # Added motif_matched_count, motif_last_matched, motif_last_checked, motif_checked_count
    for fp_id, metric_id, metric, fp_full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id, layer_matched_count, layer_human_date, layer_check_count, layer_checked_human_date, layer_label, motif_matched_count, motif_last_matched, motif_last_checked, motif_checked_count in fps:
        # @added 20181013 - Feature #2430: Ionosphere validate learnt features profiles page
        # Only add to features_profiles_to_validate if fp_id in enabled_list
        if fp_id not in enabled_list:
            continue

        if metric_id not in metric_ids_with_fps_to_validate:
            metric_ids_with_fps_to_validate.append(metric_id)
    for i_metric_id in metric_ids_with_fps_to_validate:
        fps_to_validate_count = 0
        # @modified 20210425 - Feature #4014: Ionosphere - inference
        # Added motif_matched_count, motif_last_matched, motif_last_checked, motif_checked_count
        for fp_id, metric_id, metric, fp_full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id, layer_matched_count, layer_human_date, layer_check_count, layer_checked_human_date, layer_label, motif_matched_count, motif_last_matched, motif_last_checked, motif_checked_count in fps:
            if i_metric_id != metric_id:
                continue
            # @added 20181013 - Feature #2430: Ionosphere validate learnt features profiles page
            # Only add to features_profiles_to_validate if fp_id in enabled_list
            if fp_id not in enabled_list:
                continue

            if fp_validated == 0:
                fps_to_validate_count += 1
                i_metric = metric
        if fps_to_validate_count > 0:
            metrics_with_features_profiles_to_validate.append([i_metric_id, i_metric, fps_to_validate_count])
    logger.info('%s :: metrics with features profiles to validate - %s' % (
        function_str, str(metrics_with_features_profiles_to_validate)))
    return (metrics_with_features_profiles_to_validate, fail_msg, trace)


# @added 20181205 - Bug #2746: webapp time out - Graphs in search_features_profiles
#                   Feature #2602: Graphs in search_features_profiles
def ionosphere_show_graphs(requested_timestamp, data_for_metric, fp_id):
    """
    Get a list of all graphs
    """

    # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
    # base_name = data_for_metric.replace(settings.FULL_NAMESPACE, '', 1)
    if data_for_metric.startswith(settings.FULL_NAMESPACE):
        base_name = data_for_metric.replace(settings.FULL_NAMESPACE, '', 1)
    else:
        base_name = data_for_metric

    log_context = 'features profile data show graphs'
    logger.info('%s requested for %s at %s' % (
        log_context, str(base_name), str(requested_timestamp)))
    images = []
    timeseries_dir = base_name.replace('.', '/')
    metric_data_dir = '%s/%s/%s' % (
        settings.IONOSPHERE_PROFILES_FOLDER, timeseries_dir,
        str(requested_timestamp))

    # @modified 20210710 - Bug #4168: webapp/ionosphere_backend.py - handle old features profile dir not been found
    # Circa 2017 there was a bug that was related to features_profile dir not
    # using the anomaly_timestamp but using it offset with max +60
    try:
        td_files = listdir(metric_data_dir)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error('error :: ionosphere_show_graphs :: td_files dir - %s' % e)
        image_404_path = 'webapp/static/images/skyline.ionosphere.image.404.png'
        image_404_file = path.abspath(
            path.join(path.dirname(__file__), '..', image_404_path))
        images.append(str(image_404_file))
        td_files = []

    for i_file in td_files:
        metric_file = path.join(metric_data_dir, i_file)
        if i_file.endswith('.png'):
            # @modified 20170106 - Feature #1842: Ionosphere - Graphite now graphs
            # Exclude any graphite_now png files from the images lists
            append_image = True
            if '.graphite_now.' in i_file:
                append_image = False
            # @added 20170107 - Feature #1852: Ionosphere - features_profile matched graphite graphs
            # Exclude any matched.fp-id images
            if '.matched.fp_id' in i_file:
                append_image = False
            # @added 20170308 - Feature #1960: ionosphere_layers
            #                   Feature #1852: Ionosphere - features_profile matched graphite graphs
            # Exclude any matched.fp-id images
            if '.matched.layers.fp_id' in i_file:
                append_image = False
            if append_image:
                images.append(str(metric_file))

    graphite_now_images = []
    graph_resolutions = []
    graph_resolutions = [int(settings.TARGET_HOURS), 24, 168, 720]
    # @modified 20170107 - Feature #1842: Ionosphere - Graphite now graphs
    # Exclude if matches TARGET_HOURS - unique only
    _graph_resolutions = sorted(set(graph_resolutions))
    graph_resolutions = _graph_resolutions

    for target_hours in graph_resolutions:
        try:
            graph_image_file = '%s/%s.graphite_now.%sh.png' % (metric_data_dir, base_name, str(target_hours))
            if path.isfile(graph_image_file):
                graphite_now_images.append(graph_image_file)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to get Graphite graph at %s hours for %s' % (str(target_hours), base_name))

    return (images, graphite_now_images)


# @added 20190502 - Branch #2646: slack
def webapp_update_slack_thread(base_name, metric_timestamp, value, message_context):
    """
    Update slack threads with enabled events.

    :param base_name: metric base_name
    :param metric_timestamp: the anomaly_timestamp
    :param value: the features profile id, the validated_count or None
    :param message_context: training_data_viewed or layers_created
    :type base_name: str
    :type metric_timestamp: str or int
    :type value: int or None
    :type message_context: str
    :return: True or False
    :rtype:  boolean

    """
    if not update_slack_thread:
        return False

    message_context_known = False
    if message_context == 'training_data_viewed':
        message_context_known = True
        fp_id = value
    if message_context == 'layers_created':
        message_context_known = True
        fp_id = value
    if message_context == 'validated':
        message_context_known = True
        validated_count = value

    # @added 20200929 - Task #3748: POC SNAB
    #                   Branch #3068: SNAB
    if message_context == 'snab_result':
        log_message = 'updating slack that snab result has been submitted'
        message_context_known = True
        snab_result = value

    # @added 20201004 - Task #3748: POC SNAB
    #                   Branch #3068: SNAB
    # Allow results to be changed
    if message_context == 'snab_result_changed':
        log_message = 'updating slack that the snab result has been changed'
        message_context_known = True
        try:
            snab_result_changed_from = value[0]
            snab_result_changed_to = value[1]
        except:
            fail_msg = 'error :: webapp_update_slack_thread :: snab_result_changed could not determine the original and changed to values from - %s' % str(value)
            logger.error('%s' % fail_msg)
            raise  # to webapp to return in the UI

    if not message_context_known:
        fail_msg = 'error :: webapp_update_slack_thread :: unknown message context - %s' % str(message_context)
        logger.error('%s' % fail_msg)
        raise  # to webapp to return in the UI

    update_for_message_context = True
    message_context_not_updating_log_message = False
    if message_context == 'training_data_viewed':
        log_message = 'updating slack that training data was viewed'
        message_on_training_data_viewed = False
        try:
            message_on_training_data_viewed = settings.SLACK_OPTS['message_on_training_data_viewed']
        except:
            message_on_training_data_viewed = False
        if not message_on_training_data_viewed:
            update_for_message_context = False
            message_context_not_updating_log_message = 'SLACK_OPTS[\'message_on_training_data_viewed\'] is False or not defined not updating slack'

    if message_context == 'layers_created':
        log_message = 'updating slack that a features profile was created'
        message_on_features_profile_created = False
        try:
            message_on_features_profile_created = settings.SLACK_OPTS['message_on_features_profile_created']
        except:
            message_on_features_profile_created = False
        if not message_on_features_profile_created:
            update_for_message_context = False
            message_context_not_updating_log_message = 'SLACK_OPTS[\'message_on_features_profile_created\'] is False or not defined not updating slack'

    if message_context == 'validated':
        log_message = 'updating slack that %s features profiles were validated for %s' % (str(validated_count), base_name)
        message_context_known = True
        validated_count = value
        try:
            message_on_validated_features_profiles = settings.SLACK_OPTS['message_on_validated_features_profiles']
        except:
            message_on_validated_features_profiles = False
        if not message_on_validated_features_profiles:
            update_for_message_context = False
            message_context_not_updating_log_message = 'SLACK_OPTS[\'message_on_validated_features_profiles\'] is False or not defined not updating slack'

    if not update_for_message_context:
        logger.info(message_context_not_updating_log_message)
        return False

    if throw_exception_on_default_channel:
        fail_msg = 'error :: webapp_update_slack_thread :: the default_channel or default_channel_id from settings.SLACK_OPTS is set to the default, please replace these with your channel details or set SLACK_ENABLED or SLACK_OPTS[\'thread_updates\'] to False and restart webapp'
        logger.error('%s' % fail_msg)
        raise  # to webapp to return in the UI

    if not update_slack_thread:
        return False
    logger.info(log_message)

    if message_context == 'validated':
        message = '*VALIDATED* - %s features profiles were validated for %s via the webapp' % (str(validated_count), base_name)
        slack_response = {'ok': False}
        try:
            if not channel:
                fail_msg = 'error :: webapp_update_slack_thread :: could not determine the slack default_channel or default_channel_id from settings.SLACK_OPTS please add these to your settings or set SLACK_ENABLED or SLACK_OPTS[\'thread_updates\'] to False and restart webapp'
                logger.error('%s' % fail_msg)
                raise  # to webapp to return in the UI
            slack_response = slack_post_message(skyline_app, channel, None, message)
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: webapp_update_slack_thread :: failed to slack_post_message for validated'
            logger.error('%s' % fail_msg)
            return False
        if not slack_response['ok']:
            fail_msg = 'error :: webapp_update_slack_thread :: failed to slack_post_message for validated, slack dict output follows'
            logger.error('%s' % fail_msg)
            logger.error('%s' % str(slack_response))
            return False
        else:
            logger.info('posted slack update to %s, %s features profiles were validated for %s' % (
                channel, str(validated_count), base_name))
        return True

    use_anomaly_timestamp = int(metric_timestamp)

    try:
        engine, fail_msg, trace = get_an_engine()
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('%s' % fail_msg)
        logger.error('error :: webapp_update_slack_thread :: could not get a MySQL engine to get slack_thread_ts')
        raise  # to webapp to return in the UI

    if not engine:
        trace = 'none'
        fail_msg = 'error :: webapp_update_slack_thread :: engine not obtained'
        logger.error(fail_msg)
        raise

    if message_context != 'snab_result' and message_context != 'snab_result_changed':
        try:
            ionosphere_matched_table, log_msg, trace = ionosphere_matched_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('ionosphere_matched_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: webapp_update_slack_thread :: failed to get ionosphere_checked_table meta for %s' % base_name)
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI

        try:
            metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('metrics_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: webapp_update_slack_thread :: failed to get metrics_table meta for %s' % base_name)
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI
        metric_id = None
        try:
            connection = engine.connect()
            stmt = select([metrics_table]).where(metrics_table.c.metric == base_name)
            result = connection.execute(stmt)
            for row in result:
                metric_id = int(row['id'])
            connection.close()
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: webapp_update_slack_thread :: could not determine metric id from metrics table')
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI
        logger.info('metric id determined as %s' % str(metric_id))
        if metric_id:
            try:
                anomalies_table, log_msg, trace = anomalies_table_meta(skyline_app, engine)
                logger.info(log_msg)
                logger.info('anomalies_table OK')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: webapp_update_slack_thread :: failed to get anomalies_table meta for %s' % base_name)
                if engine:
                    engine_disposal(engine)
                raise  # to webapp to return in the UI
        anomaly_id = None
        slack_thread_ts = 0
        try:
            connection = engine.connect()
            stmt = select([anomalies_table]).\
                where(anomalies_table.c.metric_id == metric_id).\
                where(anomalies_table.c.anomaly_timestamp == int(use_anomaly_timestamp))
            result = connection.execute(stmt)
            for row in result:
                anomaly_id = row['id']
                slack_thread_ts = row['slack_thread_ts']
                break
            connection.close()
            logger.info('determined anomaly id %s for metric id %s at anomaly_timestamp %s with slack_thread_ts %s' % (
                str(anomaly_id), str(metric_id),
                str(use_anomaly_timestamp), str(slack_thread_ts)))
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: webapp_update_slack_thread :: could not determine id or slack_thread_ts of the anomaly from DB for metric id %s at anomaly_timestamp %s' % (
                str(metric_id), str(use_anomaly_timestamp))
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI

    if message_context == 'snab_result' or message_context == 'snab_result_changed':
        try:
            snab_table, log_msg, trace = snab_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('snab_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: webapp_update_slack_thread :: failed to get snab_table meta')
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI
        slack_thread_ts = 0
        # The hacky bit
        snab_id = base_name
        try:
            connection = engine.connect()
            stmt = select([snab_table]).\
                where(snab_table.c.id == snab_id)
            result = connection.execute(stmt)
            for row in result:
                slack_thread_ts = row['slack_thread_ts']
                anomaly_id = row['anomaly_id']
                break
            connection.close()
            logger.info('determined slack_thread_ts %s for snab id %s' % (
                str(slack_thread_ts), str(snab_id)))
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: webapp_update_slack_thread :: could not slack_thread_ts for snab id %s' % (
                str(snab_id))
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI
        if slack_thread_ts:
            if int(slack_thread_ts) == 0:
                slack_thread_ts = None
        if not slack_thread_ts and anomaly_id:
            try:
                anomalies_table, log_msg, trace = anomalies_table_meta(skyline_app, engine)
                logger.info(log_msg)
                logger.info('anomalies_table OK')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: webapp_update_slack_thread :: failed to get anomalies_table meta for %s' % base_name)
                if engine:
                    engine_disposal(engine)
                raise  # to webapp to return in the UI
            try:
                connection = engine.connect()
                stmt = select([anomalies_table]).\
                    where(anomalies_table.c.id == anomaly_id)
                result = connection.execute(stmt)
                for row in result:
                    slack_thread_ts = row['slack_thread_ts']
                    break
                connection.close()
                logger.info('determined slack_thread_ts %s for anomaly_id %s' % (
                    str(slack_thread_ts), str(anomaly_id)))
            except:
                trace = traceback.format_exc()
                logger.error(trace)
                fail_msg = 'error :: webapp_update_slack_thread :: could not determine slack_thread_ts'
                logger.error('%s' % fail_msg)
                if engine:
                    engine_disposal(engine)
                raise  # to webapp to return in the UI

    if float(slack_thread_ts) == 0:
        # There is no known slack_thread_ts value so there is no thread to posted
        # to
        return False

    if message_context == 'training_data_viewed':
        message = 'The training data page has been reviewed'
    if message_context == 'layers_created':
        ionosphere_link = '%s/ionosphere?fp_view=true&fp_id=%s&metric=%s' % (
            settings.SKYLINE_URL, str(fp_id), base_name)
        label = None
        if 'fp_layer_label' in request.args:
            label_arg = request.args.get('fp_layer_label')
            label = label_arg[:255]
        message = '*TRAINED layers* - with label - %s, layers were created on features profile id %s for %s - %s' % (
            str(label), str(fp_id), base_name, ionosphere_link)

    # @added 20200929 - Task #3748: POC SNAB
    #                   Branch #3068: SNAB
    if message_context == 'snab_result':
        message = '*SNAB* - user *EVALUATED* as - %s' % (
            str(snab_result))

    # @added 20201004 - Task #3748: POC SNAB
    #                   Branch #3068: SNAB
    # Allow results to be changed
    if message_context == 'snab_result_changed':
        message = 'Skyline *SNAB* - the result for snab id %s with anomaly id %s was changed from %s to %s' % (
            str(snab_id), str(anomaly_id), str(snab_result_changed_from),
            str(snab_result_changed_to))
        slack_thread_ts = None

    slack_response = {'ok': False}
    try:
        if not channel:
            fail_msg = 'error :: webapp_update_slack_thread :: could not determine the slack default_channel or default_channel_id from settings.SLACK_OPTS please add these to your settings or set SLACK_ENABLED or SLACK_OPTS[\'thread_updates\'] to False and restart webapp'
            logger.error('%s' % fail_msg)
            raise  # to webapp to return in the UI
        slack_response = slack_post_message(skyline_app, channel, str(slack_thread_ts), message)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: webapp_update_slack_thread :: failed to slack_post_message'
        logger.error('%s' % fail_msg)
    if not slack_response['ok']:
        fail_msg = 'error :: webapp_update_slack_thread :: failed to slack_post_message, slack dict output follows'
        logger.error('%s' % fail_msg)
        logger.error('%s' % str(slack_response))
    else:
        logger.info('posted slack update to %s, thread %s' % (
            channel, str(slack_thread_ts)))

    try:
        reaction_emoji = settings.SLACK_OPTS['message_on_training_data_viewed_reaction_emoji']
    except:
        reaction_emoji = 'eyes'

    # @added 20200929 - Task #3748: POC SNAB
    #                   Branch #3068: SNAB
    if message_context == 'snab_result':
        if snab_result == 'tP' or snab_result == 'tN' or snab_result == 'NULL':
            reaction_emoji = 'heavy_check_mark'
        elif snab_result == 'unsure':
            reaction_emoji = 'question'
        else:
            reaction_emoji = 'x'

    slack_response = {'ok': False}
    try:
        if not channel_id:
            fail_msg = 'error :: webapp_update_slack_thread :: could not determine the slack default_channel or default_channel_id from settings.SLACK_OPTS please add these to your settings or set SLACK_ENABLED or SLACK_OPTS[\'thread_updates\'] to False and restart webapp'
            logger.error('%s' % fail_msg)
            raise  # to webapp to return in the UI
        slack_response = slack_post_reaction(skyline_app, channel_id, str(slack_thread_ts), reaction_emoji)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: webapp_update_slack_thread :: failed to slack_post_reaction to channel id %s and slack_thread_ts %s with reaction %s' % (
            str(channel_id), str(slack_thread_ts), str(reaction_emoji))
        logger.error('%s' % fail_msg)
    return True


# @added 20190601 - Feature #3084: Ionosphere - validated matches
# @modified 20190919 - Feature #3230: users DB table
#                      Ideas #2476: Label and relate anomalies
#                      Feature #2516: Add label to features profile
# Added user_id
# def validate_ionosphere_match(match_id, validate_context, match_validated):
def validate_ionosphere_match(match_id, validate_context, match_validated, user_id):
    """
    Update the validated value in the DB for the match.

    :param match_id: the match id
    :param validate_context: the context to validate either ionosphere_matched
        or ionosphere_layers_matched
    :param match_validated: 1 for valid or 2 for invalid
    :param user_id: the user id of the user validating
    :type match_id: str
    :type validate_context: str
    :type match_validated: int
    :type user_id: int
    :return: True or False
    :rtype:  boolean

    """

    try:
        engine, fail_msg, trace = get_an_engine()
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('%s' % fail_msg)
        logger.error('error :: validate_ionosphere_match :: could not get a MySQL engine to get slack_thread_ts')
        raise  # to webapp to return in the UI

    if not engine:
        trace = 'none'
        fail_msg = 'error :: validate_ionosphere_match :: engine not obtained'
        logger.error(fail_msg)
        raise

    if validate_context == 'ionosphere_matched':
        try:
            ionosphere_matched_table, log_msg, trace = ionosphere_matched_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('ionosphere_matched_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: validate_ionosphere_match :: failed to get ionosphere_checked_table meta for match_id %s' % str(match_id))
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI
        try:
            connection = engine.connect()
            # @modified 20190919 - Feature #3230: users DB table
            #                      Ideas #2476: Label and relate anomalies
            #                      Feature #2516: Add label to features profile
            # Added user_id
            connection.execute(
                ionosphere_matched_table.update(
                    ionosphere_matched_table.c.id == match_id).
                values(validated=match_validated, user_id=user_id))
            connection.close()
            logger.info('updated validated for %s by user id %s' % (str(match_id), user_id))
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: could not update validated for %s by user id %s' % (
                str(match_id), user_id)
            logger.error(fail_msg)
            if engine:
                engine_disposal(engine)
            raise

    if validate_context == 'ionosphere_layers_matched':
        try:
            ionosphere_layers_matched_table, log_msg, trace = ionosphere_layers_matched_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('ionosphere_layers_matched_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: validate_ionosphere_match :: failed to get ionosphere_checked_table meta for match_id %s' % str(match_id))
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI
        try:
            connection = engine.connect()
            connection.execute(
                ionosphere_layers_matched_table.update(
                    ionosphere_layers_matched_table.c.id == match_id).
                values(validated=match_validated))
            connection.close()
            logger.info('updated validated for %s' % str(match_id))
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: could not update validated for %s ' % str(match_id)
            logger.error(fail_msg)
            if engine:
                engine_disposal(engine)
            raise

    # @added 20210414 - Feature #4014: Ionosphere - inference
    #                   Branch #3590: inference
    if validate_context == 'matched_motifs':
        try:
            motifs_matched_table, log_msg, trace = motifs_matched_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('motifs_matched_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: validate_ionosphere_match :: failed to get ionosphere_checked_table meta for match_id %s' % str(match_id))
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI
        try:
            connection = engine.connect()
            connection.execute(
                motifs_matched_table.update(
                    motifs_matched_table.c.id == match_id).
                values(validated=match_validated, user_id=user_id))
            connection.close()
            logger.info('updated validated in the motifs_matched for id %s' % str(match_id))
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: could not update validated for %s ' % str(match_id)
            logger.error(fail_msg)
            if engine:
                engine_disposal(engine)
            raise
        try:
            ionosphere_matched_table, log_msg, trace = ionosphere_matched_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('ionosphere_matched_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: validate_ionosphere_match :: failed to get ionosphere_checked_table meta for match_id %s' % str(match_id))
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI
        try:
            connection = engine.connect()
            connection.execute(
                ionosphere_matched_table.update(
                    ionosphere_matched_table.c.motifs_matched_id == int(match_id)).
                values(validated=match_validated, user_id=user_id))
            connection.close()
            logger.info('updated validated for match with motifs_matched_id %s by user id %s in the ionosphere_matched table' % (
                str(match_id), user_id))
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: could not update validated for ionosphere_matched record with matched_motif_id %s by user id %s' % (
                str(match_id), user_id)
            logger.error(fail_msg)
            # In test mode this can be expected
            # if engine:
            #     engine_disposal(engine)
            # raise

    # @added 20190921 - Feature #3234: Ionosphere - related matches vaildation
    # TODO - here related matches will also be validated

    if engine:
        engine_disposal(engine)

    return True


# @added 20200226: Ideas #2476: Label and relate anomalies
#                  Feature #2516: Add label to features profile
def label_anomalies(start_timestamp, end_timestamp, metrics, namespaces, label):
    """
    Label all anomalies from start_timestamp to end_timestamp for metrics and
    and metric namespaces with the given label.

    :param start_timestamp: the start timestamp
    :param end_timestamp: the end timestamp
    :param metrics: a list of metric names
    :param namespaces: a list of namespaces
    :type start_timestamp: int
    :type end_timestamp: int
    :type metrics: list
    :type namespace: list
    :return: boolean
    :rtype:  (list, list, list, list)

    """
    logger.info(
        'label_anomalies :: determine metric ids for label anomalies between %s and %s with label %s' % (
            str(start_timestamp), str(end_timestamp), str(label)))
    logger.info('getting MySQL engine')
    try:
        engine, fail_msg, trace = get_an_engine()
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('%s' % fail_msg)
        logger.error('error :: could not get a MySQL engine to get fp_ids')
        raise  # to webapp to return in the UI

    if not engine:
        trace = 'none'
        fail_msg = 'error :: engine not obtained'
        logger.error(fail_msg)
        raise

    try:
        metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('metrics_table OK')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: failed to get metrics_table meta')

        # @added 20170806 - Bug #2130: MySQL - Aborted_clients
        # Added missing disposal
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    metric_ids = []
    logger.info('label_anomalies :: %s metrics passed' % str(len(metrics)))
    for metric in metrics:
        try:
            connection = engine.connect()
            stmt = select([metrics_table]).where(metrics_table.c.metric == str(metric))
            result = connection.execute(stmt)
            for row in result:
                metric_id = row['id']
                metric_ids.append(int(metric_id))
            connection.close()
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: could not determine metric id from metrics for metric %s' % str(metric))
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal and raise
            if engine:
                engine_disposal(engine)
            raise
        logger.info('label_anomalies :: %s metric ids determined from passed namespaces' % str(len(metric_ids)))

    logger.info('label_anomalies :: %s namespaces passed' % str(len(namespaces)))

    if namespaces:
        metrics_like_query = text("""SELECT id FROM metrics WHERE metric LIKE :like_string""")
        for namespace in namespaces:
            try:
                # @added 20200425 - Ideas #2476: Label and relate anomalies
                # Added missing namespace_like
                namespace_str = namespace.rstrip('.')
                wildcard = '%'
                # @modified 20210703 - Ideas #2476: Label and relate anomalies
                # Do not append double wildcard if already specified
                # namespace_like = '%s.%s' % (namespace_str, wildcard)
                if '%' not in list(namespace_str):
                    namespace_like = '%s.%s' % (namespace_str, wildcard)
                else:
                    namespace_like = namespace_str

                connection = engine.connect()
                # @modified 20200425 - Ideas #2476: Label and relate anomalies
                # Use namespace_like
                # results = connection.execute(metrics_like_query, like_string=str(namespace))
                results = connection.execute(metrics_like_query, like_string=str(namespace_like))
                connection.close()
                for row in results:
                    metric_id = str(row[0])
                    metric_ids.append(int(metric_id))
            except:
                trace = traceback.format_exc()
                logger.error(trace)
                logger.error('error :: could not determine ids from metrics table')
                if engine:
                    engine_disposal(engine)
                raise
        logger.info('label_anomalies :: %s metric ids determined from passed namespaces' % str(len(metric_ids)))

    try:
        anomalies_table, log_msg, trace = anomalies_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('anomalies_table OK')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: failed to get anomalies_table meta')
        # @added 20170806 - Bug #2130: MySQL - Aborted_clients
        # Added missing disposal
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    anomaly_ids_to_label = []
    all_anomalies = []
    try:
        connection = engine.connect()
        stmt = select([anomalies_table]).\
            where(anomalies_table.c.anomaly_timestamp >= start_timestamp).\
            where(anomalies_table.c.anomaly_timestamp <= end_timestamp)
        result = connection.execute(stmt)
        for row in result:
            anomaly_id = row['id']
            metric_id = row['metric_id']
            all_anomalies.append(int(anomaly_id))
            if int(metric_id) in metric_ids:
                anomaly_ids_to_label.append(int(anomaly_id))
        connection.close()
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: could not determine anomaly ids')
        if engine:
            engine_disposal(engine)
        raise

    logger.info('label_anomalies :: a total of %s anomalies were found in the specific time period' % str(len(all_anomalies)))
    logger.info('label_anomalies :: %s of these anomalies were related to the specified metrics/namespaces' % str(len(anomaly_ids_to_label)))

    # Label anomalies
    for anomaly_id in anomaly_ids_to_label:
        try:
            connection = engine.connect()
            connection.execute(
                anomalies_table.update(
                    anomalies_table.c.id == int(anomaly_id)).
                values(label=label))
            connection.close()
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            logger.error('error :: could not update label for anomaly_id %s ' % str(anomaly_id))
            fail_msg = 'error :: could not update label for anomaly %s ' % str(anomaly_id)
            if engine:
                engine_disposal(engine)
            raise

    # @added 20210826 - Feature #4246: label_anomalies - update slack thread
    slack_threads_to_update_with_label = {}
    if update_slack_thread:
        for anomaly_id in anomaly_ids_to_label:
            try:
                connection = engine.connect()
                stmt = select([anomalies_table.c.slack_thread_ts]).\
                    where(anomalies_table.c.id == anomaly_id)
                result = connection.execute(stmt)
                for row in result:
                    slack_thread_ts = row['slack_thread_ts']
                    break
                if slack_thread_ts > 0:
                    message = '*LABELLED* - %s' % str(label)
                    slack_threads_to_update_with_label[slack_thread_ts] = message
                connection.close()
            except:
                trace = traceback.format_exc()
                logger.error(trace)
                logger.error('error :: could not determine slack_thread_ts or create slack thread update message for anomaly_id %s ' % str(anomaly_id))
    for slack_thread_ts in list(slack_threads_to_update_with_label.keys()):
        slack_response = {'ok': False}
        message = slack_threads_to_update_with_label[slack_thread_ts]
        try:
            slack_response = slack_post_message(skyline_app, channel_id, str(slack_thread_ts), message)
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: label_anomalies :: failed to slack_post_message - %s' % message
            logger.error('%s' % fail_msg)
        if not slack_response['ok']:
            fail_msg = 'error :: label_anomalies :: failed to slack_post_message, slack dict output follows'
            logger.error('%s' % fail_msg)
            logger.error('%s' % str(slack_response))
        else:
            logger.info('label_anomalies :: posted slack update to %s, anomaly_id %s labelled - %s' % (
                channel_id, str(anomaly_id), message))

    if engine:
        engine_disposal(engine)

    return True, len(anomaly_ids_to_label)


# @added 20201213 - Feature #3890: metrics_manager - sync_cluster_files
def expected_features_profiles_dirs():
    """
    Generate a dict of all features_profiles ids and directories that SHOULD
    exist.

    :rtype:  dict

    """

    try:
        engine, fail_msg, trace = get_an_engine()
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('%s' % fail_msg)
        logger.error('error :: could not get a MySQL engine to get fp_ids')
        raise  # to webapp to return in the UI
    if not engine:
        trace = 'none'
        fail_msg = 'error :: engine not obtained'
        logger.error(fail_msg)
        raise
    try:
        ionosphere_table, log_msg, trace = ionosphere_table_meta(skyline_app, engine)
        logger.info(log_msg)
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: failed to get ionosphere_table meta for features_profiles_dirs')
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    last_fp_id = 0
    try:
        connection = engine.connect()
        stmt = select([ionosphere_table]).where(ionosphere_table.c.id > 0).order_by(ionosphere_table.c.id.desc()).limit(1)
        result = connection.execute(stmt)
        for row in result:
            last_fp_id = int(row['id'])
        connection.close()
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: could not determine details of features profiles from ionosphere table for features_profiles_dirs')
        if engine:
            engine_disposal(engine)
        raise
    logger.info('db reports last_fp_id of %s for features_profile_dirs_dict' % str(last_fp_id))

    redis_conn_decoded = None
    try:
        redis_conn_decoded = get_redis_conn_decoded(skyline_app)
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: get_redis_conn_decoded failed for features_profiles_dirs')

    features_profile_dirs_dict = {}
    if redis_conn_decoded:
        try:
            features_profile_dirs_dict = redis_conn_decoded.hgetall('analyzer.metrics_manager.local_features_profile_dirs')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to get Redis hash key analyzer.metrics_manager.local_features_profile_dirs for features_profiles_dirs')
            features_profile_dirs_dict = {}
    if not features_profile_dirs_dict:
        logger.info('no features_profile_dirs_dict found for features_profiles_dirs from analyzer.metrics_manager.local_features_profile_dirs')
    else:
        logger.info('features_profile_dirs_dict found with %s features_profiles_dirs from analyzer.metrics_manager.local_features_profile_dirs' % str(len(features_profile_dirs_dict)))

    last_fp_dir = None
    if last_fp_id and features_profile_dirs_dict:
        try:
            last_fp_dir = features_profile_dirs_dict[last_fp_id]
        except:
            last_fp_dir = None
    if last_fp_dir:
        logger.info('features_profiles_dirs - no new features profile ids, all known return %s items' % str(len(features_profile_dirs_dict)))
        return features_profile_dirs_dict
    else:
        logger.info('features_profiles_dirs - new features profile ids found determing new dirs, %s currently known dirs' % str(len(features_profile_dirs_dict)))

    fps = {}
    try:
        connection = engine.connect()
        stmt = select([ionosphere_table]).where(ionosphere_table.c.id > 0)
        result = connection.execute(stmt)
        for row in result:
            fp_id = int(row['id'])
            fp_metric_id = int(row['metric_id'])
            fp_anomaly_timestamp = int(row['anomaly_timestamp'])
            fps[fp_id] = {}
            fps[fp_id]['metric_id'] = fp_metric_id
            fps[fp_id]['anomaly_timestamp'] = fp_anomaly_timestamp
        connection.close()
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: could not determine details of features profiles from ionosphere table for features_profiles_dirs')
        if engine:
            engine_disposal(engine)
        raise
    logger.info('features_profiles_dirs - db responded with %s fps' % str(len(fps)))
    try:
        metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('metrics_table OK')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: failed to get metrics_table meta for features_profiles_dirs')
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI
    metrics = {}
    try:
        connection = engine.connect()
        stmt = select([metrics_table]).where(metrics_table.c.id > 0)
        result = connection.execute(stmt)
        for row in result:
            metric_id = row['id']
            metric_name = row['metric']
            metrics[metric_id] = {}
            metrics[metric_id]['metric'] = metric_name
        connection.close()
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: could not determine metric ids from metrics for features_profiles_dirs')
        if engine:
            engine_disposal(engine)
        raise
    logger.info('features_profiles_dirs - db responded with %s metrics' % str(len(metrics)))

    # features_profiles_dirs_error_logged = False
    added_fps = 0
    if fps and metrics:
        for fp_id in fps:
            fp_dir = None
            if features_profile_dirs_dict:
                try:
                    fp_id_str = str(fp_id)
                    fp_dir = features_profile_dirs_dict[fp_id_str]
                except:
                    # if not features_profiles_dirs_error_logged:
                    #     logger.error(traceback.format_exc())
                    #     logger.error('error :: could not determine fp_dir for fp_id %s' % str(fp_id))
                    #     features_profiles_dirs_error_logged = True
                    fp_dir = None
                    pass
            if not fp_dir:
                try:
                    metric_id = fps[fp_id]['metric_id']
                    metric = metrics[metric_id]['metric']
                    timestamp = fps[fp_id]['anomaly_timestamp']
                    metric_timeseries_dir = metric.replace('.', '/')
                    fp_dir = '%s/%s/%s' % (
                        settings.IONOSPHERE_PROFILES_FOLDER, metric_timeseries_dir,
                        str(timestamp))
                    features_profile_dirs_dict[fp_id] = fp_dir
                    added_fps += 1
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: features_profiles_dirs failed to fp_dir to features_profile_dirs_dict')
    logger.info('added %s new items to features_profiles_dirs, returning %s items' % (
        str(added_fps), str(len(features_profile_dirs_dict))))
    return features_profile_dirs_dict


# @added 20210413 - Feature #4014: Ionosphere - inference
#                   Branch #3590: inference
def get_matched_motifs(
        metric, metric_like, from_timestamp, until_timestamp, sort_by):
    """
    Get all motif matches.

    :param metric: all or the metric name
    :param metric_like: False or the metric MySQL like string e.g statsd.%
    :param from_timestamp: timestamp or None
    :param until_timestamp: timestamp or None
    :param limit: None or number to limit to
    :return: list
    :rtype: list

    """
    logger = logging.getLogger(skyline_app_logger)

    function_str = 'ionoshere_backend.py :: get_motif_matches'
    logger.info('%s :: with parameters :: %s, %s, %s, %s' % (
        function_str, str(metric), str(metric_like), str(from_timestamp),
        str(until_timestamp)))
    trace = 'none'
    fail_msg = 'none'

    dev_null = None
    logger.info('%s :: getting MySQL engine' % function_str)
    try:
        engine, fail_msg, trace = get_an_engine()
        logger.info(fail_msg)
    except Exception as e:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine - %s' % (function_str, e)
        logger.error('%s' % fail_msg)
        return False, fail_msg, trace

    if not engine:
        trace = 'none'
        fail_msg = 'error :: %s :: engine not obtained' % function_str
        logger.error(fail_msg)
        return False, fail_msg, trace

    query_string = 'SELECT * FROM motifs_matched'
    needs_and = False

    if metric and metric != 'all':
        metric_id = None
        logger.info('%s :: metric set to %s' % (function_str, str(metric)))
        try:
            metric_id = metric_id_from_base_name(skyline_app, metric)
        except Exception as e:
            logger.error('error :: %s :: failed to get metric id from db: %s' % e)
            if engine:
                engine_disposal(engine)
            return False, fail_msg, trace

        fp_ids_stmt = 'SELECT id FROM ionosphere WHERE metric_id=%s' % str(metric_id)
        fp_ids = ''
        try:
            connection = engine.connect()
            results = connection.execute(fp_ids_stmt)
            connection.close()
            for row in results:
                fp_id = str(row[0])
                if fp_ids == '':
                    fp_ids = '%s' % (fp_id)
                else:
                    new_fp_ids = '%s, %s' % (fp_ids, fp_id)
                    fp_ids = new_fp_ids
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            logger.error('error :: %s :: could not determine fp ids from ionosphere table' % function_str)
            dev_null = e
            if engine:
                engine_disposal(engine)
            return False, fail_msg, trace
        logger.info('%s :: fp_ids set to %s' % (function_str, str(fp_ids)))
        query_string = 'SELECT * FROM motifs_matched WHERE fp_id in (%s)' % str(fp_ids)
        needs_and = True

    if metric_like:
        if metric_like and metric_like != 'all':
            db_metric_ids = None
            try:
                db_metric_ids = metric_ids_from_metric_like(skyline_app, metric_like)
            except Exception as e:
                logger.error('error :: %s :: failed to get metric ids from metric_ids_from_metric_like: %s' % (function_str, e))
                return False

            metric_ids = '0'
            if db_metric_ids:
                metric_ids = ''
                for db_metric_id in db_metric_ids:
                    if metric_ids == '':
                        metric_ids = '%s' % str(db_metric_id)
                    else:
                        metric_ids = '%s, %s' % (metric_ids, str(db_metric_id))
            else:
                # Get nothing
                metric_ids = '0'

            fp_ids_stmt = 'SELECT id FROM ionosphere WHERE metric_id IN (%s)' % str(metric_ids)
            fp_ids = ''
            try:
                connection = engine.connect()
                results = connection.execute(fp_ids_stmt)
                connection.close()
                for row in results:
                    fp_id = str(row[0])
                    if fp_ids == '':
                        fp_ids = '%s' % (fp_id)
                    else:
                        new_fp_ids = '%s, %s' % (fp_ids, fp_id)
                        fp_ids = new_fp_ids
            except Exception as e:
                trace = traceback.format_exc()
                logger.error(trace)
                logger.error('error :: %s :: could not determine id from metrics table' % function_str)
                # Disposal and return False, fail_msg, trace for Bug #2130: MySQL - Aborted_clients
                dev_null = e
                if engine:
                    engine_disposal(engine)
                return False, fail_msg, trace
            query_string = 'SELECT * FROM motifs_matched WHERE fp_id in (%s)' % str(fp_ids)
            needs_and = True

    if 'from_timestamp' in request.args:
        from_timestamp = request.args.get('from_timestamp', None)
        if from_timestamp and from_timestamp != 'all':
            if ":" in from_timestamp:
                import datetime
                new_from_timestamp = time.mktime(datetime.datetime.strptime(from_timestamp, '%Y%m%d %H:%M').timetuple())
                from_timestamp = str(int(new_from_timestamp))

            # @added 20190530 - Branch #3002: docker
            # Fix search so that if timestamps are passed and there is a list of
            # fp ids the SQL does not break
            try:
                if len(fp_ids) > 0:
                    needs_and = True
            except Exception as e:
                logger.info('%s :: fp_ids length unknown, OK' % function_str)
                dev_null = e

            if needs_and:
                new_query_string = '%s AND metric_timestamp >= %s' % (query_string, from_timestamp)
                query_string = new_query_string
                needs_and = True
            else:
                new_query_string = '%s WHERE metric_timestamp >= %s' % (query_string, from_timestamp)
                query_string = new_query_string
                needs_and = True

    if 'until_timestamp' in request.args:
        until_timestamp = request.args.get('until_timestamp', None)
        if until_timestamp and until_timestamp != 'all':
            if ":" in until_timestamp:
                import datetime
                new_until_timestamp = time.mktime(datetime.datetime.strptime(until_timestamp, '%Y%m%d %H:%M').timetuple())
                until_timestamp = str(int(new_until_timestamp))

            if needs_and:
                new_query_string = '%s AND metric_timestamp <= %s' % (query_string, until_timestamp)
                query_string = new_query_string
                needs_and = True
            else:
                new_query_string = '%s WHERE metric_timestamp <= %s' % (query_string, until_timestamp)
                query_string = new_query_string
                needs_and = True

    if 'validated_equals' in request.args:
        filter_matches = False
        validated_equals = request.args.get('validated_equals', 'any')
        if validated_equals == 'any':
            filter_matches = False
        if validated_equals == 'true':
            filter_match_validation = 1
            filter_matches = True
        if validated_equals == 'false':
            filter_match_validation = 0
            filter_matches = True
        if validated_equals == 'invalid':
            filter_match_validation = 2
            filter_matches = True
        if filter_matches:
            if needs_and:
                new_query_string = '%s AND validated = %s' % (query_string, str(filter_match_validation))
                query_string = new_query_string
                needs_and = True
            else:
                new_query_string = '%s WHERE validated = %s' % (query_string, str(filter_match_validation))
                query_string = new_query_string
                needs_and = True

    if 'primary_match' in request.args:
        filter_matches = False
        primary_match = request.args.get('primary_match', 'any')
        if primary_match == 'any':
            filter_matches = False
        if primary_match == 'true':
            filter_primary_validation = 1
            filter_matches = True
        if primary_match == 'false':
            filter_primary_validation = 0
            filter_matches = True
        if filter_matches:
            if needs_and:
                new_query_string = '%s AND primary_match = %s' % (query_string, str(filter_primary_validation))
                query_string = new_query_string
                needs_and = True
            else:
                new_query_string = '%s WHERE primary_match = %s' % (query_string, str(filter_primary_validation))
                query_string = new_query_string
                needs_and = True

    # @added 20210415 - Feature #4014: Ionosphere - inference
    #                   Branch #3590: inference
    # Allow to sort_by, to enable sorting by distance as the larger the distance
    # the less similar
    if sort_by:
        new_query_string = '%s ORDER BY %s' % (query_string, sort_by)
        query_string = new_query_string

    ordered_by = 'ASC'
    if 'order_by' in request.args:
        order = request.args.get('order', 'DESC')
        if str(order) == 'DESC':
            ordered_by = 'DESC'
        if str(order) == 'ASC':
            ordered_by = 'ASC'

    # @modified 20210421 - Task #4030: refactoring
    # semgrep - python.lang.correctness.useless-comparison.no-strings-as-booleans
    # if ordered_by:
    use_ordered_by = True
    if use_ordered_by:
        if not sort_by:
            new_query_string = '%s ORDER BY id %s' % (query_string, ordered_by)
            query_string = new_query_string
        else:
            new_query_string = '%s %s' % (query_string, ordered_by)
            query_string = new_query_string

    limit = 100
    if 'limit' in request.args:
        limit = request.args.get('limit', '100')
        try:
            test_limit = int(limit) + 0
            if int(limit) != 0:
                new_query_string = '%s LIMIT %s' % (query_string, str(limit))
                query_string = new_query_string
                logger.info('%s :: test_limit tested OK with %s' % (function_str, (test_limit)))
        except Exception as e:
            logger.error('error :: %s :: limit is not an integer - %s - %s' % (function_str, str(limit), e))
            new_query_string = '%s LIMIT 100' % (query_string)

    # Get ionosphere_summary memcache object from which metric names will be
    # determined
    memcache_result = None
    ionosphere_summary_list = None
    if settings.MEMCACHE_ENABLED:
        memcache_client = pymemcache_Client((settings.MEMCACHED_SERVER_IP, settings.MEMCACHED_SERVER_PORT), connect_timeout=0.1, timeout=0.2)
    else:
        memcache_client = None
    if settings.MEMCACHE_ENABLED:
        try:
            memcache_result = memcache_client.get('ionosphere_summary_list').decode('utf-8')
        except Exception as e:
            # @modified 20200507 - stop reporting this as an error
            # it can be expected to happen from time to time
            # logger.error('error :: failed to get ionosphere_summary_list from memcache')
            logger.info('%s :: failed to get ionosphere_summary_list from memcache, will query DB' % function_str)
            dev_null = e
        try:
            memcache_client.close()
        except Exception as e:
            dev_null = e

    if memcache_result:
        try:
            logger.info('%s :: using memcache ionosphere_summary_list key data' % function_str)
            ionosphere_summary_list = literal_eval(memcache_result)
        except Exception as e:
            logger.error('error :: %s :: failed to process data from memcache key - ionosphere_summary_list - %s' % (function_str, e))
            ionosphere_summary_list = False
        try:
            del memcache_result
        except Exception as e:
            dev_null = e
    if not ionosphere_summary_list:
        stmt = "SELECT ionosphere.id, ionosphere.metric_id, metrics.metric FROM ionosphere INNER JOIN metrics ON ionosphere.metric_id=metrics.id"
        try:
            connection = engine.connect()
            results = connection.execute(stmt)
            connection.close()
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            logger.error('error :: %s :: could not determine metrics from metrics table' % function_str)
            dev_null = e
            # Disposal and raise for Bug #2130: MySQL - Aborted_clients
            if engine:
                engine_disposal(engine)
            return False, fail_msg, trace
        if results:
            # Because the each row in the results is a dict and all the rows are
            # being used, these are being converted into a list and stored in
            # memcache as a list
            ionosphere_summary_list = []
            for row in results:
                ionosphere_summary_list.append([int(row['id']), int(row['metric_id']), str(row['metric'])])
            if settings.MEMCACHE_ENABLED:
                try:
                    memcache_client.set('ionosphere_summary_list', ionosphere_summary_list, expire=600)
                    logger.info('%s :: set memcache ionosphere_summary_list key with DB results' % function_str)
                except Exception as e:
                    logger.error('error :: %s :: failed to get ionosphere_summary_list from memcache - %s' % (function_str, e))
                try:
                    memcache_client.close()
                except Exception as e:
                    dev_null = e

    matched_motifs = {}
    metric_list = []
    get_motifs_matched = True
    if get_motifs_matched:
        try:
            connection = engine.connect()
            stmt = query_string
            logger.info('%s :: executing %s' % (function_str, stmt))
            results = connection.execute(stmt)
            connection.close()
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            logger.error('error :: %s :: could not determine motif matches from motifs_matched table' % function_str)
            dev_null = e
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal and raise
            if engine:
                engine_disposal(engine)
            return False, fail_msg, trace

        for row in results:
            try:
                fp_id = int(row['fp_id'])
                try:
                    metric_list = [row[2] for row in ionosphere_summary_list if row[0] == fp_id]
                    metric = metric_list[0]
                except Exception as e:
                    metric = 'UNKNOWN'
                    dev_null = e
                match_id = int(row['id'])
                matched_motifs[match_id] = {}
                matched_motifs[match_id]['metric'] = metric
                matched_motifs[match_id]['metric_id'] = int(row['metric_id'])
                matched_motifs[match_id]['fp_id'] = fp_id
                metric_timestamp = int(row['metric_timestamp'])
                matched_motifs[match_id]['metric_timestamp'] = metric_timestamp
                matched_motifs[match_id]['human_date'] = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(metric_timestamp)))
                matched_motifs[match_id]['primary_match'] = int(row['primary_match'])
                matched_motifs[match_id]['index'] = int(row['index'])
                matched_motifs[match_id]['size'] = int(row['size'])
                matched_motifs[match_id]['distance'] = float(row['distance'])
                matched_motifs[match_id]['type_id'] = int(row['type_id'])
                matched_motifs[match_id]['validated'] = int(row['validated'])
                # @added 20210428 -
                matched_motifs[match_id]['motif_area'] = float(row['motif_area'])
                matched_motifs[match_id]['fp_motif_area'] = float(row['fp_motif_area'])
                matched_motifs[match_id]['area_percent_diff'] = float(row['area_percent_diff'])
                matched_motifs[match_id]['fps_checked'] = int(row['fps_checked'])
                matched_motifs[match_id]['runtime'] = float(row['runtime'])
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: %s :: could not add motif match to matched_motifs' % function_str)
                dev_null = e

    logger.info('%s :: returning %s matched_motifs' % (function_str, str(len(matched_motifs))))
    if engine:
        engine_disposal(engine)

    try:
        del ionosphere_summary_list
    except Exception as e:
        dev_null = e
    try:
        del metric_list
    except Exception as e:
        dev_null = e
    try:
        del results
    except Exception as e:
        dev_null = e
    if dev_null:
        del dev_null
    return matched_motifs, fail_msg, trace


# @added 20210419 - Feature #4014: Ionosphere - inference
def get_matched_motif_id(fp_id, timestamp, index, size):
    """
    Return the matched_motif_id, motif_validated, ionosphere_matched_id for a
    motif
    """

    logger = logging.getLogger(skyline_app_logger)

    function_str = 'get_matched_motif_id'
    logger.info('%s :: with parameters :: fp_id: %s, timestamp: %s, index: %s, size: %s' % (
        function_str, str(fp_id), str(timestamp), str(index), str(size)))
    matched_motif_id = None
    motif_validated = None
    ionosphere_matched_id = None
    try:
        query = 'SELECT id,validated FROM motifs_matched WHERE fp_id=%s AND metric_timestamp=%s AND `index`=%s AND size=%s' % (
            str(fp_id), str(timestamp), str(index), str(size))
        results = mysql_select(skyline_app, query)
        for result in results:
            matched_motif_id = int(result[0])
            motif_validated = int(result[1])
    except Exception as e:
        logger.error('error :: %s :: failed to get motifs_matched id and validated from the database for fp_id %s - %s' % (
            function_str, (fp_id), e))
    if matched_motif_id:
        try:
            query = 'SELECT id FROM ionosphere_matched WHERE fp_id=%s AND metric_timestamp=%s AND motifs_matched_id=%s' % (
                str(fp_id), str(timestamp), str(matched_motif_id))
            results = mysql_select(skyline_app, query)
            for result in results:
                ionosphere_matched_id = int(result[0])
        except Exception as e:
            logger.error('error :: %s :: failed to get ionosphere_matched_id from the database for fp_id %s - %s' % (
                function_str, (fp_id), e))
    return matched_motif_id, motif_validated, ionosphere_matched_id
