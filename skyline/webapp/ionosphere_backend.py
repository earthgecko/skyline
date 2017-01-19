from __future__ import division
import logging
from os import path, walk, listdir, remove
import string
import operator
import time
import re
import csv
import datetime
import shutil
import glob
from ast import literal_eval

import traceback
from flask import request
import requests
from redis import StrictRedis

from sqlalchemy import (
    create_engine, Column, Table, Integer, String, MetaData, DateTime)
from sqlalchemy.dialects.mysql import DOUBLE, TINYINT
from sqlalchemy.sql import select
import json
from tsfresh import __version__ as tsfresh_version

import settings
import skyline_version
from skyline_functions import (
    RepresentsInt, mkdir_p, write_data_to_file, get_graphite_metric)
from tsfresh_feature_names import TSFRESH_FEATURES

from database import (
    get_engine, ionosphere_table_meta, metrics_table_meta,
    ionosphere_matched_table_meta)

skyline_version = skyline_version.__absolute_version__
skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
try:
    ENABLE_WEBAPP_DEBUG = settings.ENABLE_WEBAPP_DEBUG
except:
    logger.error('error :: cannot determine ENABLE_WEBAPP_DEBUG from settings')
    ENABLE_WEBAPP_DEBUG = False

try:
    full_duration_seconds = int(settings.FULL_DURATION)
except:
    full_duration_seconds = 86400

full_duration_in_hours = full_duration_seconds / 60 / 60
exclude_redis_json = 'redis.%sh.json' % str(int(full_duration_in_hours))


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
                    metric_file = path.join(root, file)
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
    base_name = data_for_metric.replace(settings.FULL_NAMESPACE, '', 1)
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
            data_dir = '%s/%s' % (
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
        array_keys = ['algorithms', 'triggered_algorithms']
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

    base_name = data_for_metric.replace(settings.FULL_NAMESPACE, '', 1)
    if context == 'training_data':
        log_context = 'training data'
    if context == 'features_profiles':
        log_context = 'features profile data'
    logger.info('%s requested for %s at %s' % (
        context, str(base_name), str(requested_timestamp)))
    metric_paths = []
    images = []
    timeseries_dir = base_name.replace('.', '/')
    if context == 'training_data':
        metric_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, str(requested_timestamp),
            timeseries_dir)
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

    human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(requested_timestamp)))
    metric_var_filename = '%s.txt' % str(base_name)
    metric_vars_file = False
    ts_json_filename = '%s.json' % str(base_name)
    ts_json_file = 'none'
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
            if append_image:
                images.append(str(metric_file))
        if i_file == metric_var_filename:
            metric_vars_file = str(metric_file)
        if i_file == ts_json_filename:
            ts_json_file = str(metric_file)

    metric_vars_ok = False
    metric_vars = ['error: could not read metrics vars file', metric_vars_file]
    if path.isfile(metric_vars_file):
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
            metric_vars_ok = False
            logger.error(traceback.format_exc())
            logger.error('error :: failed to load metric_vars from: %s' % str(metric_vars_file))

    ts_json_ok = False
    ts_json = ['error: could not timeseries json file', ts_json_file]
    if path.isfile(ts_json_file):
        try:
            ts_json = []
            with open(ts_json_file) as f:
                for line in f:
                    ts_json.append(line)

            ts_json_ok = True
        except:
            ts_json_ok = False

    data_to_process = False
    if metric_vars_ok and ts_json_ok:
        data_to_process = True
    panorama_anomaly_id = False
    url = '%s/panorama?metric=%s&from_timestamp=%s&until_timestamp=%s&panorama_anomaly_id=true' % (settings.SKYLINE_URL, str(base_name), str(requested_timestamp), str(requested_timestamp))
    panorama_resp = None
    logger.info('getting anomaly id from panorama: %s' % str(url))
    if settings.WEBAPP_AUTH_ENABLED:
        user = str(settings.WEBAPP_AUTH_USER)
        password = str(settings.WEBAPP_AUTH_USER_PASSWORD)
    try:
        if settings.WEBAPP_AUTH_ENABLED:
            r = requests.get(url, timeout=2, auth=(user, password))
        else:
            r = requests.get(url, timeout=2)
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
    graph_resolutions = []
    # @modified 20170116 - Feature #1854: Ionosphere learn - generations
    #                      Feature #1842: Ionosphere - Graphite now graphs
    # Also include the Graphite NOW graphs in the features_profile page as
    # graphs WHEN CREATED
    # if context == 'training_data':
    if context == 'training_data' or context == 'features_profiles':
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
            fail_msg = 'error :: could not get a MySQL engine'
            logger.error('%s' % fail_msg)
            raise

        if not engine:
            trace = 'none'
            fail_msg = 'error :: engine not obtained'
            logger.error(fail_msg)
            raise

        logger.info('getting MySQL engine')
        try:
            engine, log_msg, trace = get_an_engine()
            logger.info(log_msg)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: could not get a MySQL engine to get fp_ids')
            raise  # to webapp to return in the UI

        if not engine:
            logger.error('error :: engine not obtained to get fp_ids')
            raise  # to webapp to return in the UI

        try:
            ionosphere_matched_table, log_msg, trace = ionosphere_matched_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('ionosphere_matched_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to get ionosphere_checked_table meta for %s' % base_name)
            raise  # to webapp to return in the UI

        matched_timestamps = []

        # @added 20170107 - Feature #1852: Ionosphere - features_profile matched graphite graphs
        # Added details of match anomalies for verification added to tsfresh_version
        all_calc_features_sum = None
        all_calc_features_count = None
        sum_common_values = None
        common_features_count = None
        # That is more than it looks...

        try:
            connection = engine.connect()
            stmt = select([ionosphere_matched_table]).where(ionosphere_matched_table.c.fp_id == int(fp_id))
            result = connection.execute(stmt)
            for row in result:
                matched_timestamp = row['metric_timestamp']
                matched_timestamps.append(int(matched_timestamp))
                logger.info('found matched_timestamp %s' % (str(matched_timestamp)))
            connection.close()
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: could not determine timestamps from ionosphere_matched for fp_id %s' % str(fp_id))

        len_matched_timestamps = len(matched_timestamps)
        matched_count = len_matched_timestamps
        logger.info('determined %s matched timestamps for fp_id %s' % (str(len_matched_timestamps), str(fp_id)))

        last_matched_timestamps = []
        if len_matched_timestamps > 0:
            last_graph_timestamp = int(time.time())
            skip_if_last_graph_timestamp_less_than = 600
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

        for matched_timestamp in last_matched_timestamps:
            # Get Graphite images
            graph_image = False
            try:
                key = 'full_duration'
                value_list = [var_array[1] for var_array in metric_vars if var_array[0] == key]
                full_duration = int(value_list[0])
                from_timestamp = str(int(matched_timestamp) - int(full_duration))
                until_timestamp = str(matched_timestamp)
                graph_image_file = '%s/%s.matched.fp_id-%s.%s.png' % (metric_data_dir, base_name, str(fp_id), str(matched_timestamp))
                logger.info('getting Graphite graph fpr fp_id %s matched timeseries from_timestamp - %s, until_timestamp - %s' % (str(fp_id), str(from_timestamp), str(until_timestamp)))
                graph_image = get_graphite_metric(
                    skyline_app, base_name, from_timestamp, until_timestamp, 'image',
                    graph_image_file)
                if graph_image:
                    graphite_matched_images.append(graph_image_file)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get Graphite graph for fp_id %s at %s' % (str(fp_id), str(matched_timestamp)))

        if engine:
            engine_disposal(engine)

    return (
        metric_paths, images, human_date, metric_vars, ts_json, data_to_process,
        panorama_anomaly_id, graphite_now_images, graphite_matched_images,
        matched_count)

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

    :param fp_id: the t feautres profile id
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
        fp_details_object = row
        connection.close()
        tsfresh_version = row['tsfresh_version']
        calc_time = row['calc_time']
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
        fp_details = '''
tsfresh_version   :: %s | calc_time :: %s
features_count    :: %s
features_sum      :: %s
deleted           :: %s
matched_count     :: %s
last_matched      :: %s | human_date :: %s
created_timestamp :: %s
full_duration     :: %s
checked_count     :: %s
last_checked      :: %s | human_date :: %s
parent_id         :: %s | generation :: %s
''' % (str(tsfresh_version), str(calc_time), str(features_count),
            str(features_sum), str(deleted), str(matched_count),
            str(last_matched), str(human_date), str(created_timestamp),
            str(full_duration), str(checked_count), str(last_checked),
            str(checked_human_date), str(parent_id), str(generation))
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
def ionosphere_search_defaults(get_options):

    """
    Get the default values to populate the search_features_profiles.html options

    :param get_options: list of options to get
    :type get_options: list of strings
    :return: tuple
    :rtype:  (list, list, list, list)

    """
    logger = logging.getLogger(skyline_app_logger)

    function_str = 'ionoshere_backend.py :: ionosphere_search_defaults'

    trace = 'none'
    fail_msg = 'none'

    full_duration_list = []
    enabled_list = []
    tsfresh_version_list = []
    generation_list = []

    possible_options = [
        'full_duration', 'enabled', 'tsfresh_version', 'generation']

    engine_needed = False
    for possible_option in possible_options:
        if possible_option in get_options:
            engine_needed = True
            logger.info('%s :: options for %s required from database' % (function_str, possible_option))

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

    for required_option in get_options:
        if engine_needed and engine:
            all_list = []
            try:
                stmt = 'SELECT %s FROM ionosphere WHERE enabled=1' % str(required_option)
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

    return full_duration_list, enabled_list, tsfresh_version_list, generation_list, fail_msg, trace
