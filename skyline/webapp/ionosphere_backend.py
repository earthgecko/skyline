from __future__ import division
import logging
from os import path, walk, listdir, remove
# import string
import operator
import time
import re
# import csv
# import datetime
# import shutil
# import glob
from ast import literal_eval

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

import settings
import skyline_version
# from skyline_functions import (
#    RepresentsInt, mkdir_p, write_data_to_file, get_graphite_metric)
from skyline_functions import (mkdir_p, get_graphite_metric)
# from tsfresh_feature_names import TSFRESH_FEATURES

from database import (
    get_engine, ionosphere_table_meta, metrics_table_meta,
    ionosphere_matched_table_meta,
    # @added 20170305 - Feature #1960: ionosphere_layers
    ionosphere_layers_table_meta, layers_algorithms_table_meta,
    # @added 20170307 - Feature #1960: ionosphere_layers
    # To present matched layers Graphite graphs
    ionosphere_layers_matched_table_meta
)

skyline_version = skyline_version.__absolute_version__
skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
try:
    ENABLE_WEBAPP_DEBUG = settings.ENABLE_WEBAPP_DEBUG
except EnvironmentError as err:
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

    # TODO
    # Make a sample ts for lite frontend

    ts_json_ok = False
    ts_json = ['error: no timeseries json file', ts_json_file]
    if path.isfile(ts_json_file):
        try:
            ts_json = []
            with open(ts_json_file) as f:
                for line in f:
                    ts_json.append(line)

            ts_json_ok = True
        except:
            ts_json_ok = False

    # @added 20170309 - Feature #1960: ionosphere_layers
    # Also return the Analyzer FULL_DURATION timeseries if available in a Mirage
    # based features profile
    ionosphere_json_ok = False
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
            ionosphere_json_ok = True
            # @added 20170331 - Task #1988: Review - Ionosphere layers - always show layers
            #                   Feature #1960: ionosphere_layers
            # Return the anomalous_timeseries as an array to sample
            with open((ionosphere_json_file), 'r') as f:
                raw_timeseries = f.read()
            timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
            anomalous_timeseries = literal_eval(timeseries_array_str)
        except:
            ionosphere_json_ok = False

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
        fp_details_list)


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
        # @added 20170402 - Feature #2000: Ionosphere - validated
        validated = row['validated']
        # @added 20170305 - Feature #1960: ionosphere_layers
        layers_id = row['layers_id']
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
parent_id         :: %s | generation :: %s | validated :: %s
layers_id         :: %s
''' % (str(tsfresh_version), str(calc_time), str(features_count),
            str(features_sum), str(deleted), str(matched_count),
            str(last_matched), str(human_date), str(created_timestamp),
            str(full_duration), str(checked_count), str(last_checked),
            str(checked_human_date), str(parent_id), str(generation),
            str(validated), str(layers_id))
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

    logger.info('determining search parameters')
    query_string = 'SELECT * FROM ionosphere'
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
            else:
                new_query_string = 'SELECT * FROM ionosphere WHERE metric_id=REPLACE_WITH_METRIC_ID'
                query_string = new_query_string

    if 'from_timestamp' in request.args:
        from_timestamp = request.args.get('from_timestamp', None)
        if from_timestamp and from_timestamp != 'all':

            if ":" in from_timestamp:
                new_from_timestamp = time.mktime(datetime.datetime.strptime(from_timestamp, '%Y%m%d %H:%M').timetuple())
                from_timestamp = str(int(new_from_timestamp))

            if needs_and:
                new_query_string = '%s AND anomaly_timestamp >= %s' % (query_string, from_timestamp)
                query_string = new_query_string
                needs_and = True
            else:
                new_query_string = '%s WHERE anomaly_timestamp >= %s' % (query_string, from_timestamp)
                query_string = new_query_string
                needs_and = True

    if 'until_timestamp' in request.args:
        until_timestamp = request.args.get('until_timestamp', None)
        if until_timestamp and until_timestamp != 'all':
            if ":" in until_timestamp:
                new_until_timestamp = time.mktime(datetime.datetime.strptime(until_timestamp, '%Y%m%d %H:%M').timetuple())
                until_timestamp = str(int(new_until_timestamp))

            if needs_and:
                new_query_string = '%s AND anomaly_timestamp <= %s' % (query_string, until_timestamp)
                query_string = new_query_string
                needs_and = True
            else:
                new_query_string = '%s WHERE anomaly_timestamp <= %s' % (query_string, until_timestamp)
                query_string = new_query_string
                needs_and = True

    if 'generation_greater_than' in request.args:
        generation_greater_than = request.args.get('generation_greater_than', None)
        if generation_greater_than and generation_greater_than != '0':
            if needs_and:
                new_query_string = '%s AND generation > %s' % (query_string, generation_greater_than)
                query_string = new_query_string
                needs_and = True
            else:
                new_query_string = '%s WHERE generation > %s' % (query_string, generation_greater_than)
                query_string = new_query_string
                needs_and = True

    # @added 20170315 - Feature #1960: ionosphere_layers
    if 'layers_id_greater_than' in request.args:
        layers_id_greater_than = request.args.get('layers_id_greater_than', None)
        if layers_id_greater_than and layers_id_greater_than != '0':
            if needs_and:
                new_query_string = '%s AND layers_id > %s' % (query_string, layers_id_greater_than)
                query_string = new_query_string
                needs_and = True
            else:
                new_query_string = '%s WHERE layers_id > %s' % (query_string, layers_id_greater_than)
                query_string = new_query_string
                needs_and = True

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
            else:
                new_query_string = '%s WHERE %s' % (query_string, validate_string)
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
        if count_request and search_query:
            new_query_string = '%s %s' % (query_string, ordered_by)
        else:
            new_query_string = '%s ORDER BY id %s' % (query_string, ordered_by)
        query_string = new_query_string

    if 'limit' in request.args:
        limit = request.args.get('limit', '30')
        try:
            test_limit = int(limit) + 0
            if int(limit) != 0:
                new_query_string = '%s LIMIT %s' % (query_string, str(limit))
                query_string = new_query_string
        except:
            logger.error('error :: limit is not an integer - %s' % str(limit))

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
            raise  # to webapp to return in the UI

        metrics = []
        try:
            connection = engine.connect()
            stmt = select([metrics_table]).where(metrics_table.c.id != 0)
            result = connection.execute(stmt)
            for row in result:
                metric_id = int(row['id'])
                metric_name = str(row['metric'])
                metrics.append([metric_id, metric_name])
            connection.close()
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: could not determine metrics from metrics table')

        if get_metric_profiles:
            metrics_id = None
            for metric_obj in metrics:
                if metrics_id:
                    break
                if metric == str(metric_obj[1]):
                    metrics_id = str(metric_obj[0])
            new_query_string = query_string.replace('REPLACE_WITH_METRIC_ID', metrics_id)
            query_string = new_query_string
            logger.debug('debug :: query_string - %s' % query_string)

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

    all_fps = []
    try:
        connection = engine.connect()
        stmt = select([ionosphere_table]).where(ionosphere_table.c.id != 0)
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
                all_fps.append([fp_id, fp_metric_id, str(fp_metric), full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated])
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
        del all_fps
        del metrics
        return (features_profiles, features_profiles_count, matched_count,
                checked_count, generation_count, full_duration_list,
                enabled_list, tsfresh_version_list, generation_list, fail_msg,
                trace)

    features_profiles = []
    # @added 20170322 - Feature #1960: ionosphere_layers
    # Added layers information to the features_profiles items
    layers_present = False

    if engine_needed and engine and search_query:
        try:
            connection = engine.connect()
            if get_metric_profiles:
                # stmt = select([ionosphere_table]).where(ionosphere_table.c.metric_id == int(metric_id))
                stmt = select([ionosphere_table]).where(ionosphere_table.c.metric_id == int(metrics_id))
                logger.debug('debug :: stmt - is abstracted')
            else:
                stmt = query_string
                logger.debug('debug :: stmt - %s' % stmt)
            result = connection.execute(stmt)
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
                    features_profiles.append([fp_id, metric_id, str(metric), full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id])
                except:
                    trace = traceback.format_exc()
                    logger.error('%s' % trace)
                    logger.error('error :: bad row data')
            connection.close()
            features_profiles.sort(key=operator.itemgetter(int(0)))
            logger.debug('debug :: features_profiles length - %s' % str(len(features_profiles)))
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
            logger.info('ionosphere_layers OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to get ionosphere_layers meta')
            raise  # to webapp to return in the UI
        try:
            connection = engine.connect()
            if get_metric_profiles:
                stmt = select([ionosphere_layers_table]).where(ionosphere_layers_table.c.metric_id == int(metrics_id))
                # logger.debug('debug :: stmt - is abstracted')
            else:
                layers_query_string = 'SELECT * FROM ionosphere_layers'
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
                    if str(last_checked) == '0':
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
            logger.debug('debug :: features_profiles length - %s' % str(len(features_profiles)))
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
            for fp_id, metric_id, metric, full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id in features_profiles:
                default_values = True
                if int(fp_layers_id) > 0:
                    for layer_id, layer_fp_id, layer_matched_count, layer_human_date, layer_check_count, layer_checked_human_date, layer_label in features_profiles_layers:
                        if int(fp_layers_id) == int(layer_id):
                            default_values = False
                            break
                if default_values:
                    layer_id = 0
                    layer_matched_count = 0
                    layer_human_date = 'none'
                    layer_check_count = 0
                    layer_checked_human_date = 'none'
                    layer_label = 'none'
                features_profiles_and_layers.append([fp_id, metric_id, metric, full_duration, anomaly_timestamp, tsfresh_version, calc_time, features_count, features_sum, deleted, fp_matched_count, human_date, created_timestamp, fp_checked_count, checked_human_date, fp_parent_id, fp_generation, fp_validated, fp_layers_id, layer_matched_count, layer_human_date, layer_check_count, layer_checked_human_date, layer_label])

        old_features_profile_list = features_profiles
        features_profiles = features_profiles_and_layers

        full_duration_list = None
        enabled_list = None
        tsfresh_version_list = None
        generation_list = None
        if engine:
            engine_disposal(engine)
        del all_fps
        del metrics
        return (features_profiles, features_profiles_count, matched_count,
                checked_count, generation_count, full_duration_list,
                enabled_list, tsfresh_version_list, generation_list, fail_msg,
                trace)

    get_options = [
        'full_duration', 'enabled', 'tsfresh_version', 'generation']

    if engine_needed and engine and default_query:
        for required_option in get_options:
            all_list = []
#            required_option = 'full_duration'
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

    if engine:
        engine_disposal(engine)
    del all_fps
    del metrics
    return (features_profiles, features_profiles_count, matched_count,
            checked_count, generation_count, full_duration_list,
            enabled_list, tsfresh_version_list, generation_list, fail_msg,
            trace)


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
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: d_boundary_times is not an int'
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
        raise

    if engine:
        engine_disposal(engine)

    return new_layer_id, True, layers_added, new_layer_algorithm_ids, fail_msg, trace


def feature_profile_layers_detail(fp_layers_id):
    """
    Get the Ionosphere layers details of a fetures profile

    :param fp_layers_id: the features profile layers_id
    :type fp_id: str
    :return: tuple
    :rtype:  (str, boolean, str, str)

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
E layer  :: if value %s %s in last %s values  :: [not_anomalous, if active Es, F1 and F2 layers match]  ::  ['ACTIVE']
Es layer :: if day %s %s                 :: [not_anomalous, if active F1 and F2 layers match]  ::  %s
F1 layer :: if from_time > %s              :: [not_anomalous, if active F2 layer matchs]  ::  %s
F2 layer :: if until_time < %s             :: [not_anomalous]  ::  %s
''' % (str(d_condition), str(d_boundary_limit), str(e_condition),
            str(e_boundary_limit), str(e_boundary_times), str(es_condition),
            str(es_day), str(es_layer), str(f1_from_time), str(f1_layer),
            str(f2_until_time), str(f2_layer))
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
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: d_boundary_times is not an int'
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
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: e_boundary_times is not an int'
        return False, fail_msg, trace

    # NOT IMPLEMENTED YET
    es_layer = False
    f1_layer = False
    f2_layer = False

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
def validate_fp(fp_id):
    """
    Validate a features profile.

    :param fp_id: the features profile id to validate
    :return: array
    :rtype: array

    """
    logger = logging.getLogger(skyline_app_logger)

    function_str = 'ionoshere_backend.py :: validate_fp'

    logger.info('%s validating fp_id %s' % (function_str, str(fp_id)))

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
        fail_msg = 'error :: ionosphere_backend :: failed to get ionosphere_table meta for fp_id %s' % (str(fp_id))
        logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(engine)
        raise  # to webapp to return in the UI

    try:
        connection = engine.connect()
        connection.execute(
            ionosphere_table.update(
                ionosphere_table.c.id == int(fp_id)).
            values(validated=1))
        connection.close()
        logger.info('updated validated for %s' % (str(fp_id)))
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('error :: could not update validated for fp_id %s ' % str(fp_id))
        fail_msg = 'error :: could not update label for fp_id %s ' % str(fp_id)
        if engine:
            engine_disposal(engine)
        raise

    return True, fail_msg, trace
