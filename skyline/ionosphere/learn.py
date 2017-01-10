from __future__ import division
import logging
import os
from os import kill, getpid, listdir
from os.path import join, isfile
from sys import version_info
from time import time, sleep

import re
import csv
from ast import literal_eval

from redis import StrictRedis
# from msgpack import Unpacker, packb
import traceback
import mysql.connector
from mysql.connector import errorcode

from sqlalchemy.sql import select
# import requests
# from requests.auth import HTTPBasicAuth

# To match the new order introduced via the test_tsfresh method
import numpy as np
# import pandas as pd

import settings
from skyline_functions import (
    fail_check, mysql_select, write_data_to_file, send_graphite_metric, mkdir_p,
    get_graphite_metric)

from features_profile import calculate_features_profile

from database import (
    get_engine, ionosphere_table_meta, metrics_table_meta,
    ionosphere_matched_table_meta)
from tsfresh_feature_names import TSFRESH_FEATURES

skyline_app = 'ionosphere'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

python_version = int(version_info[0])

this_host = str(os.uname()[1])

# Converting one settings variable into a local variable, just because it is a
# long string otherwise.
try:
    ENABLE_IONOSPHERE_DEBUG = settings.ENABLE_IONOSPHERE_DEBUG
except:
    logger.error('error :: learn :: cannot determine ENABLE_IONOSPHERE_DEBUG from settings' % skyline_app)
    ENABLE_IONOSPHERE_DEBUG = False

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

try:
    learn_full_duration = int(settings.IONOSPHERE_DEFAULT_LEARN_FULL_DURATION_DAYS) * 86400
except:
    learn_full_duration = 86400 * 30  # 2592000


def learn_load_metric_vars(metric_vars_file):
    """
    Load the metric variables for a check from a metric check variables file

    :param metric_vars_file: the path and filename to the metric variables files
    :type metric_vars_file: str
    :return: the metric_vars list or ``False``
    :rtype: list

    """

    if os.path.isfile(metric_vars_file):
        logger.info(
            'learn :: loading metric variables from metric_check_file - %s' % (
                str(metric_vars_file)))
    else:
        logger.error(
            'error :: learn :: loading metric variables from metric_check_file - file not found - %s' % (
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
                'error :: learn :: loading metric variables - none found' % (
                    str(metric_vars_file)))
            return False

        if settings.ENABLE_DEBUG:
            logger.info(
                'debug :: learn :: metric_vars determined - metric variable - metric - %s' % str(metric_vars.metric))

    logger.info('debug :: learn :: metric_vars for %s' % str(metric))
    logger.info('debug :: learn :: %s' % str(metric_vars_array))

    return metric_vars_array


def get_learn_json(
        learn_json_file, base_name, use_full_duration, metric_timestamp,
        use_full_duration_days):
    """
    Called by :func:`~learn` to surface a use_full_duration timeseries for the
    metric from Graphite and save as json.

    """
    ts_json = None
    try:
        get_from_timestamp = int(metric_timestamp) - int(use_full_duration)
        logger.info(
            'learn :: getting Graphite timeseries json at %s days - from_timestamp - %s, until_timestamp - %s' %
            (str(use_full_duration_days), str(from_timestamp),
                str(until_timestamp)))
        ts_json = get_graphite_metric(
            skyline_app, base_name, from_timestamp, until_timestamp, 'json',
            learn_json_file)
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: learn :: getting Graphite timeseries json')

    return ts_json


def learn_features(learn_json_file):
    """
    Called by :func:`~learn` to extract the features from the use_full_duration
    json timeseries


    """

    learn_features_file = learn_features(learn_json_file, use_full_duration, metric_timestamp)
    calculate_features_profile(skyline_app, metric_timestamp, base_name, context)

    return


def get_metric_id(base_name, engine):
    """
    Called by :func:`~learn` and returns the metric id
    """

    metrics_id = 0

    # Get the metrics_table metadata
    metrics_table = None
    try:
        metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
        logger.info('learn :: metrics_table OK for %s' % base_name)
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: learn :: failed to get metrics_table meta for %s' % base_name)
        return False

    try:
        connection = engine.connect()
        stmt = select([metrics_table]).where(metrics_table.c.metric == base_name)
        result = connection.execute(stmt)
        row = result.fetchone()
        metrics_id = row['id']
        connection.close()
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: learn :: could not determine id from metrics table for - %s' % base_name)
        return False

    return metrics_id


def get_learning_fp_ids(base_name, metrics_id, use_full_duration, engine):
    """
    Called by :func:`~learn` and returns a list of learn 30 day feature_profile
    ids.

    """

    learn_fp_ids = []

    try:
        ionosphere_table, log_msg, trace = ionosphere_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('learn :: ionosphere_table OK')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: learn :: failed to get ionosphere_table meta for %s' % base_name)
        return learn_fp_ids

    try:
        connection = engine.connect()
        stmt = select([ionosphere_table]).where(ionosphere_table.c.metric_id == metrics_id)
        result = connection.execute(stmt)
        for row in result:
            fp_id = row['id']
            if int(row['full_duration']) == int(use_full_duration):
                learn_fp_ids.append(int(fp_id))
                logger.info('learn :: using fp id %s matched full_duration %s - %s' % (str(fp_id), str(use_full_duration), base_name))
            else:
                logger.info('learn :: not using fp id %s not matched full_duration %s - %s' % (str(fp_id), str(use_full_duration), base_name))
        connection.close()
        learn_fp_count = len(learn_fp_ids)
        logger.info('learn :: determined %s learn fp ids for %s' % (str(fp_count), base_name))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: could not determine learn fp ids from DB for %s' % base_name)

    return learn_fp_ids


def learn(metric_check_file):
    """
    Called by :class:`~skyline.skyline.Ionosphere.spawn_learn_process` to
    re-evaluate anomalies

    :param metric_check_file: the path and filename
    :type metric_check_file: str

    """

    child_process_pid = os.getpid()
    logger.info('learn :: child_process_pid - %s' % str(child_process_pid))
    logger.info('learn :: processing metric check - %s' % metric_check_file)

    if not os.path.isfile(str(metric_check_file)):
        logger.error('error :: learn :: file not found - metric_check_file - %s' % (str(metric_check_file)))
        return False

    check_file_name = os.path.basename(str(metric_check_file))
    if settings.ENABLE_IONOSPHERE_DEBUG:
        logger.info('debug :: learn :: check_file_name - %s' % check_file_name)
    check_file_timestamp = check_file_name.split('.', 1)[0]
    if settings.ENABLE_IONOSPHERE_DEBUG:
        logger.info('debug :: learn :: check_file_timestamp - %s' % str(check_file_timestamp))
    check_file_metricname_txt = check_file_name.split('.', 1)[1]
    if settings.ENABLE_IONOSPHERE_DEBUG:
        logger.info('debug :: learn :: check_file_metricname_txt - %s' % check_file_metricname_txt)
    check_file_metricname = check_file_metricname_txt.replace('.txt', '')
    if settings.ENABLE_IONOSPHERE_DEBUG:
        logger.info('debug :: learn :: check_file_metricname - %s' % check_file_metricname)
    check_file_metricname_dir = check_file_metricname.replace('.', '/')
    if settings.ENABLE_IONOSPHERE_DEBUG:
        logger.info('debug :: learn :: check_file_metricname_dir - %s' % check_file_metricname_dir)

    if settings.ENABLE_IONOSPHERE_DEBUG:
        logger.info('debug :: learn :: failed_check_file - %s' % failed_check_file)

    metric_vars_array = None
    try:
        metric_vars_array = learn_load_metric_vars(str(metric_check_file))
    except:
        logger.info(traceback.format_exc())
        logger.error('error :: learn :: failed to load metric variables from check file - %s' % (metric_check_file))
        return False

    if not metric_vars_array:
        logger.error('error :: learn :: no metric_vars_array available from check file - %s' % (metric_check_file))
        return False

    # Test metric variables
    # We use a pythonic methodology to test if the variables are defined,
    # this ensures that if any of the variables are not set for some reason
    # we can handle unexpected data or situations gracefully and try and
    # ensure that the process does not hang.
    metric = None
    try:
        key = 'metric'
        value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
        metric = str(value_list[0])
        base_name = metric
        if settings.ENABLE_IONOSPHERE_DEBUG:
            logger.info('debug :: learn :: metric variable - metric - %s' % metric)
    except:
        logger.info(traceback.format_exc())
        logger.error('error :: learn :: failed to read metric variable from check file - %s' % (metric_check_file))
        metric = None

    if not metric:
        logger.error('error :: learn :: failed to load metric variable from check file - %s' % (metric_check_file))
        return False

    from_timestamp = None
    try:
        key = 'from_timestamp'
        value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
        from_timestamp = int(value_list[0])
        if settings.ENABLE_IONOSPHERE_DEBUG:
            logger.info('debug :: learn :: metric variable - from_timestamp - %s' % from_timestamp)
    except:
        logger.info(traceback.format_exc())
        logger.error('error :: learn :: failed to read from_timestamp variable from check file - %s' % (metric_check_file))
        return False

    if not from_timestamp:
        logger.error('error :: learn :: failed to load from_timestamp variable from check file - %s' % (metric_check_file))
        return False

    metric_timestamp = None
    try:
        key = 'metric_timestamp'
        value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
        metric_timestamp = int(value_list[0])
        if settings.ENABLE_IONOSPHERE_DEBUG:
            logger.info('debug :: learn :: metric variable - metric_timestamp - %s' % metric_timestamp)
    except:
        logger.info(traceback.format_exc())
        logger.error('error :: learn :: failed to read metric_timestamp variable from check file - %s' % (metric_check_file))
        metric_timestamp = None

    if not metric_timestamp:
        logger.error('error :: learn :: failed to load metric_timestamp variable from check file - %s' % (metric_check_file))
        return False

    try:
        key = 'added_at'
        value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
        added_at = int(value_list[0])
        if settings.ENABLE_IONOSPHERE_DEBUG:
            logger.info('debug :: learn :: metric variable - added_at - %s' % added_at)
    except:
        logger.error('error :: learn :: failed to read added_at variable from check file setting to all - %s' % (metric_check_file))
        added_at = metric_timestamp

    full_duration = None
    try:
        key = 'full_duration'
        value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
        full_duration = int(value_list[0])
        if settings.ENABLE_IONOSPHERE_DEBUG:
            logger.info('debug :: learn :: metric variable - full_duration - %s' % full_duration)
    except:
        logger.error('error :: learn :: failed to read full_duration variable from check file - %s' % (metric_check_file))
        full_duration = None

    if not full_duration:
        return False

    # The metric variables now known so we can process the metric
    # Determine if the metric namespace has a configured specific
    # LEARN_FULL_DURATION_DAYS and LEARN_VALID_TIMESERIES_OLDER_THAN_SECONDS
    use_full_duration = learn_full_duration
    valid_learning_duration = int(settings.IONOSPHERE_DEFAULT_LEARN_VALID_TIMESERIES_OLDER_THAN_SECONDS)
    use_full_duration_days = int(int(valid_learning_duration) / 86400)
    for namespace_config in settings.IONOSPHERE_LEARNING_NAMESPACE_CONFIG:
        NAMESPACE_MATCH_PATTERN = str(namespace_config[0])
        METRIC_PATTERN = base_name
        pattern_match = False
        try:
            # Match by regex
            namespace_match_pattern = re.compile(NAMESPACE_MATCH_PATTERN)
            pattern_match = namespace_match_pattern.match(base_name)
            if pattern_match:
                try:
                    use_full_duration_days = int(namespace_config[1])
                    use_full_duration = int(namespace_config[1]) * 86400
                    valid_learning_duration = int(namespace_config[2])
                    logger.info('learn :: %s matches %s' % (base_name, str(namespace_config)))
                    break
                except:
                    pattern_match = False
        except:
            pattern_match = False
        if not pattern_match:
            # Match by substring
            if str(namespace_config[0]) in base_name:
                try:
                    use_full_duration_days = int(namespace_config[1])
                    use_full_duration = int(namespace_config[1]) * 86400
                    logger.info('learn :: %s matches %s' % (base_name, str(namespace_config[0])))
                    break
                except:
                    pattern_match = False
        if not pattern_match:
            logger.info('learn :: no specific namespace full_duration found, using default')
    logger.info('learn :: use_full_duration - %s seconds' % (str(use_full_duration)))

    # Is a valid_learning_timeseries, a valid timeseries is older than
    # settings.IONOSPHERE_DEFAULT_LEARN_VALID_TIMESERIES_OLDER_THAN_SECONDS or a
    # matched IONOSPHERE_LEARNING_NAMESPACE_CONFIG LEARN_VALID_TIMESERIES_OLDER_THAN_SECONDS
    valid_learning_timeseries = False
    now = int(time())
    timeseries_age = now - int(metric_timestamp)
    if int(valid_learning_duration) < timeseries_age:
        logger.info(
            'learn :: the timeseries is only %s seconds old, the valid age for this timeseries is %s - %s' %
            (str(timeseries_age), str(valid_learning_duration),
                metric_check_file))
        return False

    # To ensure that the IONOSPHERE_DATA_FOLDER is not populated and learn
    # namespace resources do not conflict with any training dataa resources a
    # learn directory is created? As the new use_full_duration_days transposed
    # csv will overwrite the training data transposed csv
    # TODO
    # Create learn dir and copy check file?
    # Make learn dir /opt/skyline/ionosphere/learn/ where s/data/learn/ ?
    # hmmm calculate_features_profile is going to populate the training data dir
    # useless it is handled in features_profiles.py under a new context, messy.

    # Create a learn json data
    got_learn_json = False
    metric_timeseries_dir = base_name.replace('.', '/')
    # TODO
    metric_learn_data_dir = '%s/%s/%s' % (
        str(settings.IONOSPHERE_LEARN_FOLDER), metric_timestamp,
        metric_timeseries_dir)
    if not os.path.exists(metric_learn_data_dir):
        mkdir_p(metric_learn_data_dir)
    learn_metric_check_file = '%s/%s' % (metric_learn_data_dir, check_file_name)
    if not os.path.isfile(learn_metric_check_file):
        # TODO - write a new metric check file with the use_full_duration?
        lines = []
        try:
            logger.info('learn :: reading metric_check_file to replace full_duration - %s' % metric_check_file)
            with open(metric_check_file) as fr:
                for line in fr:
                    if 'full_duration' in line:
                        line = line.replace(str(full_duration), str(use_full_duration))
                    lines.append(line)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: learn :: failed to read metric_check_file')

        try:
            logger.info('learn :: writing learn_metric_check_file to replace full_duration - %s' % learn_metric_check_file)
            with open(learn_metric_check_file, 'w') as outfile:
                for line in lines:
                    outfile.write(line)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: learn :: failed to write learn_metric_check_file')

    if not os.path.isfile(learn_metric_check_file):
        return False

    learn_json_file = '%s/%s.json' % (metric_learn_data_dir, base_name)
    if os.path.isfile(learn_json_file):
        logger.info('learn :: learning data ts json available - %s' % (learn_json_file))
        got_learn_json = True
    else:
        try:
            logger.info('learn :: need learning data ts json from Graphite - %s' % (learn_json_file))
            got_learn_json = learn_json(
                learn_json_file, base_name, use_full_duration, metric_timestamp,
                use_full_duration_days)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: learn :: learn_json call failed')
            got_learn_json = False

    if not got_learn_json:
        logger.error(
            'error :: learn :: failed to get timeseries json from Graphite for %s at %s second for anomaly at %s' %
            (base_name, str(use_full_duration), str(metric_timestamp)))
        return False

    # Calculate the features and a features profile for the learn_json_file
    fp_csv = None
    if got_learn_json:
        learn_features_file = learn_features(learn_json_file, use_full_duration, metric_timestamp)
        calculate_features_profile(skyline_app, metric_timestamp, base_name, context)
        f_calc = None
        if not calculated_feature_file_found:
            try:
                fp_csv, successful, fp_exists, fp_id, log_msg, traceback_format_exc, f_calc = calculate_features_profile(skyline_app, metric_timestamp, base_name, context)

    # Get a MySQL engine
    try:
        engine, log_msg, trace = get_an_engine()
        logger.info('learn :: %s' % log_msg)
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: learn :: could not get a MySQL engine to determine learning fp ids')

    if not engine:
        logger.error('error :: learn :: engine not obtained to determine learning fp ids')
        return False

    metrics_id = None
    if fp_csv:
        metrics_id = get_metric_id(base_name, engine)

    # Check if any use_full_duration feature_profiles exist for the metric
    learn_fp_ids = []
    if metrics_id:
        learn_fp_ids = get_learning_fp_ids(base_name, metrics_id, engine)

    if len(learn_fp_ids) == 0:

    if engine:
        engine_disposal(engine)
    return
