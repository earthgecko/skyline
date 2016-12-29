from __future__ import division
import logging
from os import path, walk, listdir
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

from sqlalchemy import (
    create_engine, Column, Table, Integer, String, MetaData, DateTime)
from sqlalchemy.dialects.mysql import DOUBLE, TINYINT
from sqlalchemy.sql import select
import json
from tsfresh import __version__ as tsfresh_version

import settings
import skyline_version
from skyline_functions import RepresentsInt, mkdir_p, write_data_to_file
from tsfresh_feature_names import TSFRESH_FEATURES

from database import get_engine, ionosphere_table_meta, metrics_table_meta

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


def ionosphere_metric_data(requested_timestamp, data_for_metric, context):
    """
    Get a list of all training data folders and metrics
    """
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
            images.append(str(metric_file))
        if i_file == metric_var_filename:
            metric_vars_file = str(metric_file)
        if i_file == ts_json_filename:
            ts_json_file = str(metric_file)

    metric_vars_ok = False
    metric_vars = ['error: could not read metrics vars file', metric_vars_file]
    if path.isfile(metric_vars_file):
        try:
            metric_vars = []
            with open(metric_vars_file) as f:
                for line in f:
                    add_line = line.replace('\n', '')
                    metric_vars.append(add_line)

            metric_vars_ok = True
        except:
            metric_vars_ok = False

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

    return (metric_paths, images, human_date, metric_vars, ts_json, data_to_process, panorama_anomaly_id)


def get_an_engine():

    try:
        engine, fail_msg, trace = get_engine(skyline_app)
        return engine, fail_msg, trace
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to get MySQL engine for'
        logger.error('%s' % fail_msg)
        return None, fail_msg, trace


def create_features_profile(requested_timestamp, data_for_metric, context):
    """
    Add a features_profile to the Skyline DB.
    """
    base_name = data_for_metric.replace(settings.FULL_NAMESPACE, '', 1)

    if context == 'training_data':
        log_context = 'training data'
    if context == 'features_profiles':
        log_context = 'features profile data'
    logger.info('%s requested for %s at %s' % (
        context, str(base_name), str(requested_timestamp)))

    metric_timeseries_dir = base_name.replace('.', '/')
    if context == 'training_data':
        metric_training_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, str(requested_timestamp),
            metric_timeseries_dir)
    if context == 'features_profiles':
        metric_training_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_PROFILES_FOLDER, metric_timeseries_dir,
            str(requested_timestamp))
    features_file = '%s/%s.tsfresh.input.csv.features.transposed.csv' % (
        metric_training_data_dir, base_name)

    features_profile_dir = '%s/%s' % (
        settings.IONOSPHERE_PROFILES_FOLDER, metric_timeseries_dir)

    ts_features_profile_dir = '%s/%s/%s' % (
        settings.IONOSPHERE_PROFILES_FOLDER, metric_timeseries_dir,
        str(requested_timestamp))

    features_profile_created_file = '%s/%s.%s.fp.created.txt' % (
        metric_training_data_dir, str(requested_timestamp), base_name)

    features_profile_details_file = '%s/%s.%s.fp.details.txt' % (
        metric_training_data_dir, str(requested_timestamp), base_name)

    trace = 'none'
    fail_msg = 'none'
    new_fp_id = False
    calculated_with_tsfresh = False
    calculated_time = False
    fcount = None
    fsum = None
    ts_full_duration = 0

    if path.isfile(features_profile_details_file):
        # Read the details file
        with open(features_profile_details_file, 'r') as f:
            fp_details_str = f.read()
        fp_details = literal_eval(fp_details_str)
        calculated_with_tsfresh = fp_details[1]
        calculated_time = str(fp_details[2])
        fcount = str(fp_details[3])
        fsum = str(fp_details[4])
        try:
            ts_full_duration = str(fp_details[5])
        except:
            logger.error('error :: could not determine the full duration from - %s' % features_profile_details_file)
        if not ts_full_duration:
            anomaly_check_file = '%s/%s.txt' % (
                metric_training_data_dir, base_name)

            if path.isfile(anomaly_check_file):
                # Read the details file
                with open(anomaly_check_file, 'r') as f:
                    anomaly_details = f.readlines()
                    for i, line in enumerate(anomaly_details):
                        if 'full_duration' in line:
                            _ts_full_duration = '%s' % str(line).split("'", 2)
                            full_duration_array = literal_eval(_ts_full_duration)
                            ts_full_duration = str(int(full_duration_array[1]))

    if path.isfile(features_profile_created_file):
        # Read the created file
        with open(features_profile_created_file, 'r') as f:
            fp_created_str = f.read()
        fp_created = literal_eval(fp_created_str)
        new_fp_id = fp_created[0]

        return str(new_fp_id), True, True, fail_msg, trace

    # Have data
    if path.isfile(features_file):
        logger.error(traceback.format_exc())
        logger.info('features_file exists: %s' % features_file)
    else:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: features_file does not exist: %s' % features_file
        logger.error('%s' % fail_msg)
        raise

    features_data = []
    with open(features_file, 'rb') as fr:
        reader = csv.reader(fr, delimiter=',')
        for i, line in enumerate(reader):
            feature_name_item = False
            fname_id = False
            f_value = False
            feature_name = str(line[0])
            feature_name_item = filter(
                lambda x: x[1] == feature_name, TSFRESH_FEATURES)
            if feature_name_item:
                feature_name_id = feature_name_item[0]
            if feature_name_item:
                feature_name_list = feature_name_item[0]
                fname_id = int(feature_name_list[0])
            f_value = str(line[1])
            if fname_id and f_value:
                features_data.append([fname_id, f_value])

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

    # Get metric id
    metrics_id = False
    metrics_table = None
    try:
        metrics_table, fail_msg, trace = metrics_table_meta(skyline_app, engine)
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to get metrics_table meta for %s' % base_name
        logger.error('%s' % fail_msg)
        raise

    logger.info('metrics_table OK')

    try:
        connection = engine.connect()
        # @modified 20161209 -  - Branch #922: ionosphere
        #                        Task #1658: Patterning Skyline Ionosphere
        # result = connection.execute('select id from metrics where metric=\'%s\'' % base_name)
#        for row in result:
#            while not metrics_id:
#                metrics_id = row['id']
        stmt = select([metrics_table]).where(metrics_table.c.metric == base_name)
        result = connection.execute(stmt)
        # row = result.fetchone()
        for row in result:
            metrics_id = row['id']
        connection.close()
        logger.info('determined metric id: %s' % str(metrics_id))
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not determine id of metric from DB: %s' % base_name
        logger.error('%s' % fail_msg)
        # @modified 20161209 -  - Branch #922: ionosphere
        #                        Task #1658: Patterning Skyline Ionosphere
        # Turned all these to use raise and traceback.format_exc() to carry
        # ionosphere_backend.py through to the rendered page for the user, e.g
        # me.
#        return False, False, False, fail_msg, trace
        raise

    ionosphere_table = None
    try:
        ionosphere_table, fail_msg, trace = ionosphere_table_meta(skyline_app, engine)
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to get ionosphere_table meta for %s' % base_name
        logger.error('%s' % fail_msg)
        raise

    logger.info('ionosphere_table OK')

    new_fp_id = False
    try:
        connection = engine.connect()
        ins = ionosphere_table.insert().values(
            metric_id=int(metrics_id), full_duration=int(ts_full_duration),
            enabled=1, tsfresh_version=str(tsfresh_version),
            calc_time=calculated_time, features_count=fcount,
            features_sum=fsum)
        result = connection.execute(ins)
        connection.close()
        new_fp_id = result.inserted_primary_key[0]
        logger.info('new ionosphere fp_id: %s' % str(new_fp_id))
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to insert a new record into the ionosphere table for %s' % base_name
        logger.error('%s' % fail_msg)
        raise

    if not RepresentsInt(new_fp_id):
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: unknown new ionosphere new_fp_id for %s' % base_name
        logger.error('%s' % fail_msg)
        raise

    # Create z_fp_<metric_id> table
    fp_table_created = False
    fp_table_name = 'z_fp_%s' % str(metrics_id)
    try:
        fp_meta = MetaData()
        # @modified 20161222 - Task #1812: z_fp table type
        # Changed to InnoDB from MyISAM as no files open issues and MyISAM clean
        # up, there can be LOTS of file_per_table z_fp_ tables/files without
        # the MyISAM issues.  z_fp_ tables are mostly read and will be shuffled
        # in the table cache as required.
        fp_metric_table = Table(
            fp_table_name, fp_meta,
            Column('id', Integer, primary_key=True),
            Column('fp_id', Integer, nullable=False, key='fp_id'),
            Column('feature_id', Integer, nullable=False),
            Column('value', DOUBLE(), nullable=True),
            mysql_charset='utf8',
            mysql_key_block_size='255',
            mysql_engine='InnoDB')
        fp_metric_table.create(engine, checkfirst=True)
        fp_table_created = True
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to create table - %s' % fp_table_name
        logger.error('%s' % fail_msg)
        raise

    if not fp_table_created:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to determine True for create table - %s' % fp_table_name
        logger.error('%s' % fail_msg)
        raise

    # Insert features and values
    insert_statement = []
    for fname_id, f_value in features_data:
        insert_statement.append({'fp_id': new_fp_id, 'feature_id': fname_id, 'value': f_value},)
    if insert_statement == []:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: empty insert_statement for %s inserts' % fp_table_name
        logger.error('%s' % fail_msg)
        raise
    # else:
        # feature_count = sum(1 for x in a if isinstance(x, insert_statement))
        # logger.info(
        #     'fp_id - %s - %s feature values in insert_statement for %s ' %
        #     (str(feature_count), str(new_fp_id), fp_table_name))
        # feature_count = sum(1 for x in a if isinstance(x, insert_statement))
        # logger.info(
        #     'fp_id - %s - feature values in insert_statement for %s ' %
        #     (str(new_fp_id), fp_table_name))

    try:
        connection = engine.connect()
        connection.execute(fp_metric_table.insert(), insert_statement)
        connection.close()
        logger.info('fp_id - %s - feature values inserted into %s' % (str(new_fp_id), fp_table_name))
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to insert a feature values into %s' % fp_table_name
        logger.error('%s' % fail_msg)
        raise

    # Create metric ts table if not exists ts_<metric_id>
    # Create z_ts_<metric_id> table
    ts_table_created = False
    ts_table_name = 'z_ts_%s' % str(metrics_id)
    try:
        ts_meta = MetaData()
        # @modified 20161222 - Task #1812: z_fp table type
        # Changed to InnoDB from MyISAM as no files open issues and MyISAM clean
        # up, there can be LOTS of file_per_table z_fp_ tables/files without
        # the MyISAM issues.  z_fp_ tables are mostly read and will be shuffled
        # in the table cache as required.
        ts_metric_table = Table(
            ts_table_name, ts_meta,
            Column('id', Integer, primary_key=True),
            Column('fp_id', Integer, nullable=False, key='fp_id'),
            Column('timestamp', Integer, nullable=False),
            Column('value', DOUBLE(), nullable=True),
            mysql_charset='utf8',
            mysql_key_block_size='255',
            mysql_engine='InnoDB')
        ts_metric_table.create(engine, checkfirst=True)
        ts_table_created = True
        logger.info('metric ts table created OK - %s' % (ts_table_name))
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to create table - %s' % ts_table_name
        logger.error('%s' % fail_msg)
        raise

    # Insert timeseries that the features profile was created from
    anomaly_json = '%s/%s.json' % (metric_training_data_dir, base_name)
    if path.isfile(anomaly_json):
        logger.info('metric anomaly json found OK - %s' % (anomaly_json))
        try:
            # Read the timeseries json file
            with open(anomaly_json, 'r') as f:
                raw_timeseries = f.read()
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: failed to read timeseries data from %s' % anomaly_json
            logger.error('%s' % (fail_msg))
            fail_msg = 'error: failed to read timeseries data from %s' % anomaly_json
            end = timer()
            raise
    else:
        trace = 'none'
        fail_msg = 'error: file not found - %s' % (anomaly_json)
        logger.error(fail_msg)
        raise

    # Convert the timeseries to csv
    timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
    timeseries = literal_eval(timeseries_array_str)

    datapoints = timeseries
    validated_timeseries = []
    for datapoint in datapoints:
        try:
            new_datapoint = [str(int(datapoint[0])), float(datapoint[1])]
            validated_timeseries.append(new_datapoint)
        except:
            continue

    insert_statement = []
    for ts, value in validated_timeseries:
        insert_statement.append({'fp_id': new_fp_id, 'timestamp': ts, 'value': value},)
    try:
        connection = engine.connect()
        connection.execute(ts_metric_table.insert(), insert_statement)
        connection.close()
        logger.info('fp_id - %s - timeseries inserted into %s' % (str(new_fp_id), ts_table_name))
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to insert the timeseries into %s' % ts_table_name
        logger.error('%s' % fail_msg)
        raise

    # Create a created features profile file
    try:
        # data = '[%s, %s, ]' % (new_fp_id, str(int(time.time())))
        # write_data_to_file(skyline_app, features_profile_created_file, 'w', data)
        data = '[%s, %s, \'%s\', %s, %s, %s, %s]' % (new_fp_id, str(int(time.time())), str(tsfresh_version), str(calculated_time), str(fcount), str(fsum), str(ts_full_duration))
        write_data_to_file(skyline_app, features_profile_created_file, 'w', data)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to write fp.created file' % ts_table_name
        logger.error('%s' % fail_msg)

    # Set ionosphere_enabled for the metric
    try:
        # update_statement = 'UPDATE metrics SET ionosphere_enabled=1 WHERE id=%s' % str(metrics_id)
        connection = engine.connect()
        # result = connection.execute('UPDATE metrics SET ionosphere_enabled=1 WHERE id=%s' % str(metrics_id))
        # connection.execute(ts_metric_table.insert(), insert_statement)
        connection.execute(
            metrics_table.update(
                metrics_table.c.id == metrics_id).values(ionosphere_enabled=1))
        connection.close()
        logger.info('ionosphere_enabled set on metric id: %s' % str(metrics_id))
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not update metrics table and set ionosphere_enabled on id %s' % str(metrics_id)
        logger.error('%s' % fail_msg)
        raise

    # Copy data from training data dir to features_profiles dir
    if not path.isdir(ts_features_profile_dir):
        mkdir_p(ts_features_profile_dir)

    if path.isdir(ts_features_profile_dir):
        logger.info('fp_id - %s - features profile dir created - %s' % (str(new_fp_id), ts_features_profile_dir))
        # src_files = os.listdir(src)
        # for file_name in src_files:
        #    full_file_name = path.join(src, file_name)
        #    if (path.isfile(full_file_name)):
        #        shutil.copy(full_file_name, dest)

        data_files = []
        try:
            glob_path = '%s/*.*' % metric_training_data_dir
            data_files = glob.glob(glob_path)
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            logger.error('error :: glob - fp_id - %s - training data not copied to %s' % (str(new_fp_id), ts_features_profile_dir))

        for i_file in data_files:
            try:
                shutil.copy(i_file, ts_features_profile_dir)
                logger.info('fp_id - %s - training data copied - %s' % (str(new_fp_id), i_file))
            except shutil.Error as e:
                trace = traceback.format_exc()
                logger.error('%s' % trace)
                logger.error('error :: shutil error - fp_id - %s - training data not copied to %s' % (str(new_fp_id), ts_features_profile_dir))
                logger.error('error :: %s' % (e))
            # Any error saying that the directory doesn't exist
            except OSError as e:
                trace = traceback.format_exc()
                logger.error('%s' % trace)
                logger.error('error :: OSError error - fp_id - %s - training data not copied to %s' % (str(new_fp_id), ts_features_profile_dir))
                logger.error('error :: %s' % (e))
        logger.info('fp_id - %s - training data copied to %s' % (str(new_fp_id), ts_features_profile_dir))
    else:
        logger.error('error :: fp_id - %s - training data not copied to %s' % (str(new_fp_id), ts_features_profile_dir))

    return str(new_fp_id), True, False, fail_msg, trace


def features_profile_details(fp_id):
    """
    Get the Ionosphere details of a fetures profile

    :param fp_id: the t feautres profile id
    :type fp_id: str
    :return: tuple
    :rtype:  (str, boolean, str, str)

    """

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
        return False, False, fail_msg, trace

    if not engine:
        trace = 'none'
        fail_msg = 'error :: engine not obtained'
        logger.error(fail_msg)
        return False, False, fail_msg, trace
    ionosphere_table = None
    try:
        ionosphere_table, fail_msg, trace = ionosphere_table_meta(skyline_app, engine)
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to get ionosphere_table meta for fp_id %s details' % str(fp_id)
        logger.error('%s' % fail_msg)
        return False, False, fail_msg, trace

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
''' % (str(tsfresh_version), str(calc_time), str(features_count),
            str(features_sum), str(deleted), str(matched_count),
            str(last_matched), str(human_date), str(created_timestamp),
            str(full_duration), str(checked_count), str(last_checked),
            str(checked_human_date))
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: could not get fp_id %s details from ionosphere DB table' % str(fp_id)
        logger.error('%s' % fail_msg)
        return False, False, fail_msg, trace

    return fp_details, True, fail_msg, trace
