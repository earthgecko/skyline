from __future__ import division
import logging
from os import path
import time
# import string
# import operator
import re
import csv
# import datetime
import shutil
import glob
from ast import literal_eval
import traceback
from datetime import datetime

from redis import StrictRedis
# @modified 20190503 - Branch #2646: slack - linting
# from sqlalchemy import (
#     create_engine, Column, Table, Integer, String, MetaData, DateTime)
from sqlalchemy import (
    Column, Table, Integer, MetaData)

# @modified 20190503 - Branch #2646: slack - linting
# from sqlalchemy.dialects.mysql import DOUBLE, TINYINT
from sqlalchemy.dialects.mysql import DOUBLE

from sqlalchemy.sql import select
# import json
from tsfresh import __version__ as tsfresh_version

import settings
import skyline_version

from skyline_functions import RepresentsInt, mkdir_p, write_data_to_file
# @added 20200813 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
from skyline_functions import historical_data_dir_exists

from tsfresh_feature_names import TSFRESH_FEATURES

from database import (
    get_engine, ionosphere_table_meta, metrics_table_meta,
    # @added 20180414 - Branch #2270: luminosity
    luminosity_table_meta,
    # @added 20190501 - Branch #2646: slack
    anomalies_table_meta,
)
# @added 20190502 - Branch #2646: slack
from slack_functions import slack_post_message, slack_post_reaction

# @added 20200512 - Bug #2534: Ionosphere - fluid approximation - IONOSPHERE_MINMAX_SCALING_RANGE_TOLERANCE on low ranges
#                   Feature #2404: Ionosphere - fluid approximation
from create_matplotlib_graph import create_matplotlib_graph

LOCAL_DEBUG = False
skyline_version = skyline_version.__absolute_version__

try:
    full_duration_seconds = int(settings.FULL_DURATION)
except:
    full_duration_seconds = 86400

full_duration_in_hours = full_duration_seconds / 60 / 60

# @added 20200813 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
try:
    IONOSPHERE_HISTORICAL_DATA_FOLDER = settings.IONOSPHERE_HISTORICAL_DATA_FOLDER
except:
    IONOSPHERE_HISTORICAL_DATA_FOLDER = '/opt/skyline/ionosphere/historical_data'
try:
    IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR = settings.IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
except:
    IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR = []

# @created 20170114 - Feature #1854: Ionosphere learn
# This function was moved in its entirety from webapp/ionosphere_backend.py
# so as to decouple the creation of features profiles from the webapp as
# ionosphere/learn.py now requires the ability to create features profiles to.
# The only things modified were that current_skyline_app, current_logger and
# generations parameters were added the function:
# ionosphere_job, parent_id, generation


def fp_create_get_an_engine(current_skyline_app):

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        return engine, fail_msg, trace
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: fp_create_get_an_engine :: failed to get MySQL engine'
        current_logger.error('%s' % fail_msg)
        return None, fail_msg, trace


def fp_create_engine_disposal(current_skyline_app, engine):

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if engine:
        current_logger.error('fp_create_engine_disposal :: calling engine.dispose()')
        try:
            engine.dispose()
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: fp_create_engine_disposal :: calling engine.dispose()')
    return


# @added 20170115 - Feature #1854: Ionosphere learn - generations
# Added determination of the learn related variables so that Panorama,
# webapp/ionosphere and learn can access this function to determine what the
# default IONOSPHERE_LEARN_DEFAULT_ or if a namespace has specific values in
# settings.IONOSPHERE_LEARN_NAMESPACE_CONFIG

# learn_full_duration_days, learn_valid_ts_older_than,
# max_generations and max_percent_diff_from_origin value to the
# insert statement if the table is the metrics table.

# Set the learn generations variables with the IONOSPHERE_LEARN_DEFAULT_ and any
# settings.IONOSPHERE_LEARN_NAMESPACE_CONFIG values.  These will later be
# overridden by any database values determined for the specific metric if
# they exist.
def get_ionosphere_learn_details(current_skyline_app, base_name):
    """
    Determines what the default ``IONOSPHERE_LEARN_DEFAULT_`` values and what the
    specific override values are if the metric matches a pattern defined in
    :mod:`settings.IONOSPHERE_LEARN_NAMESPACE_CONFIG`. This is used in Panorama,
    webapp/ionosphere_backend

    :param current_skyline_app: the Skyline app name calling the function
    :param base_name: thee base_name of the metric
    :type current_skyline_app: str
    :type base_name: str
    :return: tuple
    :return: (use_full_duration, valid_learning_duration, use_full_duration_days, max_generations, max_percent_diff_from_origin)
    :rtype: (int, int, int, int, float)

    """

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    use_full_duration = None
    valid_learning_duration = None
    use_full_duration_days = None
    max_generations = None
    max_percent_diff_from_origin = None
    try:
        use_full_duration = int(settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS * 86400)
        valid_learning_duration = int(settings.IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS)
        use_full_duration_days = int(settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS)
        max_generations = int(settings.IONOSPHERE_LEARN_DEFAULT_MAX_GENERATIONS)
        max_percent_diff_from_origin = float(settings.IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN)
        for namespace_config in settings.IONOSPHERE_LEARN_NAMESPACE_CONFIG:
            NAMESPACE_MATCH_PATTERN = str(namespace_config[0])
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
                        max_generations = int(namespace_config[3])
                        max_percent_diff_from_origin = float(namespace_config[4])
                        current_logger.info('get_ionosphere_learn_details :: %s matches %s' % (base_name, str(namespace_config)))
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
                        valid_learning_duration = int(namespace_config[2])
                        max_generations = int(namespace_config[3])
                        max_percent_diff_from_origin = float(namespace_config[4])
                        current_logger.info('get_ionosphere_learn_details :: %s matches %s' % (base_name, str(namespace_config)))
                        break
                    except:
                        pattern_match = False
            if not pattern_match:
                current_logger.info('get_ionosphere_learn_details :: no specific namespace matches found, using default settings')
            else:
                current_logger.info('get_ionosphere_learn_details :: found namespace config match settings')
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_ionosphere_learn_details :: failed to check namespace config settings matches')

    current_logger.info('get_ionosphere_learn_details :: use_full_duration_days       :: %s days' % (str(use_full_duration_days)))
    current_logger.info('get_ionosphere_learn_details :: use_full_duration            :: %s seconds' % (str(use_full_duration)))
    current_logger.info('get_ionosphere_learn_details :: valid_learning_duration      :: %s seconds' % (str(valid_learning_duration)))
    current_logger.info('get_ionosphere_learn_details :: max_generations              :: %s' % (str(max_generations)))
    current_logger.info('get_ionosphere_learn_details :: max_percent_diff_from_origin :: %s' % (str(max_percent_diff_from_origin)))

    return use_full_duration, valid_learning_duration, use_full_duration_days, max_generations, max_percent_diff_from_origin


# @added 20200512 - Bug #2534: Ionosphere - fluid approximation - IONOSPHERE_MINMAX_SCALING_RANGE_TOLERANCE on low ranges
#                   Feature #2404: Ionosphere - fluid approximation
# Due to the loss of resolution in the Grpahite graph images due
# to their size, create a matplotlib graph for the DB fp time
# series data for more accurate validation
def create_fp_ts_graph(
    current_skyline_app, metric_data_dir, base_name, fp_id, anomaly_timestamp,
        timeseries):
    """
    Creates a png graph image using the features profile time series data
    provided or from the features profile time seires data in the DB if an empty
    list is provided.

    :param current_skyline_app: the Skyline app name calling the function
    :param metric_data_dir: the training_data or features profile directory were
        the png image is to be saved to
    :param base_name: the base_name of the metric
    :param fp_ip: the feature profile id
    :param anomaly_timestamp: the anomaly timestamp
    :param timeseries: the time series
    :type current_skyline_app: str
    :type metric_data_dir: str
    :type base_name: str
    :type fp_id: int
    :type anomaly_timestamp: int
    :type timeseries: list
    :return: boolean
    :rtype: boolean

    """

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    fp_ts_graph_file = '%s/%s.fp_id_ts.%s.matplotlib.png' % (
        metric_data_dir, base_name, str(fp_id))
    if path.isfile(fp_ts_graph_file):
        return True

    fp_id_metric_ts = []
    if not timeseries:
        # Get timeseries data from the z_fp_ts table
        current_logger.info('create_fp_ts_graph :: getting MySQL engine')
        try:
            engine, fail_msg, trace = fp_create_get_an_engine(current_skyline_app)
            current_logger.info(fail_msg)
        except:
            trace = traceback.format_exc()
            current_logger.error(trace)
            fail_msg = 'error :: create_fp_ts_graph :: could not get a MySQL engine'
            current_logger.error('%s' % fail_msg)
            return False

        # First check to determine if the z_ts_<mertic_id> for the fp
        # has data in memcache before querying the database
        ionosphere_table = None
        try:
            ionosphere_table, fail_msg, trace = ionosphere_table_meta(current_skyline_app, engine)
            current_logger.info(fail_msg)
        except:
            trace = traceback.format_exc()
            current_logger.error('%s' % trace)
            fail_msg = 'error :: create_fp_ts_graph :: failed to get ionosphere_table meta for %s' % base_name
            current_logger.error('%s' % fail_msg)
            return False

        try:
            connection = engine.connect()
            stmt = select([ionosphere_table]).where(ionosphere_table.c.id == int(fp_id))
            result = connection.execute(stmt)
            for row in result:
                metric_id = int(row['metric_id'])
                current_logger.info('found metric_id from the DB - %s' % (str(metric_id)))
            connection.close()
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: could not determine full_duration from ionosphere for fp_id %s' % str(fp_id))
            if engine:
                fp_create_engine_disposal(current_skyline_app, engine)
            return False

        metric_fp_ts_table = 'z_ts_%s' % str(metric_id)
        try:
            stmt = 'SELECT timestamp, value FROM %s WHERE fp_id=%s' % (metric_fp_ts_table, str(fp_id))  # nosec
            connection = engine.connect()
            for row in engine.execute(stmt):
                fp_id_ts_timestamp = int(row['timestamp'])
                fp_id_ts_value = float(row['value'])
                fp_id_metric_ts.append([fp_id_ts_timestamp, fp_id_ts_value])
            connection.close()
            values_count = len(fp_id_metric_ts)
            current_logger.info('determined %s values for the fp_id time series %s for %s' % (str(values_count), str(fp_id), str(base_name)))
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: could not determine timestamps and values from %s' % metric_fp_ts_table)
            if engine:
                fp_create_engine_disposal(current_skyline_app, engine)
            return False
    if fp_id_metric_ts:
        if engine:
            fp_create_engine_disposal(current_skyline_app, engine)
        timeseries = fp_id_metric_ts
    if not timeseries:
        current_logger.error('error :: could not determine timestamps and values for fp_id %s' % str(fp_id))
        return False
    created_fp_ts_graph = False
    try:
        graph_title = '%s\nFeatures profile id %s - database time series data plot' % (base_name, str(fp_id))
        created_fp_ts_graph = create_matplotlib_graph(current_skyline_app, fp_ts_graph_file, graph_title, timeseries)
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: failed to create matplotlib graph for %s fp id %s' % (base_name, str(fp_id)))
    return created_fp_ts_graph


# @modified 20190503 - Branch #2646: slack
# Added slack_ionosphere_job
# def create_features_profile(current_skyline_app, requested_timestamp, data_for_metric, context, ionosphere_job, fp_parent_id, fp_generation, fp_learn):
# @modified 20190919 - Feature #3230: users DB table
#                      Ideas #2476: Label and relate anomalies
#                      Feature #2516: Add label to features profile
# Added user_id and label
# def create_features_profile(current_skyline_app, requested_timestamp, data_for_metric, context, ionosphere_job, fp_parent_id, fp_generation, fp_learn, slack_ionosphere_job):
def create_features_profile(current_skyline_app, requested_timestamp, data_for_metric, context, ionosphere_job, fp_parent_id, fp_generation, fp_learn, slack_ionosphere_job, user_id, label):
    """
    Add a features_profile to the Skyline ionosphere database table.

    :param current_skyline_app: Skyline app name
    :param requested_timestamp: The timestamp of the dir that the features
        profile data is in
    :param data_for_metric: The base_name of the metric
    :param context: The context of the caller
    :param ionosphere_job: The ionosphere_job name related to creation request
        valid jobs are ``learn_fp_human``, ``learn_fp_generation``,
        ``learn_fp_learnt`` and ``learn_fp_automatic``.
    :param fp_parent_id: The id of the parent features profile that this was
        learnt from, 0 being an original human generated features profile
    :param fp_generation: The number of generations away for the original
        human generated features profile, 0 being an original human generated
        features profile.
    :param fp_learn: Whether Ionosphere should learn at use_full_duration_days
    :param slack_ionosphere_job: The originating ionosphere_job name
    :param user_id: The user id of the user creating the features profile
    :param label: A label for the feature profile
    :type current_skyline_app: str
    :type requested_timestamp: int
    :type data_for_metric: str
    :type context: str
    :type ionosphere_job: str
    :type fp_parent_id: int
    :type fp_generation: int
    :type fp_learn: boolean
    :type slack_ionosphere_job: str
    :type user_id: int
    :type label: str
    :return: fp_id, fp_in_successful, fp_exists, fail_msg, traceback_format_exc
    :rtype: str, boolean, boolean, str, str

    """
    try:
        python_version
    except:
        from sys import version_info
        python_version = int(version_info[0])

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
    # base_name = data_for_metric.replace(settings.FULL_NAMESPACE, '', 1)
    if data_for_metric.startswith(settings.FULL_NAMESPACE):
        base_name = data_for_metric.replace(settings.FULL_NAMESPACE, '', 1)
    else:
        base_name = data_for_metric

    # @added 20200216 - Feature #3450: Handle multiple requests to create a features profile
    # Ensure that one features profile can only be created if
    # multiple requests are received to create a features profile
    fp_pending = None
    fp_pending_cache_key = 'fp_pending.%s.%s' % (
        str(requested_timestamp), str(base_name))
    try:
        # TODO - make this use get_redis_conn, but needs testing
        if settings.REDIS_PASSWORD:
            redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        else:
            redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
    except:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: create_features_profile :: failed to establish redis_conn to determine if a features profile is pending - %s %s' % (str(requested_timestamp), base_name)
        current_logger.error('%s' % fail_msg)
        if context == 'training' or context == 'features_profile':
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        else:
            return False, False, False, fail_msg, trace
    try:
        fp_pending = redis_conn.get(fp_pending_cache_key)
    except:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: create_features_profile :: failed to determine if a features profile is pending - %s %s' % (str(requested_timestamp), base_name)
        current_logger.error('%s' % fail_msg)
        if context == 'training' or context == 'features_profile':
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        else:
            return False, False, False, fail_msg, trace
    if fp_pending:
        trace = 'None'
        fail_msg = 'create_features_profile :: a features profile is pending - %s %s' % (str(requested_timestamp), base_name)
        current_logger.info('%s' % fail_msg)
        if context == 'training' or context == 'features_profile':
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        else:
            return False, False, False, fail_msg, trace
    else:
        try:
            redis_conn.setex(fp_pending_cache_key, 60, str(current_skyline_app))
            current_logger.info('create_features_profile :: created %s Redis key with expiry of 60 seconds' % (
                fp_pending_cache_key))
        except:
            trace = traceback.format_exc()
            current_logger.error(trace)
            fail_msg = 'error :: create_features_profile :: failed to determine create %s Redis key' % fp_pending_cache_key
            current_logger.error('%s' % fail_msg)
            if context == 'training' or context == 'features_profile':
                # Raise to webbapp I believe to provide traceback to user in UI
                raise
            else:
                return False, False, False, fail_msg, trace

    if context == 'training_data':
        ionosphere_job = 'learn_fp_human'

    current_logger.info('create_features_profile :: %s :: requested for %s at %s by user id %s' % (
        context, str(base_name), str(requested_timestamp), str(user_id)))

    metric_timeseries_dir = base_name.replace('.', '/')
    # @modified 20190327 - Feature #2484: FULL_DURATION feature profiles
    # Added context ionosphere_echo
    if context == 'training_data' or context == 'ionosphere_echo' or context == 'ionosphere_echo_check':
        metric_training_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, str(requested_timestamp),
            metric_timeseries_dir)

        # @added 20200813 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
        if IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR and context == 'training_data':
            try:
                historical_data, metric_training_data_dir = historical_data_dir_exists(current_skyline_app, metric_training_data_dir)
                if historical_data:
                    current_logger.info('create_features_profile :: using historical training data - %s' % metric_training_data_dir)
            except:
                trace = traceback.format_exc()
                current_logger.error(trace)
                fail_msg = 'error :: create_features_profile :: failed to determine whether this is historical training data'
                current_logger.error('%s' % fail_msg)
                if context == 'training_data':
                    # Raise to webbapp I believe to provide traceback to user in UI
                    raise
                else:
                    return False, False, False, fail_msg, trace

    if context == 'features_profiles':
        metric_training_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_PROFILES_FOLDER, metric_timeseries_dir,
            str(requested_timestamp))

    # @added 20170113 - Feature #1854: Ionosphere learn
    if context == 'ionosphere_learn':
        # @modified 20170116 - Feature #1854: Ionosphere learn
        # Allowing ionosphere_learn to create a features profile for a training
        # data set that it has learnt is not anomalous
        if ionosphere_job != 'learn_fp_automatic':
            metric_training_data_dir = '%s/%s/%s' % (
                settings.IONOSPHERE_LEARN_FOLDER, str(requested_timestamp),
                metric_timeseries_dir)
        else:
            metric_training_data_dir = '%s/%s/%s' % (
                settings.IONOSPHERE_DATA_FOLDER, str(requested_timestamp),
                metric_timeseries_dir)

    features_file = '%s/%s.tsfresh.input.csv.features.transposed.csv' % (
        metric_training_data_dir, base_name)

    features_profile_dir = '%s/%s' % (
        settings.IONOSPHERE_PROFILES_FOLDER, metric_timeseries_dir)

    ts_features_profile_dir = '%s/%s' % (
        features_profile_dir, str(requested_timestamp))

    features_profile_created_file = '%s/%s.%s.fp.created.txt' % (
        metric_training_data_dir, str(requested_timestamp), base_name)

    features_profile_details_file = '%s/%s.%s.fp.details.txt' % (
        metric_training_data_dir, str(requested_timestamp), base_name)

    anomaly_check_file = '%s/%s.txt' % (metric_training_data_dir, base_name)

    trace = 'none'
    fail_msg = 'none'
    new_fp_id = False
    calculated_time = False
    fcount = None
    fsum = None
    # @added 20170104 - Feature #1842: Ionosphere - Graphite now graphs
    # Added the ts_full_duration parameter so that the appropriate graphs can be
    # embedded for the user in the training data page
    ts_full_duration = '0'

    if context == 'ionosphere_learn':
        if not path.isfile(features_profile_details_file):
            current_logger.error('error :: create_features_profile :: no features_profile_details_file - %s' % features_profile_details_file)
            return 'none', False, False, fail_msg, trace

    if path.isfile(features_profile_details_file):
        current_logger.info('create_features_profile :: getting features profile details from - %s' % features_profile_details_file)
        # Read the details file
        with open(features_profile_details_file, 'r') as f:
            fp_details_str = f.read()
        fp_details = literal_eval(fp_details_str)
        calculated_time = str(fp_details[2])
        fcount = str(fp_details[3])
        fsum = str(fp_details[4])
        try:
            ts_full_duration = str(fp_details[5])
        except:
            current_logger.error('error :: create_features_profile :: could not determine the full duration from - %s' % features_profile_details_file)
            ts_full_duration = '0'

        if context != 'ionosphere_learn':
            if ts_full_duration == '0':
                if path.isfile(anomaly_check_file):
                    current_logger.info('create_features_profile :: determining the full duration from anomaly_check_file - %s' % anomaly_check_file)
                    # Read the details file
                    with open(anomaly_check_file, 'r') as f:
                        anomaly_details = f.readlines()
                        for i, line in enumerate(anomaly_details):
                            if 'full_duration' in line:
                                _ts_full_duration = '%s' % str(line).split("'", 2)
                                full_duration_array = literal_eval(_ts_full_duration)
                                ts_full_duration = str(int(full_duration_array[1]))
                                current_logger.info('create_features_profile :: determined the full duration as - %s' % str(ts_full_duration))

    if path.isfile(features_profile_created_file):
        # Read the created file
        with open(features_profile_created_file, 'r') as f:
            fp_created_str = f.read()
        fp_created = literal_eval(fp_created_str)
        new_fp_id = fp_created[0]

        return str(new_fp_id), True, True, fail_msg, trace

    # Have data
    if path.isfile(features_file):
        current_logger.info('create_features_profile :: features_file exists: %s' % features_file)
    else:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: create_features_profile :: features_file does not exist: %s' % features_file
        current_logger.error('%s' % fail_msg)
        if context == 'training' or context == 'features_profile':
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        else:
            return False, False, False, fail_msg, trace

    # @added 20191029 - Task #3302: Handle csv.reader in py3
    #                      Branch #3262: py3
    if python_version == 3:
        try:
            codecs
        except:
            import codecs

    features_data = []
    try:
        # @modified 20191029 - Task #3302: Handle csv.reader in py3
        #                      Branch #3262: py3
        # with open(features_file, 'rb') as fr:
        #     reader = csv.reader(fr, delimiter=',')
        if python_version == 2:
            with_open = open(features_file, 'rb')
        else:
            with_open = open(features_file, 'r', newline='', encoding='utf-8')

        with with_open as fr:
            if python_version == 2:
                reader = csv.reader(fr, delimiter=',')
            else:
                # reader = csv.reader(codecs.iterdecode(fr, 'utf-8'), delimiter=',')
                reader = csv.reader(fr, delimiter=',')
            # current_logger.debug('debug :: accquired reader')
            # for i, line in enumerate(reader):
            #     current_logger.debug('debug :: %s, %s' % (str(i), str(line[0])))
            for i, line in enumerate(reader):
                feature_name_item = False
                fname_id = False
                f_value = False
                if LOCAL_DEBUG:
                    current_logger.debug('debug :: line - %s' % str(line))
                feature_name = str(line[0])
                if LOCAL_DEBUG:
                    current_logger.debug('debug :: feature_name - %s' % feature_name)
                if feature_name == '':
                    continue
                if python_version == 2:
                    try:
                        feature_name_item = filter(
                            lambda x: x[1] == feature_name, TSFRESH_FEATURES)
                    except:
                        continue
                    if feature_name_item:
                        feature_name_list = feature_name_item[0]
                        fname_id = int(feature_name_list[0])
                    f_value = str(line[1])
                else:
                    try:
                        for tsfresh_id, tsfresh_feature_name in TSFRESH_FEATURES:
                            if feature_name == tsfresh_feature_name:
                                feature_name_item = [tsfresh_id, tsfresh_feature_name]
                                break
                    except:
                        continue
                    if feature_name_item:
                        fname_id = feature_name_item[0]
                        if LOCAL_DEBUG:
                            current_logger.debug('debug :: fname_id - %s' % str(fname_id))
                    f_value = str(line[1])
                if fname_id and f_value:
                    features_data.append([fname_id, f_value])
    except:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: failed iterate csv data from %s' % (current_skyline_app, str(features_file))
        current_logger.error(fail_msg)
        if context == 'training' or context == 'features_profile':
            # Raise to webbapp to provide traceback to user in UI
            raise
        else:
            return False, False, False, fail_msg, trace

    # @added 20170113 - Feature #1854: Ionosphere learn - generations
    # Set the learn generations variables with the IONOSPHERE_LEARN_DEFAULT_ and any
    # settings.IONOSPHERE_LEARN_NAMESPACE_CONFIG values.  These will later be
    # overridden by any database values determined for the specific metric if
    # they exist.
    # Set defaults
    use_full_duration_days = int(settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS)
    valid_learning_duration = int(settings.IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS)
    max_generations = int(settings.IONOSPHERE_LEARN_DEFAULT_MAX_GENERATIONS)
    max_percent_diff_from_origin = float(settings.IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN)
    try:
        use_full_duration, valid_learning_duration, use_full_duration_days, max_generations, max_percent_diff_from_origin = get_ionosphere_learn_details(current_skyline_app, base_name)
        learn_full_duration_days = use_full_duration_days
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: create_features_profile :: failed to get_ionosphere_learn_details')

    current_logger.info('create_features_profile :: learn_full_duration_days     :: %s days' % (str(learn_full_duration_days)))
    current_logger.info('create_features_profile :: valid_learning_duration      :: %s seconds' % (str(valid_learning_duration)))
    current_logger.info('create_features_profile :: max_generations              :: %s' % (str(max_generations)))
    current_logger.info('create_features_profile :: max_percent_diff_from_origin :: %s' % (str(max_percent_diff_from_origin)))

    current_logger.info('create_features_profile :: getting MySQL engine')
    try:
        engine, fail_msg, trace = fp_create_get_an_engine(current_skyline_app)
        current_logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: create_features_profile :: could not get a MySQL engine'
        current_logger.error('%s' % fail_msg)
        if context == 'training' or context == 'features_profile':
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        else:
            return False, False, False, fail_msg, trace

    if not engine:
        trace = 'none'
        fail_msg = 'error :: create_features_profile :: engine not obtained'
        current_logger.error(fail_msg)
        if context == 'training' or context == 'features_profile':
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        else:
            return False, False, False, fail_msg, trace

    # Get metric details from the database
    metrics_id = False

    # Use the learn details as per config
    metric_learn_full_duration_days = int(use_full_duration_days)
    metric_learn_valid_ts_older_than = int(valid_learning_duration)
    metric_max_generations = int(max_generations)
    metric_max_percent_diff_from_origin = int(max_percent_diff_from_origin)

    metrics_table = None
    try:
        metrics_table, fail_msg, trace = metrics_table_meta(current_skyline_app, engine)
        current_logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: create_features_profile :: failed to get metrics_table meta for %s' % base_name
        current_logger.error('%s' % fail_msg)
        if context == 'training' or context == 'features_profile':
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                fp_create_engine_disposal(current_skyline_app, engine)
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        else:
            current_logger.info('create_features_profile :: disposing of any engine')
            fp_create_engine_disposal(current_skyline_app, engine)
            return False, False, False, fail_msg, trace

    current_logger.info('create_features_profile :: metrics_table OK')

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
        for row in result:
            metrics_id = row['id']
            # @added 20170113 - Feature #1854: Ionosphere learn - generations
            # Added Ionosphere LEARN generation related variables
            try:
                metric_learn_full_duration_days = int(row['learn_full_duration_days'])
                metric_learn_valid_ts_older_than = int(row['learn_valid_ts_older_than'])
                metric_max_generations = int(row['max_generations'])
                metric_max_percent_diff_from_origin = float(row['max_percent_diff_from_origin'])
            except:
                current_logger.error('error :: create_features_profile :: failed to determine learn related values from DB for %s' % base_name)
        row = result.fetchone()
        # metric_db_object = row
        connection.close()
        current_logger.info('create_features_profile :: determined db metric id: %s' % str(metrics_id))
        current_logger.info('create_features_profile :: determined db metric learn_full_duration_days: %s' % str(metric_learn_full_duration_days))
        current_logger.info('create_features_profile :: determined db metric learn_valid_ts_older_than: %s' % str(metric_learn_valid_ts_older_than))
        current_logger.info('create_features_profile :: determined db metric max_generations: %s' % str(metric_max_generations))
        current_logger.info('create_features_profile :: determined db metric max_percent_diff_from_origin: %s' % str(metric_max_percent_diff_from_origin))
    except:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: create_features_profile :: could not determine id of metric from DB: %s' % base_name
        current_logger.error('%s' % fail_msg)

    if metric_learn_full_duration_days:
        learn_full_duration_days = metric_learn_full_duration_days
        # learn_full_duration = int(learn_full_duration_days) * 86400
    if metric_learn_valid_ts_older_than:
        learn_valid_ts_older_than = metric_learn_valid_ts_older_than
    if metric_max_generations:
        max_generations = metric_max_generations
    if metric_max_percent_diff_from_origin:
        max_percent_diff_from_origin = metric_max_percent_diff_from_origin
    current_logger.info('create_features_profile :: generation info - learn_full_duration_days     :: %s' % (str(learn_full_duration_days)))
    current_logger.info('create_features_profile :: generation info - learn_valid_ts_older_than    :: %s' % (str(learn_valid_ts_older_than)))
    current_logger.info('create_features_profile :: generation info - max_generations              :: %s' % (str(max_generations)))
    current_logger.info('create_features_profile :: generation info - max_percent_diff_from_origin :: %s' % (str(max_percent_diff_from_origin)))

    # @added 20170120 - Feature #1854: Ionosphere learn
    # Always use the timestamp from the anomaly file
    use_anomaly_timestamp = int(requested_timestamp)
    # @modified 20190327 - Feature #2484: FULL_DURATION feature profiles
    # Added ionosphere_echo
    if context == 'ionosphere_learn' or context == 'ionosphere_echo' or context == 'ionosphere_echo_check':
        if path.isfile(anomaly_check_file):
            current_logger.info('create_features_profile :: determining the metric_timestamp from anomaly_check_file - %s' % anomaly_check_file)
            # Read the details file
            with open(anomaly_check_file, 'r') as f:
                anomaly_details = f.readlines()
                for i, line in enumerate(anomaly_details):
                    if 'metric_timestamp' in line:
                        _metric_timestamp = '%s' % str(line).split("'", 2)
                        metric_timestamp_array = literal_eval(_metric_timestamp)
                        use_anomaly_timestamp = (int(metric_timestamp_array[1]))
                        current_logger.info('create_features_profile :: determined the anomaly metric_timestamp as - %s' % str(use_anomaly_timestamp))

    ionosphere_table = None
    try:
        ionosphere_table, fail_msg, trace = ionosphere_table_meta(current_skyline_app, engine)
        current_logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: create_features_profile :: failed to get ionosphere_table meta for %s' % base_name
        current_logger.error('%s' % fail_msg)
        if context == 'training' or context == 'features_profile':
            # Raise to webbapp I believe to provide traceback to user in UI
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                fp_create_engine_disposal(current_skyline_app, engine)
            raise
        else:
            current_logger.info('create_features_profile :: disposing of any engine')
            fp_create_engine_disposal(current_skyline_app, engine)
            return False, False, False, fail_msg, trace

    current_logger.info('create_features_profile :: ionosphere_table OK')

    # @added 20170403 - Feature #2000: Ionosphere - validated
    # Set all learn_fp_human features profiles to validated.
    fp_validated = 0
    if ionosphere_job == 'learn_fp_human':
        fp_validated = 1

    # @added 20170424 - Feature #2000: Ionosphere - validated
    # Set all generation 0 and 1 as validated
    if int(fp_generation) <= 1:
        fp_validated = 1

    # @modified 20190327 - Feature #2484: FULL_DURATION feature profiles
    # Added ionosphere_echo
    if context == 'ionosphere_echo' or context == 'ionosphere_echo_check':
        echo_fp_value = 1
    else:
        echo_fp_value = 0

    new_fp_id = False
    try:
        connection = engine.connect()
        # @added 20170113 - Feature #1854: Ionosphere learn
        # Added learn values parent_id, generation
        # @modified 20170120 - Feature #1854: Ionosphere learn
        # Added anomaly_timestamp
        # @modified 20170403 - Feature #2000: Ionosphere - validated
        # @modified 20190327 - Feature #2484: FULL_DURATION feature profiles
        # Added ionosphere_echo echo_fp
        # @modified 20190327 - Feature #2484: FULL_DURATION feature profiles
        # Handle ionosphere_echo change in timestamp to the next second and a mismatch
        # of 1 second between the features profile directory timestamp and the DB
        # created_timestamp
        # @modified 20190919 - Feature #3230: users DB table
        #                      Ideas #2476: Label and relate anomalies
        #                      Feature #2516: Add label to features profile
        # Added user_id and label
        if context == 'ionosphere_echo' or context == 'ionosphere_echo_check':
            ts_for_db = int(requested_timestamp)
            db_created_timestamp = datetime.utcfromtimestamp(ts_for_db).strftime('%Y-%m-%d %H:%M:%S')
            ins = ionosphere_table.insert().values(
                metric_id=int(metrics_id), full_duration=int(ts_full_duration),
                anomaly_timestamp=int(use_anomaly_timestamp),
                enabled=1, tsfresh_version=str(tsfresh_version),
                calc_time=calculated_time, features_count=fcount,
                features_sum=fsum, parent_id=fp_parent_id,
                generation=fp_generation, validated=fp_validated,
                echo_fp=echo_fp_value, created_timestamp=db_created_timestamp,
                user_id=user_id, label=label)
        else:
            ins = ionosphere_table.insert().values(
                metric_id=int(metrics_id), full_duration=int(ts_full_duration),
                anomaly_timestamp=int(use_anomaly_timestamp),
                enabled=1, tsfresh_version=str(tsfresh_version),
                calc_time=calculated_time, features_count=fcount,
                features_sum=fsum, parent_id=fp_parent_id,
                generation=fp_generation, validated=fp_validated,
                echo_fp=echo_fp_value, user_id=user_id, label=label)
        result = connection.execute(ins)
        connection.close()
        new_fp_id = result.inserted_primary_key[0]
        current_logger.info('create_features_profile :: new ionosphere fp_id: %s' % str(new_fp_id))
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: create_features_profile :: failed to insert a new record into the ionosphere table for %s' % base_name
        current_logger.error('%s' % fail_msg)
        if context == 'training' or context == 'features_profile':
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                fp_create_engine_disposal(current_skyline_app, engine)
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        else:
            current_logger.info('create_features_profile :: disposing of any engine')
            fp_create_engine_disposal(current_skyline_app, engine)
            return False, False, False, fail_msg, trace

    if not RepresentsInt(new_fp_id):
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: create_features_profile :: unknown new ionosphere new_fp_id for %s' % base_name
        current_logger.error('%s' % fail_msg)
        if context == 'training' or context == 'features_profile':
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                fp_create_engine_disposal(current_skyline_app, engine)
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        else:
            current_logger.info('create_features_profile :: disposing of any engine')
            fp_create_engine_disposal(current_skyline_app, engine)
            return False, False, False, fail_msg, trace

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
            # @modified 20180324 - Bug #2340: MySQL key_block_size
            #                      MySQL key_block_size #45
            # Removed as under MySQL 5.7 breaks
            # mysql_key_block_size='255',
            mysql_engine='InnoDB')
        fp_metric_table.create(engine, checkfirst=True)
        fp_table_created = True
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: create_features_profile :: failed to create table - %s' % fp_table_name
        current_logger.error('%s' % fail_msg)
        if context == 'training' or context == 'features_profile':
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                fp_create_engine_disposal(current_skyline_app, engine)
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        else:
            current_logger.info('create_features_profile :: %s - automated so the table should exists continuing' % context)

    if not fp_table_created:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: create_features_profile :: failed to determine True for create table - %s' % fp_table_name
        current_logger.error('%s' % fail_msg)
        if context == 'training' or context == 'features_profile':
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                fp_create_engine_disposal(current_skyline_app, engine)
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        else:
            current_logger.info('create_features_profile :: %s - automated so the table should exists continuing' % context)

    # Insert features and values
    insert_statement = []
    for fname_id, f_value in features_data:
        insert_statement.append({'fp_id': new_fp_id, 'feature_id': fname_id, 'value': f_value},)
    if insert_statement == []:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: create_features_profile :: empty insert_statement for %s inserts' % fp_table_name
        current_logger.error('%s' % fail_msg)
        # raise
    # else:
        # feature_count = sum(1 for x in a if isinstance(x, insert_statement))
        # current_logger.info(
        #     'fp_id - %s - %s feature values in insert_statement for %s ' %
        #     (str(feature_count), str(new_fp_id), fp_table_name))
        # feature_count = sum(1 for x in a if isinstance(x, insert_statement))
        # current_logger.info(
        #     'fp_id - %s - feature values in insert_statement for %s ' %
        #     (str(new_fp_id), fp_table_name))

    try:
        connection = engine.connect()
        connection.execute(fp_metric_table.insert(), insert_statement)
        connection.close()
        current_logger.info('create_features_profile :: fp_id - %s - feature values inserted into %s' % (str(new_fp_id), fp_table_name))
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: create_features_profile :: failed to insert a feature values into %s' % fp_table_name
        current_logger.error('%s' % fail_msg)
        if context == 'training' or context == 'features_profile':
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                fp_create_engine_disposal(current_skyline_app, engine)
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        else:
            current_logger.info('create_features_profile :: %s - automated so the table should exists continuing' % context)

    # Create metric ts table if not exists ts_<metric_id>
    # Create z_ts_<metric_id> table
    # @modified 20170121 - Feature #1854: Ionosphere learn - generations
    # TODO Adding the option to not save timeseries to DB, as default?
    # ts_table_created = False
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
            # @modified 20180324 - Bug #2340: MySQL key_block_size
            #                      MySQL key_block_size #45
            # Removed as under MySQL 5.7 breaks
            # mysql_key_block_size='255',
            mysql_engine='InnoDB')
        ts_metric_table.create(engine, checkfirst=True)
        # ts_table_created = True
        current_logger.info('create_features_profile :: metric ts table created OK - %s' % (ts_table_name))
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: create_features_profile :: failed to create table - %s' % ts_table_name
        current_logger.error('%s' % fail_msg)
        if context == 'training' or context == 'features_profile':
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                fp_create_engine_disposal(current_skyline_app, engine)
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        else:
            current_logger.info('create_features_profile :: %s - automated so the table should exists continuing' % context)

    # Insert timeseries that the features profile was created from
    raw_timeseries = []
    anomaly_json = '%s/%s.json' % (metric_training_data_dir, base_name)

    # @added 20190327 - Feature #2484: FULL_DURATION feature profiles
    if context == 'ionosphere_echo' or context == 'ionosphere_echo_check':
        full_duration_in_hours = int(settings.FULL_DURATION / 60 / 60)
        anomaly_json = '%s/%s.mirage.redis.%sh.json' % (metric_training_data_dir, base_name, str(full_duration_in_hours))

    if path.isfile(anomaly_json):
        current_logger.info('create_features_profile :: metric anomaly json found OK - %s' % (anomaly_json))
        try:
            # Read the timeseries json file
            with open(anomaly_json, 'r') as f:
                raw_timeseries = f.read()
        except:
            trace = traceback.format_exc()
            current_logger.error(trace)
            fail_msg = 'error :: create_features_profile :: failed to read timeseries data from %s' % anomaly_json
            current_logger.error('%s' % (fail_msg))
            fail_msg = 'error: failed to read timeseries data from %s' % anomaly_json
            # end = timer()
            if context == 'training' or context == 'features_profile':
                # @added 20170806 - Bug #2130: MySQL - Aborted_clients
                # Added missing disposal
                if engine:
                    fp_create_engine_disposal(current_skyline_app, engine)
                # Raise to webbapp I believe to provide traceback to user in UI
                raise
    else:
        trace = 'none'
        fail_msg = 'error: file not found - %s' % (anomaly_json)
        current_logger.error(fail_msg)
        # raise

    # Convert the timeseries to csv
    timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
    del raw_timeseries
    timeseries = literal_eval(timeseries_array_str)

    datapoints = timeseries
    validated_timeseries = []
    for datapoint in datapoints:
        try:
            new_datapoint = [str(int(datapoint[0])), float(datapoint[1])]
            validated_timeseries.append(new_datapoint)
        # @modified 20170913 - Task #2160: Test skyline with bandit
        # Added nosec to exclude from bandit tests
        except:  # nosec
            continue

    del timeseries
    del timeseries_array_str
    del datapoints

    insert_statement = []
    for ts, value in validated_timeseries:
        insert_statement.append({'fp_id': new_fp_id, 'timestamp': ts, 'value': value},)
    try:
        connection = engine.connect()
        connection.execute(ts_metric_table.insert(), insert_statement)
        connection.close()
        current_logger.info('create_features_profile :: fp_id - %s - timeseries inserted into %s' % (str(new_fp_id), ts_table_name))
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: create_features_profile :: failed to insert the timeseries into %s' % ts_table_name
        current_logger.error('%s' % fail_msg)
        if context == 'training' or context == 'features_profile':
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                fp_create_engine_disposal(current_skyline_app, engine)
            raise
        else:
            current_logger.info('create_features_profile :: %s - automated so the table should exist continuing' % context)

    # @added 20200512 - Bug #2534: Ionosphere - fluid approximation - IONOSPHERE_MINMAX_SCALING_RANGE_TOLERANCE on low ranges
    #                   Feature #2404: Ionosphere - fluid approximation
    # Due to the loss of resolution in the Grpahite graph images due
    # to their size, create a matplotlib graph for the DB fp time
    # series data for more accurate validation
    fp_ts_graph_file = '%s/%s.fp_id_ts.%s.matplotlib.png' % (
        metric_training_data_dir, base_name, str(new_fp_id))
    if not path.isfile(fp_ts_graph_file):
        try:
            created_fp_ts_graph, fp_ts_graph_file = create_fp_ts_graph(current_skyline_app, metric_training_data_dir, base_name, int(new_fp_id), int(use_anomaly_timestamp), validated_timeseries)
        except:
            trace = traceback.format_exc()
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: create_features_profile :: failed to created_fp_ts_graph for %s fp id %s' % (base_name, str(new_fp_id)))

    del validated_timeseries
    del insert_statement

    # Create a created features profile file
    try:
        # data = '[%s, %s, ]' % (new_fp_id, str(int(time.time())))
        # write_data_to_file(skyline_app, features_profile_created_file, 'w', data)
        # @modified 20170115 -  Feature #1854: Ionosphere learn - generations
        # Added parent_id and generation
        data = '[%s, %s, \'%s\', %s, %s, %s, %s, %s, %s]' % (
            new_fp_id, str(int(time.time())), str(tsfresh_version),
            str(calculated_time), str(fcount), str(fsum), str(ts_full_duration),
            str(fp_parent_id), str(fp_generation))
        write_data_to_file(current_skyline_app, features_profile_created_file, 'w', data)
        del data
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: create_features_profile :: failed to write fp.created file'
        current_logger.error('%s' % fail_msg)

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
        current_logger.info('create_features_profile :: ionosphere_enabled set on metric id: %s' % str(metrics_id))
    except:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: create_features_profile :: could not update metrics table and set ionosphere_enabled on id %s' % str(metrics_id)
        current_logger.error('%s' % fail_msg)
        # raise

    # Copy data from training data dir to features_profiles dir
    if not path.isdir(ts_features_profile_dir):
        mkdir_p(ts_features_profile_dir)

    if path.isdir(ts_features_profile_dir):
        current_logger.info('create_features_profile :: fp_id - %s - features profile dir created - %s' % (str(new_fp_id), ts_features_profile_dir))
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
            current_logger.error('%s' % trace)
            current_logger.error('error :: create_features_profile :: glob - fp_id - %s - training data not copied to %s' % (str(new_fp_id), ts_features_profile_dir))

        for i_file in data_files:
            try:
                shutil.copy(i_file, ts_features_profile_dir)
                current_logger.info('create_features_profile :: fp_id - %s - training data copied - %s' % (str(new_fp_id), i_file))
            except shutil.Error as e:
                trace = traceback.format_exc()
                current_logger.error('%s' % trace)
                current_logger.error('error :: create_features_profile :: shutil error - fp_id - %s - training data not copied to %s' % (str(new_fp_id), ts_features_profile_dir))
                current_logger.error('error :: create_features_profile :: %s' % (e))
            # Any error saying that the directory doesn't exist
            except OSError as e:
                trace = traceback.format_exc()
                current_logger.error('%s' % trace)
                current_logger.error('error :: create_features_profile :: OSError error - fp_id - %s - training data not copied to %s' % (str(new_fp_id), ts_features_profile_dir))
                current_logger.error('error :: create_features_profile :: %s' % (e))
        current_logger.info('create_features_profile :: fp_id - %s - training data copied to %s' % (str(new_fp_id), ts_features_profile_dir))
    else:
        current_logger.error('error :: create_features_profile :: fp_id - %s - training data not copied to %s' % (str(new_fp_id), ts_features_profile_dir))

    # @added 20190501 - Branch #2646: slack
    try:
        SLACK_ENABLED = settings.SLACK_ENABLED
    except:
        SLACK_ENABLED = False
    try:
        slack_thread_updates = settings.SLACK_OPTS['thread_updates']
    except:
        slack_thread_updates = False
    try:
        message_on_features_profile_created = settings.SLACK_OPTS['message_on_features_profile_created']
    except:
        message_on_features_profile_created = False
    try:
        message_on_features_profile_learnt = settings.SLACK_OPTS['message_on_features_profile_learnt']
    except:
        message_on_features_profile_learnt = False
    update_slack_thread = False
    if context == 'training_data':
        if echo_fp_value == 0 and slack_thread_updates and SLACK_ENABLED:
            update_slack_thread = True
        if not message_on_features_profile_created:
            update_slack_thread = False
    if context == 'ionosphere_learn':
        if echo_fp_value == 0 and slack_thread_updates and SLACK_ENABLED:
            update_slack_thread = True
        if not message_on_features_profile_learnt:
            update_slack_thread = False
    if update_slack_thread:
        current_logger.info('create_features_profile :: updating slack')
        # Determine the anomaly id and then the slack_thread_ts
        try:
            anomalies_table, fail_msg, trace = anomalies_table_meta(current_skyline_app, engine)
            current_logger.info(fail_msg)
        except:
            trace = traceback.format_exc()
            current_logger.error('%s' % trace)
            fail_msg = 'error :: create_features_profile :: failed to get anomalies_table meta'
            current_logger.error('%s' % fail_msg)
        anomaly_id = None
        slack_thread_ts = 0
        try:
            connection = engine.connect()
            stmt = select([anomalies_table]).\
                where(anomalies_table.c.metric_id == metrics_id).\
                where(anomalies_table.c.anomaly_timestamp == int(use_anomaly_timestamp))
            result = connection.execute(stmt)
            for row in result:
                anomaly_id = row['id']
                slack_thread_ts = row['slack_thread_ts']
                break
            connection.close()
            current_logger.info('create_features_profile :: determined anomaly id %s for metric id %s at anomaly_timestamp %s with slack_thread_ts %s' % (
                str(anomaly_id), str(metrics_id),
                str(use_anomaly_timestamp), str(slack_thread_ts)))
        except:
            trace = traceback.format_exc()
            current_logger.error(trace)
            fail_msg = 'error :: create_features_profile :: could not determine id of anomaly from DB for metric id %s at anomaly_timestamp %s' % (
                str(metrics_id), str(use_anomaly_timestamp))
            current_logger.error('%s' % fail_msg)
        try:
            if float(slack_thread_ts) == 0:
                slack_thread_ts = None
        except:
            slack_thread_ts = None

        try:
            ionosphere_link = '%s/ionosphere?fp_view=true&fp_id=%s&metric=%s' % (
                settings.SKYLINE_URL, str(new_fp_id), base_name)
        except:
            trace = traceback.format_exc()
            current_logger.error(traceback.format_exc())
            current_logger.error('failed to interpolated the ionosphere_link')
            ionosphere_link = 'URL link failed to build'

        if not fp_learn:
            message = '*TRAINED - not anomalous* - features profile id %s was created for %s via %s - %s' % (
                str(new_fp_id), base_name, current_skyline_app, ionosphere_link)
        else:
            message = '*TRAINED - not anomalous* - features profile id %s was created for %s via %s - %s AND set to *LEARN* at %s days' % (
                str(new_fp_id), base_name, current_skyline_app, ionosphere_link,
                str(learn_full_duration_days))

        if context == 'ionosphere_learn':
            if slack_ionosphere_job != 'learn_fp_human':
                message = '*LEARNT - not anomalous* - features profile id %s was created for %s via %s - %s' % (
                    str(new_fp_id), base_name, current_skyline_app, ionosphere_link)
            else:
                message = '*LEARNING - not anomalous at %s days* - features profile id %s was created using %s days data for %s via %s - %s' % (
                    str(learn_full_duration_days), str(new_fp_id),
                    str(learn_full_duration_days), base_name,
                    current_skyline_app, ionosphere_link)

        channel = None
        try:
            channel = settings.SLACK_OPTS['default_channel']
            channel_id = settings.SLACK_OPTS['default_channel_id']
        except:
            channel = False
            channel_id = False
            fail_msg = 'error :: create_features_profile :: could not determine the slack default_channel or default_channel_id from settings.SLACK_OPTS please add these to your settings or set SLACK_ENABLED or SLACK_OPTS[\'thread_updates\'] to False and restart ionosphere and webapp'
            current_logger.error('%s' % fail_msg)
        throw_exception_on_default_channel = False
        if channel == 'YOUR_default_slack_channel':
            throw_exception_on_default_channel = True
            channel = False
        if channel_id == 'YOUR_default_slack_channel_id':
            throw_exception_on_default_channel = True
            channel_id = False
        if throw_exception_on_default_channel:
            fail_msg = 'error :: create_features_profile :: the default_channel or default_channel_id from settings.SLACK_OPTS is set to the default, please replace these with your channel details or set SLACK_ENABLED or SLACK_OPTS[\'thread_updates\'] to False and restart webapp'
            current_logger.error('%s' % fail_msg)

        slack_response = {'ok': False}
        if channel:
            try:
                slack_response = slack_post_message(current_skyline_app, channel, str(slack_thread_ts), message)
            except:
                trace = traceback.format_exc()
                current_logger.error(trace)
                fail_msg = 'error :: create_features_profile :: failed to slack_post_message'
                current_logger.error('%s' % fail_msg)
            if not slack_response['ok']:
                fail_msg = 'error :: create_features_profile :: failed to slack_post_message, slack dict output follows'
                current_logger.error('%s' % fail_msg)
                current_logger.error('%s' % str(slack_response))
            else:
                current_logger.info('create_features_profile :: posted slack update to %s, thread %s' % (
                    channel, str(slack_thread_ts)))
        if channel_id:
            if slack_thread_ts:
                try:
                    reaction_emoji = settings.SLACK_OPTS['message_on_features_profile_created_reaction_emoji']
                except:
                    reaction_emoji = 'thumbsup'
                slack_response = {'ok': False}
                try:
                    slack_response = slack_post_reaction(current_skyline_app, channel_id, str(slack_thread_ts), reaction_emoji)
                    if LOCAL_DEBUG:
                        current_logger.info('create_features_profile :: slack_response - %s' % (
                            str(slack_response)))
                except:
                    trace = traceback.format_exc()
                    current_logger.error(trace)
                    fail_msg = 'error :: create_features_profile :: failed to slack_post_reaction'
                    current_logger.error('%s' % fail_msg)

                if LOCAL_DEBUG:
                    if slack_response:
                        current_logger.info('create_features_profile :: slack_response - %s' % (
                            str(slack_response)))

                if not slack_response['ok']:
                    if str(slack_response['error']) == 'already_reacted':
                        current_logger.info(
                            'slack_post_reaction :: already_reacted to channel %s, thread %s, ok' % (
                                channel, str(slack_thread_ts)))
                    else:
                        fail_msg = 'error :: create_features_profile :: failed to slack_post_reaction, slack dict output follows'
                        current_logger.error('%s' % fail_msg)
                        current_logger.error('%s' % str(slack_response))
                if context == 'ionosphere_learn':
                    try:
                        reaction_emoji = settings.SLACK_OPTS['message_on_features_profile_learnt_reaction_emoji']
                    except:
                        reaction_emoji = 'heavy_check_mark'
                    slack_response = {'ok': False}
                    try:
                        slack_response = slack_post_reaction(current_skyline_app, channel_id, str(slack_thread_ts), reaction_emoji)
                    except:
                        trace = traceback.format_exc()
                        current_logger.error(trace)
                        fail_msg = 'error :: create_features_profile :: failed to slack_post_reaction'
                        current_logger.error('%s' % fail_msg)
                    if not slack_response['ok']:
                        if str(slack_response['error']) == 'already_reacted':
                            current_logger.info(
                                'slack_post_reaction :: already_reacted to channel %s, thread %s, ok' % (
                                    channel, str(slack_thread_ts)))
                        else:
                            fail_msg = 'error :: create_features_profile :: failed to slack_post_reaction, slack dict output follows'
                            current_logger.error('%s' % fail_msg)
                            current_logger.error('%s' % str(slack_response))

            if context == 'ionosphere_learn':
                try:
                    validate_link = '%s/ionosphere?fp_validate=true&metric=%s&validated_equals=false&limit=0&order=DESC' % (
                        settings.SKYLINE_URL, base_name)
                except:
                    trace = traceback.format_exc()
                    current_logger.error(traceback.format_exc())
                    current_logger.error('failed to interpolated the validate_link')
                    validate_link = 'validate URL link failed to build'
                message = '*Skyline Ionosphere LEARNT* - features profile id %s for %s was learnt by %s - %s - *Please validate this* at %s' % (
                    str(new_fp_id), base_name, current_skyline_app, ionosphere_link, validate_link)
                if slack_ionosphere_job != 'learn_fp_human':
                    slack_response = {'ok': False}
                    try:
                        slack_response = slack_post_message(current_skyline_app, channel, None, message)
                    except:
                        trace = traceback.format_exc()
                        current_logger.error(trace)
                        fail_msg = 'error :: create_features_profile :: failed to slack_post_message'
                        current_logger.error('%s' % fail_msg)
                    if not slack_response['ok']:
                        fail_msg = 'error :: create_features_profile :: failed to slack_post_message, slack dict output follows'
                        current_logger.error('%s' % fail_msg)
                        current_logger.error('%s' % str(slack_response))
                    else:
                        current_logger.info('create_features_profile :: posted slack update to %s, thread %s' % (
                            channel, str(slack_thread_ts)))

    current_logger.info('create_features_profile :: disposing of any engine')
    try:
        if engine:
            fp_create_engine_disposal(current_skyline_app, engine)
        else:
            current_logger.info('create_features_profile :: no engine to dispose of' % (str(new_fp_id), ts_features_profile_dir))
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        current_logger.error('error :: create_features_profile :: OSError error - fp_id - %s - training data not copied to %s' % (str(new_fp_id), ts_features_profile_dir))

    # @added 20170113 - Feature #1854: Ionosphere learn - Redis ionosphere.learn.work namespace
    # Ionosphere learn needs Redis works sets
    # When a features profile is created there needs to be work added to a Redis
    # set. When a human makes a features profile, we want Ionosphere to make a
    # use_full_duration_days features profile valid_learning_duration (e.g.
    # 3361) later.
    if settings.IONOSPHERE_LEARN and new_fp_id:
        create_redis_work_item = False
        if context == 'training_data' and ionosphere_job == 'learn_fp_human':
            create_redis_work_item = True
            # @modified 20170120 -  Feature #1854: Ionosphere learn - generations
            # Added fp_learn parameter to allow the user to not learn the
            # use_full_duration_days
            if not fp_learn:
                create_redis_work_item = False
                current_logger.info('fp_learn is False not adding an item to Redis ionosphere.learn.work set')

        if ionosphere_job == 'learn_fp_automatic':
            create_redis_work_item = True
            # @added 20170131 - Feature #1886 Ionosphere learn - child like parent with evolutionary maturity
            # TODO: here a check may be required to evaluate whether the origin_fp_id
            #       had a use_full_duration features profile created, however
            #       due to the fact that it is in learn, suggests that it did
            #       have, not 100% sure.
            # origin_fp_id_was_allowed_to_learn = False
            child_use_full_duration_count_of_origin_fp_id = 1
            # TODO: Determine the state
            # child_use_full_duration_count_of_origin_fp_id = SELECT COUNT(id) FROM ionosphere WHERE parent_id=origin_fp_id AND full_duration=use_full_duration
            if child_use_full_duration_count_of_origin_fp_id == 0:
                current_logger.info('the origin parent was not allowed to learn not adding to Redis ionosphere.learn.work set')
                create_redis_work_item = False

        if create_redis_work_item:
            try:
                current_logger.info(
                    'adding work to Redis ionosphere.learn.work set - [\'Soft\', \'%s\', %s, \'%s\', %s, %s] to make a learn features profile later' % (
                        str(ionosphere_job), str(requested_timestamp), base_name,
                        str(new_fp_id), str(fp_generation)))
                # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                # @modified 20200216 - Feature #3450: Handle multiple requests to create a features profile
                # redis_conn now provided at the start of this function
                # if settings.REDIS_PASSWORD:
                #     redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                # else:
                #     redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                # @modified 20190414 - Task #2824: Test redis-py upgrade
                #                      Task #2926: Update dependencies
                # redis_conn.sadd('ionosphere.learn.work', ['Soft', str(ionosphere_job), int(requested_timestamp), base_name, int(new_fp_id), int(fp_generation)])
                redis_conn.sadd('ionosphere.learn.work', str(['Soft', str(ionosphere_job), int(requested_timestamp), base_name, int(new_fp_id), int(fp_generation)]))
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error(
                    'error :: failed adding work to Redis ionosphere.learn.work set - [\'Soft\', \'%s\', %s, \'%s\', %s, %s] to make a learn features profile later' % (
                        str(ionosphere_job), str(requested_timestamp), base_name,
                        str(new_fp_id), str(fp_generation)))

        # @added 20170806 - Bug #2130: MySQL - Aborted_clients
        # Added missing disposal
        if engine:
            fp_create_engine_disposal(current_skyline_app, engine)

    # @added 20200108 - Feature #3380: Create echo features profile when a Mirage features profile is created
    # If a Mirage features profile was created by a user add a record to the
    # ionosphere.echo.work Redis so that Ionosphere can run process_ionosphere_echo
    # on the data and create a echo features profile asap
    if settings.IONOSPHERE_ECHO_ENABLED and new_fp_id and context == 'training_data' and ionosphere_job == 'learn_fp_human':
        if int(ts_full_duration) > int(settings.FULL_DURATION):
            # @modified 20200216 - Feature #3450: Handle multiple requests to create a features profile
            # redis_conn now provided at the start of this function
            # if settings.REDIS_PASSWORD:
            #     redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
            # else:
            #     redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
            ionosphere_job = 'echo_fp_human'
            redis_conn.sadd('ionosphere.echo.work', str(['Soft', str(ionosphere_job), int(requested_timestamp), base_name, int(new_fp_id), int(fp_generation), int(ts_full_duration)]))

    # @added 20200216 - Feature #3450: Handle multiple requests to create a features profile
    # Ensure that one features profile can only be created if
    # multiple requests are received to create a features profile
    try:
        redis_conn.delete(fp_pending_cache_key)
    except:
        pass

    return str(new_fp_id), True, False, fail_msg, trace


# @added 20180414 - Branch #2270: luminosity
def get_correlations(current_skyline_app, anomaly_id):
    """
    Get all the correlations for an anomaly from the database

    :param current_skyline_app: the Skyline app name calling the function
    :param anomaly_id: the panorama anomaly id
    :type current_skyline_app: str
    :type anomaly_id: int
    :return: list
    :return: [[metric_name, coefficient, shifted, shifted_coefficient],[metric_name, coefficient, ...]]
    :rtype: [[str, float, float, float]]
    """

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    func_name = 'get_correlations'

    correlations = []

    current_logger.info('get_correlations :: getting MySQL engine')
    try:
        engine, fail_msg, trace = fp_create_get_an_engine(current_skyline_app)
        current_logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: could not get a MySQL engine'
        current_logger.error('%s' % fail_msg)
        # return False, False, fail_msg, trace, False
        raise  # to webapp to return in the UI

    metrics_table = None
    try:
        metrics_table, fail_msg, trace = metrics_table_meta(current_skyline_app, engine)
        current_logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to get metrics_table_meta' % func_name
        current_logger.error('%s' % fail_msg)

    luminosity_table = None
    try:
        luminosity_table, fail_msg, trace = luminosity_table_meta(current_skyline_app, engine)
        current_logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to get luminosity_table_meta' % func_name
        current_logger.error('%s' % fail_msg)

    metrics_list = []
    try:
        connection = engine.connect()
        stmt = select([metrics_table]).where(metrics_table.c.id > 0)
        results = connection.execute(stmt)
        for row in results:
            metric_id = row['id']
            metric_name = row['metric']
            metrics_list.append([int(metric_id), str(metric_name)])
        connection.close()
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: could not determine metrics from MySQL')
        if engine:
            fp_create_engine_disposal(current_skyline_app, engine)
        raise

    try:
        connection = engine.connect()
        stmt = select([luminosity_table]).where(luminosity_table.c.id == int(anomaly_id))
        result = connection.execute(stmt)
        for row in result:
            metric_id = row['metric_id']
            metric_name = None
            if metric_id:
                # @modified 20180723 - Feature #2470: Correlations Graphite graph links
                #                      Branch #2270: luminosity
                # Return the metric_name as a string not a list, so that the
                # metric_name string can be used to build links to Graphite
                # graphs via webapp.py and correlations.html template.
                # metric_name = [metrics_list_name for metrics_list_id, metrics_list_name in metrics_list if int(metric_id) == int(metrics_list_id)]
                metric_name_list = [metrics_list_name for metrics_list_id, metrics_list_name in metrics_list if int(metric_id) == int(metrics_list_id)]
                metric_name = str(metric_name_list[0])
            coefficient = row['coefficient']
            shifted = row['shifted']
            shifted_coefficient = row['shifted_coefficient']
            correlations.append([metric_name, coefficient, shifted, shifted_coefficient])
        connection.close()
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: could not determine correlations for anomaly id -  %s' % str(anomaly_id))
        if engine:
            fp_create_engine_disposal(current_skyline_app, engine)
        del metrics_list
        raise

    if engine:
        fp_create_engine_disposal(current_skyline_app, engine)

    if correlations:
        sorted_correlations = sorted(correlations, key=lambda x: x[1], reverse=True)
        correlations = sorted_correlations

    try:
        del metrics_list
    except:
        pass

    return correlations, fail_msg, trace


# @added 20200113 - Feature #3390: luminosity related anomalies
#                   Branch #2270: luminosity
def get_related(current_skyline_app, anomaly_id, anomaly_timestamp):
    """
    Get all the related anomalies from the database

    :param current_skyline_app: the Skyline app name calling the function
    :param anomaly_id: the panorama anomaly id
    :param anomaly_timestamp: the anomaly timestamp
    :type current_skyline_app: str
    :type anomaly_id: int
    :type anomaly_timestamp: int
    :return: list
    :return: [[metric_name, related_timestamp],[metric_name, related_timestamp],...], [[timestamp, label]]
    :rtype: [[str, int]]
    """

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    func_name = 'get_related'

    related = []

    # @added 20200808 - Feature #3568: Ionosphere - report anomalies in training period
    labelled_anomalies = []

    # @added 20200419 - Feature #3390: luminosity related anomalies
    #                   Branch #2270: luminosity
    try:
        related_time_period = int(settings.LUMINOSITY_RELATED_TIME_PERIOD)
    except:
        related_time_period = 120

    # @modified 20200419 - Feature #3390: luminosity related anomalies
    #                      Branch #2270: luminosity
    # Use settings.LUMINOSITY_RELATED_TIME_PERIOD
    # minus_two_minutes = int(anomaly_timestamp) - 120
    # plus_two_minutes = int(anomaly_timestamp) + 120
    minus_two_minutes = int(anomaly_timestamp) - related_time_period
    plus_two_minutes = int(anomaly_timestamp) + related_time_period

    current_logger.info('get_related :: getting MySQL engine for %s and timestamp %s' % (
        str(anomaly_id), str(anomaly_timestamp)))
    try:
        engine, fail_msg, trace = fp_create_get_an_engine(current_skyline_app)
        current_logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: could not get a MySQL engine'
        current_logger.error('%s' % fail_msg)
        # return False, False, fail_msg, trace, False
        raise  # to webapp to return in the UI

    metrics_table = None
    try:
        metrics_table, fail_msg, trace = metrics_table_meta(current_skyline_app, engine)
        current_logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to get metrics_table_meta' % func_name
        current_logger.error('%s' % fail_msg)

    anomalies_table = None
    try:
        anomalies_table, fail_msg, trace = anomalies_table_meta(current_skyline_app, engine)
        current_logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to get anomalies_table_meta' % func_name
        current_logger.error('%s' % fail_msg)

    metrics_list = []
    try:
        connection = engine.connect()
        stmt = select([metrics_table]).where(metrics_table.c.id > 0)
        results = connection.execute(stmt)
        for row in results:
            metric_id = row['id']
            metric_name = row['metric']
            metrics_list.append([int(metric_id), str(metric_name)])
        connection.close()
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: could not determine metrics from MySQL')
        if engine:
            fp_create_engine_disposal(current_skyline_app, engine)
        raise

    try:
        connection = engine.connect()
        stmt = select([anomalies_table]).where(anomalies_table.c.anomaly_timestamp > minus_two_minutes).where(anomalies_table.c.anomaly_timestamp <= plus_two_minutes).where(anomalies_table.c.id != anomaly_id)
        results = connection.execute(stmt)
        for row in results:
            related_anomaly_id = row['id']
            metric_id = row['metric_id']
            metric_name = None
            if metric_id:
                metric_name_list = [metrics_list_name for metrics_list_id, metrics_list_name in metrics_list if int(metric_id) == int(metrics_list_id)]
                metric_name = str(metric_name_list[0])
            related_anomaly_timestamp = row['anomaly_timestamp']
            related_full_duration = row['full_duration']
            related.append([int(related_anomaly_id), int(metric_id), str(metric_name), int(related_anomaly_timestamp), int(related_full_duration)])
        connection.close()
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: could not determine related anomalies from DB for anomaly id -  %s' % str(anomaly_id))
        if engine:
            fp_create_engine_disposal(current_skyline_app, engine)
        # del metrics_list
        raise

    # @added 20200808 - Feature #3568: Ionosphere - report anomalies in training period
    anomaly_details = []
    try:
        connection = engine.connect()
        stmt = select([anomalies_table]).where(anomalies_table.c.id == anomaly_id)
        results = connection.execute(stmt)
        for row in results:
            related_anomaly_id = row['id']
            metric_id = row['metric_id']
            metric_name = None
            if metric_id:
                metric_name_list = [metrics_list_name for metrics_list_id, metrics_list_name in metrics_list if int(metric_id) == int(metrics_list_id)]
                metric_name = str(metric_name_list[0])
            full_duration = row['full_duration']
            anomaly_details = [int(anomaly_id), int(metric_id), str(metric_name), int(full_duration)]
            break
        connection.close()
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: could not determine anomaly details from DB for anomaly id -  %s' % str(anomaly_id))
        if engine:
            fp_create_engine_disposal(current_skyline_app, engine)
        raise
    if anomaly_details:
        try:
            from_timestamp = int(anomaly_timestamp) - int(full_duration)
            until_timestamp = int(anomaly_timestamp) + 600
            connection = engine.connect()
            stmt = select([anomalies_table]).where(anomalies_table.c.metric_id == int(metric_id)).\
                where(anomalies_table.c.anomaly_timestamp >= from_timestamp).\
                where(anomalies_table.c.anomaly_timestamp < until_timestamp).\
                where(anomalies_table.c.label.isnot(None)).\
                where(anomalies_table.c.label != 'None')
            results = connection.execute(stmt)
            for row in results:
                labelled_anomaly_id = row['id']
                labelled_anomaly_creeated_timestamp = row['created_timestamp']
                labelled_anomaly_timestamp = row['anomaly_timestamp']
                labelled_anomaly_label = row['label']
                labelled_anomalies.append([int(labelled_anomaly_id), labelled_anomaly_creeated_timestamp, int(labelled_anomaly_timestamp), labelled_anomaly_label])
            connection.close()
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: could not determine anomaly details from DB for anomaly id -  %s' % str(anomaly_id))
            if engine:
                fp_create_engine_disposal(current_skyline_app, engine)
            raise

    try:
        del metrics_list
    except:
        pass

    # @modified 20200808 - Feature #3568: Ionosphere - report anomalies in training period
    # return related, fail_msg, trace
    if len(labelled_anomalies) == 0:
        labelled_anomalies = None
    return related, labelled_anomalies, fail_msg, trace
