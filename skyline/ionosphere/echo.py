from __future__ import division
import logging
import os
from time import time, sleep
import shutil
import glob
from sys import version_info
import traceback

from sqlalchemy.sql import (select, desc)

import settings
from skyline_functions import (mkdir_p, get_memcache_metric_object)

from features_profile import calculate_features_profile

from database import (
    get_engine, ionosphere_table_meta, metrics_table_meta)

# @added 2017014 - Feature #1854: Ionosphere learn
from ionosphere_functions import create_features_profile

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
    logger.error('error :: echo :: cannot determine ENABLE_IONOSPHERE_DEBUG from settings' % skyline_app)
    ENABLE_IONOSPHERE_DEBUG = False

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

context = 'ionosphere_echo'


def echo_get_metric_from_metrics(base_name, engine):
    """
    Called by :func:`~echo` and returns the metric id and metric db object

    :param timestamp: timestamp at which learn was called
    :type timestamp: int
    :return: tuple
    :rtype: (int, object)

    """

    logger = logging.getLogger(skyline_app_logger)
    metrics_id = 0
    metric_db_object = None

    # Get the metrics_table metadata
    metrics_table = None
    try:
        metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
        logger.info('echo :: metrics_table OK for %s' % base_name)
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: echo :: failed to get metrics_table meta for %s' % base_name)
        return False

    try:
        connection = engine.connect()
        stmt = select([metrics_table]).where(metrics_table.c.metric == base_name)
        result = connection.execute(stmt)
        row = result.fetchone()
        metric_db_object = row
        metrics_id = row['id']
        connection.close()
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: echo :: could not determine id from metrics table for - %s' % base_name)
        return False

    return metrics_id, metric_db_object


# @added 20190326 - Feature #2484: FULL_DURATION feature profiles
def ionosphere_echo(base_name, mirage_full_duration):

    logger = logging.getLogger(skyline_app_logger)
    child_process_pid = os.getpid()
    context = 'ionosphere_echo'
    logger.info('ionosphere_echo :: started with child_process_pid - %s for %s' % (str(child_process_pid), base_name))
    full_duration_in_hours = int(settings.FULL_DURATION / 60 / 60)

    try:
        # Allow for 3 seconds
        ionosphere_echo_max_fp_create_time = (settings.IONOSPHERE_ECHO_MAX_FP_CREATE_TIME - 3)
    except:
        ionosphere_echo_max_fp_create_time = 52
    echo_started_at = int(time())

    def echo_get_an_engine():

        try:
            engine, fail_msg, trace = get_engine(skyline_app)
            return engine, fail_msg, trace
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: ionosphere_echo :: get_an_engine :: failed to get MySQL engine'
            logger.error('%s' % fail_msg)
            return None, fail_msg, trace

    def echo_engine_disposal(engine):
        try:
            if engine:
                try:
                    engine.dispose()
                    logger.info('ionosphere_echo :: MySQL engine disposed of')
                    return True
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: ionosphere_echo :: calling engine.dispose()')
            else:
                logger.info('ionosphere_echo :: no MySQL engine to dispose of')
                return True
        except:
            return False
        return False

    # Determine the metric details from the database
    metrics_id = None
    metric_db_object = None
    engine = None

    # Get the metric db object data to memcache it is exists
    metric_db_object = get_memcache_metric_object(skyline_app, base_name)
    if metric_db_object:
        metrics_id = metric_db_object['id']
        logger.info('ionosphere_echo :: determined metric id %s from memcache for %s' % (str(metrics_id), base_name))
    else:
        # Only if no memcache data
        # Get a MySQL engine
        try:
            engine, log_msg, trace = echo_get_an_engine()
            logger.info('ionosphere_echo :: %s' % log_msg)
            logger.info('ionosphere_echo :: determining metric id from DB as not found in memcache for %s' % (base_name))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: ionosphere_echo :: could not get a MySQL engine to get metric_db_object')

        if not engine:
            logger.error('error :: ionosphere_echo :: engine not obtained to get metric_db_object')
            logger.info('ionosphere_echo :: exiting this work but not removing work item, as database may be available again before the work expires')
            return

        try:
            metrics_id, metric_db_object = echo_get_metric_from_metrics(base_name, engine)
            echo_engine_disposal(engine)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: ionosphere_echo :: failed get the metric details from the database')
            logger.info('ionosphere_echo :: exiting this work but not removing work item, as database may be available again before the work expires')

        if not metrics_id:
            logger.error('error :: ionosphere_echo :: failed get the metrics_id from the database')
            logger.info('ionosphere_echo :: exiting this work but not removing work item, as database may be available again before the work expires')
            echo_engine_disposal(engine)
            return

        if not metric_db_object:
            logger.error('error :: ionosphere_echo :: failed get the metric_db_object from the database')
            logger.info('ionosphere_echo :: exiting this work but not removing work item, as database may be available again before the work expires')
            echo_engine_disposal(engine)
            return

    # Determine the metric fp ids from the database
    if not engine:
        logger.info('ionosphere_echo :: getting MySQL engine to determine fp ids for metric id %s - %s' % (str(metrics_id), base_name))
        try:
            engine, log_msg, trace = echo_get_an_engine()
            logger.info(log_msg)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: ionosphere_echo :: could not get a MySQL engine to get fp_ids')

    if not engine:
        logger.error('error :: ionosphere_echo :: engine not obtained to get fp_ids')
        return

    try:
        ionosphere_table, log_msg, trace = ionosphere_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('ionosphere_echo :: ionosphere_table OK')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: ionosphere_echo :: failed to get ionosphere_table meta for %s' % base_name)

    # Determine the fp_ids that exist for the metric
    echo_fp_ids_result = []
    try:
        connection = engine.connect()
        stmt = select([ionosphere_table]).where(ionosphere_table.c.metric_id == metrics_id).order_by(desc(ionosphere_table.c.id))
        echo_fp_ids = connection.execute(stmt)
        echo_fp_ids_result = [{column: value for column, value in rowproxy.items()} for rowproxy in echo_fp_ids]
        connection.close()
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: ionosphere_echo :: could not determine fp ids from DB for %s' % base_name)
        return

    if engine:
        echo_engine_disposal(engine)

    if not echo_fp_ids_result:
        logger.error('error :: ionosphere_echo :: no echo_fp_ids_result - could not determine fp ids from DB for %s' % base_name)
    else:
        logger.info('ionosphere_echo :: echo_fp_ids_result - determined fp ids from DB for %s' % base_name)

    try:
        db_fp_count = len(echo_fp_ids_result)
        logger.info('ionosphere_echo :: %s features profile ids found' % str(db_fp_count))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: ionosphere_echo :: could not calculate len of echo_fp_ids_result')

    echo_enabled_mirage_fp_ids = []
    for row in echo_fp_ids_result:
        if row['enabled'] != 1:
            continue
        if row['deleted'] == 1:
            continue
        # Only create features profiles at settings.FULL_DURATION for Mirage
        # features profiles if the fp has been validated
        if row['validated'] == 0:
            continue

        if row['full_duration'] == int(mirage_full_duration):
            fp_id = row['id']
            # @added 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
            #                   Feature #1960: ionosphere_layers
            # Any features profiles that were created before the introduction of
            # ionosphere_layers will not have a mirage.redis.24h.json file as the
            # creation of these resources in the training_data dir was only added at
            # that point, so they are excluded.  Less than 20170307 1488844800 excl.
            # @modified 20190508 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
            #                      Feature #1960: ionosphere_layers
            # Increased the date as some on the cusp were still erroring, move
            # forward 2 days to 20170309
            # if int(row['anomaly_timestamp']) < 1488844800:
            if int(row['anomaly_timestamp']) < 1489017600:
                logger.info('ionosphere_echo :: skipping fp id %s as predates having a mirage.redis json file' % str(fp_id))
                continue
            echo_enabled_mirage_fp_ids.append(fp_id)

    echo_enabled_mirage_fp_ids_count = len(echo_enabled_mirage_fp_ids)
    logger.info('ionosphere_echo :: %s Mirage features profile ids found' % str(echo_enabled_mirage_fp_ids_count))

    # Check which Mirage features profile do not have has a
    # settings.FULL_DURATION features profile
    mirage_fd_fp_count = 0
    echo_create_fd_fp_for = []
    for validated_mirage_fp_id in echo_enabled_mirage_fp_ids:
        mirage_fd_fp_exists = False
        for row in echo_fp_ids_result:
            if int(row['parent_id']) != int(validated_mirage_fp_id):
                continue
            if int(row['full_duration']) == int(settings.FULL_DURATION):
                mirage_fd_fp_exists = True
                mirage_fd_fp_count += 1
        if not mirage_fd_fp_exists:
            echo_create_fd_fp_for.append(int(validated_mirage_fp_id))

    del echo_enabled_mirage_fp_ids

    logger.info('ionosphere_echo :: there are %s FULL_DURATION features profiles for %s' % (str(mirage_fd_fp_count), base_name))

    echo_create_fd_fp_for_count = len(echo_create_fd_fp_for)
    logger.info('ionosphere_echo :: %s FULL_DURATION features profiles to be created for %s' % (str(echo_create_fd_fp_for_count), base_name))
    echo_created_fp_count = 0

    # @added 20190404 - Bug #2904: Initial Ionosphere echo load and Ionosphere feedback
    #                   Feature #2484: FULL_DURATION feature profiles
    # Rate limit the creation of ionosphere_echo FULL_DURATION features profiles
    # this only effects managing the creation of lots of features profiles if
    # Ionosphere echo is enabled on a Skyline instance with lots of existing
    # features profiles for Mirage metrics.  Only create 5 FULL_DURATION
    # features profiles from the latest Mirage based features profiles, which
    # takes around 10 seconds
    if echo_create_fd_fp_for_count > 5:
        logger.info('ionosphere_echo :: rate limiting and only creating the 5 least FULL_DURATION features profiles for %s' % (base_name))
        # Reverse, newest first, using slicing to produce a reversed copy
        reverse_echo_create_fd_fp_for = echo_create_fd_fp_for[::-1]
        rate_limit_echo_create_fd_fp_for = reverse_echo_create_fd_fp_for[0:5]
        echo_create_fd_fp_for = rate_limit_echo_create_fd_fp_for

    last_created_fp = int(time())
    for mirage_fp_id in echo_create_fd_fp_for:
        fp_timestamp = None
        for row in echo_fp_ids_result:
            if int(row['id']) != int(mirage_fp_id):
                continue
            else:
                fp_timestamp = int(row['anomaly_timestamp'])
                fp_generation = int(row['generation'])
        if not fp_timestamp:
            continue
        if not fp_generation:
            fp_generation = 0

        time_check_now = int(time())
        echo_runtime = time_check_now - echo_started_at
        if echo_runtime >= ionosphere_echo_max_fp_create_time:
            logger.info('ionosphere_echo :: ionosphere_echo running for %s seconds, exiting before IONOSPHERE_ECHO_MAX_FP_CREATE_TIME of %s seconds is breached' % (str(echo_runtime), str(ionosphere_echo_max_fp_create_time)))
            break

        logger.info('ionosphere_echo :: creating FULL_DURATION features profile based on data from fp id %s - %s' % (str(mirage_fp_id), base_name))

        context = 'ionosphere_echo'
        ionosphere_job = 'learn_fp_human'
        generation = fp_generation + 1
        fp_learn = False

        # What is the path of the features profile files
        metric_timeseries_dir = base_name.replace('.', '/')
        metric_fp_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_PROFILES_FOLDER, metric_timeseries_dir,
            str(fp_timestamp))

        # What is the path of the new training data dir to copy the files to
        # and create it
        created_ts = int(time())
        # Ensure features profile creation timestamps do not overlap
        if last_created_fp == created_ts:
            sleep(1)
            created_ts = int(time())
            last_created_fp = created_ts

        metric_training_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, str(created_ts), metric_timeseries_dir)
        if not os.path.exists(metric_training_data_dir):
            try:
                mkdir_p(metric_training_data_dir)
                logger.info('ionosphere_echo :: training data dir created - %s' % metric_training_data_dir)
            except:
                logger.error('error :: ionosphere_echo :: failed to create training data dir - %s' % metric_training_data_dir)
                continue

        if not os.path.isdir(metric_fp_data_dir):
            logger.error('error :: ionosphere_echo :: features profile data dir does not exist - %s' % metric_fp_data_dir)
            continue

        data_files = []
        try:
            glob_path = '%s/*.*' % metric_fp_data_dir
            data_files = glob.glob(glob_path)
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            logger.error('error :: ionosphere_echo :: glob could not read - %s' % metric_fp_data_dir)

        # Make a list of the files to copy
        copy_files = []
        for i_file in data_files:
            # Exclude all the file resources related to the Mirage
            # full_duration features profile
            if 'matched.fp_id' in i_file:
                continue
            if 'fp.created.txt' in i_file:
                continue
            if 'fp.details.txt' in i_file:
                continue
            if 'csv.features.transposed.csv' in i_file:
                continue
            # Specifically include the required files
            if 'graphite_now' in i_file:
                copy_files.append(i_file)
            echo_metric_txt = '%s.txt' % base_name
            if echo_metric_txt in i_file:
                copy_files.append(i_file)
            echo_metric_json = '%s.json' % base_name
            if echo_metric_json in i_file:
                copy_files.append(i_file)
            if 'mirage.graphite' in i_file:
                copy_files.append(i_file)
            if 'mirage.redis' in i_file:
                copy_files.append(i_file)

        # Copy the required files to the new training_data dir
        for i_file in copy_files:
            copying_filename = os.path.basename(i_file)
            dest_file = '%s/%s' % (metric_training_data_dir, copying_filename)
            if not os.path.isfile(dest_file):
                try:
                    shutil.copy(i_file, metric_training_data_dir)
                    # logger.info('ionosphere_echo ::  training data copied - %s' % (i_file))
                except shutil.Error as e:
                    trace = traceback.format_exc()
                    logger.error('%s' % trace)
                    logger.error('error :: ionosphere_echo ::  shutil error - training data not copied to %s' % metric_training_data_dir)
                    logger.error('error :: ionosphere_echo ::  %s' % (e))
                # Any error saying that the directory doesn't exist
                except OSError as e:
                    trace = traceback.format_exc()
                    logger.error('%s' % trace)
                    logger.error('error :: ionosphere_echo ::  OSError error - training data not copied to %s' % metric_training_data_dir)
                    logger.error('error :: %s' % (e))

        calculated_feature_file = '%s/%s.tsfresh.input.csv.features.transposed.csv' % (metric_training_data_dir, base_name)
        calculated_feature_file_found = False
        fp_csv = None
        if os.path.isfile(calculated_feature_file):
            calculated_feature_file_found = True
            fp_csv = calculated_feature_file
            logger.info('ionosphere_echo :: calculated features file is available - %s' % (calculated_feature_file))

        echo_json_file = '%s.mirage.redis.%sh.json' % (base_name, str(full_duration_in_hours))
        if not calculated_feature_file_found:
            logger.info('ionosphere_echo :: calculating features from mirage.redis data ts json - %s' % (echo_json_file))
            str_created_ts = str(created_ts)
            try:
                fp_csv, successful, fp_exists, fp_id, log_msg, traceback_format_exc, f_calc = calculate_features_profile(skyline_app, str_created_ts, base_name, context)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: ionosphere_echo :: failed to calculate features')
                continue
        else:
            logger.info('ionosphere_echo :: using available calculated features file')

        if os.path.isfile(calculated_feature_file):
            logger.info('ionosphere_echo :: calculated features - %s' % (calculated_feature_file))
        else:
            logger.error('error :: ionosphere_echo :: failed to calculate features no file found - %s' % calculated_feature_file)
            continue

        # Create the new settings.FULL_DURATION features profile
        ionosphere_job = 'learn_fp_human'

        # @added 20190503 - Branch #2646: slack
        # Added slack_ionosphere_job
        slack_ionosphere_job = ionosphere_job

        fp_learn = False
        try:
            # @modified 20190503 - Branch #2646: slack
            # Added slack_ionosphere_job
            # fp_id, fp_in_successful, fp_exists, fail_msg, traceback_format_exc = create_features_profile(skyline_app, created_ts, base_name, context, ionosphere_job, mirage_fp_id, generation, fp_learn)
            # @modified 20190922 - Feature #3230: users DB table
            #                      Ideas #2476: Label and relate anomalies
            #                      Feature #2516: Add label to features profile
            # Added user_id
            # fp_id, fp_in_successful, fp_exists, fail_msg, traceback_format_exc = create_features_profile(skyline_app, created_ts, base_name, context, ionosphere_job, mirage_fp_id, generation, fp_learn, slack_ionosphere_job, user_id)
            user_id = 1
            # @modified 20191030 - Feature #2516: Add label to features profile
            # Added missing label
            label = 'echo'

            fp_id, fp_in_successful, fp_exists, fail_msg, traceback_format_exc = create_features_profile(skyline_app, created_ts, base_name, context, ionosphere_job, mirage_fp_id, generation, fp_learn, slack_ionosphere_job, user_id, label)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: ionosphere_echo :: failed to create a settings.FULL_DURATION features profile from fp_id %s for %s' % (str(mirage_fp_id), base_name))
            continue
        if not fp_in_successful:
            logger.error(traceback.format_exc())
            logger.error('error :: ionosphere_echo :: create_features_profile failed')
            continue
        else:
            echo_created_fp_count += 1
            logger.info(
                'ionosphere_echo :: new generation %s features profile with id %s settings.FULL_DURATION created from parent feature profile with id %s' % (
                    str(generation), str(fp_id), str(mirage_fp_id)))

    del echo_create_fd_fp_for
    if engine:
        echo_engine_disposal(engine)
    logger.info('ionosphere_echo :: created %s of %s FULL_DURATION features profile that were to be created for %s' % (str(echo_created_fp_count), str(echo_create_fd_fp_for_count), base_name))

    return
