from __future__ import division
import logging
import os
from os import getpid, listdir
from os.path import join, isfile
from sys import version_info
from time import time, sleep

import re
import csv
from ast import literal_eval
import shutil
import glob
import sys

from redis import StrictRedis
import traceback
import mysql.connector
from mysql.connector import errorcode

from sqlalchemy.sql import select

import numpy as np

# @added 20170123 - Feature #1854: Ionosphere learn - generations
# Required for ionosphere_learn to test that use_full_duration_days data is
# available
import json

import settings
from skyline_functions import (
    fail_check, mysql_select, write_data_to_file, send_graphite_metric, mkdir_p,
    get_graphite_metric, send_anomalous_metric_to)

from features_profile import calculate_features_profile

from database import (
    get_engine, ionosphere_table_meta, metrics_table_meta,
    ionosphere_matched_table_meta)
from tsfresh_feature_names import TSFRESH_FEATURES

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
    logger.error('error :: learn :: cannot determine ENABLE_IONOSPHERE_DEBUG from settings' % skyline_app)
    ENABLE_IONOSPHERE_DEBUG = False

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

try:
    learn_full_duration = int(settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS) * 86400
except:
    learn_full_duration = 86400 * 30  # 2592000

redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
context = 'ionosphere_learn'


def learn_load_metric_vars(metric_vars_file):
    """
    Load the metric variables for a check from a metric check variables file

    :param metric_vars_file: the path and filename to the metric variables files
    :type metric_vars_file: str
    :return: the metric_vars list or ``False``
    :rtype: list

    """

    logger = logging.getLogger(skyline_app_logger)
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
    # @modified 20170127 - Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
    # Added ionosphere_parent_id, always zero from Analyzer and Mirage
    int_keys = [
        'from_timestamp', 'metric_timestamp', 'added_at', 'full_duration',
        'ionosphere_parent_id']
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

    logger.info('debug :: learn :: metric_vars for %s' % str(metric))
    logger.info('debug :: learn :: %s' % str(metric_vars_array))

    return metric_vars_array


def get_learn_json(
        learn_json_file, base_name, use_full_duration, metric_timestamp,
        learn_full_duration_days):
    """
    Called by :func:`~learn` to surface a use_full_duration timeseries for the
    metric from Graphite and save as json.

    """
    logger = logging.getLogger(skyline_app_logger)
    ts_json = None
    try:
        from_timestamp = int(metric_timestamp) - int(use_full_duration)
        until_timestamp = int(metric_timestamp)
        logger.info(
            'learn :: getting Graphite timeseries json at %s days - from_timestamp - %s, until_timestamp - %s' %
            (str(learn_full_duration_days), str(from_timestamp),
                str(metric_timestamp)))
        ts_json = get_graphite_metric(
            skyline_app, base_name, from_timestamp, until_timestamp, 'json',
            learn_json_file)
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: learn :: getting Graphite timeseries json')

    return ts_json


def get_metric_from_metrics(base_name, engine):
    """
    Called by :func:`~learn` and returns the metric id and metric db object

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
        metric_db_object = row
        metrics_id = row['id']
        connection.close()
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: learn :: could not determine id from metrics table for - %s' % base_name)
        return False

    return metrics_id, metric_db_object


# @added 20170117 - Feature #1854: Ionosphere learn - learn_fp_learnt
# To determine origin fp features sum
def get_ionosphere_fp_ids(base_name, metrics_id, engine):
    """
    Called by :func:`~learn` and returns the fp_ids list

    """

    logger = logging.getLogger(skyline_app_logger)
    fp_ids = []

    try:
        ionosphere_table, log_msg, trace = ionosphere_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('learn :: ionosphere_table OK')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: learn :: failed to get ionosphere_table meta for %s' % base_name)
        return fp_ids

    try:
        connection = engine.connect()
        stmt = select([ionosphere_table]).where(ionosphere_table.c.metric_id == metrics_id)
        results = connection.execute(stmt)
        for row in results:
            fp_ids.append(row['id'])
        connection.close()
        fp_ids_count = len(fp_ids)
        logger.info('learn :: detemined %s fp ids for %s' % (str(fp_ids_count), base_name))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: could not get the fp_ids_db_object_count from the DB for %s' % base_name)

    return fp_ids


# @added 20170114 - Feature #1854: Ionosphere learn - Redis ionosphere.learn.work namespace
def get_ionosphere_record(fp_id, engine):
    """
    Called by :func:`~learn` and returns the fp row object

    """

    logger = logging.getLogger(skyline_app_logger)
    row = None

    try:
        ionosphere_table, log_msg, trace = ionosphere_table_meta(skyline_app, engine)
        logger.info(log_msg)
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: learn :: failed to get ionosphere_table meta for %s' % base_name)
        return row

    try:
        connection = engine.connect()
        stmt = select([ionosphere_table]).where(ionosphere_table.c.id == fp_id)
        result = connection.execute(stmt)
        row = result.fetchone()
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: could not get the fp_ids_db_object_count from the DB for %s' % base_name)

    return row

# @added 20170114 - Feature #1854: Ionosphere learn - Redis ionosphere.learn.work namespace
# Make a reusable function to remove the work from the Redis set


def remove_work_list_from_redis_set(learn_metric_list):
    """
    Called by :func:`~learn` to remove a work item from the Redis set

    """

    logger = logging.getLogger(skyline_app_logger)
    work_set = 'ionosphere.learn.work'
    try:
        redis_conn.srem(work_set, learn_metric_list)
        logger.info('learn :: removed work item - %s - from Redis set - %s' % (str(learn_metric_list), work_set))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: learn :: failed to remove work item list from Redis set - %s' % work_set)
        return False

    return True

# @modified 20170113 - Feature #1854: Ionosphere learn - Redis ionosphere.learn.work namespace
# Added work deadlines and changed to learn.py understanding and mangaing its
# own work queue so that anything can just queue work to learn.  Under this
# methodolgy, learn will run every minute if there is work in its queue that
# is nearing deadline, let us add and do some real-time computing


# def learn(metric_check_file):
# @modified 20170117 - Feature #1854: Ionosphere learn - generations
# Renamed the function from simple learn to the meme it has become
# learn(timestamp)
# def learn(timestamp):
def ionosphere_learn(timestamp):

    """
    Called by :class:`~skyline.skyline.Ionosphere.spawn_learn_process` to
    re-evaluate anomalies and such, like creating learning features profiles,
    when a human makes a features profile and the automated creation of learnt
    features profiles and learn features profiles.

    :param timestamp: timestamp at which learn was called
    :type timestamp: int
    :return: True or False
    :rtype: boolean

    learn uses a Redis set as a work queue.  This set is populated with the
    learn jobs, jobs being pieces of work that learn needs to do.  The learn
    Redis work set list item has the following elements:

    ``[str(deadline_type), str(job_type), int(metric_timestamp), str(base_name), int(parent_id), int(generation)]``

    Each job_type has a deadline_time that learn calculates from the job_type

    ionosphere.learn.work deadlines - deadline_types

    - Hard - missing a deadline is a total system failure.
    - Firm - infrequent deadline misses are tolerable, but may degrade the
      system's quality of service. The usefulness of a result is zero after
      its deadline.
    - Soft - the usefulness of a result degrades after its deadline, thereby
      degrading the system's quality of service.

    references:

    - Brian L. Troutwine @bltroutwine - seminal Belgium 2014 devopsdays
      presentation - Automation with Humans in Mind: Making Complex Systems Predictable, Reliable and Humane -
      https://legacy.devopsdays.org/events/2014-belgium/proposals/automation-with-humans-in-mind/ -
      video - http://www.ustream.tv/recorded/54703629
    - https://en.wikipedia.org/wiki/Real-time_computing#Criteria_for_real-time_computing

    learn work types:

    - **learn_fp_human** - Create a features profile for the human created features profile
      after ``LEARN_VALID_TIMESERIES_OLDER_THAN_SECONDS`` at ``LEARN_FULL_DURATION_DAYS``
      whatever those maybe it, will find out.  Copy the training_data dir to
      :mod:`settings.IONOSPHERE_LEARN_FOLDER` and the metric_check_file is rewritten
      with the determined (``LEARN_FULL_DURATION_DAYS`` * 86400) as the new
      full_duration.  Then this features profile is created as a generation 0
      features profile at the learn use_full_duration seconds.  This jobs is
      only ever add via the Ionosphere UI
      `~skyline.ionosphere_functions.create_features_profile`
      TODO: now_timestamp/ where the now_timestamp relaces the metric_timestamp
      context and as the metric_check_file has been replaced and this features
      profile was created later.  This prevents the pollution of metric_timestamp
      training data features profile.  This is a new features profile.  Is it
      generation 0 or generation 1?  The more I think I am edging to generation
      1, however as long as the first automated full_duration profiles that they
      create are generation 1 as well... hmm the generation game... TDB
      - deadline: 'Soft'

    - **learn_fp_automatic** - Create a features profile for the automatic
      created features profile as per the learn_fp_human above.
      - deadline: 'Soft'

    - **learn_fp_generation** - Create a features profile for the Ionosphere
      training_data metric after ``LEARN_VALID_TIMESERIES_OLDER_THAN_SECONDS`` at
      ``LEARN_FULL_DURATION_DAYS`` have passed and compare to other known
      ``LEARN_FULL_DURATION_DAYS`` features profiles.  If it matches any, then the
      metric training_data is added as features profile incremented by 1
      generation if ``MAX_GENERATIONS`` is not breached.
      - deadline: 'Soft'

    - **learn_fp_learnt** - Create a features profile for the Ionosphere learnt
      features profile after ``LEARN_VALID_TIMESERIES_OLDER_THAN_SECONDS`` at ``LEARN_FULL_DURATION_DAYS``
      whatever those maybe it, will find out.  Copies the training_data dir to
      :mod:`settings.IONOSPHERE_LEARN_FOLDER`/now_timestamp/ where the now_timestamp
      relaces the metric_timestamp context and the metric_check_file has the
      full_duration replace by the relevant use_full_duration.  Then this
      features profile is created as an incremented generation features profile
      at use_full_duration via learn.
      - deadline: 'Soft'

    """
    logger = logging.getLogger(skyline_app_logger)
    child_process_pid = os.getpid()
    logger.info('learn :: child_process_pid - %s' % str(child_process_pid))

    def learn_get_an_engine():

        try:
            engine, fail_msg, trace = get_engine(skyline_app)
            return engine, fail_msg, trace
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: learn :: get_an_engine :: failed to get MySQL engine'
            logger.error('%s' % fail_msg)
            return None, fail_msg, trace

    def learn_engine_disposal(engine):
        try:
            if engine:
                try:
                    engine.dispose()
                    logger.info('learn :: MySQL engine disposed of')
                    return True
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: calling engine.dispose()')
            else:
                logger.info('learn :: no MySQL engine to dispose of')
                return True
        except:
            return False
        return False

    # @added 20170112 - Feature #1854: Ionosphere learn - Redis ionosphere.learn.work namespace
    # Ionosphere learn needs Redis works sets
    # When a features profile is created there needs to be work added to a Redis
    # set
    # When a human makes a features profile, we want Ionosphere to make a
    # use_full_duration_days features profile valid_learning_duration (e.g.
    # 3361) later. Jack White style Redis work queue why not?  A departure from
    # check files the normal check files method, but this is fairly lite weight
    # in Redis terms.
    # @added 20170113 - Feature #1854: Ionosphere learn - Redis ionosphere.learn.work namespace
    # work_set and work deadlines
    redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
    work_set = 'ionosphere.learn.work'
    learn_work = None
    try:
        learn_work = redis_conn.smembers(work_set)
        logger.info('learn :: got Redis %s set' % work_set)
    except Exception as e:
        logger.error('error :: learn :: could not query Redis for ionosphere.learn.work - %s' % (work_set, e))

    if not learn_work:
        logger.info('learn :: no work ready to be done')
        return

    work_items_todo = len(learn_work)

    if work_items_todo == 0:
        logger.info('learn :: no work do')
        return
    else:
        logger.info('learn :: work items in queue - %s' % str(work_items_todo))

    for index, ionosphere_learn_work in enumerate(learn_work):
        try:
            learn_metric_list = literal_eval(ionosphere_learn_work)
            deadline = str(learn_metric_list[0])
            work = str(learn_metric_list[1])
            learn_metric_timestamp = int(learn_metric_list[2])
            learn_base_name = str(learn_metric_list[3])
            if str(work) != 'learn_fp_generation':
                learn_parent_id = int(learn_metric_list[4])
                learn_generation = int(learn_metric_list[5])
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: learn :: could not determine details from work item')
            continue

        logger.info('learn :: checking work item - %s' % (str(learn_metric_list)))

        # @added 20170127 - Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
        # If the work is older than 7200 seconds

        # The metric learn work variables now known so we can process the metric
        # Determine the metric details from the database
        metrics_id = None
        metric_db_object = None
        engine = None
        # Get a MySQL engine
        try:
            engine, log_msg, trace = learn_get_an_engine()
            logger.info('learn :: %s' % log_msg)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: learn :: could not get a MySQL engine to get metric_db_object')

        if not engine:
            logger.error('error :: learn :: engine not obtained to get metric_db_object')
            logger.info('learn :: exiting this work but not removing work item, as database may be available again before the work expires')
            continue

        try:
            metrics_id, metric_db_object = get_metric_from_metrics(learn_base_name, engine)
            learn_engine_disposal(engine)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: learn :: failed get the metric details from the database')
            logger.info('learn :: exiting this work but not removing work item, as database may be available again before the work expires')

        if not metrics_id:
            logger.error('error :: learn :: failed get the metrics_id from the database')
            logger.info('learn :: exiting this work but not removing work item, as database may be available again before the work expires')
            learn_engine_disposal(engine)
            continue

        if not metric_db_object:
            logger.error('error :: learn :: failed get the metric_db_object from the database')
            logger.info('learn :: exiting this work but not removing work item, as database may be available again before the work expires')
            learn_engine_disposal(engine)
            continue

        learn_valid_ts_older_than = None
        try:
            _learn_valid_ts_older_than = metric_db_object['learn_valid_ts_older_than']
            learn_valid_ts_older_than = int(_learn_valid_ts_older_than)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: learn :: failed get the determine the learn_valid_ts_older_than from the metric_db_object')
            use_full_duration = None
        if not learn_valid_ts_older_than:
            logger.info('learn :: exiting this work but not removing work item, as database may be available again before the work expires')
            learn_engine_disposal(engine)
            continue

        time_check = int(time())
        work_age = time_check - int(learn_metric_timestamp)
        if work_age < int(learn_valid_ts_older_than):
            logger.info('learn :: this work is not ready')
            continue
        else:
            logger.info('learn :: this work is ready - work_age - %s' % str(work_age))

        logger.info('learn :: processing %s for %s - %s' % (work, learn_base_name, str(learn_metric_list)))

        # First learn checks if the metric_training_data_dir exists, if it does not
        # there is nothing to learn with.
        metric_timeseries_dir = learn_base_name.replace('.', '/')
        metric_training_data_dir = '%s/%s/%s' % (
            str(settings.IONOSPHERE_DATA_FOLDER), str(learn_metric_timestamp),
            metric_timeseries_dir)
        if not os.path.exists(metric_training_data_dir):
            logger.info('learn :: cannot process as the training data directory no longer exists - %s' % (metric_training_data_dir))
            remove_work_list_from_redis_set(learn_metric_list)
            continue
        else:
            logger.info('learn :: metric_training_data_dir exists - %s' % (metric_training_data_dir))

        # If a learning directory does not exist, create it and populate it with the
        # training data directory image files and the check file, this is required
        # to ensure that the IONOSPHERE_DATA_FOLDER is not polluted and learn
        # namespace resources do not conflict with any training data resources a
        # learn directory is created.  As otherwise the new use_full_duration_days
        # transposed csv would overwrite the training data transposed csv
        if str(work) != 'learn_fp_learnt':
            metric_learn_data_dir = '%s/%s/%s' % (
                str(settings.IONOSPHERE_LEARN_FOLDER), str(learn_metric_timestamp),
                metric_timeseries_dir)
        else:
            metric_learn_data_dir = '%s/%s/%s' % (
                str(settings.IONOSPHERE_DATA_FOLDER), str(learn_metric_timestamp),
                metric_timeseries_dir)

        original_metric_check_file = '%s/%s.txt' % (metric_training_data_dir, learn_base_name)
        metric_check_file = '%s/%s.txt' % (metric_learn_data_dir, learn_base_name)

        if not os.path.exists(metric_learn_data_dir):
            try:
                mkdir_p(metric_learn_data_dir)
                logger.info('learn :: learning data dir created - %s' % metric_learn_data_dir)
            except:
                logger.error('error :: learn :: failed to create learning data dir - %s' % metric_learn_data_dir)

        if not os.path.isfile(metric_check_file):
            try:
                # @modified 20170117 - Feature #1854: Ionosphere learn
                # This is causing a bug where if the check file or dir is
                # present shutil is creating a dir with the full learn/opt/skyline
                # path. TODO: fixed DONE
                # shutil.copy(original_metric_check_file, metric_learn_data_dir)
                lines = []
                try:
                    logger.info('learn :: reading original_metric_check_file to metric_check_file - %s' % metric_check_file)
                    with open(original_metric_check_file) as fr:
                        for line in fr:
                            lines.append(line)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: learn :: failed to read original_metric_check_file' % original_metric_check_file)
                try:
                    logger.info('learn :: writing metric_check_file - %s' % metric_check_file)
                    with open(metric_check_file, 'w') as outfile:
                        for line in lines:
                            outfile.write(line)
                    logger.info('learn :: created metric_check_file from training data - %s' % (original_metric_check_file))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: learn :: failed to write metric_check_file')
            # except shutil.Error as e:
            #    trace = traceback.format_exc()
            #    logger.error('%s' % trace)
            #    logger.error('error :: learn :: shutil error - training data not copied to %s' % metric_learn_data_dir)
            #    logger.error('error :: learn :: %s' % (e))
            # # Any error saying that the directory doesn't exist
            # except OSError as e:
            #    trace = traceback.format_exc()
            #    logger.error('%s' % trace)
            #    logger.error('error :: learn :: OSError error - training data not copied to %s' % metric_learn_data_dir)
            #    logger.error('error :: %s' % (e))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: learn :: failed to write the metric_check_file')

        if str(work) != 'learn_fp_learnt':
            if os.path.isdir(metric_learn_data_dir):
                data_files = []
                try:
                    glob_path = '%s/*.*' % metric_training_data_dir
                    data_files = glob.glob(glob_path)
                except:
                    trace = traceback.format_exc()
                    logger.error('%s' % trace)
                    logger.error('error :: learn :: glob training data not copied to %s' % metric_learn_data_dir)

                for i_file in data_files:
                    # Only copy images, no data
                    if i_file.endswith('.png'):
                        copying_filename = os.path.basename(i_file)
                        dest_file = '%s/%s' % (metric_learn_data_dir, copying_filename)
                        if not os.path.isfile(dest_file):
                            try:
                                shutil.copy(i_file, metric_learn_data_dir)
                                logger.info('learn :: training data copied - %s' % (i_file))
                            except shutil.Error as e:
                                trace = traceback.format_exc()
                                logger.error('%s' % trace)
                                logger.error('error :: learn :: shutil error - training data not copied to %s' % metric_learn_data_dir)
                                logger.error('error :: learn :: %s' % (e))
                            # Any error saying that the directory doesn't exist
                            except OSError as e:
                                trace = traceback.format_exc()
                                logger.error('%s' % trace)
                                logger.error('error :: learn :: OSError error - training data not copied to %s' % metric_learn_data_dir)
                                logger.error('error :: %s' % (e))
            else:
                logger.error('error :: learn :: training data not copied to %s' % metric_learn_data_dir)

        if not os.path.isfile(str(metric_check_file)):
            logger.error('error :: learn :: file not found - metric_check_file - %s' % (str(metric_check_file)))
            remove_work_list_from_redis_set(learn_metric_list)
            continue

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
            remove_work_list_from_redis_set(learn_metric_list)
            continue

        if not metric_vars_array:
            logger.error('error :: learn :: no metric_vars_array available from check file - %s' % (metric_check_file))
            remove_work_list_from_redis_set(learn_metric_list)
            continue

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
            remove_work_list_from_redis_set(learn_metric_list)
            continue

        value = None
        try:
            key = 'value'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            value = float(value_list[0])
            anomalous_value = value
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: learn :: metric variable - value - %s' % str(value))
        except:
            logger.error('error :: learn :: failed to read value variable from check file - %s' % (metric_check_file))
            value = None

        if not value:
            logger.error('error :: learn :: failed to load value variable from check file - %s' % (metric_check_file))
            remove_work_list_from_redis_set(learn_metric_list)
            continue

        from_timestamp = None
        try:
            key = 'from_timestamp'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            from_timestamp = int(value_list[0])
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: learn :: metric variable - from_timestamp - %s' % str(from_timestamp))
        except:
            logger.info(traceback.format_exc())
            logger.error('error :: learn :: failed to read from_timestamp variable from check file - %s' % (metric_check_file))

        metric_timestamp = None
        try:
            key = 'metric_timestamp'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            metric_timestamp = int(value_list[0])
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: learn :: metric variable - metric_timestamp - %s' % str(metric_timestamp))
        except:
            logger.info(traceback.format_exc())
            logger.error('error :: learn :: failed to read metric_timestamp variable from check file - %s' % (metric_check_file))
            metric_timestamp = None

        if not metric_timestamp:
            logger.error('error :: learn :: failed to load metric_timestamp variable from check file - %s' % (metric_check_file))
            remove_work_list_from_redis_set(learn_metric_list)
            continue

        # @added 20170117 - Feature #1854: Ionosphere learn - generations
        # This metric var is required as it was contirbuting to ionosphere_learn
        # not logging some 2nd generation work due to send_anomalous_metric_to
        # being sent ionosphere_learn which has no own logger per se
        added_by = None
        try:
            key = 'added_by'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            added_by = str(value_list[0])
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: metric variable - added_by - %s' % added_by)
        except:
            logger.error('error :: failed to read added_by variable from check file - %s' % (metric_check_file))
            added_by = None

        if not added_by:
            remove_work_list_from_redis_set(learn_metric_list)
            continue

        try:
            key = 'added_at'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            added_at = int(value_list[0])
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: learn :: metric variable - added_at - %s' % str(added_at))
        except:
            logger.error('error :: learn :: failed to read added_at variable from check file setting to all - %s' % (metric_check_file))
            added_at = metric_timestamp

        full_duration = None
        try:
            key = 'full_duration'
            value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
            full_duration = int(value_list[0])
            if settings.ENABLE_IONOSPHERE_DEBUG:
                logger.info('debug :: learn :: metric variable - full_duration - %s' % str(full_duration))
        except:
            logger.error('error :: learn :: failed to read full_duration variable from check file - %s' % (metric_check_file))
            full_duration = None

        if not full_duration:
            logger.error('error :: learn :: failed to determine full_duration variable from check file - %s' % (metric_check_file))
            remove_work_list_from_redis_set(learn_metric_list)
            continue

        # Before we continue, now that we have the actual metric_db_object
        learn_full_duration_days = None
        learn_full_duration = None
        use_full_duration = None
        try:
            learn_full_duration_days = metric_db_object['learn_full_duration_days']
            learn_full_duration = int(learn_full_duration_days) * 86400
            use_full_duration = learn_full_duration
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: learn :: failed get the determine the learn_full_duration_days from the metric_db_object')
            logger.info('learn :: exiting this work but not removing work item, as database may be available again before the work expires')
            continue

        if str(work) == 'learn_fp_learnt':
            learn_metric_check_file = '%s/%s' % (metric_learn_data_dir, check_file_name)

        if str(work) != 'learn_fp_learnt':
            # Write a new metric check file with the learn_full_duration
            old_metric_check_file = '%s.original.training' % metric_check_file
            if not os.path.isfile(old_metric_check_file):
                try:
                    shutil.move(metric_check_file, old_metric_check_file)
                except:
                    logger.error('error :: learn :: moving metric_check_file in the learning data dir')
                    remove_work_list_from_redis_set(learn_metric_list)
                    continue

            learn_metric_check_file = '%s/%s' % (metric_learn_data_dir, check_file_name)
            if not os.path.isfile(learn_metric_check_file):
                lines = []
                try:
                    logger.info('learn :: reading metric_check_file to replace full_duration - %s' % old_metric_check_file)
                    with open(old_metric_check_file) as fr:
                        for line in fr:
                            if 'full_duration' in line:
                                line = line.replace(str(full_duration), str(learn_full_duration))
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
            logger.error('error :: learn :: learn_metric_check_file not created')
            remove_work_list_from_redis_set(learn_metric_list)
            continue

        # Create a learn json data if it does not exist
        got_learn_json = False
        learn_json_file = '%s/%s.json' % (metric_learn_data_dir, base_name)
        if os.path.isfile(learn_json_file):
            logger.info('learn :: learning data ts json available - %s' % (learn_json_file))
            got_learn_json = True
        else:
            try:
                logger.info(
                    'learn :: need learning data ts json from Graphite at %s days - %s' % (
                        str(learn_full_duration_days), learn_json_file))
                got_learn_json = get_learn_json(
                    learn_json_file, base_name, use_full_duration, metric_timestamp,
                    learn_full_duration_days)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: learn :: learn_json call failed')
                got_learn_json = False

        if not got_learn_json:
            logger.error(
                'error :: learn :: failed to get timeseries json from Graphite for %s at %s second for anomaly at %s' %
                (base_name, str(use_full_duration), str(metric_timestamp)))
            logger.info('learn :: exiting this work but not removing work item, as Graphite may be available again before the work expires')
            continue

        # @added 20170123 - Feature #1854: Ionosphere learn - generations
        # TODO
        # ionosphere_learn needs to test that use_full_duration_days data is available
        # before any use_full_duration_days features profiles are created a metric, this
        # ensures that newly added metrics are not learnt in the use_full_duration_days
        # until there is use_full_duration_days available
        try:
            with open((learn_json_file), 'r') as f:
                # @modified 20170131 - Feature #1854: Ionosphere learn - generations
                #                      Feature #1886 Ionosphere learn - child like parent with evolutionary maturity
                # Corrected method
                # timeseries = json.loads(f.read())
                raw_timeseries = f.read()
                timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
                timeseries = literal_eval(timeseries_array_str)
                datapoints = timeseries
                validated_timeseries = []
                for datapoint in datapoints:
                    try:
                        new_datapoint = [int(datapoint[0]), float(datapoint[1])]
                        validated_timeseries.append(new_datapoint)
                    except:
                        continue
                timeseries = validated_timeseries

                # @modified 20170129 - Bug #1898: Ionosphere - missing json
                # logger.info('learn :: data points surfaced :: %s' % (len(timeseries)))
                logger.info('learn :: data points surfaced :: %s' % (str(len(timeseries))))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: learn :: failed to read learning data ts json - %s' % (learn_json_file))
            remove_work_list_from_redis_set(learn_metric_list)
            continue
        # Tested and Graphite returns null, the Mirage json converted pattern
        # discards null.
        # [1483552680.0, 0.0]
        # gary@mc11:/tmp$ date -d @1483552680
        # Wed Jan  4 17:58:00 GMT 2017
        # gary@mc11:/tmp$
        # This should be 24 Dec 2016 as that is a request for 30 days data, so a simple check would be...
        # If first data point not on the first day of the use_full_duration_days then not valid to learn.
        first_timestamp = 0
        try:
            first_timestamp = int(timeseries[0][0])
        except IndexError:
            logger.error(traceback.format_exc())
            logger.error('error :: learn :: failed to determine the first timestamp from learning data ts json - %s' % (learn_json_file))
            remove_work_list_from_redis_set(learn_metric_list)
            continue
        if first_timestamp == 0:
            logger.error('error :: learn :: no first timestamp from learning data ts json - %s' % (learn_json_file))
            remove_work_list_from_redis_set(learn_metric_list)
            continue
        # TODO still calculate age and discard if no data from the first day of
        # use_full_duration_days

        # Calculate the features and a features profile for the learn_json_file
        calculated_feature_file = '%s/%s.tsfresh.input.csv.features.transposed.csv' % (metric_learn_data_dir, base_name)
        calculated_feature_file_found = False
        fp_csv = None
        if os.path.isfile(calculated_feature_file):
            calculated_feature_file_found = True
            fp_csv = calculated_feature_file
            logger.info('learn :: calculated features file is available - %s' % (calculated_feature_file))

        if got_learn_json:
            if not calculated_feature_file_found:
                logger.info('learn :: need to calculate features from learning data ts json - %s' % (learn_json_file))
                try:
                    fp_csv, successful, fp_exists, fp_id, log_msg, traceback_format_exc, f_calc = calculate_features_profile(skyline_app, metric_timestamp, base_name, context)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: learn :: failed to calculate features')
                    remove_work_list_from_redis_set(learn_metric_list)
                    continue
            else:
                logger.info('learn :: using available calculated features file')

        if os.path.isfile(calculated_feature_file):
            calculated_feature_file_found = True
            fp_csv = calculated_feature_file

        # @added 20170131 - Feature #1886 Ionosphere learn - child like parent with evolutionary maturity
        allowed_to_learn = False

        # @added 20170116 - Feature #1854: Ionosphere learn - generations
        # Rate limit by generation and by max_percent_diff_from_origin
        max_generations = None
        try:
            _max_generations = metric_db_object['max_generations']
            max_generations = int(_max_generations)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: learn :: failed to determine the max_generations from the metric_db_object')
            use_full_duration = None
        if not max_generations:
            logger.info('learn :: exiting this work but not removing work item, as database may be available again before the work expires')
            learn_engine_disposal(engine)
            continue
        if str(work) == 'learn_fp_learnt' and calculated_feature_file_found:
            if int(learn_generation) >= max_generations:
                logger.error(
                    'error :: learn :: a %s job has been added to try and create a %s generation features profile from features profile id %s' % (
                        str(work), str(learn_generation), str(learn_parent_id)))
                logger.info('debug :: learn :: a %s job was %s' % (
                    str(work), str(learn_metric_list)))
                remove_work_list_from_redis_set(learn_metric_list)
                continue
            max_percent_diff_from_origin = None
            try:
                _max_percent_diff_from_origin = metric_db_object['max_percent_diff_from_origin']
                max_percent_diff_from_origin = float(_max_percent_diff_from_origin)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: learn :: failed to determine the max_percent_diff_from_origin from the metric_db_object')
                max_percent_diff_from_origin = None
            if not max_percent_diff_from_origin:
                logger.info('learn :: exiting this work but not removing work item, as database may be available again before the work expires')
                learn_engine_disposal(engine)
                continue

            # The metric learn work variables now known so we can process the metric
            # Determine the metric details from the database
            fp_ids = None
            engine = None
            # Get a MySQL engine
            try:
                engine, log_msg, trace = learn_get_an_engine()
                logger.info('learn :: %s' % log_msg)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: learn :: could not get a MySQL engine to get fp_ids_db_object')

            if not engine:
                logger.error('error :: learn :: engine not obtained to get fp_ids_db_object')
                logger.info('learn :: exiting this work but not removing work item, as database may be available again before the work expires')
                continue

            try:
                fp_ids = get_ionosphere_fp_ids(learn_base_name, metrics_id, engine)
                # learn_engine_disposal(engine)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: learn :: failed get the fp_ids from the database')
                logger.info('learn :: exiting this work but not removing work item, as database may be available again before the work expires')
                learn_engine_disposal(engine)
                continue

            # if not fp_ids:
            if fp_ids == []:
                logger.error('error :: learn :: failed get the fp_ids from the database')
                logger.info('learn :: exiting this work but not removing work item, as database may be available again before the work expires')
                learn_engine_disposal(engine)
                continue

            # Determine the sum of the origin features profile which means
            # having to trace back from the learn_parent_id to the origin
            origin_fp_id = None
            origin_features_profile_sum = None
            origin_fp_full_duration = None
            current_parent_id = learn_parent_id
            current_generation = learn_generation
            if int(current_generation) == 0:
                logger.error('error :: learn :: ionosphere_learn does not handle generation %s profiles' % str(current_generation))
                logger.info('learn :: exiting this work and removing work item')
                remove_work_list_from_redis_set(learn_metric_list)
                learn_engine_disposal(engine)
                continue

            logger.info('learn :: determining the id and features sum value for the origin features profile from the fp_ids for fp id %s' % str(learn_parent_id))
            while current_generation != 0:
                for fp_id in fp_ids:
                    try:
                        if int(current_parent_id) == int(fp_id):
                            row = get_ionosphere_record(int(fp_id), engine)
                            current_fp_parent_id = int(row['parent_id'])
                            current_fp_generation = int(row['generation'])
                            if current_fp_parent_id == 0:
                                origin_features_profile_sum = float(row['features_sum'])
                                origin_fp_id = int(current_parent_id)
                                origin_fp_full_duration = int(row['full_duration'])
                                logger.info(
                                    'learn :: origin fp id %s of generation %s' % (
                                        str(current_parent_id),
                                        str(current_fp_generation)))
                                logger.info(
                                    'learn :: origin fp id features sum - %s' % (
                                        str(origin_features_profile_sum)))
                                current_generation = current_fp_generation
                            else:
                                logger.info('learn :: fp id %s of generation %s has parent fp id %s of generation %s' % (
                                    str(current_parent_id), str(current_generation),
                                    str(current_fp_parent_id), str(current_fp_generation)))
                                current_parent_id = current_fp_parent_id
                                current_generation = current_fp_generation
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: learn :: determining parent id of the 0 generation origin')
                        break

            # @added 20170131 - Feature #1886 Ionosphere learn - child like parent with evolutionary maturity
            # TODO: here a check may be required to evaluate whether the origin_fp_id
            #       had a use_full_duration features profile created, however
            #       due to the fact that it is in learn, suggests that it did
            #       have, not 100% sure.
            child_use_full_duration_count_of_origin_fp_id = 0
            try:
                connection = engine.connect()
                result = connection.execute(
                    'SELECT COUNT(id) FROM ionosphere WHERE parent_id=%s AND full_duration=%s' % (
                        str(origin_fp_id), str(use_full_duration)))
                for row in result:
                    child_fp_count = row['COUNT(id)']
                child_use_full_duration_count_of_origin_fp_id = int(child_fp_count)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: learn :: determining parent id of the 0 generation origin')
            if child_use_full_duration_count_of_origin_fp_id > 0:
                logger.info('learn :: the origin_fp_id %s was allowed to learn, allowing to learn' % str(origin_fp_id))
                allowed_to_learn = True
            else:
                logger.info('learn :: the origin_fp_id %s was not allowed to learn, not allowing learning' % str(origin_fp_id))

            learn_engine_disposal(engine)

            if not origin_features_profile_sum:
                logger.info('learn :: exiting this work and removing as the origin fp features sum could not be determined')
                # TODO: remove this just testing
                # remove_work_list_from_redis_set(learn_metric_list)
                learn_engine_disposal(engine)
                continue
            logger.info(
                'learn :: the origin zero generation features profile %s - features_sum - %s' % (
                    str(origin_fp_id), str(origin_features_profile_sum)))
            # Determine the features sum from the training data features profile
            # details file
            learnt_features_profile_details_file = '%s/%s.%s.fp.details.txt' % (
                metric_learn_data_dir, str(metric_timestamp),
                base_name)
            if not os.path.isfile(learnt_features_profile_details_file):
                logger.error('error :: learn :: the learnt features profile fp details file is no longer available - %s' % learnt_features_profile_details_file)
                logger.info('learn :: cannot calculate the percent diff of the learnt features_profiles to the origin')
                remove_work_list_from_redis_set(learn_metric_list)
                continue

            learnt_fp_features_sum = None
            try:
                with open(learnt_features_profile_details_file, 'r') as f:
                    fp_details_str = f.read()
                fp_details_array = literal_eval(fp_details_str)
                learnt_fp_features_sum = float(fp_details_array[4])
            except:
                trace = traceback.format_exc()
                logger.error(trace)
                logger.error(
                    'error: failed to read from %s' % (features_profile_details_file))

            if not learnt_fp_features_sum:
                logger.error('error :: learn :: could not determine the features sum from the learnt features profile fp details file - %s' % learnt_features_profile_details_file)
                logger.info('learn :: cannot calculate the percent diff of the learnt features_profiles to the origin')
                remove_work_list_from_redis_set(learn_metric_list)
                continue

            # TODO: base this on common features
            logger.info('learn :: checking the percent difference for the origin features sum')
            percent_different = 100
            sums_array = np.array([origin_features_profile_sum, learnt_fp_features_sum], dtype=float)
            try:
                calc_percent_different = np.diff(sums_array) / sums_array[:-1] * 100.
                percent_different = calc_percent_different[0]
                logger.info(
                    'learn :: percent_different between features sums of the learnt training data and origin fp_id %s is %s' % (
                        str(origin_fp_id), str(percent_different)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to calculate percent_different')
                remove_work_list_from_redis_set(learn_metric_list)
                continue
            # Check that the max_percent_diff_from_origin is not breached
            if percent_different < 0:
                new_pdiff = percent_different * -1
                percent_different = new_pdiff

            if float(percent_different) > float(max_percent_diff_from_origin):
                logger.info(
                    'learn :: the calculated features sum breaches the max_percent_diff_from_origin of %s for %s' % (
                        str(max_percent_diff_from_origin), base_name))
                logger.info('learn :: cannot use training_data as a learnt features profile - %s' % learnt_features_profile_details_file)
                remove_work_list_from_redis_set(learn_metric_list)
                continue

        # @modified 20170116 - Feature #1854: Ionosphere learn - generations
        # A features profile can be created in the learn_fp_human and in the
        # learn_fp_learnt context
        # if str(work) == 'learn_fp_human' and calculated_feature_file_found:
        create_a_learn_features_profile = False
        if calculated_feature_file_found:
            if str(work) == 'learn_fp_human':
                create_a_learn_features_profile = True
                profile_context = 'human generated'
                ionosphere_job = 'none'
            if str(work) == 'learn_fp_automatic':
                create_a_learn_features_profile = True
                profile_context = 'automatically generated'
                ionosphere_job = 'none'
            if str(work) == 'learn_fp_learnt':
                create_a_learn_features_profile = True
                profile_context = 'automatically generated'
                ionosphere_job = 'learn_fp_automatic'
                # TODO: make Graphite NOW graphs.  A simple requests call?

        # @added 20170118 - Feature #1854: Ionosphere learn - generations
        #                   Feature #1842: Ionosphere - Graphite now graphs
        if str(work) == 'learn_fp_learnt':
            logger.info('learn :: requesting Ionosphere webapp training_data page to generate Graphite NOW graphs')
            import requests
            # @modified 20170122 - Feature #1854: Ionosphere learn - generations
            #                      Feature #1842: Ionosphere - Graphite now graphs
            # Corrected typos in url
            # url = '%s/ionosphere?timestamp=%smetric=%s' % (
            #    settings.SKYLINE_URL, str(learn_base_name), str(metric_timestamp))
            url = '%s/ionosphere?timestamp=%s&metric=%s' % (
                settings.SKYLINE_URL, str(metric_timestamp), str(learn_base_name))

            logger.info('learn :: training_data URL - %s' % str(url))
            ionosphere_resp = None
            if settings.WEBAPP_AUTH_ENABLED:
                user = str(settings.WEBAPP_AUTH_USER)
                password = str(settings.WEBAPP_AUTH_USER_PASSWORD)
            try:
                if settings.WEBAPP_AUTH_ENABLED:
                    r = requests.get(url, timeout=10, auth=(user, password))
                else:
                    r = requests.get(url, timeout=10)
                if int(r.status_code) == 200:
                    ionosphere_resp = True
                    logger.info('learn :: Graphite NOW graphs for training_data created')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get anomaly id from panorama: %s' % str(url))

        if create_a_learn_features_profile:
            if fp_csv:
                logger.info('learn :: %s :: fp_csv for create_features_profile - %s' % (profile_context, str(fp_csv)))
            if str(work) == 'learn_fp_learnt':
                fp_created_at = int(metric_timestamp)

            if str(work) != 'learn_fp_learnt':
                fp_created_at = int(time())
                metric_new_fp_data_dir = '%s/%s/%s' % (
                    str(settings.IONOSPHERE_LEARN_FOLDER), str(fp_created_at),
                    metric_timeseries_dir)

                # Create a new timestamped directory at the timestamp that this features
                # profile is created
                if not os.path.exists(metric_new_fp_data_dir):
                    logger.info('learn :: %s :: creating new timestamped learning dir - %s' % (profile_context, metric_new_fp_data_dir))
                    try:
                        shutil.move(metric_learn_data_dir, metric_new_fp_data_dir)
                        logger.info('learn :: %s :: moved original learning dir to new timestamped learning dir' % profile_context)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: learn :: %s :: failed to create the new features profile learn directory' % profile_context)
                        remove_work_list_from_redis_set(learn_metric_list)
                        continue

                original_fp_details_file = '%s/%s.%s.fp.details.txt' % (
                    metric_new_fp_data_dir, str(metric_timestamp), base_name)
                new_fp_details_file = '%s/%s.%s.fp.details.txt' % (
                    metric_new_fp_data_dir, str(fp_created_at), base_name)
                if not os.path.isfile(new_fp_details_file):
                    try:
                        shutil.move(original_fp_details_file, new_fp_details_file)
                    except:
                        logger.error('error :: learn :: %s :: renaming features details file to the new timestamp' % profile_context)

                if not os.path.isfile(new_fp_details_file):
                    logger.error('error :: learn :: %s :: the new_fp_details_file does not exist - %s' % (profile_context, new_fp_details_file))
                    remove_work_list_from_redis_set(learn_metric_list)
                    continue
                logger.info('learn :: %s :: timestamped learning dir exists' % profile_context)

            # Create a use_full_duration learn features profile
            generation = int(learn_generation) + 1
            if int(generation) >= int(max_generations):
                logger.error(
                    'error :: learn :: %s :: not creating a features profile as it would breach max_generations as would be generation %s' % (
                        profile_context, str(generation)))
                remove_work_list_from_redis_set(learn_metric_list)
                continue
            else:
                logger.info('learn :: %s :: generation limit OK at %s' % (profile_context, str(generation)))

            # @added 20170129 - Feature #1886 Ionosphere learn - child like parent with evolutionary maturity
            # Before a features profile can be created by a child, a check must
            # be made to determine if the parent was allowed to learn.  If a
            # features profile was initially created but not set to learn, then
            # the child should not pass learn either, by default, unless the
            # child features profile is later found to match a KNOWN learn
            # use_full_duration profile that DOES match.  At this point the
            # timeseries has reached maturity in its current state.  It could be
            # considered that the timeseries has achieved a more stable Active
            # Brownian Motion https://github.com/blue-yonder/tsfresh/pull/143#issuecomment-272314801
            # Whatever anomalies the operator did not want to ship in or learn
            # are no longer part of the use_full_duration_days profile.  At this
            # point even if the parent could not learn, other generations agree
            # that the current timeseries has now matured since the original
            # parent generation that use_full_duration_days is now normal.
            # At this point the generation restriction is removed and Skyline
            # can learn at use_full_duration_days as well.  Easier than it
            # sounds...
            do_not_learn = False
            if not allowed_to_learn:
                logger.info('learn :: the origin parent was not allowed to learn so setting do_not_learn to True')
                do_not_learn = True
            else:
                logger.info('learn :: the origin parent was allowed to learn so do_not_learn is set to False')

            try:
                # @modified 20170120 -  Feature #1854: Ionosphere learn - generations
                # Added fp_learn parameter to allow the user to not learn the
                # use_full_duration_days, this can be passed via the UI as False
                fp_learn = True
                # @added 20170129 - Feature #1886 Ionosphere learn - child like parent with evolutionary maturity
                if do_not_learn:
                    fp_learn = False
                fp_id, fp_in_successful, fp_exists, fail_msg, traceback_format_exc = create_features_profile(skyline_app, fp_created_at, learn_base_name, context, ionosphere_job, learn_parent_id, generation, fp_learn)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: learn :: %s :: failed to create a features profile' % profile_context)
            if not fp_in_successful:
                logger.error(traceback.format_exc())
                logger.error('error :: learn :: %s :: create_features_profile failed' % profile_context)
                remove_work_list_from_redis_set(learn_metric_list)
                continue
            else:
                logger.info(
                    'learn :: %s :: new generation %s features profile with id %s using %s days data based on the human generated parent feature profile with id %s' % (
                        profile_context, str(generation), str(fp_id),
                        str(learn_full_duration_days), str(learn_parent_id)))

            remove_work_list_from_redis_set(learn_metric_list)
            continue

        # @added 20170116 - Feature #1854: Ionosphere learn - generations
        # This is it.  Here we go! Learn!
        if str(work) == 'learn_fp_generation' and calculated_feature_file_found:
            logger.info('learn :: fp_csv for create_features_profile - %s' % str(fp_csv))
            logger.info('learn :: adding an Ionosphere check file')
            # These are not required in the Ionosphere check context
            triggered_algorithms = 'None'
            timeseries = []
            # @modified 20170129 - Feature #1854: Ionosphere learn - generations
            #                      Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
            # For learn_fp_generation the learn_parent_id is not set so set to 0
            learn_parent_id = 0
            try:
                send_anomalous_metric_to(
                    # @modified 20170118 - Feature #1854: Ionosphere learn - generations
                    # Fixed bug, as the current_skyline_app must have a logger
                    # and added send_to_app as ionosphere_learn_to_ionosphere
                    # which is handled in the send_anomalous_metric_to function
                    # now.
                    # 'ionosphere_learn', 'ionosphere', metric_learn_data_dir,
                    # @modified 20170127 - Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
                    # Added parent_id
                    'ionosphere', 'ionosphere_learn_to_ionosphere',
                    str(metric_learn_data_dir), str(metric_timestamp),
                    base_name, str(value), str(from_timestamp),
                    triggered_algorithms, timeseries, str(use_full_duration),
                    str(learn_parent_id))
                logger.info(
                    'learn :: ionosphere check added at %s full_duration for %s' % (
                        str(use_full_duration), str(fp_csv)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: learn :: failed to send_anomalous_metric_to to Ionosphere at %s full_duration for %s' % (
                    str(use_full_duration), str(fp_csv)))

            remove_work_list_from_redis_set(learn_metric_list)
            continue

    if engine:
        learn_engine_disposal(engine)
    return
