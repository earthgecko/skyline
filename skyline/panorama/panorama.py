"""
panorama.py
"""
import logging
# try:
#     from Queue import Empty
# except:
#     from queue import Empty
from time import time, sleep
from threading import Thread
# @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
# Use Redis sets in place of Manager().list() to reduce memory and number of
# processes
# from multiprocessing import Process, Manager
from multiprocessing import Process
import os
from os import kill, getpid, listdir
from os.path import join, isfile
from ast import literal_eval
# @added 20240119 - Task #5228: panorama - optimise insertions
import math

# from redis import StrictRedis
from msgpack import Unpacker, packb
import traceback
from sys import version_info
from sys import exit as sys_exit
import mysql.connector
from mysql.connector import errorcode

# @added 20190502 - Branch #2646: slack
from sqlalchemy.sql import select

# @modified 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
#                      Task #4778: v4.0.0 - update dependencies
# Use sqlalchemy rather than string-based query construction
# Added Table and MetaData
from sqlalchemy import select, Table, MetaData

import settings

from skyline_functions import (
    fail_check, mkdir_p,
    # @added 20191031 - Bug #3266: py3 Redis binary objects not strings
    #                   Branch #3262: py3
    # Added a single functions to deal with Redis connection and the
    # charset='utf-8', decode_responses=True arguments required in py3
    get_redis_conn, get_redis_conn_decoded,
    # @added 20200413 - Feature #3486: analyzer_batch
    #                   Feature #3480: batch_processing
    is_batch_metric)

# @added 20170115 - Feature #1854: Ionosphere learn - generations
# Added determination of the learn related variables so that any new metrics
# that Panorama adds to the Skyline database, it adds the default
# IONOSPHERE_LEARN_DEFAULT_ values or the namespace specific values matched
# from settings.IONOSPHERE_LEARN_NAMESPACE_CONFIG to the metric database
# entry.
from ionosphere_functions import get_ionosphere_learn_details

# @added 20190502 - Branch #2646: slack
from database import (
    get_engine, metrics_table_meta, anomalies_table_meta,
    # @added 20200928 - Task #3748: POC SNAB
    #                   Branch #3068: SNAB
    snab_table_meta,
)

# @added 20211001 - Feature #4268: Redis hash key - panorama.metrics.latest_anomaly
#                   Feature #4264: luminosity - cross_correlation_relationships
from functions.panorama.update_metric_latest_anomaly import update_metric_latest_anomaly

# @added 20220722 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.metrics.get_base_name_from_labelled_metrics_name import get_base_name_from_labelled_metrics_name

# @added 20230110 - Task #4022: Move mysql_select calls to SQLAlchemy
#                   Task #4778: v4.0.0 - update dependencies
# Replace mysql_select for all metrics
from functions.database.queries.get_all_db_metric_names import get_all_db_metric_names

# @added 20240122 - Task #5228: panorama - optimise insertions
from functions.database.queries.get_apps import get_apps
from functions.database.queries.get_algorithms import get_algorithms
from functions.database.queries.get_sources import get_sources
from functions.database.queries.get_hosts import get_hosts

# @added 20240229 - Feature #5294: panorama - retry failed checks
from functions.panorama.get_failed_checks_to_retry import get_failed_checks_to_retry

# @added 20240602 - Feature #5368: analyzer_batch - reprocess
from functions.database.queries.query_anomalies import get_anomalies_for_period

# @added 20241203 - Bug #5522: Handle duplicate metric names
from slack_functions import slack_post_message

skyline_app = 'panorama'
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
    ENABLE_PANORAMA_DEBUG = settings.ENABLE_PANORAMA_DEBUG
except:
    logger.error('error :: cannot determine ENABLE_PANORAMA_DEBUG from settings')
    ENABLE_PANORAMA_DEBUG = False

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

# @added 20190523 - Branch #2646: slack
try:
    SLACK_ENABLED = settings.SLACK_ENABLED
except:
    SLACK_ENABLED = False

skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)

failed_checks_dir = '%s_failed' % settings.PANORAMA_CHECK_PATH

# @added 20160907 - Handle Panorama stampede on restart after not running #26
# Allow to expire check if greater than PANORAMA_CHECK_MAX_AGE, backwards
# compatible
try:
    test_max_age_set = 1 + settings.PANORAMA_CHECK_MAX_AGE
    if test_max_age_set > 1:
        max_age = True
    if test_max_age_set == 1:
        max_age = False
    max_age_seconds = settings.PANORAMA_CHECK_MAX_AGE
except:
    max_age = False
    max_age_seconds = 0
expired_checks_dir = '%s_expired' % settings.PANORAMA_CHECK_PATH

# @added 20200128 - Feature #3418: PANORAMA_CHECK_INTERVAL
try:
    PANORAMA_CHECK_INTERVAL = int(settings.PANORAMA_CHECK_INTERVAL)
except:
    PANORAMA_CHECK_INTERVAL = 20

# @added 20200204 - Feature #3442: Panorama - add metric to metrics table immediately
try:
    # @modified 20230204 - Task #2732: Prometheus to Skyline
    #                      Branch #4300: prometheus
    #                      Feature #3442: Panorama - add metric to metrics table immediately
    # With the addition of analyzer/labelled_metrics, mirage/mirage_labelled_metrics,
    # and the plethora of changes that supporting the ingestion, storage and
    # analysis of labelled_metrics, Panorama now needs to insert the metrics as soon
    # as possible as they are not accepted by flux/prometheus/write until the new,
    # incoming metric/s have an id assigned to them.  Therefore as of v4.0.0 this
    # was set to True so that Panorama checks and inserts new metrics every 60
    # seconds.
    # PANORAMA_INSERT_METRICS_IMMEDIATELY = settings.PANORAMA_INSERT_METRICS_IMMEDIATELY
    PANORAMA_INSERT_METRICS_IMMEDIATELY = True
except:
    # PANORAMA_INSERT_METRICS_IMMEDIATELY = False
    PANORAMA_INSERT_METRICS_IMMEDIATELY = True



# @added 20200413 - Feature #3486: analyzer_batch
#                   Feature #3480: batch_processing
try:
    from settings import BATCH_PROCESSING
except:
    BATCH_PROCESSING = None
try:
    # @modified 20200606 - Bug #3572: Apply list to settings import
    # from settings import BATCH_PROCESSING_NAMESPACES
    BATCH_PROCESSING_NAMESPACES = list(settings.BATCH_PROCESSING_NAMESPACES)
except:
    BATCH_PROCESSING_NAMESPACES = []

# @added 20200929 - Task #3748: POC SNAB
#                   Branch #3068: SNAB
# If SNAB is enabled override PANORAMA_CHECK_INTERVAL
try:
    SNAB_ENABLED = settings.SNAB_ENABLED
except:
    SNAB_ENABLED = False
if SNAB_ENABLED:
    PANORAMA_CHECK_INTERVAL = 1

# Database configuration
config = {'user': settings.PANORAMA_DBUSER,
          'password': settings.PANORAMA_DBUSERPASS,
          'host': settings.PANORAMA_DBHOST,
          'port': settings.PANORAMA_DBPORT,
          'database': settings.PANORAMA_DATABASE,
          'raise_on_warnings': True}


class Panorama(Thread):
    """
    The Panorama class which controls the panorama thread and spawned processes.
    """

    def __init__(self, parent_pid):
        """
        Initialize Panorama

        Create the :obj:`mysql_conn`

        """
        # @modified 20230110 - Task #4778: v4.0.0 - update dependencies
        # Changed to Python 3 style super without arguments
        # super(Panorama, self).__init__()
        super().__init__()
        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        # @modified 20191031 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # Use get_redis_conn and get_redis_conn_decoded
        # if settings.REDIS_PASSWORD:
        #     self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        # else:
        #     self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)

        # @added 20191031 - Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
        # Added a single functions to deal with Redis connection and the
        # charset='utf-8', decode_responses=True arguments required in py3
        self.redis_conn = get_redis_conn(skyline_app)
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
        #                      Task #3032: Debug number of Python processes and memory use
        #                      Branch #3002: docker
        # Reduce amount of Manager instances that are used as each requires a
        # copy of entire memory to be copied into each subprocess so this
        # results in a python process per Manager instance, using as much
        # memory as the parent.  OK on a server, not so much in a container.
        # Disabled all the Manager().list() below and replaced with Redis sets
        # self.anomalous_metrics = Manager().list()
        # self.metric_variables = Manager().list()
        self.mysql_conn = mysql.connector.connect(**config)

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            # @added 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
            # Log warning
            logger.info('warning :: parent or current process dead')
            sys_exit(0)


    # @added 20200204 - Feature #3442: Panorama - add metric to metrics table immediately
    # @modified 20240119 - Task #5228: panorama - optimise insertions
    # Optimise the insertions of new metric by inserting multiple metrics at
    # once rather than one at a time
    # def insert_new_metric(self, metric_name):
    def insert_new_metric(self, metric_name,  metrics_to_insert=None):
        """
        Insert a new metrics into the metrics tables.

        :param metric_name: metric name
        :param metrics_to_insert: metric names
        :type metric_name: str
        :type metrics_to_insert: list
        :return: inserted_metric_ids

        """

        # @added 20240119 - Task #5228: panorama - optimise insertions
        # Optimise the insertions of new metric by inserting multiple metrics at
        # once rather than one at a time
        start = time()
        inserted_metric_ids = []
        get_ionosphere_learn_details_errors = []
        insert_errors = []
        if not metrics_to_insert and metric_name:
            metrics_to_insert = [metric_name]

        # @added 20241120 - Bug #5522: Handle duplicate metric names
        metrics_to_insert = list(set(metrics_to_insert))

        logger.info('insert_new_metric :: %s new metrics to insert into the metrics table' % (
            str(len(metrics_to_insert))))
        try:
            engine, log_msg, trace = get_engine(skyline_app)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: insert_new_metric :: failed to get MySQL engine, err: %s' % err)
            return inserted_metric_ids
        got_metrics_table = False
        try:
            metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
            got_metrics_table = True
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: insert_new_metric :: failed to get metrics_table meta - %s' % err)
            try:
                engine.dispose()
            except Exception as err:
                logger.error('error :: insert_new_metric :: engine.dispose() failed - %s' % (
                    err))
            return inserted_metric_ids

        # @added 20241203 - Bug #5522: Handle duplicate metric names
        duplicate_inserts = {}

        # @modified 20240119 - Task #5228: panorama - optimise insertions
        # Optimise the insertions of new metric by inserting multiple metrics at
        # once rather than one at a time
        inserted_metrics_dict = {}
        for metric_name in metrics_to_insert:

            if (time() - start) >= 10:
                logger.info('insert_new_metric :: inserted %s (of %s) new metrics, stopping after 10 seconds' % (
                    str(len(inserted_metric_ids)), str(len(metrics_to_insert))))
                break
                
            # Set defaults
            learn_full_duration_days = int(settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS)
            valid_learning_duration = int(settings.IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS)
            max_generations = int(settings.IONOSPHERE_LEARN_DEFAULT_MAX_GENERATIONS)
            max_percent_diff_from_origin = float(settings.IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN)
            try:
                use_full_duration, valid_learning_duration, use_full_duration_days, max_generations, max_percent_diff_from_origin = get_ionosphere_learn_details(skyline_app, metric_name, log=False)
                learn_full_duration_days = use_full_duration_days
            except Exception as err:
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # logger.error(traceback.format_exc())
                # logger.error('error :: failed to get_ionosphere_learn_details for %s' % metric_name)
                get_ionosphere_learn_details_errors.append([metric_name, err])

            # @modified 20240119 - Task #5228: panorama - optimise insertions
            # Reduce logging for big inserts
            if len(metrics_to_insert) < 100:
                logger.info('metric learn details determined for %s' % metric_name)
                logger.info('learn_full_duration_days     :: %s days' % (str(learn_full_duration_days)))
                logger.info('valid_learning_duration      :: %s seconds' % (str(valid_learning_duration)))
                logger.info('max_generations              :: %s' % (str(max_generations)))
                logger.info('max_percent_diff_from_origin :: %s' % (str(max_percent_diff_from_origin)))

            # @added 20230110 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                   Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            determined_id = None

            # @modified 20240119 - Task #5228: panorama - optimise insertions
            # Get the engine once
            if not engine:
                try:
                    engine, log_msg, trace = get_engine(skyline_app)
                    # @modified 20230116 - Task #4022: Move mysql_select calls to SQLAlchemy
                    #                      Task #4778: v4.0.0 - update dependencies
                    # DO NOT return the engine you muppet
                    # return engine, log_msg, trace
                except Exception as err:
                    logger.error(traceback.format_exc())
                    log_msg = 'error :: insert_new_metric :: failed to get MySQL engine - %s' % err
                    logger.error(log_msg)
                    return inserted_metric_ids

            # @modified 20240119 - Task #5228: panorama - optimise insertions
            # Get the metrics table once
            if not got_metrics_table:
                try:
                    metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    log_msg = 'error :: insert_new_metric :: failed to get metrics_table meta - %s' % err
                    logger.error(log_msg)
                    try:
                        engine.dispose()
                    except Exception as err:
                        logger.error('error :: insert_new_metric :: engine.dispose() failed - %s' % (
                            err))
                    return inserted_metric_ids

            # @modified 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                      Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            # insert_query_string = '%s (%s, learn_full_duration_days, learn_valid_ts_older_than, max_generations, max_percent_diff_from_origin) VALUES (\'%s\', %s, %s, %s, %s)' % (
            #     'metrics', 'metric', metric_name, str(learn_full_duration_days),
            #     str(valid_learning_duration), str(max_generations),
            #     str(max_percent_diff_from_origin))
            # insert_query = 'insert into %s' % insert_query_string  # nosec

            # @modified 20230116 - Bug #4816: Handle error in sqlalchemy change
            #                      Task #4022: Move mysql_select calls to SQLAlchemy
            #                      Task #4778: v4.0.0 - update dependencies
            # Simplify log arguments
            # logger.info('inserting %s into %s table' % (metric_name, 'metrics'))
            # @modified 20240119 - Task #5228: panorama - optimise insertions
            # Reduce logging for big inserts
            if len(metrics_to_insert) < 100:
                logger.info('inserting %s into metrics table' % metric_name)

            # @added 20241203 - Bug #5522: Handle duplicate metric names
            # This is not ideal as the reason for doing bulk inserts was to
            # reduce load on the DB but until and if the conditions that result
            # in duplicate metrics are isolated and mitigated, this is the best
            # way to ensure that duplicate metrics are not inserted into the DB,
            # without setting the metric column in the metrics table as unique,
            # which could have quite significant backwards compatibility
            # implications in some set ups.
            known_metric_id = 0
            try:
                connection = engine.connect()
                stmt = select([metrics_table.c.id]).where(metrics_table.c.metric == metric_name)
                result = connection.execute(stmt)
                try:
                    for row in result:
                        known_metric_id = int(row['id'])
                        if known_metric_id:
                            duplicate_inserts[known_metric_id] = metric_name
                            break
                except:
                    pass
                connection.close()
            except Exception as err:
                insert_errors.append(['duplicate metric check', metric_name, err])
            if known_metric_id:
                continue

            try:
                # @modified 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
                #                      Task #4778: v4.0.0 - update dependencies
                # Use sqlalchemy rather than string-based query construction
                # results = self.mysql_insert(insert_query)
                insert_stmt = metrics_table.insert().values(
                    metric=metric_name,
                    learn_full_duration_days=int(learn_full_duration_days),
                    learn_valid_ts_older_than=int(valid_learning_duration),
                    max_generations=int(max_generations),
                    max_percent_diff_from_origin=int(max_percent_diff_from_origin))
                connection = engine.connect()
                result = connection.execute(insert_stmt)
                connection.close()
                determined_id = result.inserted_primary_key[0]
            except Exception as err:
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # logger.error(traceback.format_exc())
                # logger.error('error :: insert_new_metric :: failed to insert metric %s - %s' % (metric_name, err))
                insert_errors.append([metric_name, err])

            # @added 20230110 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                   Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            # @modified 20240119 - Task #5228: panorama - optimise insertions
            # Dispose of engine at the end
            # try:
            #     engine.dispose()
            # except Exception as err:
            #     logger.error('error :: insert_new_metric :: engine.dispose() failed - %s' % (
            #         err))

            # @modified 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                      Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            # determined_id = 0
            # if results:
            #     determined_id = int(results)
            #     return determined_id
            # else:
            #    logger.error('error :: results not set')
            #    raise ValueError('error :: results not set')
            if not determined_id:
                logger.error('error :: insert_new_metric :: failed to determine the inserted id for %s' % metric_name)
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return None
                continue

            # @modified 20240119 - Task #5228: panorama - optimise insertions
            # return determined_id
            inserted_metric_ids.append(determined_id)
            labelled_metric_name = None
            if '_tenant_id="' in metric_name:
                labelled_metric_name = 'labelled_metrics.%s' % str(determined_id)
            inserted_metrics_dict[id] = {'metric': metric_name, 'labelled_metric_name': labelled_metric_name}

        # @added 20240119 - Task #5228: panorama - optimise insertions
        # Dispose of engine at the end
        try:
            engine.dispose()
        except Exception as err:
            logger.error('error :: insert_new_metric :: engine.dispose() failed - %s' % (
                err))

        # @added 20241203 - Bug #5522: Handle duplicate metric names
        duplicate_metrics_count = len(duplicate_inserts)
        if duplicate_metrics_count > 0:
            logger.info('insert_new_metric :: %s duplicate metrics identified and not inserted, recorded in Redis hash panorama.not_inserted_duplicate_metrics' % (
                str(duplicate_metrics_count)))
            try:
                self.redis_conn_decoded.delete('panorama.not_inserted_duplicate_metrics')
                self.redis_conn_decoded.hset('panorama.not_inserted_duplicate_metrics', mapping=duplicate_inserts)
                # Send a notification - thunder
            except Exception as err:
                logger.error('error :: failed to hset panorama.not_inserted_duplicate_metrics' % (
                    err))
            try:
                notify_slack = len(settings.SLACK_OPTS['bot_user_oauth_access_token'])
            except:
                notify_slack = False
            if notify_slack:
                slack_alerted = False
                try:
                    slack_alerted = self.redis_conn_decoded.exists('panorama.not_inserted_duplicate_metrics.slack_alerted')
                except Exception as err:
                    logger.error('error :: failed to hset panorama.not_inserted_duplicate_metrics.slack_alerted' % (
                        err))
                if slack_alerted:
                    notify_slack = False
            if notify_slack:
                slack_post = None
                try:
                    alert_slack_channel = None
                    slack_message = '*Skyline - NOTICE - panorama* - %s duplicate metrics were submitted to be inserted into the database, see Redis hash panorama.not_inserted_duplicate_metrics (which is replaced every time duplicate insertions are requested).  This notice has a 6 hour expiry, this notice Redis hash is panorama.not_inserted_duplicate_metrics.slack_alerted' % (
                        str(duplicate_metrics_count))
                    slack_post = slack_post_message(skyline_app, alert_slack_channel, None, slack_message)
                    logger.info('posted notice to slack - %s' % (slack_message))
                except Exception as err:
                    logger.error('error :: slack_post_message failed, err: %s' % (
                        err))
                if slack_post:
                    try:
                        self.redis_conn_decoded.hset('panorama.not_inserted_duplicate_metrics.slack_alerted', mapping=duplicate_inserts)
                        self.redis_conn_decoded.expire('panorama.not_inserted_duplicate_metrics.slack_alerted', 21600)
                    except Exception as err:
                        logger.error('error :: failed to hset panorama.not_inserted_duplicate_metrics.slack_alerted' % (
                            err))

        # @added 20240122 - Task #5228: panorama - optimise insertions
        # Add to the Redis ionosphere.untrainable_metrics set
        redis_set = 'ionosphere.untrainable_metrics'
        now_ts = int(time())
        remove_after_timestamp = now_ts + (86400 * 7)
        for metric_id, item in inserted_metrics_dict.items():
            use_base_name = item['metric']
            if item['labelled_metric_name']:
                use_base_name = item['labelled_metric_name']
            try:
                data = str([use_base_name, now_ts, None, now_ts, None, (86400 * 7), remove_after_timestamp])
                self.redis_conn_decoded.sadd(redis_set, data)
            except:
                logger.error('error :: failed to add %s to Redis set %s' % (
                    str(data), str(redis_set)))

        if get_ionosphere_learn_details_errors:
            logger.error('error :: insert_new_metric :: get_ionosphere_learn_details reported %s errors, sample last 3: %s' % (
                str(len(get_ionosphere_learn_details)), str(get_ionosphere_learn_details[-3:])))
        if insert_errors:
            logger.error('error :: insert_new_metric :: insert reported %s errors, sample last 3: %s' % (
                str(len(insert_errors)), str(insert_errors[-3:])))
        logger.info('insert_new_metric :: inserted %s new metrics in %s seconds' % (
            str(len(inserted_metric_ids)), str(time() - start)))
        return inserted_metric_ids

    # @added 20170101 - Feature #1830: Ionosphere alerts
    #                   Bug #1460: panorama check file fails
    #                   Panorama check file fails #24
    # Get rid of the skyline_functions imp as imp is deprecated in py3 anyway
    def new_load_metric_vars(self, metric_vars_file):
        """
        Load the metric variables for a check from a metric check variables file

        :param metric_vars_file: the path and filename to the metric variables files
        :type metric_vars_file: str
        :return: the metric_vars module object or ``False``
        :rtype: list

        """
        if os.path.isfile(metric_vars_file):
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

        # @added 20200429 - Feature #3486: analyzer_batch
        #                   Feature #3480: batch_processing
        # Allow the check file to already hold a valid python list on one line
        # so that a check can be added by simply echoing to debug metric_vars
        # line from to log for any failed checks into a new Panorama check file
        # The original above pattern is still the default, this is for the check
        # files to be added by the operator from the log or for debugging.
        try_literal_eval = False
        if metric_vars:
            if isinstance(metric_vars, list):
                pass
            else:
                try_literal_eval = True
                logger.info('metric_vars is not a list, set to try_literal_eval')
            if len(metric_vars) < 2:
                try_literal_eval = True
                logger.info('metric_vars is not a list of lists, set to try_literal_eval')
        else:
            try_literal_eval = True
            logger.info('metric_vars is not defined, set to try_literal_eval')
        if try_literal_eval:
            try:
                with open(metric_vars_file) as f:
                    for line in f:
                        metric_vars = literal_eval(line)
                        if metric_vars:
                            break
            except:
                logger.error(traceback.format_exc())
                logger.error('metric_vars not loaded with literal_eval')
                metric_vars = []

        # @modified 20200420 - Feature #3500: webapp - crucible_process_metrics
        #                      Feature #1448: Crucible web UI
        #                      Branch #868: crucible
        # Added label to string_keys and user_id to int_keys
        string_keys = ['metric', 'anomaly_dir', 'added_by', 'app', 'source', 'label']
        float_keys = ['value']
        # @modified 20241120 - Feature #5064: mirage.inflection
        # Added anomaly_id
        int_keys = [
            'from_timestamp', 'metric_timestamp', 'added_at', 'full_duration',
            'user_id', 'anomaly_id',
        ]
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
                value = int(float(value_str))
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
                    'error :: loading metric variables - none found in %s' % (
                        str(metric_vars_file)))
                return False

            if settings.ENABLE_DEBUG:
                logger.info(
                    # @modified 20200713 - AttributeError: 'list' object has no attribute 'metric' - Panorama not working #255
                    #                      Bug #1460: panorama check file fails
                    # Missed changing old method
                    # 'debug :: metric_vars determined - metric variable - metric - %s' % str(metric_vars.metric))
                    'debug :: metric_vars determined - metric variable - metric - %s' % str(metric))

        logger.info('debug :: metric_vars for %s' % str(metric))
        logger.info('debug :: %s' % str(metric_vars_array))

        return metric_vars_array

    # @modified 20200929 - Task #3748: POC SNAB
    #                      Branch #3068: SNAB
    # Handle snab as well
    # def update_slack_thread_ts(self, i, base_name, metric_timestamp, slack_thread_ts):
    def update_slack_thread_ts(self, i, base_name, metric_timestamp, slack_thread_ts, snab_id):
        """
        Update an anomaly record with the slack_thread_ts.

        :param i: python process id
        :param metric_check_file: full path to the metric check file

        :return: returns True

        """

        def get_an_engine():
            try:
                engine, log_msg, trace = get_engine(skyline_app)
                return engine, log_msg, trace
            except:
                logger.error(traceback.format_exc())
                log_msg = 'error :: update_slack_thread_ts :: failed to get MySQL engine in update_slack_thread_ts'
                logger.error('error :: update_slack_thread_ts :: failed to get MySQL engine in update_slack_thread_ts')
                return None, log_msg, trace

        def engine_disposal(engine):
            if engine:
                try:
                    engine.dispose()
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: update_slack_thread_ts :: calling engine.dispose()')
            return

        child_process_pid = os.getpid()

        if base_name:
            logger.info('update_slack_thread_ts :: child_process_pid %s, processing %s, %s, %s' % (
                str(child_process_pid), base_name, str(metric_timestamp),
                str(slack_thread_ts)))
        if snab_id:
            logger.info('update_slack_thread_ts :: child_process_pid %s, processing snab id - %s, slack_thread_ts - %s' % (
                str(child_process_pid), str(snab_id),
                str(slack_thread_ts)))

        try:
            engine, log_msg, trace = get_an_engine()
        except:
            logger.error(traceback.format_exc())
            if base_name:
                logger.error('error :: update_slack_thread_ts :: could not get a MySQL engine to update slack_thread_ts in anomalies for %s' % (base_name))
            if snab_id:
                logger.error('error :: update_slack_thread_ts :: could not get a MySQL engine to update slack_thread_ts in snab for %s' % str(snab_id))
        if not engine:
            if base_name:
                logger.error('error :: update_slack_thread_ts :: engine not obtained to update slack_thread_ts in anomalies for %s' % (base_name))
            if snab_id:
                logger.error('error :: update_slack_thread_ts :: engine not obtained to update slack_thread_ts in snab for %s' % str(snab_id))
            return False

        metric_id = None
        use_base_name = str(base_name)
        # @modified 20230418 - Feature #4848: mirage - analyse.irregular.unstable.timeseries.at.30days
        #                      Task #2732: Prometheus to Skyline
        #                      Branch #4300: prometheus
        # Handle snab using None as base_name (not a str)
        # if base_name.startswith('labelled_metrics.'):
        if use_base_name.startswith('labelled_metrics.'):
            metric_id_str = base_name.replace('labelled_metrics.', '', 1)
            if metric_id_str:
                metric_id = int(float(metric_id_str))
            try:
                use_base_name = get_base_name_from_labelled_metrics_name('panorama', base_name)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: update_slack_thread_ts :: engine not obtained to update slack_thread_ts in anomalies for %s - %s' % (
                    base_name, err))
        anomaly_record_updated = False
        if base_name:
            if not metric_id:
                try:
                    metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
                    logger.info(log_msg)
                    logger.info('update_slack_thread_ts :: metrics_table OK')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: update_slack_thread_ts :: failed to get metrics_table meta for %s' % base_name)
                try:
                    connection = engine.connect()
                    # stmt = select([metrics_table]).where(metrics_table.c.metric == base_name)
                    stmt = select([metrics_table]).where(metrics_table.c.metric == use_base_name)
                    result = connection.execute(stmt)
                    for row in result:
                        metric_id = int(row['id'])
                        # @added 20241120 - Bug #5522: Handle duplicate metric names
                        break
                    connection.close()
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: update_slack_thread_ts :: could not determine metric id from metrics table')
            logger.info('update_slack_thread_ts :: metric id determined as %s' % str(metric_id))
            try:
                anomalies_table, log_msg, trace = anomalies_table_meta(skyline_app, engine)
                logger.info(log_msg)
                logger.info('update_slack_thread_ts :: anomalies_table OK')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: update_slack_thread_ts :: failed to get anomalies_table meta for %s' % base_name)
            anomaly_id = None
            if metric_id:
                try:
                    connection = engine.connect()
                    stmt = select([anomalies_table]).\
                        where(anomalies_table.c.metric_id == metric_id).\
                        where(anomalies_table.c.anomaly_timestamp == metric_timestamp)
                    result = connection.execute(stmt)
                    for row in result:
                        anomaly_id = int(row['id'])
                    connection.close()
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: update_slack_thread_ts :: could not determine anomaly id from anomaly table')
                logger.info('update_slack_thread_ts :: anomaly id determined as %s' % str(anomaly_id))
            if anomaly_id:
                try:
                    connection = engine.connect()
                    connection.execute(
                        anomalies_table.update(
                            anomalies_table.c.id == anomaly_id).
                        values(slack_thread_ts=slack_thread_ts))
                    connection.close()
                    logger.info('update_slack_thread_ts :: updated slack_thread_ts for anomaly id %s' % str(anomaly_id))
                    anomaly_record_updated = True
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: update_slack_thread_ts :: could not update slack_thread_ts for anomaly id %s' % str(anomaly_id))
                # @added 20201002 - Task #3748: POC SNAB
                #                   Branch #3068: SNAB
                # Add a Redis key for snab to also update the original anomaly
                # id slack_thread_ts
                if anomaly_record_updated and SNAB_ENABLED:
                    anomaly_id_slack_thread_ts_redis_key = 'panorama.anomaly.id.%s.slack_thread_ts' % (str(anomaly_id))
                    try:
                        self.redis_conn.setex(anomaly_id_slack_thread_ts_redis_key, 3600, str(slack_thread_ts))
                        logger.info('update_slack_thread_ts :: created Redis key %s for snab with slack_thread_ts %s' % (
                            anomaly_id_slack_thread_ts_redis_key, str(slack_thread_ts)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: update_slack_thread_ts :: failed to create Redis key - %s' % (
                            anomaly_id_slack_thread_ts_redis_key))

            # @added 20230925 - Task #5000: Replace alert key scans with sets
            if anomaly_id is None:
                anomaly_record_updated = True

        # @added 20200929 - Task #3748: POC SNAB
        #                   Branch #3068: SNAB
        # Handle snab as well
        if snab_id:
            try:
                snab_table, log_msg, trace = snab_table_meta(skyline_app, engine)
                logger.info(log_msg)
                logger.info('update_slack_thread_ts :: snab_table OK')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: update_slack_thread_ts :: failed to get snab_table meta for %s' % str(snab_id))
            try:
                connection = engine.connect()
                stmt = snab_table.update().\
                    values(slack_thread_ts=slack_thread_ts).\
                    where(snab_table.c.id == int(snab_id))
                connection.execute(stmt)
                connection.close()
                logger.info('update_slack_thread_ts :: updated slack_thread_ts for snab id %s' % str(snab_id))
                anomaly_record_updated = True
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: update_slack_thread_ts :: could not update slack_thread_ts for snab id %s' % str(snab_id))

        if engine:
            try:
                engine_disposal(engine)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: update_slack_thread_ts :: could not dispose engine')

        if base_name:
            cache_key = 'panorama.slack_thread_ts.%s.%s' % (str(metric_timestamp), base_name)
            # cache_key = 'panorama.slack_thread_ts.%s.%s' % (str(metric_timestamp), use_base_name)
        if snab_id:
            cache_key = 'panorama.snab.slack_thread_ts.%s.%s' % (str(metric_timestamp), str(snab_id))

        delete_cache_key = False
        if anomaly_record_updated:
            delete_cache_key = True
        if not anomaly_record_updated:
            # Allow for 3600 seconds for an anomaly to be added
            now = time()
            anomaly_age = int(now) - int(metric_timestamp)
            if anomaly_age > 3600:
                delete_cache_key = True
        if delete_cache_key:
            logger.info('update_slack_thread_ts :: deleting cache_key %s' % cache_key)
            try:
                self.redis_conn.delete(cache_key)
                logger.info('update_slack_thread_ts :: cache_key %s deleted' % cache_key)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: update_slack_thread_ts :: failed to delete cache_key %s' % cache_key)

            # @added 20230925 - Task #5000: Replace alert key scans with sets
            hash_key = 'panorama.slack_threads_ts'
            hash_key_key = '%s' % str(slack_thread_ts)
            if snab_id:
                hash_key = 'panorama.snab.slack_threads_ts'
                hash_key_key = '%s.%s' % (str(metric_timestamp), str(snab_id))
            try:
                self.redis_conn.hdel(hash_key, hash_key_key)
                logger.info('update_slack_thread_ts :: key %s deleted from %s' % (hash_key_key, hash_key))
            except Exception as err:
                logger.error('error :: update_slack_thread_ts :: failed to delete key %s from %s - %s' % (
                    hash_key_key, hash_key, err))

        return

    # @added 20191107 - Feature #3306: Record anomaly_end_timestamp
    #                   Branch #3262: py3
    # @modified 20240119 - Task #5228: panorama - optimise insertions
    # Process multiple update_anomaly_end_timestamp in a single
    # process rather than handling a anomaly_end_timestamp per
    # process. Added anomalies_to_update.
    # def update_anomaly_end_timestamp(self, i, anomaly_id, anomaly_end_timestamp):
    def update_anomaly_end_timestamp(
            self, i, anomaly_id, anomaly_end_timestamp, anomalies_to_update):
        """
        Update anomaly records with the anomaly_end_timestamp.

        :param self: self
        :param i: python process id
        :param anomaly_id: the anomaly id
        :param anomaly_end_timestamp: the anomaly end timestamp
        :param anomalies_to_update: a dict of anomaly ids and anomaly_end_timestamps
        :type self: object
        :type i: object
        :type anomaly_id: int
        :type anomaly_end_timestamp: int
        :type anomalies_to_update: dict
        :return: anomaly_end_timestamps_updated
        :rtype: list
        """

        def get_an_engine():
            try:
                engine, log_msg, trace = get_engine(skyline_app)
                return engine, log_msg, trace
            except:
                logger.error(traceback.format_exc())
                log_msg = 'error :: update_anomaly_end_timestamp :: failed to get MySQL engine in update_anomaly_end_timestamp'
                logger.error('error :: update_anomaly_end_timestamp :: failed to get MySQL engine in update_anomaly_end_timestamp')
                return None, log_msg, trace

        def engine_disposal(engine):
            if engine:
                try:
                    engine.dispose()
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: update_anomaly_end_timestamp :: calling engine.dispose()')
            return

        # @added 20240119 - Task #5228: panorama - optimise insertions
        start = time()
        anomaly_end_timestamps_updated = []
        if not anomalies_to_update:
            if anomaly_id and anomaly_end_timestamp:
                anomalies_to_update[anomaly_id] = anomaly_end_timestamp

        child_process_pid = os.getpid()
        # @modified 20240119 - Task #5228: panorama - optimise insertions
        # logger.info('update_anomaly_end_timestamp :: child_process_pid %s, processing anomaly_id %s and setting anomaly_end_timestamp as %s' % (
        #     str(child_process_pid), str(anomaly_id), str(anomaly_end_timestamp)))
        # updated_anomaly_record = False
        logger.info('update_anomaly_end_timestamp :: child_process_pid %s, processing %s anomalies' % (
            str(child_process_pid), str(len(anomalies_to_update))))

        try:
            engine, log_msg, trace = get_an_engine()
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: update_anomaly_end_timestamp :: could not get a MySQL engine to update slack_thread_ts in anomalies for anomaly_id %s' % (str(anomaly_id)))
        if not engine:
            logger.error('error :: update_anomaly_end_timestamp :: engine not obtained to update slack_thread_ts in anomalies for anomaly_id %s' % (str(anomaly_id)))
            return False

        try:
            anomalies_table, log_msg, trace = anomalies_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('update_anomaly_end_timestamp :: anomalies_table OK')
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: update_anomaly_end_timestamp :: failed to get anomalies_table meta for anomaly_id %s, err: %s' % (
                str(anomaly_id), err))

        try:
            connection = engine.connect()
            # @modified 20240119 - Task #5228: panorama - optimise insertions
            # Update multiple
            # connection.execute(
            #     anomalies_table.update(
            #         anomalies_table.c.id == anomaly_id).
            #     values(anomaly_end_timestamp=anomaly_end_timestamp))
            for anomaly_id, anomaly_end_timestamp in anomalies_to_update.items():
                if (time() - start) >= 9:
                    logger.info('update_anomaly_end_timestamp :: %s of (%s) anomalies updated, breaking due to run time' % (
                        str(len(anomaly_end_timestamps_updated)),
                        str(len(anomalies_to_update))))
                    break
                try:
                    connection.execute(
                        anomalies_table.update(
                            anomalies_table.c.id == anomaly_id).
                            values(anomaly_end_timestamp=anomaly_end_timestamp))
                    anomaly_end_timestamps_updated.append(anomaly_id)
                    logger.info('update_anomaly_end_timestamp :: updated anomaly_end_timestamp (%s) for anomaly id %s' % (
                        str(anomaly_end_timestamp), str(anomaly_id)))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: update_anomaly_end_timestamp :: failed to update anomaly_end_timestamp for anomaly_id %s, err: %s' % (
                        str(anomaly_id), err))
            connection.close()
            # @modified 20240119 - Task #5228: panorama - optimise insertions
            # Update multiple
            # updated_anomaly_record = True
            # logger.info('update_anomaly_end_timestamp :: updated anomaly_end_timestamp (%s) for anomaly id %s' % (
            #     str(anomaly_end_timestamp), str(anomaly_id)))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: update_anomaly_end_timestamp :: error occurred with connection to update anomalies, err: %s' % (
                err))
        if engine:
            try:
                engine_disposal(engine)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: update_anomaly_end_timestamp :: could not dispose engine')
        # @modified 20240119 - Task #5228: panorama - optimise insertions
        # return updated_anomaly_record
        logger.info('update_anomaly_end_timestamp :: updated %s of (%s) anomalies' % (
            str(len(anomaly_end_timestamps_updated)),
            str(len(anomalies_to_update))))
        redis_key = 'panorama.update_anomaly_end_timestamp'
        if anomaly_end_timestamps_updated:
            try:
                self.redis_conn_decoded.sadd(redis_key, *set(anomaly_end_timestamps_updated))
                logger.info('update_anomaly_end_timestamp :: updated key %s' % redis_key)
            except Exception as err:
                logger.error('error :: update_anomaly_end_timestamp :: failed to sadd to key %s, err: %s' % (
                    redis_key, err))
        return anomaly_end_timestamps_updated


    # @added 20200825 - Feature #3704: Add alert to anomalies
    def update_alert_ts(self, i, base_name, metric_timestamp, alerted_at):
        """
        Update an anomaly record with the alert_ts.

        :param i: python process id
        :param base_name: base name
        :param metric_timestamp: the anomaly timestamp
        :param alerted_at: the alert timestamp

        :return: returns True

        """

        def get_an_engine():
            try:
                engine, log_msg, trace = get_engine(skyline_app)
                return engine, log_msg, trace
            except:
                logger.error(traceback.format_exc())
                log_msg = 'error :: update_alert_ts :: failed to get MySQL engine in update_alert_ts'
                logger.error('error :: update_alert_ts :: failed to get MySQL engine in update_alert_ts')
                return None, log_msg, trace

        def engine_disposal(engine):
            if engine:
                try:
                    engine.dispose()
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: update_alert_ts :: calling engine.dispose()')
            return

        child_process_pid = os.getpid()
        logger.info('update_alert_ts :: child_process_pid %s, processing %s, %s, %s' % (
            str(child_process_pid), base_name, str(metric_timestamp),
            str(alerted_at)))
        try:
            engine, log_msg, trace = get_an_engine()
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: update_alert_ts :: could not get a MySQL engine to update slack_thread_ts in anomalies for %s' % (base_name))
        if not engine:
            logger.error('error :: update_alert_ts :: engine not obtained to update slack_thread_ts in anomalies for %s' % (base_name))
            return False

        metric_id = 0

        # @added 20220810 - Task #2732: Prometheus to Skyline
        #                   Branch #4300: prometheus
        if base_name.startswith('labelled_metrics.'):
            try:
                metric_id_str = base_name.replace('labelled_metrics.', '', 1)
                metric_id = int(float(metric_id_str))
            except Exception as err:
                logger.error('error :: update_alert_ts :: failed to determine metric id for %s - %s' % (
                    base_name, err))

        if not metric_id:
            try:
                metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
                logger.info(log_msg)
                logger.info('update_alert_ts :: metrics_table OK')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: update_alert_ts :: failed to get metrics_table meta for %s' % base_name)
            try:
                connection = engine.connect()
                stmt = select([metrics_table]).where(metrics_table.c.metric == base_name)
                result = connection.execute(stmt)
                for row in result:
                    metric_id = int(row['id'])
                    # @added 20241120 - Bug #5522: Handle duplicate metric names
                    break
                connection.close()
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: update_alert_ts :: could not determine metric id from metrics table')
        logger.info('update_alert_ts :: metric id determined as %s' % str(metric_id))

        try:
            anomalies_table, log_msg, trace = anomalies_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('update_alert_ts :: anomalies_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: update_alert_ts :: failed to get anomalies_table meta for %s' % base_name)

        anomaly_id = None
        try:
            connection = engine.connect()
            stmt = select([anomalies_table]).\
                where(anomalies_table.c.metric_id == metric_id).\
                where(anomalies_table.c.anomaly_timestamp == metric_timestamp)
            result = connection.execute(stmt)
            for row in result:
                anomaly_id = int(row['id'])
            connection.close()
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: update_alert_ts :: could not determine anomaly id from anomaly table')
        logger.info('update_alert_ts :: anomaly id determined as %s' % str(anomaly_id))
        anomaly_record_updated = False
        if anomaly_id:
            try:
                connection = engine.connect()
                connection.execute(
                    anomalies_table.update(
                        anomalies_table.c.id == anomaly_id).
                    values(alert=alerted_at))
                connection.close()
                logger.info('update_alert_ts :: updated alert for anomaly id %s' % str(anomaly_id))
                anomaly_record_updated = True
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: update_alert_ts :: could not update alert for anomaly id %s' % str(anomaly_id))
        if engine:
            try:
                engine_disposal(engine)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: update_alert_ts :: could not dispose engine')
        cache_key = 'panorama.alert.%s.%s' % (str(metric_timestamp), base_name)
        delete_cache_key = False
        if anomaly_record_updated:
            delete_cache_key = True
        if not anomaly_record_updated:
            # Allow for 120 seconds for an anomaly to be added
            now = time()
            anomaly_age = int(now) - int(metric_timestamp)
            if anomaly_age > 120:
                delete_cache_key = True
        if delete_cache_key:
            logger.info('update_alert_ts :: deleting cache_key %s' % cache_key)
            try:
                self.redis_conn.delete(cache_key)
                logger.info('update_alert_ts :: cache_key %s deleted' % cache_key)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: update_alert_ts :: failed to delete cache_key %s' % cache_key)

            # @added 20230925 - Task #5000: Replace alert key scans with sets
            hash_key = 'panorama.alerts'
            hash_key_key = '%s.%s' % (str(metric_timestamp), base_name)
            try:
                self.redis_conn.hdel(hash_key, hash_key_key)
                logger.info('update_alert_ts :: key %s deleted from %s' % (hash_key_key, hash_key))
            except Exception as err:
                logger.error('error :: update_alert_ts :: failed to delete key %s from %s - %s' % (
                    hash_key_key, hash_key, err))

        return

    # @added 20200928 - Task #3748: POC SNAB
    #                   Branch #3068: SNAB
    # Determine id of something thing
    # @modified 20240119 - Task #5228: panorama - optimise insertions
    # Added engine and use_table to pass the created engine and table which is
    # done once in the spin_process 
    # def determine_db_id(self, table, key, value):
    def determine_db_id(self, table, key, value, engine=None, use_table=None, log=True):
        """
        Get the id of something from the database and create a new entry if it
        does not exist

        :param table: table name
        :param key: key name
        :param value: value name
        :param engine: the engine object
        :param use_table: the use_table object
        :type table: str
        :type key: str
        :type value: str
        :type engine: object
        :type use_table: object
        :return: int or boolean

        """

        determined_id = None

        # @modified 20240119 - Task #5228: panorama - optimise insertions
        engine_called = False

        # @added 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
        #                   Task #4778: v4.0.0 - update dependencies
        # Use sqlalchemy rather than string-based query construction
        # @modified 20240119 - Task #5228: panorama - optimise insertions
        # Only get engine if no engine is passed
        if not engine:
            try:
                engine, fail_msg, trace = get_engine(skyline_app)
                # @added 20240119 - Task #5228: panorama - optimise insertions
                if engine:
                    engine_called = True
            except Exception as err:
                logger.error('error :: determine_db_id :: could not get a MySQL engine - %s' % err)
                return determined_id

        # Use the MetaData autoload rather than string-based query construction
        # @modified 20240119 - Task #5228: panorama - optimise insertions
        # Only surface the use_table if no use_table is passed
        if 'sqlalchemy.sql.schema.Table' not in str(type(use_table)):
            try:
                use_table_meta = MetaData()
                use_table = Table(table, use_table_meta, autoload=True, autoload_with=engine)
            except Exception as err:
                logger.error('error :: determine_db_id :: use_table Table failed on %s table - %s' % (
                    table, err))

        # @modified 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
        #                      Task #4778: v4.0.0 - update dependencies
        # Use sqlalchemy rather than string-based query construction
        # query = 'select id FROM %s WHERE %s=\'%s\'' % (table, key, value)  # nosec

        determined_id = 0
        # results = None
        try:
            # @modified 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                      Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            # results = self.mysql_select(query)
            stmt = select([use_table.c.id]).where(use_table.c[key] == value)
            connection = engine.connect()
            for row in engine.execute(stmt):
                determined_id = row['id']
                break
            connection.close()
        except Exception as err:
            logger.error('error :: determine_db_id :: failed to determine results from - %s - %s' % (
                str(stmt), err))

        # @modified 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
        #                      Task #4778: v4.0.0 - update dependencies
        # Use sqlalchemy rather than string-based query construction
        # determined_id = 0
        # if results:
        #     try:
        #         determined_id = int(results[0][0])
        #     except:
        #         logger.error(traceback.format_exc())
        #         logger.error('error :: determined_id is not an int')
        #         determined_id = 0

        if determined_id > 0:
            # @added 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                   Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            # @modified 20240119 - Task #5228: panorama - optimise insertions
            # Only dispose of the engine if called and not passed
            if engine_called:
                try:
                    engine.dispose()
                except Exception as err:
                    logger.error('error :: determine_db_id :: engine.dispose() failed - %s' % (
                        err))

            return int(determined_id)

        # @added 20170115 - Feature #1854: Ionosphere learn - generations
        # Added determination of the learn related variables
        # learn_full_duration_days, learn_valid_ts_older_than,
        # max_generations and max_percent_diff_from_origin value to the
        # insert statement if the table is the metrics table.
        if table == 'metrics' and key == 'metric':
            # Set defaults
            learn_full_duration_days = int(settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS)
            valid_learning_duration = int(settings.IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS)
            max_generations = int(settings.IONOSPHERE_LEARN_DEFAULT_MAX_GENERATIONS)
            max_percent_diff_from_origin = float(settings.IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN)
            try:
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # Reduce logging on bulk inserts
                # use_full_duration, valid_learning_duration, use_full_duration_days, max_generations, max_percent_diff_from_origin = get_ionosphere_learn_details(skyline_app, value)
                use_full_duration, valid_learning_duration, use_full_duration_days, max_generations, max_percent_diff_from_origin = get_ionosphere_learn_details(skyline_app, value, log=log)
                learn_full_duration_days = use_full_duration_days
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get_ionosphere_learn_details for %s' % value)

            # @modified 20240119 - Task #5228: panorama - optimise insertions
            # Reduce logging on bulk inserts
            if log:
                logger.info('metric learn details determined for %s' % value)
                logger.info('learn_full_duration_days     :: %s days' % (str(learn_full_duration_days)))
                logger.info('valid_learning_duration      :: %s seconds' % (str(valid_learning_duration)))
                logger.info('max_generations              :: %s' % (str(max_generations)))
                logger.info('max_percent_diff_from_origin :: %s' % (str(max_percent_diff_from_origin)))

        # INSERT because no known id
        if table == 'metrics' and key == 'metric':

            # @modified 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                   Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            # Comment out insert_query
            # insert_query_string = '%s (%s, learn_full_duration_days, learn_valid_ts_older_than, max_generations, max_percent_diff_from_origin) VALUES (\'%s\', %s, %s, %s, %s)' % (
            #     table, key, value, str(learn_full_duration_days),
            #     str(valid_learning_duration), str(max_generations),
            #     str(max_percent_diff_from_origin))
            # insert_query = 'insert into %s' % insert_query_string  # nosec

            # @added 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                   Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            insert_stmt = use_table.insert().values(**{
                key: value,
                'learn_full_duration_days': int(learn_full_duration_days),
                'learn_valid_ts_older_than': int(valid_learning_duration),
                'max_generations': int(max_generations),
                'max_percent_diff_from_origin': int(max_percent_diff_from_origin)})

        else:
            # @modified 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                   Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            # Comment out insert_query
            # insert_query = 'insert into %s (%s) VALUES (\'%s\')' % (table, key, value)  # nosec

            # @added 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                   Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            insert_stmt = use_table.insert().values(**{key: value})

        # @modified 20240119 - Task #5228: panorama - optimise insertions
        # Reduce logging on bulk inserts
        if log:
            logger.info('inserting %s into %s table' % (value, table))

        try:
            # @modified 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                      Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            # results = self.mysql_insert(insert_query)
            connection = engine.connect()
            result = connection.execute(insert_stmt)
            connection.close()
            determined_id = result.inserted_primary_key[0]
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to determine the id of %s from the insert' % (value))
            # @added 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                   Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            # @modified 20240119 - Task #5228: panorama - optimise insertions
            # Only dispose of the engine if called and not passed
            if engine_called:
                try:
                    engine.dispose()
                except Exception as err:
                    logger.error('error :: determine_db_id :: engine.dispose() failed - %s' % (
                        err))

            raise

        # @modified 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
        #                      Task #4778: v4.0.0 - update dependencies
        # Use sqlalchemy rather than string-based query construction
        # determined_id = 0
        # if results:
        #    determined_id = int(results)
        # else:
        if not determined_id:
            # @added 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                   Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            # @modified 20240119 - Task #5228: panorama - optimise insertions
            # Only dispose of the engine if called and not passed
            if engine_called:
                try:
                    engine.dispose()
                except Exception as err:
                    logger.error('error :: determine_db_id :: engine.dispose() failed - %s' % (
                        err))

            logger.error('error :: no determined_id')
            raise ValueError('error :: no determined_id')

        # @added 20220822 - Task #2732: Prometheus to Skyline
        #                   Branch #4300: prometheus
        # Training data should only be returned for fp creation if there is
        # sufficient data for a metric.  By adding to the Redis set
        # ionosphere.untrainable_metrics, training data items are removed
        # from the api training_data response, defaulting and hardcoding 7
        # days because the metric may not fall into an alert tuple
        if determined_id > 0:
            if table == 'metrics' and key == 'metric':
                redis_set = 'ionosphere.untrainable_metrics'
                use_base_name = str(value)
                if '_tenant_id' in value:
                    use_base_name = 'labelled_metrics.%s' % str(determined_id)
                try:
                    now_ts = int(time())
                    remove_after_timestamp = now_ts + (86400 * 7)
                    data = str([use_base_name, now_ts, None, now_ts, None, (86400 * 7), remove_after_timestamp])
                    self.redis_conn.sadd(redis_set, data)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to add %s to Redis set %s' % (
                        str(data), str(redis_set)))

        # @added 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
        #                   Task #4778: v4.0.0 - update dependencies
        # Use sqlalchemy rather than string-based query construction
        # @modified 20240119 - Task #5228: panorama - optimise insertions
        # Only dispose of the engine if called and not passed
        if engine_called:
            try:
                engine.dispose()
            except Exception as err:
                logger.error('error :: determine_db_id :: engine.dispose() failed - %s' % (
                    err))

        if determined_id > 0:
            return determined_id

        logger.error('error :: determine_db_id :: failed to determine the inserted id for %s' % value)
        return False

    # @added 20200928 - Task #3748: POC SNAB
    #                   Branch #3068: SNAB
    def update_snab(self, i, snab_panorama_items):
        """
        Update snab anomaly records.

        :param i: python process id
        :return: returns True

        """

        def get_an_engine():
            try:
                engine, log_msg, trace = get_engine(skyline_app)
                return engine, log_msg, trace
            except:
                logger.error(traceback.format_exc())
                log_msg = 'error :: update_snab :: failed to get MySQL engine'
                logger.error('error :: update_snab :: failed to get MySQL engine')
                return None, log_msg, trace

        def engine_disposal(engine):
            if engine:
                try:
                    engine.dispose()
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: update_snab :: calling engine.dispose()')
            return

        child_process_pid = os.getpid()
        logger.info('update_snab :: child_process_pid %s' % str(child_process_pid))

        redis_set = 'snab.panorama'
        try:
            snab_panorama_items = self.redis_conn_decoded.smembers(redis_set)
        except:
            logger.info(traceback.format_exc())
            logger.error('error :: failed to get data from %s Redis set' % redis_set)
            snab_panorama_items = []
        if not snab_panorama_items:
            return False

        try:
            engine, log_msg, trace = get_an_engine()
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: update_snab :: could not get a MySQL engine to update snab table')
        if not engine:
            logger.error('error :: update_snab :: engine not obtained to update snab table')
            return False
        try:
            metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('update_snab :: metrics_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: update_snab :: failed to get metrics_table meta')
            if engine:
                engine_disposal(engine)
            return False
        try:
            anomalies_table, log_msg, trace = anomalies_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('update_snab :: anomalies_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: update_snab :: failed to get anomalies_table meta')
            if engine:
                engine_disposal(engine)
            return False
        try:
            snab_table, log_msg, trace = snab_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('update_snab :: snab_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: update_snab :: failed to get snab_table meta')
            if engine:
                engine_disposal(engine)
            return False

        for item in snab_panorama_items:
            remove_item = False
            base_name = None
            try:
                snab_item = literal_eval(item)
                base_name = snab_item['metric']
                metric_timestamp = snab_item['timestamp']
                try:
                    analysis_run_time = snab_item['analysis_run_time']
                except:
                    analysis_run_time = 0.0
                source_app = snab_item['source']
                algorithm_group = snab_item['algorithm_group']
                algorithm = snab_item['algorithm']
                added_at = snab_item['added_at']
                # @added 20201001 - Task #3748: POC SNAB
                # Added anomalyScore
                anomalyScore = snab_item['anomalyScore']
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to evaluate data from item %s from Redis set %s' % (
                    str(item), redis_set))
            metric_id = None
            if base_name:
                try:
                    connection = engine.connect()
                    stmt = select([metrics_table]).where(metrics_table.c.metric == base_name)
                    result = connection.execute(stmt)
                    for row in result:
                        metric_id = int(row['id'])
                        # @added 20241120 - Bug #5522: Handle duplicate metric names
                        break
                    connection.close()
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: update_snab :: could not determine metric id from metrics table')
                logger.info('update_snab :: metric id determined as %s' % str(metric_id))
            anomaly_id = None
            if metric_id:
                try:
                    connection = engine.connect()
                    stmt = select([anomalies_table]).\
                        where(anomalies_table.c.metric_id == metric_id).\
                        where(anomalies_table.c.anomaly_timestamp == metric_timestamp)
                    result = connection.execute(stmt)
                    for row in result:
                        anomaly_id = int(row['id'])
                    connection.close()
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: update_snab :: could not determine anomaly id from anomaly table')
            logger.info('update_snab :: anomaly id determined as %s' % str(anomaly_id))
            if anomaly_id:
                app_id = None
                try:
                    app_id = self.determine_db_id('apps', 'app', source_app)
                except:
                    logger.error('error :: update_snab :: failed to determine app_id for - %s' % (source_app))
                    app_id = None
                algorithm_group_id = None
                try:
                    algorithm_group_id = self.determine_db_id('algorithm_groups', 'algorithm_group', algorithm_group)
                except:
                    logger.error('error :: update_snab :: failed to determine algorithm_group_id for - %s' % (algorithm_group))
                    algorithm_group = 0
                algorithm_id = None
                if algorithm:
                    try:
                        algorithm_id = self.determine_db_id('algorithms', 'algorithm', algorithm)
                    except:
                        logger.error('error :: update_snab :: failed to determine algorithm_group_id for - %s' % (algorithm_group))
                        algorithm_id = None
                new_snab_id = None
                try:
                    connection = engine.connect()
                    if algorithm_id:
                        ins = snab_table.insert().values(
                            anomaly_id=anomaly_id,
                            # @added 20201001 - Task #3748: POC SNAB
                            # Added anomalyScore
                            anomalyScore=anomalyScore,
                            app_id=int(app_id),
                            algorithm_group_id=int(algorithm_group_id),
                            algorithm_id=int(algorithm_id),
                            runtime=float(analysis_run_time),
                            snab_timestamp=int(added_at))
                    else:
                        ins = snab_table.insert().values(
                            anomaly_id=anomaly_id,
                            # @added 20201001 - Task #3748: POC SNAB
                            # Added anomalyScore
                            anomalyScore=anomalyScore,
                            app_id=int(app_id),
                            algorithm_group_id=int(algorithm_group_id),
                            runtime=float(analysis_run_time),
                            snab_timestamp=int(added_at))
                    result = connection.execute(ins)
                    new_snab_id = result.inserted_primary_key[0]
                    connection.close()
                    logger.info('update_snab :: new snab id: %s' % str(new_snab_id))
                    remove_item = True
                except:
                    logger.error(traceback.format_exc())
                    logger.error(
                        'error :: update_snab :: could not update snab for %s with with timestamp %s' % (
                            str(base_name), str(metric_timestamp)))
                if new_snab_id:
                    snab_id_redis_key = 'snab.id.%s.%s.%s.%s' % (algorithm_group, str(metric_timestamp), base_name, str(added_at))
                    # @added 20230419 - Feature #4848: mirage - analyse.irregular.unstable.timeseries.at.30days
                    #                   Task #2732: Prometheus to Skyline
                    #                   Branch #4300: prometheus
                    if '_tenant_id="' in base_name:
                        labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
                        snab_id_redis_key = 'snab.id.%s.%s.%s.%s' % (algorithm_group, str(metric_timestamp), labelled_metric_name, str(added_at))

                    try:
                        self.redis_conn.setex(snab_id_redis_key, 3600, int(new_snab_id))
                        logger.info('update_snab :: created Redis key %s for snab id %s' % (
                            snab_id_redis_key, str(new_snab_id)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: update_snab :: failed to create Redis key - %s' % (
                            snab_id_redis_key))

            if not anomaly_id:
                if (int(time()) - 600) > added_at:
                    logger.error(
                        'error :: update_snab :: removing snab.panorama item as no anomaly id can be determined after 600 seconds - %s' % (
                            str(snab_item)))
                    remove_item = True
            if remove_item:
                try:
                    self.redis_conn.srem(redis_set, str(item))
                    logger.info('update_snab :: removed item from Redis set %s - %s' % (
                        redis_set, str(item)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: update_snab :: failed to remove item from Redis set %s - %s' % (
                        redis_set, str(item)))
        if engine:
            engine_disposal(engine)
        return

    # @modified 20240119 - Task #5228: panorama - optimise insertions
    # def spin_process(self, i, metric_check_file):
    def spin_process(self, i, metric_check_file, assigned_metric_check_files):
        """
        Assign a metric anomaly to process.

        :param i: python process id
        :param metric_check_file: full path to the metric check file

        :return: returns True

        """

        child_process_pid = os.getpid()
        if settings.ENABLE_PANORAMA_DEBUG:
            logger.info('debug :: child_process_pid - %s' % str(child_process_pid))

        # @added 20240119 - Task #5228: panorama - optimise insertions
        # Process multiple metric check file in a single process rather than
        # handle a single metric_check_file per process.  Create the engine once
        # and get the tables required once
        start = time()
        logger.info('spin_process :: child_process_pid - %s, assigned %s checks to process' % (
            str(child_process_pid), str(len(assigned_metric_check_files))))
        inserted_anomaly_ids = []
        processed_checks = {}
        engine = None
        try:
            engine, fail_msg, trace = get_engine(skyline_app)
        except Exception as err:
            logger.error('error :: spin_process :: could not get a MySQL engine - %s' % err)
            engine = None
        hosts_table = None
        apps_table = None
        sources_table = None
        metrics_table = None
        algorithms_table = None
        # @added 20240122 - Task #5228: panorama - optimise insertions
        # Only get anomalies_table once
        anomalies_table = None

        required_tables = ['hosts', 'apps', 'sources', 'metrics', 'algorithms', 'anomalies']
        if engine:
            for i_table in required_tables:
                try:
                    use_table_meta = MetaData()
                    if i_table == 'hosts':
                        hosts_table = Table(i_table, use_table_meta, autoload=True, autoload_with=engine)
                    if i_table == 'apps':
                        apps_table = Table(i_table, use_table_meta, autoload=True, autoload_with=engine)
                    if i_table == 'sources':
                        sources_table = Table(i_table, use_table_meta, autoload=True, autoload_with=engine)
                    if i_table == 'metrics':
                        metrics_table = Table(i_table, use_table_meta, autoload=True, autoload_with=engine)
                    if i_table == 'algorithms':
                        algorithms_table = Table(i_table, use_table_meta, autoload=True, autoload_with=engine)
                    # @added 20240122 - Task #5228: panorama - optimise insertions
                    # Only get anomalies_table once
                    if i_table == 'anomalies':
                        anomalies_table = Table(i_table, use_table_meta, autoload=True, autoload_with=engine)
                except Exception as err:
                    logger.error('error :: spin_process :: use_table Table failed on %s table, err: %s' % (
                        i_table, err))

        # @added 20240122 - Task #5228: panorama - optimise insertions
        # Get apps, algorithms, sources and hosts once
        apps = {}
        try:
            apps = get_apps(skyline_app)
        except Exception as err:
            logger.error('error :: spin_process :: get_apps failed - %s' % (
                err))
        all_algorithms = {}
        all_algorithms_by_id = {}
        try:
            all_algorithms, all_algorithms_by_id = get_algorithms(skyline_app, return_all_algorithms_by_id=True)
        except Exception as err:
            logger.error('error :: spin_process :: get_algorithms failed - %s' % err)
        all_algorithms_by_name = {}
        for id in list(all_algorithms_by_id.keys()):
            all_algorithms_by_name[all_algorithms_by_id[id]] = id
        sources = {}
        try:
            sources = get_sources(skyline_app)
        except Exception as err:
            logger.error('error :: spin_process :: get_sources failed - %s' % err)
        sources_by_id = {}
        if sources:
            for source, id in sources.items():
                sources_by_id[id] = source
        hosts = {}
        try:
            hosts = get_hosts(skyline_app)
        except Exception as err:
            logger.error('error :: spin_process :: get_hosts failed - %s' % (err))
        hosts_by_id = {}
        if hosts:
            for i_host, id in hosts.items():
                hosts_by_id[id] = i_host

        # @modified 20240119 - Task #5228: panorama - optimise insertions
        # Process multiple metric check file in a single process rather than
        # handle a single metric_check_file per process. Using the same code as
        # the single metric_check_file process only per metric_check_file in
        # assigned_metric_check_files
        for metric_check_file in assigned_metric_check_files:

            # @added 20240119 - Task #5228: panorama - optimise insertions
            # Process multiple metric check file in a single process rather than
            run_seconds = time() - start
            if run_seconds >= 19:
                logger.info('spin_process :: child_process_pid - %s, processed %s checks of the %s assigned checks in %s, max run time reached stopping' % (
                    str(child_process_pid), str(len(processed_checks)),
                    str(len(assigned_metric_check_files)), str(run_seconds)))
                break
                
            processed_checks[metric_check_file] = 0

            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: spin_process :: processing metric check - %s' % metric_check_file)

            if not os.path.isfile(str(metric_check_file)):
                logger.error('error :: file not found - metric_check_file - %s' % (str(metric_check_file)))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            check_file_name = os.path.basename(str(metric_check_file))
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: check_file_name - %s' % check_file_name)
            check_file_timestamp = check_file_name.split('.', 1)[0]
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: check_file_timestamp - %s' % str(check_file_timestamp))
            check_file_metricname_txt = check_file_name.split('.', 1)[1]
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: check_file_metricname_txt - %s' % check_file_metricname_txt)
            check_file_metricname = check_file_metricname_txt.replace('.txt', '')
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: check_file_metricname - %s' % check_file_metricname)
            check_file_metricname_dir = check_file_metricname.replace('.', '/')
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: check_file_metricname_dir - %s' % check_file_metricname_dir)

            metric_failed_check_dir = '%s/%s/%s' % (failed_checks_dir, check_file_metricname_dir, check_file_timestamp)

            failed_check_file = '%s/%s' % (metric_failed_check_dir, check_file_name)
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: failed_check_file - %s' % failed_check_file)

            # Load and validate metric variables
            try:
                # @modified 20170101 - Feature #1830: Ionosphere alerts
                #                      Bug #1460: panorama check file fails
                #                      Panorama check file fails #24
                # Get rid of the skyline_functions imp as imp is deprecated in py3 anyway
                # Use def new_load_metric_vars(self, metric_vars_file):
                # metric_vars = load_metric_vars(skyline_app, str(metric_check_file))
                metric_vars_array = self.new_load_metric_vars(str(metric_check_file))
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to load metric variables from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            # Test metric variables
            # We use a pythonic methodology to test if the variables are defined,
            # this ensures that if any of the variables are not set for some reason
            # we can handle unexpected data or situations gracefully and try and
            # ensure that the process does not hang.
            metric = None
            try:
                # metric_vars.metric
                # metric = str(metric_vars.metric)
                key = 'metric'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                metric = str(value_list[0])
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: metric variable - metric - %s' % metric)
            except:
                logger.error('error :: failed to read metric variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            if not metric:
                logger.error('error :: failed to load metric variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            value = None
            # @added 20171214 - Bug #2234: panorama metric_vars value check
            value_valid = None
            try:
                # metric_vars.value
                # value = str(metric_vars.value)
                key = 'value'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                value = float(value_list[0])
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: metric variable - value - %s' % (value))
                # @added 20171214 - Bug #2234: panorama metric_vars value check
                value_valid = True
            except:
                logger.error('error :: failed to read value variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            # @added 20171214 - Bug #2234: panorama metric_vars value check
            # If value was float of 0.0 then this was interpolated as not set
            # if not value:
            if not value_valid:
                # @added 20171214 - Bug #2234: panorama metric_vars value check
                # Added exception handling here
                logger.info(traceback.format_exc())
                logger.error('error :: failed to read value variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            from_timestamp = None
            try:
                # metric_vars.from_timestamp
                # from_timestamp = str(metric_vars.from_timestamp)
                key = 'from_timestamp'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                from_timestamp = int(value_list[0])
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: metric variable - from_timestamp - %s' % from_timestamp)
            except:
                # @added 20160822 - Bug #1460: panorama check file fails
                # Added exception handling here
                logger.info(traceback.format_exc())
                logger.error('error :: failed to read from_timestamp variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            if not from_timestamp:
                logger.error('error :: failed to load from_timestamp variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            metric_timestamp = None
            try:
                # metric_vars.metric_timestamp
                # metric_timestamp = str(metric_vars.metric_timestamp)
                key = 'metric_timestamp'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                metric_timestamp = int(value_list[0])
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: metric variable - metric_timestamp - %s' % metric_timestamp)
            except:
                logger.error('error :: failed to read metric_timestamp variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            if not metric_timestamp:
                logger.error('error :: failed to load metric_timestamp variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            algorithms = None
            try:
                # metric_vars.algorithms
                # algorithms = metric_vars.algorithms
                key = 'algorithms'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                algorithms = value_list[0]
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: metric variable - algorithms - %s' % str(algorithms))
            except:
                logger.error('error :: failed to read algorithms variable from check file setting to all - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            if not algorithms:
                logger.error('error :: failed to load algorithms variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            triggered_algorithms = None
            try:
                # metric_vars.triggered_algorithms
                # triggered_algorithms = metric_vars.triggered_algorithms
                key = 'triggered_algorithms'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                triggered_algorithms = value_list[0]
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: metric variable - triggered_algorithms - %s' % str(triggered_algorithms))
            except:
                logger.error('error :: failed to read triggered_algorithms variable from check file setting to all - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            if not triggered_algorithms:
                logger.error('error :: failed to load triggered_algorithms variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            app = None
            try:
                # metric_vars.app
                # app = str(metric_vars.app)
                key = 'app'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                app = str(value_list[0])
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: metric variable - app - %s' % app)
            except:
                logger.error('error :: failed to read app variable from check file setting to all  - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            if not app:
                logger.error('error :: failed to load app variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            source = None
            try:
                # metric_vars.source
                # source = str(metric_vars.source)
                key = 'source'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                source = str(value_list[0])
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: metric variable - source - %s' % source)
            except:
                logger.error('error :: failed to read source variable from check file setting to all  - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            if not app:
                logger.error('error :: failed to load app variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            added_by = None
            try:
                # metric_vars.added_by
                # added_by = str(metric_vars.added_by)
                key = 'added_by'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                added_by = str(value_list[0])
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: metric variable - added_by - %s' % added_by)
            except:
                logger.error('error :: failed to read added_by variable from check file setting to all - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            if not added_by:
                logger.error('error :: failed to load added_by variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            added_at = None
            try:
                # metric_vars.added_at
                # added_at = str(metric_vars.added_at)
                key = 'added_at'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                added_at = str(value_list[0])
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: metric variable - added_at - %s' % added_at)
            except:
                logger.error('error :: failed to read added_at variable from check file setting to all - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            if not added_at:
                logger.error('error :: failed to load added_at variable from check file - %s' % (metric_check_file))
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            # @modified 20200420 - Feature #3500: webapp - crucible_process_metrics
            #                      Feature #1448: Crucible web UI
            #                      Branch #868: crucible
            # Added label to string_keys and user_id to int_keys
            label = None
            try:
                key = 'label'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                label = str(value_list[0])
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: metric variable - label - %s' % label)
            except:
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: metric variable - label was not found - %s' % label)
            user_id = None
            try:
                key = 'user_id'
                value_list = [var_array[1] for var_array in metric_vars_array if var_array[0] == key]
                user_id = str(value_list[0])
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: metric variable - user_id - %s' % user_id)
            except:
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: metric variable - user_id was not found - %s' % user_id)

            record_anomaly = True
            cache_key = '%s.last_check.%s.%s' % (skyline_app, app, metric)
            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: cache_key - %s.last_check.%s.%s' % (
                    skyline_app, app, metric))
            try:
                # @modified 20200603 - Task #3562; Change panorama.last_check keys to timestamp value
                #                      Feature #3486: analyzer_batch
                #                      Feature #3480: batch_processing
                # As a int is now used and not a msgpack value, decoded the key
                # value
                # last_check = self.redis_conn.get(cache_key)
                last_check = self.redis_conn_decoded.get(cache_key)
            except Exception as e:
                logger.error(
                    'error :: could not query cache_key - %s.last_check.%s.%s - %s' % (
                        skyline_app, app, metric, e))
                last_check = None

            # @modified 20200420 - Feature #3500: webapp - crucible_process_metrics
            #                      Feature #1448: Crucible web UI
            #                      Branch #868: crucible
            # Added label to string_keys and user_id to int_keys
            if app in ['webapp', 'crucible']:
                logger.info('anomaly added by %s recording anomaly unsetting last_check' % (
                    app))
                last_check = None

            # @added 20200413 - Feature #3486: analyzer_batch
            #                   Feature #3480: batch_processing
            # Evaluate the reported anomaly timestamp to determine whether
            # PANORAMA_EXPIRY_TIME should be applied to a batch metric
            batch_metric = None
            if BATCH_PROCESSING:
                batch_metric = True
                try:
                    batch_metric = is_batch_metric(skyline_app, metric)
                except:
                    batch_metric = True
            if batch_metric:
                # Is this a analyzer_batch related anomaly
                analyzer_batch_anomaly = None
                analyzer_batch_metric_anomaly_key = 'analyzer_batch.anomaly.%s.%s' % (
                    str(metric_timestamp), metric)
                try:
                    analyzer_batch_anomaly = self.redis_conn_decoded.get(analyzer_batch_metric_anomaly_key)
                except Exception as e:
                    logger.error(
                        'error :: could not query cache_key - %s - %s' % (
                            analyzer_batch_metric_anomaly_key, e))
                    analyzer_batch_anomaly = None
                if analyzer_batch_anomaly:
                    logger.info('identified as an analyzer_batch triggered anomaly from the presence of the Redis key %s' % analyzer_batch_metric_anomaly_key)
                else:
                    logger.info('not identified as an analyzer_batch triggered anomaly as no Redis key found - %s' % analyzer_batch_metric_anomaly_key)
                if last_check and analyzer_batch_anomaly:
                    last_timestamp = None
                    try:
                        last_timestamp = int(last_check)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to determine last_timestamp for batch metric Panorama key- %s' % cache_key)
                        last_timestamp = None
                    seconds_between_batch_anomalies = None
                    if last_timestamp:
                        try:
                            seconds_between_batch_anomalies = int(metric_timestamp) - int(last_timestamp)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to determine seconds_between_batch_anomalies for batch metric Panorama key- %s' % cache_key)
                            last_timestamp = None
                    if seconds_between_batch_anomalies:
                        if seconds_between_batch_anomalies > settings.PANORAMA_EXPIRY_TIME:
                            logger.info('the difference between the last anomaly timestamp (%s) and the current anomaly timestamp (%s) for batch metric %s is greater than %s' % (
                                str(last_timestamp), str(metric_timestamp), metric,
                                str(settings.PANORAMA_EXPIRY_TIME)))
                            logger.info('recording anomaly for batch metric %s, so setting last_check to None' % (
                                metric))
                            last_check = None
                        else:
                            logger.info('the difference between the last anomaly timestamp (%s) and the current anomaly timestamp (%s) for batch metric %s is less than %s, not recording the anomaly' % (
                                str(last_timestamp), str(metric_timestamp), metric,
                                str(settings.PANORAMA_EXPIRY_TIME)))

                        # @added 20200603 - Task #3562; Change panorama.last_check keys to timestamp value
                        #                   Feature #3486: analyzer_batch
                        #                   Feature #3480: batch_processing
                        # If the metric is a batch processing metric and the anomaly
                        # timestamp is less than the last_check timestamp, insert
                        # the anomaly
                        if int(metric_timestamp) < last_timestamp:
                            logger.info('batch anomaly timestamp (%s) less than the last_check timestamp (%s), recording anomaly for batch metric %s, so setting last_check to None' % (
                                str(metric_timestamp), str(last_timestamp), metric))
                            last_check = None

            if last_check:
                record_anomaly = False
                logger.info(
                    'Panorama metric key not expired - %s.last_check.%s.%s' % (
                        skyline_app, app, metric))

            # @added 20200420 - Feature #3500: webapp - crucible_process_metrics
            #                   Feature #1448: Crucible web UI
            #                   Branch #868: crucible
            # Added check_max_age
            check_max_age = True
            set_anomaly_key = True
            add_to_current_anomalies = True
            if app in ['webapp', 'crucible']:
                check_max_age = False
                set_anomaly_key = False
                add_to_current_anomalies = False
            if batch_metric:
                check_max_age = False

            # @added 20160907 - Handle Panorama stampede on restart after not running #26
            # Allow to expire check if greater than PANORAMA_CHECK_MAX_AGE
            # @modified 20200413 - Feature #3486: analyzer_batch
            #                      Feature #3480: batch_processing
            # Only evaluate max_age is the metric is not a batch metric
            # @modied 20200420 - Feature #3500: webapp - crucible_process_metrics
            #                   Feature #1448: Crucible web UI
            #                   Branch #868: crucible
            # if not batch_metric:
            if check_max_age:
                if max_age:
                    now = time()
                    anomaly_age = int(now) - int(metric_timestamp)
                    if anomaly_age > max_age_seconds:
                        record_anomaly = False
                        logger.info(
                            'Panorama check max age exceeded - %s - %s seconds old, older than %s seconds discarding' % (
                                metric, str(anomaly_age), str(max_age_seconds)))

            if not record_anomaly:
                logger.info('not recording anomaly for - %s' % (metric))
                if os.path.isfile(str(metric_check_file)):
                    try:
                        os.remove(str(metric_check_file))
                        logger.info('metric_check_file removed - %s' % str(metric_check_file))
                    except OSError:
                        pass

                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return
                continue

            # @added 20240122 - Task #5228: panorama - optimise insertions
            # Get apps, algorithms, sources and hosts once
            added_by_host_id = None
            try:
                added_by_host_id = hosts[added_by]
            except Exception as err:
                logger.error('error :: spin_process :: failed to determine added_by_host_id from hosts dict, err: %s' % (err))
            
            # @modified 20240122 - Task #5228: panorama - optimise insertions
            # Only query DB if not determined from hosts dict
            if not added_by_host_id:
                try:
                    # @modified 20230110 - Task #4022: Move mysql_select calls to SQLAlchemy
                    #                     Task #4778: v4.0.0 - update dependencies
                    # Use sqlalchemy rather than string-based query construction
                    # Use self.determine_db_id instead, declare the function once
                    # added_by_host_id = determine_id('hosts', 'host', added_by)
                    # @modified 20240119 - Task #5228: panorama - optimise insertions
                    # Added engine and use_table
                    # added_by_host_id = self.determine_db_id('hosts', 'host', added_by)
                    added_by_host_id = self.determine_db_id('hosts', 'host', added_by, engine=engine, use_table=hosts_table)
                except Exception as err:
                    # logger.error('error :: failed to determine id of %s' % (added_by))
                    logger.error(traceback.format_exc())
                    logger.error('error :: self.determine_db_id failed to determine id of %s - %s' % (added_by, err))
                    fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                    # @modified 20240119 - Task #5228: panorama - optimise insertions
                    # return False
                    continue

            # @added 20240122 - Task #5228: panorama - optimise insertions
            # Get apps, algorithms, sources and hosts once
            app_id = None
            try:
                app_id = apps[app]
            except Exception as err:
                logger.error('error :: spin_process :: failed to determine app_id from apps dict, err: %s' % (err))
            
            # @modified 20240122 - Task #5228: panorama - optimise insertions
            # Only query DB if not determined from apps dict
            if not app_id:
                try:
                    # @modified 20230110 - Task #4022: Move mysql_select calls to SQLAlchemy
                    #                     Task #4778: v4.0.0 - update dependencies
                    # Use sqlalchemy rather than string-based query construction
                    # Use self.determine_db_id instead, declare the function once
                    # app_id = determine_id('apps', 'app', app)
                    # @modified 20240119 - Task #5228: panorama - optimise insertions
                    # Added engine and use_table
                    # app_id = self.determine_db_id('apps', 'app', app)
                    app_id = self.determine_db_id('apps', 'app', app, engine=engine, use_table=apps_table)
                except Exception as err:
                    # logger.error('error :: failed to determine id of %s' % (app))
                    logger.error('error :: self.determine_db_id failed to determine id of %s - %s' % (app, err))
                    fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                    # @modified 20240119 - Task #5228: panorama - optimise insertions
                    # return False
                    continue

            # @added 20240122 - Task #5228: panorama - optimise insertions
            # Get apps, algorithms, sources and hosts once
            source_id = None
            try:
                source_id = sources[source]
            except Exception as err:
                logger.error('error :: spin_process :: failed to determine app_id from sources dict, err: %s' % (err))
            
            # @modified 20240122 - Task #5228: panorama - optimise insertions
            # Only query DB if not determined from apps dict
            if not source_id:
                try:
                    # @modified 20230110 - Task #4022: Move mysql_select calls to SQLAlchemy
                    #                     Task #4778: v4.0.0 - update dependencies
                    # Use sqlalchemy rather than string-based query construction
                    # Use self.determine_db_id instead, declare the function once
                    # source_id = determine_id('sources', 'source', source)
                    # @modified 20240119 - Task #5228: panorama - optimise insertions
                    # Added engine and use_table
                    # source_id = self.determine_db_id('sources', 'source', source)
                    source_id = self.determine_db_id('sources', 'source', source, engine=engine, use_table=sources_table)
                except Exception as err:
                    # logger.error('error :: failed to determine id of %s' % (source))
                    logger.error('error :: self.determine_db_id failed to determine id of %s- %s' % (source, err))
                    fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                    # @modified 20240119 - Task #5228: panorama - optimise insertions
                    # return False
                    continue

            # @added 20220722 - Task #2732: Prometheus to Skyline
            #                   Branch #4300: prometheus
            labelled_metrics_name = None
            if metric.startswith('labelled_metrics.'):
                labelled_metrics_name = str(metric)
                logger.info('looking up metric for %s' % metric)
                try:
                    metric = get_base_name_from_labelled_metrics_name('panorama', metric)
                except Exception as err:
                    logger.error('error :: get_base_name_from_labelled_metrics_name failed for %s - %s' % (metric, err))
                    fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                    # @modified 20240119 - Task #5228: panorama - optimise insertions
                    # return
                    continue

            # @added 20240122 - Task #5228: panorama - optimise insertions
            # Try and determine the metric id from Redis
            metric_id = 0
            try:
                metric_id_str = self.redis_conn_decoded.hget('aet.metrics_manager.metric_names_with_ids', str(metric))
                if metric_id_str:
                    metric_id = int(metric_id_str)
            except Exception as err:
                logger.error('error :: hget on aet.metrics_manager.metric_names_with_ids failed for %s, err: %s' % (metric, err))

            # @added 20240122 - Task #5228: panorama - optimise insertions
            # Only try get the metric id from the DB if it was not retrieved
            # from Redis
            if not metric_id:
                try:
                    # @modified 20230110 - Task #4022: Move mysql_select calls to SQLAlchemy
                    #                     Task #4778: v4.0.0 - update dependencies
                    # Use sqlalchemy rather than string-based query construction
                    # Use self.determine_db_id instead, declare the function once
                    # metric_id = determine_id('metrics', 'metric', metric)
                    # @modified 20240119 - Task #5228: panorama - optimise insertions
                    # Added engine and use_table
                    # metric_id = self.determine_db_id('metrics', 'metric', metric)
                    metric_id = self.determine_db_id('metrics', 'metric', metric, engine=engine, use_table=metrics_table, log=False)
                except Exception as err:
                    # logger.error('error :: failed to determine id of %s' % (metric))
                    logger.error('error :: self.determine_db_id failed to determine id of %s - %s' % (metric, err))
                    fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                    # @modified 20240119 - Task #5228: panorama - optimise insertions
                    # return False
                    continue

            # @added 20221103 - Task #2732: Prometheus to Skyline
            #                   Branch #4300: prometheus
            if '_tenant_id="' in metric:
                labelled_metrics_name = 'labelled_metrics.%s' % str(metric_id)

            algorithms_ids_csv = ''
            for algorithm in algorithms:
                # @added 20220822 - Task #2732: Prometheus to Skyline
                #                   Branch #4300: prometheus
                # Strip override string
                if ' (override - ' in algorithm:
                    algorithm = algorithm.split(' ', 1)[0]

                # @added 20240122 - Task #5228: panorama - optimise insertions
                # Get apps, algorithms, sources and hosts once
                algorithm_id = None
                try:
                    algorithm_id = all_algorithms_by_name[algorithm]
                except Exception as err:
                    logger.error('error :: spin_process :: failed to determine algorithm_id from algorithms dict, err: %s' % (err))
                
                # @modified 20240122 - Task #5228: panorama - optimise insertions
                # Only query DB if not determined from apps dict
                if not algorithm_id:
                    try:
                        # @modified 20230110 - Task #4022: Move mysql_select calls to SQLAlchemy
                        #                     Task #4778: v4.0.0 - update dependencies
                        # Use sqlalchemy rather than string-based query construction
                        # Use self.determine_db_id instead, declare the function once
                        # algorithm_id = determine_id('algorithms', 'algorithm', algorithm)
                        # @modified 20240119 - Task #5228: panorama - optimise insertions
                        # Added engine and use_table
                        # algorithm_id = self.determine_db_id('algorithms', 'algorithm', algorithm)
                        algorithm_id = self.determine_db_id('algorithms', 'algorithm', algorithm, engine=engine, use_table=algorithms_table)
                    except Exception as err:
                        # logger.error('error :: failed to determine id of %s' % (algorithm))
                        logger.error('error :: self.determine_db_id failed to determine id of %s - %s' % (algorithm, err))
                        fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                        # @modified 20240119 - Task #5228: panorama - optimise insertions
                        # return False
                        continue

                if algorithms_ids_csv == '':
                    algorithms_ids_csv = str(algorithm_id)
                else:
                    new_algorithms_ids_csv = '%s,%s' % (algorithms_ids_csv, str(algorithm_id))
                    algorithms_ids_csv = new_algorithms_ids_csv

            triggered_algorithms_ids_csv = ''
            for triggered_algorithm in triggered_algorithms:
                # @added 20220822 - Task #2732: Prometheus to Skyline
                #                   Branch #4300: prometheus
                # Strip override string
                if ' (override - ' in str(triggered_algorithm):
                    triggered_algorithm = triggered_algorithm.split(' ', 1)[0]

                # @added 20240122 - Task #5228: panorama - optimise insertions
                # Get apps, algorithms, sources and hosts once
                triggered_algorithm_id = None
                if not isinstance(triggered_algorithm, int):
                    try:
                        triggered_algorithm_id = all_algorithms_by_name[triggered_algorithm]
                    except Exception as err:
                        logger.error('error :: spin_process :: failed to determine triggered_algorithm_id from algorithms dict, err: %s' % (err))
                
                # @modified 20240122 - Task #5228: panorama - optimise insertions
                # Only query DB if not determined from apps dict
                if not triggered_algorithm_id:
                    try:
                        # @modified 20230110 - Task #4022: Move mysql_select calls to SQLAlchemy
                        #                     Task #4778: v4.0.0 - update dependencies
                        # Use sqlalchemy rather than string-based query construction
                        # Use self.determine_db_id instead, declare the function once
                        # triggered_algorithm_id = determine_id('algorithms', 'algorithm', triggered_algorithm)
                        # @modified 20240119 - Task #5228: panorama - optimise insertions
                        # Added engine and use_table
                        # triggered_algorithm_id = self.determine_db_id('algorithms', 'algorithm', triggered_algorithm)
                        triggered_algorithm_id = self.determine_db_id('algorithms', 'algorithm', triggered_algorithm, engine=engine, use_table=algorithms_table)
                    except Exception as err:
                        # logger.error('error :: failed to determine id of %s' % (triggered_algorithm))
                        logger.error('error :: self.determine_db_id failed to determine id of %s - %s' % (triggered_algorithm, err))
                        fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                        # @modified 20240119 - Task #5228: panorama - optimise insertions
                        # return False
                        continue

                if triggered_algorithms_ids_csv == '':
                    triggered_algorithms_ids_csv = str(triggered_algorithm_id)
                else:
                    new_triggered_algorithms_ids_csv = '%s,%s' % (
                        triggered_algorithms_ids_csv, str(triggered_algorithm_id))
                    triggered_algorithms_ids_csv = new_triggered_algorithms_ids_csv

            logger.info('inserting anomaly for %s' % str(metric))
            try:
                full_duration = int(metric_timestamp) - int(from_timestamp)
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: full_duration - %s' % str(full_duration))
            except:
                logger.error('error :: failed to determine full_duration')
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return False
                continue

            try:
                anomalous_datapoint = round(float(value), 6)
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: anomalous_datapoint - %s' % str(anomalous_datapoint))
            except:
                logger.error('error :: failed to determine anomalous_datapoint')
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return False
                continue

            try:
                # @modified 20200420 - Feature #3500: webapp - crucible_process_metrics
                #                      Feature #1448: Crucible web UI
                #                      Branch #868: crucible
                # Added label and user_id
                columns = '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s' % (
                    'metric_id', 'host_id', 'app_id', 'source_id',
                    'anomaly_timestamp', 'anomalous_datapoint', 'full_duration',
                    'algorithms_run', 'triggered_algorithms', 'label', 'user_id')
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: columns - %s' % str(columns))
            except:
                logger.error('error :: failed to construct columns string')
                logger.info(traceback.format_exc())
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return False
                continue

            # @added 20230110 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                   Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            # @modified 20240122 - Task #5228: panorama - optimise insertions
            # Only get_engine once
            try:
                if not engine:
                    try:
                        engine, fail_msg, trace = get_engine(skyline_app)
                    except Exception as err:
                        logger.error('error :: could not get a MySQL engine for anomalies_table - %s' % err)
            except Exception as err:
                logger.info(traceback.format_exc())
                logger.error('error :: could not determine if engine is defined, err: %s' % err)

            # @modified 20240122 - Task #5228: panorama - optimise insertions
            # Only get anomalies_table once
            try:
                if 'sqlalchemy.sql.schema.Table' not in str(type(anomalies_table)):
                    try:
                        anomalies_table, log_msg, trace = anomalies_table_meta(skyline_app, engine)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: spin_process :: failed to get anomalies_table meta, err: %s' % err)
            except Exception as err:
                logger.info(traceback.format_exc())
                logger.error('error :: could not determine if anomalies_table is defined, err: %s' % err)

            try:
                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                # @modified 20200420 - Feature #3500: webapp - crucible_process_metrics
                #                      Feature #1448: Crucible web UI
                #                      Branch #868: crucible
                # Added label and user_id
                if not user_id:
                    # User the Skyline user id
                    user_id = 1
                query_string = '(%s) VALUES (%d, %d, %d, %d, %s, %.6f, %d, \'%s\', \'%s\', \'%s\', %d)' % (
                    columns, metric_id, added_by_host_id, app_id, source_id,
                    metric_timestamp, anomalous_datapoint, full_duration,
                    algorithms_ids_csv, triggered_algorithms_ids_csv, str(label),
                    int(user_id))
                query = 'insert into anomalies %s' % query_string  # nosec
            except Exception as err:
                logger.info(traceback.format_exc())
                logger.error('error :: failed to construct insert query, err: %s' % err)
                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return False
                continue

            # @added 20240602 - Feature #5368: analyzer_batch - reprocess
            # Deduplicate anomalies allowing analyzer_batch to reprocess metrics
            if batch_metric:
                anomalies_dict = {}
                try:
                    anomalies_dict = get_anomalies_for_period(skyline_app, [metric_id], metric_timestamp, metric_timestamp)
                except Exception as err:
                    logger.error('error :: get_anomalies_for_period failed for metric_id %s - %s' % (
                        str(metric_id), err))
                logger.info('got %s anomalies for batch_metric %s at %s' % (str(len(anomalies_dict)), metric, str(metric_timestamp)))
                if len(anomalies_dict) > 0:
                    logger.info('not recording duplicate anomaly for %s' % metric)
                    try:
                        engine.dispose()
                    except Exception as err:
                        logger.error('error :: engine.dispose() failed - %s' % err)
                    if os.path.isfile(str(metric_check_file)):
                        try:
                            os.remove(str(metric_check_file))
                            logger.info('metric_check_file removed - %s' % str(metric_check_file))
                        except OSError:
                            pass
                    continue

            if settings.ENABLE_PANORAMA_DEBUG:
                logger.info('debug :: anomaly insert - %s' % str(query))

            logger.info('executing inserting statement for anomaly on %s' % str(metric))
            anomaly_id = None
            try:
                # @added 20230110 - Task #4022: Move mysql_select calls to SQLAlchemy
                #                   Task #4778: v4.0.0 - update dependencies
                # Use sqlalchemy rather than string-based query construction
                # anomaly_id = self.mysql_insert(query)
                insert_stmt = anomalies_table.insert().values(
                    metric_id=int(metric_id),
                    host_id=int(added_by_host_id),
                    app_id=int(app_id),
                    source_id=int(source_id),
                    anomaly_timestamp=int(metric_timestamp),
                    anomalous_datapoint=float(anomalous_datapoint),
                    full_duration=int(full_duration),
                    algorithms_run=str(algorithms_ids_csv),
                    triggered_algorithms=str(triggered_algorithms_ids_csv),
                    label=str(label),
                    user_id=int(user_id))
                connection = engine.connect()
                result = connection.execute(insert_stmt)
                connection.close()
                anomaly_id = result.inserted_primary_key[0]
                logger.info('anomaly id - %d - created for %s at %s' % (
                    anomaly_id, metric, str(metric_timestamp)))

            except Exception as err:
                logger.error('error :: failed to insert anomaly for %s at %s - %s' % (
                    metric, str(metric_timestamp), err))
                # @added 20230110 - Task #4022: Move mysql_select calls to SQLAlchemy
                #                   Task #4778: v4.0.0 - update dependencies
                # Use sqlalchemy rather than string-based query construction
                try:
                    engine.dispose()
                except Exception as err:
                    logger.error('error :: engine.dispose() failed - %s' % err)

                fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                # @modified 20240119 - Task #5228: panorama - optimise insertions
                # return False
                continue

            # @added 20230110 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                   Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            # @modified 20240119 - Task #5228: panorama - optimise insertions
            # Dispose of engine once at the end
            # try:
            #     engine.dispose()
            # except Exception as err:
            #     logger.error('error :: engine.dispose() failed - %s' % err)

            # @added 20211001 - Feature #4268: Redis hash key - panorama.metrics.latest_anomaly
            #                   Feature #4264: luminosity - cross_correlation_relationships
            if anomaly_id:
                latest_anomaly = {}
                try:
                    latest_anomaly = update_metric_latest_anomaly(skyline_app, metric, metric_id)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: update_metric_latest_anomaly failed - %s' % str(err))

            # @added 20200929 - Task #3748: POC SNAB
            #                   Branch #3068: SNAB
            # If snab is enabled add a Redis key with the anomaly_id
            if anomaly_id:
                anomaly_id_redis_key = 'panorama.anomaly_id.%s.%s' % (str(int(metric_timestamp)), metric)
                # @added 20220915 - Task #2732: Prometheus to Skyline
                #                   Branch #4300: prometheus
                if labelled_metrics_name:
                    anomaly_id_redis_key = 'panorama.anomaly_id.%s.%s' % (str(int(metric_timestamp)), labelled_metrics_name)
                try:
                    self.redis_conn.setex(anomaly_id_redis_key, 86400, int(anomaly_id))
                    logger.info('set Redis anomaly_key - %s' % (anomaly_id_redis_key))
                except Exception as e:
                    logger.error(
                        'error :: could not set anomaly_id_redis_key - %s - %s' % (
                            anomaly_id_redis_key, e))

            # @added 20241119 - Feature #5064: mirage.inflection
            mirage_inflection = False
            if anomaly_id and app in ['mirage', 'ionosphere']:
                if 'mmzrmp' not in triggered_algorithms:
                    mirage_inflection = True
            if mirage_inflection:
                key_data = {
                    'anomaly_id': anomaly_id, 'app': app,
                    'base_name': metric, 'metric': metric,
                    'full_duration': full_duration,
                    'timestamp': int(metric_timestamp)
                }
                if labelled_metrics_name:
                    key_data['metric'] = labelled_metrics_name
                key_name = '%s.%s' % (str(metric_timestamp), str(anomaly_id))
                logger.info('adding %s to mirage.inflection' % key_name)
                try:
                    self.redis_conn_decoded.hset('mirage.inflection', key_name, str(key_data))
                    logger.info('added %s key to mirage.inflection' % key_name)
                except Exception as err:
                    logger.error(
                        'error :: hset failed on mirage.inflection for key %s, err: %s' % (
                            key_name, err))

            # Set anomaly record cache key
            # @modified 20200420 - Feature #3500: webapp - crucible_process_metrics
            #                      Feature #1448: Crucible web UI
            #                      Branch #868: crucible
            # Only if it was not added_by webapp or crucible
            if set_anomaly_key:
                try:
                    # @modified 20200413 - Feature #3486: analyzer_batch
                    #                   Feature #3480: batch_processing
                    # Set key to timestamp if a batch metric.  I have looked and cannot
                    # find where the panorama.last_check is used anyway else other than
                    # above in panorama its self and further it does not appear that the
                    # packb(value) is used at all, just the existence of the key its
                    # self.
                    if batch_metric:
                        self.redis_conn.setex(
                            cache_key, settings.PANORAMA_EXPIRY_TIME,
                            int(metric_timestamp))
                    else:
                        self.redis_conn.setex(
                            # @modified 20200603 - Feature #3486: analyzer_batch
                            #                      Feature #3480: batch_processing
                            # As per above do not use msgpack with the value, set the
                            # value to the timestamp
                            # cache_key, settings.PANORAMA_EXPIRY_TIME, packb(value))
                            cache_key, settings.PANORAMA_EXPIRY_TIME, int(metric_timestamp))
                    logger.info('set cache_key - %s.last_check.%s.%s - %s' % (
                        skyline_app, app, metric, str(settings.PANORAMA_EXPIRY_TIME)))
                except Exception as e:
                    logger.error(
                        'error :: could not set cache_key - %s.last_check.%s.%s - %s' % (
                            skyline_app, app, metric, e))

            # @added 20191031 - Feature #3306: Record anomaly_end_timestamp
            # Add to current anomalies set
            # @modified 20200420 - Feature #3500: webapp - crucible_process_metrics
            #                      Feature #1448: Crucible web UI
            #                      Branch #868: crucible
            # Only if it was not added_by webapp or crucible
            if add_to_current_anomalies:
                try:
                    redis_set = 'current.anomalies'
                    data = [metric, metric_timestamp, anomaly_id, None]
                    self.redis_conn.sadd(redis_set, str(data))
                    logger.info('added %s to Redis set %s' % (str(data), redis_set))
                except Exception as e:
                    logger.error(
                        'error :: could not add %s to Redis set %s - %s' % (
                            str(data), redis_set, e))

            if os.path.isfile(str(metric_check_file)):
                try:
                    os.remove(str(metric_check_file))
                    logger.info('metric_check_file removed - %s' % str(metric_check_file))
                except OSError:
                    pass

            # @modified 20240119 - Task #5228: panorama - optimise insertions
            # return anomaly_id
            if anomaly_id:
                inserted_anomaly_ids.append(anomaly_id)
                processed_checks[metric_check_file] = anomaly_id

                # @added 20240229 - Feature #5294: panorama - retry failed checks
                # If this is a retried failed check, remove it from the
                # panorama.retry_failed_checks Redis hash
                fail_count = 0
                failed_check_data = None
                try:
                    failed_check_data_str = self.redis_conn_decoded.hget('panorama.retry_failed_checks', check_file_name)
                    if failed_check_data_str:
                        try:
                            failed_check_data = literal_eval(failed_check_data_str)
                            try:
                                fail_count = failed_check_data['fail_count']
                            except:
                                fail_count = 0
                        except Exception as err:
                            logger.error('error :: failed to literal_eval values from %s key in Redis hash panorama.retry_failed_checks, err: %s' % (
                                check_file_name, err))
                except Exception as err:
                    logger.error('error :: failed to literal_eval values from %s key in Redis hash panorama.retry_failed_checks, err: %s' % (
                        check_file_name, err))
                if failed_check_data:
                    try:
                        failed_check_data['succeeded'] = True
                        self.redis_conn_decoded.hset('panorama.retry_failed_checks', check_file_name, str(failed_check_data))
                        logger.info('%s retried and set to succeeded in panorama.retry_failed_checks after %s previous fails' % (
                            str(metric_check_file), str(fail_count)))
                    except Exception as err:
                        logger.error('error :: failed to hset %s key in Redis hash panorama.retry_failed_checks with succeeded, err: %s' % (
                            check_file_name, err))

        # @added 20240119 - Task #5228: panorama - optimise insertions
        if engine:
            try:
                engine.dispose()
            except Exception as err:
                logger.error('error :: engine.dispose() failed - %s' % err)
        run_seconds = time() - start
        logger.info('spin_process :: processed %s checks of the %s assigned checks and inserted %s anomalies in %s seconds' % (
            str(len(processed_checks)), str(len(assigned_metric_check_files)),
            str(len(inserted_anomaly_ids)), str(run_seconds)))
        return inserted_anomaly_ids


    def run(self):
        """
        Called when the process intializes.

        Determine if what is known in the Skyline DB
        blah

        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        if os.path.isfile(skyline_app_logwait):
            try:
                logger.info('removing %s' % skyline_app_logwait)
                os.remove(skyline_app_logwait)
            except OSError:
                logger.error('error :: failed to remove %s, continuing' % skyline_app_logwait)

        now = time()
        log_wait_for = now + 5
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time()
            else:
                now = log_wait_for + 1

        logger.info('starting %s run' % skyline_app)
        if os.path.isfile(skyline_app_loglock):
            logger.error('error :: bin/%s.d log management seems to have failed, continuing' % skyline_app)
            try:
                os.remove(skyline_app_loglock)
                logger.info('log lock file removed')
            except OSError:
                logger.error('error :: failed to remove %s, continuing' % skyline_app_loglock)
        else:
            logger.info('bin/%s.d log management done' % skyline_app)

        # See if I am known in the DB, if so, what are my variables
        # self.populate mysql
        # What is my host id in the Skyline panorama DB?
        #   - if not known - INSERT hostname INTO hosts
        # What are the known apps?
        #   - if returned make a dictionary
        # What are the known algorithms?
        #   - if returned make a dictionary

        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
                if ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: connected to Redis')
            except:
                logger.error('error :: cannot connect to redis at socket path %s' % (
                    settings.REDIS_SOCKET_PATH))
                sleep(30)
                # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                # @modified 20191031 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # if settings.REDIS_PASSWORD:
                #     self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                # else:
                #     self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                self.redis_conn = get_redis_conn(skyline_app)
                continue

            # Report app up
            try:
                self.redis_conn.setex(skyline_app, 120, now)
                logger.info('updated Redis key for %s up' % skyline_app)
            except:
                logger.error('error :: failed to update Redis key for %s up' % skyline_app)

            # @modified 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                      Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            # Disabled all ENABLE_PANORAMA_DEBUG mysql tests
            # if ENABLE_PANORAMA_DEBUG:
            #     # Make sure mysql is available
            #     mysql_down = True
            #     while mysql_down:
            #         query = 'SHOW TABLES'
            #         results = self.mysql_select(query)
            #         if results:
            #             mysql_down = False
            #             logger.info('debug :: tested database query - OK')
            #         else:
            #             logger.error('error :: failed to query database')
            #             sleep(30)
            #     try:
            #         query = 'SELECT id, test FROM test'
            #         result = self.mysql_select(query)
            #         logger.info('debug :: tested mysql SELECT query - OK')
            #         logger.info('debug :: result: %s' % str(result))
            #         logger.info('debug :: result[0]: %s' % str(result[0]))
            #         logger.info('debug :: result[1]: %s' % str(result[1]))
# Works
# 2016-06-10 19:07:23 :: 4707 :: result: [(1, u'test1')]
            #     except:
            #         logger.error(
            #             'error :: mysql error - %s' %
            #             traceback.print_exc())
            #         logger.error('error :: failed to SELECT')

            # self.populate the database metatdata tables
            # What is my host id in the Skyline panorama DB?
            host_id = False
            # @modified 20170913 - Task #2160: Test skyline with bandit
            # Added nosec to exclude from bandit tests
            # @modified 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                      Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            # If this case use the determine_db_id function which now uses
            # sqlalchemy
            # query = 'select id FROM hosts WHERE host=\'%s\'' % this_host  # nosec
            # results = self.mysql_select(query)
            # if results:
            #     host_id = results[0][0]
            #     logger.info('host_id: %s' % str(host_id))
            # else:
            #     logger.info('failed to determine host id of %s' % this_host)
            # #   - if not known - INSERT hostname INTO host
            # if not host_id:
            #     logger.info('inserting %s into hosts table' % this_host)
            #     # @modified 20170913 - Task #2160: Test skyline with bandit
            #     # Added nosec to exclude from bandit tests
            #     query = 'insert into hosts (host) VALUES (\'%s\')' % this_host  # nosec
            #     host_id = self.mysql_insert(query)
            #     if host_id:
            #         logger.info('new host_id: %s' % str(host_id))

            # @added 20230109 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                   Task #4778: v4.0.0 - update dependencies
            # Use sqlalchemy rather than string-based query construction
            try:
                host_id = self.determine_db_id('hosts', 'host', this_host)
                logger.info('host_id: %s' % str(host_id))
            except Exception as err:
                logger.error('error :: determine_db_id failed - %s' % err)

            if not host_id:
                logger.error(
                    'error :: failed to determine populate %s into the hosts table' %
                    this_host)
                sleep(30)
                continue

            # Like loop through the panorama dir and see if anyone has left you
            # any work, etc
            # Make sure check_dir exists and has not been removed
            try:
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: checking check dir exists - %s' % settings.PANORAMA_CHECK_PATH)
                os.path.exists(settings.PANORAMA_CHECK_PATH)
            except:
                logger.error('error :: check dir did not exist - %s' % settings.PANORAMA_CHECK_PATH)
                mkdir_p(settings.PANORAMA_CHECK_PATH)

                logger.info('check dir created - %s' % settings.PANORAMA_CHECK_PATH)
                os.path.exists(settings.PANORAMA_CHECK_PATH)
                # continue

            # Determine if any metric has been added to add
            while True:
                metric_var_files = False
                try:
                    metric_var_files = [f for f in listdir(settings.PANORAMA_CHECK_PATH) if isfile(join(settings.PANORAMA_CHECK_PATH, f))]
                except:
                    logger.error('error :: failed to list files in check dir')
                    logger.info(traceback.format_exc())

                if not metric_var_files:

                    # @added 20200204 - Feature #3442: Panorama - add metric to metrics table immediately
                    if PANORAMA_INSERT_METRICS_IMMEDIATELY:
                        # Only check once a minute
                        # @modified 20240119 - Task #5228: panorama - optimise insertions
                        # Changed to check every 20 seconds
                        redis_insert_new_metrics_key = 'panorama.insert_new_metrics'
                        redis_insert_new_metrics_key_exists = False
                        try:
                            redis_insert_new_metrics_key_exists = self.redis_conn.get(redis_insert_new_metrics_key)
                        except Exception as e:
                            logger.error('error :: could not query Redis for key %s: %s' % (redis_insert_new_metrics_key, e))
                        check_metrics_to_insert = False
                        if not redis_insert_new_metrics_key_exists:
                            check_metrics_to_insert = True
                            logger.info('checking for new metrics to insert into the metrics table')
                        if check_metrics_to_insert:
                            logger.info('checking for new unique_metrics to insert into the metrics table')
                            redis_set = '%sunique_metrics' % settings.FULL_NAMESPACE
                            try:
                                unique_metrics = list(self.redis_conn_decoded.smembers(redis_set))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to get data from %s Redis set' % redis_set)
                                unique_metrics = []
                            db_fullnamespace_unique_metrics = []
                            results = False

                            # @added 20220630 - Task #2732: Prometheus to Skyline
                            #                   Branch #4300: prometheus
                            db_metric_names = []

                            if unique_metrics:

                                # @modified 20230110 - Task #4022: Move mysql_select calls to SQLAlchemy
                                #                      Task #4778: v4.0.0 - update dependencies
                                # Use sqlalchemy rather than string-based query construction
                                # But in this case use the get_all_db_metric_names
                                # function
                                # query = 'SELECT metric FROM metrics'
                                # results = self.mysql_select(query)
                                try:
                                    with_ids = False
                                    results = get_all_db_metric_names(skyline_app, with_ids)
                                except Exception as err:
                                    logger.error('error :: get_all_db_metric_names failed - %s' % err)

                                if results:
                                    for result in results:
                                        # @modified 20230110 - Task #4022: Move mysql_select calls to SQLAlchemy
                                        #                      Task #4778: v4.0.0 - update dependencies
                                        # Use sqlalchemy rather than string-based query construction
                                        # db_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(result[0]))
                                        db_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(result))
                                        db_fullnamespace_unique_metrics.append(db_metric_name)
                                        # @added 20220630 - Task #2732: Prometheus to Skyline
                                        #                   Branch #4300: prometheus
                                        # @modified 20230110 - Task #4022: Move mysql_select calls to SQLAlchemy
                                        #                      Task #4778: v4.0.0 - update dependencies
                                        # Use sqlalchemy rather than string-based query construction
                                        # db_metric_names.append(str(result[0]))
                                        db_metric_names.append(str(result))

                            # @added 20240119 - Task #5228: panorama - optimise insertions
                            # Optimise the insertions of new metric by inserting
                            # multiple metrics at once rather than one at a time
                            # and change to a set difference instead of a for
                            # loop
                            new_base_names_to_insert = []
                            new_base_names_used_set = False
                            new_base_names_to_insert_set = None
                            try:
                                unique_metric_set = set(unique_metrics)
                                db_fullnamespace_unique_metrics_set = set(db_fullnamespace_unique_metrics)
                                new_base_names_to_insert_set = unique_metric_set.difference(db_fullnamespace_unique_metrics_set)
                                new_base_names_used_set = True
                            except Exception as err:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to build new_base_names_to_insert_set, err: %s' % err)
                            if new_base_names_to_insert_set:
                                for unique_metric in new_base_names_to_insert_set:
                                    if unique_metric.startswith(settings.FULL_NAMESPACE):
                                        base_name = unique_metric.replace(settings.FULL_NAMESPACE, '', 1)
                                    else:
                                        base_name = unique_metric

                                        # @added 20241120 - Bug #5522: Handle duplicate metric names
                                        # It is possible that when a lot of new metrics are recieved such as
                                        # when a new Skyline or existing Skyline instance has lots of new
                                        # metrics submitted to it that metrics can be identified as new
                                        # metrics multiple times due to them not being in the Redis sets yet
                                        # which are checked to determine if they are new metrics because
                                        # horizon skips until a metric id is assigned.  This should also
                                        # cover labelled_metrics.
                                        if base_name in db_metric_names:
                                            continue

                                    new_base_names_to_insert.append(base_name)

                            # @modified 20240119 - Task #5228: panorama - optimise insertions
                            # Optimise and use set difference above
                            # if unique_metrics and db_fullnamespace_unique_metrics:
                            if not new_base_names_used_set and unique_metrics and db_fullnamespace_unique_metrics:
                                for unique_metric in unique_metrics:
                                    if unique_metric not in db_fullnamespace_unique_metrics:
                                        try:
                                            # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
                                            # base_name = unique_metric.replace(settings.FULL_NAMESPACE, '', 1)
                                            if unique_metric.startswith(settings.FULL_NAMESPACE):
                                                base_name = unique_metric.replace(settings.FULL_NAMESPACE, '', 1)
                                            else:
                                                base_name = unique_metric

                                            # @added 20241120 - Bug #5522: Handle duplicate metric names
                                            # It is possible that when a lot of new metrics are recieved such as
                                            # when a new Skyline or existing Skyline instance has lots of new
                                            # metrics submitted to it that metrics can be identified as new
                                            # metrics multiple times due to them not being in the Redis sets yet
                                            # which are checked to determine if they are new metrics because
                                            # horizon skips until a metric id is assigned.  This should also
                                            # cover labelled_metrics.
                                            if base_name in db_metric_names:
                                                continue

                                            # @added 20240119 - Task #5228: panorama - optimise insertions
                                            # Optimise the insertions of new metric by inserting
                                            # multiple metrics at once rather than one at a time
                                            # metric_id = self.insert_new_metric(base_name)
                                            # logger.info('inserted %s into metrics table from db_fullnamespace_unique_metrics, assigned id %s' % (base_name, str(metric_id)))
                                            new_base_names_to_insert.append(base_name)

                                        except:
                                            logger.error(traceback.format_exc())
                                            logger.error('error :: failed to insert %s into metrics table' % unique_metric)

                            # @added 20240119 - Task #5228: panorama - optimise insertions
                            # Optimise the insertions of new metric by inserting
                            # multiple metrics at once rather than one at a time
                            if new_base_names_to_insert:
                                inserted_metric_ids = []
                                try:
                                    inserted_metric_ids = self.insert_new_metric(base_name, metrics_to_insert=new_base_names_to_insert)
                                except Exception as err:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to insert %s into metrics table, err: %s' % (
                                        str(len(new_base_names_to_insert)), err))
                                logger.info('inserted %s (of %s) new base_names into metrics table from db_fullnamespace_unique_metrics' % (
                                    str(len(inserted_metric_ids)), str(len(new_base_names_to_insert))))
                                try:
                                    del new_base_names_to_insert
                                except:
                                    pass

                            # @added 20220630 - Task #2732: Prometheus to Skyline
                            #                   Branch #4300: prometheus
                            logger.info('checking for new labelled_metrics to insert into the metrics table against %s known metrics' % str(len(db_metric_names)))
                            unique_labelled_metrics = []
                            unique_labelled_metric_ids = []
                            try:
                                unique_labelled_metric_ids = list(self.redis_conn_decoded.smembers('labelled_metrics.unique_labelled_metrics'))
                            except Exception as err:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to get data from labelled_metrics.unique_labelled_metrics Redis set - %s' % err)
                                unique_labelled_metric_ids = []
                            if unique_labelled_metric_ids:
                                ids_with_base_names = {}
                                try:
                                    ids_with_base_names = self.redis_conn_decoded.hgetall('aet.metrics_manager.ids_with_metric_names')
                                except Exception as err:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: get_all_db_metric_names failed - %s' % err)
                                for unique_labelled_metric_id in unique_labelled_metric_ids:
                                    try:
                                        metric_id_str = unique_labelled_metric_id.replace('labelled_metrics.', '', 1)
                                    except:
                                        continue
                                    try:
                                        base_name = ids_with_base_names[metric_id_str]
                                    except:
                                        continue
                                    unique_labelled_metrics.append(base_name)
                            del unique_labelled_metric_ids
                            horizon_metrics_without_ids = []
                            try:
                                horizon_metrics_without_ids = list(self.redis_conn_decoded.smembers('panorama.horizon.metrics_with_no_id'))
                                if horizon_metrics_without_ids:
                                    logger.info('got %s metrics without ids from panorama.horizon.metrics_with_no_id' % str(len(horizon_metrics_without_ids)))
                            except Exception as err:
                                logger.error('error :: smembers failed on Redis set panorama.horizon.metrics_with_no_id - %s' % (
                                    str(err)))
                            if horizon_metrics_without_ids:
                                unique_labelled_metrics = unique_labelled_metrics + horizon_metrics_without_ids
                                try:
                                    # self.redis_conn_decoded.delete('panorama.horizon.metrics_with_no_id')
                                    new_key_name = 'panorama.horizon.metrics_with_no_id.%s' % str(int(time()))

                                    # @modified 20231223 - Task #5188: Optimise redis renames
                                    #                      Task #5178: Build and test skyline v4.1.0
                                    # self.redis_conn_decoded.rename('panorama.horizon.metrics_with_no_id', new_key_name)
                                    exists = False
                                    try:
                                        exists = self.redis_conn_decoded.exists('panorama.horizon.metrics_with_no_id')
                                    except Exception as err:
                                        logger.error('error :: exists failed on panorama.horizon.metrics_with_no_id Redis key, err: %s' % err)
                                    if exists:
                                        self.redis_conn_decoded.rename('panorama.horizon.metrics_with_no_id', new_key_name)
                                        self.redis_conn_decoded.expire(new_key_name, 600)
                                except Exception as err:
                                    logger.error('error :: failed to delete Redis set panorama.horizon.metrics_with_no_id - %s' % (
                                        str(err)))
                            del horizon_metrics_without_ids

                            # @added 20240119 - Task #5228: panorama - optimise insertions
                            # Optimise the insertions of new metric by inserting
                            # multiple metrics at once rather than one at a time
                            new_labelled_metrics_to_insert = []

                            # @added 20220819 -
                            # Added inserted metrics to the relevant Redis resources
                            # so that metrics are processed
                            inserted_metric_ids = []

                            if unique_labelled_metrics and db_metric_names:
                                # Using a sets comparison rather than a for loop
                                # takes 0 seconds rather than 5 seconds for a
                                # loop over 42955 metrics
                                labelled_metrics = [unique_labelled_metric.replace('labelled_metrics.', '') for unique_labelled_metric in unique_labelled_metrics]
                                db_metric_names_set = set(db_metric_names)
                                labelled_metrics_set = set(labelled_metrics)
                                unknown_metrics = list(labelled_metrics_set.difference(db_metric_names_set))
                                for labelled_basename in unknown_metrics:
                                    try:

                                        # @modified 20240119 - Task #5228: panorama - optimise insertions
                                        # Optimise the insertions of new metric by inserting
                                        # multiple metrics at once rather than one at a time
                                        # metric_id = self.insert_new_metric(labelled_basename)
                                        # logger.info('inserted %s into metrics table from unique_labelled_metrics and db_metric_names, assigned id %s' % (labelled_basename, str(metric_id)))
                                        # # @added 20220819
                                        # inserted_metric_ids.append(metric_id)
                                        new_labelled_metrics_to_insert.append(labelled_basename)

                                    except Exception as err:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: failed to insert %s into metrics table - %s' % (labelled_basename, err))

                            # @added 20240119 - Task #5228: panorama - optimise insertions
                            # Optimise the insertions of new metric by inserting
                            # multiple metrics at once rather than one at a time
                            if new_labelled_metrics_to_insert:
                                try:
                                    inserted_metric_ids = self.insert_new_metric(base_name, metrics_to_insert=new_labelled_metrics_to_insert)
                                except Exception as err:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to insert %s new labelled_metrics into metrics table, err: %s' % (
                                        str(len(new_labelled_metrics_to_insert)), err))
                                logger.info('inserted %s (of %s) new labelled_metrics into metrics table from unique_labelled_metrics and db_metric_names' % (
                                    str(len(inserted_metric_ids)),
                                    str(len(new_labelled_metrics_to_insert))))
                                try:
                                    del new_labelled_metrics_to_insert
                                except:
                                    pass

                            # @added 20220819
                            # Added inserted metrics to the relevant Redis resources
                            # so that metrics are processed
                            if inserted_metric_ids:
                                now_ts = int(time())
                                data_dict = {}
                                inserted_labelled_metrics = []
                                for metric_id in inserted_metric_ids:
                                    data_dict[str(metric_id)] = now_ts
                                    labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
                                    inserted_labelled_metrics.append(labelled_metric_name)
                                logger.info('inserting %s entries into analyzer_labelled_metrics.last_timeseries_timestamp Redis hash' % str(len(data_dict)))
                                try:
                                    self.redis_conn_decoded.hset('analyzer_labelled_metrics.last_timeseries_timestamp', mapping=data_dict)
                                except Exception as err:
                                    logger.error('error :: failed to update analyzer_labelled_metrics.last_timeseries_timestamp Redis hash - %s' % (
                                        str(err)))
                                logger.info('inserting %s metrics into labelled_metrics.unique_labelled_metrics Redis set' % str(len(inserted_labelled_metrics)))
                                try:
                                    self.redis_conn_decoded.sadd('labelled_metrics.unique_labelled_metrics', *inserted_labelled_metrics)
                                except Exception as err:
                                    logger.error('error :: failed to sadd to labelled_metrics.unique_labelled_metrics Redis set - %s' % (
                                        str(err)))

                            try:
                                del unknown_metrics
                            except:
                                pass
                            try:
                                del unique_labelled_metrics
                            except:
                                pass
                            try:
                                del db_metric_names
                            except:
                                pass

                            try:
                                del unique_metrics
                            except:
                                pass
                            try:
                                del results
                            except:
                                pass
                            try:
                                # @modified 20240119 - Task #5228: panorama - optimise insertions
                                # Change to 20 seconds
                                # self.redis_conn.setex(redis_insert_new_metrics_key, 60, int(time()))
                                self.redis_conn.setex(redis_insert_new_metrics_key, 20, int(time()))
                            except:
                                logger.error('error :: failed to set key :: %s' % redis_insert_new_metrics_key)
                            # @added 20240119 - Task #5228: panorama - optimise insertions
                            if inserted_metric_ids:
                                try:
                                    del inserted_metric_ids
                                except:
                                    pass

                    # @modified 20200128 - Feature #3418: PANORAMA_CHECK_INTERVAL
                    # Allow Panaroma to check for anomalies more frequently.  At
                    # some point TODO replace Panorama check files with Redis
                    # keys or a set.
                    # logger.info('sleeping 20 no metric check files')
                    # sleep(20)
                    logger.info('sleeping for %s seconds - no metric check files' % str(PANORAMA_CHECK_INTERVAL))
                    sleep(PANORAMA_CHECK_INTERVAL)

                # Discover metric anomalies to insert
                metric_var_files = False
                try:
                    metric_var_files = [f for f in listdir(settings.PANORAMA_CHECK_PATH) if isfile(join(settings.PANORAMA_CHECK_PATH, f))]
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to list files in check dir, err:  %s' % err)

                # @added 20240229 - Feature #5294: panorama - retry failed checks
                # At times when there are potential DB issues or network
                # partitions from the DB panorama will fail checks.  This adds
                # the functionality for panorama to retry checks that have
                # recently failed.
                retry_checks_exist = False
                if not metric_var_files:
                    try:
                        retry_checks_exist = self.redis_conn_decoded.exists('panorama.retry_failed_checks')
                    except Exception as err:
                        logger.error('error :: failed to exists on panorama.retry_failed_checks, err: %s' % (
                            err))
                if retry_checks_exist:
                    retry_checks = []
                    try:
                        retry_checks = get_failed_checks_to_retry(self)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: get_failed_checks_to_retry failed, err: %s' % (
                            err))
                    if retry_checks:
                        logger.info('retrying %s failed checks' % str(len(retry_checks)))
                        metric_var_files = list(retry_checks)

                if metric_var_files:
                    break

                # @added 20191107 - Feature #3306: Record anomaly_end_timestamp
                #                   Branch #3262: py3
                # Set the anomaly_end_timestamp for any metrics no longer anomalous
                redis_set = 'current.anomalies'
                try:
                    current_anomalies = self.redis_conn_decoded.smembers(redis_set)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to get data from %s Redis set' % redis_set)
                    current_anomalies = []

                # @added 20240119 - Task #5228: panorama - optimise insertions
                # Process multiple update_anomaly_end_timestamp in a single
                # process rather than handling a anomaly_end_timestamp per
                # process
                anomalies_to_update = {}

                if current_anomalies:
                    for item in current_anomalies:
                        remove_item = False
                        try:
                            list_data = literal_eval(item)
                            anomalous_metric = str(list_data[0])
                            anomaly_timestamp = int(list_data[1])
                            try:
                                anomaly_id = int(list_data[2])
                            except:
                                anomaly_id = None
                            try:
                                anomaly_end_timestamp = int(list_data[3])
                            except:
                                anomaly_end_timestamp = None
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to evaluate data from item %s from Redis set %s' % (
                                str(item), redis_set))
                        if not anomaly_id:
                            if anomaly_timestamp > (int(time()) - settings.STALE_PERIOD):
                                # If no anomaly is has been created by now, it
                                # never will be created.
                                remove_item = True
                        if not anomaly_end_timestamp:
                            if not remove_item:
                                continue

                        # @added 20240119 - Task #5228: panorama - optimise insertions
                        if anomaly_id and anomaly_end_timestamp:
                            anomalies_to_update[anomaly_id] = anomaly_end_timestamp

                        # @modified 20240119 - Task #5228: panorama - optimise insertions
                        # Move out of (unindented) to outside the block
                        # if anomaly_id and anomaly_end_timestamp:
                        #     # Update SQL
                        #    # Spawn update_anomaly_end_timestamp process

                    # @modified 20240119 - Task #5228: panorama - optimise insertions
                    # Move out of (unindented) the above block and use anomalies_to_update
                    # if anomaly_id and anomaly_end_timestamp:
                    if anomalies_to_update:
                        # Update SQL
                        # Spawn update_anomaly_end_timestamp process
                        pids = []
                        spawned_pids = []
                        pid_count = 0
                        now = time()
                        for i in range(1, 2):
                            try:
                                # @modified 20240119 - Task #5228: panorama - optimise insertions
                                # Update multiple
                                # p = Process(target=self.update_anomaly_end_timestamp, args=(i, anomaly_id, anomaly_end_timestamp))
                                anomaly_id = None
                                anomaly_end_timestamp = None
                                p = Process(target=self.update_anomaly_end_timestamp, args=(i, anomaly_id, anomaly_end_timestamp, anomalies_to_update))

                                pids.append(p)
                                pid_count += 1
                                logger.info('starting update_anomaly_end_timestamp')
                                p.start()
                                spawned_pids.append(p.pid)
                            except:
                                logger.info(traceback.format_exc())
                                logger.error('error :: to start update_anomaly_end_timestamp')
                                continue
                        p_starts = time()
                        # If the Skyline MySQL database is on a remote host
                        # 2 seconds here is sometimes not sufficient so
                        # increased to 10
                        while time() - p_starts <= 10:
                            if any(p.is_alive() for p in pids):
                                # Just to avoid hogging the CPU
                                sleep(.1)
                            else:
                                # All the processes are done, break now.
                                time_to_run = time() - p_starts
                                logger.info(
                                    '%s :: update_anomaly_end_timestamp completed in %.2f seconds' % (
                                        skyline_app, time_to_run))
                                remove_item = True
                                break
                        else:
                            # We only enter this if we didn't 'break' above.
                            logger.info('%s :: timed out, killing all update_anomaly_end_timestamp processes' % (skyline_app))
                            for p in pids:
                                p.terminate()

                        # @added 20240119 - Task #5228: panorama - optimise insertions
                        anomaly_ids_updated = []
                        redis_key = 'panorama.update_anomaly_end_timestamp'
                        try:
                            anomaly_ids_updated = self.redis_conn_decoded.smembers(redis_key)
                            self.redis_conn_decoded.delete(redis_key)
                            if not anomaly_ids_updated:
                                anomaly_ids_updated = []
                        except Exception as err:
                            logger.error('error :: update_anomaly_end_timestamp :: failed to sadd to key %s, err: %s' % (
                                redis_key, err))
                        anomalies_updated = []
                        for aid in anomaly_ids_updated:
                            anomalies_updated.append(int(aid))
                        logger.info('update_anomaly_end_timestamp :: %s anomaly ids updated to remove from current.anomalies' % str(len(anomalies_updated)))
                        if anomalies_updated:
                            redis_set = 'current.anomalies'
                            for item in current_anomalies:
                                remove_item = False
                                try:
                                    list_data = literal_eval(item)
                                    # anomalous_metric = str(list_data[0])
                                    # anomaly_timestamp = int(list_data[1])
                                    try:
                                        anomaly_id = int(list_data[2])
                                    except:
                                        anomaly_id = None
                                    if anomaly_id in anomalies_updated:
                                        try:
                                            self.redis_conn.srem(redis_set, str(list_data))
                                        except:
                                            logger.error(traceback.format_exc())
                                            logger.error('error :: failed to remove %s from Redis set %s' % (str(list_data), redis_set))
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to evaluate data from item %s from Redis set %s' % (
                                        str(item), redis_set))

                # @added 20200928 - Task #3748: POC SNAB
                #                   Branch #3068: SNAB
                redis_set = 'snab.panorama'
                snab_panorama_items = []
                if SNAB_ENABLED:
                    try:
                        snab_panorama_items = self.redis_conn_decoded.smembers(redis_set)
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: failed to get data from %s Redis set' % redis_set)
                        snab_panorama_items = []
                if snab_panorama_items:
                    # Update SQL
                    # Spawn update_snab process
                    pids = []
                    spawned_pids = []
                    pid_count = 0
                    now = time()
                    for i in range(1, 2):
                        try:
                            p = Process(target=self.update_snab, args=(i, snab_panorama_items))
                            pids.append(p)
                            pid_count += 1
                            logger.info('starting update_snab')
                            p.start()
                            spawned_pids.append(p.pid)
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: to start update_snab')
                            continue
                    p_starts = time()
                    del snab_panorama_items
                    # If the Skyline MySQL database is on a remote host
                    # 2 seconds here is sometimes not sufficient so
                    # increased to 10
                    while time() - p_starts <= 10:
                        if any(p.is_alive() for p in pids):
                            # Just to avoid hogging the CPU
                            sleep(.1)
                        else:
                            # All the processes are done, break now.
                            time_to_run = time() - p_starts
                            logger.info(
                                '%s :: update_snab completed in %.2f seconds' % (
                                    skyline_app, time_to_run))
                            remove_item = True
                            break
                    else:
                        # We only enter this if we didn't 'break' above.
                        logger.info('%s :: timed out, killing all update_snab processes' % (skyline_app))
                        for p in pids:
                            p.terminate()

                # @added 20190501 - Branch #2646: slack
                # Check if any Redis keys exist with a slack_thread_ts to update
                # any anomaly records
                slack_thread_ts_updates = None

                # @added 20230925 - Task #5000: Replace alert key scans with sets
                # Instead of using expiring keys with have a compute cost with
                # using scan_iter(match='[PATTERN]') switch to using entries in a
                # hash key, the management of which has a must lower compute cost.
                hash_slack_thread_ts_updates_keys = []
                hash_snab_slack_thread_ts_updates_keys = []

                # @added 20190523 - Branch #3002: docker
                #                   Branch #2646: slack
                # Only check if slack is enabled
                if SLACK_ENABLED:
                    try:
                        # @modified 20200430 - Bug #3266: py3 Redis binary objects not strings
                        #                      Branch #3262: py3
                        # slack_thread_ts_updates = list(self.redis_conn.scan_iter(match='panorama.slack_thread_ts.*'))
                        # @modified 20230925 - Task #5000: Replace alert key scans with sets
                        # Switch to hash key instead of scan_iter
                        # slack_thread_ts_updates = list(self.redis_conn_decoded.scan_iter(match='panorama.slack_thread_ts.*'))
                        slack_thread_ts_updates = []
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to scan panorama.slack_thread_ts.* from Redis')
                        slack_thread_ts_updates = []
                    # @modified 20230925 - Task #5000: Replace alert key scans with sets
                    # Switch to hash key instead of scan_iter
                    # if not slack_thread_ts_updates:
                    #     logger.info('no panorama.slack_thread_ts Redis keys to process, OK')

                    # @added 20230925 - Task #5000: Replace alert key scans with sets
                    # Instead of using expiring keys with have a compute cost with
                    # using scan_iter(match='[PATTERN]') switch to using entries in a
                    # hash key, the management of which has a must lower compute cost.
                    hash_slack_thread_ts_updates_keys = []
                    slack_thread_ts_updates_hash_key = 'panorama.slack_threads_ts'
                    try:
                        hash_slack_thread_ts_updates_keys = self.redis_conn_decoded.hkeys(slack_thread_ts_updates_hash_key)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: hkeys failed %s Redis hash - %s' % (
                            slack_thread_ts_updates_hash_key, err))
                        hash_slack_thread_ts_updates_keys = []
                    if hash_slack_thread_ts_updates_keys:
                        logger.debug('debug :: would process %s panorama.slack_threads_ts from Redis hash' % (
                            str(len(hash_slack_thread_ts_updates_keys))))
                        # This new method requires the management of the hash
                        # entries as they do not have an expire like the keys
                        current_timestamp = int(time())
                        for slack_thread_ts_str in hash_slack_thread_ts_updates_keys:
                            slack_thread_timestamp_str = slack_thread_ts_str.split('.')[0]
                            slack_thread_timestamp = int(slack_thread_timestamp_str)
                            # previous keys expired after a day
                            if (slack_thread_timestamp + 86400) < current_timestamp:
                                try:
                                    self.redis_conn_decoded.hdel(slack_thread_ts_updates_hash_key, slack_thread_ts_str)
                                    logger.info('deleted expired key - %s from %s Redis hash' % (
                                        slack_thread_ts_str, slack_thread_ts_updates_hash_key))
                                except Exception as err:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to delete key from %s Redis hash - %s' % (
                                        slack_thread_ts_updates_hash_key, err))
                    else:
                        logger.info('no entries in %s Redis hash to process, OK' % slack_thread_ts_updates_hash_key)

                    # @added 20200929 - Task #3748: POC SNAB
                    #                   Branch #3068: SNAB
                    # Handle snab slack threads as well
                    snab_slack_thread_ts_updates = []
                    if SNAB_ENABLED:
                        try:
                            # @modified 20230925 - Task #5000: Replace alert key scans with sets
                            # Switch to hash key instead of scan_iter
                            # snab_slack_thread_ts_updates = list(self.redis_conn_decoded.scan_iter(match='panorama.snab.slack_thread_ts.*'))
                            snab_slack_thread_ts_updates = []
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to scan panorama.snab.slack_thread_ts.* from Redis')
                            snab_slack_thread_ts_updates = []
                        if snab_slack_thread_ts_updates:
                            for snab_slack_thread_ts_update in snab_slack_thread_ts_updates:
                                slack_thread_ts_updates.append(snab_slack_thread_ts_update)
                        if not slack_thread_ts_updates:
                            logger.info('no panorama.snab.slack_thread_ts Redis keys to process, OK')

                        # @added 20230925 - Task #5000: Replace alert key scans with sets
                        # Instead of using expiring keys with have a compute cost with
                        # using scan_iter(match='[PATTERN]') switch to using entries in a
                        # hash key, the management of which has a must lower compute cost.
                        snab_slack_thread_ts_updates_hash_key = 'panorama.snab.slack_threads_ts'
                        try:
                            hash_snab_slack_thread_ts_updates_keys = self.redis_conn_decoded.hkeys(snab_slack_thread_ts_updates_hash_key)
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: hkeys failed %s Redis hash - %s' % (
                                snab_slack_thread_ts_updates_hash_key, err))
                            hash_snab_slack_thread_ts_updates_keys = []
                        if hash_snab_slack_thread_ts_updates_keys:
                            logger.debug('debug :: would process %s panorama.snab.slack_threads_ts from Redis hash' % (
                                str(len(hash_snab_slack_thread_ts_updates_keys))))
                        else:
                            logger.info('no entries in %s Redis hash to process, OK' % snab_slack_thread_ts_updates_hash_key)
                # @added 20230925 - Task #5000: Replace alert key scans with sets
                # Instead of using expiring keys with have a compute cost with
                # using scan_iter(match='[PATTERN]') switch to using entries in a
                # hash key, the management of which has a must lower compute cost.
                hash_slack_thread_ts_updates = []
                try:
                    hash_slack_thread_ts_updates = hash_slack_thread_ts_updates_keys + hash_snab_slack_thread_ts_updates_keys
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to create hash_slack_thread_ts_updates, err: %s' % err)

                for key in hash_slack_thread_ts_updates:
                    update_db_record = False
                    update_table = 'anomalies'
                    snab_id = None
                    if key in hash_snab_slack_thread_ts_updates_keys:
                        update_table = 'snab'
                    base_name = None
                    metric_timestamp = None
                    if update_table == 'anomalies':
                        try:
                            update_on = self.redis_conn_decoded.hget(slack_thread_ts_updates_hash_key, key)
                            update_for = literal_eval(update_on)
                            base_name = update_for['metric']
                            metric_timestamp = update_for['timestamp']
                            slack_thread_ts = float(update_for['slack_thread_ts'])
                        except Exception as err:
                            logger.error('error :: hget failed on %s from %s Redis hash - %s' % (
                                key, slack_thread_ts_updates_hash_key, err))
                        if base_name and metric_timestamp:
                            update_db_record = True
                        else:
                            logger.info('Could not determine base_name and metric_timestamp from hash key %s in %s Redis hash, deleting key' % (
                                key, slack_thread_ts_updates_hash_key))
                            try:
                                self.redis_conn.hdel(slack_thread_ts_updates_hash_key, key)
                            except Exception as err:
                                logger.error('error :: failed to delete key from hash - %s' % err)
                    if update_table == 'snab':
                        hash_key_key_elements = key.split('.')
                        if hash_key_key_elements[1] == 'None':
                            try:
                                self.redis_conn.hdel(snab_slack_thread_ts_updates_hash_key, key)
                            except Exception as err:
                                logger.error('error :: failed to hdel %s from %s Redis hash - %s' % (
                                    key, snab_slack_thread_ts_updates_hash_key, err))
                        else:
                            try:
                                slack_thread_ts = self.redis_conn_decoded.hget(snab_slack_thread_ts_updates_hash_key, key)
                            except Exception as err:
                                logger.error('error :: failed to hget %s from %s Redis hash - %s' % (
                                    key, snab_slack_thread_ts_updates_hash_key, err))
                            try:
                                metric_timestamp = int(hash_key_key_elements[0])
                                snab_id = int(hash_key_key_elements[1])
                            except Exception as err:
                                logger.error('error :: failed to determine metric_timestamp from key elements %s - %s' % (
                                    str(snab_slack_thread_ts_updates_hash_key), err))
                            if slack_thread_ts and metric_timestamp:
                                update_db_record = True
                    if update_db_record:
                        # Spawn update_slack_thread_ts process
                        process_data = [base_name, metric_timestamp, slack_thread_ts, snab_id]
                        pids = []
                        spawned_pids = []
                        pid_count = 0
                        now = time()
                        for i in range(1, 2):
                            try:
                                # @modified 20200929 - Task #3748: POC SNAB
                                #                      Branch #3068: SNAB
                                # Handle snab as well
                                # p = Process(target=self.update_slack_thread_ts, args=(i, base_name, metric_timestamp, slack_thread_ts))
                                p = Process(target=self.update_slack_thread_ts, args=(i, base_name, metric_timestamp, slack_thread_ts, snab_id))
                                pids.append(p)
                                pid_count += 1
                                logger.info('starting update_slack_thread_ts - %s' % process_data)
                                p.start()
                                spawned_pids.append(p.pid)
                            except:
                                logger.info(traceback.format_exc())
                                logger.error('error :: to start update_slack_thread_ts')
                                continue
                        p_starts = time()
                        # @modified 20190509 - Branch #2646: slack
                        # If the Skyline MySQL database is on a remote host
                        # 2 seconds here is sometimes not sufficient so
                        # increased to 10
                        while time() - p_starts <= 10:
                            if any(p.is_alive() for p in pids):
                                # Just to avoid hogging the CPU
                                sleep(.1)
                            else:
                                # All the processes are done, break now.
                                time_to_run = time() - p_starts
                                logger.info(
                                    '%s :: update_slack_thread_ts completed in %.2f seconds' % (
                                        skyline_app, time_to_run))
                                break
                        else:
                            # We only enter this if we didn't 'break' above.
                            logger.info('%s :: timed out, killing all update_slack_thread_ts processes' % (skyline_app))
                            for p in pids:
                                p.terminate()
                if slack_thread_ts_updates:
                    logger.debug('debug :: switching from scan_iter to hash key method setting slack_thread_ts_updates to empty list')
                    slack_thread_ts_updates = []

                if slack_thread_ts_updates:
                    for cache_key in slack_thread_ts_updates:

                        # @added 20200929 - Task #3748: POC SNAB
                        #                   Branch #3068: SNAB
                        update_table = 'anomalies'
                        snab_id = None
                        if 'panorama.snab.slack_thread_ts' in cache_key:
                            update_table = 'snab'

                        base_name = None
                        metric_timestamp = None

                        if update_table == 'anomalies':
                            try:
                                # @modified 20191106 - Bug #3266: py3 Redis binary objects not strings
                                #                      Branch #3262: py3
                                # update_on = self.redis_conn.get(cache_key)
                                update_on = self.redis_conn_decoded.get(cache_key)
                                # cache_key_value = [base_name, metric_timestamp, slack_thread_ts]
                                update_for = literal_eval(update_on)
                                base_name = str(update_for[0])
                                metric_timestamp = int(float(update_for[1]))
                                slack_thread_ts = float(update_for[2])
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to get details from cache_key %s' % cache_key)
                            update_db_record = False
                            if base_name and metric_timestamp:
                                update_db_record = True
                            else:
                                logger.info('Could not determine base_name and metric_timestamp from cache_key %s, deleting' % cache_key)
                                try:
                                    self.redis_conn.delete(cache_key)
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to delete cache_key %s' % cache_key)

                        # @added 20200929 - Task #3748: POC SNAB
                        #                   Branch #3068: SNAB
                        if update_table == 'snab':
                            cache_key_elements = cache_key.split('.')
                            if cache_key_elements[4] == 'None':
                                try:
                                    self.redis_conn.delete(cache_key)
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to delete cache_key %s' % cache_key)
                            else:
                                try:
                                    slack_thread_ts = self.redis_conn_decoded.get(cache_key)
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to read cache_key %s' % cache_key)
                                try:
                                    metric_timestamp = int(cache_key_elements[3])
                                    snab_id = int(cache_key_elements[4])
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to determine metric_timestamp from cache_key element - %s' % str(cache_key))
                                if slack_thread_ts and metric_timestamp:
                                    update_db_record = True

                        if update_db_record:
                            # Spawn update_slack_thread_ts process
                            process_data = [base_name, metric_timestamp, slack_thread_ts, snab_id]
                            pids = []
                            spawned_pids = []
                            pid_count = 0
                            now = time()
                            for i in range(1, 2):
                                try:
                                    # @modified 20200929 - Task #3748: POC SNAB
                                    #                      Branch #3068: SNAB
                                    # Handle snab as well
                                    # p = Process(target=self.update_slack_thread_ts, args=(i, base_name, metric_timestamp, slack_thread_ts))
                                    p = Process(target=self.update_slack_thread_ts, args=(i, base_name, metric_timestamp, slack_thread_ts, snab_id))
                                    pids.append(p)
                                    pid_count += 1
                                    logger.info('starting update_slack_thread_ts - %s' % process_data)
                                    p.start()
                                    spawned_pids.append(p.pid)
                                except:
                                    logger.info(traceback.format_exc())
                                    logger.error('error :: to start update_slack_thread_ts')
                                    continue
                            p_starts = time()
                            # @modified 20190509 - Branch #2646: slack
                            # If the Skyline MySQL database is on a remote host
                            # 2 seconds here is sometimes not sufficient so
                            # increased to 10
                            while time() - p_starts <= 10:
                                if any(p.is_alive() for p in pids):
                                    # Just to avoid hogging the CPU
                                    sleep(.1)
                                else:
                                    # All the processes are done, break now.
                                    time_to_run = time() - p_starts
                                    logger.info(
                                        '%s :: update_slack_thread_ts completed in %.2f seconds' % (
                                            skyline_app, time_to_run))
                                    break
                            else:
                                # We only enter this if we didn't 'break' above.
                                logger.info('%s :: timed out, killing all update_slack_thread_ts processes' % (skyline_app))
                                for p in pids:
                                    p.terminate()

                # @added 20210612 - Branch #1444: thunder
                # Report app up
                try:
                    self.redis_conn.setex(skyline_app, 120, int(time()))
                    logger.info('updated Redis key for %s up' % skyline_app)
                except:
                    logger.error('error :: failed to update Redis key for %s up' % skyline_app)

                # @added 20200825 - Feature #3704: Add alert to anomalies
                # Check if any Redis keys exist with anomaly alerts to update
                # the alert field on any anomaly records
                alert_updates = None
                try:
                    # @modified 20230925 - Task #5000: Replace alert key scans with sets
                    # Switch to hash key instead of scan_iter
                    # alert_updates = list(self.redis_conn_decoded.scan_iter(match='panorama.alert.*'))
                    alert_updates = []
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to scan panorama.alert.* from Redis')
                    alert_updates = []

                # @added 20230908 - Task #5000: Replace alert key scans with sets
                hash_alert_updates_key = 'panorama.alerts'
                hash_alert_updates = {}
                try:
                    hash_alert_updates = self.redis_conn_decoded.hgetall(hash_alert_updates_key)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to hgetall on panorama.alerts from Redis')
                    hash_alert_updates = {}
                if hash_alert_updates:
                    logger.debug('debug :: would process %s panorama.alerts from Redis hash' % str(len(hash_alert_updates)))
                    logger.debug('debug :: hash_alert_updates: %s' % str(hash_alert_updates))
                    try:
                        self.redis_conn_decoded.delete('panorama.alerts')
                        logger.info('testing - deleted panorama.alerts from Redis hash')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to delete panorama.alerts from Redis')
                else:
                    logger.info('no panorama.alerts in Redis hash to process, OK')
                # @added 20230925 - Task #5000: Replace alert key scans with sets
                # Instead of using expiring keys with have a compute cost with
                # using scan_iter(match='[PATTERN]') switch to using entries in a
                # hash key, the management of which has a must lower compute cost.
                if hash_alert_updates:
                    for hash_key_key, update_on in hash_alert_updates.items():
                        base_name = None
                        metric_timestamp = None
                        try:
                            # update_on = self.redis_conn_decoded.hget(hash_alert_updates_key, hash_key_key)
                            # key_value = "{'metric': base_name, 'timestamp': metric_timestamp, 'alerted_at': alerted_at}"
                            update_for = literal_eval(update_on)
                            base_name = update_for['metric']
                            metric_timestamp = update_for['timestamp']
                            alerted_at = update_for['alerted_at']
                        except Exception as err:
                            logger.error('error :: failed to get details from key %s in %s Redis hash - %s' % (
                                hash_key_key, hash_alert_updates_key, err))
                        update_db_record = False
                        if base_name and metric_timestamp and alerted_at:
                            update_db_record = True
                        else:
                            logger.info('Could not determine base_name, metric_timestamp or alerted_at from key %s in %s Redis hash, deleting key' % (
                                hash_key_key, hash_alert_updates_key))
                            try:
                                self.redis_conn.hdel(key)
                            except Exception as err:
                                # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
                                # bandit - B608:hardcoded_sql_expressions
                                logger.error('error :: failed to delete from key %s in %s Redis hash - %s' % (
                                    hash_key_key, hash_alert_updates_key, err))  # nosec B608
                        if update_db_record:
                            # Spawn update_alert_ts process
                            pids = []
                            spawned_pids = []
                            pid_count = 0
                            now = time()
                            for i in range(1, 2):
                                try:
                                    p = Process(target=self.update_alert_ts, args=(i, base_name, metric_timestamp, alerted_at))
                                    pids.append(p)
                                    pid_count += 1
                                    logger.info('starting update_alert_ts')
                                    p.start()
                                    spawned_pids.append(p.pid)
                                except:
                                    logger.info(traceback.format_exc())
                                    logger.error('error :: failed to start update_alert_ts')
                                    continue
                            p_starts = time()
                            while time() - p_starts <= 10:
                                if any(p.is_alive() for p in pids):
                                    # Just to avoid hogging the CPU
                                    sleep(.1)
                                else:
                                    # All the processes are done, break now.
                                    time_to_run = time() - p_starts
                                    logger.info(
                                        '%s :: update_alert_ts completed in %.2f seconds' % (
                                            skyline_app, time_to_run))
                                    break
                            else:
                                # We only enter this if we didn't 'break' above.
                                logger.info('%s :: timed out, killing all update_alert_ts processes' % (skyline_app))
                                for p in pids:
                                    p.terminate()


                # @modified 20230925 - Task #5000: Replace alert key scans with sets
                # Switch to hash key instead of scan_iter
                # if not alert_updates:
                #     logger.info('no panorama.alert Redis keys to process, OK')

                if alert_updates:
                    for cache_key in alert_updates:
                        base_name = None
                        metric_timestamp = None
                        try:
                            update_on = self.redis_conn_decoded.get(cache_key)
                            # cache_key_value = [base_name, metric_timestamp, alerted_at_timestamp]
                            update_for = literal_eval(update_on)
                            base_name = str(update_for[0])
                            metric_timestamp = int(float(update_for[1]))
                            alerted_at = int(update_for[2])
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to get details from cache_key %s' % cache_key)
                        update_db_record = False
                        if base_name and metric_timestamp and alerted_at:
                            update_db_record = True
                        else:
                            logger.info('Could not determine base_name, metric_timestamp or alerted_at from cache_key %s, deleting' % cache_key)
                            try:
                                self.redis_conn.delete(cache_key)
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to delete cache_key %s' % cache_key)
                        if update_db_record:
                            # Spawn update_alert_ts process
                            pids = []
                            spawned_pids = []
                            pid_count = 0
                            now = time()
                            for i in range(1, 2):
                                try:
                                    p = Process(target=self.update_alert_ts, args=(i, base_name, metric_timestamp, alerted_at))
                                    pids.append(p)
                                    pid_count += 1
                                    logger.info('starting update_alert_ts')
                                    p.start()
                                    spawned_pids.append(p.pid)
                                except:
                                    logger.info(traceback.format_exc())
                                    logger.error('error :: failed to start update_alert_ts')
                                    continue
                            p_starts = time()
                            while time() - p_starts <= 10:
                                if any(p.is_alive() for p in pids):
                                    # Just to avoid hogging the CPU
                                    sleep(.1)
                                else:
                                    # All the processes are done, break now.
                                    time_to_run = time() - p_starts
                                    logger.info(
                                        '%s :: update_alert_ts completed in %.2f seconds' % (
                                            skyline_app, time_to_run))
                                    break
                            else:
                                # We only enter this if we didn't 'break' above.
                                logger.info('%s :: timed out, killing all update_alert_ts processes' % (skyline_app))
                                for p in pids:
                                    p.terminate()

            metric_var_files_sorted = sorted(metric_var_files)
            metric_check_file = '%s/%s' % (settings.PANORAMA_CHECK_PATH, str(metric_var_files_sorted[0]))

            # @modified 20240119 - Task #5228: panorama - optimise insertions
            # logger.info('assigning anomaly for insertion - %s' % str(metric_var_files_sorted[0]))
            logger.info('assigning %s anomalies for insertion' % str(len(metric_var_files_sorted)))

            # @added 20240119 - Task #5228: panorama - optimise insertions
            # Process multiple metric check file in a single process rather than
            # handle a single metric_check_file per process.
            process_metric_check_files = {}
            checks_per_process = math.ceil(len(metric_var_files_sorted) / settings.PANORAMA_PROCESSES)
            metric_check_files_to_process = []
            for i in metric_var_files_sorted:
                add_check_file = '%s/%s' % (settings.PANORAMA_CHECK_PATH, str(i))
                metric_check_files_to_process.append(add_check_file)
            assigned_checks = [metric_check_files_to_process[i:(i + checks_per_process)] for i in range(0, len(metric_check_files_to_process), checks_per_process)]
            for i in range(1, settings.PANORAMA_PROCESSES + 1):
                process_metric_check_files[i] = assigned_checks[(i - 1)]

            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            now = time()
            for i in range(1, settings.PANORAMA_PROCESSES + 1):
                try:

                    # @added 20240119 - Task #5228: panorama - optimise insertions
                    # Process multiple metric check file in a single process rather than
                    # handle a single metric_check_file per process.
                    assigned_metric_check_files = []
                    if process_metric_check_files:
                        assigned_metric_check_files = process_metric_check_files[i]

                    # @modified 20240119 - Task #5228: panorama - optimise insertions
                    # p = Process(target=self.spin_process, args=(i, metric_check_file))
                    p = Process(target=self.spin_process, args=(i, metric_check_file, assigned_metric_check_files))

                    pids.append(p)
                    pid_count += 1
                    logger.info('starting %s of %s spin_process/es' % (str(pid_count), str(settings.PANORAMA_PROCESSES)))
                    p.start()
                    spawned_pids.append(p.pid)
                except:
                    logger.error('error :: to start spin_process')
                    logger.info(traceback.format_exc())
                    continue

            # Send wait signal to zombie processes
            # for p in pids:
            #     p.join()
            # Self monitor processes and terminate if any spin_process has run
            # for longer than CRUCIBLE_TESTS_TIMEOUT
            p_starts = time()
            while time() - p_starts <= 20:
                if any(p.is_alive() for p in pids):
                    # Just to avoid hogging the CPU
                    sleep(.1)
                else:
                    # All the processes are done, break now.
                    time_to_run = time() - p_starts
                    logger.info(
                        '%s :: %s spin_process/es completed in %.2f seconds' % (
                            skyline_app, str(settings.PANORAMA_PROCESSES),
                            time_to_run))
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('%s :: timed out, killing all spin_process processes' % (skyline_app))
                for p in pids:
                    p.terminate()
                    # p.join()

                check_file_name = os.path.basename(str(metric_check_file))
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: check_file_name - %s' % check_file_name)
                check_file_timestamp = check_file_name.split('.', 1)[0]
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: check_file_timestamp - %s' % str(check_file_timestamp))
                check_file_metricname_txt = check_file_name.split('.', 1)[1]
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: check_file_metricname_txt - %s' % check_file_metricname_txt)
                check_file_metricname = check_file_metricname_txt.replace('.txt', '')
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: check_file_metricname - %s' % check_file_metricname)
                check_file_metricname_dir = check_file_metricname.replace('.', '/')
                if settings.ENABLE_PANORAMA_DEBUG:
                    logger.info('debug :: check_file_metricname_dir - %s' % check_file_metricname_dir)

                metric_failed_check_dir = '%s/%s/%s' % (failed_checks_dir, check_file_metricname_dir, check_file_timestamp)

                # @modified 20240229 - Feature #5294: panorama - retry failed checks
                #fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
                failed_check_success = False
                failed_check_success = fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))

                # @added 20240229 - Feature #5294: panorama - retry failed checks
                # At times when there are potential DB issues or network
                # partitions from the DB panorama will fail checks.  This adds
                # the functionality for panorama to retry checks that have
                # recently failed.
                if failed_check_success:
                    failed_check_file = '%s/%s' % (metric_failed_check_dir, check_file_name)
                    # Check to see if it is already in the hash
                    failed_check_data_str = None
                    failed_count = 1
                    last_fail_timestamp = int(time())
                    key_data = {
                        'check_timestamp': int(check_file_timestamp),
                        'failed_check_file': failed_check_file,
                        'fail_count': failed_count,
                        'last_fail_timestamp': last_fail_timestamp,
                        'max_age_seconds': max_age_seconds,
                    }
                    try:
                        failed_check_data_str = self.redis_conn_decoded.hget('panorama.retry_failed_checks', check_file_name)
                        if failed_check_data_str:
                            try:
                                failed_check_data = literal_eval(failed_check_data_str)
                                try:
                                    key_data['fail_count'] = failed_check_data['failed_count'] + 1
                                except:
                                    key_data['fail_count'] = 1
                            except Exception as err:
                                logger.error('error :: failed to literal_eval values from %s key in Redis hash panorama.retry_failed_checks, err: %s' % (
                                    check_file_name, err))
                    except Exception as err:
                        logger.error('error :: failed to add %s with %s to Redis hash panorama.retry_failed_checks, err: %s' % (
                            check_file_name, failed_check_file, err))
                    try:
                        self.redis_conn_decoded.hset('panorama.retry_failed_checks', check_file_name, str(key_data))
                    except Exception as err:
                        logger.error('error :: failed to add %s with %s to Redis hash panorama.retry_failed_checks, err: %s' % (
                            check_file_name, str(key_data), err))

            for p in pids:
                if p.is_alive():
                    logger.info('%s :: stopping spin_process - %s' % (skyline_app, str(p.is_alive())))
                    # @modified 20240108 - Task #5178: Build and test skyline v4.1.0
                    # Commented out p.join
                    # p.join()
                    # @added 20240104 - Task #5178: Build and test skyline v4.1.0
                    p.terminate()

