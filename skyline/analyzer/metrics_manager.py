from __future__ import division
import logging
from time import time, sleep
from threading import Thread
from multiprocessing import Process
import os
from os import kill, getpid
import traceback
from sys import version_info
import os.path
from ast import literal_eval
from timeit import default_timer as timer

# @added 20201209 - Feature #3870: metrics_manager - check_data_sparsity
from msgpack import Unpacker
# from collections import Counter

# @added 20201213 - Feature #3890: metrics_manager - sync_cluster_files
import requests

import settings
from skyline_functions import (
    # @modified 20201209 - Feature #3870: metrics_manager - check_data_sparsity
    # Added send_graphite_metric
    get_redis_conn, get_redis_conn_decoded, send_graphite_metric,
    # @added 20201213 - Feature #3890: metrics_manager - sync_cluster_files
    mkdir_p,
    # @added 20210624 - Feature #4150: metrics_manager - roomba batch processing metrics
    sort_timeseries)
from matched_or_regexed_in_list import matched_or_regexed_in_list

# @added 20210430 - Task #4030: refactoring
from functions.database.queries.get_all_db_metric_names import get_all_db_metric_names

# @added 20210519 - Feature #4076: CUSTOM_STALE_PERIOD
from functions.settings.get_custom_stale_period import custom_stale_period
from functions.thunder.stale_metrics import thunder_stale_metrics

# @added 20210601 - Feature #4000: EXTERNAL_SETTINGS
from functions.settings.manage_external_settings import manage_external_settings

# @added 20210602 - Feature #4076: CUSTOM_STALE_PERIOD
from functions.settings.get_external_settings import get_external_settings
from functions.thunder.manage_thunder_alerted_on_stale_metrics_hash_key import manage_thunder_alerted_on_stale_metrics_hash_key
from functions.redis.prune_metrics_timestamp_hash_key import prune_metrics_timestamp_hash_key
from functions.thunder.no_data import thunder_no_data

# @added 20210619 - Feature #4148: analyzer.metrics_manager.resolutions
#                   Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
#                   Feature #3870: metrics_manager - check_data_sparsity
# Use common functions
from functions.timeseries.determine_data_frequency import determine_data_frequency
from functions.timeseries.determine_data_sparsity import determine_data_sparsity

# @added 20220110 - Bug #4364: Prune old thunder.events
#                   Branch #1444: thunder
from functions.redis.update_set import update_redis_set

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

python_version = int(version_info[0])

this_host = str(os.uname()[1])

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

# @modified 20190524 - Feature #2882: Mirage - periodic_check
#                      Branch #3002: docker
# Moved the interpolation of the MIRAGE_PERIODIC_ variables to the top
# of the file out of spawn_process so they can be accessed in run too
# @added 20190408 - Feature #2882: Mirage - periodic_check
# Add Mirage periodic checks so that Mirage is analysing each metric at
# least once per hour.
try:
    # @modified 20200606 - Bug #3572: Apply list to settings imports
    MIRAGE_PERIODIC_CHECK = list(settings.MIRAGE_PERIODIC_CHECK)
except:
    MIRAGE_PERIODIC_CHECK = False
try:
    MIRAGE_PERIODIC_CHECK_INTERVAL = settings.MIRAGE_PERIODIC_CHECK_INTERVAL
except:
    MIRAGE_PERIODIC_CHECK_INTERVAL = 3600
# @added 20200505 - Feature #2882: Mirage - periodic_check
# Surface this once
try:
    # @modified 20200606 - Bug #3572: Apply list to settings imports
    mirage_periodic_check_namespaces = list(settings.MIRAGE_PERIODIC_CHECK_NAMESPACES)
except:
    mirage_periodic_check_namespaces = []
try:
    ANALYZER_ENABLED = settings.ANALYZER_ENABLED
except:
    ANALYZER_ENABLED = True
    logger.info('warning :: ANALYZER_ENABLED is not declared in settings.py, defaults to True')

# @added 20200528 - Feature #3560: External alert config
try:
    EXTERNAL_ALERTS = list(settings.EXTERNAL_ALERTS)
except:
    EXTERNAL_ALERTS = {}
# @added 20200602 - Feature #3560: External alert config
if EXTERNAL_ALERTS:
    from external_alert_configs import get_external_alert_configs

# @added 20200607 - Feature #3566: custom_algorithms
try:
    MIRAGE_ALWAYS_METRICS = list(settings.MIRAGE_ALWAYS_METRICS)
except:
    MIRAGE_ALWAYS_METRICS = []

# @added 20200827 - Feature #3708: FLUX_ZERO_FILL_NAMESPACES
try:
    FLUX_ZERO_FILL_NAMESPACES = settings.FLUX_ZERO_FILL_NAMESPACES
except:
    FLUX_ZERO_FILL_NAMESPACES = []

# @added 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
try:
    FLUX_LAST_KNOWN_VALUE_NAMESPACES = settings.FLUX_LAST_KNOWN_VALUE_NAMESPACES
except:
    FLUX_LAST_KNOWN_VALUE_NAMESPACES = []

# @added 20201017 - Feature #3818: ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED
# This was implemented to allow a busy analyzer to offload low priority metrics
# to analyzer_batch, unsuccessfully.  It works, but takes loanger and ages
# actually.  Being left in as may be workable with a different logic.
try:
    ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED = settings.ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED
except:
    ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED = False
# Always disable until refactored to work more efficiently if possible
ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED = False

# @added 20201030 - Feature #3812: ANALYZER_ANALYZE_LOW_PRIORITY_METRICS
try:
    ANALYZER_ANALYZE_LOW_PRIORITY_METRICS = settings.ANALYZER_ANALYZE_LOW_PRIORITY_METRICS
except:
    ANALYZER_ANALYZE_LOW_PRIORITY_METRICS = True
# @added 20201030 - Feature #3808: ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS
try:
    ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS = settings.ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS
except:
    ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS = False
# @added 20201018 - Feature #3810: ANALYZER_MAD_LOW_PRIORITY_METRICS
try:
    ANALYZER_MAD_LOW_PRIORITY_METRICS = settings.ANALYZER_MAD_LOW_PRIORITY_METRICS
except:
    ANALYZER_MAD_LOW_PRIORITY_METRICS = 0
# @added 20201030 - Feature #3808: ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS
# Set the default ANALYZER_MAD_LOW_PRIORITY_METRICS to 10 if not set and
# ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS is set.
if ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS:
    if not ANALYZER_MAD_LOW_PRIORITY_METRICS:
        ANALYZER_MAD_LOW_PRIORITY_METRICS = 10

# Determine all the settings that place Analyzer in a mode to handle low
# priority metrics differently
ANALYZER_MANAGE_LOW_PRIORITY_METRICS = False
if ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED:
    ANALYZER_MANAGE_LOW_PRIORITY_METRICS = True
if not ANALYZER_ANALYZE_LOW_PRIORITY_METRICS:
    ANALYZER_MANAGE_LOW_PRIORITY_METRICS = True
if ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS:
    ANALYZER_MANAGE_LOW_PRIORITY_METRICS = True
if ANALYZER_MAD_LOW_PRIORITY_METRICS:
    ANALYZER_MANAGE_LOW_PRIORITY_METRICS = True
low_priority_metrics_hash_key = 'analyzer.low_priority_metrics.last_analyzed_timestamp'
metrics_last_timestamp_hash_key = 'analyzer.metrics.last_analyzed_timestamp'

# @added 20201209 - Feature #3870: metrics_manager - check_data_sparsity
try:
    CHECK_DATA_SPARSITY = settings.CHECK_DATA_SPARSITY
except:
    CHECK_DATA_SPARSITY = True
try:
    inactive_after = settings.FULL_DURATION - settings.METRICS_INACTIVE_AFTER
except:
    inactive_after = settings.FULL_DURATION - 3600
# @added 20201210 - Feature #3870: metrics_manager - check_data_sparsity
try:
    SKIP_CHECK_DATA_SPARSITY_NAMESPACES = list(settings.SKIP_CHECK_DATA_SPARSITY_NAMESPACES)
except:
    SKIP_CHECK_DATA_SPARSITY_NAMESPACES = []
# Declare how often to run metrics_manager is seconds as this is used on some
# Redis key TTLs as well
RUN_EVERY = 300

# @added 20201212 - Feature #3884: ANALYZER_CHECK_LAST_TIMESTAMP
try:
    ANALYZER_CHECK_LAST_TIMESTAMP = settings.ANALYZER_CHECK_LAST_TIMESTAMP
except:
    ANALYZER_CHECK_LAST_TIMESTAMP = False

# @added 20201213 - Feature #3890: metrics_manager - sync_cluster_files
try:
    REMOTE_SKYLINE_INSTANCES = list(settings.REMOTE_SKYLINE_INSTANCES)
except:
    REMOTE_SKYLINE_INSTANCES = []
try:
    SYNC_CLUSTER_FILES = settings.SYNC_CLUSTER_FILES
except:
    SYNC_CLUSTER_FILES = False
try:
    FAKE_CLUSTER_SYNC = settings.FAKE_CLUSTER_SYNC
except:
    FAKE_CLUSTER_SYNC = False
# @added 20201214 - Feature #3890: metrics_manager - sync_cluster_files
#                   Feature #3820: HORIZON_SHARDS
try:
    HORIZON_SHARDS = settings.HORIZON_SHARDS.copy()
except:
    HORIZON_SHARDS = {}
number_of_horizon_shards = 0
HORIZON_SHARD = 0
if HORIZON_SHARDS:
    import zlib
    number_of_horizon_shards = len(HORIZON_SHARDS)
    HORIZON_SHARD = HORIZON_SHARDS[this_host]

# @added 20210202 - Feature #3934: ionosphere_performance
try:
    IONOSPHERE_PERFORMANCE_DATA_POPULATE_CACHE = settings.IONOSPHERE_PERFORMANCE_DATA_POPULATE_CACHE
except:
    IONOSPHERE_PERFORMANCE_DATA_POPULATE_CACHE = False
try:
    IONOSPHERE_PERFORMANCE_DATA_POPULATE_CACHE_DEPTH = int(settings.IONOSPHERE_PERFORMANCE_DATA_POPULATE_CACHE_DEPTH)
except:
    IONOSPHERE_PERFORMANCE_DATA_POPULATE_CACHE_DEPTH = 0

# @added 20210406 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
try:
    FLUX_AGGREGATE_NAMESPACES = settings.FLUX_AGGREGATE_NAMESPACES.copy()
except:
    FLUX_AGGREGATE_NAMESPACES = {}
try:
    FLUX_EXTERNAL_AGGREGATE_NAMESPACES = settings.FLUX_EXTERNAL_AGGREGATE_NAMESPACES
except:
    FLUX_EXTERNAL_AGGREGATE_NAMESPACES = False

# @added 20210513 - Feature #4068: ANALYZER_SKIP
try:
    ANALYZER_SKIP = settings.ANALYZER_SKIP
except:
    ANALYZER_SKIP = []

# @added 20210519 - Feature #4076: CUSTOM_STALE_PERIOD
try:
    CUSTOM_STALE_PERIOD = settings.CUSTOM_STALE_PERIOD.copy()
except:
    CUSTOM_STALE_PERIOD = {}

# @added 20210601 - Feature #4000: EXTERNAL_SETTINGS
try:
    EXTERNAL_SETTINGS = settings.EXTERNAL_SETTINGS.copy()
except:
    EXTERNAL_SETTINGS = {}

skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)

full_uniques = '%sunique_metrics' % settings.FULL_NAMESPACE

LOCAL_DEBUG = False


class Metrics_Manager(Thread):
    """
    The Metrics_Manager class which controls the metrics_manager thread and
    spawned processes.

    All of this functionality was previously done in the Analyzer thread itself
    however with 10s of 1000s of metrics, this process can take longer than a
    minute to achieve, which would make Analyzer lag.  All the original commits
    and references from the Analyzer code have been maintained here, although
    the logical order has been changed and the blocks ordered in a different,
    but more appropriate and efficient manner than they were laid out in Analyzer.
    Further some blocks from Analyzer were removed with the new consolidated
    methods using sets, they were no longer required.

    """

    def __init__(self, parent_pid):
        """
        Initialize the Metrics_Manager
        """
        super(Metrics_Manager, self).__init__()
        self.redis_conn = get_redis_conn(skyline_app)
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            exit(0)

    # @added 20201214 - Feature #3890: metrics_manager - sync_cluster_files
    #                   Feature #3820: HORIZON_SHARDS
    def assigned_to_shard(self, metric_name):
        """
        Determine which shard a metric is assigned to.
        """
        assigned_host = this_host
        if HORIZON_SHARDS:
            try:
                metric_as_bytes = str(metric_name).encode()
                value = zlib.adler32(metric_as_bytes)
                modulo_result = value % number_of_horizon_shards
                for shard_hostname in HORIZON_SHARDS:
                    if HORIZON_SHARDS[shard_hostname] == modulo_result:
                        assigned_host = shard_hostname
            except Exception as e:
                logger.error('error :: metrics_manager :: failed to determine what shard %s is assigned to via modulo and HORIZON_SHARDS: %s' % (str(metric_name), e))
        return assigned_host

    def get_remote_data(self, remote_skyline_instance, data_required, endpoint, save_file=False):
        try:
            connect_timeout = int(settings.GRAPHITE_CONNECT_TIMEOUT)
            read_timeout = int(settings.GRAPHITE_READ_TIMEOUT)
        except:
            connect_timeout = 5
            read_timeout = 10
        use_timeout = (int(connect_timeout), int(read_timeout))
        data = []
        r = None
        user = None
        password = None
        use_auth = False
        try:
            user = str(remote_skyline_instance[1])
            password = str(remote_skyline_instance[2])
            use_auth = True
        except:
            user = None
            password = None
        logger.info('metrics_manager :: sync_cluster_files - querying %s for %s on %s' % (
            str(remote_skyline_instance[0]), str(data_required), str(endpoint)))
        try:
            url = '%s/%s' % (str(remote_skyline_instance[0]), endpoint)
            if use_auth:
                r = requests.get(url, timeout=use_timeout, auth=(user, password))
            else:
                r = requests.get(url, timeout=use_timeout)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: sync_cluster_files - failed to get %s from %s' % (
                str(endpoint), str(remote_skyline_instance[0])))
        if not r:
            logger.warning('warning :: metrics_manager :: sync_cluster_files - no r from %s on %s' % (
                endpoint, str(remote_skyline_instance[0])))
            return data
        if r:
            if r.status_code != 200:
                logger.error('error :: metrics_manager :: sync_cluster_files - %s from %s responded with status code %s and reason %s' % (
                    endpoint, str(remote_skyline_instance[0]), str(r.status_code), str(r.reason)))
            if save_file and r.status_code == 200:
                file_saved = False
                try:
                    open(save_file, 'wb').write(r.content)
                    if not os.path.isfile(save_file):
                        logger.error('error :: metrics_manager :: sync_cluster_files - failed to save_file %s from %s' % (
                            str(save_file), str(remote_skyline_instance[0])))
                    else:
                        file_saved = True
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: sync_cluster_files - failed to get %s from %s' % (
                        endpoint, str(remote_skyline_instance[0])))
                return file_saved

            js = None
            try:
                js = r.json()
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: sync_cluster_files - failed to get json from the response from %s on %s' % (
                    endpoint, str(remote_skyline_instance)))
            if js:
                logger.info('metrics_manager :: sync_cluster_files - got response for %s from %s' % (
                    str(data_required), str(remote_skyline_instance[0])))
                try:
                    data = js['data'][data_required]
                    logger.debug('metrics_manager :: debug :: sync_cluster_files - response for %s has %s items' % (
                        str(data_required), str(len(data))))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: sync_cluster_files - failed to build remote_training_data from %s on %s' % (
                        str(data_required), str(remote_skyline_instance)))
        return data

    # @added 20201213 - Feature #3890: metrics_manager - sync_cluster_files
    def sync_cluster_files(self, i):
        """
        Fetch any missing training_data and features_profiles directories and
        files from REMOTE_SKYLINE_INSTANCES
        """
        spin_start = time()
        logger.info('metrics_manager :: sync_cluster_files started')

        local_skyline_instance = None
        for remote_skyline_instance in REMOTE_SKYLINE_INSTANCES:
            # @added 20201213 - Feature #3890: metrics_manager - sync_cluster_files
            # if remote_skyline_instance == settings.SKYLINE_URL:
            if remote_skyline_instance[3] == this_host:
                local_skyline_instance = remote_skyline_instance
        if FAKE_CLUSTER_SYNC:
            logger.info('metrics_manager :: sync_cluster_files FAKE_CLUSTER_SYNC - True')
            skyline_url = 'http://%s:%s' % (settings.WEBAPP_IP, str(settings.WEBAPP_PORT))
            if settings.WEBAPP_AUTH_ENABLED:
                local_skyline_instance = [
                    skyline_url, settings.WEBAPP_AUTH_USER,
                    settings.WEBAPP_AUTH_USER_PASSWORD, this_host
                ]
            else:
                local_skyline_instance = [skyline_url, None, None, this_host]

        if not local_skyline_instance:
            logger.info('metrics_manager :: sync_cluster_files cound not determine local_skyline_instance from REMOTE_SKYLINE_INSTANCES, created')
            skyline_url = 'http://%s:%s' % (settings.WEBAPP_IP, str(settings.WEBAPP_PORT))
            if settings.WEBAPP_AUTH_ENABLED:
                local_skyline_instance = [
                    skyline_url, settings.WEBAPP_AUTH_USER,
                    settings.WEBAPP_AUTH_USER_PASSWORD, this_host
                ]
            else:
                local_skyline_instance = [skyline_url, None, None, this_host]

        # Check training data on REMOTE_SKYLINE_INSTANCES and determine what
        # training_data needs to be fetched
        training_data_raw = []
        try:
            training_data_raw = list(self.redis_conn_decoded.smembers('ionosphere.training_data'))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: sync_cluster_files - failed to generate a list from ionosphere.training_data Redis set')
            training_data_raw = None

        training_data = []
        if training_data_raw:
            for training_data_str in training_data_raw:
                try:
                    training_data_item = literal_eval(training_data_str)
                    training_metric = str(training_data_item[0])
                    training_timestamp = int(training_data_item[1])
                    training_data.append([training_metric, training_timestamp])
                except Exception as e:
                    logger.error('error :: metrics_manager :: sync_cluster_files - failed to interpolate training data: %s' % e)
                    training_data_raw = None
        training_data_already_present = 0
        training_data_to_fetch = 0
        training_data_fetched = 0

        # Rate limit this so that a new instance becomes eventually consistent
        # add does not thunder against all the other Skyline instances to
        # populate itself
        max_training_data_to_fetch = 30

        for remote_skyline_instance in REMOTE_SKYLINE_INSTANCES:
            if training_data_fetched >= max_training_data_to_fetch:
                logger.warning('warning :: metrics_manager :: fetched training data has reached the limit of %s, not continuing to fetch more this run' % str(max_training_data_to_fetch))
                break
            remote_training_data = []
            data_required = 'metrics'
            endpoint = 'api?training_data'
            save_file = False
            try:
                remote_training_data = self.get_remote_data(remote_skyline_instance, data_required, endpoint, save_file)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: sync_cluster_files - failed to get %s from %s' % (
                    endpoint, str(remote_skyline_instance[0])))
            remote_training_data_to_fetch = []
            training_data_already_fetched = 0
            if remote_training_data:
                logger.info('metrics_manager :: sync_cluster_files - got %s %s from %s' % (
                    str(len(remote_training_data)), str(data_required), str(remote_skyline_instance[0])))
                for remote_training_data_item in remote_training_data:
                    if remote_training_data_item in training_data:
                        training_data_already_present += 1
                        continue
                    else:
                        training_data_to_fetch += 1
                        remote_training_data_to_fetch.append(remote_training_data_item)
            logger.info('metrics_manager :: sync_cluster_files - %s training_data dirs from %s already present locally' % (
                str(training_data_already_present), str(remote_skyline_instance[0])))
            logger.info('metrics_manager :: sync_cluster_files - %s training_data dirs to fetch from %s' % (
                str(len(remote_training_data_to_fetch)), str(remote_skyline_instance[0])))
            if remote_training_data_to_fetch:
                for fetch_item in remote_training_data_to_fetch:
                    cache_key_exists = False
                    try:
                        cache_key = 'analyzer.metrics_manager.training_data_fetched.%s.%s' % (
                            str(fetch_item[0]), str(fetch_item[1]))
                        cache_key_exists = self.redis_conn.get(cache_key)
                    except:
                        pass
                    if cache_key_exists:
                        training_data_already_fetched += 1
                        continue
                    remote_training_data_files = []
                    try:
                        data_required = 'files'
                        save_file = False
                        endpoint = 'ionosphere_files?source=training_data&metric=%s&timestamp=%s' % (
                            str(fetch_item[0]), str(fetch_item[1]))
                        remote_training_data_files = self.get_remote_data(remote_skyline_instance, data_required, endpoint, save_file)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: sync_cluster_files - failed to get %s from %s' % (
                            endpoint, str(remote_skyline_instance[0])))
                    if not remote_training_data_files:
                        logger.debug('metrics_manager :: debug :: sync_cluster_files - no training_data files were returned from %s' % (
                            str(remote_skyline_instance[0])))
                    if remote_training_data_files:
                        files_to_fetch = len(remote_training_data_files)
                        files_fetched = 0
                        files_exist = 0
                        logger.info('metrics_manager :: sync_cluster_files - %s training_data files to fetch from %s' % (
                            str(files_to_fetch),
                            str(remote_skyline_instance[0])))
                        for remote_file in remote_training_data_files:
                            try:
                                data_file = remote_training_data_files[remote_file]
                                data_dir = os.path.dirname(data_file)
                                if not os.path.exists(data_dir):
                                    mkdir_p(data_dir)
                                if not os.path.exists(data_dir):
                                    logger.error('error :: metrics_manager :: sync_cluster_files - failed to create dir - %s' % data_dir)
                                    continue
                                data_required = 'file_saved'
                                endpoint = 'ionosphere_file?file=%s' % str(data_file)
                                file_saved = None
                                if not os.path.isfile(data_file):
                                    file_saved = self.get_remote_data(remote_skyline_instance, data_required, endpoint, data_file)
                                    if not file_saved:
                                        logger.error('error :: metrics_manager :: sync_cluster_files - failed to get %s from %s' % (
                                            data_file, str(remote_skyline_instance[0])))
                                        continue
                                    else:
                                        logger.info('metrics_manager :: sync_cluster_files - got %s from %s' % (
                                            data_file, str(remote_skyline_instance[0])))
                                        files_fetched += 1
                                else:
                                    if LOCAL_DEBUG:
                                        logger.info('metrics_manager :: sync_cluster_files - file exists locally nothing to do %s' % (
                                            data_file))
                                    files_exist += 1
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: sync_cluster_files - failed to get %s from %s' % (
                                    endpoint, str(remote_skyline_instance[0])))
                        if files_fetched == files_to_fetch:
                            training_data_fetched += 1
                            try:
                                cache_key = 'analyzer.metrics_manager.training_data_fetched.%s.%s' % (
                                    str(fetch_item[0]), str(fetch_item[1]))
                                self.redis_conn.setex(cache_key, 900, int(time()))
                                logger.info('created Redis key - %s' % (cache_key))
                            except:
                                pass
                        if files_exist == files_to_fetch:
                            logger.info('metrics_manager :: sync_cluster_files - all %s files to fetch exist locally, nothing to do' % (
                                str(files_to_fetch)))
                            try:
                                cache_key = 'analyzer.metrics_manager.training_data_fetched.%s.%s' % (
                                    str(fetch_item[0]), str(fetch_item[1]))
                                self.redis_conn.setex(cache_key, 900, int(time()))
                                logger.info('created Redis key - %s' % (cache_key))
                            except:
                                pass
                        if (files_fetched + files_exist) != files_to_fetch:
                            logger.error('error :: metrics_manager :: sync_cluster_files - failed to get all %s training_data files for %s from %s' % (
                                str(files_to_fetch), str(fetch_item), str(remote_skyline_instance[0])))
            logger.info('metrics_manager :: sync_cluster_files - %s training_data dirs already fetched and present locally' % (
                str(training_data_already_fetched)))

        # Check what features_profiles directories need to be retrieved from
        # REMOTE_SKYLINE_INSTANCES and saved locally
        logger.info('metrics_manager :: checking for expected_features_profiles_dir')
        expected_features_profile_dirs = {}
        try:
            endpoint = 'api?expected_features_profiles_dir'
            data_required = 'features_profile_dirs'
            save_file = False
            expected_features_profile_dirs = self.get_remote_data(local_skyline_instance, data_required, endpoint, save_file)
            logger.info('metrics_manager :: %s entries in the expected_features_profiles_dir response' % str(len(expected_features_profile_dirs)))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: sync_cluster_files - failed to get %s from %s' % (
                endpoint, str(local_skyline_instance[0])))
            expected_features_profile_dirs = {}
        local_features_profile_dirs = {}
        try:
            local_features_profile_dirs = self.redis_conn_decoded.hgetall('analyzer.metrics_manager.local_features_profile_dirs')
            logger.info('metrics_manager :: %s entries in the analyzer.metrics_manager.local_features_profile_dirs Redis hash key' % str(len(local_features_profile_dirs)))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: failed to get Redis hash key analyzer.metrics_manager.local_features_profile_dirs')
            local_features_profile_dirs = {}
        get_features_profile_dirs = {}
        if len(local_features_profile_dirs) == len(expected_features_profile_dirs):
            logger.info('metrics_manager :: all %s expected features_profile_dirs are present locally' % str(len(expected_features_profile_dirs)))
        else:
            for expected_features_profile_dir in expected_features_profile_dirs:
                dir_known = None
                if local_features_profile_dirs:
                    try:
                        dir_known = local_features_profile_dirs[expected_features_profile_dir]
                    except:
                        dir_known = False
                if dir_known:
                    continue
                try:
                    expected_dir = expected_features_profile_dirs[expected_features_profile_dir]
                    if os.path.exists(expected_dir):
                        try:
                            self.redis_conn.hset(
                                'analyzer.metrics_manager.local_features_profile_dirs',
                                expected_features_profile_dir, expected_dir)
                            logger.info('metrics_manager :: %s exists locally, added to analyzer.metrics_manager.local_features_profile_dirs' % str(expected_dir))
                            continue
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: metrics_manager :: failed to add entry in analyzer.metrics_manager.local_features_profile_dirs for - %s' % str(expected_dir))
                except Exception as e:
                    logger.error('error :: metrics_manager :: failed to test expected_dir: %s' % e)
                try:
                    get_features_profile_dirs[expected_features_profile_dir] = expected_features_profile_dirs[expected_features_profile_dir]
                except Exception as e:
                    logger.error('error :: metrics_manager :: failed to add to get_features_profile_dirs: %s' % e)
            logger.info('metrics_manager :: set to get %s features_profile_dirs not present locally' % str(len(get_features_profile_dirs)))

        # Add any entires that need to be checked for an update
        local_features_profile_dirs_to_update = {}
        try:
            local_features_profile_dirs_to_update = self.redis_conn_decoded.hgetall('analyzer.metrics_manager.local_features_profile_dirs.to_update')
            logger.info('metrics_manager :: %s entries in the analyzer.metrics_manager.local_features_profile_dirs.to_update Redis hash key' % str(len(local_features_profile_dirs_to_update)))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: failed to get Redis hash key analyzer.metrics_manager.local_features_profile_dirs.to_update')
            local_features_profile_dirs_to_update = {}
        if local_features_profile_dirs_to_update:
            for local_features_profile_dir_to_update in local_features_profile_dirs_to_update:
                try:
                    get_features_profile_dirs[local_features_profile_dir_to_update] = local_features_profile_dirs_to_update[local_features_profile_dir_to_update]
                except Exception as e:
                    logger.error('error :: metrics_manager :: failed to add to get_features_profile_dirs: %s' % e)

        # Rate limit this so that a new instance becomes eventually consistent
        # add does not thunder against all the other Skyline instances to
        # populate itself
        max_fps_to_fetch = 30
        fps_fetched = 0

        if get_features_profile_dirs:
            for fp_id in get_features_profile_dirs:
                if fps_fetched >= max_fps_to_fetch:
                    logger.warning('warning :: metrics_manager :: get_features_profile_dirs has reached the limit of %s, not continuing to fetch more this run' % str(max_fps_to_fetch))
                    break
                features_profile_dir = None
                endpoint = None
                try:
                    features_profile_dir = get_features_profile_dirs[fp_id]
                    features_profile_dir_elements = features_profile_dir.split('/')
                    timestamp = features_profile_dir_elements[-1]
                    metric_and_timestamp_path = features_profile_dir.replace(settings.IONOSPHERE_PROFILES_FOLDER, '')
                    metric_and_timestamp_path_elements = metric_and_timestamp_path.split('/')
                    metric = None
                    for element in metric_and_timestamp_path_elements:
                        if element != timestamp:
                            if metric:
                                metric = '%s.%s' % (metric, str(element))
                            else:
                                metric = str(element)
                    endpoint = 'ionosphere_files?source=features_profiles&metric=%s&timestamp=%s' % (
                        str(metric), str(timestamp))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: sync_cluster_files - failed to determine endpoint related to fp_id - %s' % (
                        str(fp_id)))
                features_profile_files = {}
                remote_skyline_instance = False
                USE_REMOTE_SKYLINE_INSTANCES = []
                for remote_skyline_instance in REMOTE_SKYLINE_INSTANCES:
                    USE_REMOTE_SKYLINE_INSTANCES.append(remote_skyline_instance)
                if not USE_REMOTE_SKYLINE_INSTANCES and FAKE_CLUSTER_SYNC:
                    USE_REMOTE_SKYLINE_INSTANCES.append(local_skyline_instance)

                # @added 20201214 - Feature #3890: metrics_manager - sync_cluster_files
                #                   Feature #3820: HORIZON_SHARDS
                # Determine the authorative REMOTE_SKYLINE_INSTANCES which is
                # assigned to metric and query it first.
                authorative_host = None
                if REMOTE_SKYLINE_INSTANCES:
                    try:
                        authorative_host = self.assigned_to_shard(metric)
                    except Exception as e:
                        logger.error('error :: metrics_manager :: sync_cluster_files - failed to determine authorative_host for %s - %s' % (
                            str(metric), e))
                    if authorative_host:
                        USE_REMOTE_SKYLINE_INSTANCES = []
                        for remote_skyline_instance in REMOTE_SKYLINE_INSTANCES:
                            try:
                                if remote_skyline_instance[3] == authorative_host:
                                    USE_REMOTE_SKYLINE_INSTANCES.append(remote_skyline_instance)
                            except Exception as e:
                                logger.error('error :: metrics_manager :: sync_cluster_files - failed to the remote_skyline_instance list for the authorative_host - %s - %s' % (
                                    str(authorative_host), e))
                        for remote_skyline_instance in REMOTE_SKYLINE_INSTANCES:
                            try:
                                if remote_skyline_instance[3] != authorative_host:
                                    USE_REMOTE_SKYLINE_INSTANCES.append(remote_skyline_instance)
                            except Exception as e:
                                logger.error('error :: metrics_manager :: sync_cluster_files - failed to the secondary remote_skyline_instance list for the authorative_host - %s - %s' % (
                                    str(authorative_host), e))

                for remote_skyline_instance in USE_REMOTE_SKYLINE_INSTANCES:
                    if endpoint:
                        data_required = 'files'
                        save_file = False
                        host_features_profile_files = {}
                        try:
                            host_features_profile_files = self.get_remote_data(remote_skyline_instance, data_required, endpoint, save_file)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: metrics_manager :: sync_cluster_files - failed to get %s from %s' % (
                                endpoint, str(remote_skyline_instance[0])))
                            host_features_profile_files = {}
                        if host_features_profile_files:
                            for features_profile_file in host_features_profile_files:
                                known_features_profile_file = False
                                try:
                                    known_features_profile_file = features_profile_files[features_profile_file]
                                except:
                                    known_features_profile_file = None
                                if not known_features_profile_file:
                                    try:
                                        features_profile_files[features_profile_file] = host_features_profile_files[features_profile_file]
                                    except Exception as e:
                                        logger.error('error :: metrics_manager :: sync_cluster_files - failed to add features_profile_file %s to features_profile_files - %s' % (
                                            str(features_profile_file), e))
                    if len(features_profile_files) > 0:
                        logger.info('%s features profile files found to download from %s to %s' % (
                            str(len(features_profile_files)), remote_skyline_instance[0],
                            features_profile_dir))
                        # break
                    else:
                        logger.info('%s features profile files found to download from %s to %s' % (
                            str(len(features_profile_files)), remote_skyline_instance[0],
                            features_profile_dir))
                    if features_profile_files and FAKE_CLUSTER_SYNC:
                        features_profile_files = {}
                        logger.info('FAKE_CLUSTER_SYNC reseting features_profile_files with %s items found' % str(len(features_profile_files)))
                    if FAKE_CLUSTER_SYNC and not features_profile_files:
                        logger.info('FAKE_CLUSTER_SYNC no features_profile_files found')
                    if FAKE_CLUSTER_SYNC:
                        try:
                            self.redis_conn.hset(
                                'analyzer.metrics_manager.local_features_profile_dirs',
                                expected_features_profile_dir, expected_dir)
                            logger.info('metrics_manager :: %s exists locally, added to analyzer.metrics_manager.local_features_profile_dirs' % expected_dir)
                            continue
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: metrics_manager :: failed to add entry in analyzer.metrics_manager.local_features_profile_dirs for - %s' % str(expected_dir))
                            features_profile_files = {}
                        continue

                    if features_profile_files:
                        files_to_fetch = len(features_profile_files)
                        files_fetched = 0
                        files_present = 0
                        logger.info('metrics_manager :: sync_cluster_files - %s features_profile files to fetch for fp_id %s from %s' % (
                            str(files_to_fetch), str(fp_id),
                            str(remote_skyline_instance[0])))
                        data_required = 'file_saved'
                        for remote_file in features_profile_files:
                            try:
                                data_file = features_profile_files[remote_file]
                                data_dir = os.path.dirname(data_file)
                                if not os.path.exists(data_dir):
                                    mkdir_p(data_dir)
                                if not os.path.exists(data_dir):
                                    logger.error('error :: metrics_manager :: sync_cluster_files - failed to create dir - %s' % data_dir)
                                    continue
                                endpoint = 'ionosphere_file?file=%s' % str(data_file)
                                # Only fetch it if it does not exist
                                if not os.path.isfile(data_file):
                                    data_required = 'file_saved'
                                    file_saved = False
                                    file_saved = self.get_remote_data(remote_skyline_instance, data_required, endpoint, data_file)
                                    if not file_saved:
                                        logger.error('error :: metrics_manager :: sync_cluster_files - failed to get %s from %s' % (
                                            data_file, str(remote_skyline_instance[0])))
                                        continue
                                    else:
                                        logger.info('metrics_manager :: sync_cluster_files - got %s from %s' % (
                                            data_file, str(remote_skyline_instance[0])))
                                        files_fetched += 1
                                else:
                                    files_present += 1
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: sync_cluster_files - failed to get %s from %s' % (
                                    endpoint, str(remote_skyline_instance[0])))
                        if (files_fetched + files_present) == files_to_fetch:
                            logger.info('metrics_manager :: sync_cluster_files - got all %s features_profile files that needed to be fetch (%s were already present) for %s from %s' % (
                                str(files_to_fetch), str(files_present), str(fp_id),
                                str(remote_skyline_instance[0])))
                            try:
                                self.redis_conn.hset(
                                    'analyzer.metrics_manager.local_features_profile_dirs',
                                    expected_features_profile_dir, expected_dir)
                                logger.info('metrics_manager :: %s features profile dir exists locally, added to analyzer.metrics_manager.local_features_profile_dirs' % str(expected_dir))
                                continue
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: failed to add entry in analyzer.metrics_manager.local_features_profile_dirs for - %s' % str(expected_dir))
                            try:
                                self.redis_conn.hset(
                                    'analyzer.metrics_manager.local_features_profile_dirs.to_update',
                                    expected_features_profile_dir, expected_dir)
                                logger.info('metrics_manager :: %s features profile, added to analyzer.metrics_manager.local_features_profile_dirs.to_update' % str(expected_dir))
                                continue
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: failed to add entry in analyzer.metrics_manager.local_features_profile_dirs for - %s' % str(expected_dir))
                            in_local_features_profile_dirs_to_update = False
                            try:
                                for local_features_profile_dir_to_update in local_features_profile_dirs_to_update:
                                    if local_features_profile_dir_to_update == expected_features_profile_dir:
                                        in_local_features_profile_dirs_to_update = True
                                        try:
                                            self.redis_conn.hdel(
                                                'analyzer.metrics_manager.local_features_profile_dirs.to_update',
                                                expected_features_profile_dir, expected_dir)
                                            logger.info('metrics_manager :: %s features profile, removed from analyzer.metrics_manager.local_features_profile_dirs.to_update' % str(expected_dir))
                                            continue
                                        except:
                                            logger.error(traceback.format_exc())
                                            logger.error('error :: metrics_manager :: failed to add entry in analyzer.metrics_manager.local_features_profile_dirs for - %s' % str(expected_dir))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: failed to determine if entry should be remove from in analyzer.metrics_manager.local_features_profile_dirs.to_update')
                            if not in_local_features_profile_dirs_to_update:
                                try:
                                    self.redis_conn.hset(
                                        'analyzer.metrics_manager.local_features_profile_dirs.to_update',
                                        expected_features_profile_dir, expected_dir)
                                    logger.info('metrics_manager :: %s features profile, added to analyzer.metrics_manager.local_features_profile_dirs.to_update' % str(expected_dir))
                                    continue
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: metrics_manager :: failed to add entry in analyzer.metrics_manager.local_features_profile_dirs for - %s' % str(expected_dir))
                        else:
                            logger.error('error :: metrics_manager :: sync_cluster_files - failed to get all %s features_profile files for %s from %s' % (
                                str(files_to_fetch), str(fp_id), str(remote_skyline_instance[0])))
                        if files_fetched > 0:
                            fps_fetched += 1

        logger.info('metrics_manager :: features_profiles_dir done')

        spin_end = time() - spin_start
        logger.info('metrics_manager :: sync_cluster_files took %.2f seconds' % spin_end)
        return

    def metric_management_process(self, i):
        """
        Create and manage the required lists and Redis sets
        """
        spin_start = time()
        logger.info('metrics_manager :: metric_management_process started')

        last_run_timestamp = 0
        try:
            last_run_timestamp = self.redis_conn_decoded.get('analyzer.metrics_manager.last_run_timestamp')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: failed to generate a list from %s Redis set' % full_uniques)
            last_run_timestamp = 0
        if last_run_timestamp:
            logger.info('metrics_manager :: analyzer.metrics_manager.last_run_timestamp Redis key has not expired, not running')
            return

        unique_metrics = []
        try:
            unique_metrics = list(self.redis_conn_decoded.smembers(full_uniques))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: failed to generate a list from %s Redis set' % full_uniques)
            unique_metrics = []

        # Check if this process is unnecessary
        if len(unique_metrics) == 0:
            logger.error('error :: metrics_manager :: there are no metrics in %s Redis set' % full_uniques)
            return

        ####
        # Check whether any alert settings or metrics have been changed, added
        # or removed.  If so do a full refresh.
        ####
        refresh_redis_alert_sets = False

        ####
        # Create a list of base_names from the unique_metrics
        ####

        # @added 20200723 - Feature #3560: External alert config
        # Speed this up only check alerters if not already in the set
        unique_base_names = []
        logger.info('metrics_manager :: creating unique_base_names list')
        for metric_name in unique_metrics:
            # @added 20191014 - Bug #3266: py3 Redis binary objects not strings
            #                   Branch #3262: py3
            if python_version == 3:
                metric_name = str(metric_name)
            # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
            # base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
            if metric_name.startswith(settings.FULL_NAMESPACE):
                base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
            else:
                base_name = metric_name

            # @added 20200723 - Feature #3560: External alert config
            # Speed this up only check alerters if not already in the set
            # metric_in_smtp_alerters_set = False
            unique_base_names.append(base_name)
        logger.info('metrics_manager :: created unique_base_names list of %s metrics' % str(len(unique_base_names)))

        # @added 20210308 - Feature #3978: luminosity - classify_metrics
        #                  Feature #3642: Anomaly type classification
        # Make a Redis set of unique_base_names for other apps to use
        if unique_base_names:
            unique_base_names = list(set(unique_base_names))
            logger.info('metrics_manager :: recreating the analyzer.unique_base_names set')
            try:
                self.redis_conn.sadd('new_analyzer.unique_base_names', *set(unique_base_names))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to add multiple members to the new_analyzer.unique_base_names Redis set')
            try:
                self.redis_conn.delete('new_analyzer.unique_base_names.old')
            except:
                pass
            try:
                self.redis_conn.rename('analyzer.unique_base_names', 'analyzer.unique_base_names.old')
            except:
                pass
            try:
                self.redis_conn.rename('new_analyzer.unique_base_names', 'analyzer.unique_base_names')
            except:
                pass
            try:
                self.redis_conn.delete('unique_base_names.old')
            except:
                pass
            try:
                self.redis_conn.sunionstore('aet.analyzer.unique_base_names', 'analyzer.unique_base_names')
                logger.info('metrics_manager :: copied Redis set analyzer.unique_base_names to aet.analyzer.unique_base_names via sunion')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to copy Redis set analyzer.unique_base_names to aet.analyzer.unique_base_names via sunion')

        #####
        # Check whether any internal or external alert settings have been changed
        # if so do a full refresh
        ####

        # @added 20200528 - Feature #3560: External alert config
        external_alerts = {}
        external_from_cache = None
        internal_alerts = {}
        internal_from_cache = None
        all_alerts = list(settings.ALERTS)
        all_from_cache = None
        if EXTERNAL_ALERTS:
            try:
                external_alerts, external_from_cache, internal_alerts, internal_from_cache, all_alerts, all_from_cache = get_external_alert_configs(skyline_app)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: could not determine external alert configs')
            logger.info('metrics_manager :: retrieved %s external_alerts configurations from_cache %s, %s internal_alerts from_cache %s and %s all_alerts from_cache %s' % (
                str(len(external_alerts)), str(external_from_cache),
                str(len(internal_alerts)), str(internal_from_cache),
                str(len(all_alerts)), str(all_from_cache)))
            if LOCAL_DEBUG:
                logger.debug('debug :: metrics_manager :: all_alerts :: %s' % str(all_alerts))
        if not all_alerts:
            logger.error('error :: metrics_manager :: all_alerts is not set, so creating from settings.ALERTS')
            all_alerts = list(settings.ALERTS)

        # If there was a last known alerts configuration compare it to the
        # current known alerts configuration if they are different do a full
        # refresh

        # @added 20201017 - Feature #3788: snab_flux_load_test
        #                   Feature #3560: External alert config
        last_all_alerts_set = None
        try:
            last_all_alerts_data = self.redis_conn_decoded.get('analyzer.last_all_alerts')
            if last_all_alerts_data:
                last_all_alerts = literal_eval(last_all_alerts_data)
                # A normal sorted nor set can be used as the list has dicts in it
                last_all_alerts_set = sorted(last_all_alerts, key=lambda item: item[0])
                logger.info('metrics_manager :: last_all_alerts_set from analyzer.last_all_alerts Redis set has %s items' % str(len(last_all_alerts_set)))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: failed to generate a list from the analyzer.last_all_alerts Redis key')
            last_all_alerts_set = None
        all_alerts_set = None
        if all_alerts:
            try:
                all_alerts_list = [list(row) for row in all_alerts]
                # A normal sorted nor set can be used as the list has dicts in it
                all_alerts_set = sorted(all_alerts_list, key=lambda item: item[0])
                logger.info('metrics_manager :: all_alerts_set from all_alerts has %s items' % str(len(all_alerts_set)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to create a sorted list from all_alerts object of type %s' % str(type(all_alerts_list)))

            # Set the last known alert configuration to the current configuration
            try:
                self.redis_conn.set('analyzer.last_all_alerts', str(all_alerts_set))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to set analyzer.last_all_alerts Redis key')

        # Compare the last known with the current, if there was a last known
        # configuration, if different do a full refresh
        if last_all_alerts_set:
            if str(all_alerts_set) != str(last_all_alerts_set):
                logger.info('metrics_manager :: alert settings have changed, sets will be refreshed')
                refresh_redis_alert_sets = True

        # Compare the current unique_metrics to the last smtp_alerter_metrics +
        # non_smtp_alerter_metrics, if they have changed do a full refresh
        if not refresh_redis_alert_sets:
            smtp_alerter_metrics = []
            non_smtp_alerter_metrics = []
            try:
                smtp_alerter_metrics = list(self.redis_conn_decoded.smembers('analyzer.smtp_alerter_metrics'))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to get list from analyzer.smtp_alerter_metrics Redis key')
                refresh_redis_alert_sets = True
                smtp_alerter_metrics = None
            try:
                non_smtp_alerter_metrics = list(self.redis_conn_decoded.smembers('analyzer.non_smtp_alerter_metrics'))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to get list from analyzer.non_smtp_alerter_metrics Redis key')
                non_smtp_alerter_metrics = None
            known_alerter_metrics_set = None
            if smtp_alerter_metrics or non_smtp_alerter_metrics:
                try:
                    known_alerter_metrics = smtp_alerter_metrics + non_smtp_alerter_metrics
                    known_alerter_metrics_set = set(known_alerter_metrics)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to get list from analyzer.non_smtp_alerter_metrics Redis key')

            # Compare known metrics to current unique_base_names if they are
            # different do a full refresh
            if known_alerter_metrics_set:
                changed_metrics = []
                try:
                    unique_base_names_set = set(list(unique_base_names))
                    if unique_base_names_set == known_alerter_metrics_set:
                        logger.info('metrics_manager :: unique_base_names_set and known_alerter_metrics_set are the same')
                    else:
                        set_difference = unique_base_names_set.difference(known_alerter_metrics_set)
                        for metric in set_difference:
                            changed_metrics.append(metric)
                        logger.info('metrics_manager :: there are %s metrics that have changed, sets will be refreshed' % str(len(changed_metrics)))
                        refresh_redis_alert_sets = True
                        del set_difference
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to determine hether the unique_base_names_set and known_alerter_metrics_set are different')

        smtp_alerter_metrics = []
        non_smtp_alerter_metrics = []
        mirage_metrics = []

        refresh_redis_alert_sets = True

        # @added 20201104 - Feature #3788: snab_flux_load_test
        #                   Feature #3560: External alert config
        if refresh_redis_alert_sets:
            logger.info('metrics_manager :: sets being refreshed, determining smtp_alerter_metrics')
            all_smtp_alerter_metrics = []
            all_mirage_metrics = []
            mirage_metrics_expiration_times = []
            mirage_metrics_keys = []
            start_refresh = timer()
            for base_name in unique_base_names:
                if base_name not in all_smtp_alerter_metrics:
                    # Use the all_alerts list which includes external alert configs
                    # for alert in settings.ALERTS:
                    for alert in all_alerts:
                        pattern_match = False
                        if str(alert[1]) == 'smtp':
                            try:
                                pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, [alert[0]])
                                if LOCAL_DEBUG and pattern_match:
                                    logger.debug('debug :: metrics_manager :: %s matched alert - %s' % (base_name, alert[0]))
                                try:
                                    del metric_matched_by
                                except:
                                    pass
                                if pattern_match:
                                    all_smtp_alerter_metrics.append(base_name)
                                    # @added 20160922 - Branch #922: Ionosphere
                                    # Add a Redis set of mirage.unique_metrics
                                    if settings.ENABLE_MIRAGE:
                                        mirage_metric = False
                                        try:
                                            SECOND_ORDER_RESOLUTION_FULL_DURATION = int(alert[3])
                                            if SECOND_ORDER_RESOLUTION_FULL_DURATION > 24:
                                                mirage_metric = True
                                        except:
                                            mirage_metric = False
                                        if mirage_metric:
                                            metric_name = '%s%s' % (settings.FULL_NAMESPACE, base_name)
                                            all_mirage_metrics.append(metric_name)

                                            # @added 20200805 - Task #3662: Change mirage.last_check keys to timestamp value
                                            #                   Feature #3486: analyzer_batch
                                            #                   Feature #3480: batch_processing
                                            # Add the mirage metric and its EXPIRATION_TIME to
                                            # the mirage.metrics_expiration_times so that Mirage
                                            # can determine the metric EXPIRATION_TIME without
                                            # having to create and iterate the all_alerts
                                            # object in the Mirage analysis phase so that the
                                            # reported anomaly timestamp can be used to determine
                                            # whether the EXPIRATION_TIME should be applied to a
                                            # batch metric in the alerting and Ionosphere contexts
                                            # mirage_alert_expiration_data = [base_name, int(alert[2])]
                                            mirage_alert_expiration_data = [base_name, int(alert[2])]
                                            mirage_metrics_expiration_times.append(mirage_alert_expiration_data)

                                            # @added 20200904 - Task #3730: Validate Mirage running multiple processes
                                            # Also always add the mirage.metrics Redis key for the
                                            # metric which contains its hours_to_resolve so
                                            # that the spin_process can add the mirage check
                                            # files immediately, rather than waiting to add
                                            # the mirage checks all in the alerting phase.
                                            # This is done to reduce the time it takes to
                                            # get through the analysis pipeline.
                                            mirage_metrics_keys.append([base_name, int(alert[2]), SECOND_ORDER_RESOLUTION_FULL_DURATION])
                                    break
                            except:
                                pattern_match = False
            end_classify = timer()
            logger.info('metrics_manager :: classifying metrics took %.6f seconds' % (end_classify - start_refresh))

            logger.info('metrics_manager :: %s all_smtp_alerter_metrics were determined' % str(len(all_smtp_alerter_metrics)))
            if all_smtp_alerter_metrics:
                smtp_alerter_metrics = list(set(list(all_smtp_alerter_metrics)))
                logger.info('metrics_manager :: %s unique smtp_alerter_metrics determined' % str(len(smtp_alerter_metrics)))
            # Recreate the Redis set the analyzer.smtp_alerter_metrics
            if smtp_alerter_metrics:
                logger.info('metrics_manager :: recreating the analyzer.smtp_alerter_metrics Redis set')
                try:
                    self.redis_conn.sadd('new_analyzer.smtp_alerter_metrics', *set(smtp_alerter_metrics))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to add multiple members to the new_analyzer.smtp_alerter_metrics Redis set')
                try:
                    self.redis_conn.delete('analyzer.smtp_alerter_metrics.old')
                except:
                    pass
                try:
                    self.redis_conn.rename('analyzer.smtp_alerter_metrics', 'analyzer.smtp_alerter_metrics.old')
                except:
                    pass
                try:
                    # @added 20180423 - Feature #2360: CORRELATE_ALERTS_ONLY
                    #                   Branch #2270: luminosity
                    # Add a Redis set of smtp_alerter_metrics for Luminosity to only
                    # cross correlate on metrics with an alert setting
                    self.redis_conn.rename('new_analyzer.smtp_alerter_metrics', 'analyzer.smtp_alerter_metrics')
                except:
                    pass
                try:
                    self.redis_conn.delete('analyzer.smtp_alerter_metrics.old')
                except:
                    pass
                logger.info('metrics_manager :: recreated the analyzer.smtp_alerter_metrics Redis set')

            logger.info('metrics_manager :: determing non_smtp_alerter_metrics')
            try:
                unique_base_names_set = set(list(unique_base_names))
                smtp_alerter_metrics_set = set(list(smtp_alerter_metrics))
                if unique_base_names_set == smtp_alerter_metrics_set:
                    logger.info('metrics_manager :: unique_base_names_set and smtp_alerter_metrics_set are the same, no non_smtp_alerter_metrics')
                else:
                    set_difference = unique_base_names_set.difference(smtp_alerter_metrics_set)
                    for metric in set_difference:
                        non_smtp_alerter_metrics.append(metric)
                    logger.info('metrics_manager :: there are %s non_alerter_metrics' % str(len(non_smtp_alerter_metrics)))
                    del set_difference
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to determine non_smtp_alerter_metrics from sets')

            # Recreate the Redis set the analyzer.non_smtp_alerter_metrics
            if non_smtp_alerter_metrics:
                logger.info('metrics_manager :: recreating the analyzer.non_smtp_alerter_metrics Redis set')
                try:
                    self.redis_conn.sadd('new_analyzer.non_smtp_alerter_metrics', *set(non_smtp_alerter_metrics))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to add multiple members to the new_analyzer.non_smtp_alerter_metrics Redis set')
                try:
                    self.redis_conn.delete('analyzer.non_smtp_alerter_metrics.old')
                except:
                    pass
                try:
                    self.redis_conn.rename('analyzer.non_smtp_alerter_metrics', 'analyzer.non_smtp_alerter_metrics.old')
                except:
                    pass
                try:
                    self.redis_conn.rename('new_analyzer.non_smtp_alerter_metrics', 'analyzer.non_smtp_alerter_metrics')
                except:
                    pass
                try:
                    self.redis_conn.delete('analyzer.non_smtp_alerter_metrics.old')
                except:
                    pass
                logger.info('metrics_manager :: recreated the analyzer.non_smtp_alerter_metrics Redis set')

            try:
                self.redis_conn.sunionstore('aet.analyzer.smtp_alerter_metrics', 'analyzer.smtp_alerter_metrics')
                logger.info('metrics_manager :: copied Redis set analyzer.smtp_alerter_metrics to aet.analyzer.smtp_alerter_metrics via sunion')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to copy Redis set analyzer.smtp_alerter_metrics to aet.analyzer.smtp_alerter_metrics via sunion')
            try:
                self.redis_conn.sunionstore('aet.analyzer.non_smtp_alerter_metrics', 'analyzer.non_smtp_alerter_metrics')
                logger.info('metrics_manager :: copied Redis set analyzer.non_smtp_alerter_metrics to aet.analyzer.non_smtp_alerter_metrics via sunion')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to copy Redis set analyzer.non_smtp_alerter_metrics to aet.analyzer.non_smtp_alerter_metrics via sunion')

            logger.info('metrics_manager :: %s mirage metrics determined' % str(len(all_mirage_metrics)))
            if all_mirage_metrics:
                mirage_metrics = list(set(list(all_mirage_metrics)))
                logger.info('metrics_manager :: %s unique mirage_metrics determined' % str(len(mirage_metrics)))

            # Recreate the Redis set the mirage.unique_metrics
            if mirage_metrics:
                logger.info('metrics_manager :: recreating the mirage.unique_metrics Redis set')
                try:
                    self.redis_conn.sadd('new_mirage.unique_metrics', *set(mirage_metrics))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to add multiple members to the new_mirage.unique_metrics Redis set')
                try:
                    self.redis_conn.delete('mirage.unique_metrics.old')
                except:
                    pass
                try:
                    self.redis_conn.rename('mirage.unique_metrics', 'mirage.unique_metrics.old')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to rename Redis set mirage.unique_metrics to mirage.unique_metrics.old')
                try:
                    self.redis_conn.rename('new_mirage.unique_metrics', 'mirage.unique_metrics')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to rename Redis set new_mirage.unique_metrics to mirage.unique_metrics')
                try:
                    self.redis_conn.delete('mirage.unique_metrics.old')
                except:
                    pass
                logger.info('metrics_manager :: recreated the mirage.unique_metrics Redis set')

            end_refresh = timer()
            logger.info('metrics_manager :: refresh of smtp_alerter_metrics, non_smtp_alerter_metrics and mirage_metrics took %.6f seconds' % (end_refresh - start_refresh))

            if mirage_metrics_expiration_times:
                logger.info('metrics_manager :: managing mirage.hash_key.metrics_expiration_times Redis hash key')
                updated_keys = 0
                added_keys = 0
                removed_keys = 0
                mirage_metrics_expiration_times_errors = 0
                last_metrics_expiration_times = []
                try:
                    raw_last_metrics_expiration_times = self.redis_conn_decoded.hgetall('mirage.hash_key.metrics_expiration_times')
                    for base_name_bytes in raw_last_metrics_expiration_times:
                        base_name = str(base_name_bytes)
                        expiration_time = int(raw_last_metrics_expiration_times[base_name])
                        last_metrics_expiration_times.append([base_name, expiration_time])
                    logger.info('metrics_manager :: %s entries in mirage.hash_key.metrics_expiration_times Redis hash key' % str(len(last_metrics_expiration_times)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to get Redis hash key mirage.hash_key.metrics_expiration_times')
                    last_metrics_expiration_times = []
                # Add them all if there are none in the hash key
                if not last_metrics_expiration_times:
                    logger.info('metrics_manager :: nothing found in Redis hash key, added all %s metrics from mirage_metrics_expiration_times' % (
                        str(len(mirage_metrics_expiration_times))))
                    error_logged = False
                    for item in mirage_metrics_expiration_times:
                        try:
                            self.redis_conn.hset(
                                'mirage.hash_key.metrics_expiration_times',
                                item[0], int(item[1]))
                            added_keys += 1
                        except:
                            mirage_metrics_expiration_times_errors += 1
                            if not error_logged:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: failed to add entry in mirage.hash_key.metrics_expiration_times for - %s' % str(item))
                                error_logged = True
                    logger.info('metrics_manager :: added all %s metrics to mirage.hash_key.metrics_expiration_times Redis hash' % (
                        str(len(mirage_metrics_expiration_times))))
                # Determine the base_names in the last_metrics_expiration_times
                last_metrics_expiration_times_metrics = []
                if last_metrics_expiration_times:
                    try:
                        last_metrics_expiration_times_metrics = [item[0] for item in last_metrics_expiration_times]
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to generate list of metric names from last_metrics_expiration_times')
                if last_metrics_expiration_times_metrics:
                    logger.info('metrics_manager :: checking entries in mirage.hash_key.metrics_expiration_times Redis hash key are correct')
                    error_logged = False
                    for item in mirage_metrics_expiration_times:
                        try:
                            base_name = item[0]
                            if base_name in last_metrics_expiration_times_metrics:
                                last_expiration_time = int(raw_last_metrics_expiration_times[base_name])
                                if last_expiration_time != int(item[1]):
                                    self.redis_conn.hset(
                                        'mirage.hash_key.metrics_expiration_times',
                                        base_name, int(item[1]))
                                    updated_keys += 1
                            else:
                                self.redis_conn.hset(
                                    'mirage.hash_key.metrics_expiration_times',
                                    base_name, int(item[1]))
                                added_keys += 1
                        except:
                            mirage_metrics_expiration_times_errors += 1
                            if not error_logged:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: failed to manage entry in mirage.hash_key.metrics_expiration_times for - %s' % str(item))
                                error_logged = True
                    logger.info('metrics_manager :: checked entries in mirage.hash_key.metrics_expiration_times Redis hash key, %s updated, %s added' % (
                        str(updated_keys), str(added_keys)))
                    # Remove any metrics in no longer present
                    present_metrics_expiration_times_metrics = []
                    try:
                        present_metrics_expiration_times_metrics = [item[0] for item in mirage_metrics_expiration_times]
                        logger.info('metrics_manager :: %s current known metrics from mirage_metrics_expiration_times' % str(len(present_metrics_expiration_times_metrics)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to generate list of metric names from mirage_metrics_expiration_times')
                        present_metrics_expiration_times_metrics = None
                    if present_metrics_expiration_times_metrics:
                        logger.info('metrics_manager :: checking if any entries in mirage.hash_key.metrics_expiration_times Redis hash key need to be removed')
                        error_logged = False
                        for base_name in last_metrics_expiration_times_metrics:
                            try:
                                if base_name not in present_metrics_expiration_times_metrics:
                                    self.redis_conn.hdel(
                                        'mirage.hash_key.metrics_expiration_times',
                                        base_name)
                                    removed_keys += 1
                            except:
                                mirage_metrics_expiration_times_errors += 1
                                if not error_logged:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: metrics_manager :: failed to remove entry from mirage.hash_key.metrics_expiration_times for - %s' % str(base_name))
                                    error_logged = True
                        logger.info('metrics_manager :: removed %s entries in mirage.hash_key.metrics_expiration_times Redis hash key' % str(removed_keys))
                logger.info('metrics_manager :: managed mirage.hash_key.metrics_expiration_times Redis hash key')

            # @added 20200904 - Task #3730: Validate Mirage running multiple processes
            # Also always add the mirage.metrics Redis key for the
            # metric which contains its hours_to_resolve so
            # that the spin_process can add the mirage check
            # files immediately, rather than waiting to add
            # the mirage checks all in the alerting phase.
            # This is done to reduce the time it takes to
            # get through the analysis pipeline.
            # @modified 20201109 - Feature #3830: metrics_manager
            # Changed to a single mirage.hash_key.metrics_resolutions hash key
            # rather than individual mirage.metrics. Redis keys for each mirage
            # metric
            if mirage_metrics_keys:
                logger.info('metrics_manager :: managing the mirage.hash_key.metrics_resolutions Redis hash key')
                last_metrics_resolutions = {}
                try:
                    raw_last_metrics_resolutions = self.redis_conn_decoded.hgetall('mirage.hash_key.metrics_resolutions')
                    for base_name_bytes in raw_last_metrics_resolutions:
                        base_name = str(base_name_bytes)
                        hours_to_resolve = int(raw_last_metrics_resolutions[base_name])
                        last_metrics_resolutions[base_name] = hours_to_resolve
                    logger.info('metrics_manager :: %s entries in mirage.hash_key.metrics_resolutions Redis hash key' % str(len(last_metrics_resolutions)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to get Redis hash key mirage.hash_key.metrics_resolutions')
                    last_metrics_resolutions = {}
                logger.info('metrics_manager :: there are %s metrics in the mirage.hash_key.metrics_resolutions Redis hash key' % str(len(last_metrics_resolutions)))
                logger.info('metrics_manager :: determining if any metrics need to be removed from the mirage.hash_key.metrics_resolutions Redis hash key, via set difference')
                metrics_to_remove = []
                current_metrics_resolutions = {}
                for item in mirage_metrics_keys:
                    base_name = item[0]
                    current_metrics_resolutions[base_name] = item[2]
                if current_metrics_resolutions:
                    try:
                        last_metrics_resolutions_set = set(list(last_metrics_resolutions))
                        current_metrics_resolutions_set = set(list(current_metrics_resolutions))
                        set_difference = last_metrics_resolutions_set.difference(current_metrics_resolutions_set)
                        metrics_to_remove = set_difference
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to determine metrics to remove from mirage.hash_key.metrics_resolutions')
                        metrics_to_remove = []
                logger.info('metrics_manager :: there are %s metrics to remove from the mirage.hash_key.metrics_resolutions Redis hash key' % str(len(metrics_to_remove)))
                if metrics_to_remove:
                    metrics_to_remove_error_logged = False
                    metrics_removed = 0
                    for base_name in metrics_to_remove:
                        try:
                            self.redis_conn.hdel(
                                'mirage.hash_key.metrics_resolutions', base_name)
                            metrics_removed += 1
                        except:
                            if not metrics_to_remove_error_logged:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: failed to determine metrics to remove from mirage.hash_key.metrics_resolutions')
                                metrics_to_remove_error_logged = True
                    logger.info('metrics_manager :: removed %s metrics from the mirage.hash_key.metrics_resolutions Redis hash key' % str(metrics_removed))
                logger.info('metrics_manager :: determining if there are any new metrics to add to the mirage.hash_key.metrics_resolutions Redis hash key, via set difference')
                metrics_to_add = []
                if current_metrics_resolutions:
                    try:
                        last_metrics_resolutions_set = set(list(last_metrics_resolutions))
                        current_metrics_resolutions_set = set(list(current_metrics_resolutions))
                        set_difference = last_metrics_resolutions_set.difference(last_metrics_resolutions_set)
                        metrics_to_add = set_difference
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to determine metrics to remove from mirage.hash_key.metrics_resolutions')
                        metrics_to_add = []
                metrics_added = 0
                logger.info('metrics_manager :: there are %s metrics to add to the mirage.hash_key.metrics_resolutions Redis hash key' % str(len(metrics_to_add)))
                if metrics_to_add:
                    metrics_to_add_error_logged = False
                    for base_name in metrics_to_add:
                        try:
                            hours_to_resolve = current_metrics_resolutions[base_name]
                            self.redis_conn.hset(
                                'mirage.hash_key.metrics_resolutions', base_name,
                                hours_to_resolve)
                            metrics_added += 1
                        except Exception as e:
                            if not metrics_to_add_error_logged:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: failed to add %s to mirage.hash_key.metrics_resolutions - %s' % (base_name, e))
                                metrics_to_add_error_logged = True
                    logger.info('metrics_manager :: added %s metrics to the mirage.hash_key.metrics_resolutions Redis hash key' % str(metrics_added))

                # Update any changed metric resolutions, this is a fast iterator
                logger.info('metrics_manager :: checking if any metrics need their resolution updated in the mirage.hash_key.metrics_resolutions Redis hash key')
                metrics_resolutions_updated = 0
                metrics_updated_error_logged = False
                for base_name in current_metrics_resolutions:
                    update_metric_resolution = False
                    try:
                        last_resolution = last_metrics_resolutions[base_name]
                    except:
                        last_resolution = 0
                    try:
                        current_resolution = current_metrics_resolutions[base_name]
                    except:
                        current_resolution = 0
                    if not last_resolution:
                        update_metric_resolution = True
                        last_resolution = current_resolution
                    if last_resolution != current_resolution:
                        update_metric_resolution = True
                    if update_metric_resolution:
                        try:
                            self.redis_conn.hset(
                                'mirage.hash_key.metrics_resolutions', base_name,
                                current_resolution)
                            metrics_resolutions_updated += 1
                        except:
                            if not metrics_updated_error_logged:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: failed to update the resolution of %s to mirage.hash_key.metrics_resolutions' % base_name)
                                metrics_updated_error_logged = True
                logger.info('metrics_manager :: updated the resolutions of %s metrics in the mirage.hash_key.metrics_resolutions Redis hash key' % str(metrics_resolutions_updated))

        logger.info('metrics_manager :: smtp_alerter_metrics     :: %s' % str(len(smtp_alerter_metrics)))
        logger.info('metrics_manager :: non_smtp_alerter_metrics :: %s' % str(len(non_smtp_alerter_metrics)))
        logger.info('metrics_manager :: mirage_metrics :: %s' % str(len(mirage_metrics)))

        # @added 20210601 - Feature #4000: EXTERNAL_SETTINGS
        external_settings = {}
        external_from_cache = False
        if EXTERNAL_SETTINGS:
            logger.info('metrics_manager :: managing external_settings')
            try:
                external_settings, external_from_cache = manage_external_settings(skyline_app)
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: fetch_external_settings failed - %s' % (
                    e))
            if external_settings:
                logger.info('metrics_manager :: %s external_settings from cache %s' % (
                    str(len(list(external_settings.keys()))), str(external_from_cache)))

        # @added 20210406 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
        # Analyzer determines what metrics flux should aggregate by creating the
        # the flux.aggregate_metrics Redis set, which flux/listen and flux/aggregate
        # reference.  This is done in analyzer/metrics_manager because it manages
        # metric Redis sets as it always runs.  It is only managed every 5 mins.
        aggregate_namespaces = []
        flux_aggregate_metrics = []
        flux_aggregate_zerofill_metrics = []
        flux_aggregate_lkv_metrics = []
        if FLUX_AGGREGATE_NAMESPACES or FLUX_EXTERNAL_AGGREGATE_NAMESPACES:
            if FLUX_AGGREGATE_NAMESPACES:
                aggregate_namespaces = list(FLUX_AGGREGATE_NAMESPACES.keys())
            if FLUX_EXTERNAL_AGGREGATE_NAMESPACES:
                logger.info('metrics_manager :: TODO :: FLUX_EXTERNAL_AGGREGATE_NAMESPACES')
                # TODO
                # flux_external_aggregate_namespaces_dict = self.redis_conn_decoded.hgetall(...
                # for i_metric in ...:
                #      aggregate_namespaces.append(i_metric)
            manage_flux_aggregate_namespaces = False
            # Only manage every 5 mins
            manage_flux_aggregate_namespaces_redis_key = 'metrics_manager.manage_flux_aggregate_namespaces'
            try:
                manage_flux_aggregate_namespaces = self.redis_conn.get(manage_flux_aggregate_namespaces_redis_key)
            except Exception as e:
                if LOCAL_DEBUG:
                    logger.error('error :: metrics_manager :: could not query Redis for metrics_manager.manage_flux_aggregate_namespaces key: %s' % str(e))
            if not manage_flux_aggregate_namespaces:
                logger.info('metrics_manager :: managing FLUX_AGGREGATE_NAMESPACES Redis sets')
                try:
                    self.redis_conn.delete('analyzer.flux_aggregate_metrics')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to delete analyzer.flux_aggregate_metrics Redis set')

                try:
                    self.redis_conn.delete('metrics_manager.flux_zero_fill_aggregate_metrics')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to delete metrics_manager.flux_zero_fill_aggregate_metrics Redis set')
                try:
                    self.redis_conn.delete('metrics_manager.flux_last_known_value_aggregate_metrics')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to delete metrics_manager.flux_last_known_value_aggregate_metrics Redis set')

                for i_base_name in unique_base_names:
                    pattern_match, metric_matched_by = matched_or_regexed_in_list('analyzer', i_base_name, aggregate_namespaces)
                    if pattern_match:
                        flux_aggregate_metrics.append(i_base_name)
                        matched_namespace = metric_matched_by['matched_namespace']
                        metric_aggregation_settings = FLUX_AGGREGATE_NAMESPACES[matched_namespace]
                        # Add the configuration to Redis
                        self.redis_conn.hset('metrics_manager.flux.aggregate_namespaces.settings', i_base_name, str(metric_aggregation_settings))
                        # @added 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                        # Allow for zero_fill and last_known_value setting to be
                        # defined in the FLUX_AGGREGATE_NAMESPACES setting
                        add_to_zero_fill = False
                        try:
                            add_to_zero_fill = metric_aggregation_settings['zero_fill']
                        except:
                            add_to_zero_fill = False
                        if add_to_zero_fill:
                            flux_aggregate_zerofill_metrics.append(i_base_name)
                        add_to_last_known_value = False
                        try:
                            add_to_last_known_value = metric_aggregation_settings['last_known_value']
                        except:
                            add_to_last_known_value = False
                        if add_to_last_known_value:
                            flux_aggregate_lkv_metrics.append(i_base_name)
                if flux_aggregate_zerofill_metrics:
                    try:
                        logger.info('metrics_manager :: adding %s aggregate metrics that have zerofill aggregation setting to metrics_manager.flux_zero_fill_aggregate_metrics' % str(len(flux_aggregate_zerofill_metrics)))
                        self.redis_conn.sadd('metrics_manager.flux_zero_fill_aggregate_metrics', *set(flux_aggregate_zerofill_metrics))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to add multiple aggregation metric members to the metrics_manager.flux_zero_fill_aggregate_metrics Redis set')
                if flux_aggregate_lkv_metrics:
                    try:
                        logger.info('metrics_manager :: adding %s aggregate metrics that have last_known_value aggregation setting to metrics_manager.flux_last_known_value_aggregate_metrics' % str(len(flux_aggregate_lkv_metrics)))
                        self.redis_conn.sadd('metrics_manager.flux_last_known_value_aggregate_metrics', *set(flux_aggregate_lkv_metrics))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to add multiple members to the metrics_manager.flux_last_known_value_aggregate_metrics Redis set')
                if flux_aggregate_metrics:
                    logger.info('metrics_manager :: popuating analyzer.flux_aggregate_metrics Redis set with %s metrics' % str(len(flux_aggregate_metrics)))
                    try:
                        self.redis_conn.sadd('analyzer.flux_aggregate_metrics', *set(flux_aggregate_metrics))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to add multiple members to the analyzer.flux_aggregate_metrics Redis set')
                try:
                    key_timestamp = int(time())
                    self.redis_conn.setex(manage_flux_aggregate_namespaces_redis_key, RUN_EVERY, key_timestamp)
                except:
                    logger.error('error :: metrics_manager :: failed to set key :: manage_flux_aggregate_namespaces_redis_key' % manage_flux_aggregate_namespaces_redis_key)
                logger.info('metrics_manager :: checking if any metrics need to be removed from analyzer.flux_aggregate_metrics')
                flux_aggregate_metrics_to_remove = []
                flux_aggregate_metrics_list = []
                try:
                    flux_aggregate_metrics_list = list(self.redis_conn_decoded.smembers('analyzer.flux_aggregate_metrics'))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to generate a list from analyzer.flux_aggregate_metrics Redis set')
                for flux_aggregate_base_name in flux_aggregate_metrics_list:
                    if flux_aggregate_base_name not in unique_base_names:
                        flux_aggregate_metrics_to_remove.append(flux_aggregate_base_name)
                if flux_aggregate_metrics_to_remove:
                    try:
                        logger.info('metrics_manager :: removing %s metrics from analyzer.flux_aggregate_metrics' % str(len(flux_aggregate_metrics_to_remove)))
                        self.redis_conn.srem('analyzer.flux_aggregate_metrics', *set(flux_aggregate_metrics_to_remove))
                        # Reload the new set
                        try:
                            flux_aggregate_metrics_list = list(self.redis_conn_decoded.smembers('analyzer.flux_aggregate_metrics'))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: metrics_manager :: failed to generate a list from analyzer.flux_aggregate_metrics Redis set after removals')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to add multiple members to the analyzer.flux_aggregate_metrics Redis set')
                else:
                    logger.info('metrics_manager :: no metrics need to remove from analyzer.flux_aggregate_metrics')
                if flux_aggregate_metrics_list:
                    # Replace the existing flux.aggregate_metrics Redis set
                    try:
                        self.redis_conn.sunionstore('metrics_manager.flux.aggregate_metrics', 'analyzer.flux_aggregate_metrics')
                        logger.info('metrics_manager :: replaced metrics_manager.flux.aggregate_metrics Redis set with the newly created analyzer.flux_aggregate_metrics set')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to sunionstore metrics_manager.flux.aggregate_metrics from analyzer.flux_aggregate_metrics Redis sets')
                    # Create a Redis hash for flux/listen.  These entries are
                    # managed flux/listen, if the timestamp for an entry is
                    # older than 12 hours flux/listen removes it from the hash
                    time_now = int(time())
                    for flux_aggregate_metric in flux_aggregate_metrics_list:
                        try:
                            self.redis_conn.hset(
                                'metrics_manager.flux.aggregate_metrics.hash', flux_aggregate_metric,
                                time_now)
                        except:
                            pass

        # @added 20200827 - Feature #3708: FLUX_ZERO_FILL_NAMESPACES
        # Analyzer determines what metrics flux should 0 fill by creating
        # the flux.zero_fill_metrics Redis set, which flux references.  This
        # is done in Analyzer because it manages metric Redis sets as it
        # always runs.  It is only managed in Analyzer every 5 mins.
        # @modified 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
        # if FLUX_ZERO_FILL_NAMESPACES:
        if FLUX_ZERO_FILL_NAMESPACES or flux_aggregate_zerofill_metrics:
            manage_flux_zero_fill_namespaces = False
            flux_zero_fill_metrics = []
            # Only manage every 5 mins
            manage_flux_zero_fill_namespaces_redis_key = 'metrics_manager.manage_flux_zero_fill_namespaces'
            try:
                manage_flux_zero_fill_namespaces = self.redis_conn.get(manage_flux_zero_fill_namespaces_redis_key)
            except Exception as e:
                if LOCAL_DEBUG:
                    logger.error('error :: metrics_manager :: could not query Redis for metrics_manager.manage_flux_zero_fill_namespaces key: %s' % str(e))
            if not manage_flux_zero_fill_namespaces:
                logger.info('metrics_manager :: managing FLUX_ZERO_FILL_NAMESPACES Redis sets')
                try:
                    self.redis_conn.delete('analyzer.flux_zero_fill_metrics')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to delete analyzer.flux_zero_fill_metrics Redis set')
                # @added 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                if flux_aggregate_zerofill_metrics:
                    for i_base_name in flux_aggregate_zerofill_metrics:
                        flux_zero_fill_metrics.append(i_base_name)
                    logger.info('metrics_manager :: added %s flux_aggregate_zerofill_metrics to add to Redis sets' % str(len(flux_aggregate_zerofill_metrics)))

                for i_base_name in unique_base_names:
                    flux_zero_fill_metric = False
                    pattern_match, metric_matched_by = matched_or_regexed_in_list('analyzer', i_base_name, FLUX_ZERO_FILL_NAMESPACES)
                    if pattern_match:
                        flux_zero_fill_metric = True
                    if flux_zero_fill_metric:
                        flux_zero_fill_metrics.append(i_base_name)
                if flux_zero_fill_metrics:
                    logger.info('metrics_manager :: popuating analyzer.flux_zero_fill_metrics Redis set with %s metrics' % str(len(flux_zero_fill_metrics)))
                    try:
                        self.redis_conn.sadd('analyzer.flux_zero_fill_metrics', *set(flux_zero_fill_metrics))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to add multiple members to the analyzer.flux_zero_fill_metrics Redis set')
                try:
                    key_timestamp = int(time())
                    self.redis_conn.setex(manage_flux_zero_fill_namespaces_redis_key, RUN_EVERY, key_timestamp)
                except:
                    logger.error('error :: metrics_manager :: failed to set key :: manage_flux_zero_fill_namespaces_redis_key' % manage_flux_zero_fill_namespaces_redis_key)
                logger.info('metrics_manager :: checking if any metrics need to be removed from analyzer.flux_zero_fill_metrics')
                flux_zero_fill_metrics_to_remove = []
                flux_zero_fill_metrics_list = []
                try:
                    flux_zero_fill_metrics_list = list(self.redis_conn_decoded.smembers('analyzer.flux_zero_fill_metrics'))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to generate a list from analyzer.flux_zero_fill_metrics Redis set')
                for flux_zero_fill_base_name in flux_zero_fill_metrics_list:
                    if flux_zero_fill_base_name not in unique_base_names:
                        flux_zero_fill_metrics_to_remove.append(flux_zero_fill_base_name)
                if flux_zero_fill_metrics_to_remove:
                    try:
                        logger.info('metrics_manager :: removing %s metrics from smtp_alerter_metrics' % str(len(flux_zero_fill_metrics_to_remove)))
                        self.redis_conn.srem('analyzer.flux_zero_fill_metrics', *set(flux_zero_fill_metrics_to_remove))
                        # Reload the new set
                        try:
                            flux_zero_fill_metrics_list = list(self.redis_conn_decoded.smembers('analyzer.flux_zero_fill_metrics'))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: metrics_manager :: failed to generate a list from analyzer.flux_zero_fill_metrics Redis set after removals')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to add multiple members to the analyzer.flux_zero_fill_metrics Redis set')
                else:
                    logger.info('metrics_manager :: no metrics need to remove from analyzer.flux_zero_fill_metrics')
                if flux_zero_fill_metrics_list:
                    # Replace the existing flux.zero_fill_metrics Redis set
                    try:
                        self.redis_conn.sunionstore('flux.zero_fill_metrics', 'analyzer.flux_zero_fill_metrics')
                        logger.info('metrics_manager :: replaced flux.zero_fill_metrics Redis set with the newly created analyzer.flux_zero_fill_metrics set')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to sunionstore flux.zero_fill_metrics from analyzer.flux_zero_fill_metrics Redis sets')

        # @added 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
        # Analyzer determines what metrics flux send the last known value for by
        # creating
        # the flux.last_known_value_metrics Redis set, which flux references.  This
        # is done in Analyzer because it manages metric Redis sets as it
        # always runs.  It is only managed in Analyzer every 5 mins.
        if FLUX_LAST_KNOWN_VALUE_NAMESPACES or flux_aggregate_lkv_metrics:
            manage_flux_last_known_value_namespaces = False
            flux_last_known_value_metrics = []
            # Only manage every 5 mins
            manage_flux_last_known_value_namespaces_redis_key = 'metrics_manager.manage_flux_last_known_value_namespaces'
            try:
                manage_flux_last_known_value_namespaces = self.redis_conn.get(manage_flux_last_known_value_namespaces_redis_key)
            except Exception as e:
                if LOCAL_DEBUG:
                    logger.error('error :: metrics_manager :: could not query Redis for metrics_manager.manage_flux_last_known_value_namespaces key: %s' % str(e))
            if not manage_flux_last_known_value_namespaces:
                logger.info('metrics_manager :: managing FLUX_LAST_KNOWN_VALUE_NAMESPACES Redis sets')
                try:
                    self.redis_conn.delete('analyzer.flux_last_known_value_metrics')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to delete analyzer.flux_last_known_value_metrics Redis set')
                if flux_aggregate_lkv_metrics:
                    for i_base_name in flux_aggregate_lkv_metrics:
                        flux_last_known_value_metrics.append(i_base_name)
                    logger.info('metrics_manager :: added %s flux_aggregate_lkv_metrics to add to Redis sets' % str(len(flux_aggregate_lkv_metrics)))

                for i_base_name in unique_base_names:
                    flux_last_known_value_metric = False
                    pattern_match, metric_matched_by = matched_or_regexed_in_list('analyzer', i_base_name, FLUX_LAST_KNOWN_VALUE_NAMESPACES)
                    if pattern_match:
                        flux_last_known_value_metric = True
                    if flux_last_known_value_metric:
                        flux_last_known_value_metrics.append(i_base_name)
                if flux_last_known_value_metrics:
                    logger.info('metrics_manager :: popuating analyzer.flux_last_known_value_metrics Redis set with %s metrics' % str(len(flux_last_known_value_metrics)))
                    try:
                        self.redis_conn.sadd('analyzer.flux_last_known_value_metrics', *set(flux_last_known_value_metrics))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to add multiple members to the analyzer.flux_last_known_value_metrics Redis set')
                try:
                    key_timestamp = int(time())
                    self.redis_conn.setex(manage_flux_last_known_value_namespaces_redis_key, RUN_EVERY, key_timestamp)
                except:
                    logger.error('error :: metrics_manager :: failed to set key :: manage_flux_last_known_value_namespaces_redis_key' % manage_flux_last_known_value_namespaces_redis_key)
                logger.info('metrics_manager :: checking if any metrics need to be removed from analyzer.flux_last_known_value_metrics')
                flux_last_known_value_metrics_to_remove = []
                flux_last_known_value_metrics_list = []
                try:
                    flux_last_known_value_metrics_list = list(self.redis_conn_decoded.smembers('analyzer.flux_last_known_value_metrics'))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to generate a list from analyzer.flux_last_known_value_metrics Redis set')
                for flux_last_known_value_base_name in flux_last_known_value_metrics_list:
                    if flux_last_known_value_base_name not in unique_base_names:
                        flux_last_known_value_metrics_to_remove.append(flux_last_known_value_base_name)
                if flux_last_known_value_metrics_to_remove:
                    try:
                        logger.info('metrics_manager :: removing %s metrics from analyzer.flux_last_known_value_metrics' % str(len(flux_last_known_value_metrics_to_remove)))
                        self.redis_conn.srem('analyzer.flux_last_known_value_metrics', *set(flux_last_known_value_metrics_to_remove))
                        # Reload the new set
                        try:
                            flux_last_known_value_metrics_list = list(self.redis_conn_decoded.smembers('analyzer.flux_last_known_value_metrics'))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: metrics_manager :: failed to generate a list from analyzer.flux_last_known_value_metrics Redis set after removals')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to add multiple members to the analyzer.flux_last_known_value_metrics Redis set')
                else:
                    logger.info('metrics_manager :: no metrics need to remove from analyzer.flux_last_known_value_metrics')
                if flux_last_known_value_metrics_list:
                    # Replace the existing flux.last_known_value_metrics Redis set
                    try:
                        self.redis_conn.sunionstore('flux.last_known_value_metrics', 'analyzer.flux_last_known_value_metrics')
                        logger.info('metrics_manager :: replaced flux.last_known_value_metrics Redis set with the newly created analyzer.flux_last_known_value_metrics set')
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to sunionstore flux.last_known_value_metrics from analyzer.flux_last_known_value_metrics Redis sets - %s' % e)

        # @added 20210513 - Feature #4068: ANALYZER_SKIP
        analyzer_skip_metrics = []
        if ANALYZER_SKIP:
            logger.info('metrics_manager :: determining ANALYZER_SKIP metrics')
            for i_metric in unique_metrics:
                pattern_match, metric_matched_by = matched_or_regexed_in_list('analyzer', i_metric, ANALYZER_SKIP)
                if pattern_match:
                    analyzer_skip_metrics.append(i_metric)
        if analyzer_skip_metrics:
            logger.info('metrics_manager :: adding %s metrics to analyzer.metrics_manager.analyzer_skip' % str(len(analyzer_skip_metrics)))
            try:
                self.redis_conn.sadd('new.analyzer.metrics_manager.analyzer_skip', *set(analyzer_skip_metrics))
                self.redis_conn.sunionstore('analyzer.metrics_manager.analyzer_skip', 'new.analyzer.metrics_manager.analyzer_skip')
                logger.info('metrics_manager :: replaced analyzer.metrics_manager.analyzer_skip Redis set with the newly created new.analyzer.metrics_manager.analyzer_skip set')
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to sunionstore analyzer.metrics_manager.analyzer_skip from new.analyzer.metrics_manager.analyzer_skip Redis sets - %s' % e)
            del analyzer_skip_metrics

        # @added 20210519 - Feature #4076: CUSTOM_STALE_PERIOD
        custom_stale_periods = []
        custom_stale_period_metrics = []
        default_metric_stale_period = int(settings.STALE_PERIOD)
        if not CUSTOM_STALE_PERIOD:
            logger.info('metrics_manager :: no CUSTOM_STALE_PERIOD metrics defined')
        if CUSTOM_STALE_PERIOD:
            logger.info('metrics_manager :: determining known CUSTOM_STALE_PERIOD metrics from analyzer.metrics_manager.custom_stale_period')
            custom_stale_metrics_hash_key = 'analyzer.metrics_manager.custom_stale_periods'
            known_custom_stale_metrics_dict = {}
            try:
                known_custom_stale_metrics_dict = self.redis_conn_decoded.hgetall(custom_stale_metrics_hash_key)
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to create known_custom_stale_metrics_dict from Redis hash key %s - %s' % (
                    custom_stale_metrics_hash_key, e))
            known_custom_stale_metrics = []
            if known_custom_stale_metrics_dict:
                try:
                    known_custom_stale_metrics = list(known_custom_stale_metrics_dict.keys())
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to create list of known_custom_stale_metrics from known_custom_stale_metrics_dict - %s' % (
                        e))
            logger.info('metrics_manager :: determined %s known CUSTOM_STALE_PERIOD metrics from analyzer.metrics_manager.custom_stale_period' % str(len(known_custom_stale_metrics)))
            logger.info('metrics_manager :: determining unique_base_names matching CUSTOM_STALE_PERIOD')

            # @added 20210601 - Feature #4000: EXTERNAL_SETTINGS
            # Pass the external_settings as a custom_stale_period argument
            if not external_settings:
                try:
                    # debug
                    # external_settings = get_external_settings(skyline_app)
                    external_settings = get_external_settings(skyline_app, None, True)
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: get_external_settings failed - %s' % (
                        e))

            for i_metric in unique_base_names:
                if i_metric in custom_stale_period_metrics:
                    continue
                metric_stale_period = int(default_metric_stale_period)
                try:
                    # @modified 20210601 - Feature #4000: EXTERNAL_SETTINGS
                    # Pass the external_settings as a custom_stale_period argument
                    metric_stale_period = custom_stale_period(skyline_app, i_metric, external_settings, log=False)
                except Exception as e:
                    logger.error('error :: failed running custom_stale_period - %s' % e)
                    metric_stale_period = int(default_metric_stale_period)
                if int(float(metric_stale_period)) != default_metric_stale_period:
                    custom_stale_periods.append([i_metric, int(float(metric_stale_period))])
                    custom_stale_period_metrics.append(i_metric)

            logger.info('metrics_manager :: determined %s custom_stale_period metrics' % (
                str(len(custom_stale_periods))))

        if custom_stale_periods:
            metrics_unchanged = 0
            metrics_added_to_hash_key = 0
            metrics_updated_in_hash_key = 0
            metrics_removed_from_hash_key = 0
            logger.info('metrics_manager :: managing %s metrics in analyzer.metrics_manager.custom_stale_period hash key' % (
                str(len(custom_stale_periods))))
            for i_metric, i_stale_period in custom_stale_periods:
                update_hash_key = False
                to_add_to_hash_key = False
                if i_metric in known_custom_stale_metrics:
                    if int(float(i_stale_period)) != int(float(known_custom_stale_metrics_dict[i_metric])):
                        update_hash_key = True
                    else:
                        metrics_unchanged += 1
                else:
                    update_hash_key = True
                    to_add_to_hash_key = True
                if update_hash_key:
                    try:
                        self.redis_conn.hset(
                            custom_stale_metrics_hash_key,
                            i_metric, int(float(i_stale_period)))
                        if to_add_to_hash_key:
                            metrics_added_to_hash_key += 1
                        else:
                            metrics_updated_in_hash_key += 1
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to add entry in analyzer.metrics_manager.hash_key.metrics_data_sparsity for - %s - %s' % (
                            str(metric_name), e))
            metrics_to_remove_from_custom_stale_period_hash = []
            for i_metric in known_custom_stale_metrics:
                if i_metric not in custom_stale_period_metrics:
                    metrics_to_remove_from_custom_stale_period_hash.append(i_metric)
            if metrics_to_remove_from_custom_stale_period_hash:
                try:
                    self.redis_conn.hdel(custom_stale_metrics_hash_key, *set(metrics_to_remove_from_custom_stale_period_hash))
                    metrics_removed_from_hash_key = len(set(metrics_to_remove_from_custom_stale_period_hash))
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to remove %s metricsfrom the Redis hash key %s - %s' % (
                        str(len(metrics_to_remove_from_custom_stale_period_hash)),
                        custom_stale_metrics_hash_key, e))
            logger.info('metrics_manager :: managed analyzer.metrics_manager.custom_stale_period - unchanged: %s, added: %s, updated: %s, removed: %s' % (
                str(metrics_unchanged), str(metrics_added_to_hash_key),
                str(metrics_updated_in_hash_key),
                str(metrics_removed_from_hash_key)))

        # @added 20210519 - Feature #4076: CUSTOM_STALE_PERIOD
        #                   Branch #1444: thunder
        # Prune any entries out of the analyzer.metrics.last_timeseries_timestamp
        # has key that are older that FULL_DURATION
        logger.info('metrics_manager :: pruning analyzer.metrics.last_timeseries_timestamp Redis hash key')
        removed_from_hash = None
        prune_hash_key = 'analyzer.metrics.last_timeseries_timestamp'
        older_than_timestamp = int(time()) - settings.FULL_DURATION
        try:
            removed_from_hash = prune_metrics_timestamp_hash_key(skyline_app, prune_hash_key, older_than_timestamp, True)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: prune_metrics_timestamp_hash_key failed - %s' % e)
        logger.info('metrics_manager :: removed %s entries from %s hash key' % (
            str(removed_from_hash), prune_hash_key))

        # @added 20210518 - Branch #1444: thunder
        # Determine stale metrics AND custom stale metrics per parent namespace.
        # If any are present send them to thunder.
        namespace_stale_metrics_dict = {}
        namespace_recovered_metrics_dict = {}
        try:
            namespace_stale_metrics_dict, namespace_recovered_metrics_dict = thunder_stale_metrics(skyline_app, log=True)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: thunder_stale_metrics falied to get get namespace_stale_metrics_dict via - %s' % (
                e))
        logger.info('metrics_manager :: %s namespaces checked for stale metrics discovered with thunder_stale_metrics' % (
            str(len(namespace_stale_metrics_dict))))
        if namespace_stale_metrics_dict:
            try:
                self.redis_conn.set('analyzer.metrics_manager.namespaces.stale_metrics', str(namespace_stale_metrics_dict))
                logger.info('metrics_manager :: set analyzer.metrics_manager.namespaces.stale_metrics Redis key')
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to set analyzer.metrics_manager.namespaces.stale_metrics Redis key - %s' % e)
        # Thunder stale_metrics alerts on recovered metrics
        if namespace_recovered_metrics_dict:
            try:
                self.redis_conn.set('analyzer.metrics_manager.namespaces.recovered.stale_metrics', str(namespace_recovered_metrics_dict))
                logger.info('metrics_manager :: set analyzer.metrics_manager.namespaces.recovered.stale_metrics Redis key')
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to set analyzer.metrics_manager.namespaces.recovered.stale_metrics Redis key - %s' % e)
        logger.info('metrics_manager :: managing thunder alerted on stale metrics hash key')
        removed_from_hash = None
        try:
            removed_from_hash = manage_thunder_alerted_on_stale_metrics_hash_key(skyline_app, True)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: manage_thunder_alerted_on_stale_metrics_hash_key failed - %s' % e)
        logger.info('metrics_manager :: removed %s from thunder alerted on stale metrics hash key' % str(removed_from_hash))

        # @added 20210518 - Branch #1444: thunder
        # Determine no data per parent namespace and if any are found send them
        # to thunder
        namespaces_no_data_dict = {}
        try:
            namespaces_no_data_dict = thunder_no_data(skyline_app, log=True)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: thunder_no_data failed - %s' % (
                e))
        if namespaces_no_data_dict:
            logger.info('metrics_manager :: %s namespaces discovered not receiving data with thunder_no_data' % (
                str(len(namespaces_no_data_dict))))
        else:
            logger.info('metrics_manager :: thunder_no_data - all namespaces receiving data OK')

        # @added 20220110 - Bug #4364: Prune old thunder.events
        #                   Branch #1444: thunder
        # Any thunder events that are not acted on subsequent remain in the
        # thunder.events set. Remove entries older than 3 days.
        thunder_events_list = []
        try:
            thunder_events_list = list(self.redis_conn_decoded.smembers('thunder.events'))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: smembers thunder.events falied - %s' % (
                e))
        if thunder_events_list:
            pruned_events = 0
            oldest_event_timestamp = (int(spin_start) - (86400 * 3))
            thunder_events_list_len = len(thunder_events_list)
            logger.info('metrics_manager :: checking %s thunder.events items for pruning' % (
                str(thunder_events_list_len)))
            for event_item in thunder_events_list:
                remove_item = False
                event_timestamp = 0
                event_expiry = 0
                try:
                    event_data = literal_eval(event_item)
                    event_timestamp = int(float(event_data['timestamp']))
                    event_expiry = int(float(event_data['expiry']))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: thunder.events determining event.timestamp failed - %s' % (
                        err))
                if not event_timestamp:
                    remove_item = True
                if event_timestamp < oldest_event_timestamp:
                    remove_item = True
                if event_expiry:
                    if int(spin_start) > event_timestamp + event_expiry:
                        remove_item = True
                if remove_item:
                    # Delete the item from the Redis set
                    try:
                        update_redis_set(
                            skyline_app, 'thunder.events', event_item,
                            'remove', log=False)
                        pruned_events += 1
                    except Exception as err:
                        logger.error('error :: could not remove item from Redis set thunder.events - %s' % (
                            err))
            logger.info('metrics_manager :: pruned %s old thunder.events items' % (
                str(pruned_events)))

        # @added 20210720 - Feature #4188: metrics_manager.boundary_metrics
        if settings.BOUNDARY_METRICS:
            # Build boundary metrics
            boundary_metrics_hash_key = 'metrics_manager.boundary_metrics'
            boundary_metrics = {}
            for base_name in unique_base_names:
                try:
                    for metric in settings.BOUNDARY_METRICS:
                        pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, [metric[0]])
                        if pattern_match:
                            boundary_metrics[base_name] = {}
                            algorithm = metric[1]
                            boundary_metrics[base_name][algorithm] = {}
                            boundary_metrics[base_name][algorithm]['expiry'] = metric[2]
                            boundary_metrics[base_name][algorithm]['min_average'] = metric[3]
                            boundary_metrics[base_name][algorithm]['min_average_seconds'] = metric[4]
                            boundary_metrics[base_name][algorithm]['trigger_value'] = metric[5]
                            boundary_metrics[base_name][algorithm]['alert_threshold'] = metric[6]
                            boundary_metrics[base_name][algorithm]['alert_vias'] = metric[7]
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to determine boundary_metrics - %s' % e)
            boundary_metrics_redis_dict = {}
            boundary_metrics_base_names = []
            logger.info('metrics_manager :: %s boundary_metrics identified from the %s unique_base_names' % (
                str(len(boundary_metrics)), str(len(unique_base_names))))
            boundary_metrics_keys_updated = 0
            if boundary_metrics:
                boundary_metrics_base_names = list(boundary_metrics.keys())
                try:
                    boundary_metrics_redis_dict = self.redis_conn_decoded.hgetall(boundary_metrics_hash_key)
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to get Redis hash key %s - %s' % (
                        boundary_metrics_hash_key, e))
                for base_name in boundary_metrics_base_names:
                    try:
                        self.redis_conn.hset(
                            boundary_metrics_hash_key, base_name,
                            str(boundary_metrics[base_name]))
                        boundary_metrics_keys_updated += 1
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to add entry to %s for - %s - %s' % (
                            boundary_metrics_hash_key, base_name, e))

            # Remove entries not defined in BOUNDARY_METRICS
            boundary_metrics_keys_removed = 0
            if boundary_metrics and boundary_metrics_redis_dict:
                for base_name in list(boundary_metrics_redis_dict.keys()):
                    if base_name not in boundary_metrics_base_names:
                        try:
                            self.redis_conn.hdel(boundary_metrics_hash_key, base_name)
                            boundary_metrics_keys_removed += 1
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: metrics_manager :: failed to delete entry in %s for - %s - %s' % (
                                boundary_metrics_hash_key, base_name, e))
            if boundary_metrics:
                logger.info('metrics_manager :: updated: %s, removed: %s entries in the Redis hash key %s' % (
                    str(boundary_metrics_keys_updated),
                    str(boundary_metrics_keys_removed), boundary_metrics_hash_key))

        del unique_base_names

        # @added 20201030 - Feature #3808: ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS
        # Remove any entries in the Redis low_priority_metrics_hash_key
        # that are not in unique_metrics
        if ANALYZER_MANAGE_LOW_PRIORITY_METRICS:
            logger.info('metrics_manager :: managing the Redis hash key %s and removing any metrics not in unique_metrics' % (
                low_priority_metrics_hash_key))
            low_priority_metrics_last_analyzed = []
            raw_low_priority_metrics_last_analyzed = {}
            try:
                raw_low_priority_metrics_last_analyzed = self.redis_conn_decoded.hgetall(low_priority_metrics_hash_key)
                for base_name_bytes in raw_low_priority_metrics_last_analyzed:
                    base_name = str(base_name_bytes)
                    last_analyzed = int(raw_low_priority_metrics_last_analyzed[base_name])
                    low_priority_metrics_last_analyzed.append([base_name, last_analyzed])
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to get Redis hash key %s' % (
                    low_priority_metrics_hash_key))
                low_priority_metrics_last_analyzed = []
            del raw_low_priority_metrics_last_analyzed
            low_priority_analyzed_metrics = []
            if low_priority_metrics_last_analyzed:
                try:
                    low_priority_analyzed_metrics = [item[0] for item in low_priority_metrics_last_analyzed]
                    logger.info('metrics_manager :: there are %s metrics in the Redis hash key %s' % (
                        str(len(low_priority_analyzed_metrics)),
                        low_priority_metrics_hash_key))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to generate low_priority_metrics_last_analyzed')
                    low_priority_analyzed_metrics = []
                try:
                    del low_priority_metrics_last_analyzed
                except:
                    pass
            if low_priority_analyzed_metrics:
                low_priority_analyzed_metrics_set = None
                try:
                    low_priority_analyzed_metrics_set = set(low_priority_analyzed_metrics)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to generate low_priority_analyzed_metrics_set')
                try:
                    del low_priority_analyzed_metrics
                except:
                    pass
                unique_metrics_set = None
                try:
                    unique_metrics_list = list(unique_metrics)
                    unique_metrics_set = set(unique_metrics_list)
                    del unique_metrics_list
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to generate unique_metrics_set')
                if low_priority_analyzed_metrics_set and unique_metrics_set:
                    low_priority_metrics_to_remove = []
                    try:
                        set_difference = low_priority_analyzed_metrics_set.difference(unique_metrics_set)
                        for metric in set_difference:
                            low_priority_metrics_to_remove.append(metric)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: determining difference between low_priority_analyzed_metrics_set and unique_metrics_set')
                    try:
                        del low_priority_analyzed_metrics_set
                    except:
                        pass
                    try:
                        del unique_metrics_set
                    except:
                        pass
                    try:
                        del set_difference
                    except:
                        pass
                    if low_priority_metrics_to_remove:
                        try:
                            logger.info('metrics_manager :: removing %s metrics from the Redis hash key %s' % (
                                str(len(low_priority_metrics_to_remove)),
                                low_priority_metrics_hash_key))
                            self.redis_conn.hdel(low_priority_metrics_hash_key, *set(low_priority_metrics_to_remove))
                            logger.info('metrics_manager :: removed %s metrics from the Redis hash key %s' % (
                                str(len(low_priority_metrics_to_remove)),
                                low_priority_metrics_hash_key))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: metrics_manager :: failed to remove the low_priority_metrics_to_remove the Redis hash key %s' % (
                                low_priority_metrics_hash_key))
                        try:
                            del low_priority_metrics_to_remove
                        except:
                            pass
                    else:
                        logger.info('metrics_manager :: no metrics need to be removed from the Redis hash key %s' % (
                            low_priority_metrics_hash_key))

        # @added 20210619 - Feature #4148: analyzer.metrics_manager.resolutions
        #                   Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
        # Surface the Redis hash key data
        added_metric_resolution_keys = 0
        updated_metric_resolution_keys = 0
        metrics_resolutions_hash_key = 'analyzer.metrics_manager.resolutions'
        metrics_resolutions_dict = {}
        try:
            metrics_resolutions_dict = self.redis_conn_decoded.hgetall(metrics_resolutions_hash_key)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: failed to get %s Redis hash key - %s' % (
                metrics_resolutions_hash_key, e))
            metrics_resolutions_dict = {}
        # Set check_metrics before CHECK_DATA_SPARSITY
        check_metrics = list(unique_metrics)
        # @added 20210621 - Feature #4148: analyzer.metrics_manager.resolutions
        #                   Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
        metrics_with_unknown_resolution = []

        # @added 20201209 - Feature #3870: metrics_manager - check_data_sparsity
        if CHECK_DATA_SPARSITY:
            check_data_sparsity_start = time()
            logger.info('metrics_manager :: checking data sparsity')

            # @added 20201210 - Feature #3870: metrics_manager - check_data_sparsity
            # Allow SKIP_CHECK_DATA_SPARSITY_NAMESPACES
            check_metrics = list(unique_metrics)
            do_not_check_metrics = []
            if SKIP_CHECK_DATA_SPARSITY_NAMESPACES:
                logger.info('metrics_manager :: determing which metric to skip checking data sparsity on as SKIP_CHECK_DATA_SPARSITY_NAMESPACES has %s namespaces declared' % str(len(SKIP_CHECK_DATA_SPARSITY_NAMESPACES)))
                check_metrics = []
                skip_check_data_sparsity_error_logged = False
                try:
                    for metric in unique_metrics:
                        try:
                            metric_name = str(metric)
                            if metric_name.startswith(settings.FULL_NAMESPACE):
                                base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                            else:
                                base_name = metric_name
                            pattern_match = None
                            try:
                                pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, SKIP_CHECK_DATA_SPARSITY_NAMESPACES)
                            except Exception as e:
                                if not skip_check_data_sparsity_error_logged:
                                    logger.error('error :: metrics_manager :: an error occurred while matched_or_regexed_in_list in SKIP_CHECK_DATA_SPARSITY_NAMESPACES - %s' % e)
                                    skip_check_data_sparsity_error_logged = True
                                pattern_match = False
                            if pattern_match:
                                do_not_check_metrics.append([metric_name, metric_matched_by])
                            else:
                                check_metrics.append(metric_name)
                        except Exception as e:
                            # Only log one error as to not fill the log
                            if not skip_check_data_sparsity_error_logged:
                                logger.error('error :: metrics_manager :: an error occurred while evaluating %s for check_metrics in SKIP_CHECK_DATA_SPARSITY_NAMESPACES - %s' % (str(metric), e))
                                skip_check_data_sparsity_error_logged = True
                            else:
                                pass
                except Exception as e:
                    logger.error('error :: metrics_manager :: an error occurred while determining check_metrics in SKIP_CHECK_DATA_SPARSITY_NAMESPACES - %s' % e)
                if do_not_check_metrics:
                    try:
                        logger.info('metrics_manager :: excluding %s metrics from check_data_sparsity as they match a namespace in SKIP_CHECK_DATA_SPARSITY_NAMESPACES' % str(len(do_not_check_metrics)))
                        try:
                            self.redis_conn.set(
                                'analyzer.metrics_manager.metrics_sparsity.skip_check_data_sparsity_metrics', str(do_not_check_metrics))
                        except Exception as e:
                            logger.error('error :: metrics_manager :: could not set Redis analyzer.metrics_manager.metrics_sparsity.skip_check_data_sparsity_metrics: %s' % e)
                    except Exception as e:
                        logger.error('error :: metrics_manager :: an error occurred while setting Redis analyzer.metrics_manager.metrics_sparsity.skip_check_data_sparsity_metrics - %s' % e)

            # Multi get series
            try:
                # @modified 20201210 - Feature #3870: metrics_manager - check_data_sparsity
                # Allow SKIP_CHECK_DATA_SPARSITY_NAMESPACES
                # raw_assigned = self.redis_conn.mget(unique_metrics)
                raw_assigned = self.redis_conn.mget(check_metrics)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to get check_metrics from Redis')
                raw_assigned = []
            if not raw_assigned:
                logger.info('error :: metrics_manager :: No raw_assigned set, returning')
            else:
                logger.info('metrics_manager :: checking data sparsity on %s metric timeseries from Redis' % str(len(raw_assigned)))
            last_metrics_data_sparsity = {}
            try:
                last_metrics_data_sparsity = self.redis_conn_decoded.hgetall('analyzer.metrics_manager.hash_key.metrics_data_sparsity')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to get analyzer.metrics_manager.hash_key.metrics_data_sparsity Redis hash key')
                last_metrics_data_sparsity = {}
            try:
                self.redis_conn.delete('analyzer.metrics_manager.hash_key.metrics_data_sparsity')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to delete analyzer.metrics_manager.hash_key.metrics_data_sparsity')

            # @added 20210617 - Feature #3870: metrics_manager - check_data_sparsity
            #                   Branch ##1444: thunder
            metrics_timestamp_resolutions_count_dict = {}
            try:
                metrics_timestamp_resolutions_count_dict = self.redis_conn_decoded.hgetall('analyzer.metrics_manager.hash_key.metrics_timestamp_resolutions')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to get analyzer.metrics_manager.hash_key.metrics_timestamp_resolutions Redis hash key')
                metrics_timestamp_resolutions_count_dict = {}

            check_sparsity_error_log = False
            check_sparsity_error_count = 0
            added_data_sparsity_keys = 0
            metrics_sparsity = []
            metrics_fully_populated = []  # 100% of datapoints
            metrics_sparsity_decreasing = []  # becoming more densely populated (good)
            metrics_sparsity_increasing = []  # becoming more sparsely populated (bad)
            metrics_stale = []
            metrics_inactive = []
            sparsities = []
            metrics_sparsity_use_namespace = skyline_app_graphite_namespace + '.metrics_sparsity'

            # Distill timeseries strings into lists
            # @modified 20201210 - Feature #3870: metrics_manager - check_data_sparsity
            # Allow SKIP_CHECK_DATA_SPARSITY_NAMESPACES
            # for i, metric_name in enumerate(unique_metrics):
            for check_metric_index, metric_name in enumerate(check_metrics):
                try:
                    try:
                        raw_series = raw_assigned[check_metric_index]
                        unpacker = Unpacker(use_list=False)
                        unpacker.feed(raw_series)
                        timeseries = list(unpacker)
                    except:
                        timeseries = []

                    # @added 20210619 - Feature #4148: analyzer.metrics_manager.resolutions
                    #                   Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
                    # Populate Redis hash key data with the base_name
                    metric_name = str(metric_name)
                    if metric_name.startswith(settings.FULL_NAMESPACE):
                        base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    else:
                        base_name = metric_name

                    # @added 20210619 - Feature #4148: analyzer.metrics_manager.resolutions
                    #                   Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
                    # Use determine_data_frequency function
                    metric_resolution = 0
                    timestamp_resolutions_count = {}
                    try:
                        metric_resolution, timestamp_resolutions_count = determine_data_frequency(skyline_app, timeseries, False)
                    except Exception as e:
                        if not check_sparsity_error_log:
                            logger.error(traceback.format_exc())
                            logger.error('error :: metrics_manager :: determine_data_frequency failed for %s - %s' % (
                                base_name, e))
                            check_sparsity_error_log = True
                        check_sparsity_error_count += 1

                    update_metric_timestamp_resolutions_key = False
                    metric_timestamp_resolutions_count_str = None
                    try:
                        metric_timestamp_resolutions_count_str = metrics_timestamp_resolutions_count_dict[metric_name]
                    except KeyError:
                        metric_timestamp_resolutions_count_str = None
                        update_metric_timestamp_resolutions_key = True
                    except Exception as e:
                        if not check_sparsity_error_log:
                            logger.error(traceback.format_exc())
                            logger.error('error :: metrics_manager :: failed to get entry from metrics_timestamp_resolutions_count_dict for - %s - %s' % (
                                str(metric_name), e))
                            check_sparsity_error_log = True
                        metric_timestamp_resolutions_count_str = None
                        update_metric_timestamp_resolutions_key = True
                    if metric_timestamp_resolutions_count_str:
                        if metric_timestamp_resolutions_count_str != str(dict(timestamp_resolutions_count)):
                            update_metric_timestamp_resolutions_key = True
                    if update_metric_timestamp_resolutions_key:
                        try:
                            self.redis_conn.hset(
                                'analyzer.metrics_manager.hash_key.metrics_timestamp_resolutions',
                                metric_name, str(dict(timestamp_resolutions_count)))
                        except Exception as e:
                            if not check_sparsity_error_log:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: failed to add entry in analyzer.metrics_manager.hash_key.metrics_timestamp_resolutions for - %s - %s' % (
                                    str(metric_name), e))
                                check_sparsity_error_log = True

                    # @added 20210619 - Feature #4148: analyzer.metrics_manager.resolutions
                    #                   Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
                    #                   Feature #4144: webapp - stale_metrics API endpoint
                    #                   Feature #4076: CUSTOM_STALE_PERIOD
                    update_metrics_resolutions_key = True
                    last_known_metric_resolution = None
                    if metric_resolution and metrics_resolutions_dict:
                        try:
                            last_known_metric_resolution = metrics_resolutions_dict[base_name]
                        except KeyError:
                            update_metrics_resolutions_key = True
                            last_known_metric_resolution = None
                        except Exception as e:
                            if not check_sparsity_error_log:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: update_metrics_resolutions_key - failed to determine metric resolution for %s - %s' % (
                                    base_name, e))
                                check_sparsity_error_log = True
                            check_sparsity_error_count += 1
                            last_known_metric_resolution = None
                        if last_known_metric_resolution:
                            if int(float(last_known_metric_resolution)) == metric_resolution:
                                update_metrics_resolutions_key = False

                    # @added 20210621 - Feature #4148: analyzer.metrics_manager.resolutions
                    #                   Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
                    if not metric_resolution:
                        if update_metrics_resolutions_key:
                            update_metrics_resolutions_key = False
                        metrics_with_unknown_resolution.append(base_name)

                    # @added 20210702 - Feature #4148: analyzer.metrics_manager.resolutions
                    #                   Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
                    # Never set the metric_resolution to a negative int
                    if update_metrics_resolutions_key:
                        if int(metric_resolution) < 1:
                            logger.info('metrics_manager :: update_metrics_resolutions_key - not updating %s resolution from %s to %s (invalid resolution)' % (
                                base_name, str(last_known_metric_resolution),
                                str(metric_resolution)))
                            update_metrics_resolutions_key = False

                    if update_metrics_resolutions_key:
                        if last_known_metric_resolution is not None:
                            logger.info('metrics_manager :: update_metrics_resolutions_key - updating %s resolution from %s to %s' % (
                                base_name, str(last_known_metric_resolution),
                                str(metric_resolution)))
                            updated_metric_resolution_keys += 1
                        else:
                            added_metric_resolution_keys += 1
                        try:
                            self.redis_conn.hset(
                                metrics_resolutions_hash_key,
                                base_name, int(metric_resolution))
                        except Exception as e:
                            if not check_sparsity_error_log:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: failed to add entry in analyzer.metrics_manager.hash_key.metrics_data_sparsity for - %s - %s' % (
                                    str(metric_name), e))
                                check_sparsity_error_log = True
                            check_sparsity_error_count += 1

                    # @added 20210702 - Feature #4148: analyzer.metrics_manager.resolutions
                    #                   Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
                    # Explicitly set data_sparsity
                    data_sparsity = None

                    if metric_resolution:
                        try:
                            data_sparsity = determine_data_sparsity(skyline_app, timeseries, resolution=metric_resolution, log=False)
                        except Exception as e:
                            if not check_sparsity_error_log:
                                logger.error('error :: metrics_manager :: determine_data_sparsity failed during check_data_sparsity - %s' % str(e))
                                check_sparsity_error_log = True
                            check_sparsity_error_count += 1

                    previous_sparsity = 0
                    if last_metrics_data_sparsity:
                        try:
                            previous_sparsity = float(last_metrics_data_sparsity[metric_name])
                        except:
                            pass
                    if data_sparsity or data_sparsity == 0:
                        try:
                            self.redis_conn.hset(
                                'analyzer.metrics_manager.hash_key.metrics_data_sparsity',
                                metric_name, data_sparsity)
                            added_data_sparsity_keys += 1
                        except Exception as e:
                            if not check_sparsity_error_log:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: failed to add entry in analyzer.metrics_manager.hash_key.metrics_data_sparsity for - %s - %s' % (
                                    str(metric_name), e))
                                check_sparsity_error_log = True
                            check_sparsity_error_count += 1
                        sparsity_change = 0
                        # if data_sparsity >= 100:
                        if data_sparsity >= settings.FULLY_POPULATED_PERCENTAGE:
                            metrics_fully_populated.append(metric_name)
                        else:
                            if previous_sparsity < data_sparsity:
                                metrics_sparsity_decreasing.append(metric_name)
                                sparsity_change = previous_sparsity - data_sparsity
                            if previous_sparsity > data_sparsity:
                                metrics_sparsity_increasing.append(metric_name)
                                sparsity_change = previous_sparsity - data_sparsity
                        metric_sparsity_dict = {
                            'metric': metric_name,
                            'timestamp': int(timeseries[-1][0]),
                            'resolution': metric_resolution,
                            'data_sparsity': data_sparsity,
                            'last_data_sparsity': previous_sparsity,
                            'change': sparsity_change,
                        }
                        metrics_sparsity.append(str(metric_sparsity_dict))
                        sparsities.append(data_sparsity)
                except Exception as e:
                    if not check_sparsity_error_log:
                        logger.error('error :: metrics_manager :: an error occurred during check_data_sparsity - %s' % str(e))
                        check_sparsity_error_log = True
                    check_sparsity_error_count += 1
            logger.info('metrics_manager :: check_data_sparsity added %s metrics of %s total metrics to analyzer.metrics_manager.hash_key.metrics_data_sparsity Redis hash key' % (
                str(added_data_sparsity_keys), str(len(unique_metrics))))

            if metrics_fully_populated:
                metrics_fully_populated_count = len(metrics_fully_populated)
                try:
                    self.redis_conn.rename('analyzer.metrics_manager.metrics_fully_populated', 'aet.analyzer.metrics_manager.metrics_fully_populated')
                    logger.info('metrics_manager :: created the aet.analyzer.metrics_manager.metrics_fully_populated Redis set')
                except:
                    logger.error('metrics_manager :: failed to created the aet.analyzer.metrics_manager.metrics_fully_populated Redis set')
                try:
                    self.redis_conn.sadd('analyzer.metrics_manager.metrics_fully_populated', *set(metrics_fully_populated))
                    logger.info('metrics_manager :: created and added %s metrics to the analyzer.metrics_manager.metrics_fully_populated Redis set' % str(metrics_fully_populated_count))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to add multiple members to the analyzer.metrics_manager.metrics_fully_populated Redis set')
                try:
                    self.redis_conn.set(
                        'analyzer.metrics_manager.metrics_sparsity.metrics_fully_populated', metrics_fully_populated_count)
                except Exception as e:
                    logger.error('error :: metrics_manager :: could not set Redis analyzer.metrics_manager.metrics_sparsity.metrics_fully_populated: %s' % e)
                send_metric_name = metrics_sparsity_use_namespace + '.metrics_fully_populated'
                try:
                    send_graphite_metric(skyline_app, send_metric_name, str(metrics_fully_populated_count))
                    logger.info('metrics_manager :: sent Graphite metric - %s %s' % (send_metric_name, str(metrics_fully_populated_count)))
                except Exception as e:
                    logger.error('error :: metrics_manager :: could not send send_graphite_metric %s %s: %s' % (
                        send_metric_name, str(metrics_fully_populated_count), e))

            if metrics_sparsity_decreasing:
                metrics_sparsity_decreasing_count = len(metrics_sparsity_decreasing)
                try:
                    self.redis_conn.rename('analyzer.metrics_manager.metrics_sparsity_decreasing', 'aet.analyzer.metrics_manager.metrics_sparsity_decreasing')
                    logger.info('metrics_manager :: created the aet.analyzer.metrics_manager.metrics_sparsity_decreasing Redis set')
                except:
                    logger.error('metrics_manager :: failed to created the aet.analyzer.metrics_manager.metrics_sparsity_decreasing Redis set')
                try:
                    self.redis_conn.sadd('analyzer.metrics_manager.metrics_sparsity_decreasing', *set(metrics_sparsity_decreasing))
                    logger.info('metrics_manager :: created and added %s metrics to the analyzer.metrics_manager.metrics_sparsity_decreasing Redis set' % str(metrics_sparsity_decreasing_count))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to add multiple members to the analyzer.metrics_manager.metrics_sparsity_decreasing Redis set')
                try:
                    self.redis_conn.set(
                        'analyzer.metrics_manager.metrics_sparsity.metrics_sparsity_decreasing', metrics_sparsity_decreasing_count)
                except Exception as e:
                    logger.error('error :: metrics_manager :: could not set Redis analyzer.metrics_manager.metrics_sparsity.metrics_sparsity_decreasing: %s' % e)
                send_metric_name = metrics_sparsity_use_namespace + '.metrics_sparsity_decreasing'
                try:
                    send_graphite_metric(skyline_app, send_metric_name, str(metrics_sparsity_decreasing_count))
                    logger.info('metrics_manager :: sent Graphite metric - %s %s' % (send_metric_name, str(metrics_sparsity_decreasing_count)))
                except Exception as e:
                    logger.error('error :: metrics_manager :: could not send send_graphite_metric %s %s: %s' % (
                        send_metric_name, str(metrics_sparsity_decreasing_count), e))
            if metrics_sparsity_increasing:
                metrics_sparsity_increasing_count = len(metrics_sparsity_increasing)
                try:
                    self.redis_conn.rename('analyzer.metrics_manager.metrics_sparsity_increasing', 'aet.analyzer.metrics_manager.metrics_sparsity_increasing')
                    logger.info('metrics_manager :: created the aet.analyzer.metrics_manager.metrics_sparsity_increasing Redis set')
                except:
                    logger.error('metrics_manager :: failed to created the aet.analyzer.metrics_manager.metrics_sparsity_increasing Redis set')
                try:
                    self.redis_conn.sadd('analyzer.metrics_manager.metrics_sparsity_increasing', *set(metrics_sparsity_increasing))
                    logger.info('metrics_manager :: created and added %s metrics to the analyzer.metrics_manager.metrics_sparsity_increasing Redis set' % str(metrics_sparsity_increasing_count))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to add multiple members to the analyzer.metrics_manager.metrics_sparsity_increasing Redis set')
                try:
                    self.redis_conn.set(
                        'analyzer.metrics_manager.metrics_sparsity.metrics_sparsity_increasing', metrics_sparsity_increasing_count)
                except Exception as e:
                    logger.error('error :: metrics_manager :: could not set Redis analyzer.metrics_manager.metrics_sparsity.metrics_sparsity_increasing: %s' % e)
                send_metric_name = metrics_sparsity_use_namespace + '.metrics_sparsity_increasing'
                try:
                    send_graphite_metric(skyline_app, send_metric_name, str(metrics_sparsity_increasing_count))
                    logger.info('metrics_manager :: sent Graphite metric - %s %s' % (send_metric_name, str(metrics_sparsity_increasing_count)))
                except Exception as e:
                    logger.error('error :: metrics_manager :: could not send send_graphite_metric %s %s: %s' % (
                        send_metric_name, str(metrics_sparsity_increasing_count), e))

            if metrics_sparsity:
                try:
                    self.redis_conn.rename('analyzer.metrics_manager.metrics_sparsity', 'aet.analyzer.metrics_manager.metrics_sparsity')
                    logger.info('metrics_manager :: created the aet.analyzer.metrics_manager.metrics_sparsity Redis set')
                except:
                    logger.error('metrics_manager :: failed to created the aet.analyzer.metrics_manager.metrics_sparsity Redis set')
                try:
                    self.redis_conn.sadd('analyzer.metrics_manager.metrics_sparsity', *set(metrics_sparsity))
                    logger.info('metrics_manager :: created and added %s metrics to the analyzer.metrics_manager.metrics_sparsity Redis set' % str(len(metrics_sparsity)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to add multiple members to the analyzer.metrics_manager.metrics_sparsity_increasing Redis set')

            avg_sparsity = None
            if sparsities:
                float_sparsities = []
                for item in sparsities:
                    try:
                        float_sparsities.append(float(item))
                    except:
                        pass
                try:
                    avg_sparsity = sum(float_sparsities) / len(sparsities)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to calculate avg_sparsity')
            if avg_sparsity:
                try:
                    self.redis_conn.set(
                        'analyzer.metrics_manager.metrics_sparsity.avg_sparsity', float(avg_sparsity))
                except Exception as e:
                    logger.error('error :: metrics_manager :: could not set Redis analyzer.metrics_manager.metrics_sparsity.avg_sparsity: %s' % e)
                send_metric_name = metrics_sparsity_use_namespace + '.avg_sparsity'
                try:
                    send_graphite_metric(skyline_app, send_metric_name, str(avg_sparsity))
                    logger.info('metrics_manager :: sent Graphite metric - %s %s' % (send_metric_name, str(avg_sparsity)))
                except Exception as e:
                    logger.error('error :: metrics_manager :: could not send send_graphite_metric %s %s: %s' % (
                        send_metric_name, str(avg_sparsity), e))
                try:
                    self.redis_conn.setex(
                        'analyzer.metrics_manager.sent.metrics_sparsity.metrics', 60, int(time()))
                except Exception as e:
                    logger.error('error :: metrics_manager :: could not set Redis analyzer.metrics_manager.sent.metrics_sparsity.metrics: %s' % e)

            logger.info('metrics_manager :: check_data_sparsity - of the %s unique_metrics, %s metrics are fully populated' % (
                str(len(unique_metrics)), str(len(metrics_fully_populated))))
            logger.info('metrics_manager :: check_data_sparsity - of the %s unique_metrics, %s metrics are increasing in sparsity (this is could be bad)' % (
                str(len(unique_metrics)), str(len(metrics_sparsity_increasing))))
            logger.info('metrics_manager :: check_data_sparsity - of the %s unique_metrics, %s metrics are decreasing in sparsity (this is good)' % (
                str(len(unique_metrics)), str(len(metrics_sparsity_decreasing))))
            logger.info('metrics_manager :: check_data_sparsity - of the %s unique_metrics, %s metrics are stale' % (
                str(len(unique_metrics)), str(len(metrics_stale))))
            logger.info('metrics_manager :: check_data_sparsity - of the %s unique_metrics, %s metrics are inactive' % (
                str(len(unique_metrics)), str(len(metrics_inactive))))
            check_data_sparsity_time = time() - check_data_sparsity_start
            logger.info('metrics_manager :: check data sparsity took %.2f seconds' % check_data_sparsity_time)

        # @added 20210619 - Feature #4148: analyzer.metrics_manager.resolutions
        #                   Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
        # Also determine resolution of metrics that were not checked for data
        # sparsity
        unique_metrics_set = set(list(unique_metrics))
        check_metrics_set = set(check_metrics)
        set_difference = unique_metrics_set.difference(check_metrics_set)
        unchecked_resolution_metrics = list(set_difference)
        logger.info('metrics_manager :: update_metrics_resolutions_key - determining resolution of %s metric which were skipped from sparsity check' % (
            str(len(unchecked_resolution_metrics))))
        unchecked_raw_assigned = None
        if unchecked_resolution_metrics:
            try:
                unchecked_raw_assigned = self.redis_conn.mget(unchecked_resolution_metrics)
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to get unchecked_resolution_metrics from Redis - %s' % e)
                unchecked_raw_assigned = []
        for i, metric_name in enumerate(unchecked_resolution_metrics):
            try:
                try:
                    raw_series = unchecked_raw_assigned[i]
                    unpacker = Unpacker(use_list=False)
                    unpacker.feed(raw_series)
                    timeseries = list(unpacker)
                except:
                    timeseries = []
                metric_name = str(metric_name)
                if metric_name.startswith(settings.FULL_NAMESPACE):
                    base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                else:
                    base_name = metric_name
                metric_resolution = 0
                timestamp_resolutions_count = {}
                try:
                    metric_resolution, timestamp_resolutions_count = determine_data_frequency(skyline_app, timeseries, False)
                except Exception as e:
                    if not check_sparsity_error_log:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: unchecked_resolution_metrics - determine_data_frequency failed for %s - %s' % (
                            base_name, e))
                        check_sparsity_error_log = True
                update_metric_timestamp_resolutions_key = False
                metric_timestamp_resolutions_count_str = None
                try:
                    metric_timestamp_resolutions_count_str = metrics_timestamp_resolutions_count_dict[metric_name]
                except KeyError:
                    metric_timestamp_resolutions_count_str = None
                    update_metric_timestamp_resolutions_key = True
                except Exception as e:
                    if not check_sparsity_error_log:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: unchecked_resolution_metrics - failed to get entry from metrics_timestamp_resolutions_count_dict for - %s - %s' % (
                            str(metric_name), e))
                        check_sparsity_error_log = True
                    metric_timestamp_resolutions_count_str = None
                    update_metric_timestamp_resolutions_key = True
                if metric_timestamp_resolutions_count_str:
                    if metric_timestamp_resolutions_count_str != str(dict(timestamp_resolutions_count)):
                        update_metric_timestamp_resolutions_key = True
                if update_metric_timestamp_resolutions_key:
                    try:
                        self.redis_conn.hset(
                            'analyzer.metrics_manager.hash_key.metrics_timestamp_resolutions',
                            metric_name, str(dict(timestamp_resolutions_count)))
                    except Exception as e:
                        if not check_sparsity_error_log:
                            logger.error(traceback.format_exc())
                            logger.error('error :: metrics_manager :: failed to add entry in analyzer.metrics_manager.hash_key.metrics_timestamp_resolutions for - %s - %s' % (
                                str(metric_name), e))
                            check_sparsity_error_log = True
                update_metrics_resolutions_key = True
                last_known_metric_resolution = None
                if metric_resolution and metrics_resolutions_dict:
                    try:
                        last_known_metric_resolution = metrics_resolutions_dict[base_name]
                    except KeyError:
                        update_metrics_resolutions_key = True
                        last_known_metric_resolution = None
                    except Exception as e:
                        if not check_sparsity_error_log:
                            logger.error(traceback.format_exc())
                            logger.error('error :: metrics_manager :: update_metrics_resolutions_key - failed to determine metric resolution for %s - %s' % (
                                base_name, e))
                            check_sparsity_error_log = True
                        last_known_metric_resolution = None
                    if last_known_metric_resolution:
                        if int(float(last_known_metric_resolution)) == metric_resolution:
                            update_metrics_resolutions_key = False

                # @added 20210621 - Feature #4148: analyzer.metrics_manager.resolutions
                #                   Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
                if not metric_resolution:
                    if update_metrics_resolutions_key:
                        update_metrics_resolutions_key = False
                    metrics_with_unknown_resolution.append(base_name)

                if update_metrics_resolutions_key:
                    if last_known_metric_resolution is not None:
                        logger.info('metrics_manager :: update_metrics_resolutions_key - updating %s resolution from %s to %s' % (
                            base_name, str(last_known_metric_resolution),
                            str(metric_resolution)))
                        updated_metric_resolution_keys += 1
                    else:
                        added_metric_resolution_keys += 1
                    try:
                        self.redis_conn.hset(
                            metrics_resolutions_hash_key,
                            base_name, int(metric_resolution))
                    except Exception as e:
                        if not check_sparsity_error_log:
                            logger.error(traceback.format_exc())
                            logger.error('error :: metrics_manager :: failed to add entry in analyzer.metrics_manager.hash_key.metrics_data_sparsity for - %s - %s' % (
                                str(metric_name), e))
                            check_sparsity_error_log = True
            except Exception as e:
                if not check_sparsity_error_log:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed unchecked_resolution_metrics - %s' % e)
                    check_sparsity_error_log = True
        logger.info('metrics_manager :: update_metrics_resolutions_key - added %s metric resolutions to %s Redis hash key' % (
            str(added_metric_resolution_keys), metrics_resolutions_hash_key))
        logger.info('metrics_manager :: update_metrics_resolutions_key - updated %s metric resolution in %s Redis hash key' % (
            str(updated_metric_resolution_keys), metrics_resolutions_hash_key))

        # @added 20210621 - Feature #4148: analyzer.metrics_manager.resolutions
        #                   Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
        logger.info('metrics_manager :: update_metrics_resolutions_key - %s metrics have unknown resolution' % (
            str(len(metrics_with_unknown_resolution))))

        # @added 20201212 - Feature #3884: ANALYZER_CHECK_LAST_TIMESTAMP
        if ANALYZER_CHECK_LAST_TIMESTAMP:
            try:
                metrics_last_timestamp_dict = self.redis_conn_decoded.hgetall(metrics_last_timestamp_hash_key)
                if metrics_last_timestamp_dict:
                    logger.info('metrics_manager :: ANALYZER_CHECK_LAST_TIMESTAMP - got %s metrics and last analysed timestamps from %s Redis hash key to manage' % (
                        str(len(metrics_last_timestamp_dict)),
                        metrics_last_timestamp_hash_key))
                else:
                    logger.warning('warning :: ANALYZER_CHECK_LAST_TIMESTAMP enabled but got no data from the %s Redis hash key' % (
                        metrics_last_timestamp_hash_key))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get Redis hash key %s' % (
                    metrics_last_timestamp_hash_key))
                metrics_last_timestamp_dict = {}
            removed_old_keys = 0
            if metrics_last_timestamp_dict:
                for metric in metrics_last_timestamp_dict:
                    try:
                        last_analyzed_timestamp = int(metrics_last_timestamp_dict[metric])
                    except:
                        last_analyzed_timestamp = None
                    if last_analyzed_timestamp:
                        try:
                            if last_analyzed_timestamp < (int(spin_start) - settings.FULL_DURATION):
                                self.redis_conn.hdel(metrics_last_timestamp_hash_key, metric)
                                removed_old_keys += 1
                        except Exception as e:
                            logger.error('error :: metrics_manager :: ANALYZER_CHECK_LAST_TIMESTAMP could not remove %s from Redis %s: %s' % (
                                str(metric), metrics_last_timestamp_hash_key, e))
            logger.info('metrics_manager :: ANALYZER_CHECK_LAST_TIMESTAMP - removed %s inactive metrics from the %s Redis hash key' % (
                str(removed_old_keys), metrics_last_timestamp_hash_key))

        # @added 20210330 - Feature #3994: Panorama - mirage not anomalous
        # The mirage.panorama.not_anomalous_metrics is managed here and entries
        # older than 7 days are removed.
        redis_hash = 'mirage.panorama.not_anomalous_metrics'
        mirage_panorama_not_anomalous = {}
        try:
            mirage_panorama_not_anomalous = self.redis_conn_decoded.hgetall(redis_hash)
            logger.info('metrics_manager :: %s entries to check in the %s Redis hash key' % (
                str(len(mirage_panorama_not_anomalous)), redis_hash))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: failed to get Redis hash key %s' % redis_hash)
            mirage_panorama_not_anomalous = {}
        timestamp_floats = []
        if mirage_panorama_not_anomalous:
            timestamp_floats = list(mirage_panorama_not_anomalous.keys())
        timestamp_floats_to_remove = []
        if timestamp_floats:
            try:
                timestamp_week_ago = int(time()) - (86400 - 7)
                for timestamp_float in timestamp_floats:
                    if int(float(timestamp_float)) < timestamp_week_ago:
                        timestamp_floats_to_remove.append(timestamp_float)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to determine entries to remove from Redis hash key %s' % (
                    redis_hash))
        if timestamp_floats_to_remove:
            try:
                self.redis_conn.hdel(redis_hash, *set(timestamp_floats_to_remove))
                logger.info('metrics_manager :: %s entries were removed from Redis hash %s' % (
                    str(len(set(timestamp_floats_to_remove))), redis_hash))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to %s remove entries from Redis hash key %s' % (
                    str(len(timestamp_floats_to_remove)), redis_hash))
        else:
            logger.info('metrics_manager :: there are no entries that need to be removed from Redis hash %s' % (
                redis_hash))

        # @added 20210429 - Feature #3994: Panorama - mirage not anomalous
        # The ionosphere.panorama.not_anomalous_metrics is managed here and entries
        # older than 7 days are removed.
        redis_hash = 'ionosphere.panorama.not_anomalous_metrics'
        ionosphere_panorama_not_anomalous = {}
        try:
            ionosphere_panorama_not_anomalous = self.redis_conn_decoded.hgetall(redis_hash)
            logger.info('metrics_manager :: %s entries to check in the %s Redis hash key' % (
                str(len(ionosphere_panorama_not_anomalous)), redis_hash))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: failed to get Redis hash key %s' % redis_hash)
            ionosphere_panorama_not_anomalous = {}
        ionosphere_timestamp_floats = []
        if ionosphere_panorama_not_anomalous:
            ionosphere_timestamp_floats = list(ionosphere_panorama_not_anomalous.keys())
        ionosphere_timestamp_floats_to_remove = []
        if ionosphere_timestamp_floats:
            try:
                timestamp_week_ago = int(time()) - (86400 - 7)
                for timestamp_float in ionosphere_timestamp_floats:
                    if int(float(timestamp_float)) < timestamp_week_ago:
                        ionosphere_timestamp_floats_to_remove.append(timestamp_float)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to determine entries to remove from Redis hash key %s' % (
                    redis_hash))
        if ionosphere_timestamp_floats_to_remove:
            try:
                self.redis_conn.hdel(redis_hash, *set(ionosphere_timestamp_floats_to_remove))
                logger.info('metrics_manager :: %s entries were removed from Redis hash %s' % (
                    str(len(set(ionosphere_timestamp_floats_to_remove))), redis_hash))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to %s remove entries from Redis hash key %s' % (
                    str(len(timestamp_floats_to_remove)), redis_hash))
        else:
            logger.info('metrics_manager :: there are no entries that need to be removed from Redis hash %s' % (
                redis_hash))

        # @added 20210330 - Feature #3994: Panorama - mirage not anomalous
        # The panorama.not_anomalous_plots are managed here and entries
        # older than 7 days are removed and files deleted
        redis_hash = 'panorama.not_anomalous_plots'
        panorama_not_anomalous_plots = {}
        try:
            panorama_not_anomalous_plots = self.redis_conn_decoded.hgetall(redis_hash)
            logger.info('metrics_manager :: %s entries to check in the %s Redis hash key' % (
                str(len(panorama_not_anomalous_plots)), redis_hash))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: failed to get Redis hash key %s' % redis_hash)
            panorama_not_anomalous_plots = {}
        timestamp_floats = []
        if panorama_not_anomalous_plots:
            timestamp_floats = list(panorama_not_anomalous_plots.keys())

        timestamp_floats_to_remove = []
        if timestamp_floats:
            try:
                timestamp_week_ago = int(time()) - (86400 - 7)
                for timestamp_float in timestamp_floats:
                    if int(float(timestamp_float)) < timestamp_week_ago:
                        timestamp_floats_to_remove.append(timestamp_float)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to determine entries to remove from Redis hash key %s' % (
                    redis_hash))
        if timestamp_floats_to_remove:
            # Remove plot files
            for timestamp_float in timestamp_floats_to_remove:
                file_to_remove = None
                try:
                    file_to_remove = self.redis_conn_decoded.hget(redis_hash, timestamp_float)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to determine file to remove from %s from Redis hash key %s' % (
                        str(timestamp_float), redis_hash))
                if file_to_remove:
                    try:
                        if os.path.exists(file_to_remove):
                            os.remove(file_to_remove)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to file for remove entries from Redis hash key %s' % (
                            redis_hash))
            # Remove entries from the hash
            try:
                self.redis_conn.hdel(redis_hash, *set(timestamp_floats_to_remove))
                logger.info('metrics_manager :: %s entries were removed from Redis hash %s' % (
                    str(len(set(timestamp_floats_to_remove))), redis_hash))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to %s remove entries from Redis hash key %s' % (
                    str(len(timestamp_floats_to_remove)), redis_hash))
        else:
            logger.info('metrics_manager :: there are no entries that need to be removed from Redis hash %s' % (
                redis_hash))

        # @added 20210803 - Feature #4164: luminosity - cloudbursts
        # Manage the luminosity.cloudbursts.anomalies_processed Redis hash and
        # remove keys that are older than 24 hours
        redis_hash = 'luminosity.cloudbursts.anomalies_processed'
        remove_before = int(time()) - 86400
        luminosity_cloudburst_anomalies_processed = {}
        try:
            luminosity_cloudburst_anomalies_processed = self.redis_conn_decoded.hgetall(redis_hash)
            logger.info('metrics_manager :: %s entries to check in the %s Redis hash key' % (
                str(len(luminosity_cloudburst_anomalies_processed)), redis_hash))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: failed to get Redis hash key %s' % redis_hash)
            luminosity_cloudburst_anomalies_processed = {}
        remove_keys = []
        for key in list(luminosity_cloudburst_anomalies_processed.keys()):
            try:
                if luminosity_cloudburst_anomalies_processed[key] < remove_before:
                    remove_keys.append(key)
            except Exception as e:
                logger.error('error :: metrics_manager :: failed to determine timestamp from %s entry from Redis hash key %s - %s' % (
                    key, redis_hash, e))
        if remove_keys:
            # Remove entries from the hash
            try:
                self.redis_conn.hdel(redis_hash, *set(remove_keys))
                logger.info('metrics_manager :: %s entries were removed from Redis hash %s' % (
                    str(len(set(remove_keys))), redis_hash))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to %s remove entries from Redis hash key %s - %s' % (
                    str(len(remove_keys)), redis_hash, e))
        else:
            logger.info('metrics_manager :: there are no entries or files that need to be removed from Redis hash %s' % (
                redis_hash))

        # @added 20210825 - Feature #4164: luminosity - cloudbursts
        metric_names_with_ids = {}
        # @added 20211004 - Feature #4264: luminosity - cross_correlation_relationships
        ids_with_metric_names = {}

        # @added 20210430 - Task #4030: refactoring
        metric_names = []
        try:
            # @modified 20210825 - Feature #4164: luminosity - cloudbursts
            # metric_names = get_all_db_metric_names(skyline_app)
            with_ids = True
            metric_names, metric_names_with_ids = get_all_db_metric_names(skyline_app, with_ids)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: failed to get_all_db_metric_names - %s' % (
                err))
        if metric_names:
            try:
                self.redis_conn.rename('analyzer.metrics_manager.db.metric_names', 'aet.analyzer.metrics_manager.db.metric_names')
                logger.info('metrics_manager :: created the aet.analyzer.metrics_manager.db.metric_names Redis set')
            except Exception as e:
                logger.error('metrics_manager :: failed to created the aet.analyzer.metrics_manager.db.metrics_fully_populated Redis set - %s' % e)
            try:
                self.redis_conn.sadd('analyzer.metrics_manager.db.metric_names', *set(metric_names))
                logger.info('metrics_manager :: created and added %s metrics to the analyzer.metrics_manager.db.metric_names Redis set' % str(len(metric_names)))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to add multiple members to the analyzer.metrics_manager.db.metrics_fully_populated Redis set - %s' % e)

        # @added 20211011 - Feature #4278: luminosity - related_namespaces
        # Create and manage the metrics_manager.namespaces Redis hash but only
        # update it if new data
        if not metric_names_with_ids:
            logger.error('error :: metrics_manager :: metric_names_with_ids is empty')
        if metric_names_with_ids:
            update_namespaces = False
            logger.info('metrics_manager :: %s metric_names_with_ids retrieved from DB' % str(len(metric_names_with_ids)))

            aet_metric_names_with_ids = {}
            try:
                aet_metric_names_with_ids = self.redis_conn_decoded.hgetall('aet.metrics_manager.metric_names_with_ids')
                logger.info('metrics_manager :: got %s last aet.metrics_manager.metric_names_with_ids from Redis to check namespaces' % str(len(aet_metric_names_with_ids)))
            except Exception as err:
                logger.error('metrics_manager :: failed to hgetall aet.metrics_manager.metric_names_with_ids to check namespace - %s' % err)
            if len(aet_metric_names_with_ids) == 0 and metric_names_with_ids:
                logger.info('metrics_manager :: aet.metrics_manager.metric_names_with_ids from Redis is empty so using metric_names_with_ids to check namespaces')
                aet_metric_names_with_ids = dict(metric_names_with_ids)

            namespaces = {}
            try:
                namespaces = self.redis_conn_decoded.hgetall('metrics_manager.namespaces')
                logger.info('metrics_manager :: got %s metrics_manager.namespaces from Redis to check namespaces' % str(len(namespaces)))
            except Exception as err:
                logger.error('metrics_manager :: failed to hgetall metrics_manager.namespaces to check namespaces - %s' % err)
            if not namespaces:
                update_namespaces = True
                logger.info('metrics_manager :: no known namespaces updating namespaces')
            if aet_metric_names_with_ids:
                last_metric_names = list(set(list(aet_metric_names_with_ids.keys())))
                if metric_names:
                    current_metric_names = list(set(list(aet_metric_names_with_ids.keys())))
                if current_metric_names != last_metric_names:
                    update_namespaces = True
                    logger.info('metrics_manager :: metrics changes updating namespaces')
            if not update_namespaces:
                logger.info('metrics_manager :: no changes to metrics not updating namespaces')
            if update_namespaces:
                logger.info('metrics_manager :: updating namespaces')
                namespaces_list = []
                for base_name in current_metric_names:
                    namespace_elements = base_name.split('.')
                    namespaces_list.append('.'.join(namespace_elements[0:-1]))
                namespaces = {}
                for namespace in namespaces_list:
                    if namespace == '':
                        continue
                    namespace_element_count = 0
                    for r_base_name in current_metric_names:
                        if namespace in r_base_name:
                            namespace_element_count += 1
                    if namespace_element_count:
                        namespaces[namespace] = namespace_element_count
                sorted_namespaces = {}
                for namespace in sorted(list(namespaces.keys())):
                    sorted_namespaces[namespace] = namespaces[namespace]
                if sorted_namespaces:
                    try:
                        self.redis_conn.hset('metrics_manager.namespaces', mapping=sorted_namespaces)
                        logger.info('metrics_manager :: set %s namespaces in metrics_manager.namespaces Redis hash' % str(len(list(sorted_namespaces.keys()))))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to create metrics_manager.namespaces Redis hash - %s' % str(err))

        if metric_names_with_ids:
            try:
                self.redis_conn.rename('metrics_manager.metric_names_with_ids', 'aet.metrics_manager.metric_names_with_ids')
                logger.info('metrics_manager :: created the aet.metrics_manager.metric_names_with_ids Redis set')
            except Exception as err:
                logger.error('metrics_manager :: failed to created the aet.metrics_manager.metric_names_with_ids Redis set - %s' % err)
            try:
                self.redis_conn.hset('metrics_manager.metric_names_with_ids', mapping=metric_names_with_ids)
                logger.info('metrics_manager :: created and added %s metrics to the metrics_manager.metric_names_with_ids Redis hash' % str(len(metric_names_with_ids)))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to create metrics_manager.metric_names_with_ids Redis hash - %s' % err)

            # @added 20211004 - Feature #4264: luminosity - cross_correlation_relationships
            ids_with_metric_names = {}
            for c_metric_name in list(metric_names_with_ids.keys()):
                c_metric_id = int(str(metric_names_with_ids[c_metric_name]))
                ids_with_metric_names[c_metric_id] = c_metric_name
            try:
                self.redis_conn.rename('metrics_manager.ids_with_metric_names', 'aet.metrics_manager.ids_with_metric_names')
                logger.info('metrics_manager :: created the aet.metrics_manager.ids_with_metric_names Redis set')
            except Exception as err:
                logger.error('metrics_manager :: failed to created the aet.metrics_manager.ids_with_metric_names Redis set - %s' % err)
            try:
                self.redis_conn.hset('metrics_manager.ids_with_metric_names', mapping=ids_with_metric_names)
                logger.info('metrics_manager :: created and added %s metrics to the metrics_manager.ids_with_metric_names Redis hash' % str(len(ids_with_metric_names)))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to create metrics_manager.ids_with_metric_names Redis hash - %s' % err)

        # @added 20211012 - Feature #4280: aet.metrics_manager.derivative_metrics Redis hash
        current_derivative_metrics = []
        try:
            current_derivative_metrics = list(self.redis_conn_decoded.smembers('derivative_metrics'))
        except Exception as err:
            logger.error('metrics_manager :: failed to get derivative_metrics Redis set - %s' % str(err))
        add_derivative_metrics = {}
        derivative_metric_timestamp = int(time())
        for derivative_metric in current_derivative_metrics:
            add_derivative_metrics[derivative_metric] = derivative_metric_timestamp
        if add_derivative_metrics:
            try:
                self.redis_conn.hset('aet.metrics_manager.derivative_metrics.timestamps', mapping=add_derivative_metrics)
                logger.info('metrics_manager :: set %s namespaces in aet.metrics_manager.derivative_metrics.timestamps Redis hash' % str(len(list(add_derivative_metrics.keys()))))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to create aet.metrics_manager.derivative_metrics.timestamps Redis hash - %s' % str(err))
        aet_metrics_manager_derivative_metrics_timestamps = {}
        try:
            aet_metrics_manager_derivative_metrics_timestamps = self.redis_conn_decoded.hgetall('aet.metrics_manager.derivative_metrics.timestamps')
            logger.info('metrics_manager :: got aet.metrics_manager.derivative_metrics.timestamps from Redis to check derivative_metric timestamps')
        except Exception as err:
            logger.error('metrics_manager :: failed to hgetall aet.metrics_manager.derivative_metrics.timestamps to check derivative_metric timestamps - %s' % str(err))
        derivative_metrics_to_remove = []
        new_current_derivative_metrics = []
        remove_derivative_metric_older_than = derivative_metric_timestamp - 3600
        if aet_metrics_manager_derivative_metrics_timestamps:
            for derivative_metric in list(aet_metrics_manager_derivative_metrics_timestamps.keys()):
                try:
                    derivative_metric_last_updated = int(str(aet_metrics_manager_derivative_metrics_timestamps[derivative_metric]))
                    if derivative_metric_last_updated < remove_derivative_metric_older_than:
                        derivative_metrics_to_remove.append(derivative_metric)
                    else:
                        new_current_derivative_metrics.append(derivative_metric)
                except Exception as err:
                    logger.error('metrics_manager :: failed to determine derivative_metric_last_updated - %s' % str(err))
        if not derivative_metrics_to_remove:
            logger.info('metrics_manager :: not removing any metrics from aet.metrics_manager.derivative_metrics.timestamps Redis hash as all have been updated in last hour')
        if derivative_metrics_to_remove:
            logger.info('metrics_manager :: removing %s metrics from aet.metrics_manager.derivative_metrics.timestamps Redis hash as older than 1 hour' % str(len(derivative_metrics_to_remove)))
            try:
                self.redis_conn.hdel('aet.metrics_manager.derivative_metrics.timestamps', *set(derivative_metrics_to_remove))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to remove %s metrics from the Redis hash key aet.metrics_manager.derivative_metrics.timestamps - %s' % (
                    str(len(derivative_metrics_to_remove)), str(err)))
            logger.info('metrics_manager :: removing %s metrics from aet.metrics_manager.derivative_metrics Redis set as older than 1 hour' % str(len(derivative_metrics_to_remove)))
            try:
                self.redis_conn.srem('aet.metrics_manager.derivative_metrics', *set(derivative_metrics_to_remove))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to remove %s metrics from the Redis hash key aet.metrics_manager.derivative_metrics - %s' % (
                    str(len(derivative_metrics_to_remove)), str(err)))
        if new_current_derivative_metrics:
            logger.info('metrics_manager :: adding %s metrics to aet.metrics_manager.derivative_metrics Redis set' % str(len(new_current_derivative_metrics)))
            try:
                self.redis_conn.sadd('aet.metrics_manager.derivative_metrics', *set(new_current_derivative_metrics))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to add multiple members to the aet.metrics_manager.derivative_metrics Redis set')

        # @added 20210624 - Feature #4150: metrics_manager - roomba batch processing metrics
        #                   Feature #3650: ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
        #                   Feature #3480: batch_processing
        # When there are batch processing metrics and ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
        # is enabled analyzer_batch only roombas metrics when they are received.
        # If batch metrics stop, they never get roomba'ed again and will remain
        # in Redis and never get purged.
        # Once an hour make metrics_manager just remove any batch processing
        # metrics that are older than FULL_DURATION + (FULL_DURATION / 2)
        if settings.BATCH_PROCESSING:
            inactive_batch_metrics_timestamp = int(time()) - (settings.FULL_DURATION + (settings.FULL_DURATION / 2))
            roomba_cleaned = False
            roomba_cache_key = 'analyzer.metrics_manager.roomba.batch_metrics'
            try:
                roomba_cleaned = self.redis_conn.get(roomba_cache_key)
                if roomba_cleaned:
                    logger.info('metrics_manager :: roomba batch_processing_metrics Redis key %s exists, nothing to do' % (
                        roomba_cache_key))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: roomba batch_processing_metrics failed to get Redis key %s - %s' % (
                    roomba_cache_key, e))
            batch_processing_base_names = []
            if not roomba_cleaned:
                try:
                    batch_processing_base_names = list(self.redis_conn_decoded.smembers('aet.analyzer.batch_processing_metrics'))
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: roomba batch_processing_metrics failed to get Redis set aet.analyzer.batch_processing_metrics - %s' % (
                        e))
            batch_processing_metrics = []
            if batch_processing_base_names:
                for base_name in batch_processing_base_names:
                    metric_name = '%s%s' % (settings.FULL_NAMESPACE, base_name)
                    batch_processing_metrics.append(metric_name)
            raw_assigned = None
            if batch_processing_metrics:
                logger.info('metrics_manager :: roomba checking %s batch_processing_metrics' % (
                    str(len(batch_processing_metrics))))
                try:
                    raw_assigned = self.redis_conn.mget(batch_processing_metrics)
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: roomba batch_processing_metrics failed to get raw_assigned from Redis - %s' % e)
            roomba_removed = []
            if raw_assigned:
                # Distill timeseries strings into lists
                for i, metric_name in enumerate(batch_processing_metrics):
                    timeseries = []
                    try:
                        raw_series = raw_assigned[i]
                        unpacker = Unpacker(use_list=False)
                        unpacker.feed(raw_series)
                        timeseries = list(unpacker)
                    except Exception as e:
                        logger.error('error :: metrics_manager :: roomba batch_processing_metrics failed to unpack %s timeseries - %s' % (
                            str(metric_name), e))
                        timeseries = []
                    # To ensure that there are no unordered timestamps in the time
                    # series which are artefacts of the collector or carbon-relay, sort
                    # all time series by timestamp before analysis.
                    original_timeseries = timeseries
                    if original_timeseries:
                        try:
                            timeseries = sort_timeseries(original_timeseries)
                            del original_timeseries
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: metrics_manager :: roomba batch_processing_metrics failed to determine whther to remove %s timeseries - %s' % (
                                metric_name, e))
                    remove_timeseries = False
                    if timeseries:
                        try:
                            last_timeseries_timestamp = int(timeseries[-1][0])
                            if last_timeseries_timestamp < inactive_batch_metrics_timestamp:
                                remove_timeseries = True
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: metrics_manager :: roomba batch_processing_metrics failed to determine whther to remove %s timeseries - %s' % (
                                metric_name, e))
                    if remove_timeseries:
                        try:
                            self.redis_conn.delete(metric_name)
                            logger.info('metrics_manager :: roomba removed batch processing metric %s' % metric_name)
                            roomba_removed.append(metric_name)
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: metrics_manager :: roomba falied to remove batch processing metric %s - %s' % (
                                metric_name, e))
                        # TODO - set inactive
            if not roomba_cleaned:
                try:
                    self.redis_conn.setex(roomba_cache_key, 3600, str(roomba_removed))
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to set Redis key %s - %s' % (
                        roomba_cache_key, e))

        spin_end = time() - spin_start

        # @added 20210619 - Feature #4148: analyzer.metrics_manager.resolutions
        try:
            self.redis_conn.set(
                'analyzer.metrics_manager.run_time', spin_end)
        except Exception as e:
            logger.error('error :: metrics_manager :: could not set Redis analyzer.metrics_manager.run_time: %s' % e)
        send_metric_name = '%s.metrics_manager_run_time' % skyline_app_graphite_namespace
        try:
            send_graphite_metric(skyline_app, send_metric_name, str(spin_end))
            logger.info('metrics_manager :: sent Graphite metric - %s %s' % (send_metric_name, str(spin_end)))
        except Exception as e:
            logger.error('error :: metrics_manager :: could not send send_graphite_metric %s %s: %s' % (
                send_metric_name, str(spin_end), e))

        logger.info('metrics_manager :: metric_management_process took %.2f seconds' % spin_end)
        return

    def run(self):
        """
        - Called when the process intializes.

        - Determine if Redis is up

        - Spawn a process to manage metrics lists and Redis sets

        - Wait for the process to finish.

        - Log the details about the run to the skyline analyzer log.

        - Send skyline.analyzer.metrics_manager metrics to `GRAPHITE_HOST`
        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        now = time()
        log_wait_for = now + 5
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time()
            else:
                now = log_wait_for + 1

        logger.info('metrics_manager :: starting %s metrics_manager' % skyline_app)

        # @added 20190417 - Feature #2950: Report defaulted settings to log
        # Added all the globally declared settings to enable reporting in the
        # log the state of each setting.
        try:
            SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
            if SERVER_METRIC_PATH == '.':
                SERVER_METRIC_PATH = ''
        except Exception as e:
            SERVER_METRIC_PATH = ''
            logger.warning('warning :: metrics_manager :: settings.SERVER_METRICS_NAME is not declared in settings.py, defaults to \'\' - %s' % e)
        try:
            ANALYZER_ENABLED = settings.ANALYZER_ENABLED
            logger.info('metrics_manager :: ANALYZER_ENABLED is set to %s' % str(ANALYZER_ENABLED))
        except Exception as e:
            ANALYZER_ENABLED = True
            logger.info('warning :: metrics_manager :: ANALYZER_ENABLED is not declared in settings.py, defaults to True - %s' % e)

        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager cannot connect to redis at socket path %s - %s' % (
                    settings.REDIS_SOCKET_PATH, e))
                sleep(10)
                try:
                    self.redis_conn = get_redis_conn(skyline_app)
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager cannot connect to get_redis_conn - %s' % e)
                continue

            # Report app up
            try:
                self.redis_conn.setex('analyzer.metrics_manager', 120, now)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: could not update the Redis analyzer.metrics_manager key')

            # Discover unique metrics
            unique_metrics_count = 0
            try:
                raw_unique_metrics_count = self.redis_conn_decoded.scard(full_uniques)
                unique_metrics_count = int(raw_unique_metrics_count)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager ::: could not get the count of %s from Redis' % full_uniques)
                sleep(10)
                continue

            if unique_metrics_count == 0:
                logger.info('metrics_manager :: no metrics in redis. try adding some - see README')
                sleep(10)
                continue

            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            try:
                p = Process(target=self.metric_management_process, args=(0,))
                pids.append(p)
                pid_count += 1
                logger.info('metrics_manager :: starting metric_management_process')
                p.start()
                spawned_pids.append(p.pid)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to spawn process')

            # Send wait signal to zombie processes
            # for p in pids:
            #     p.join()
            # Self monitor processes and terminate if any metric_management_process has run
            # for longer than 180 seconds - 20160512 @earthgecko
            p_starts = time()
            # TESTING p.join removal
            # while time() - p_starts <= 1:
            while time() - p_starts <= RUN_EVERY:
                if any(p.is_alive() for p in pids):
                    # Just to avoid hogging the CPU
                    sleep(.1)
                else:
                    # All the processes are done, break now.
                    time_to_run = time() - p_starts
                    logger.info('metrics_manager :: metric_management_process completed in %.2f seconds' % (time_to_run))
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('metrics_manager :: timed out, killing metric_management_process process')
                for p in pids:
                    logger.info('metrics_manager :: killing metric_management_process process')
                    p.terminate()
                    # p.join()
                    logger.info('metrics_manager :: killed metric_management_process process')

            for p in pids:
                if p.is_alive():
                    try:
                        logger.info('metrics_manager :: stopping metric_management_process - %s' % (str(p.is_alive())))
                        # p.join()
                        p.terminate()
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to stop spawn process')

            process_runtime = time() - now
            if process_runtime < RUN_EVERY:
                sleep_for = (RUN_EVERY - process_runtime)

                # @added 20201213 - Feature #3890: metrics_manager - sync_cluster_files
                # If this is a clustered Skyline instance and SYNC_CLUSTER_FILES
                # is enabled run the sync_cluster_files process every minute
                run_sync_cluster_files = False
                if REMOTE_SKYLINE_INSTANCES:
                    if SYNC_CLUSTER_FILES:
                        run_sync_cluster_files = True
                if FAKE_CLUSTER_SYNC:
                    run_sync_cluster_files = True
                    logger.info('metrics_manager :: run_sync_cluster_files as FAKE_CLUSTER_SYNC')
                logger.info('metrics_manager :: run_sync_cluster_files is set to %s' % str(run_sync_cluster_files))
                if run_sync_cluster_files:
                    logger.info('metrics_manager :: running sync_cluster_files')
                    try:
                        sync_until = now + (sleep_for - 60)
                        sync_time = time()
                        last_sync_cluster_time = None
                        while sync_time < sync_until:
                            if last_sync_cluster_time:
                                check_sync_time = time()
                                sync_sleep_for = 60 - (check_sync_time - last_sync_cluster_time)
                                if sync_sleep_for:
                                    logger.info('metrics_manager :: sleeping for %.2f seconds between sync_cluster_files runs' % sync_sleep_for)
                                    sleep(sync_sleep_for)
                            last_sync_cluster_time = time()
                            # Spawn sync_cluster_files
                            pids = []
                            spawned_pids = []
                            pid_count = 0
                            try:
                                p = Process(target=self.sync_cluster_files, args=(0,))
                                pids.append(p)
                                pid_count += 1
                                logger.info('metrics_manager :: starting sync_cluster_files')
                                p.start()
                                spawned_pids.append(p.pid)
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: failed to spawn sync_cluster_files process')
                            p_starts = time()
                            while time() - p_starts <= 60:
                                if any(p.is_alive() for p in pids):
                                    # Just to avoid hogging the CPU
                                    sleep(.1)
                                else:
                                    # All the processes are done, break now.
                                    time_to_run = time() - p_starts
                                    logger.info('metrics_manager :: sync_cluster_files completed in %.2f seconds' % (time_to_run))
                                    break
                            else:
                                # We only enter this if we didn't 'break' above.
                                logger.info('metrics_manager :: timed out, killing sync_cluster_files process')
                                for p in pids:
                                    logger.info('metrics_manager :: killing sync_cluster_files process')
                                    p.terminate()
                                    # p.join()
                                    logger.info('metrics_manager :: killed sync_cluster_files process')
                            for p in pids:
                                if p.is_alive():
                                    try:
                                        logger.info('metrics_manager :: stopping sync_cluster_files - %s' % (str(p.is_alive())))
                                        # p.join()
                                        p.terminate()
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: metrics_manager :: failed to stop sync_cluster_files process')
                            sync_time = time()
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: sync_cluster_files spawning failed')

                process_runtime_now = time() - now
                sleep_for = (RUN_EVERY - process_runtime_now)

                logger.info('metrics_manager :: sleeping for %.2f seconds due to low run time...' % sleep_for)
                sleep(sleep_for)
                try:
                    del sleep_for
                except:
                    logger.error('error :: metrics_manager :: failed to del sleep_for')
            try:
                del process_runtime
            except:
                logger.error('error :: metrics_manager :: failed to del process_runtime')
