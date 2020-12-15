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
from collections import Counter

# @added 20201213 - Feature #3890: metrics_manager - sync_cluster_files
import requests

import settings
from skyline_functions import (
    # @modified 20201209 - Feature #3870: metrics_manager - check_data_sparsity
    # Added send_graphite_metric
    get_redis_conn, get_redis_conn_decoded, send_graphite_metric,
    # @added 20201213 - Feature #3890: metrics_manager - sync_cluster_files
    mkdir_p)
from matched_or_regexed_in_list import matched_or_regexed_in_list

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
    SNYC_CLUSTER_FILES = settings.SNYC_CLUSTER_FILES
except:
    SNYC_CLUSTER_FILES = False
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

skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)

full_uniques = '%sunique_metrics' % settings.FULL_NAMESPACE

LOCAL_DEBUG = False


class Metrics_Manager(Thread):
    """
    The Analyzer class which controls the metrics_manager thread and
    spawned processes.

    All of this functionality was previously done in the Analyzer thread itself
    however with 10s of 1000s of metrics, this process can take longer than a
    minute to achieve, which would make Analyzer lag.  All the original commits
    and references from the Analyzer code has been maintained here, although
    the logically order has been changed and the blocks ordered in a different,
    but more appropriate an efficient manner than they were laid out in Analyzer.
    Further some blocks from Analyzer were removed as with the new consolidated
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
            if remote_skyline_instance == settings.SKYLINE_URL:
                local_skyline_instance = remote_skyline_instance
                break
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

        # @added 20201214 - Feature #3890: metrics_manager - sync_cluster_files
        #                   Feature #3820: HORIZON_SHARDS
        def assigned_to_shard(metric_name):
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

        def get_remote_data(remote_skyline_instance, data_required, endpoint, save_file=False):
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
                logger.warn('warning :: metrics_manager :: sync_cluster_files - no r from %s on %s' % (
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
                            return False
                        else:
                            file_saved = True
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: sync_cluster_files - failed to get %s from %s' % (
                            endpoint, str(remote_skyline_instance[0])))
                        return False
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
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: sync_cluster_files - failed to build remote_training_data from %s on %s' % (
                            str(data_required), str(remote_skyline_instance)))
            return data

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
                logger.warn('warning :: metrics_manager :: fetched training data has reached the limit of %s, not continuing to fetch more this run' % str(max_training_data_to_fetch))
                break
            remote_training_data = []
            data_required = 'metrics'
            endpoint = 'api?training_data'
            save_file = False
            try:
                remote_training_data = get_remote_data(remote_skyline_instance, data_required, endpoint, save_file)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: sync_cluster_files - failed to get %s from %s' % (
                    endpoint, str(remote_skyline_instance[0])))
            remote_training_data_to_fetch = []
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
                data_required = 'files'
                save_file = False
                for fetch_item in remote_training_data_to_fetch:
                    remote_training_data_files = None
                    try:
                        endpoint = 'ionosphere_files?source=training_data&metric=%s&timestamp=%s' % (
                            str(fetch_item[0]), str(fetch_item[1]))
                        remote_training_data_files = get_remote_data(remote_skyline_instance, data_required, endpoint, save_file)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: metrics_manager :: sync_cluster_files - failed to get %s from %s' % (
                            endpoint, str(remote_skyline_instance[0])))
                    if remote_training_data_files:
                        files_to_fetch = len(remote_training_data_files)
                        files_fetched = 0
                        logger.info('metrics_manager :: sync_cluster_files - %s training_data files to fetch for %s for %s from %s' % (
                            str(files_to_fetch),
                            str(remote_skyline_instance[0])))
                        data_required = 'file_saved'
                        for data_file in remote_training_data_files:
                            try:
                                data_dir = os.path.dirname(data_file)
                                if not os.path.exists(data_dir):
                                    mkdir_p(data_dir)
                                if not os.path.exists(data_dir):
                                    logger.error('error :: metrics_manager :: sync_cluster_files - failed to create dir - %s' % data_dir)
                                    continue
                                endpoint = 'ionosphere_file?file=%s' % str(data_file)
                                file_saved = get_remote_data(remote_skyline_instance, data_required, endpoint, data_file)
                                if not file_saved:
                                    logger.error('error :: metrics_manager :: sync_cluster_files - failed to get %s from %s' % (
                                        data_file, str(remote_skyline_instance[0])))
                                    continue
                                else:
                                    logger.info('metrics_manager :: sync_cluster_files - got %s from %s' % (
                                        data_file, str(remote_skyline_instance[0])))
                                    files_fetched += 1
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: sync_cluster_files - failed to get %s from %s' % (
                                    endpoint, str(remote_skyline_instance[0])))
                        if files_fetched == files_to_fetch:
                            training_data_fetched += 1
                        else:
                            logger.error('error :: metrics_manager :: sync_cluster_files - failed to get all %s training_data files for %s from %s' % (
                                str(files_to_fetch), str(fetch_item), str(remote_skyline_instance[0])))

        # Check what features_profiles directories need to be retrieved from
        # REMOTE_SKYLINE_INSTANCES and saved locally
        logger.info('metrics_manager :: checking for expected_features_profiles_dir')
        expected_features_profile_dirs = {}
        try:
            endpoint = 'api?expected_features_profiles_dir'
            data_required = 'features_profile_dirs'
            save_file = False
            expected_features_profile_dirs = get_remote_data(local_skyline_instance, data_required, endpoint, save_file)
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
                    logger.warn('warning :: metrics_manager :: get_features_profile_dirs has reached the limit of %s, not continuing to fetch more this run' % str(max_fps_to_fetch))
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
                        authorative_host = assigned_to_shard(metric)
                    except Exception as e:
                        logger.error('error :: metrics_manager :: sync_cluster_files - failed to determine authorative_host for %s' % (
                            str(metric)))
                    if authorative_host:
                        USE_REMOTE_SKYLINE_INSTANCES = []
                        for remote_skyline_instance in REMOTE_SKYLINE_INSTANCES:
                            try:
                                if remote_skyline_instance[3] == authorative_host:
                                    USE_REMOTE_SKYLINE_INSTANCES.append(remote_skyline_instance)
                            except Exception as e:
                                logger.error('error :: metrics_manager :: sync_cluster_files - failed to the remote_skyline_instance list for the authorative_host - %s' % (
                                    str(authorative_host)))
                        for remote_skyline_instance in REMOTE_SKYLINE_INSTANCES:
                            try:
                                if remote_skyline_instance[3] != authorative_host:
                                    USE_REMOTE_SKYLINE_INSTANCES.append(remote_skyline_instance)
                            except Exception as e:
                                logger.error('error :: metrics_manager :: sync_cluster_files - failed to the secondary remote_skyline_instance list for the authorative_host - %s' % (
                                    str(authorative_host)))

                for remote_skyline_instance in USE_REMOTE_SKYLINE_INSTANCES:
                    if endpoint:
                        data_required = 'files'
                        save_file = False
                        host_features_profile_files = {}
                        try:
                            host_features_profile_files = get_remote_data(remote_skyline_instance, data_required, endpoint, save_file)
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
                                        logger.error('error :: metrics_manager :: sync_cluster_files - failed to add features_profile_file %s to features_profile_files' % (
                                            str(features_profile_file)))
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
                        for data_file in features_profile_files:
                            try:
                                data_dir = os.path.dirname(data_file)
                                if not os.path.exists(data_dir):
                                    mkdir_p(data_dir)
                                if not os.path.exists(data_dir):
                                    logger.error('error :: metrics_manager :: sync_cluster_files - failed to create dir - %s' % data_dir)
                                    continue
                                endpoint = 'ionosphere_file?file=%s' % str(data_file)
                                # Only fetch it if it does not exist
                                if not os.path.isfile(data_file):
                                    file_saved = False
                                    file_saved = get_remote_data(remote_skyline_instance, data_required, endpoint, data_file)
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
                        except:
                            if not metrics_to_add_error_logged:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: failed to add %s to mirage.hash_key.metrics_resolutions' % base_name)
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

        # @added 20200827 - Feature #3708: FLUX_ZERO_FILL_NAMESPACES
        # Analyzer determines what metrics flux should 0 fill by creating
        # the flux.zero_fill_metrics Redis set, which flux references.  This
        # is done in Analyzer because it manages metric Redis sets as it
        # always runs.  It is only managed in Analyzer every 5 mins.
        if FLUX_ZERO_FILL_NAMESPACES:
            manage_flux_zero_fill_namespaces = False
            flux_zero_fill_metrics = []
            # Only manage every 5 mins
            manage_flux_zero_fill_namespaces_redis_key = 'analyzer.manage_flux_zero_fill_namespaces'
            try:
                manage_flux_zero_fill_namespaces = self.redis_conn.get(manage_flux_zero_fill_namespaces_redis_key)
            except Exception as e:
                if LOCAL_DEBUG:
                    logger.error('error :: metrics_manager :: could not query Redis for analyzer.manage_mirage_unique_metrics key: %s' % str(e))
            if not manage_flux_zero_fill_namespaces:
                logger.info('metrics_manager :: managing FLUX_ZERO_FILL_NAMESPACES Redis sets')
                try:
                    self.redis_conn.delete('analyzer.flux_zero_fill_metrics')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: failed to delete analyzer.flux_zero_fill_metrics Redis set')
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
                    logger.info(traceback.format_exc())
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
                            logger.info(traceback.format_exc())
                            logger.error('error :: metrics_manager :: failed to generate a list from analyzer.flux_zero_fill_metrics Redis set after removals')
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to add multiple members to the analyzer.flux_zero_fill_metrics Redis set')
                else:
                    logger.info('metrics_manager :: no metrics need to remove from analyzer.flux_zero_fill_metrics')
                if flux_zero_fill_metrics_list:
                    # Replace the existing flux.zero_fill_metrics Redis set
                    try:
                        self.redis_conn.sunionstore('flux.zero_fill_metrics', 'analyzer.flux_zero_fill_metrics')
                        logger.info('metrics_manager :: replaced flux.zero_fill_metrics Redis set with the newly created analyzer.flux_zero_fill_metrics set')
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: metrics_manager :: failed to sunionstore flux.zero_fill_metrics from analyzer.flux_zero_fill_metrics Redis sets')
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
            for i, metric_name in enumerate(check_metrics):
                try:
                    try:
                        raw_series = raw_assigned[i]
                        unpacker = Unpacker(use_list=False)
                        unpacker.feed(raw_series)
                        timeseries = list(unpacker)
                    except:
                        timeseries = []

                    timeseries_full_duration = 0
                    if timeseries:
                        first_timeseries_timestamp = 0
                        try:
                            first_timeseries_timestamp = int(timeseries[0][0])
                        except:
                            pass
                        last_timeseries_timestamp = 0
                        try:
                            last_timeseries_timestamp = int(timeseries[-1][0])
                        except:
                            pass
                        timeseries_full_duration = last_timeseries_timestamp - first_timeseries_timestamp
                    if timeseries_full_duration:
                        if last_timeseries_timestamp < (int(check_data_sparsity_start) - settings.STALE_PERIOD):
                            metrics_stale.append(metric_name)
                            # @modified 20201210 - Feature #3870: metrics_manager - check_data_sparsity
                            # Keep stale metrics in the so that when metrics
                            # become stale they do not cause a level shift via
                            # their removal and suggest that things are
                            # recovering
                            # continue
                        if last_timeseries_timestamp < (int(check_data_sparsity_start) - inactive_after):
                            metrics_inactive.append(metric_name)
                            continue

                    metric_resolution = 0
                    if timeseries_full_duration:
                        try:
                            # Determine resolution from the last 30 data points
                            resolution_timestamps = []
                            # @modified 20201215 - Feature #3870: metrics_manager - check_data_sparsity
                            # The last 30 measures for uploaded data to flux
                            # seemed sufficient, however from real time metrics
                            # there is more variance, so using a larger sample
                            # for metric_datapoint in timeseries[-30:]:
                            for metric_datapoint in timeseries[-100:]:
                                timestamp = int(metric_datapoint[0])
                                resolution_timestamps.append(timestamp)
                            timestamp_resolutions = []
                            if resolution_timestamps:
                                last_timestamp = None
                                for timestamp in resolution_timestamps:
                                    if last_timestamp:
                                        resolution = timestamp - last_timestamp
                                        timestamp_resolutions.append(resolution)
                                        last_timestamp = timestamp
                                    else:
                                        last_timestamp = timestamp
                                try:
                                    del resolution_timestamps
                                except:
                                    pass
                            if timestamp_resolutions:
                                try:
                                    timestamp_resolutions_count = Counter(timestamp_resolutions)
                                    ordered_timestamp_resolutions_count = timestamp_resolutions_count.most_common()
                                    metric_resolution = int(ordered_timestamp_resolutions_count[0][0])
                                except:
                                    if not check_sparsity_error_log:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: metrics_manager :: failed to determine metric_resolution from timeseries')
                                        check_sparsity_error_log = True
                                    check_sparsity_error_count += 1
                                try:
                                    del timestamp_resolutions
                                except:
                                    pass
                        except:
                            # @added 20201215 - Feature #3870: metrics_manager - check_data_sparsity
                            if not check_sparsity_error_log:
                                logger.error(traceback.format_exc())
                                logger.error('error :: metrics_manager :: resolution_error - failed to determine metric_resolution')
                                check_sparsity_error_log = True
                            check_sparsity_error_count += 1
                            pass
                        expected_datapoints = 0

                        # @added 20201215 - Feature #3870: metrics_manager - check_data_sparsity
                        # Handle the slight variances that occur in real time
                        # metric streams
                        if metric_resolution in range(57, 63):
                            metric_resolution = 60

                        if metric_resolution:
                            try:
                                expected_datapoints = timeseries_full_duration / metric_resolution
                            except:
                                pass
                        data_sparsity = 0
                        if expected_datapoints:
                            try:
                                datapoints_present = len(timeseries)
                                data_sparsity = round(datapoints_present / expected_datapoints * 100, 2)
                            except Exception as e:
                                if not check_sparsity_error_log:
                                    logger.error('error :: metrics_manager :: an error occurred during check_data_sparsity - %s' % str(e))
                                    check_sparsity_error_log = True
                                check_sparsity_error_count += 1
                        previous_sparsity = 0
                        if last_metrics_data_sparsity:
                            try:
                                previous_sparsity = float(last_metrics_data_sparsity[metric_name])
                            except:
                                pass
                        if data_sparsity:
                            try:
                                self.redis_conn.hset(
                                    'analyzer.metrics_manager.hash_key.metrics_data_sparsity',
                                    metric_name, data_sparsity)
                                added_data_sparsity_keys += 1
                            except:
                                if not check_sparsity_error_log:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: metrics_manager :: failed to add entry in analyzer.metrics_manager.hash_key.metrics_data_sparsity for - %s' % str(metric_name))
                                    check_sparsity_error_log = True
                                check_sparsity_error_count += 1
                            sparsity_change = 0
                            if data_sparsity >= 100:
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
                                'timestamp': last_timeseries_timestamp,
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
                    logger.info('metrics_manager - sent Graphite metric - %s %s' % (send_metric_name, str(metrics_fully_populated_count)))
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
                    logger.info('metrics_manager - sent Graphite metric - %s %s' % (send_metric_name, str(metrics_sparsity_decreasing_count)))
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
                    logger.info('metrics_manager - sent Graphite metric - %s %s' % (send_metric_name, str(metrics_sparsity_increasing_count)))
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
                    logger.info('metrics_manager - sent Graphite metric - %s %s' % (send_metric_name, str(avg_sparsity)))
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

        # @added 20201212 - Feature #3884: ANALYZER_CHECK_LAST_TIMESTAMP
        if ANALYZER_CHECK_LAST_TIMESTAMP:
            try:
                metrics_last_timestamp_dict = self.redis_conn_decoded.hgetall(metrics_last_timestamp_hash_key)
                if metrics_last_timestamp_dict:
                    logger.info('metrics_manager :: ANALYZER_CHECK_LAST_TIMESTAMP - got %s metrics and last analysed timestamps from %s Redis hash key to manage' % (
                        str(len(metrics_last_timestamp_dict)),
                        metrics_last_timestamp_hash_key))
                else:
                    logger.warn('warning :: ANALYZER_CHECK_LAST_TIMESTAMP enabled but got no data from the %s Redis hash key' % (
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

        spin_end = time() - spin_start
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
        except:
            SERVER_METRIC_PATH = ''
        try:
            ANALYZER_ENABLED = settings.ANALYZER_ENABLED
            logger.info('metrics_manager :: ANALYZER_ENABLED is set to %s' % str(ANALYZER_ENABLED))
        except:
            ANALYZER_ENABLED = True
            logger.info('warning :: metrics_manager :: ANALYZER_ENABLED is not declared in settings.py, defaults to True')

        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except:
                logger.error('error :: metrics_manager cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                logger.info(traceback.format_exc())
                sleep(10)
                try:
                    self.redis_conn = get_redis_conn(skyline_app)
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: metrics_manager cannot connect to get_redis_conn')
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
                    if SNYC_CLUSTER_FILES:
                        run_sync_cluster_files = True
                if FAKE_CLUSTER_SYNC:
                    run_sync_cluster_files = True
                    logger.info('metrics_manager :: run_sync_cluster_files as FAKE_CLUSTER_SYNC')
                if run_sync_cluster_files:
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
                                logger.info('metrics_manager :: timed out, killingsync_cluster_files process')
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
                        logger.error('error :: metrics_manager :: sync_cluster_files spawing failed')

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
