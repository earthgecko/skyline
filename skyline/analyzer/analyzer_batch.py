"""
analyzer_batch.py
"""
from __future__ import division
import logging
try:
    from Queue import Empty
except:
    from queue import Empty
from time import time, sleep, strftime, gmtime
from threading import Thread
from collections import defaultdict
# @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
# Use Redis sets in place of Manager().list to reduce memory and number of
# processes
# from multiprocessing import Process, Manager, Queue
from multiprocessing import Process, Queue
import os
from os import kill, getpid
import traceback
import re
from sys import version_info
from sys import exit as sys_exit
# import os.path
from ast import literal_eval
import copy
import json

from msgpack import Unpacker

import settings
from skyline_functions import (
    write_data_to_file, send_anomalous_metric_to, mkdir_p,
    filesafe_metricname,
    # @added 20170602 - Feature #2034: analyse_derivatives
    nonNegativeDerivative, in_list,
    # @added 20191030 - Bug #3266: py3 Redis binary objects not strings
    #                   Branch #3262: py3
    # Added a single functions to deal with Redis connection and the
    # charset='utf-8', decode_responses=True arguments required in py3
    get_redis_conn, get_redis_conn_decoded,
    # @added 20200506 - Feature #3532: Sort all time series
    sort_timeseries,
    # @added 20240524 - Feature #5352: vista - bigquery
    get_graphite_metric,
    )

# @added 20170602 - Feature #2034: analyse_derivatives
# @modified 20220419 - Feature #4528: metrics_manager - derivative_metric_check
#                      Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
# Use strictly_increasing_monotonicity shared function
from functions.timeseries.strictly_increasing_monotonicity import strictly_increasing_monotonicity

# @added 20200425 - Feature #3512: matched_or_regexed_in_list function
#                   Feature #3508: ionosphere_untrainable_metrics
#                   Feature #3486: analyzer_batch
from matched_or_regexed_in_list import matched_or_regexed_in_list

# @added 20220406 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
#                   Feature #4520: settings - ZERO_FILL_NAMESPACES
from functions.metrics.last_known_value_metrics_list import last_known_value_metrics_list
from functions.metrics.zero_fill_metrics_list import zero_fill_metrics_list
# @added 20220407 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
#                   Feature #4520: settings - ZERO_FILL_NAMESPACES
from functions.timeseries.full_duration_timeseries_fill import full_duration_timeseries_fill
# @added 20220421 - Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
#                   Task #3868: POC MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
from functions.metrics.non_derivative_metrics_list import non_derivative_metrics_list

# @added 20220504 - Feature #2580: illuminance
from functions.illuminance.add_illuminance_entries import add_illuminance_entries

# @modified 20200423 - Feature #3504: Handle airgaps in batch metrics
#                      Feature #3480: batch_processing
#                      Feature #3486: analyzer_batch
# Changed to algoritms_batch so there is no pollution and
# analyzer and analyzer_batch are totally independent
# from algorithms import run_selected_algorithm
from algorithms_batch import run_selected_batch_algorithm

from algorithm_exceptions import TooShort, Stale, Boring

# @added 20220919 - Feature #4676: analyzer - illuminance.all key
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name

# @added 20240520 - Feature #5352: vista - bigquery
from functions.settings.get_bq_accounts_settings import get_bq_accounts_settings
from functions.timeseries.determine_data_frequency import determine_data_frequency
# @added 20240524 - Feature #5352: vista - bigquery
from functions.redis.populate_redis_metric import populate_redis_metric

# @added 20240602 - Feature #5368: analyzer_batch - reprocess
from functions.cluster.is_shard_metric import is_shard_metric

# TODO if settings.ENABLE_CRUCIBLE: and ENABLE_PANORAMA
#    from spectrum import push_to_crucible

skyline_app = 'analyzer_batch'
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

# @added 20190410 - Feature #2916: ANALYZER_ENABLED setting
try:
    ANALYZER_ENABLED = settings.ANALYZER_ENABLED
    logger.info('ANALYZER_ENABLED is set to %s' % str(ANALYZER_ENABLED))
except:
    ANALYZER_ENABLED = True
    logger.info('warning :: ANALYZER_ENABLED is not declared in settings.py, defaults to True')

try:
    from settings import BATCH_PROCESSING
except:
    BATCH_PROCESSING = None

# @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
# Determine if any metrcs have negatives values some they can be
# added to the ionosphere.untrainable_metrics Redis set
try:
    # @modified 20200606 - Bug #3572: Apply list to settings import
    KNOWN_NEGATIVE_METRICS = list(settings.KNOWN_NEGATIVE_METRICS)
except:
    KNOWN_NEGATIVE_METRICS = []

# @added 20200607 - Feature #3566: custom_algorithms
try:
    CUSTOM_ALGORITHMS = copy.deepcopy(settings.CUSTOM_ALGORITHMS)
except:
    CUSTOM_ALGORITHMS = None
# @added 20230616 - Feature #3566: custom_algorithms
# Filter out only the ones in which analyzer is
# declared in use_with
if CUSTOM_ALGORITHMS:
    ASSIGNED_CUSTOM_ALGORITHMS = {}
    for custom_algorithm in CUSTOM_ALGORITHMS:
        try:
            if 'analyzer' in CUSTOM_ALGORITHMS[custom_algorithm]['use_with']:
                ASSIGNED_CUSTOM_ALGORITHMS[custom_algorithm] = copy.deepcopy(CUSTOM_ALGORITHMS[custom_algorithm])
        except:
            pass
    CUSTOM_ALGORITHMS = copy.deepcopy(ASSIGNED_CUSTOM_ALGORITHMS)

try:
    DEBUG_CUSTOM_ALGORITHMS = settings.DEBUG_CUSTOM_ALGORITHMS
except:
    DEBUG_CUSTOM_ALGORITHMS = False

# @added 20200727 - Feature #3650: ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
#                   Feature #3480: batch_processing
#                   Feature #3486: analyzer_batch
try:
    from settings import ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
except:
    ROOMBA_DO_NOT_PROCESS_BATCH_METRICS = False
if ROOMBA_DO_NOT_PROCESS_BATCH_METRICS:
    try:
        from types import TupleType
    except ImportError:
        eliminated_in_python3 = True
    from redis import WatchError
    from msgpack import packb
# @added 20200817 - Feature #3684: ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS
#                   Feature #3650: ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
#                   Feature #3480: batch_processing
# Allow for custom durations on namespaces
ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS = []
if ROOMBA_DO_NOT_PROCESS_BATCH_METRICS:
    try:
        from settings import ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS
    except:
        ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS = []

# @added 20240520 - Feature #5352: vista - bigquery
BATCH_METRICS_CUSTOM_FULL_DURATIONS = {}
try:
    BATCH_METRICS_CUSTOM_FULL_DURATIONS = copy.deepcopy(settings.BATCH_METRICS_CUSTOM_FULL_DURATIONS)
except:
    BATCH_METRICS_CUSTOM_FULL_DURATIONS = {}

# @added 20200815 - Feature #3678:  SNAB - anomalyScore
try:
    SNAB_DATA_DIR = settings.SNAB_DATA_DIR
except:
    SNAB_DATA_DIR = '/opt/skyline/SNAB'
try:
    SNAB_anomalyScore = settings.SNAB_anomalyScore
except:
    SNAB_anomalyScore = {}

# @added 20201017 - Feature #3818: ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED
try:
    ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED = settings.ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED
except:
    ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED = False
try:
    BATCH_MODE = settings.BATCH_PROCESSING_BATCH_MODE
except:
    BATCH_MODE = True

# @added 20240602 - Feature #5368: analyzer_batch - reprocess
try:
    HORIZON_SHARDS = copy.deepcopy(settings.HORIZON_SHARDS)
except:
    HORIZON_SHARDS = {}
number_of_horizon_shards = 0
this_host = str(os.uname()[1])
HORIZON_SHARD = 0
if HORIZON_SHARDS:
    number_of_horizon_shards = len(HORIZON_SHARDS)
    HORIZON_SHARD = HORIZON_SHARDS[this_host]

# @added 20240602 - Feature #5368: analyzer_batch - reprocess
try:
    ANALYZER_BATCH_PROCESSES = settings.ANALYZER_BATCH_PROCESSES
except:
    # ANALYZER_BATCH_PROCESSES = 2
    ANALYZER_BATCH_PROCESSES = 8


skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)

LOCAL_DEBUG = False


class AnalyzerBatch(Thread):
    """
    The AnalyzerBatch class which controls the analyzer.batch thread and spawned
    processes.

    Made with love to the analyzer_batch playlist:
    https://soundcloud.com/earthgecko/sets/analyzer_batch
    https://soundcloud.com/thedeltariggs/ode-to-jeremiah (I can't tell what I've seen..)
    https://soundcloud.com/egroove/premiere-francesco-chiocci-feat-black-soda-musumeci-remix-connaisseur-recordings (picking up pieces of my weary mind)
    https://soundcloud.com/when-we-dip/premiere-francesco-chiocci-ft-black-soda-black-sunrise-peter-pardeike-remix
    https://soundcloud.com/timgreen/atelier-francesco-manuel-feat-astrid-dead-end-tim-green-remixcityfox-1
    https://soundcloud.com/imbernonmusic/edu-imbernon-fixing-fires
    https://soundcloud.com/deep-house-amsterdam/oliver-koletzki-deep-house-amsterdam-dgtl-podcast-007
    https://soundcloud.com/crosstownrebels/crm140-damian-lazarus-the-ancient-moons-vermillion-agoria-remix-1
    https://soundcloud.com/wiewouwat/joy-wellboy-before-the-sunrise
    https://soundcloud.com/agoria/damian-lazarus-the-ancent-moons-vermillion-agoria-remix
    https://soundcloud.com/wearesoundspace/premiere-just-her-feat-kieran-fowkes-let-myself-go
    https://soundcloud.com/watergaterecords/matthias-meyer-november-rain
    https://soundcloud.com/musicthatmakesmewannasurf/mixtape-2-w-kosson
    """

    def __init__(self, parent_pid):
        """
        Initialize the AnalyzerBatch

        Create the :obj:`self.batch_exceptions_q` queue
        Create the :obj:`self.batch_anomaly_breakdown_q` queue

        """
        super(AnalyzerBatch, self).__init__()
        self.redis_conn = get_redis_conn(skyline_app)
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        self.batch_exceptions_q = Queue()
        self.batch_anomaly_breakdown_q = Queue()

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            sys_exit(0)

    # @modified 20240602 - Feature #5368: analyzer_batch - reprocess
    # Added reprocess_work
    def spin_batch_process(
            self, i, run_timestamp, metric_name, last_analyzed_timestamp,
            batch=[], reprocess_work={}):
        """
        Assign a metric and last_analyzed_timestamp for a process to analyze.

        :param i: python process id
        :param run_timestamp: the epoch timestamp at which this process was called
        :param metric_name: the FULL_NAMESPACE metric name as keyed in Redis
        :param last_analyzed_timestamp: the last analysed timestamp as recorded
            in the Redis key last_timestamp.basename key.

        :return: returns True

        """

        spin_start = time()
        child_batch_process_pid = os.getpid()

        LOCAL_DEBUG = False

        metrics_processed = 0

        # @modified 20240602 - Feature #5368: analyzer_batch - reprocess
        # Added reprocess_work
        if not reprocess_work:
            if not batch:
                batch_mode = False
                metrics = [[metric_name, last_analyzed_timestamp]]
                logger.info('child_batch_process_pid - %s, processing %s from %s' % (
                    str(child_batch_process_pid), metric_name, str(last_analyzed_timestamp)))
            else:
                batch_mode = True
                metrics = batch
                number_of_metrics = len(batch)
                logger.info('child_batch_process_pid - %s, processing %s metrics in batch mode' % (
                    str(child_batch_process_pid), str(number_of_metrics)))

        # @added 20240522 - Task #5358: analyzer.last_timestamp.base_names Redis hash to replace last_timestamp keys
        #                   Feature #5352: vista - bigquery
        redis_last_timestamp_hash = 'analyzer.last_timestamp.base_names'
        redis_reprocess_hash = 'analyzer.batch.reprocess_work'

        # @modified 20240602 - Feature #5368: analyzer_batch - reprocess
        # Added reprocess_work
        full_uniques = settings.FULL_NAMESPACE + 'unique_metrics'
        reprocess = False
        dry_run = True
        # All reprocess work in a job will have the
        original_reprocess_until = 0
        if reprocess_work:
            batch_mode = True
            reprocess = False
            metrics = []
            for base_name, reprocess_dict in reprocess_work.items():
                try:
                    reprocess_from = reprocess_dict['from_timestamp']
                except KeyError:
                    reprocess_from = 0
                try:
                    reprocess_until = reprocess_dict['until_timestamp']
                    original_reprocess_until = int(reprocess_until)
                except KeyError:
                    reprocess_until = 0
                last_redis_timestamp = 0
                try:
                    last_redis_timestamp_data = self.redis_conn_decoded.hget(redis_last_timestamp_hash, base_name)
                    if last_redis_timestamp_data:
                        last_redis_timestamp = int(last_redis_timestamp_data)
                except Exception as err:
                    logger.error('error :: failed to %s from get Redis hash %s, err: %s' % (
                        base_name, redis_last_timestamp_hash, err))
                if not reprocess_until and last_redis_timestamp:
                    reprocess_until = int(last_redis_timestamp)
                if last_redis_timestamp:
                    if reprocess_until > last_redis_timestamp:
                        logger.info('setting reprocess_until: %s to %s as that is the last Redis timestamp for %s' % (
                            str(reprocess_until), str(last_redis_timestamp), base_name))
                        reprocess_until = int(last_redis_timestamp)
                try:
                    dry_run = reprocess_dict['dry_run']
                except KeyError:
                    dry_run = True
                # @added 20240607 - Feature #5368: analyzer_batch - reprocess
                # Allow for oneshot analysis
                try:
                    oneshot = reprocess_dict['oneshot']
                except KeyError:
                    oneshot = False

                metrics.append([base_name, reprocess_until, reprocess_from, dry_run, oneshot])
                reprocess = True
            number_of_metrics = len(reprocess_work)
            logger.info('child_batch_process_pid - %s, reprocessing %s metrics in batch mode, dry_run: %s, oneshot: %s' % (
                str(child_batch_process_pid), str(number_of_metrics),
                str(dry_run), str(oneshot)))

        # Make process-specific dicts
        exceptions = defaultdict(int)
        anomaly_breakdown = defaultdict(int)

        # Determine the unique Mirage and Ionosphere metrics once, which are
        # used later to determine how Analyzer should handle/route anomalies
        try:
            mirage_unique_metrics = list(self.redis_conn_decoded.smembers('mirage.unique_metrics'))
        except:
            mirage_unique_metrics = []
        try:
            ionosphere_unique_metrics = list(self.redis_conn_decoded.smembers('ionosphere.unique_metrics'))
        except:
            ionosphere_unique_metrics = []

        # In order to convert monotonic, incrementing metrics to a deriative
        # metric
        try:
            # @modified 20211012 - Feature #4280: aet.metrics_manager.derivative_metrics Redis hash
            # derivative_metrics = list(self.redis_conn_decoded.smembers('derivative_metrics'))
            derivative_metrics = list(self.redis_conn_decoded.smembers('aet.metrics_manager.derivative_metrics'))
        except:
            derivative_metrics = []

        # @added 20220323 - Feature #4502: settings - MONOTONIC_METRIC_NAMESPACES
        always_derivative_metrics = []
        try:
            always_derivative_metrics = list(self.redis_conn_decoded.smembers('metrics_manager.always_derivative_metrics'))
        except Exception as err:
            logger.error('error :: failed to get metrics_manager.always_derivative_metrics Redis set - %s' % str(err))
        if always_derivative_metrics:
            all_derivative_metrics = derivative_metrics + always_derivative_metrics
            derivative_metrics = list(set(all_derivative_metrics))

        try:
            non_derivative_metrics = list(self.redis_conn_decoded.smembers('non_derivative_metrics'))
        except:
            non_derivative_metrics = []

        # @added 20220419 - Feature #4528: metrics_manager - derivative_metric_check
        #                   Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
        longterm_non_derivative_metrics = []
        try:
            longterm_non_derivative_metrics_dict = self.redis_conn_decoded.hgetall('metrics_manager.longterm_non_derivative_metrics')
            longterm_non_derivative_metrics = list(longterm_non_derivative_metrics_dict.keys())
            non_derivative_metrics = list(set(non_derivative_metrics + longterm_non_derivative_metrics))
        except Exception as err:
            logger.error('error :: metrics_manager :: derivative_metric_check :: failed to hgetall metrics_manager.longterm_non_derivative_metrics - %s' % str(err))

        try:
            # @modified 20200606 - Bug #3572: Apply list to settings import
            non_derivative_monotonic_metrics = list(settings.NON_DERIVATIVE_MONOTONIC_METRICS)
        except:
            non_derivative_monotonic_metrics = []

        # @added 20220421 - Feature #4528: metrics_manager - derivative_metric_check
        #                   Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
        for longterm_non_derivative_metric in longterm_non_derivative_metrics:
            if longterm_non_derivative_metric.startswith(settings.FULL_NAMESPACE):
                longterm_non_derivative_base_name = longterm_non_derivative_metric.replace(settings.FULL_NAMESPACE, '', 1)
            else:
                longterm_non_derivative_base_name = str(longterm_non_derivative_metric)
            non_derivative_monotonic_metrics.append(longterm_non_derivative_base_name)

        non_smtp_alerter_metrics = []
        try:
            non_smtp_alerter_metrics = list(self.redis_conn_decoded.smembers('analyzer.non_smtp_alerter_metrics'))
        except:
            non_smtp_alerter_metrics = []

        # @added 20220406 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
        #                   Feature #4520: settings - ZERO_FILL_NAMESPACES
        last_known_value_metrics = []
        try:
            last_known_value_metrics = last_known_value_metrics_list(skyline_app)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: last_known_value_metrics_list failed - %s' % err)
        zero_fill_metrics = []
        try:
            zero_fill_metrics = zero_fill_metrics_list(skyline_app)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: zero_fill_metrics_list failed - %s' % err)
        # @added 20220421 - Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
        #                   Task #3868: POC MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
        user_non_derivative_metrics = []
        try:
            user_non_derivative_metrics = non_derivative_metrics_list(skyline_app)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: non_derivative_metrics_list failed - %s' % err)

        # @modified 20240104 - Task #5088: Change membership of the list checks to sets
        #                      Task #5178: Build and test skyline v4.1.0
        # all_non_derivative_metrics = list(set(user_non_derivative_metrics + non_derivative_monotonic_metrics))
        all_non_derivative_metrics = set(user_non_derivative_metrics + non_derivative_monotonic_metrics)

        # @added 20240520 - Feature #5352: vista - bigquery
        zero_fill_analysis_namespaces = []
        non_derivative_bq_namespaces = []
        namespace_resolutions = {}
        bq_accounts_settings = {}
        try:
            bq_accounts_settings = get_bq_accounts_settings(skyline_app)
        except Exception as err:
            logger.error('error :: get_bq_accounts_settings failed, err: %s' % err)
        for bq_account, bq_account_settings in bq_accounts_settings.items():
            try:
                batch_processing_namespace = bq_account_settings['batch_processing_namespace']
                namespace_full_duration = int(bq_account_settings['full_duration'])
            except Exception as err:
                logger.error('error :: failed to determine batch_processing_namespace and full_duration from bq_accounts_settings for %s, err: %s' % (
                    bq_account, err))
                continue
            try:
                BATCH_METRICS_CUSTOM_FULL_DURATIONS[batch_processing_namespace] = namespace_full_duration
                roomba_item = [batch_processing_namespace, namespace_full_duration]
                if roomba_item not in ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS:
                    ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS.append(roomba_item)
            except Exception as err:
                logger.error('error :: failed to add batch_processing_namespace and full_duration from bq_accounts_settings for %s to BATCH_METRICS_CUSTOM_FULL_DURATIONS and ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS, err: %s' % (
                    bq_account, err))
            zero_fill_analysis = False
            try:
                zero_fill_analysis = bq_account_settings['zero_fill_analysis']
            except:
                zero_fill_analysis = False
            if zero_fill_analysis:
                zero_fill_analysis_namespaces.append(batch_processing_namespace)
            non_derivative_namespaces = None
            try:
                non_derivative_namespaces = bq_account_settings['non_derivative_namespaces']
            except:
                non_derivative_namespaces = None
            if non_derivative_namespaces:
                if isinstance(non_derivative_namespaces, list):
                    non_derivative_bq_namespaces = non_derivative_bq_namespaces + non_derivative_namespaces
            namespace_resolution = None
            try:
                namespace_resolution = int(bq_account_settings['resolution'])
            except:
                namespace_resolution = None
            if namespace_resolution:
                namespace_resolutions[batch_processing_namespace] = namespace_resolution

        # @added 20240522 - Task #5358: analyzer.last_timestamp.base_names Redis hash to replace last_timestamp keys
        #                   Feature #5352: vista - bigquery
        last_timestamps_hash_dict = {}

        # @added 20220420 - Feature #4530: namespace.analysed_events
        analysed_metrics = []

        # @added 20240725 - Feature #5400: analyzer_batch - stats
        stats = {}
        total_anomalies = 0

        # @added 20220504 - Feature #2580: illuminance
        illuminance_dict = {}
        # @added 20220919 - Feature #4676: analyzer - illuminance.all key
        illuminance_all_dict = {}
        errors = []
        algorithms = {}
        try:
            algorithms = self.redis_conn_decoded.hgetall('metrics_manager.algorithms.ids')
        except Exception as err:
            logger.error('error :: hgetall metrics_manager.algorithms.ids - %s' % str(err))

        for item in metrics:
            metric_name = item[0]
            last_analyzed_timestamp = item[1]
            reprocess_until = None

            # @added 20240607 - Feature #5368: analyzer_batch - reprocess
            # Allow for oneshot analysis
            oneshot = False

            # @added 20240602 - Feature #5368: analyzer_batch - reprocess
            shard_metric = None
            timeseries = []
            dry_run = False
            if reprocess:
                reprocess_until = int(last_analyzed_timestamp)
                try:
                    reprocess_from = item[2]
                except:
                    reprocess_from = 0
                try:
                    dry_run = item[3]
                except:
                    dry_run = True
                # @added 20240607 - Feature #5368: analyzer_batch - reprocess
                # Allow for oneshot analysis
                try:
                    oneshot = item[4]
                except:
                    oneshot = False

            if int(time()) > (spin_start + 295):
                logger.info('approaching max runtime, stopping')
                break

            if batch_mode:
                metrics_processed += 1
                logger.info('processing metric %s of %s' % (
                    str(metrics_processed), str(number_of_metrics)))
            # Identify last timestamp
            metric_timestamp = None
            # Identify anomalies
            # Handle EXPIRATION_TIME
            # Ship to Analyzer, Mirage or Ionosphere

            # @added 20200728 - Feature #3480: batch_processing
            #                   Feature #3486: analyzer_batch
            # If multiple work items exist and the timestamp in the work item is
            # older than the last analyzed timestamp reported by Redis key, just
            # skip and remove the work item
            if metric_name.startswith(settings.FULL_NAMESPACE):
                base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
            else:
                base_name = metric_name

            if not metric_name.startswith(settings.FULL_NAMESPACE):
                metric_name = '%s%s' % (settings.FULL_NAMESPACE, metric_name)

            # Check the last_timestamp metric Redis key
            last_metric_timestamp_key = 'last_timestamp.%s' % base_name
            redis_key_set = None
            last_redis_timestamp = 0

            # @added 20240522 - Task #5358: analyzer.last_timestamp.base_names Redis hash to replace last_timestamp keys
            try:
                last_redis_timestamp_data = self.redis_conn_decoded.hget(redis_last_timestamp_hash, base_name)
                if last_redis_timestamp_data:
                    last_redis_timestamp = int(last_redis_timestamp_data)
            except Exception as err:
                logger.error('error :: failed to %s from get Redis hash %s, err: %s' % (
                    base_name, redis_last_timestamp_hash, err))

            # @modified 20240522 - Task #5358: analyzer.last_timestamp.base_names Redis hash to replace last_timestamp keys
            # Only try key if not found in hash
            if not last_redis_timestamp:
                try:
                    last_redis_timestamp_data = self.redis_conn_decoded.get(last_metric_timestamp_key)
                    last_redis_timestamp = int(last_redis_timestamp_data)
                except:
                    logger.error('error :: failed to get Redis key %s' % last_metric_timestamp_key)

            get_raw_series = True
            if last_redis_timestamp:
                if last_redis_timestamp > last_analyzed_timestamp:
                    get_raw_series = False
                    logger.info('The %s is %s, the passed last_analyzed_timestamp is %s, not getting raw_series returning' % (
                        last_metric_timestamp_key, str(last_redis_timestamp),
                        str(last_analyzed_timestamp)))

            # @added 20240602 - Feature #5368: analyzer_batch - reprocess
            # Allow analyzer_batch to reprocess metrics
            if reprocess:
                logger.info('reprocess_from: %s, reprocessing %s, the passed last_analyzed_timestamp is %s, getting raw_series' % (
                    str(reprocess_from), base_name, str(last_analyzed_timestamp)))
                get_raw_series = True

            if LOCAL_DEBUG:
                logger.debug('debug :: getting Redis time series data for %s, last_analyzed_timestamp: %s' % (base_name, str(last_analyzed_timestamp)))

            raw_series = None
            # @modified 20200728 - Feature #3480: batch_processing
            #                      Feature #3486: analyzer_batch
            # Only resurface the timeseries if the work item timestamp is greater
            # than the last analyzed timestamp reported by Redis key
            if get_raw_series:
                try:
                    raw_series = self.redis_conn.get(metric_name)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to get %s from Redis' % metric_name)
                    raw_series = None

            if not raw_series and not reprocess:
                logger.info('No raw_series defined, returning')

                # Remove for work list
                redis_set = 'analyzer.batch'
                data = [metric_name, int(last_analyzed_timestamp)]
                try:
                    self.redis_conn.srem(redis_set, str(data))
                    logger.info('analyzer_batch :: removed batch metric item - %s - from Redis set - %s' % (str(data), redis_set))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: analyzer_batch :: failed to remove batch metric item - %s - from Redis set - %s' % (str(data), redis_set))

                if batch_mode:
                    continue
                return

            try:
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)
            except:
                timeseries = []

            # @added 20200506 - Feature #3532: Sort all time series
            # To ensure that there are no unordered timestamps in the time
            # series which are artefacts of the collector or carbon-relay, sort
            # all time series by timestamp before analysis.
            original_timeseries = timeseries
            if original_timeseries:
                timeseries = sort_timeseries(original_timeseries)
                del original_timeseries

            try:
                del raw_series
            except:
                pass

            try:
                first_timeseries_timestamp = timeseries[0][0]
                last_timeseries_timestamp = timeseries[-1][0]
            except:
                first_timeseries_timestamp = None
                last_timeseries_timestamp = None

            if LOCAL_DEBUG:
                logger.debug('debug :: got Redis time series data for %s, from: %s, until: %s' % (
                    base_name, str(first_timeseries_timestamp),
                    str(last_timeseries_timestamp)))

            # @added 20240519 - Feature #5352: vista - bigquery
            use_full_duration = settings.FULL_DURATION
            for batch_processing_namespace, namespace_full_duration in BATCH_METRICS_CUSTOM_FULL_DURATIONS.items():
                if batch_processing_namespace in base_name:
                    use_full_duration = int(namespace_full_duration)
                    break

            # @added 20200727 - Feature #3650: ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
            #                   Feature #3480: batch_processing
            #                   Feature #3486: analyzer_batch
            # euthanize keys if not done in roomba, allows for backfill processing
            # via analyzer_batch
            roombaed = False
            # if ROOMBA_DO_NOT_PROCESS_BATCH_METRICS:
            if ROOMBA_DO_NOT_PROCESS_BATCH_METRICS and len(timeseries) > 0:
                if LOCAL_DEBUG:
                    logger.debug('debug :: checking if roomba needs to be run on %s' % (base_name))
                now = int(time())
                duration = settings.FULL_DURATION + settings.ROOMBA_GRACE_TIME
                key = metric_name

                # @added 20220113 - Feature #3566: custom_algorithms
                #                   Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
                # Enable pruning at custom_duration
                custom_duration = False

                # @added 20200817 - Feature #3684: ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS
                #                   Feature #3650: ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
                #                   Feature #3480: batch_processing
                # Allow for custom durations on namespaces, this is for testing to
                # allow the Redis key to have data at a different resolution than
                # FULL_DURATION, which allows for feeding a metric at 1 data point
                # per 10 mins (ala fake Mirage)
                try:
                    if ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS:
                        for metric_namespace, i_custom_full_duration in ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS:
                            if metric_namespace in base_name:
                                duration = i_custom_full_duration + settings.ROOMBA_GRACE_TIME
                                logger.info('batch_processing :: %s found in ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS, duration for roomba set to %s' % (
                                    base_name, str(duration)))
                                # @added 20220113 - Feature #3566: custom_algorithms
                                #                   Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
                                custom_duration = True
                                break

                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: analyzer_batch :: failed to remove batch metric item - %s - from Redis set - %s' % (str(data), redis_set))

                namespace_unique_metrics = '%sunique_metrics' % str(settings.FULL_NAMESPACE)
                euthanized = 0
                trimmed_keys = 0
                active_keys = 0
                try:
                    # Put pipe back in multi mode
                    pipe = self.redis_conn.pipeline()
                    # WATCH the key
                    pipe.watch(key)
                    pipe.multi()
                    # There's one value. Purge if it's too old
                    last_timestamp = int(timeseries[-1][0])
                    # Do not purge if it has not been analyzed
                    if (last_timestamp - duration) > last_analyzed_timestamp:
                        logger.info('batch_processing :: last_timestamp is %s, but for roomba setting to the last_analyzed_timestamp (%s) as it has not been analyzed' % (
                            str(last_timestamp), str(last_analyzed_timestamp)))
                        last_timestamp = last_analyzed_timestamp

                    # @added 20220113 - Feature #3566: custom_algorithms
                    #                   Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
                    # I am not 100% certain how this will affect historical
                    # batch processing
                    drop_old_data = False
                    if custom_duration and last_timestamp < (int(timeseries[-1][0]) - duration):
                        last_timestamp = int(time()) - duration
                        logger.info('batch_processing :: custom_duration set last_timestamp to %s' % (
                            str(last_timestamp)))
                        drop_old_data = True

                    now = int(last_analyzed_timestamp)

                    # @added 20220113 - Feature #3566: custom_algorithms
                    #                   Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
                    if drop_old_data:
                        now = last_timestamp

                    logger.info('batch_processing :: doing roomba on %s with %s data points' % (key, str(len(timeseries))))
                    roombaed = True
                    # Delete the key if it is not a list of tuples and the
                    # last timestamp is old, e.g. bad data
                    try:
                        if python_version == 2:
                            if not isinstance(timeseries[0], TupleType):
                                if timeseries[0] < last_timestamp - duration:
                                    pipe.delete(key)
                                    pipe.srem(namespace_unique_metrics, key)
                                    pipe.execute()
                                    euthanized += 1
                                timeseries = []
                        if python_version == 3:
                            if not isinstance(timeseries[0], tuple):
                                if timeseries[0] < now - duration:
                                    pipe.delete(key)
                                    pipe.srem(namespace_unique_metrics, key)
                                    pipe.execute()
                                    euthanized += 1
                                timeseries = []
                    except IndexError:
                        timeseries = []
                    # Check if the last value is too old and purge
                    if timeseries[-1][0] < now - duration:
                        pipe.delete(key)
                        pipe.srem(namespace_unique_metrics, key)
                        pipe.execute()
                        euthanized += 1
                        timeseries = []
                    # Remove old datapoints and duplicates from timeseries
                    temp = set()
                    temp_add = temp.add
                    delta = now - duration
                    trimmed = [
                        tuple for tuple in timeseries
                        if tuple[0] > delta and
                        tuple[0] not in temp and not
                        temp_add(tuple[0])
                    ]
                    # Purge if everything was deleted, set key otherwise
                    if len(trimmed) > 0:
                        # Serialize and turn key back into not-an-array
                        btrimmed = packb(trimmed)
                        if len(trimmed) <= 15:
                            value = btrimmed[1:]
                        elif len(trimmed) <= 65535:
                            value = btrimmed[3:]
                            trimmed_keys += 1
                        else:
                            value = btrimmed[5:]
                            trimmed_keys += 1
                        pipe.set(key, value)
                        active_keys += 1
                    else:
                        pipe.delete(key)
                        pipe.srem(namespace_unique_metrics, key)
                        euthanized += 1
                    pipe.execute()
                except WatchError:
                    logger.info('batch_processing :: blocked from euthanizing %s' % (key))
                except Exception as e:
                    # If something bad happens, zap the key and hope it goes away
                    # pipe.delete(key)
                    # pipe.srem(namespace_unique_metrics, key)
                    # pipe.execute()
                    # euthanized += 1
                    logger.info(e)
                    logger.info('batch_processing :: something bad happened but not euthanizing %s' % (key))
                finally:
                    pipe.reset()
                raw_series = None
                try:
                    raw_series = self.redis_conn.get(metric_name)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to get %s from Redis' % metric_name)
                    raw_series = None
                if not raw_series:
                    logger.info('No raw_series defined after euthanizing %s, returning' % (key))
                    # Remove for work list
                    redis_set = 'analyzer.batch'
                    data = [metric_name, int(last_analyzed_timestamp)]
                    try:
                        self.redis_conn.srem(redis_set, str(data))
                        logger.info('analyzer_batch :: removed batch metric item - %s - from Redis set - %s' % (str(data), redis_set))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: analyzer_batch :: failed to remove batch metric item - %s - from Redis set - %s' % (str(data), redis_set))
                    if batch_mode:
                        continue
                    return

                try:
                    unpacker = Unpacker(use_list=False)
                    unpacker.feed(raw_series)
                    timeseries = list(unpacker)
                    if roombaed:
                        logger.info('batch_processing :: after roomba %s has %s data points' % (key, str(len(timeseries))))
                except:
                    timeseries = []

                # @added 20200506 - Feature #3532: Sort all time series
                # To ensure that there are no unordered timestamps in the time
                # series which are artefacts of the collector or carbon-relay, sort
                # all time series by timestamp before analysis.
                original_timeseries = list(timeseries)
                if original_timeseries:
                    timeseries = sort_timeseries(original_timeseries)
                    del original_timeseries
                try:
                    del raw_series
                except:
                    pass

                try:
                    first_timeseries_timestamp_after_roomba = timeseries[0][0]
                    last_timeseries_timestamp_after_roomba = timeseries[-1][0]
                except:
                    first_timeseries_timestamp_after_roomba = None
                    last_timeseries_timestamp_after_roomba = None

                if LOCAL_DEBUG:
                    logger.debug('debug :: got Redis time series data for %s after roomba, from: %s, until: %s' % (
                        base_name, str(first_timeseries_timestamp_after_roomba),
                        str(last_timeseries_timestamp_after_roomba)))

            # @added 20211124 - Task #4322: Handle historical batch metrics full duration
            # first_last_duration_timestamp = timeseries[-1][0] - settings.FULL_DURATION
            # @modified 20240520 - Feature #5352: vista - bigquery
            # first_last_duration_timestamp = last_analyzed_timestamp - settings.FULL_DURATION
            first_last_duration_timestamp = last_analyzed_timestamp - use_full_duration

            timeseries_length = len(timeseries)
            full_duration_timeseries = []
            try:
                full_duration_timeseries = [item for item in timeseries if item[0] >= first_last_duration_timestamp]
                if full_duration_timeseries:
                    full_duration_timeseries_length = len(full_duration_timeseries)
                    timeseries = list(full_duration_timeseries)
                    del full_duration_timeseries
                    if timeseries_length != full_duration_timeseries_length:
                        logger.info('timeseries has been pruned from %s datapoints to its FULL_DURATION %s datapoints' % (
                            str(timeseries_length), str(full_duration_timeseries_length)))
            except:
                timeseries = []

            # @added 20240604 - Feature #5352: vista - bigquery
            # Ensure that only metrics belonging to the shard are processed on
            # the node
            if HORIZON_SHARDS:
                shard_metric = is_shard_metric(base_name)
                if not shard_metric:
                    try:
                        self.redis_conn_decoded.srem(full_uniques, metric_name)
                    except Exception as err:
                        logger.error('error :: failed to srem %s from %s, err: %s' % (
                            base_name, full_uniques, err))                        
                    try:
                        self.redis_conn_decoded.delete(metric_name)
                    except Exception as err:
                        logger.error('error :: failed to delete Redis key %s, err: %s' % (
                            metric_name, err))                        
                    continue

            # @added 20240602 - Feature #5368: analyzer_batch - reprocess
            if reprocess:
                try:
                    self.redis_conn.hdel(redis_reprocess_hash, base_name)
                    logger.info('analyzer_batch :: processing, removed reprocess metric %s from Redis hash %s' % (
                        base_name, redis_reprocess_hash))
                except Exception as err:
                    logger.error('error :: analyzer_batch :: failed to remove %s from Redis hash %s, err: %s' % (
                        base_name, redis_reprocess_hash, err))

            # @added 20240524 - Feature #5352: vista - bigquery
            # Allow to autofill from Graphite, this mitigates situations where
            # data is backfilled and roomba prunes it before the metric is
            # assigned as a batch metric internally
            fetch_data_from_graphite = False
            replace_redis_data_with_graphite_data = False
            last_filled_from_graphite = None
            if len(timeseries) <= 100:
                fetch_data_from_graphite = True
                try:
                    last_filled_from_graphite = self.redis_conn_decoded.hget('analyzer_batch.last_filled_from_graphite', base_name)
                except Exception as err:
                    logger.error('error :: analyzer_batch :: hget failed on analyzer_batch.last_filled_from_graphite for %s, err: %s' % (
                        base_name, err))
                if last_filled_from_graphite:
                    # @modified 20240604 - Feature #5368: analyzer_batch - reprocess
                    # if (int(float(last_filled_from_graphite)) + 90001) < int(time()):
                    if (int(float(last_filled_from_graphite)) + 300) < int(time()):
                        fetch_data_from_graphite = False
                # @added 20240604 - Feature #5368: analyzer_batch - reprocess
                if reprocess:
                    fetch_data_from_graphite = True

                logger.info('short time series for %s, fetch_data_from_graphite: %s, last_filled_from_graphite: %s' % (
                    base_name, str(fetch_data_from_graphite),
                    str(last_filled_from_graphite)))

            # @added 20240704 - Feature #5368: analyzer_batch - reprocess
            if reprocess and len(timeseries) > 0 and not fetch_data_from_graphite:
                last_timestamp_present = timeseries[-1][0]
                if last_timestamp_present < reprocess_until:
                    fetch_data_from_graphite = True

            if fetch_data_from_graphite:
                graphite_timeseries = []
                use_until_timestamp = int(time())
                try:
                    graphite_timeseries = get_graphite_metric(skyline_app, base_name, first_last_duration_timestamp, use_until_timestamp, 'list', 'object')
                    if graphite_timeseries:
                        logger.info('fetched time series for %s from Graphite, len(timeseries): %s, from_timestamp: %s, until_timestamp: %s' % (
                            base_name, str(len(graphite_timeseries)),
                            str(first_last_duration_timestamp),
                            str(use_until_timestamp)))
                except Exception as err:
                    logger.error('error :: get_graphite_metric failed for %s, err: %s' % (base_name, err))
                if len(graphite_timeseries) > len(timeseries):
                    logger.info('replacing Redis %s time series data len(timeseries): %s with Graphite data' % (
                        base_name, str(len(timeseries))))
                    replace_redis_data_with_graphite_data = True
                try:
                    self.redis_conn_decoded.hset('analyzer_batch.last_filled_from_graphite', base_name, str(int(time())))
                except Exception as err:
                    logger.error('error :: analyzer_batch :: hset failed on analyzer_batch.last_filled_from_graphite for %s, err: %s' % (
                        base_name, err))
            populated_timeseries = []
            if replace_redis_data_with_graphite_data:
                try:
                    populated_timeseries = populate_redis_metric(skyline_app, base_name, from_timestamp=first_last_duration_timestamp, until_timestamp=use_until_timestamp, timeseries=graphite_timeseries, replace=True, verify_ssl=settings.VERIFY_SSL)
                except Exception as err:
                    logger.error('error :: populate_redis_metric failed for %s, err: %s' % (
                        base_name, err))
                logger.info('replaced Redis key data with %s data points for %s' % (
                    str(len(populated_timeseries)), base_name))
                if populated_timeseries:
                    if len(populated_timeseries) > 0:
                        timeseries = list(populated_timeseries)

            # @added 20240602 - Feature #5368: analyzer_batch - reprocess
            if reprocess:
                if reprocess_until:
                    timeseries = [[int(t), v] for t, v in timeseries if int(t) <= reprocess_until]
                # Only process from when there is data from
                if len(timeseries) > 0:
                    try:
                        first_data_timestamp = timeseries[0][0]
                        if first_data_timestamp > reprocess_from:
                            logger.info('setting reprocess_from: %s to first_data_timestamp: %s, the first timestamp in Redis for %s' % (
                                str(reprocess_from), str(first_data_timestamp), base_name))
                            reprocess_from = int(first_data_timestamp)
                    except Exception as err:
                        logger.error('error :: failed to compare first_data_timestamp to reprocess_from for %s, err: %s' % (
                            base_name, err))
                last_analyzed_timestamp = reprocess_from

            # @added 20240520 - Feature #5352: vista - bigquery
            # Allow for sparse data to be dynamically zero filled
            zero_fill_analysis = False
            unzerofilled_timeseries = []
            zero_filled_timestamps = []
            use_last_analyzed_timestamp = int(last_analyzed_timestamp)
            if len(timeseries) > 0:
                try:
                    for zero_fill_analysis_namespace in zero_fill_analysis_namespaces:
                        if zero_fill_analysis_namespace in base_name:
                            zero_fill_analysis = True
                            break
                    if zero_fill_analysis:
                        # Determine data resolution
                        resolution = None
                        for namespace, namespace_resolution in namespace_resolutions.items():
                            if namespace in base_name:
                                resolution = int(namespace_resolution)
                                break
                        if not resolution:
                            try:
                                resolution = determine_data_frequency(skyline_app, timeseries, False)
                            except Exception as err:
                                logger.error('error :: analyzer_batch :: determine_data_frequency failed, err: %s' % err)

                        # IT IS NOT POSSIBLE TO DETERMINE RESOLUTION ACCURATELY FROM SPARSE DATA

                    zero_filled_timestamps = []
                    if zero_fill_analysis and resolution:
                        logger.info('%s - before zerofill - len(timeseries): %s, timeseries[0]: %s, timeseries[-1]: %s' % (
                            base_name, str(len(timeseries)),
                            str(timeseries[0]), str(timeseries[-1])))

                        unzerofilled_timeseries = list(timeseries)
                        aligned_timeseries = [[int(int(ts) // resolution * resolution), v] for ts, v in timeseries]
                        aligned_timeseries_timestamp_indices = {}
                        for index, item in enumerate(aligned_timeseries):
                            ts = item[0]
                            aligned_timeseries_timestamp_indices[ts] = index
                        last_timestamp = aligned_timeseries[-1][0]
                        expected_first_timestamp = int(int(first_last_duration_timestamp) // resolution * resolution)
                        last_analyzed_timestamp = int(int(last_analyzed_timestamp) // resolution * resolution)
                        expected_timestamps = list(range(expected_first_timestamp, (last_timestamp + resolution), resolution))
                        current_last_expected_timestamp = expected_timestamps[-1]
                        last_expected_timestamp = aligned_timeseries[-1][0]
                        while current_last_expected_timestamp < last_expected_timestamp:
                            current_last_expected_timestamp = current_last_expected_timestamp + resolution
                            expected_timestamps.append(current_last_expected_timestamp)
                        logger.info('%s - before zerofill - expected_timestamps[0]: %s, expected_timestamps[-1]: %s' % (
                            base_name, str(expected_timestamps[0]), str(expected_timestamps[-1])))
                        logger.info('%s - before zerofill - aligned_timeseries[0]: %s, aligned_timeseries[-1]: %s' % (
                            base_name, str(aligned_timeseries[0]), str(aligned_timeseries[-1])))

                        timeseries_dict = {}
                        for ts, v in aligned_timeseries:
                            timeseries_dict[ts] = v
                        zero_filled_timeseries = []
                        for ts in expected_timestamps:
                            try:
                                value = float(timeseries_dict[ts])
                            except:
                                value = 0.0
                                # @added 20240605 - Feature #5352: vista - bigquery
                                # Add to the list of zero filled timestamps that
                                # must not be analysed
                                zero_filled_timestamps.append(ts)
                            zero_filled_timeseries.append([ts, value])
                        if zero_filled_timeseries:
                            logger.info('%s - len(timeseries): %s, zero_fill_analysis len(zero_filled_timeseries): %s, zero_filled_timeseries[0]: %s, zero_filled_timeseries[-1]: %s' % (
                                base_name, str(len(unzerofilled_timeseries)),
                                str(len(zero_filled_timeseries)),
                                str(zero_filled_timeseries[0]), str(zero_filled_timeseries[-1])))

                            timeseries = list(zero_filled_timeseries)
                            del zero_filled_timeseries
                            del expected_timestamps
                            del timeseries_dict

                except Exception as err:
                    logger.error('error :: analyzer_batch :: zero_fill_analysis block failed, err: %s' % err)
                    continue

            # @added 20240604 - Feature #5352: vista - bigquery
            # Ensure that only metrics belonging to the shard are processed on
            # the node
            if HORIZON_SHARDS:
                if shard_metric is None:
                    shard_metric = is_shard_metric(base_name)
                if not shard_metric:
                    logger.info('warning :: removing non shard metric %s from this cluster node and resetting to an empty timeseries' % base_name)
                    try:
                        self.redis_conn_decoded.srem(full_uniques, metric_name)
                    except Exception as err:
                        logger.error('error :: failed to srem %s from %s, err: %s' % (
                            metric_name, full_uniques, err))                        
                    try:
                        self.redis_conn_decoded.delete(metric_name)
                    except Exception as err:
                        logger.error('error :: failed to delete Redis key %s, err: %s' % (
                            metric_name, err))                        
                    timeseries = []

            timestamps_to_analyse = []
            # Reverse the time series so that only the first (last) items now to be
            # iterated and break after the necessary iterations so the entire
            # time series is not iterated over.
            reversed_timeseries = list(reversed(timeseries))
            for timestamp, value in reversed_timeseries:
                # @modified 20240608 - Feature #5352: vista - bigquery
                # Exclude zero filled timestamps
                if int(timestamp) > last_analyzed_timestamp:
                    if int(timestamp) not in zero_filled_timestamps:
                        timestamps_to_analyse.append(int(timestamp))
                else:
                    break
            if len(timestamps_to_analyse) == 0:
                for timestamp, value in reversed_timeseries:
                    if int(timestamp) >= last_analyzed_timestamp:
                        if int(timestamp) not in zero_filled_timestamps:
                            timestamps_to_analyse.append(int(timestamp))
                    else:
                        break

            del reversed_timeseries
            timestamps_to_analyse = list(reversed(timestamps_to_analyse))

            # @added 20200413 - Feature #3486: analyzer_batch
            #                   Feature #3480: batch_processing
            # Handle there being no timestamps_to_analyse and report such as
            # otherwise the only info logged is that the work key just gets removed
            # 2020-04-14 12:57:25 :: 3222 :: there are 1 metrics to process in the analyzer.batch Redis set
            # 2020-04-14 12:57:25 :: 3222 :: processing - ['vista.demo_robustperception_io.prometheus.node_disk_read_time_seconds_total', 1586868000]
            # 2020-04-14 12:57:25 :: 3222 :: starting 1 of 1 spin_batch_process
            # 2020-04-14 12:57:25 :: 7852 :: batch :: child_batch_process_pid - 7852, processing vista.demo_robustperception_io.prometheus.node_disk_read_time_seconds_total from 1586868000
            # 2020-04-14 12:57:25 :: 7852 :: analyzer_batch :: removed work item - ['vista.demo_robustperception_io.prometheus.node_disk_read_time_seconds_total', 1586868000] - from Redis set - analyzer.batch
            # 2020-04-14 12:57:25 :: 7852 :: spin_batch_process took 0.04 seconds
            # 2020-04-14 12:57:25 :: 3222 :: 1 spin_batch_process completed in 0.10 seconds
            # 2020-04-14 12:57:25 :: 3222 :: exceptions - Stale: 9, Boring: 6, TooShort: 0, Other: 0
            # 2020-04-14 12:57:25 :: 3222 :: anomaly_breakdown - histogram_bins: 0, first_hour_average: 0, stddev_from_average: 0, grubbs: 0, ks_test: 0, mean_subtraction_cumulation: 0, median_absolute_deviation: 0, stddev_from_moving_average: 0, least_squares: 0
            number_of_timestamps_to_analyze = len(timestamps_to_analyse)
            if number_of_timestamps_to_analyze == 0:
                logger.info('no timestamps were found to analyze for %s from %s, nothing to do, timeseries[-3:]: %s' % (
                    metric_name, str(last_analyzed_timestamp), str(timeseries[-3:])))

                # @added 20240521 - Feature #5352: vista - bigquery
                try:
                    last_timestamps_hash_dict[base_name] = int(last_timeseries_timestamp)
                except Exception as err:
                    logger.error('error :: analyzer_batch :: failed to update last_timestamps_hash_dict for %s, err: %s' % (
                        base_name, err))
                if use_last_analyzed_timestamp:
                    last_analyzed_timestamp = use_last_analyzed_timestamp

                # @added 20240602 - Feature #5368: analyzer_batch - reprocess
                if reprocess:
                    try:
                        del last_timestamps_hash_dict[base_name]
                    except:
                        pass
                    try:
                        self.redis_conn.hdel(redis_reprocess_hash, base_name)
                        logger.info('analyzer_batch :: removed reprocess metric %s from Redis hash %s' % (
                            base_name, redis_reprocess_hash))
                    except Exception as err:
                        logger.error('error :: analyzer_batch :: failed to remove %s from Redis hash %s, err: %s' % (
                            base_name, redis_reprocess_hash, err))
                    if batch_mode:
                        continue

                # @added 20200424 - Feature #3486: analyzer_batch
                #                   Feature #3480: batch_processing
                #                   Feature #3504: Handle airgaps in batch metrics
                # If there are no data points to analyze remove from the set
                redis_set = 'analyzer.batch'
                data = [metric_name, int(last_analyzed_timestamp)]
                try:
                    self.redis_conn.srem(redis_set, str(data))
                    logger.info('analyzer_batch :: removed batch metric item - %s - from Redis set - %s' % (str(data), redis_set))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: analyzer_batch :: failed to remove batch metric item - %s - from Redis set - %s' % (str(data), redis_set))

                # Clean up and return
                try:
                    del timeseries
                except:
                    pass
                try:
                    del timestamps_to_analyse
                except:
                    pass
                if batch_mode:
                    continue
                try:
                    del mirage_unique_metrics
                except:
                    pass
                try:
                    del ionosphere_unique_metrics
                except:
                    pass
                try:
                    del derivative_metrics
                except:
                    pass
                try:
                    del non_derivative_metrics
                except:
                    pass
                try:
                    del non_derivative_monotonic_metrics
                except:
                    pass
                try:
                    del non_smtp_alerter_metrics
                except:
                    pass

                # @added 20240520 - Feature #5352: vista - bigquery
                if unzerofilled_timeseries:
                    try:
                        del unzerofilled_timeseries
                    except:
                        pass

                return

            last_redis_data_timestamp = timestamps_to_analyse[-1]
            logger.info('%s timestamps were found to analyze for %s from %s to %s' % (
                str(number_of_timestamps_to_analyze), metric_name,
                str(last_analyzed_timestamp), str(last_redis_data_timestamp)))

            if LOCAL_DEBUG:
                logger.debug('debug :: %s - timestamps_to_analyse: %s' % (
                    base_name, str(timestamps_to_analyse)))

            # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
            # base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
            if metric_name.startswith(settings.FULL_NAMESPACE):
                base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
            else:
                base_name = metric_name

            # @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
            # Determine if any metrcs have negatives values some they can be
            # added to the ionosphere_untrainable_metrics Redis set
            run_negatives_present = False
            if settings.IONOSPHERE_ENABLED:
                run_negatives_present = True
                try:
                    known_negative_metric, known_negative_metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, KNOWN_NEGATIVE_METRICS)
                    del known_negative_metric_matched_by
                    if known_negative_metric:
                        run_negatives_present = False
                except:
                    run_negatives_present = True

            # @added 20170602 - Feature #2034: analyse_derivatives
            # In order to convert monotonic, incrementing metrics to a deriative
            # metric
            known_derivative_metric = False
            unknown_deriv_status = True

            # @modified 20200601 - Feature #3480: batch_processing
            #                      Bug #2050: analyse_derivatives - change in monotonicity
            # Switch the order in which they are checked and do not check if
            # not manage_derivative_metrics as will only be set to True anyway
            # if metric_name in non_derivative_metrics:
            #     unknown_deriv_status = False
            # if unknown_deriv_status:
            #     if metric_name in derivative_metrics:
            #         known_derivative_metric = True
            #         unknown_deriv_status = False
            if metric_name in derivative_metrics:
                known_derivative_metric = True
                unknown_deriv_status = False
            if unknown_deriv_status:
                if metric_name in non_derivative_metrics:
                    unknown_deriv_status = False
                # @added 20220421 - Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
                #                   Task #3868: POC MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
                if metric_name in all_non_derivative_metrics:
                    unknown_deriv_status = False
                if base_name in all_non_derivative_metrics:
                    unknown_deriv_status = False

                # @added 20240520 - Feature #5352: vista - bigquery
                if non_derivative_bq_namespaces and unknown_deriv_status:
                    for non_derivative_bq_namespace in non_derivative_bq_namespaces:
                        if non_derivative_bq_namespace in base_name:
                            unknown_deriv_status = False
                            all_non_derivative_metrics.append(base_name)

            # First check if it has its own Redis z.derivative_metric key
            # that has not expired
            derivative_metric_key = 'z.derivative_metric.%s' % str(base_name)

            # @added 20200601 - Feature #3480: batch_processing
            #                   Bug #2050: analyse_derivatives - change in monotonicity
            # When a monotonic metric changes in the last run before a
            # manage_derivative_metrics run, when manage_derivative_metrics runs
            # it classifies it and adds it to non_derivative_metrics the only
            # way to stop this is check the key for each metric
            last_derivative_metric_key = None
            try:
                last_derivative_metric_key = self.redis_conn_decoded.get(derivative_metric_key)
            except Exception as e:
                logger.error('error :: could not query Redis for last_derivative_metric_key: %s' % e)
            if last_derivative_metric_key:
                known_derivative_metric = True

            # @added 20220421 - Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
            #                   Task #3868: POC MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
            identified_non_derivative_metric = False
            if base_name in all_non_derivative_metrics:
                identified_non_derivative_metric = True
            if metric_name in all_non_derivative_metrics:
                identified_non_derivative_metric = True
            if metric_name in longterm_non_derivative_metrics:
                identified_non_derivative_metric = True
            if identified_non_derivative_metric:
                try:
                    del derivative_metrics[metric_name]
                except:
                    pass
                unknown_deriv_status = False
                is_strictly_increasing_monotonically = False
                known_derivative_metric = False

            if unknown_deriv_status:
                # @added 20170617 - Bug #2050: analyse_derivatives - change in monotonicity
                # @modified 20200601 - Feature #3480: batch_processing
                #                      Bug #2050: analyse_derivatives - change in monotonicity
                # Always check moved to above
                # last_derivative_metric_key = False
                # try:
                #     last_derivative_metric_key = self.redis_conn.get(derivative_metric_key)
                # except Exception as e:
                #     logger.error('error :: could not query Redis for last_derivative_metric_key: %s' % e)

                # @modified 20200601 - Feature #3480: batch_processing
                #                      Bug #2050: analyse_derivatives - change in monotonicity
                # Apply skip_derivative
                skip_derivative = in_list(base_name, non_derivative_monotonic_metrics)
                is_strictly_increasing_monotonically = False

                if not skip_derivative:

                    # @added 20220323 - Feature #4502: settings - MONOTONIC_METRIC_NAMESPACES
                    if metric_name in always_derivative_metrics:
                        is_strictly_increasing_monotonically = True

                    if not is_strictly_increasing_monotonically:
                        is_strictly_increasing_monotonically = strictly_increasing_monotonicity(timeseries)

                    if is_strictly_increasing_monotonically:
                        try:
                            last_expire_set = int(time())
                            self.redis_conn.setex(
                                derivative_metric_key, settings.FULL_DURATION, last_expire_set)
                        except Exception as e:
                            logger.error('error :: could not set Redis derivative_metric key: %s' % e)
                else:
                    is_strictly_increasing_monotonically = False

                # Determine if it is a strictly increasing monotonically metric
                # or has been in last FULL_DURATION via its z.derivative_metric
                # key
                if last_derivative_metric_key:
                    # Until the z.derivative_metric key expires, it is classed
                    # as such
                    is_strictly_increasing_monotonically = True

                if skip_derivative:
                    is_strictly_increasing_monotonically = False
                if is_strictly_increasing_monotonically:
                    known_derivative_metric = True
                    try:
                        self.redis_conn.sadd('derivative_metrics', metric_name)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis derivative_metrics set')
                    try:
                        self.redis_conn.sadd('new_derivative_metrics', metric_name)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis new_derivative_metrics set')
                    try:
                        last_expire_set = int(time())
                        self.redis_conn.setex(
                            derivative_metric_key, settings.FULL_DURATION, last_expire_set)
                    except Exception as e:
                        logger.error('error :: could not set Redis derivative_metric key: %s' % e)
                    # @added 20210325 - Feature #3480: batch_processing
                    #                   Bug #2050: analyse_derivatives - change in monotonicity
                    # Remove from non_derivative_metrics as per analyzer
                    try:
                        self.redis_conn.srem('non_derivative_metrics', metric_name)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis non_derivative_metrics set')
                else:
                    try:
                        self.redis_conn.sadd('non_derivative_metrics', metric_name)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis non_derivative_metrics set')
                    try:
                        self.redis_conn.sadd('new_non_derivative_metrics', metric_name)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to add metric to Redis new_non_derivative_metrics set')

            not_anomalous_count = 0

            # @added 20200815 - Feature #3678:  SNAB - anomalyScore
            record_anomalyScore = False
            if SNAB_anomalyScore:
                SNAB_metrics = []
                try:
                    SNAB_all_metrics = SNAB_anomalyScore['all']
                    if SNAB_all_metrics:
                        for SNAB_metric in SNAB_all_metrics:
                            SNAB_metrics.append(SNAB_metric)
                except:
                    SNAB_all_metrics = []
                try:
                    SNAB_app_metrics = SNAB_anomalyScore[skyline_app]
                    if SNAB_app_metrics:
                        for SNAB_metric in SNAB_app_metrics:
                            SNAB_metrics.append(SNAB_metric)
                except:
                    SNAB_app_metrics = []
                if SNAB_metrics:
                    for SNAB_metric_namespace in list(set(SNAB_metrics)):
                        if SNAB_metric_namespace in base_name:
                            record_anomalyScore = True
                            break
            test_anomaly = False
            test_anomaly_at = None
            try:
                test_anomaly_key = 'analyzer_batch.test.%s' % base_name
                try:
                    test_anomaly = self.redis_conn.get(test_anomaly_key)
                    test_anomaly_at = int(test_anomaly)
                    logger.info('test_anomaly - testing anomly on %s at %s' % (metric_name, str(test_anomaly_at)))
                except:
                    test_anomaly = None
            except:
                test_anomaly = False

            if LOCAL_DEBUG:
                logger.debug('debug :: %s - timeseries sample (last 4): %s' % (
                    base_name, str(timeseries[-4:])))

            # @added 20220113 - Feature #3566: custom_algorithms
            #                   Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
            timestamps_processed = 0

            # @added 20240607 - Feature #5368: analyzer_batch - reprocess
            # Allow oneshot analysis for reprocessing
            anomaly_window = 1
            custom_algorithm_overrides = {}
            oneshot_results = {}
            oneshot_timestamp = None
            if reprocess and oneshot:
                logger.info('oneshot reprocessing %s' % base_name)
                anomaly_window = len(timestamps_to_analyse)
                custom_algorithm_overrides = {'anomaly_window': anomaly_window}
                # Reorder timestamps_to_analyse
                oneshot_timestamp = timestamps_to_analyse[-1]
                oneshot_timestamps_to_analyse = [oneshot_timestamp]
                for timestamp_to_analyse in timestamps_to_analyse:
                    if timestamp_to_analyse not in oneshot_timestamps_to_analyse:
                        oneshot_timestamps_to_analyse.append(timestamp_to_analyse)
                original_timestamps_to_analyse = list(timestamps_to_analyse)
                timestamps_to_analyse = list(oneshot_timestamps_to_analyse)

            # Distill timeseries strings into lists
            for i, batch_timestamp in enumerate(timestamps_to_analyse):
                self.check_if_parent_is_alive()

                # @added 20240605 - Feature #5352: vista - bigquery
                if batch_timestamp in zero_filled_timestamps:
                    continue

                batch_timeseries = []

                # @added 20240523 - Feature #5352: vista - bigquery
                #                   Feature #5190: Add custom_algorithm results to Mirage and plots
                # Added custom_algorithm_results
                custom_algorithms_results = {}

                # @added 20220113 - Feature #3566: custom_algorithms
                #                   Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
                # Reset anomalous for every iteration, print iterations and
                # break if approaching spin_batch_process limit
                anomalous = False
                timestamps_processed += 1
                if not timestamps_processed % 500:
                    logger.info('%s of %s timestamps processed for %s' % (
                        str(timestamps_processed),
                        str(len(timestamps_to_analyse)), base_name))
                if int(time()) > (int(spin_start) + 290):
                    logger.info('%s of %s timestamps processed, approaching time limit, breaking' % (
                        str(timestamps_processed),
                        str(len(timestamps_to_analyse))))
                    break

                for timestamp, value in timeseries:
                    if int(timestamp) <= batch_timestamp:
                        batch_timeseries.append([timestamp, value])

                if known_derivative_metric:
                    try:
                        derivative_timeseries = nonNegativeDerivative(batch_timeseries)
                        batch_timeseries = derivative_timeseries
                    except Exception as err:
                        logger.error('error :: nonNegativeDerivative failed - %s' % err)
                        batch_timeseries = []

                # @added 20220407 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
                #                   Feature #4520: settings - ZERO_FILL_NAMESPACES
                if base_name in zero_fill_metrics:
                    if not known_derivative_metric:
                        try:
                            timeseries = full_duration_timeseries_fill(self, skyline_app, base_name, timeseries, 'zero')
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: full_duration_timeseries_fill failed - %s' % err)
                    else:
                        # Fix the badly defined metric if a metric has been defined
                        # as a zero_fill metric but is a derivative_metric apply
                        # last_known_value AFTER nonNegativeDerivative because
                        # zero filling derivative metrics does not have the desired
                        # effect
                        last_known_value_metrics.append(base_name)
                if base_name in last_known_value_metrics:
                    try:
                        timeseries = full_duration_timeseries_fill(self, skyline_app, base_name, timeseries, 'last_known_value')
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: full_duration_timeseries_fill failed - %s' % err)

                if LOCAL_DEBUG:
                    logger.debug('debug :: %s - batch_timestamp: %s, batch_timeseries sample (last 2): %s' % (
                        base_name, str(batch_timestamp), str(batch_timeseries[-2:])))

                try:
                    # Allow for testing.  If you want to test a metric and then stop
                    # the metric sending data to carbon-relay (use a vista metric).
                    # Determine a timestamp that will fall into the stopped period
                    # Add the timestamp to a Redis key called
                    # analyzer_batch.test.<metric_name>
                    # Start the metric sending data again (re-enable in vista)
                    # vista/flux will fill the missing data, when analyzer pushes
                    # the metric to analyzer_batch to process, if analyzer_batch
                    # is set to test_anomaly True and finds the key, if the
                    # timestamp matches the timestamp in the key, analyzer_batch
                    # will multiply the timestamp data point by 15, this should
                    # trigger an anomaly.  Ensure you use a metric which will
                    # trigger, a load related metric is usually adequate.
                    # test_anomaly = False
                    test_anomaly_at = None
                    test_anomaly_batch_timeseries = []
                    if test_anomaly:
                        test_anomaly_at = None
                        test_anomaly_key = 'analyzer_batch.test.%s' % base_name
                        try:
                            test_anomaly_at = self.redis_conn.get(test_anomaly_key)
                        except:
                            test_anomaly_at = None
                        if test_anomaly_at:
                            if int(test_anomaly_at) == int(batch_timeseries[-1][0]):
                                for timestamp, value in batch_timeseries:
                                    if int(timestamp) == int(test_anomaly_at):
                                        anomaly_value = value * 100
                                        logger.info('test_anomaly - replacing value %s with anomaly_value of %s at %s in %s timeseries' % (
                                            str(value), str(anomaly_value),
                                            str(test_anomaly_at), metric_name))
                                        value = anomaly_value
                                    test_anomaly_batch_timeseries.append([timestamp, value])
                                if test_anomaly_batch_timeseries:
                                    batch_timeseries = test_anomaly_batch_timeseries
                                    logger.info('test_anomaly - replaced %s timeseries with anomaly value in it' % (
                                        metric_name))
                                    try:
                                        self.redis_conn.delete(test_anomaly_key)
                                        logger.info('test_anomaly - deleted test_anomaly Redis key - %s' % str(test_anomaly_key))
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: failed to delete test_anomaly Redis key - %s' % str(test_anomaly_key))

                    datapoint = batch_timeseries[-1][1]
                    logger.info('analysing %s - timestamp: %s, value: %s' % (
                        base_name, str(batch_timestamp), str(datapoint)))

                    # @modified 20200423 - Feature #3504: Handle airgaps in batch metrics
                    #                      Feature #3480: batch_processing
                    #                      Feature #3486: analyzer_batch
                    # Changed to algoritms_batch so there is no pollution and
                    # analyzer and analyzer_batch are totally independent
                    # metric_airgaps = []
                    # anomalous, ensemble, datapoint = run_selected_algorithm(batch_timeseries, metric_name, metric_airgaps)
                    # @modified 20200425 - Feature #3508: ionosphere.untrainable_metrics
                    # Added run_negatives_present and added negatives_found
                    # anomalous, ensemble, datapoint = run_selected_batch_algorithm(batch_timeseries, metric_name)
                    # @modified 20200607 - Feature #3566: custom_algorithms
                    # Added algorithms_run
                    # @modified 20200815 - Feature #3678: SNAB - anomalyScore
                    # Added the number_of_algorithms to calculate anomalyScore from
                    # @added 20240523 - Feature #5352: vista - bigquery
                    #                   Feature #5190: Add custom_algorithm results to Mirage and plots
                    # Added custom_algorithms_results
                    # @added 20240607 - Feature #5368: analyzer_batch - reprocess
                    # Use oneshot analysis for reprocessing added custom_algorithm_overrides
                    # and only run if a oneshot_results dict does not exist
                    triggered_algorithms = None
                    if len(oneshot_results) == 0:
                        anomalous, ensemble, datapoint, negatives_found, algorithms_run, number_of_algorithms, custom_algorithms_results = run_selected_batch_algorithm(batch_timeseries, metric_name, run_negatives_present, custom_algorithm_overrides=custom_algorithm_overrides)

                        # @added 20241120 - Task #5526: Build v5.0.0 and upgrade deps
                        #                   Branch #5532: v5.0.0-alpha
                        # Coerce all numpy.bool_ typed elements introduced with
                        # numpy >= 2 to Python bool so they are literal_eval and
                        # json safe
                        ensemble = [item if item is None else bool(item) for item in ensemble]
        
                    else:
                        # Check if the timestamp is recorded in the anomalies
                        # as the oneshot analysis could be anomalous because
                        # there are anomalies in the anomaly_window, but the
                        # actual batch_timestamp may not be anomalous
                        if 'anomalies' in oneshot_results.keys():
                            if batch_timestamp not in oneshot_results['anomalies'].keys():
                                anomalous = False
                            else:
                                anomalous = True
                                ensemble = [True]
                                logger.info('oneshot - %s anomalous: True, timestamp: %s, value: %s ' % (
                                    base_name, str(batch_timestamp), str(datapoint)))
                        if not anomalous:
                            ensemble = [False]
                        negatives_found = oneshot_results['oneshot_returns']['negatives_found']
                        algorithms_run = oneshot_results['oneshot_returns']['algorithms_run']
                        number_of_algorithms = oneshot_results['oneshot_returns']['number_of_algorithms']
                        custom_algorithms_results = oneshot_results['oneshot_returns']['custom_algorithms_results']
                        if anomalous:
                            if 'algorithms_results' in oneshot_results['anomalies'][batch_timestamp]:
                                try:
                                    ensemble = list(oneshot_results['anomalies'][batch_timestamp]['algorithms_results'].values())
                                    # @added 20241120 - Task #5526: Build v5.0.0 and upgrade deps
                                    #                   Branch #5532: v5.0.0-alpha
                                    # Coerce all numpy.bool_ typed elements introduced with
                                    # numpy >= 2 to Python bool so they are literal_eval and
                                    # json safe
                                    ensemble = [item if item is None else bool(item) for item in ensemble]

                                    algorithms_run = list(oneshot_results['anomalies'][batch_timestamp]['algorithms_results'].keys())
                                except:
                                    pass
                            if 'triggered_algorithms' in oneshot_results['anomalies'][batch_timestamp]:
                                try:
                                    triggered_algorithms = oneshot_results['anomalies'][batch_timestamp]['triggered_algorithms']
                                    ensemble = [True for t in triggered_algorithms]
                                except:
                                    pass

                    # @added 20240607 - Feature #5368: analyzer_batch - reprocess
                    # Allow oneshot analysis for reprocessing
                    if reprocess and oneshot:
                        if batch_timestamp == oneshot_timestamp:
                            oneshot_results = copy.deepcopy(custom_algorithms_results)
                            oneshot_results['oneshot_returns'] = {
                                'ensemble': ensemble,
                                'negatives_found': negatives_found,
                                'algorithms_run': algorithms_run,
                                'number_of_algorithms': number_of_algorithms,
                                'custom_algorithms_results': custom_algorithms_results
                            }
                            # Check if the timestamp is recorded in the anomalies
                            # as the oneshot analysis could be anomalous because
                            # there are anomalies in the anomaly_window, but the
                            # actual batch_timestamp may not be anomalous
                            if 'anomalies' in custom_algorithms_results.keys():
                                if batch_timestamp not in custom_algorithms_results['anomalies'].keys():
                                    anomalous = False
                                else:
                                    anomalous = True

                    # @added 20220420 - Feature #4530: namespace.analysed_events
                    analysed_metrics.append(base_name)

                    # @added 20240521 - Feature #5352: vista - bigquery
                    reference_batch_timestamp = int(batch_timestamp)
                    if unzerofilled_timeseries:
                        try:
                            batch_timestamp_index = aligned_timeseries_timestamp_indices[reference_batch_timestamp]
                            reference_batch_timestamp = unzerofilled_timeseries[batch_timestamp_index][0]
                        except Exception as err:
                            errors.append([base_name, 'reference_batch_timestamp', err])
                            reference_batch_timestamp = int(batch_timestamp)

                    # @added 20240602 - Feature #5368: analyzer_batch - reprocess
                    if reprocess and dry_run:
                        logger.info('analyzer_batch :: DRY_RUN - only reporting in log - %s, anomalous: %s, timestamp: %s, value: %s, continuing' % (
                            base_name, str(anomalous), str(reference_batch_timestamp),
                            str(datapoint)))
                        try:
                            self.redis_conn.hdel(redis_reprocess_hash, base_name)
                            logger.info('analyzer_batch :: removed reprocess metric %s from Redis hash %s' % (
                                base_name, redis_reprocess_hash))
                        except Exception as err:
                            logger.error('error :: analyzer_batch :: failed to remove %s from Redis hash %s, err: %s' % (
                                base_name, redis_reprocess_hash, err))
                        if batch_mode:
                            continue

                    # @added 20220919 - Feature #4676: analyzer - illuminance.all key
                    if ensemble.count(True) > 0:

                        # @added 20240607 - Feature #5368: analyzer_batch - reprocess
                        # Allow oneshot analysis for reprocessing and use the
                        # triggered_algorithms is they are returned
                        if triggered_algorithms is None:
                            triggered_algorithms = []
                            for index, value in enumerate(ensemble):
                                if value:
                                    algorithm = algorithms_run[index]
                                    try:
                                        algorithm_id = int(algorithms[algorithm])
                                    except:
                                        algorithm_id = 0
                                    triggered_algorithms.append(algorithm_id)
                        metric_id = 0
                        try:
                            metric_id = get_metric_id_from_base_name(skyline_app, base_name)
                        except Exception as err:
                            errors.append([base_name, 'get_metric_id_from_base_name failed', err])
                            metric_id = 0
                        if metric_id:
                            try:
                                # @modified 20240521 - Feature #5352: vista - bigquery
                                # Change batch_timeseries to reference_batch_timestamp
                                illuminance_all_dict[str(metric_id)] = {
                                    #'t': batch_timeseries,
                                    't': reference_batch_timestamp,
                                    'v': float(datapoint),
                                    'a': triggered_algorithms}
                            except Exception as err:
                                errors.append([base_name, 'failed to add illuminance_all_dict', str(err)])

                    if LOCAL_DEBUG:
                        logger.debug('debug :: %s - anomalous: %s, datapoint: %s, timestamp: %s, ensemble: %s, algorithms_run: %s, len(batch_timeseries): %s' % (
                            base_name, str(anomalous), str(datapoint),
                            # @modified 20240521 - Feature #5352: vista - bigquery
                            # str(batch_timeseries[-1][0]), str(ensemble),
                            str(reference_batch_timestamp), str(ensemble),
                            str(algorithms_run), str(len(batch_timeseries))))

                    if anomalous:
                        logger.info('%s - anomalous: %s, timestamp: %s, value: %s, ensemble: %s' % (
                            base_name, str(anomalous), str(reference_batch_timestamp),
                            str(datapoint), str(ensemble)))
                        # @added 20240726 - Feature #5400: analyzer_batch - stats
                        total_anomalies += 1

                    if test_anomaly_batch_timeseries:
                        logger.info('test_anomaly - analyzed %s data with anomaly value in it and anomalous = %s' % (
                            metric_name, str(anomalous)))

                    # @added 20200815 - Feature #3678: SNAB - anomalyScore
                    if record_anomalyScore:
                        anomalyScore_file = '%s/%s/%s/skyline.SNAB.%s.anomalyScore.csv' % (
                            SNAB_DATA_DIR, skyline_app, base_name, base_name)
                        # Get the anomaly breakdown - who returned True?
                        triggered_algorithms = []
                        run_debug = False
                        if ensemble.count(True) and algorithms_run:
                            run_debug = True
                        if (int(batch_timestamp) % 20000) == 0:
                            run_debug = True
                        if run_debug:
                            logger.debug('debug :: ensemble to calculate anomalyScore - %s' % str(ensemble))
                            logger.debug('debug :: algorithms_run to calculate anomalyScore - %s' % str(algorithms_run))
                        for index, value in enumerate(ensemble):
                            if value:
                                algorithm = algorithms_run[index]
                                triggered_algorithms.append(algorithm)
                        if run_debug:
                            logger.debug('debug :: triggered_algorithms to calculate anomalyScore - %s' % str(triggered_algorithms))
                        anomalyScore = 0.0
                        try:
                            if len(triggered_algorithms) > 0 and number_of_algorithms > 0:
                                if len(triggered_algorithms) > settings.CONSENSUS:
                                    anomalyScore = 1.0
                                else:
                                    anomalyScore = len(triggered_algorithms) / settings.CONSENSUS
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to calculate anomalyScore')
                        if not os.path.isfile(anomalyScore_file):
                            data = 'timestamp,value,anomalyScore,triggered_algorithms\n'
                            write_data_to_file(skyline_app, anomalyScore_file, 'w', data)
                        # @modified 20240521 - Feature #5352: vista - bigquery
                        # data = '%s,%s,%s,%s\n' % (str(int(batch_timestamp)), str(datapoint), str(anomalyScore), str(triggered_algorithms))
                        data = '%s,%s,%s,%s\n' % (str(reference_batch_timestamp), str(datapoint), str(anomalyScore), str(triggered_algorithms))
                        write_data_to_file(skyline_app, anomalyScore_file, 'a', data)
                        if run_debug:
                            # @modified 20240521 - Feature #5352: vista - bigquery
                            # logger.debug('%s,%s,%s,%s' % (str(int(batch_timestamp)), str(datapoint), str(anomalyScore), str(triggered_algorithms)))
                            logger.debug('%s,%s,%s,%s' % (str(reference_batch_timestamp), str(datapoint), str(anomalyScore), str(triggered_algorithms)))

                    # Update the last_timestamp metric Redis key
                    # @modified 20240522 - Feature #5352: vista - bigquery
                    # Declare last_metric_timestamp_key once
                    # last_metric_timestamp_key = 'last_timestamp.%s' % base_name
                    redis_key_set = None
                    try:

                        # @modified 20240521 - Feature #5352: vista - bigquery
                        # int_metric_timestamp = int(batch_timestamp)
                        int_metric_timestamp = int(reference_batch_timestamp)

                        # @modified 20200503 - Feature #3504: Handle airgaps in batch metrics
                        #                      Feature #3480: batch_processing
                        #                      Feature #3486: analyzer_batch
                        # Set the last_timestamp expiry time to 1 month rather than
                        # settings.FULL_DURATION
                        self.redis_conn.setex(
                            last_metric_timestamp_key, 2592000, int_metric_timestamp)
                        redis_key_set = True
                    except:
                        logger.error('error :: failed to set Redis key %s' % last_metric_timestamp_key)

                    # @added 20240522 - Task #5358: analyzer.last_timestamp.base_names Redis hash to replace last_timestamp keys
                    last_timestamps_hash_dict[base_name] = int(reference_batch_timestamp)

                    # @added 20240602 - Feature #5368: analyzer_batch - reprocess
                    if reprocess:
                        try:
                            del last_timestamps_hash_dict[base_name]
                        except:
                            pass

                    if anomalous:
                        if redis_key_set:
                            logger.info('anomalous :: anomaly detected on %s at %s with %s, set Redis key %s to %s' % (
                                base_name, str(int_metric_timestamp), str(datapoint),
                                last_metric_timestamp_key, str(int_metric_timestamp)))

                        else:
                            logger.info('anomalous :: anomaly detected on %s at %s with %s' % (
                                base_name, str(int_metric_timestamp),
                                str(datapoint)))

                        # @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
                        # Determine if any metrcs have negatives values some they can be
                        # added to the ionosphere.untrainable_metrics Redis set
                        if run_negatives_present and negatives_found:
                            redis_set = 'ionosphere.untrainable_metrics'
                            try:
                                last_negative_timestamp = int(negatives_found[-1][0])
                                last_negative_value = negatives_found[-1][1]
                                remove_after_timestamp = int(last_negative_timestamp + settings.FULL_DURATION)
                                # @modified 20240521 - Feature #5352: vista - bigquery
                                # data = str([metric_name, batch_timestamp, datapoint, last_negative_timestamp, last_negative_value, settings.FULL_DURATION, remove_after_timestamp])
                                data = str([metric_name, reference_batch_timestamp, datapoint, last_negative_timestamp, last_negative_value, settings.FULL_DURATION, remove_after_timestamp])
                                self.redis_conn.sadd(redis_set, data)
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to add data to Redis set %s' % (
                                    str(redis_set)))

                        # Added a Redis key for Mirage, Panorama and Ionosphere to
                        # query to identify if an anomaly has been added by
                        # analyzer_batch and set a longish TTL as if multiple
                        # anomalies for multiple metrics in a batch are sent to
                        # Ionosphere it could take Ionosphere a while to analyze
                        # them all.  This key circumvents the requirement of each
                        # app to determine if a metric is a batch metric, as this
                        # is only created for batch metric anomalies.
                        analyzer_batch_metric_anomaly_key = '%s.anomaly.%s.%s' % (
                            skyline_app, str(int_metric_timestamp), base_name)
                        try:
                            # @modified 20240521 - Feature #5352: vista - bigquery
                            # int_metric_timestamp = int(batch_timestamp)
                            int_metric_timestamp = int(reference_batch_timestamp)

                            self.redis_conn.setex(
                                analyzer_batch_metric_anomaly_key,
                                86400, int_metric_timestamp)
                            logger.info('set Redis key %s with %s for other apps to identify this as an analyzer_batch anomaly' % (
                                analyzer_batch_metric_anomaly_key,
                                str(int_metric_timestamp)))
                        except:
                            logger.error('error :: failed to set Redis key %s' % analyzer_batch_metric_anomaly_key)
                    else:
                        if redis_key_set:
                            not_anomalous_count += 1

                            if LOCAL_DEBUG:
                                logger.debug('debug :: not anomalous :: %s at %s with %s' % (
                                    base_name, str(int_metric_timestamp), str(datapoint)))

                            # @modified 20200728 - Feature #3480: batch_processing
                            #                      Feature #3486: analyzer_batch
                            # Only log on the last data point, not on all
                            if int_metric_timestamp == int(last_redis_data_timestamp):
                                logger.info('not anomalous :: %s at %s with %s (along with %s other not anomalous data points), set Redis key %s to %s' % (
                                    base_name, str(int_metric_timestamp), str(datapoint),
                                    str(not_anomalous_count),
                                    last_metric_timestamp_key, str(int_metric_timestamp)))
                        else:
                            logger.info('not anomalous :: %s at %s with %s' % (
                                base_name, str(int_metric_timestamp),
                                str(datapoint)))

                    # @added 20190408 - Feature #2882: Mirage - periodic_check
                    # Add for Mirage periodic - is really anomalous add to
                    # real_anomalous_metrics and if in mirage_periodic_check_metric_list
                    # add as anomalous
                    if anomalous:
                        # @modified 20240521 - Feature #5352: vista - bigquery
                        #metric_timestamp = batch_timeseries[-1][0]
                        metric_timestamp = reference_batch_timestamp

                        metric = [datapoint, base_name, metric_timestamp]

                        # Get the anomaly breakdown - who returned True?
                        if not triggered_algorithms:
                            triggered_algorithms = []
                            for index, value in enumerate(ensemble):
                                if value:
                                    # @modified 20200607 - Feature #3566: custom_algorithms
                                    # algorithm = settings.ALGORITHMS[index]
                                    algorithm = algorithms_run[index]

                                    anomaly_breakdown[algorithm] += 1
                                    triggered_algorithms.append(algorithm)

                        # @added 20220504 - Feature #2580: illuminance
                        try:
                            illuminance_dict[base_name] = {
                                'timestamp': int(metric_timestamp),
                                'value': float(datapoint),
                                'triggered_algorithms_count': len(triggered_algorithms)}
                        except Exception as err:
                            logger.error('error :: failed to add %s to illuminance_dict, err: %s' % (
                                str(base_name), err))

                        # @added 20170206 - Bug #1904: Handle non filesystem friendly metric names in check files
                        sane_metricname = filesafe_metricname(str(base_name))

                        # If Panorama is enabled determine details
                        determine_anomaly_details = False
                        if settings.PANORAMA_ENABLED:
                            determine_anomaly_details = True

                        # If Ionosphere is enabled determine details
                        try:
                            ionosphere_enabled = settings.IONOSPHERE_ENABLED
                            if settings.IONOSPHERE_ENABLED:
                                determine_anomaly_details = True
                        except:
                            ionosphere_enabled = False

                        if determine_anomaly_details:
                            # @modified 20240521 - Feature #5352: vista - bigquery
                            # metric_timestamp = str(int(batch_timeseries[-1][0]))
                            # from_timestamp = str(int(batch_timeseries[1][0]))
                            metric_timestamp = str(int(float(reference_batch_timestamp)))
                            from_timestamp = str(int(batch_timeseries[0][0]))
                            timeseries_dir = base_name.replace('.', '/')

                        send_back_to_analyzer = None

                        # @added 20161119 - Branch #922: ionosphere
                        #                   Task #1718: review.tsfresh
                        # Set defaults which can be used later to determine how
                        # Analyzer should handle/route anomalies
                        analyzer_metric = True
                        mirage_metric = False
                        ionosphere_metric = False
                        send_to_ionosphere = False

                        if metric_name in ionosphere_unique_metrics:
                            ionosphere_metric = True
                            send_to_ionosphere = True

                        if metric_name in mirage_unique_metrics:
                            analyzer_metric = False
                            ionosphere_metric = False
                            mirage_metric = True
                            send_to_ionosphere = False

                        # @added 20170108 - Feature #1830: Ionosphere alerts
                        # Only send smtp_alerter_metrics to Ionosphere
                        smtp_alert_enabled_metric = True
                        if base_name in non_smtp_alerter_metrics:
                            smtp_alert_enabled_metric = False

                        if ionosphere_enabled:
                            if analyzer_metric:
                                # We do not want send all anomalous metrics to
                                # Ionosphere if they are not being alerted on as
                                # they will be pointless they will have no alert if
                                # it is within the EXPIRATION_TIME and there will be
                                # no reference graphs from an alert for the user to
                                # action.
                                cache_key = 'last_alert.smtp.%s' % (base_name)
                                last_alert = False
                                try:
                                    last_alert = self.redis_conn.get(cache_key)
                                except Exception as e:
                                    logger.error('error :: could not query Redis for cache_key: %s' % e)

                                if not last_alert:
                                    send_to_ionosphere = True
                                else:
                                    send_to_ionosphere = False
                                    if ionosphere_metric:
                                        logger.info('not sending to Ionosphere - alert key exists - %s' % (base_name))
                            else:
                                if mirage_metric:
                                    logger.info('not sending to Ionosphere - Mirage metric - %s' % (base_name))
                                    send_to_ionosphere = False

                                    # analyzer_batch sends Analyzer and Mirage
                                    # metrics back to analyzer
                                    send_back_to_analyzer = True

                                    # @added 20170306 - Feature #1960: ionosphere_layers
                                    # Ionosphere layers require the timeseries at
                                    # FULL_DURATION so if this is a Mirage and
                                    # Ionosphere metric, Analyzer needs to provide
                                    # the timeseries file for later (within 60
                                    # seconds) analysis, however we want the data
                                    # that triggered the anomaly, as before this was
                                    # only created by Mirage if an alert was
                                    # triggered, but Ionosphere layers now require
                                    # this file before an alert is triggered
                                    timeseries_dir = base_name.replace('.', '/')
                                    training_dir = '%s/%s/%s' % (
                                        settings.IONOSPHERE_DATA_FOLDER, str(int(float(metric_timestamp))),
                                        str(timeseries_dir))
                                    if not os.path.exists(training_dir):
                                        mkdir_p(training_dir)
                                    full_duration_in_hours = int(settings.FULL_DURATION) / 3600
                                    # @added 20240521 - Feature #5352: vista - bigquery
                                    full_duration_in_hours = int(use_full_duration) / 3600

                                    ionosphere_json_file = '%s/%s.mirage.redis.%sh.json' % (
                                        training_dir, base_name,
                                        str(int(full_duration_in_hours)))
                                    if not os.path.isfile(ionosphere_json_file):
                                        timeseries_json = str(batch_timeseries).replace('[', '(').replace(']', ')')
                                        try:
                                            write_data_to_file(skyline_app, ionosphere_json_file, 'w', timeseries_json)
                                            logger.info('%s added Ionosphere Mirage %sh Redis data timeseries json file :: %s' % (
                                                skyline_app, str(int(full_duration_in_hours)), ionosphere_json_file))
                                        except:
                                            logger.error(traceback.format_exc())
                                            logger.error('error :: failed to add %s Ionosphere Mirage Redis data timeseries json file - %s' % (skyline_app, ionosphere_json_file))

                        # @modified 20170108 - Feature #1830: Ionosphere alerts
                        # Only send smtp_alerter_metrics to Ionosphere
                        # if send_to_ionosphere:
                        if send_to_ionosphere and smtp_alert_enabled_metric:
                            if metric_name in ionosphere_unique_metrics:
                                logger.info('sending an ionosphere metric to Ionosphere - %s' % (base_name))
                            else:
                                logger.info('sending an analyzer metric to Ionosphere for training - %s' % (base_name))

                                # @added 20240523 - Feature #5352: vista - bigquery
                                #                   Feature #5190: Add custom_algorithm results to Mirage and plots
                                #                   Feature #3566: custom_algorithms
                                # Save custom_algorithms_results
                                if custom_algorithms_results:
                                    timeseries_dir = base_name.replace('.', '/')
                                    training_dir = '%s/%s/%s' % (
                                        settings.IONOSPHERE_DATA_FOLDER, str(int(float(metric_timestamp))),
                                        str(timeseries_dir))
                                    if not os.path.exists(training_dir):
                                        mkdir_p(training_dir)
                                    if custom_algorithms_results:
                                        custom_algorithms_results['results_path'] = training_dir
                                        timeseries_json_file = '%s/%s.json' % (training_dir, base_name)
                                        custom_algorithms_results['timeseries_json'] = timeseries_json_file
                                        results_json = '%s/%s.custom_algorithms_results.json' % (training_dir, base_name)
                                        try:
                                            with open(results_json, 'w') as f:
                                                f.write(json.dumps(custom_algorithms_results, skipkeys=True))
                                            os.chmod(results_json, mode=0o644)
                                            logger.info('saved custom_algorithms_results json %s' % results_json)
                                        except Exception as err:
                                            logger.error(traceback.format_exc())
                                            logger.error('error :: failed to save results json %s - %s' % (
                                                results_json, err))

                            try:
                                # @modified 20161228 Feature #1828: ionosphere - mirage Redis data features
                                # Added full_duration
                                # @modified 20170127 - Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
                                # Added ionosphere_parent_id, always zero from Analyzer and Mirage
                                ionosphere_parent_id = 0
                                send_anomalous_metric_to(
                                    skyline_app, 'ionosphere', timeseries_dir,
                                    metric_timestamp, base_name, str(datapoint),
                                    from_timestamp, triggered_algorithms,
                                    # batch_timeseries, str(settings.FULL_DURATION),
                                    batch_timeseries, str(use_full_duration),
                                    str(ionosphere_parent_id))
                                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                                # Moved to Redis key block below
                                # self.sent_to_ionosphere.append(base_name)
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to send_anomalous_metric_to to ionosphere')

                            # @added 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                            redis_set = 'analyzer.batch.sent_to_ionosphere'
                            data = str(base_name)
                            try:
                                self.redis_conn.sadd(redis_set, data)
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to add %s to Redis set %s' % (
                                    str(data), str(redis_set)))

                            # @added 20170403 - Feature #1994: Ionosphere training_dir keys
                            #                   Feature #2000: Ionosphere - validated
                            #                   Feature #1996: Ionosphere - matches page
                            # The addition of this key data could be done in
                            # skyline_function.py, however that would introduce
                            # Redis requirements in the send_anomalous_metric_to
                            # function, which is not desirable I think. So this is
                            # a non-KISS pattern that is replicated in mirage.py as
                            # well.
                            # Each training_dir and data set is now Redis keyed to increase efficiency
                            # in terms of disk I/O for ionosphere.py and making keyed data
                            # available for each training_dir data set so that transient matched data
                            # can be surfaced for the webapp along with directory paths, etc
                            ionosphere_training_data_key = 'ionosphere.training_data.%s.%s' % (str(metric_timestamp), base_name)
                            ionosphere_training_data_key_data = [
                                ['metric_timestamp', int(float(metric_timestamp))],
                                ['base_name', str(base_name)],
                                ['timeseries_dir', str(timeseries_dir)],
                                ['added_by', str(skyline_app)]
                            ]
                            try:
                                self.redis_conn.setex(
                                    ionosphere_training_data_key,
                                    settings.IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR,
                                    # @modified 20190413 - Task #2824: Test redis-py upgrade
                                    #                      Task #2926: Update dependencies
                                    # redis-py 3.x only accepts user data as bytes, strings or
                                    # numbers (ints, longs and floats).  All 2.X users should
                                    # make sure that the keys and values they pass into redis-py
                                    # are either bytes, strings or numbers. Use str
                                    str(ionosphere_training_data_key_data))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to set Redis key %s' % ionosphere_training_data_key)
                            try:
                                del ionosphere_training_data_key_data
                            except:
                                pass

                        if ionosphere_metric:
                            analyzer_metric = False

                        # Only send Analyzer metrics
                        if analyzer_metric and settings.PANORAMA_ENABLED:
                            if not os.path.exists(settings.PANORAMA_CHECK_PATH):
                                mkdir_p(settings.PANORAMA_CHECK_PATH)

                            # @added 20240621 - Feature #5368: analyzer_batch - reprocess
                            # Remove panorama key
                            if reprocess:
                                panorama_key = 'panorama.last_check.anlyzer_batch.%s' % base_name
                                try:
                                    self.redis_conn.delete(panorama_key)
                                except Exception as err:
                                    logger.error('error :: failed to delete Redis key %s, err: %s' % (
                                        panorama_key, err))

                            # Note:
                            # The values are enclosed is single quoted intentionally
                            # as the imp.load_source used results in a shift in the
                            # decimal position when double quoted, e.g.
                            # value = "5622.0" gets imported as
                            # 2016-03-02 12:53:26 :: 28569 :: metric variable - value - 562.2
                            # single quoting results in the desired,
                            # 2016-03-02 13:16:17 :: 1515 :: metric variable - value - 5622.0
                            added_at = str(int(time()))
                            source = 'graphite'
                            panorama_anomaly_data = 'metric = \'%s\'\n' \
                                                    'value = \'%s\'\n' \
                                                    'from_timestamp = \'%s\'\n' \
                                                    'metric_timestamp = \'%s\'\n' \
                                                    'algorithms = %s\n' \
                                                    'triggered_algorithms = %s\n' \
                                                    'app = \'%s\'\n' \
                                                    'source = \'%s\'\n' \
                                                    'added_by = \'%s\'\n' \
                                                    'added_at = \'%s\'\n' \
                                % (base_name, str(datapoint), from_timestamp,
                                   # @modified 20200603 - Feature #3566: custom_algorithms
                                   # metric_timestamp, str(settings.ALGORITHMS),
                                   metric_timestamp, str(algorithms_run),
                                   triggered_algorithms, skyline_app, source,
                                   this_host, added_at)

                            # Create an anomaly file with details about the anomaly
                            panorama_anomaly_file = '%s/%s.%s.txt' % (
                                settings.PANORAMA_CHECK_PATH, added_at,
                                sane_metricname)
                            try:
                                write_data_to_file(
                                    skyline_app, panorama_anomaly_file, 'w',
                                    panorama_anomaly_data)
                                logger.info('added panorama anomaly file :: %s' % (panorama_anomaly_file))
                                if reprocess and oneshot:
                                    # The panorama files are timestamps and this
                                    # process runs fast so if there are multiple
                                    # anomalies chances most will be overwritten
                                    # in the same file and panorama will only
                                    # get one to record
                                    sleep(1)
                                # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                                # Moved to Redis set block below
                                # self.sent_to_panorama.append(base_name)
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to add panorama anomaly file :: %s' % (panorama_anomaly_file))
                            try:
                                del panorama_anomaly_data
                            except:
                                pass

                            # @modified 20240524 - Feature #5352: vista - bigquery
                            # This set is not used in analyzer_batch the same
                            # way that it is used in analyzer to metric the
                            # number of anomalies sent to panorama
                            panorama_metric = False
                            if panorama_metric:
                                redis_set = 'analyzer_batch.sent_to_panorama'
                                data = str(base_name)
                                try:
                                    self.redis_conn.sadd(redis_set, data)
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to add %s to Redis set %s' % (
                                        str(data), str(redis_set)))
                        else:
                            # @modified 20160207 - Branch #922: Ionosphere
                            # Handle if all other apps are not enabled
                            other_app = 'none'
                            if mirage_metric:
                                other_app = 'Mirage'
                            if ionosphere_metric:
                                other_app = 'Ionosphere'
                            logger.info('not adding panorama anomaly file for %s - %s' % (other_app, metric))

                        # Send back to Analyzer to alert
                        if analyzer_metric:
                            send_back_to_analyzer = True

                        # @added 20240606 - Feature #5368: analyzer_batch - reprocess
                        # Do not alert on reprocessing
                        if reprocess:
                            send_back_to_analyzer = False

                        if send_back_to_analyzer:
                            cache_key = '%s.alert.%s.%s' % (skyline_app, metric_timestamp, base_name)
                            # @modified 20201008 - Feature #3772: Add the anomaly_id to the http_alerter json
                            #                      Branch #3068: SNAB
                            # Added algorithms_run
                            cache_key_value = [float(datapoint), base_name, int(float(metric_timestamp)), triggered_algorithms, algorithms_run]
                            try:
                                self.redis_conn.setex(
                                    cache_key, 300,
                                    str(cache_key_value))
                                logger.info(
                                    'add Redis alert key - %s - %s' %
                                    (cache_key, str(cache_key_value)))
                            except Exception as e:
                                logger.error(traceback.format_exc())
                                logger.error(
                                    'error :: failed to add Redis key - %s - [%s, \'%s\', %s, %s, %s] - %s' %
                                    (cache_key, str(datapoint), base_name,
                                        str(int(float(metric_timestamp))),
                                        str(triggered_algorithms),
                                        str(algorithms_run), e))

                            # @added 20230925 - Task #5000: Replace alert key scans with sets
                            # Instead of using expiring keys with have a compute cost with
                            # using scan_iter(match='[PATTERN]') switch to using entries in a
                            # hash key, the management of which has a must lower compute cost.
                            alerts_hash_key = '%s.alerts' % skyline_app
                            hash_key_key = '%s.%s' % (str(metric_timestamp), base_name)
                            hash_key_value = {
                                'value': float(datapoint), 'metric': base_name,
                                'timestamp': int(float(metric_timestamp)),
                                'triggered_algorithms': triggered_algorithms,
                                'algorithms_run': algorithms_run}
                            try:
                                self.redis_conn.hset(alerts_hash_key, hash_key_key, str(hash_key_value))
                                logger.info('added Redis alert key - %s to %s with %s' % (
                                    hash_key_key, alerts_hash_key, str(hash_key_value)))
                            except Exception as err:
                                logger.error('error :: failed to add key - %s to %s with %s - %s' % (
                                    hash_key_key, alerts_hash_key,
                                    str(hash_key_value), err))

                # It could have been deleted by the Roomba
                except TypeError:
                    # @added 20200430 - Feature #3480: batch_processing
                    # Added logging here as the DeletedByRoomba exception is
                    # generally not related to that but related to some other fail
                    # in the processing of the run algorithms phase
                    logger.error(traceback.format_exc())
                    logger.error('error :: added as DeletedByRoomba but possibly not see traceback above')

                    exceptions['DeletedByRoomba'] += 1
                    # @added 20200423 - Feature #3504: Handle airgaps in batch metrics
                    #                   Feature #3480: batch_processing
                    #                   Feature #3486: analyzer_batch
                    # Handle analyzer_batch work being added over and over every
                    # minute by also updating the last_timestamp key if stale,
                    # boring, etc
                    # @modified 20240522 - Feature #5352: vista - bigquery
                    # Declare last_metric_timestamp_key once
                    # last_metric_timestamp_key = 'last_timestamp.%s' % base_name
                    try:
                        int_metric_timestamp = int(time())
                        self.redis_conn.setex(
                            last_metric_timestamp_key, 2592000, int_metric_timestamp)
                        logger.info('set Redis key %s to %s, even though it has been deleted by Roomba' % (
                            last_metric_timestamp_key, str(int_metric_timestamp)))
                    except:
                        logger.error('error :: failed to set Redis key %s, even though it is has been deleted by Roomba' % last_metric_timestamp_key)

                    # @added 20240522 - Task #5358: analyzer.last_timestamp.base_names Redis hash to replace last_timestamp keys
                    last_timestamps_hash_dict[base_name] = int(time())
                    # @added 20240602 - Feature #5368: analyzer_batch - reprocess
                    if reprocess:
                        try:
                            del last_timestamps_hash_dict[base_name]
                        except:
                            pass

                except TooShort:
                    exceptions['TooShort'] += 1
                    # @added 20200423 - Feature #3504: Handle airgaps in batch metrics
                    #                   Feature #3480: batch_processing
                    #                   Feature #3486: analyzer_batch
                    # @modified 20240522 - Feature #5352: vista - bigquery
                    # Declare last_metric_timestamp_key once
                    # last_metric_timestamp_key = 'last_timestamp.%s' % base_name
                    try:
                        int_metric_timestamp = int(batch_timeseries[-1][0])
                    except:
                        int_metric_timestamp = None
                    if int_metric_timestamp:
                        try:
                            self.redis_conn.setex(
                                last_metric_timestamp_key, 2592000,
                                int_metric_timestamp)
                            # @modified 20200728 - Feature #3480: batch_processing
                            #                      Feature #3486: analyzer_batch
                            # Only log on the last data point, not on all
                            if int_metric_timestamp == int(last_redis_data_timestamp):
                                logger.info('set Redis key %s to %s, even though it is too short' % (
                                    last_metric_timestamp_key, str(int_metric_timestamp)))
                        except Exception as err:
                            logger.error('error :: failed to set Redis key %s, even though it is too short - %s' % (
                                last_metric_timestamp_key, err))

                        # @added 20240522 - Task #5358: analyzer.last_timestamp.base_names Redis hash to replace last_timestamp keys
                        last_timestamps_hash_dict[base_name] = int(batch_timeseries[-1][0])
                    # @added 20240602 - Feature #5368: analyzer_batch - reprocess
                    if reprocess:
                        try:
                            del last_timestamps_hash_dict[base_name]
                        except:
                            pass

                except Stale:
                    exceptions['Stale'] += 1
                    # @added 20200423 - Feature #3504: Handle airgaps in batch metrics
                    #                   Feature #3480: batch_processing
                    #                   Feature #3486: analyzer_batch
                    # @modified 20240522 - Feature #5352: vista - bigquery
                    # Declare last_metric_timestamp_key once
                    # last_metric_timestamp_key = 'last_timestamp.%s' % base_name
                    try:
                        int_metric_timestamp = int(batch_timeseries[-1][0])
                    except:
                        int_metric_timestamp = None
                    if int_metric_timestamp:
                        try:
                            self.redis_conn.setex(
                                last_metric_timestamp_key, 2592000,
                                int_metric_timestamp)
                            # @modified 20200728 - Feature #3480: batch_processing
                            #                      Feature #3486: analyzer_batch
                            # Only log on the last data point, not on all
                            if int_metric_timestamp == int(last_redis_data_timestamp):
                                logger.info('set Redis key %s to %s, even though it is stale' % (
                                    last_metric_timestamp_key, str(int_metric_timestamp)))
                        except:
                            logger.error('error :: failed to set Redis key %s, even though it is stale' % last_metric_timestamp_key)
                        # @added 20240522 - Task #5358: analyzer.last_timestamp.base_names Redis hash to replace last_timestamp keys
                        last_timestamps_hash_dict[base_name] = int(batch_timeseries[-1][0])

                    # @added 20240602 - Feature #5368: analyzer_batch - reprocess
                    if reprocess:
                        try:
                            del last_timestamps_hash_dict[base_name]
                        except:
                            pass

                except Boring:
                    exceptions['Boring'] += 1
                    # @added 20200423 - Feature #3504: Handle airgaps in batch metrics
                    #                   Feature #3480: batch_processing
                    #                   Feature #3486: analyzer_batch
                    # @modified 20240522 - Feature #5352: vista - bigquery
                    # Declare last_metric_timestamp_key once
                    # last_metric_timestamp_key = 'last_timestamp.%s' % base_name
                    try:
                        int_metric_timestamp = int(batch_timeseries[-1][0])
                        self.redis_conn.setex(
                            last_metric_timestamp_key, 2592000,
                            int_metric_timestamp)
                        # @modified 20200728 - Feature #3480: batch_processing
                        #                      Feature #3486: analyzer_batch
                        # Only log on the last data point, not on all
                        if int_metric_timestamp == int(last_redis_data_timestamp):
                            logger.info('set Redis key %s to %s, even though it is boring' % (
                                last_metric_timestamp_key, str(int_metric_timestamp)))
                    except:
                        logger.error('error :: failed to set Redis key %s, even though it is boring' % last_metric_timestamp_key)

                    # @added 20240522 - Task #5358: analyzer.last_timestamp.base_names Redis hash to replace last_timestamp keys
                    last_timestamps_hash_dict[base_name] = int(batch_timeseries[-1][0])
                    # @added 20240602 - Feature #5368: analyzer_batch - reprocess
                    if reprocess:
                        try:
                            del last_timestamps_hash_dict[base_name]
                        except:
                            pass

                except:
                    logger.error(traceback.format_exc())
                    logger.error('error - Other error reported')
                    exceptions['Other'] += 1
                    # @added 20200423 - Feature #3504: Handle airgaps in batch metrics
                    #                   Feature #3480: batch_processing
                    #                   Feature #3486: analyzer_batch
                    # @modified 20240522 - Feature #5352: vista - bigquery
                    # Declare last_metric_timestamp_key once
                    # last_metric_timestamp_key = 'last_timestamp.%s' % base_name
                    try:
                        int_metric_timestamp = int(time())
                        self.redis_conn.setex(
                            last_metric_timestamp_key, 2592000,
                            int_metric_timestamp)
                        logger.error('error :: set Redis key %s to %s, even though it an other error has been thrown' % (
                            last_metric_timestamp_key, str(int_metric_timestamp)))
                    except:
                        logger.error('error :: failed to set Redis key %s, when other exception was thrown' % last_metric_timestamp_key)

                    # @added 20240522 - Task #5358: analyzer.last_timestamp.base_names Redis hash to replace last_timestamp keys
                    last_timestamps_hash_dict[base_name] = int(time())
                    # @added 20240602 - Feature #5368: analyzer_batch - reprocess
                    if reprocess:
                        try:
                            del last_timestamps_hash_dict[base_name]
                        except:
                            pass

            # @added 20220113 - Feature #3566: custom_algorithms
            #                   Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
            # break if approaching spin_batch_process limit
            if int(time()) > (int(spin_start) + 290):
                logger.info('approaching time limit, exiting')
                break

            # Remove for work list
            redis_set = 'analyzer.batch'

            # @added 20240602 - Feature #5368: analyzer_batch - reprocess
            if reprocess:
                try:
                    self.redis_conn.hdel(redis_reprocess_hash, base_name)
                    logger.info('analyzer_batch :: removed reprocess metric %s from Redis hash %s' % (
                        base_name, redis_reprocess_hash))
                except Exception as err:
                    logger.error('error :: analyzer_batch :: failed to remove %s from Redis hash %s, err: %s' % (
                        base_name, redis_reprocess_hash, err))
                continue

            # @added 20240521 - Feature #5352: vista - bigquery
            if use_last_analyzed_timestamp:
                last_analyzed_timestamp = use_last_analyzed_timestamp

            data = [metric_name, int(last_analyzed_timestamp)]
            try:
                self.redis_conn.srem(redis_set, str(data))
                logger.info('analyzer_batch :: removed batch metric item - %s - from Redis set - %s' % (str(data), redis_set))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: analyzer_batch :: failed to remove batch metric item - %s - from Redis set - %s' % (str(data), redis_set))

            try:
                del timeseries
            except:
                pass
            try:
                del timestamps_to_analyse
            except:
                pass
            try:
                del batch_timeseries
            except:
                pass

            # @added 20240520 - Feature #5352: vista - bigquery
            if unzerofilled_timeseries:
                try:
                    del unzerofilled_timeseries
                except:
                    pass

            if not batch_mode:
                try:
                    del mirage_unique_metrics
                except:
                    pass
                try:
                    del ionosphere_unique_metrics
                except:
                    pass
                try:
                    del derivative_metrics
                except:
                    pass
                try:
                    del non_derivative_metrics
                except:
                    pass
                try:
                    del non_derivative_monotonic_metrics
                except:
                    pass
                try:
                    del non_smtp_alerter_metrics
                except:
                    pass

        # Add values to the queue so the parent process can collate
        for key, value in anomaly_breakdown.items():
            self.batch_anomaly_breakdown_q.put((key, value))

        for key, value in exceptions.items():
            self.batch_exceptions_q.put((key, value))

        # @added 20220420 - Feature #4530: namespace.analysed_events
        namespace_analysed = defaultdict(int)
        for base_name in analysed_metrics:
            parent_namespace = base_name.split('.')[0]
            namespace_analysed[parent_namespace] += 1
        date_string = str(strftime('%Y-%m-%d', gmtime()))
        namespace_analysed_events_hash = 'namespace.analysed_events.%s.%s' % (skyline_app, date_string)
        for namespace in list(namespace_analysed.keys()):
            try:
                self.redis_conn.hincrby(namespace_analysed_events_hash, namespace, namespace_analysed[namespace])
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to increment %s Redis hash - %s' % (
                    namespace_analysed_events_hash, err))
        try:
            self.redis_conn.expire(namespace_analysed_events_hash, (86400 * 15))
            logger.info('updated %s Redis hash' % namespace_analysed_events_hash)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to set expire %s Redis hash - %s' % (
                namespace_analysed_events_hash, err))

        # @added 20220504 - Feature #2580: illuminance
        if len(illuminance_dict) > 0:
            logger.info('calling add_illuminance_entries with %s entries to add' % (
                str(len(illuminance_dict))))
            current_illuminance_dict = {}
            try:
                current_illuminance_dict = add_illuminance_entries(self, skyline_app, int(run_timestamp), illuminance_dict)
            except Exception as err:
                logger.error('error :: add_illuminance_entries failed - %s' % (
                    err))
            logger.info('illuminance Redis hash now has %s entries' % (
                str(len(current_illuminance_dict))))

        # @added 20220919 - Feature #4676: analyzer - illuminance.all key
        if len(illuminance_all_dict) > 0:
            logger.info('calling add_illuminance_entries (all) with %s entries to add' % (
                str(len(illuminance_all_dict))))
            current_illuminance_all_dict = {}
            try:
                current_illuminance_all_dict = add_illuminance_entries(self, skyline_app, int(run_timestamp), illuminance_all_dict)
            except Exception as err:
                logger.error('error :: add_illuminance_entries (all) failed - %s' % (
                    err))
            logger.info('illuminance_all Redis hash now has %s entries' % (
                str(len(current_illuminance_all_dict))))

        # @added 20240522 - Task #5358: analyzer.last_timestamp.base_names Redis hash to replace last_timestamp keys
        if len(last_timestamps_hash_dict) > 0:
            try:
                self.redis_conn.hset(redis_last_timestamp_hash, mapping=last_timestamps_hash_dict)
                logger.info('%s updated with last timestamp for %s metrics' % (
                    redis_last_timestamp_hash, str(len(last_timestamps_hash_dict))))
            except Exception as err:
                logger.error('error :: failed to update %s Redis hash key, err: %s' % (
                    redis_last_timestamp_hash, err))

        # @added 20240725 - Feature #5400: analyzer_batch - stats
        stats['total_analyzed'] = len(analysed_metrics)
        stats['total_anomalies'] = len(total_anomalies)
        stats['run_time'] = time() - spin_start
        try:
            self.redis_conn.hset('analyzer_batch.stats', str(i), str(stats))
            logger.info('analyzer_batch.stats updated with stats: %s' % str(stats))
        except Exception as err:
            logger.error('error :: failed to update analyzer_batch.stats Redis hash key with stats: %s, err: %s' % (
                str(stats), err))

        # @added 20240602 - Feature #5368: analyzer_batch - reprocess
        if reprocess:
            logger.info('reprocessed %s metrics in %s seconds' % (
                str(metrics_processed), str((time() - spin_start))))

        LOCAL_DEBUG = False

        spin_end = time() - spin_start
        logger.info('spin_batch_process took %.2f seconds' % spin_end)
        return

    def run(self):
        """
        - Called when the process intializes.

        - Determine if Redis is up and discover the number of `unique metrics`.

        - Divide the `unique_metrics` between the number of `ANALYZER_PROCESSES`
          and assign each process a set of metrics to analyse for anomalies.

        - Wait for the processes to finish.

        - Determine whether if any anomalous metrics require:

            - Alerting on (and set `EXPIRATION_TIME` key in Redis for alert).
            - Feed to another module e.g. mirage.
            - Alert to syslog.

        - Populate the webapp json with the anomalous_metrics details.

        - Log the details about the run to the skyline analyzer log.

        - Send skyline.analyzer metrics to `GRAPHITE_HOST`
        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        if os.path.isfile(skyline_app_logwait):
            try:
                os.remove(skyline_app_logwait)
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_logwait)

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
            logger.error('error - bin/%s.d log management seems to have failed, continuing' % skyline_app)
            try:
                os.remove(skyline_app_loglock)
                logger.info('log lock file removed')
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_loglock)
        else:
            logger.info('bin/%s.d log management done' % skyline_app)

        # @added 20190417 - Feature #2950: Report defaulted settings to log
        # Added all the globally declared settings to enable reporting in the
        # log the state of each setting.
        try:
            SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
            if SERVER_METRIC_PATH == '.':
                SERVER_METRIC_PATH = ''
            logger.info('SERVER_METRIC_PATH is set from settings.py to %s' % str(SERVER_METRIC_PATH))
        except:
            SERVER_METRIC_PATH = ''
            logger.info('warning :: SERVER_METRIC_PATH is not declared in settings.py, defaults to \'\'')
        logger.info('skyline_app_graphite_namespace is set to %s' % str(skyline_app_graphite_namespace))
        try:
            ANALYZER_ENABLED = settings.ANALYZER_ENABLED
            logger.info('ANALYZER_ENABLED is set to %s' % str(ANALYZER_ENABLED))
        except:
            ANALYZER_ENABLED = True
            logger.info('warning :: ANALYZER_ENABLED is not declared in settings.py, defaults to True')

        if not os.path.exists(settings.SKYLINE_TMP_DIR):
            # @modified 20160803 - Adding additional exception handling to Analyzer
            try:
                mkdir_p(settings.SKYLINE_TMP_DIR)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to create %s' % settings.SKYLINE_TMP_DIR)

        while 1:
            now = time()
            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Analyzer cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                sleep(10)
                try:
                    self.redis_conn = get_redis_conn(skyline_app)
                except:
                    logger.error(traceback.format_exc())
                    # logger.error('error :: Analyzer cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                    logger.error('error :: Analyzer cannot connect to get_redis_conn')
                continue
            try:
                self.redis_conn_decoded.ping()
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Analyzer batch cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                sleep(10)
                try:
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                except:
                    logger.error(traceback.format_exc())
                    # logger.error('error :: Analyzer cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                    logger.error('error :: Analyzer batch cannot connect to get_redis_conn')
                continue

            # @added 20240602 - Feature #5368: analyzer_batch - reprocess
            analyzer_batch_reprocess_work = {}
            analyzer_batch_work = []

            # Determine if any metric has been added to process
            while True:

                # Report app up
                try:
                    last_up = int(now)
                    self.redis_conn.setex(skyline_app, 120, last_up)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: Analyzer batch could not update the Redis %s key' % skyline_app)

                analyzer_batch_work = []

                # @added 20240602 - Feature #5368: analyzer_batch - reprocess
                # Discover metrics to reprocess
                analyzer_batch_reprocess_work = {}
                redis_analyzer_batch_reprocess_work = {}
                redis_hash = 'analyzer.batch.reprocess_work'
                try:
                    redis_analyzer_batch_reprocess_work = self.redis_conn_decoded.hgetall(redis_hash)
                except Exception as err:
                    logger.error('error :: could not query Redis hash %s, err: %s' % (redis_hash, err))

                if redis_analyzer_batch_reprocess_work:
                    if len(redis_analyzer_batch_reprocess_work) > 0:
                        logger.debug('debug :: len(redis_analyzer_batch_reprocess_work): %s' % str(len(redis_analyzer_batch_reprocess_work)))
                        for base_name, dict_str in redis_analyzer_batch_reprocess_work.items():
                            try:
                                metric_name = str(base_name)
                                if HORIZON_SHARDS:
                                    shard_metric = is_shard_metric(base_name)
                                    if not shard_metric:
                                        try:
                                            self.redis_conn_decoded.hdel(redis_hash, base_name)
                                        except Exception as err:
                                            logger.error('error :: failed to hdel %s from %s, err: %s' % (
                                                base_name, redis_hash, err))                        
                                        continue
                                reprocess_work_dict = literal_eval(dict_str)
                                reprocess_from = None
                                try:
                                    reprocess_from = reprocess_work_dict['from_timestamp']
                                except Exception as err:
                                    logger.error('error :: failed to determine reprocess_from for %s from analyzer_batch_reprocess_work, err: %s' % (
                                        base_name, err))                        
                                if not reprocess_from:
                                    logger.error('error :: cannot add %s to reprocess removing from %s' % (
                                        base_name, redis_hash))
                                    try:
                                        self.redis_conn_decoded.hdel(redis_hash, base_name)
                                    except Exception as err:
                                        logger.error('error :: failed to hdel %s from %s, err: %s' % (
                                            base_name, redis_hash, err))                        
                                    continue
                                analyzer_batch_reprocess_work[base_name] = reprocess_work_dict
                            except Exception as err:
                                logger.error('error :: failed to add %s to analyzer_batch_reprocess_work for %s, err: %s' % (
                                    str(dict_str), base_name, err))                        
                if len(analyzer_batch_reprocess_work) > 0:
                    logger.info('there are %s metrics to reprocess in the %s Redis hash' % (
                        str(len(analyzer_batch_reprocess_work)), redis_hash))
                    break

                # Discover metrics to analyze
                redis_set = 'analyzer.batch'
                try:
                    analyzer_batch_work = self.redis_conn_decoded.smembers(redis_set)
                except Exception as e:
                    logger.error('error :: could not query Redis for set %s - %s' % (redis_set, e))
                if analyzer_batch_work:
                    analyzer_batch_work_queue_items = len(analyzer_batch_work)
                    if analyzer_batch_work_queue_items > 0:
                        logger.info('there are %s metrics to process in the %s Redis set' % (
                            str(analyzer_batch_work_queue_items), redis_set))
                        break
                else:
                    logger.info('there are no batch metrics to process')
                    sleep(1)

            metric_name = None
            last_analyzed_timestamp = None

            for analyzer_batch in analyzer_batch_work:
                try:
                    batch_processing_metric = literal_eval(analyzer_batch)
                    metric_name = str(batch_processing_metric[0])
                    last_analyzed_timestamp = int(batch_processing_metric[1])
                    break
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: could not determine details from analyzer_batch entry')
                    metric_name = None
                    last_analyzed_timestamp = None
                    batch_processing_metric = None
                    sleep(1)

            # @added 20200728 - Feature #3480: batch_processing
            #                   Feature #3486: analyzer_batch
            # If multiple work items exist sort them by oldest timestamp and
            # process the item with the oldest timestamp first
            if analyzer_batch_work:
                unsorted_analyzer_batch_work = []
                for analyzer_batch in analyzer_batch_work:
                    try:
                        batch_processing_metric = literal_eval(analyzer_batch)
                        metric_name = str(batch_processing_metric[0])
                        last_analyzed_timestamp = int(batch_processing_metric[1])
                        unsorted_analyzer_batch_work.append([metric_name, last_analyzed_timestamp])
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: could not determine details from analyzer_batch entry')
                sorted_analyzer_batch_work = sorted(unsorted_analyzer_batch_work, key=lambda x: x[1])
                logger.info('there are %s work items in the sorted_analyzer_batch_work list' % (str(len(sorted_analyzer_batch_work))))

                # @added 20201017 - Feature #3818: ANALYZER_BATCH_PROCESSING_OVERFLOW_ENABLED
                # Remove multiple entries for metrics and only add the latest
                # timestamp item per metric
                original_work_queue_length = len(sorted_analyzer_batch_work)
                metrics = list(set([item[0] for item in sorted_analyzer_batch_work]))
                logger.info('there are %s unique metrics with work items in the sorted_analyzer_batch_work list' % (str(len(metrics))))
                if len(metrics) < original_work_queue_length:
                    new_analyzer_batch_work = []
                    for metric in metrics:
                        work_timestamps = []
                        for item in sorted_analyzer_batch_work:
                            if item[0] == metric:
                                timestamp = item[1]
                                work_timestamps.append(timestamp)
                        new_analyzer_batch_work.append([metric, timestamp])
                        if len(work_timestamps) > 1:
                            last_work_timestamp = work_timestamps[-1]
                            for work_timestamp in work_timestamps:
                                if work_timestamp != last_work_timestamp:
                                    # Remove from work list
                                    redis_set = 'analyzer.batch'
                                    data = [metric, int(work_timestamp)]
                                    try:
                                        self.redis_conn.srem('analyzer.batch', str(data))
                                        logger.info('analyzer_batch :: newer work exists, removed older work item - %s - from Redis set - %s' % (str(data), redis_set))
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: analyzer_batch :: failed to remove older work item - %s - from Redis set - %s' % (str(data), redis_set))
                    sorted_analyzer_batch_work = sorted(new_analyzer_batch_work, key=lambda x: x[1])
                    new_work_queue_length = len(sorted_analyzer_batch_work)
                    if original_work_queue_length != new_work_queue_length:
                        pruned_item_count = original_work_queue_length - new_work_queue_length
                        logger.info('the analyzer.batch Redis set was pruned of %s older items which have newer work items' % str(pruned_item_count))

                try:
                    metric_name = str(sorted_analyzer_batch_work[0][0])
                    last_analyzed_timestamp = int(sorted_analyzer_batch_work[0][1])
                    batch_processing_metric = [metric_name, last_analyzed_timestamp]
                except Exception as err:
                    logger.error('error :: analyzer_batch :: failed to determine metric_name, err: %s' % err)
                    metric_name = None

            # @added 20240602 - Feature #5368: analyzer_batch - reprocess
            if len(analyzer_batch_reprocess_work) > 0:
                metric_names = list(analyzer_batch_reprocess_work.keys())
                metric_name = metric_names[0]
                batch_processing_metric = 'reprocess job'

            if not metric_name:
                break

            # @added 20200904 - Feature #3486: analyzer_batch
            #                   Feature #3480: batch_processing
            #                   Task #3730: Validate Mirage running multiple processes
            # Remove any existing algorithm.error and timing files from any
            # previous runs
            pattern = '%s.*.algorithm.error' % skyline_app
            try:
                for f in os.listdir(settings.SKYLINE_TMP_DIR):
                    if re.search(pattern, f):
                        try:
                            os.remove(os.path.join(settings.SKYLINE_TMP_DIR, f))
                            logger.info('cleaning up old error file - %s' % (str(f)))
                        except OSError:
                            pass
            except:
                logger.error('error :: failed to cleanup algorithm.error files')
                logger.info(traceback.format_exc())
            # @modified 20220506 - Feature #3486: analyzer_batch
            # pattern = '%s.*.algorithm.timings' % skyline_app
            pattern = '%s.*.(timings|count)' % skyline_app
            try:
                for f in os.listdir(settings.SKYLINE_TMP_DIR):
                    if re.search(pattern, f):
                        try:
                            os.remove(os.path.join(settings.SKYLINE_TMP_DIR, f))
                            logger.info('cleaning up old timings/count file - %s' % (str(f)))
                        except OSError:
                            pass
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to cleanup algorithm.timing files')

            logger.info('processing - %s' % str(batch_processing_metric))

            # Spawn processes
            batch_pids = []
            spawned_batch_pids = []
            batch_pid_count = 0
            run_timestamp = now

            # @modified 20240602 - Feature #5368: analyzer_batch - reprocess
            if len(analyzer_batch_reprocess_work) == 0:
                for i in range(1, 2):
                    if BATCH_MODE:
                        batch_p = Process(target=self.spin_batch_process, args=(i, run_timestamp, 'batch_mode', 0, sorted_analyzer_batch_work[0:300]))
                    else:
                        batch_p = Process(target=self.spin_batch_process, args=(i, run_timestamp, metric_name, last_analyzed_timestamp))
                    batch_pids.append(batch_p)
                    batch_pid_count += 1
                    logger.info('starting 1 of %s spin_batch_process' % (str(batch_pid_count)))
                    batch_p.start()
                    spawned_batch_pids.append(batch_p.pid)

            # @added 20240602 - Feature #5368: analyzer_batch - reprocess
            if len(analyzer_batch_reprocess_work) > 0:
                process_analyzer_batch_reprocess_work = {}
                for i in range(1, (ANALYZER_BATCH_PROCESSES + 1)):
                    process_analyzer_batch_reprocess_work[i] = {}
                assign_to = 1
                for base_name in list(analyzer_batch_reprocess_work.keys()):
                    process_analyzer_batch_reprocess_work[assign_to][base_name] = analyzer_batch_reprocess_work[base_name]
                    assign_to += 1
                    if assign_to > ANALYZER_BATCH_PROCESSES:
                        assign_to = 1
                for i in range(1, (ANALYZER_BATCH_PROCESSES + 1)):
                    assigned_work = process_analyzer_batch_reprocess_work[i]
                    if len(assigned_work) > 0:
                        try:
                            batch_p = Process(target=self.spin_batch_process, args=(i, run_timestamp, 'batch_mode', 0, [], assigned_work))
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: spin_batch_process failed with analyzer_batch_reprocess_work: %s, err: %s' % (
                                str(analyzer_batch_reprocess_work), err))                        
                        batch_pids.append(batch_p)
                        batch_pid_count += 1
                        logger.info('starting %s of %s reprocessing spin_batch_process' % (str(batch_pid_count), str(ANALYZER_BATCH_PROCESSES)))
                        batch_p.start()
                        spawned_batch_pids.append(batch_p.pid)

            # Send wait signal to zombie processes
            # for p in pids:
            #     p.join()
            # Self monitor processes and terminate if any spin_batch_process
            # that has run for longer than 300 seconds
            p_starts = time()
            while time() - p_starts <= 300:
                if any(p.is_alive() for p in batch_pids):
                    # Just to avoid hogging the CPU
                    if int(time()) > (last_up + 55):
                        # Report app up
                        try:
                            last_up = int(time())
                            self.redis_conn.setex(skyline_app, 120, last_up)
                        except Exception as err:
                            logger.error('error :: Analyzer batch could not update the Redis %s key, err: %s' % (skyline_app, err))
                    sleep(.5)
                else:
                    # All the processes are done, break now.
                    time_to_run = time() - p_starts
                    logger.info('1 spin_batch_process completed in %.2f seconds' % (time_to_run))
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('timed out, killing all spin_batch_process processes')
                for p in batch_pids:
                    p.terminate()
                    # p.join()

            for p in batch_pids:
                if p.is_alive():
                    logger.info('stopping spin_process - %s' % (str(p.is_alive())))
                    # @modified 20240202 - Task #5178: Build and test skyline v4.1.0
                    # p.join()
                    killing_pid = p.pid
                    logger.info('%s :: kill spin_process with pid: %s' % (skyline_app, str(killing_pid)))
                    p.terminate()
                    logger.info('%s :: killed spin_process process with pid: %s' % (skyline_app, str(killing_pid)))

            # Grab data from the queue and populate dictionaries
            exceptions = {}
            anomaly_breakdown = {}
            while 1:
                try:
                    key, value = self.batch_anomaly_breakdown_q.get_nowait()
                    if key not in anomaly_breakdown.keys():
                        anomaly_breakdown[key] = value
                    else:
                        anomaly_breakdown[key] += value
                except Empty:
                    break

            while 1:
                try:
                    key, value = self.batch_exceptions_q.get_nowait()
                    if key not in exceptions.keys():
                        exceptions[key] = value
                    else:
                        exceptions[key] += value
                except Empty:
                    break

            # @added 20200904 - Feature #3486: analyzer_batch
            #                   Feature #3480: batch_processing
            #                   Task #3730: Validate Mirage running multiple processes
            # Report any algorithm errors
            pattern = '%s.*.algorithm.error' % skyline_app
            try:
                for f in os.listdir(settings.SKYLINE_TMP_DIR):
                    if re.search(pattern, f):
                        try:
                            algorithm_error_file = os.path.join(settings.SKYLINE_TMP_DIR, f)
                            if os.path.isfile(algorithm_error_file):
                                logger.error('error :: error reported in %s' % (
                                    algorithm_error_file))
                                try:
                                    with open(algorithm_error_file, 'r') as f:
                                        error_string = f.read()
                                    logger.error('%s' % str(error_string))
                                except:
                                    logger.error('error :: failed to read error file - %s' % algorithm_error_file)
                                try:
                                    os.remove(algorithm_error_file)
                                except OSError:
                                    pass
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to check algorithm errors')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to check algorithm errors')

            # @added 20191021 - Bug #3288: Always send anomaly_breakdown and exception metrics
            #                   Branch #3262: py3
            exceptions_metrics = ['Boring', 'Stale', 'TooShort', 'Other']
            try:
                for i_exception in exceptions_metrics:
                    if i_exception not in exceptions.keys():
                        exceptions[i_exception] = 0

                # @added 20200607 - Feature #3566: custom_algorithms
                anomaly_breakdown_algorithms = list(settings.ALGORITHMS)
                if CUSTOM_ALGORITHMS:
                    for custom_algorithm in CUSTOM_ALGORITHMS:
                        anomaly_breakdown_algorithms.append(custom_algorithm)

                # @modified 20200607 - Feature #3566: custom_algorithms
                # for i_anomaly_breakdown in settings.ALGORITHMS:
                for i_anomaly_breakdown in anomaly_breakdown_algorithms:
                    if i_anomaly_breakdown not in anomaly_breakdown.keys():
                        anomaly_breakdown[i_anomaly_breakdown] = 0

                exceptions_string = ''
                for i_exception in list(exceptions.keys()):
                    if exceptions_string == '':
                        exceptions_string = '%s: %s' % (str(i_exception), str(exceptions[i_exception]))
                    else:
                        exceptions_string = '%s, %s: %s' % (exceptions_string, str(i_exception), str(exceptions[i_exception]))
                logger.info('exceptions - %s' % str(exceptions_string))

                anomaly_breakdown_string = ''
                if anomaly_breakdown:
                    for i_anomaly_breakdown in list(anomaly_breakdown.keys()):
                        if anomaly_breakdown_string == '':
                            anomaly_breakdown_string = '%s: %s' % (str(i_anomaly_breakdown), str(anomaly_breakdown[i_anomaly_breakdown]))
                        else:
                            anomaly_breakdown_string = '%s, %s: %s' % (anomaly_breakdown_string, str(i_anomaly_breakdown), str(anomaly_breakdown[i_anomaly_breakdown]))
                    logger.info('anomaly_breakdown - %s' % str(anomaly_breakdown_string))
                else:
                    logger.info('anomaly_breakdown - none, no anomalies')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: could not exceptions and anomaly_breakdown details')
            try:
                del exceptions
            except:
                pass
            try:
                del anomaly_breakdown
            except:
                pass
            try:
                # with self.batch_exceptions_q.mutex:
                #     self.batch_exceptions_q.queue.clear()
                logger.info('clearing self.batch_exceptions_q of %s items' % str(self.batch_exceptions_q.qsize()))
                while not self.batch_exceptions_q.empty():
                    try:
                        drop_value = self.batch_exceptions_q.get()
                        del drop_value
                    except:
                        pass
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: clearing self.batch_exceptions_q failed - %s' % err)
            try:
                # with self.batch_anomaly_breakdown_q.mutex:
                #    self.batch_anomaly_breakdown_q.queue.clear()
                logger.info('clearing self.batch_anomaly_breakdown_q of %s items' % str(self.batch_anomaly_breakdown_q.qsize()))
                while not self.batch_anomaly_breakdown_q.empty():
                    try:
                        drop_value = self.batch_anomaly_breakdown_q.get()
                        del drop_value
                    except:
                        pass
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: clearing self.batch_anomaly_breakdown_q failed, err: %s' % err)
