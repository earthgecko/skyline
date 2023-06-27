"""
analyzer_labelled_metrics.py
"""
import logging
try:
    from Queue import Empty
except:
    from queue import Empty
# from redis import StrictRedis
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
from signal import SIGKILL
from math import ceil
import traceback
from sys import exit as sys_exit
import resource
from ast import literal_eval

from random import shuffle
# @added 20220722 - Task #4624: Change all dict copy to deepcopy
import copy

import numpy as np
import pandas as pd
import memray

from prometheus_client.parser import _parse_labels as parse_labels

import settings
from skyline_functions import (
    mkdir_p, nonNegativeDerivative, get_redis_conn, get_redis_conn_decoded,
    write_data_to_file)

# @added 20200425 - Feature #3512: matched_or_regexed_in_list function
#                   Feature #3508: ionosphere.untrainable_metrics
#                   Feature #3486: analyzer_batch
from matched_or_regexed_in_list import matched_or_regexed_in_list

from alerters import trigger_alert
from algorithms import run_selected_algorithm
from algorithm_exceptions import TooShort, Stale, Boring, EmptyTimeseries

from functions.illuminance.add_illuminance_entries import add_illuminance_entries
# from functions.prometheus.metric_name_labels_parser import metric_name_labels_parser
from functions.timeseries.downsample import downsample_timeseries
from functions.timeseries.is_stationary import is_stationary

from functions.graphite.send_graphite_metric import send_graphite_metric
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name

from functions.timeseries.strictly_increasing_monotonicity import strictly_increasing_monotonicity

# @added 20230329 - Feature #4882: labelled_metrics - resolution and data sparsity
#                   Feature #3870: metrics_manager - check_data_sparsity
from functions.timeseries.determine_data_frequency import determine_data_frequency
from functions.timeseries.determine_data_sparsity import determine_data_sparsity

# @added 20230404 - Feature #4890: analyzer_labelled_metrics - use mrange
from functions.analyzer.get_tenant_id_mrange_split import get_tenant_id_mrange_split

send_algorithm_run_metrics = False
skyline_app = 'analyzer'
skyline_app_thunder_key = 'analyzer_labelled_metrics'

skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

this_host = str(os.uname()[1])

try:
    ANALYZER_LABELLED_METRICS_PROCESSES = settings.ANALYZER_LABELLED_METRICS_PROCESSES
except:
    ANALYZER_LABELLED_METRICS_PROCESSES = 2

try:
    MAX_ANALYZER_LABELLED_METRICS_PROCESS_RUNTIME = settings.MAX_ANALYZER_LABELLED_METRICS_PROCESS_RUNTIME
except:
    MAX_ANALYZER_LABELLED_METRICS_PROCESS_RUNTIME = 180

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

try:
    ANALYZER_ENABLED = settings.ANALYZER_ENABLED
    logger.info('ANALYZER_ENABLED is set to %s' % str(ANALYZER_ENABLED))
except:
    ANALYZER_ENABLED = True
    logger.info('warning :: ANALYZER_ENABLED is not declared in settings.py, defaults to True')

try:
    # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
    # CUSTOM_ALGORITHMS = settings.CUSTOM_ALGORITHMS.copy()
    CUSTOM_ALGORITHMS = copy.deepcopy(settings.CUSTOM_ALGORITHMS)
except:
    CUSTOM_ALGORITHMS = None
try:
    DEBUG_CUSTOM_ALGORITHMS = settings.DEBUG_CUSTOM_ALGORITHMS
except:
    DEBUG_CUSTOM_ALGORITHMS = False

try:
    VERBOSE_LOGGING = settings.ANALYZER_VERBOSE_LOGGING
except:
    VERBOSE_LOGGING = False

# @added 20210513 - Feature #4068: ANALYZER_SKIP
try:
    ANALYZER_SKIP = list(settings.ANALYZER_SKIP)
except:
    ANALYZER_SKIP = []

# @added 20210519 - Feature #4076: CUSTOM_STALE_PERIOD
custom_stale_metrics_hash_key = 'analyzer.metrics_manager.custom_stale_periods'
try:
    # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
    # CUSTOM_STALE_PERIOD = settings.CUSTOM_STALE_PERIOD.copy()
    CUSTOM_STALE_PERIOD = copy.deepcopy(settings.CUSTOM_STALE_PERIOD)
except:
    CUSTOM_STALE_PERIOD = {}

# @added 20230404 - Feature #4888: analyzer - load_shedding
LOAD_SHEDDING_ENABLED = True
try:
    LOAD_SHEDDING_ENABLED = settings.LOAD_SHEDDING_ENABLED
except:
    LOAD_SHEDDING_ENABLED = True

skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)

LOCAL_DEBUG = False

# @added 20191107 - Branch #3262: py3
alert_test_file = '%s/%s_alert_test.txt' % (settings.SKYLINE_TMP_DIR, skyline_app)

# In Skyline a metric is either a counter (derivative) or a gauge
skyline_metric_types = {'COUNTER': 1, 'GAUGE': 0}
skyline_metric_types_by_id = {}
for key in list(skyline_metric_types.keys()):
    skyline_metric_types_by_id[skyline_metric_types[key]] = key

# @added 20230404 - Feature #4888: analyzer - load_shedding
metrics_last_analysis_hash_key = 'analyzer_labelled_metrics.metrics.last_analysis_timestamp'


class AnalyzerLabelledMetrics(Thread):
    """
    The AnalyzerLabelledMetrics class which controls the analyzer_labelled_metrics
    thread and spawned processes.
    """

    def __init__(self, parent_pid):
        """
        Initialize the AnalyzerLabelledMetrics

        Create the :obj:`self.exceptions_q` queue
        Create the :obj:`self.anomaly_breakdown_q` queue

        """
        # super(AnalyzerLabelledMetrics, self).__init__()
        super().__init__()
        self.redis_conn = get_redis_conn(skyline_app)
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        self.exceptions_q = Queue()
        self.anomaly_breakdown_q = Queue()
        self.stats_q = Queue()
        self.new_metrics_q = Queue()

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            logger.warning('warning :: parent or current process dead')
            sys_exit(0)

    def spawn_alerter_process(self, alert, metric, context):
        """
        Spawn a process to trigger an alert.

        This is used by smtp alerters so that matplotlib objects are cleared
        down and the alerter cannot create a memory leak in this manner and
        plt.savefig keeps the object in memory until the process terminates.
        Seeing as data is being surfaced and processed in the alert_smtp
        context, multiprocessing the alert creation and handling prevents any
        memory leaks in the parent.

        Added 20160814 relating to:

        * Bug #1558: Memory leak in Analyzer
        * Issue #21 Memory leak in Analyzer see https://github.com/earthgecko/skyline/issues/21

        Parameters as per :py:func:`skyline.analyzer.alerters.trigger_alert
        <analyzer.alerters.trigger_alert>`

        """

        trigger_alert(alert, metric, context)

    # Although this could be imported by functions.prometheus.metric_name_labels_parser
    # memray highlights that the import method generates 1000s of allocations
    # whereas defined locally incurs none.
    def metric_name_labels_parser(self, metric):
        """
        Given a Prometheus metric string return a dict of the metric name and labels.
        :param metric: the prometheus metric
        :type metric: str
        :return: metric_dict
        :rtype: dict
        """
        metric_dict = {}
        metric_name = None
        try:
            metric_elements = metric.split('{', 1)
            metric_name = metric_elements[0]
            metric_labels_str = metric_elements[1]
            metric_labels = metric_labels_str.rstrip('}')
        except Exception as err:
            logger.error('error :: labelled_metrics_spin_process :: failed to parse metric %s - %s' % (
                str(metric), str(err)))
            return metric_dict
        labels = {}
        try:
            labels = parse_labels(metric_labels)
        except Exception as err:
            logger.error('error :: labelled_metrics_spin_process :: failed to parse labels %s - %s' % (
                str(metric), str(err)))
            return metric_dict
        metric_dict['metric'] = metric_name
        metric_dict['labels'] = labels
        return metric_dict

    # @modified 20230404 - Feature #4890: analyzer_labelled_metrics - use mrange
    # Added filters
    def labelled_metrics_spin_process(self, i_process, assigned_metrics_dict, filters=None):
        """
        Assign a bunch of metrics for a process to analyze.

        Multiple get the assigned_metrics to the process from Redis.

        For each metric:

        - unpack the `raw_timeseries` for the metric.
        - Analyse each timeseries against `ALGORITHMS` to determine if it is
          anomalous.
        - If anomalous add it to the Redis set analyzer.anomalous_metrics
        - Add what algorithms triggered to the :obj:`self.anomaly_breakdown_q`
          queue
        - If :mod:`settings.ENABLE_CRUCIBLE` is ``True``:

          - Add a crucible data file with the details about the timeseries and
            anomaly.
          - Write the timeseries to a json file for crucible.

        Add keys and values to the queue so the parent process can collate for:\n
        * :py:obj:`self.anomaly_breakdown_q`
        * :py:obj:`self.exceptions_q`
        """

        memray_file = '%s/analyzer_labelled_metrics.labelled_metrics_spin_process.%s.bin' % (settings.SKYLINE_TMP_DIR, str(i_process))
        memray_file_last = '%s.last' % memray_file
        if os.path.isfile(memray_file_last):
            os.remove(memray_file_last)
            logger.info('labelled_metrics_spin_process :: removed %s' % str(memray_file_last))
        if os.path.isfile(memray_file):
            os.rename(memray_file, memray_file_last)
            logger.info('labelled_metrics_spin_process :: removed %s' % str(memray_file_last))

        try:
            with memray.Tracker(memray_file):
                def median_absolute_deviation(timeseries):
                    try:
                        series = pd.Series([x[1] for x in timeseries])
                        median = series.median()
                        demedianed = np.abs(series - median)
                        median_deviation = demedianed.median()
                    except:
                        return None
                    if median_deviation == 0:
                        return False
                    try:
                        test_statistic = demedianed.iat[-1] / median_deviation
                    except:
                        return None
                    if test_statistic > 6:
                        return True
                    return False

                try:
                    spin_start = time()
                    logger.info('labelled_metrics_spin_process :: started process %s' % str(i_process))
                except Exception as err:
                    logger.error('error :: labelled_metrics_spin_process :: getting spin_start time failed - %s' % (
                        err))
                    spin_start = time()

                until_timestamp = int(spin_start)
                from_timestamp = until_timestamp - settings.FULL_DURATION
                current_aligned_ts = int(until_timestamp // 60 * 60)
                metric_airgaps = []
                metric_airgaps_filled = []
                run_negatives_present = False
                check_for_airgaps_only = False
                custom_stale_metrics_dict = {}

                # @added 20230401 - Feature #4886: analyzer - operation_timings
                # Only sadd once
                identified_boring_metrics = []
                identified_stale_metrics = []
                identified_tooshort_metrics = []

                last_reported_up = 0
                try:
                    last_reported_up = self.redis_conn_decoded.get(skyline_app)
                    if not last_reported_up:
                        last_reported_up = 0
                    else:
                        last_reported_up = int(float(last_reported_up))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: labelled_metrics_spin_process :: Analyzer could not update the Redis %s key - %s' % (
                        err))

                # Check the unique_labelled_metrics list is valid
                assigned_metrics = list(assigned_metrics_dict.keys())

                # @added 20230404 - Feature #4890: analyzer_labelled_metrics - use mrange
                # Added filters
                filters_str = None
                if filters:
                    filters_str = '_tenant_id=(' + ','.join(filters) + ')'

                # @added 20220504 - Feature #2580: illuminance
                illuminance_dict = {}
                # @added 20220919 - Feature #4676: analyzer - illuminance.all key
                illuminance_all_dict = {}
                algorithms = {}
                try:
                    algorithms = self.redis_conn_decoded.hgetall('metrics_manager.algorithms.ids')
                except Exception as err:
                    logger.error('error :: labelled_metrics_spin_process :: hgetall metrics_manager.algorithms.ids - %s' % str(err))

                # @added 20210513 - Feature #4068: ANALYZER_SKIP
                analyzer_skip_metrics_skipped = 0
                analyzer_skip_metrics = []
                if ANALYZER_SKIP:
                    logger.info('labelled_metrics_spin_process :: determining ANALYZER_SKIP metrics from analyzer.metrics_manager.analyzer_skip Redis set')
                    try:
                        analyzer_skip_metrics = list(self.redis_conn_decoded.smembers('analyzer.metrics_manager.analyzer_skip'))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to generate a list from analyzer.metrics_manager.analyzer_skip Redis set - %s' % err)
                        analyzer_skip_metrics = []
                    if analyzer_skip_metrics:
                        logger.info('labelled_metrics_spin_process :: removing %s ANALYZER_SKIP metrics from the %s assigned_metrics' % (
                            str(len(analyzer_skip_metrics)), str(assigned_metrics)))
                        unique_labelled_metrics = list(set(unique_labelled_metrics) - set(analyzer_skip_metrics))
                        analyzer_skip_metrics_skipped = len(set(analyzer_skip_metrics))
                    else:
                        logger.info('labelled_metrics_spin_process :: did not determine any ANALYZER_SKIP metrics from from analyzer.metrics_manager.analyzer_skip Redis set, will check dynamically')

                # @added 20190410 - Feature #2916: ANALYZER_ENABLED setting
                if not ANALYZER_ENABLED:
                    # len_assigned_metrics = len(assigned_metrics)
                    logger.info('labelled_metrics_spin_process :: ANALYZER_ENABLED is set to %s removing the %s assigned_metrics' % (
                        str(ANALYZER_ENABLED), str(len(assigned_metrics))))
                    assigned_metrics = []
                    del unique_labelled_metrics

                # Check if this process is unnecessary
                if len(assigned_metrics) == 0:
                    logger.info('labelled_metrics_spin_process :: 0 assigned metrics, nothing to do')
                    return

                run_selected_algorithm_count = 0

                # Make process-specific dicts
                exceptions = defaultdict(int)
                anomaly_breakdown = defaultdict(int)

                # Surface the last_timeseries_timestamp Redis hash key so that the
                # timestamps can be compared as Thunder stale_metrics requires all
                # timestamps for all metrics
                metrics_last_timeseries_timestamp_update_dict = {}
                metrics_last_timeseries_timestamp_hash_key = 'analyzer_labelled_metrics.last_timeseries_timestamp'

                metrics_stationary_update_dict = {}
                metrics_stationary_hash_key = 'analyzer_labelled_metrics.stationary_metrics'

                logger.info('labelled_metrics_spin_process :: checking %s assigned_metrics' % str(len(assigned_metrics)))

                metric_type_redis_keys = {}
                tenant_ids = []

                metrics_checked = 0
                metrics_analysed = 0
                metrics_skipped = 0
                mad_analysed = 0
                mad_anomalies = 0
                anomalies = 0
                no_anomalies = 0
                stationary = 0
                not_stationary = 0
                stationary_not_expired = 0

                redis_timings = []
                redis_full_duration_timings = []
                stationary_timings = []
                mad_timings = []
                downsample_timings = []
                threesigma_timings = []

                last_analysed_timeseries = []
                last_analysed_metric = None
                mad_anomalous_dict = {}
                threesigma_anomalous_dict = {}
                stale_metrics = {}
                empty_metrics = []
                no_new_data_metrics = {}
                tooshort_metrics = {}

                metrics_dict = {}

                not_anomalous_list = []
                empty_timeseries_list = []
                boring_dict = {}

                used_metric_id_keys = 0

                metric_types_known = {
                    'COUNTER': {'known': 0, 'looked_up': 0},
                    'GAUGE': {'known': 0, 'looked_up': 0},
                    'SUMMARY': {'known': 0, 'looked_up': 0},
                    'HISTOGRAM': {'known': 0, 'looked_up': 0},
                    'UNKNOWN': {'known': 0, 'looked_up': 0},
                }

                metric_subtypes_added = {
                    'COUNTER': {'COUNTER': 0, 'GAUGE': 0},
                    'GAUGE': {'COUNTER': 0, 'GAUGE': 0},
                    'SUMMARY': {'COUNTER': 0, 'GAUGE': 0},
                    'HISTOGRAM': {'COUNTER': 0, 'GAUGE': 0},
                    'UNKNOWN': {'COUNTER': 0, 'GAUGE': 0},
                }
                metric_subtypes_queried = {
                    'COUNTER': {'COUNTER': 0, 'GAUGE': 0},
                    'GAUGE': {'COUNTER': 0, 'GAUGE': 0},
                    'SUMMARY': {'COUNTER': 0, 'GAUGE': 0},
                    'HISTOGRAM': {'COUNTER': 0, 'GAUGE': 0},
                    'UNKNOWN': {'COUNTER': 0, 'GAUGE': 0},
                }

                monotonicity_not_expired = 0
                monotonicity_checked = 0
                monotonicity_check_per_minute = int(len(assigned_metrics) / (60 * 4))
                monotonicity_changed = 0

                metrics_type_dict = {}

                # @added 20230309 - Feature #4864: analyzer_labelled_metrics - exceptions metrics set
                redis_set_errors = []

                # @added 20221229 - Bug #4766: analyzer_labelled_metrics - monotonicity checked incorrect classification
                # Fetch the data once because with 1000s of new metrics added it
                # becomes more efficient
                skyline_labelled_metrics_id_types = {}
                try:
                    skyline_labelled_metrics_id_types = self.redis_conn_decoded.hgetall('skyline.labelled_metrics.id.type')
                except Exception as err:
                    logger.error('error :: labelled_metrics_spin_process :: failed to hgetall skyline.labelled_metrics.id.type - %s' % err)

                # @added 20230518 - metric_type.longterm_expire
                skyline_labelled_metrics_id_types_longterm_expire = {}
                try:
                    skyline_labelled_metrics_id_types_longterm_expire = self.redis_conn_decoded.hgetall('skyline.labelled_metrics.id.type.longterm_expire')
                except Exception as err:
                    logger.error('error :: labelled_metrics_spin_process :: failed to hgetall skyline.labelled_metrics.id.type.longterm_expire - %s' % err)

                # @added 20230329 - Feature #4882: labelled_metrics - resolution and data sparsity
                #                   Feature #3870: metrics_manager - check_data_sparsity
                resolution_and_sparsity_timings = []
                labelled_metrics_resolutions = {}
                labelled_metrics_sparsity = {}
                labelled_metrics_resolution_sparsity_last_checked_dict = {}
                labelled_metrics_resolution_sparsity_checked_dict = {}
                labelled_metrics_resolution_sparsity_recently_checked = 0

                labelled_metrics_resolution_sparsity_last_checked_hash_key = 'labelled_metrics.resolution_sparsity_last_checked'
                try:
                    labelled_metrics_resolution_sparsity_last_checked_dict = self.redis_conn_decoded.hgetall(labelled_metrics_resolution_sparsity_last_checked_hash_key)
                except Exception as err:
                    logger.error('error :: labelled_metrics_spin_process :: failed to hgetall %s - %s' % (
                        labelled_metrics_resolution_sparsity_last_checked_hash_key, err))
                    labelled_metrics_resolution_sparsity_last_checked_dict = {}

                # @added 20230404 - Feature #4888: analyzer - load_shedding
                analyzer_last_run_time = 0
                analyzer_last_run_time_timestamp = int(spin_start)
                load_shedding_active = False
                if LOAD_SHEDDING_ENABLED:
                    logger.info('labelled_metrics_spin_process :: load_shedding_active - checking')
                    try:
                        analyzer_last_run_time = float(self.redis_conn_decoded.hget('analyzer_labelled_metrics.run_time', 'value'))
                        analyzer_last_run_time_timestamp = int(self.redis_conn_decoded.hget('analyzer_labelled_metrics.run_time', 'timestamp'))
                    except Exception as err:
                        logger.error('error :: labelled_metrics_spin_process :: load_shedding_active failed to get keys from analyzer.run_time Redis hash - %s' % err)
                        analyzer_last_run_time = 0
                        analyzer_last_run_time_timestamp = int(spin_start)
                    if analyzer_last_run_time >= (settings.MAX_ANALYZER_PROCESS_RUNTIME - 5):
                        load_shedding_active = True
                        logger.info('labelled_metrics_spin_process :: load_shedding_active set to True because analyzer_last_run_time: %s' % str(analyzer_last_run_time))
                    if analyzer_last_run_time_timestamp <= (int(spin_start) - (settings.MAX_ANALYZER_PROCESS_RUNTIME + 30)):
                        load_shedding_active = True
                        logger.info('labelled_metrics_spin_process :: load_shedding_active set to True because analyzer_last_run_time_timestamp is older than % seconds' % (
                            str(analyzer_last_run_time_timestamp)))
                logger.info('labelled_metrics_spin_process :: load_shedding_active: %s' % str(load_shedding_active))
                last_analysed_ordered_metrics = []
                load_shedding_assigned_metrics = []
                metrics_last_analysis_dict = {}
                # If the load_shedding hash exist keep updating it until it expires and
                # does not exist.  If it exists but load_shedding_active is not True the
                # hash expiry does not get updated it only gets updated if
                # load_shedding_active is True.  This is so that if load_shedding becomes
                # active on 1 run, but is not active on the next run the hash last
                # analysis times are still updated for another 4 minutes so that if
                # load_shedding becomes active in the next 4 minutes, it is not starting
                # from scratch and will have data to do an ordered analysis on.
                load_shedding_hash_exists = False
                last_metrics_last_analysis_dict = {}
                try:
                    load_shedding_hash_exists = self.redis_conn_decoded.exists(metrics_last_analysis_hash_key)
                except Exception as err:
                    logger.error('error :: load_shedding_active failed to hgetall %s from Redis hash - %s' % (metrics_last_analysis_hash_key, err))
                if load_shedding_active or load_shedding_hash_exists:
                    start_load_shedding_get = time()
                    log_str = 'load shedding hash exists'
                    if load_shedding_active:
                        log_str = 'load_shedding_active'
                    try:
                        last_metrics_last_analysis_dict = self.redis_conn_decoded.hgetall(metrics_last_analysis_hash_key)
                        logger.info('labelled_metrics_spin_process :: %s got %s metrics last analysed timestamps from %s to reorder %s assigned_metrics' % (
                            log_str, str(len(last_metrics_last_analysis_dict)), metrics_last_analysis_hash_key,
                            str(len(assigned_metrics))))
                    except Exception as err:
                        logger.error('error :: labelled_metrics_spin_process :: load_shedding_active failed to hgetall %s from Redis hash - %s' % (metrics_last_analysis_hash_key, err))
                if load_shedding_active:
                    start_load_shedding_get = time()
                    assigned_labelled_metrics = []
                    assigned_labelled_metrics_dict = {}
                    for base_name in assigned_metrics:
                        metric_id = assigned_metrics_dict[base_name]['id']
                        labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
                        assigned_labelled_metrics.append(labelled_metric_name)
                        assigned_labelled_metrics_dict[labelled_metric_name] = base_name
                    if metrics_last_analysis_dict:
                        metrics_last_analyzed_timestamp = [[int(timestamp_str), metric] for metric, timestamp_str in metrics_last_analysis_dict.items()]
                        metrics_last_analyzed_timestamp_sorted = sorted(metrics_last_analyzed_timestamp, key=lambda x: x[0])
                        del metrics_last_analyzed_timestamp
                        del metrics_last_analysis_dict
                        metrics_last_analysis_dict = {}
                        metrics_last_analyzed_timestamp_sorted = [item for item in metrics_last_analyzed_timestamp_sorted if item[1] in assigned_labelled_metrics]
                        logger.info('labelled_metrics_spin_process :: load_shedding_active reordered metrics by last analysed timestamp, first metric in list now: %s' % (
                            str(metrics_last_analyzed_timestamp_sorted[0])))
                        logger.info('labelled_metrics_spin_process :: load_shedding_active reordered metrics by last analysed timestamp, last metric in list now: %s' % (
                            str(metrics_last_analyzed_timestamp_sorted[-1])))
                        last_analysed_ordered_metrics = [labelled_metric for ts, labelled_metric in metrics_last_analyzed_timestamp_sorted]
                        del metrics_last_analyzed_timestamp_sorted
                    if last_analysed_ordered_metrics:
                        load_shedding_assigned_metrics = [labelled_metric for labelled_metric in last_analysed_ordered_metrics if labelled_metric in assigned_labelled_metrics]
                    logger.info('labelled_metrics_spin_process :: load_shedding_last_analysed_ordered_metrics took %s seconds' % str((time() - start_load_shedding_get)))
                if load_shedding_assigned_metrics:
                    logger.info('labelled_metrics_spin_process :: load_shedding_active replacing %s assigned_metrics with %s metrics ordered by last analysed timestamp' % (
                        str(len(assigned_labelled_metrics)), str(len(load_shedding_assigned_metrics))))
                    try:
                        set_assigned_metrics = set(assigned_labelled_metrics)
                        set_load_shedding_assigned_metrics = set(load_shedding_assigned_metrics)
                        not_present_metrics_set = set_assigned_metrics.difference(set_load_shedding_assigned_metrics)
                        not_present_metrics = list(not_present_metrics_set)
                        del set_assigned_metrics
                        del set_load_shedding_assigned_metrics
                        del not_present_metrics_set
                    except Exception as err:
                        logger.error('error :: labelled_metrics_spin_process :: load_shedding_active failed determine not_present_metrics - %s' % err)
                        not_present_metrics = []
                    if not_present_metrics:
                        logger.info('labelled_metrics_spin_process :: load_shedding_active appending %s metrics which had no last analysed timestamp' % (
                            str(len(not_present_metrics))))
                        load_shedding_assigned_metrics = load_shedding_assigned_metrics + not_present_metrics
                        del not_present_metrics
                    assigned_metrics = []
                    for labelled_metric in load_shedding_assigned_metrics:
                        try:
                            assigned_metrics.append(assigned_labelled_metrics_dict[labelled_metric])
                        except:
                            errors.append([labelled_metric, 'load_shedding assigned_metrics', str(err)])
                    del load_shedding_assigned_metrics
                    logger.info('labelled_metrics_spin_process :: load_shedding_active now %s assigned_metrics' % (
                        str(len(assigned_metrics))))

                # @added 20230404 - Feature #4890: analyzer_labelled_metrics - use mrange
                # Added filters
                all_timeseries = []
                labelled_metric_in_all_timeseries = []
                labelled_metric_and_indices_in_all_timeseries = {}
                timeseries_not_present_in_all_timeseries = {}
                if filters_str:
                    filters = [filters_str]
                    logger.info('labelled_metrics_spin_process :: calling mrange with filters: %s' % str(filters))
                    r_start = time()
                    try:
                        all_timeseries = self.redis_conn_decoded.ts().mrange(((until_timestamp - (180 * 60)) * 1000), (until_timestamp * 1000), filters)
                    except Exception as err:
                        logger.error('error :: labelled_metrics_spin_process :: failed to get Redis timeseries with mrange - %s' % (
                            err))
                        errors.append(['all_timeseries', 'ts().mrange', str(err)])
                        all_timeseries = []
                    redis_timings.append(time() - r_start)
                    if all_timeseries:
                        logger.info('labelled_metrics_spin_process :: mrange returned %s timeseries' % str(len(all_timeseries)))
                        labelled_metric_in_all_timeseries = [list(tlist.keys())[0] for tlist in all_timeseries]
                        for index, labelled_metric in enumerate(labelled_metric_in_all_timeseries):
                            labelled_metric_and_indices_in_all_timeseries[labelled_metric] = index

                # @added 20230404 - Feature #4888: analyzer - load_shedding
                load_shedding_active_log_stop = False
                activating_load_shedding = False

                # @added 20230425 - Feature #4894: labelled_metrics - SKYLINE_FEEDBACK_NAMESPACES
                feedback_metrics_skipped = []
                feedback_labelled_metric_ids = []
                analyzer_labelled_metrics_busy = False
                if settings.SKYLINE_FEEDBACK_NAMESPACES:
                    try:
                        analyzer_labelled_metrics_busy = self.redis_conn_decoded.get('analyzer_labelled_metrics.busy')
                    except Exception as err:
                        logger.error('error :: labelled_metrics_spin_process :: failed to get analyzer_labelled_metrics.busy Redis key' % (
                            err))
                if analyzer_labelled_metrics_busy:
                    logger.info('labelled_metrics_spin_process :: analyzer_labelled_metrics_busy found')
                    try:
                        feedback_labelled_metric_ids = list(self.redis_conn_decoded.smembers('aet.metrics_manager.feedback.labelled_metric_ids'))
                    except Exception as err:
                        logger.error('error :: labelled_metrics_spin_process :: smembers failed on Redis set aet.metrics_manager.feedback.labelled_metric_ids - %s' % (
                            err))

                # @added 20230426 - Feature #4724: custom_algorithms - anomalous_daily_peak
                # Added expiry to record metrics identified as normal by anomalous_daily_peaks
                normal_daily_peak_metrics_skipped = []
                current_now = int(time())
                normal_daily_peaks_keys = []
                for i in sorted(list(range(1, 3)), reverse=True):
                    key_ts = current_aligned_ts - (60 * i)
                    key = 'mirage.normal_daily_peak_metrics.%s' % str(key_ts)
                    normal_daily_peaks_keys.append(key)
                normal_daily_peak_metrics_expiry = {}
                for normal_daily_peaks_key in normal_daily_peaks_keys:
                    try:
                        current_key_data  = self.redis_conn_decoded.hgetall(normal_daily_peaks_key)
                    except Exception as err:
                        logger.error('error :: labelled_metrics_spin_process :: failed to hgetall Redis hash %s - %s' % (
                            str(normal_daily_peaks_key), err))
                    for labelled_metric in list(current_key_data):
                        expire_at = 0
                        try:
                            expire_at = int(float(current_key_data[labelled_metric]))
                        except:
                            expire_at = 0
                        if expire_at:
                            if current_now > expire_at:
                                if labelled_metric in labelled_metric_in_all_timeseries:
                                    try:
                                        self.redis_conn_decoded.hdel(normal_daily_peaks_key, labelled_metric)
                                    except Exception as err:
                                        logger.error('error :: mirage_labelled_metrics :: failed to remove %s from Redis hash %s - %s' % (
                                            labelled_metric, normal_daily_peaks_key, err))
                            else:
                                normal_daily_peak_metrics_expiry[labelled_metric] = expire_at
                logger.info('labelled_metrics_spin_process :: surfaced %s metrics currently in anomalous_daily_peak expiry' % str(len(normal_daily_peak_metrics_expiry)))

                errors = []
                for base_name in assigned_metrics:
                    metrics_dict[base_name] = {}
                    self.check_if_parent_is_alive()

                    # @added 20230404 - Feature #4888: analyzer - load_shedding
                    # If load shedding is active and the process in approaching the
                    # MAX_ANALYZER_PROCESS_RUNTIME stop
                    right_now = int(time())
                    if not activating_load_shedding:
                        if right_now >= (int(spin_start) + (settings.MAX_ANALYZER_PROCESS_RUNTIME - 5)):
                            metrics_done = len(assigned_metrics) - len(metrics_dict)
                            metrics_not_done = len(assigned_metrics) - metrics_done
                            activating_load_shedding = True
                            logger.info('labelled_metrics_spin_process :: activating load shedding - approaching MAX_ANALYZER_PROCESS_RUNTIME so stopping, analysed %s metrics of the %s assigned metrics, %s metric not analysed' % (
                                str(metrics_done), str(len(assigned_metrics)),
                                str(metrics_not_done)))
                            if not load_shedding_active_log_stop and load_shedding_active:
                                logger.info('labelled_metrics_spin_process :: load_shedding_active - approaching MAX_ANALYZER_PROCESS_RUNTIME so stopping, analysed %s metrics of the %s assigned metrics, %s metric not analysed' % (
                                    str(metrics_done), str(len(assigned_metrics)),
                                    str(metrics_not_done)))
                                load_shedding_active_log_stop = True
                    if activating_load_shedding:
                        metric_id = 0
                        try:
                            metric_id = assigned_metrics_dict[base_name]['id']
                        except Exception as err:
                            logger.error('error :: labelled_metrics_spin_process :: failed to get assigned_metrics_dict for %s  - %s' % (
                                str(base_name), err))
                            errors.append([base_name, 'assigned_metrics_dict', str(err)])
                        labelled_metric = 'labelled_metrics.%s' % str(metric_id)
                        if last_metrics_last_analysis_dict:
                            try:
                                last_anlysis_timestamp_str = last_metrics_last_analysis_dict[labelled_metric]
                                if last_anlysis_timestamp_str:
                                    metrics_last_analysis_dict[labelled_metric] = int(last_anlysis_timestamp_str)
                            except:
                                metrics_last_analysis_dict[labelled_metric] = int(right_now) - 60
                        else:
                            metrics_last_analysis_dict[labelled_metric] = int(right_now) - 60
                        continue

                    metrics_checked += 1
                    if not metrics_checked % 1000:
                        stats = {
                            'checked': metrics_checked,
                            'analysed': metrics_analysed,
                            'skipped': metrics_skipped,
                            'mad anomalous': mad_anomalies,
                            'anomalous': anomalies,
                            'not anomalous': no_anomalies,
                            'stationary': stationary,
                            'not stationary': not_stationary,
                            'monotonicity_checked': monotonicity_checked,
                            'monotonicity_changed': monotonicity_changed,
                            # @added 20230426 - Feature #4724: custom_algorithms - anomalous_daily_peak
                            # Added expiry to record metrics identified as normal by anomalous_daily_peaks
                            'normal_daily_peak_metrics_skipped': len(normal_daily_peak_metrics_skipped),
                            # @added 20230425 - Feature #4894: labelled_metrics - SKYLINE_FEEDBACK_NAMESPACES
                            'feedback_metrics_skipped': len(feedback_metrics_skipped),
                        }
                        logger.info('labelled_metrics_spin_process :: progress: %s' % str(stats))

                    # @added 20210513 - Feature #4068: ANALYZER_SKIP
                    if ANALYZER_SKIP and not analyzer_skip_metrics:
                        pattern_match, metric_matched_by = matched_or_regexed_in_list('analyzer', base_name, ANALYZER_SKIP)
                        del metric_matched_by
                        if pattern_match:
                            analyzer_skip_metrics_skipped += 1
                            metrics_skipped += 1
                            # @added 20230404 - Feature #4888: analyzer - load_shedding
                            metric_id = assigned_metrics_dict[base_name]['id']
                            labelled_metric = 'labelled_metrics.%s' % str(metric_id)
                            metrics_last_analysis_dict[labelled_metric] = int(right_now)

                            continue

                    # @added 20210520 - Branch #1444: thunder
                    # Added to supplement the ran Report app up if analyzer is just running
                    # long and over running.
                    update_analyzer_up_key = False
                    right_now = int(time())
                    if not last_reported_up:
                        update_analyzer_up_key = True
                    else:
                        try:
                            if right_now > (last_reported_up + 60):
                                update_analyzer_up_key = True
                        except Exception as err:
                            logger.error('error :: labelled_metrics_spin_process :: could not determine if last_reported_up time is exceeded - %s' % (
                                err))
                    if update_analyzer_up_key:
                        # Report app up
                        try:
                            self.redis_conn.setex(skyline_app_thunder_key, 120, right_now)
                            last_reported_up = right_now
                        except Exception as err:
                            logger.error('error :: labelled_metrics_spin_process :: could not update the Redis %s key - %s' % (
                                err))

                    # labelled_metric = 'labelled_metrics.%s' % str(base_name)

                    # Use the metrics_dict data to optimise when data should be
                    # retrieved and analysed
                    last_ts = 0
                    try:
                        metric_id = assigned_metrics_dict[base_name]['id']
                        last_ts = int(float(assigned_metrics_dict[base_name]['last_ts']))
                        stationary_status = assigned_metrics_dict[base_name]['stationary']
                    except Exception as err:
                        logger.error('error :: labelled_metrics_spin_process :: failed to get assigned_metrics_dict for %s  - %s' % (
                            str(base_name), err))
                        errors.append([base_name, 'assigned_metrics_dict', str(err)])
                        continue

                    labelled_metric = 'labelled_metrics.%s' % str(metric_id)

                    if metrics_checked == 1:
                        logger.debug('debug :: labelled_metrics_spin_process :: %s, metrics_dict: %s' % (
                            str(base_name), str(assigned_metrics_dict[base_name])))

                    labelled_metric_by_id = 'labelled_metrics.%s' % str(metric_id)
                    redis_ts_key = labelled_metric_by_id

                    check_id_key = False
                    if check_id_key:
                        try:
                            metrics_id_ts_keys_exists = self.redis_conn_decoded.exists(labelled_metric_by_id)
                            if metrics_id_ts_keys_exists:
                                redis_ts_key = labelled_metric_by_id
                                used_metric_id_keys += 1
                        except:
                            pass

                    # @added 20230426 - Feature #4724: custom_algorithms - anomalous_daily_peak
                    # Added expiry to record metrics identified as normal by anomalous_daily_peaks
                    normal_daily_peak_expiry = 0
                    try:
                        normal_daily_peak_expiry = normal_daily_peak_metrics_expiry[labelled_metric]
                    except:
                        normal_daily_peak_expiry = 0

                    # @added 20230425 - Feature #4894: labelled_metrics - SKYLINE_FEEDBACK_NAMESPACES
                    feedback_metric = False
                    if analyzer_labelled_metrics_busy:
                        if str(metric_id) in feedback_labelled_metric_ids:
                            feedback_metric = True
                    feedback_metric_expiry = 0
                    if feedback_metric:
                        feedback_key = 'mirage_labelled_metrics.feedback.expiry.%s' % str(metric_id)
                        try:
                            feedback_metric_expiry = self.redis_conn_decoded.get(feedback_key)
                        except Exception as err:
                            errors.append([labelled_metric, feedback_key, str(err)])
                            feedback_metric_expiry = 0

                    timeseries = []

                    # @added 20230404 - Feature #4890: analyzer_labelled_metrics - use mrange
                    # Added filters and all_timeseries
                    timeseries_not_present = False
                    if all_timeseries:
                        try:
                            all_timeseries_index = labelled_metric_and_indices_in_all_timeseries[labelled_metric]
                            timeseries = all_timeseries[all_timeseries_index][labelled_metric][1]
                        except KeyError:
                            logger.info('labelled_metrics_spin_process :: not present in all_timeseries - %s - %s' % (
                                str(labelled_metric), base_name))
                            timeseries_not_present_in_all_timeseries[labelled_metric] = base_name
                            timeseries_not_present = True
                        except Exception as err:
                            logger.error('error :: labelled_metrics_spin_process :: failed to get timeseries from all_timeseries for %s - %s' % (
                                str(labelled_metric), err))
                            errors.append([labelled_metric, 'all_timeseries', str(err)])
                            timeseries = []

                    # @modified 20230404 - Feature #4890: analyzer_labelled_metrics - use mrange
                    # Added filters and all_timeseries and only query Redis if
                    # the timeseries is not retrieved from all_timeseries
                    if not timeseries and not timeseries_not_present:
                        r_start = time()
                        try:
                            # timeseries = self.redis_conn_decoded.ts().range(labelled_metric, (from_timestamp * 1000), (until_timestamp * 1000))
                            timeseries = self.redis_conn_decoded.ts().range(redis_ts_key, ((until_timestamp - (180 * 60)) * 1000), (until_timestamp * 1000))
                        except Exception as err:
                            if str(err) == 'TSDB: the key does not exist':
                                exceptions['TimeseriesDoesNotExist'] += 1
                                redis_timings.append(time() - r_start)
                                empty_timeseries_list.append(labelled_metric)
                                # @added 20230404 - Feature #4888: analyzer - load_shedding
                                metrics_last_analysis_dict[labelled_metric] = int(right_now)

                                continue
                            logger.error('error :: labelled_metrics_spin_process :: failed to get Redis timeseries for %s - %s' % (
                                str(labelled_metric), err))
                            errors.append([labelled_metric, 'ts().range', str(err)])
                            timeseries = []
                        redis_timings.append(time() - r_start)

                    if not timeseries:
                        metrics_skipped += 1
                        exceptions['EmptyTimeseries'] += 1
                        empty_metrics.append(base_name)
                        empty_timeseries_list.append(labelled_metric)
                        # @added 20230404 - Feature #4888: analyzer - load_shedding
                        metrics_last_analysis_dict[labelled_metric] = int(right_now)
                        continue

                    # Convert Redis millisecond timestamps to second timestamps
                    timeseries = [[int(mts / 1000), value] for mts, value in timeseries]

                    last_timeseries_timestamp = 0
                    try:
                        last_timeseries_timestamp = int(timeseries[-1][0])
                    except:
                        metrics_skipped += 1
                        exceptions['EmptyTimeseries'] += 1
                        empty_metrics.append(base_name)
                        empty_timeseries_list.append(labelled_metric)
                        # @added 20230404 - Feature #4888: analyzer - load_shedding
                        metrics_last_analysis_dict[labelled_metric] = int(right_now)
                        continue

                    if (until_timestamp - last_timeseries_timestamp) >= settings.STALE_PERIOD:
                        metrics_skipped += 1
                        exceptions['Stale'] += 1
                        stale_metrics[metric_id] = str(last_timeseries_timestamp)
                        # @added 20230309 - Feature #4864: analyzer_labelled_metrics - exceptions metrics set
                        # @modified 20230401 - Feature #4886: analyzer - operation_timings
                        # Only sadd once
                        identified_stale_metrics.append(str(metric_id))
                        # try:
                        #     self.redis_conn_decoded.sadd('analyzer_labelled_metrics.stale', str(metric_id))
                        # except Exception as err:
                        #    redis_set_errors.append(['analyzer_labelled_metrics.stale', str(err)])
                        # @added 20230404 - Feature #4888: analyzer - load_shedding
                        metrics_last_analysis_dict[labelled_metric] = int(right_now)

                        continue
                    # TODO: How to not classify as short if the metric stops and starts
                    # again.  In analyzer with FULL_DURATION data this is not so much of
                    # an issue, but with 3 hours of data...
                    # if len(timeseries) < 180:
                    # if len(timeseries) < 60:
                    if len(timeseries) < settings.MIN_TOLERABLE_LENGTH:
                        metrics_skipped += 1
                        exceptions['TooShort'] += 1
                        tooshort_metrics[base_name] = len(timeseries)
                        # @added 20230309 - Feature #4864: analyzer_labelled_metrics - exceptions metrics set
                        # @modified 20230401 - Feature #4886: analyzer - operation_timings
                        # Only sadd once
                        identified_tooshort_metrics.append(str(metric_id))
                        # try:
                        #     self.redis_conn_decoded.sadd('analyzer_labelled_metrics.tooshort', str(metric_id))
                        # except Exception as err:
                        #     redis_set_errors.append(['analyzer_labelled_metrics.tooshort', str(err)])
                        # @added 20230404 - Feature #4888: analyzer - load_shedding
                        metrics_last_analysis_dict[labelled_metric] = int(right_now)
                        continue

                    # Maintain a Redis hash key of the last timestamp of all metrics and
                    # only update the hash key if the timestamp is recent.  This Redis
                    # hash key is pruned in metrics_manager when an entry has a
                    # timestamp older that now - FULL_DURATION
                    # Thunder stale_metrics check requires all metrics to have their
                    # timestamps recorded in the hash key
                    update_last_timestamp_hash_key = False
                    if last_ts:
                        if last_ts < last_timeseries_timestamp:
                            update_last_timestamp_hash_key = True
                    else:
                        update_last_timestamp_hash_key = True

                    if update_last_timestamp_hash_key:
                        metrics_last_timeseries_timestamp_update_dict[metric_id] = last_timeseries_timestamp
                    else:
                        # No new data since last analysis
                        metrics_skipped += 1
                        exceptions['NoNewData'] += 1
                        no_new_data_metrics[metric_id] = last_timeseries_timestamp
                        # @added 20230404 - Feature #4888: analyzer - load_shedding
                        metrics_last_analysis_dict[labelled_metric] = int(right_now)
                        continue

                    # Get rid of boring series
                    boring = False
                    if len(set(item[1] for item in timeseries[-settings.MAX_TOLERABLE_BOREDOM:])) == settings.BOREDOM_SET_SIZE:
                        # metrics_skipped += 1
                        exceptions['Boring'] += 1
                        boring = True
                        # Class boring as analysed
                        # continue
                        # @added 20230309 - Feature #4864: analyzer_labelled_metrics - exceptions metrics set
                        # @modified 20230401 - Feature #4886: analyzer - operation_timings
                        # Only sadd once
                        identified_boring_metrics.append(str(metric_id))
                        # try:
                        #     self.redis_conn_decoded.sadd('analyzer_labelled_metrics.boring', str(metric_id))
                        # except Exception as err:
                        #     redis_set_errors.append(['analyzer_labelled_metrics.boring', str(err)])

                    metric_type_redis_key = None
                    metric_dict = {}
                    try:
                        metric_dict = assigned_metrics_dict[base_name]['metric_dict']
                    except:
                        metric_dict = {}
                    if not metric_dict:
                        try:
                            # metric_dict = metric_name_labels_parser(skyline_app, base_name)
                            metric_dict = self.metric_name_labels_parser(base_name)
                            if metric_dict:
                                metrics_dict[base_name]['metric_dict'] = metric_dict
                        except Exception as err:
                            err_msg = '%s - %s' % (err, traceback.format_exc())
                            errors.append([labelled_metric, 'metric_name_labels_parser', str(err_msg)])
                            metrics_skipped += 1
                            exceptions['LabelsError'] += 1
                            # @added 20230404 - Feature #4888: analyzer - load_shedding
                            metrics_last_analysis_dict[labelled_metric] = int(right_now)
                            continue

                    try:
                        metric_name = metric_dict['metric']
                        tenant_id = metric_dict['labels']['_tenant_id']
                        server_id = metric_dict['labels']['_server_id']
                        metric_type_redis_key = 'metrics_manager.prometheus.metrics_type.%s.%s' % (str(tenant_id), str(server_id))
                        tenant_ids.append(tenant_id)
                    except Exception as err:
                        errors.append([labelled_metric, 'parsing metric_dict', str(err)])
                        metrics_skipped += 1
                        exceptions['metricDict'] += 1
                        # @added 20230404 - Feature #4888: analyzer - load_shedding
                        metrics_last_analysis_dict[labelled_metric] = int(right_now)
                        continue

                    determine_monotonic = False
                    calculate_derivative = False
                    metric_type_known = False
                    # The metric_subtype allows for SUMMARY and HISTOGRAM metrics
                    # to be defined as COUNTER or GAUGE metrics due to the fact
                    # that these metrics can be COUNTERs or GAUGES
                    metric_subtype = None
                    add_metric_subtype = False

                    metric_type = None
                    try:
                        # metric_type = metrics_dict[base_name]['type']
                        metric_type = metric_dict['type']
                    except:
                        metric_type = None

                    if not metric_type:
                        try:
                            metric_type = metrics_dict[base_name]['type']
                        except:
                            metric_type = None

                    in_labelled_metrics_id_types = False
                    try:
                        in_labelled_metrics_id_types = metric_dict['in_labelled_metrics_id_types']
                    except:
                        in_labelled_metrics_id_types = False

                    if metric_type:
                        metric_types_known[metric_type]['known'] += 1

                    # There is an issue in Prometheus metric types in so far as
                    # developers will not always adhere to metric typing rules
                    # and in some exporter you will find COUNTER and GAUGE
                    # metrics in the same metric namespace.  To overcome this
                    # analyzer periodically checks the metrics and identifies
                    # the skyline_metric_type, because the Prometheus metadata
                    # cannot be trusted.
                    if not metric_type:
                        try:
                            # @modified 20221229 - Bug #4766: analyzer_labelled_metrics - monotonicity checked incorrect classification
                            # Fetch the data once because with 1000s of new metrics added it
                            # becomes more efficient
                            # metric_type_id_str = self.redis_conn_decoded.hget('skyline.labelled_metrics.id.type', str(metric_id))
                            metric_type_id_str = skyline_labelled_metrics_id_types[str(metric_id)]
                            if metric_type_id_str:
                                metric_type = skyline_metric_types_by_id[int(metric_type_id_str)]
                        except Exception as err:
                            err_msg = 'hgetall skyline.labelled_metrics.id.type'
                            errors.append([labelled_metric, err_msg, str(err)])
                        if metric_type:
                            metric_subtype = str(metric_type)
                            metric_types_known[metric_type]['looked_up'] += 1

                    # metric_type = 'COUNTER'
                    if not metric_type and metric_type_redis_key:
                        if metric_type_redis_key not in list(metric_type_redis_keys.keys()):
                            metric_type_redis_key_dict = None
                            try:
                                metric_type_redis_key_dict = self.redis_conn_decoded.hgetall(metric_type_redis_key)
                            except Exception as err:
                                err_msg = 'hgetall %s' % metric_type_redis_key
                                errors.append([labelled_metric, err_msg, str(err)])
                            metric_type_redis_keys[metric_type_redis_key] = metric_type_redis_key_dict
                        try:
                            metric_type = metric_type_redis_keys[metric_type_redis_key][metric_name]
                            if metric_type:
                                metrics_dict[base_name]['type'] = metric_type
                                if metric_type in ['COUNTER', 'GAUGE']:
                                    metrics_type_dict[metric_id] = skyline_metric_types[metric_type]
                                metric_types_known[metric_type]['looked_up'] += 1
                        except:
                            metric_type = None

                    # TODO - function to determine monotonic and add to hash, remember to remove id
                    # from the hash when metric becomes inactive.  Probably best to do in metrics_manager
                    if metric_type == 'COUNTER':
                        metric_subtype = str(metric_type)
                        calculate_derivative = True
                        metric_type_known = True
                    if metric_type == 'GAUGE':
                        metric_subtype = str(metric_type)
                        calculate_derivative = False
                        metric_type_known = True
                    metric_subtype_redis_key = 'metrics_manager.prometheus.metrics_subtype.%s.%s' % (str(tenant_id), str(server_id))
                    if metric_type in ['SUMMARY', 'HISTOGRAM', None]:
                        try:
                            metric_subtype = self.redis_conn_decoded.hget(metric_subtype_redis_key, str(metric_id))
                        except Exception as err:
                            errors.append([labelled_metric, metric_subtype_redis_key, str(err)])
                        if metric_subtype == 'None':
                            metric_subtype = None
                        if not metric_subtype:
                            add_metric_subtype = True
                        else:
                            if not metric_type:
                                # metric_type = 'UNKNOWN'
                                metric_type = None
                            try:
                                metric_subtypes_queried['UNKNOWN'][metric_subtype] += 1
                            except Exception as err:
                                msg = 'subtype queried update dict: metric_subtypes_queried[%s][%s]' % (str(metric_type), str(metric_subtype))
                                errors.append([labelled_metric, msg, str(err)])
                    if metric_subtype and not metric_type:
                        metric_type = str(metric_subtype)
                    if not metric_subtype:
                        determine_monotonic = True

                    # Periodically check metric monotonicity every 4 hours
                    if monotonicity_checked < monotonicity_check_per_minute:
                        continue_check = True
                        # Do not check not older than 1 hour
                        try:
                            if timeseries[0][0] > (until_timestamp - 3600):
                                continue_check = False
                        except:
                            continue_check = False
                        # @modified 20221228 - Bug #4766: analyzer_labelled_metrics - monotonicity checked incorrect classification
                        # The timeseries length could be less than 180 if a few
                        # data points are missed.  Using MIN_TOLERABLE_LENGTH
                        # if len(timeseries) < 180:
                        #    continue_check = False
                        if boring:
                            continue_check = False

                        # @added 20230426 - Feature #4724: custom_algorithms - anomalous_daily_peak
                        # Added expiry to record metrics identified as normal by anomalous_daily_peaks
                        if normal_daily_peak_expiry:
                            continue_check = False

                        # @added 20230425 - Feature #4894: labelled_metrics - SKYLINE_FEEDBACK_NAMESPACES
                        if feedback_metric_expiry:
                            continue_check = False

                        if continue_check:
                            monotonicity_last_checked = None
                            check_monotonicity = False
                            try:
                                monotonicity_last_checked = int(float(self.redis_conn_decoded.hget('analyzer_labelled_metrics.monotonicity_last_checked', str(metric_id))))
                            except:
                                monotonicity_last_checked = None
                            if not monotonicity_last_checked:
                                check_monotonicity = True
                            if monotonicity_last_checked:
                                if monotonicity_last_checked < (until_timestamp - (3600 * 4)):
                                    check_monotonicity = True
                            if check_monotonicity:
                                determine_monotonic = True
                                # DISABLED
                                # @modified 20221228 - Bug #4766: analyzer_labelled_metrics - monotonicity checked incorrect classification
                                # RE-ENABLED
                                # if int(metric_id) != 12060:
                                #     determine_monotonic = False
                            else:
                                monotonicity_not_expired += 1

                    if add_metric_subtype:
                        determine_monotonic = True

                    # @added 20230426 - Feature #4724: custom_algorithms - anomalous_daily_peak
                    # Added expiry to record metrics identified as normal by anomalous_daily_peaks
                    if normal_daily_peak_expiry:
                        determine_monotonic = False

                    # @added 20230425 - Feature #4894: labelled_metrics - SKYLINE_FEEDBACK_NAMESPACES
                    if feedback_metric_expiry:
                        determine_monotonic = False

                    # @added 20230518 - metric_type.longterm_expire
                    # Do not overwrite longterm metric_type classifications
                    metric_type_longterm_expire = None
                    if determine_monotonic and skyline_labelled_metrics_id_types_longterm_expire:
                        try:
                            metric_type_longterm_expire = skyline_labelled_metrics_id_types_longterm_expire[str(metric_id)]
                        except KeyError:
                            metric_type_longterm_expire = None
                        except Exception as err:
                            logger.error('error :: labelled_metrics_spin_process :: failed to determine expire_str from skyline_labelled_metrics_id_types_longterm_expire - %s' % err)
                        if metric_type_longterm_expire:
                            determine_monotonic = False

                    if determine_monotonic:
                        monotonicity_checked += 1
                        is_strictly_increasing_monotonicity = False
                        try:
                            is_strictly_increasing_monotonicity = strictly_increasing_monotonicity(timeseries)
                        except Exception as err:
                            errors.append([labelled_metric, 'is_strictly_increasing_monotonicity', str(err)])
                        try:
                            monotonicity_last_checked = self.redis_conn_decoded.hset('analyzer_labelled_metrics.monotonicity_last_checked', str(metric_id), str(until_timestamp))
                        except:
                            pass

                        if is_strictly_increasing_monotonicity:
                            calculate_derivative = True
                            if metric_type != 'COUNTER':
                                logger.debug('debug :: labelled_metrics_spin_process :: monotonicity changed for %s, from type: %s to COUNTER - %s' % (
                                    str(metric_id), str(metric_type), str(assigned_metrics_dict[base_name])))
                                metric_type = 'COUNTER'
                                monotonicity_changed += 1
                            if metric_subtype != 'COUNTER':
                                logger.debug('debug :: labelled_metrics_spin_process :: monotonicity changed for %s, from subtype: %s to COUNTER' % (
                                    str(metric_id), str(metric_subtype)))
                                metric_subtype = 'COUNTER'
                                add_metric_subtype = True
                                monotonicity_changed += 1
                            metric_type = 'COUNTER'
                            metric_subtype = 'COUNTER'
                        else:
                            calculate_derivative = False
                            if metric_type != 'GAUGE':
                                logger.debug('debug :: labelled_metrics_spin_process :: monotonicity changed for %s, from type: %s to GAUGE - %s' % (
                                    str(metric_id), str(metric_type), str(assigned_metrics_dict[base_name])))
                                metric_type = 'GAUGE'
                                monotonicity_changed += 1
                            if metric_subtype != 'GAUGE':
                                logger.debug('debug :: labelled_metrics_spin_process :: monotonicity changed for %s, from subtype: %s to GAUGE' % (
                                    str(metric_id), str(metric_subtype)))
                                metric_subtype = 'GAUGE'
                                monotonicity_changed += 1
                                add_metric_subtype = True
                            metric_type = 'GAUGE'
                            metric_subtype = 'GAUGE'

                    if add_metric_subtype:
                        try:
                            self.redis_conn_decoded.hset(metric_subtype_redis_key, str(metric_id), metric_subtype)
                            # metric_subtypes_added += 1
                            metric_subtypes_added[metric_type][metric_subtype] += 1
                        except Exception as err:
                            errors.append([labelled_metric, 'metric_subtype', str(err)])
                    if metric_subtype == 'COUNTER':
                        calculate_derivative = True

                    # Add type to dict for metrics_manager to update
                    if metric_type in ['COUNTER', 'GAUGE']:
                        metrics_type_dict[metric_id] = skyline_metric_types[metric_type]
                    if metric_subtype in ['COUNTER', 'GAUGE']:
                        metrics_type_dict[metric_id] = skyline_metric_types[metric_subtype]

                    if not in_labelled_metrics_id_types and not boring:
                        if metric_subtype:
                            metrics_type_dict[metric_id] = skyline_metric_types[metric_subtype]

                    if calculate_derivative and not boring:
                        try:
                            timeseries = nonNegativeDerivative(timeseries)
                        except Exception as err:
                            errors.append([labelled_metric, 'nonNegativeDerivative timeseries', str(err)])

                    check_for_anomalous = True
                    if boring:
                        check_for_anomalous = False
                        boring_dict[metric_id] = str(timeseries[-1][1])

                    # @added 20230426 - Feature #4724: custom_algorithms - anomalous_daily_peak
                    # Added expiry to record metrics identified as normal by anomalous_daily_peaks
                    if normal_daily_peak_expiry:
                        check_for_anomalous = False
                        normal_daily_peak_metrics_skipped.append(metric_id)
                        last_analysed_timeseries = list(timeseries)
                        last_analysed_metric = base_name

                    # @added 20230425 - Feature #4894: labelled_metrics - SKYLINE_FEEDBACK_NAMESPACES
                    if feedback_metric_expiry:
                        check_for_anomalous = False
                        feedback_metrics_skipped.append(metric_id)
                        last_analysed_timeseries = list(timeseries)
                        last_analysed_metric = base_name

                    # @added 20230404 - Feature #4888: analyzer - load_shedding
                    metrics_last_analysis_dict[labelled_metric] = int(right_now)

                    mad_anomalous = None
                    use_mad = False
                    metrics_analysed += 1

                    # Only check if stationary every ~30 minutes
                    stationary_status = None
                    if check_for_anomalous:
                        try:
                            stationary_status = assigned_metrics_dict[base_name]['stationary']['v']
                            stationary_last_checked = int(assigned_metrics_dict[base_name]['stationary']['ts'])
                            # Only do 1000 stationary tests per run
                            if len(stationary_timings) < 1000:
                                if stationary_last_checked < (current_aligned_ts - 1800):
                                    stationary_status = None
                                else:
                                    stationary_not_expired += 1
                            else:
                                stationary_not_expired += 1
                        except:
                            stationary_status = None

                        try:
                            if str(stationary_status) == 'None':
                                s_start = time()
                                stationary_status = is_stationary(timeseries)
                                stationary_timings.append(time() - s_start)
                                value_data = '%s,%s' % (str(stationary_status), str(current_aligned_ts))
                                metrics_stationary_update_dict[str(metric_id)] = value_data
                            if stationary_status:
                                stationary += 1
                                use_mad = True
                            else:
                                not_stationary += 1
                        except Exception as err:
                            errors.append([labelled_metric, 'is_stationary', str(err)])

                        if use_mad:
                            m_start = time()
                            try:
                                mad_anomalous = median_absolute_deviation(timeseries)
                                mad_analysed += 1
                                if str(mad_anomalous) == 'False':
                                    check_for_anomalous = False
                                if mad_anomalous:
                                    mad_anomalies += 1
                                    mad_anomalous_dict[base_name] = str(timeseries)
                            except Exception as err:
                                errors.append([labelled_metric, 'median_absolute_deviation', str(err)])
                            mad_timings.append(time() - m_start)
                        last_analysed_timeseries = list(timeseries)
                        last_analysed_metric = base_name

                    full_duration_timeseries = []
                    if check_for_anomalous:
                        r_start = time()
                        try:
                            full_duration_timeseries = self.redis_conn_decoded.ts().range(redis_ts_key, (from_timestamp * 1000), (until_timestamp * 1000))
                        except Exception as err:
                            logger.error('error :: labelled_metrics_spin_process :: failed to get Redis full_duration timeseries for %s  - %s' % (
                                str(labelled_metric), err))
                            errors.append([labelled_metric, 'ts().range', str(err)])
                            full_duration_timeseries = []
                        redis_full_duration_timings.append(time() - r_start)
                        if not full_duration_timeseries:
                            check_for_anomalous = False
                        else:
                            timeseries = full_duration_timeseries
                            timeseries = [[int(mts / 1000), value] for mts, value in timeseries]

                    # @added 20230329 - Feature #4882: labelled_metrics - resolution and data sparsity
                    #                   Feature #3870: metrics_manager - check_data_sparsity
                    # Handling the populating of the labelled_metrics data
                    # sparsity directly in analyzer_labelled_metrics to
                    # prevent having to surface the data in metrics_manager
                    # if the sparsity has been recently updated.  Running
                    # the determine_data_frequency and the
                    # determine_data_sparsity functions, combined together
                    # take 77.1 µs (microseconds) to run, this means that
                    # we can process ~12970 timeseries per seconds with
                    # these functions.
                    if check_for_anomalous:
                        update_resolution_and_sparsity = False
                        last_sparsity_check_timestamp = 0
                        try:
                            last_sparsity_check_timestamp_str = labelled_metrics_resolution_sparsity_last_checked_dict[str(metric_id)]
                            if last_sparsity_check_timestamp_str:
                                last_sparsity_check_timestamp = int(float(last_sparsity_check_timestamp_str))
                                if (last_sparsity_check_timestamp + 1800) > int(r_start):
                                    update_resolution_and_sparsity = True
                                else:
                                    labelled_metrics_resolution_sparsity_recently_checked += 1
                        except:
                            last_sparsity_check_timestamp = 0
                            update_resolution_and_sparsity = True
                        if update_resolution_and_sparsity:
                            start_resolution_and_sparsity_timing = time()
                            metric_resolution = 0
                            data_sparsity = None
                            try:
                                metric_resolution, timestamp_resolutions_count = determine_data_frequency(skyline_app, timeseries, False)
                            except Exception as err:
                                errors.append([labelled_metric, 'determine_data_frequency', str(err)])
                                metric_resolution = 0
                            if metric_resolution:
                                labelled_metrics_resolutions[str(metric_id)] = metric_resolution
                                try:
                                    data_sparsity = determine_data_sparsity(skyline_app, timeseries, resolution=metric_resolution, log=False)
                                except Exception as err:
                                    errors.append([labelled_metric, 'determine_data_sparsity', str(err)])
                                    data_sparsity = None
                            if isinstance(data_sparsity, float):
                                labelled_metrics_sparsity[str(metric_id)] = data_sparsity
                                labelled_metrics_resolution_sparsity_checked_dict[str(metric_id)] = int(right_now)
                            resolution_and_sparsity_timings.append(time() - start_resolution_and_sparsity_timing)

                    anomalous = None
                    ensemble = []
                    datapoint = None
                    algorithms_run = []

                    # @added 20221228 - Bug #4766: analyzer_labelled_metrics - monotonicity checked incorrect classification
                    # Always check the monotonicity of the timeseries when it
                    # moves to the full_duration analysis and update the Redis
                    # sets as appropriate.
                    # if check_for_anomalous and metric_type != 'COUNTER' and metric_subtype != 'COUNTER':
                    # @modified 20230518 - metric_type.longterm_expire
                    # Do not overwrite longterm metric_type classifications
                    # if full_duration_timeseries:
                    if full_duration_timeseries and not metric_type_longterm_expire:
                        monotonicity_checked += 1
                        is_strictly_increasing_monotonicity = False
                        try:
                            is_strictly_increasing_monotonicity = strictly_increasing_monotonicity(full_duration_timeseries)
                        except Exception as err:
                            errors.append([labelled_metric, 'is_strictly_increasing_monotonicity', str(err)])
                        try:
                            monotonicity_last_checked = self.redis_conn_decoded.hset('analyzer_labelled_metrics.monotonicity_last_checked', str(metric_id), str(until_timestamp))
                        except:
                            pass
                        add_metric_subtype = False
                        if is_strictly_increasing_monotonicity:
                            calculate_derivative = True
                            if metric_type != 'COUNTER':
                                logger.debug('debug :: labelled_metrics_spin_process :: full_duration monotonicity changed for %s, from type: %s to COUNTER - %s' % (
                                    str(metric_id), str(metric_type), str(assigned_metrics_dict[base_name])))
                                metric_type = 'COUNTER'
                                monotonicity_changed += 1
                            if metric_subtype != 'COUNTER':
                                logger.debug('debug :: labelled_metrics_spin_process :: full_duration monotonicity changed for %s, from subtype: %s to COUNTER' % (
                                    str(metric_id), str(metric_subtype)))
                                metric_subtype = 'COUNTER'
                                add_metric_subtype = True
                            metric_type = 'COUNTER'
                            metric_subtype = 'COUNTER'
                        else:
                            # @added 20230517 - Bug #4766: analyzer_labelled_metrics - monotonicity checked incorrect classification
                            # Handle changes from COUNTER to GAUGE as well
                            if metric_type != 'GAUGE':
                                logger.debug('debug :: labelled_metrics_spin_process :: full_duration monotonicity changed for %s, from type: %s to GAUGE - %s' % (
                                    str(metric_id), str(metric_type), str(assigned_metrics_dict[base_name])))
                                metric_type = 'GAUGE'
                                monotonicity_changed += 1
                            if metric_subtype != 'GAUGE':
                                logger.debug('debug :: labelled_metrics_spin_process :: full_duration monotonicity changed for %s, from subtype: %s to GAUGE' % (
                                    str(metric_id), str(metric_subtype)))
                                metric_subtype = 'GAUGE'
                                add_metric_subtype = True
                            metric_subtype = 'GAUGE'
                            metric_type = 'GAUGE'
                        if add_metric_subtype:
                            try:
                                self.redis_conn_decoded.hset(metric_subtype_redis_key, str(metric_id), metric_subtype)
                                metric_subtypes_added[metric_type][metric_subtype] += 1
                            except Exception as err:
                                errors.append([labelled_metric, 'metric_subtype', str(err)])
                        if metric_subtype == 'COUNTER':
                            calculate_derivative = True
                        # Add type to dict for metrics_manager to update
                        if metric_type in ['COUNTER', 'GAUGE']:
                            metrics_type_dict[metric_id] = skyline_metric_types[metric_type]
                        if metric_subtype in ['COUNTER', 'GAUGE']:
                            metrics_type_dict[metric_id] = skyline_metric_types[metric_subtype]

                    if check_for_anomalous:
                        run_selected_algorithm_count += 1
                        d_start = time()
                        try:
                            downsampled_timeseries = downsample_timeseries('analyzer', timeseries, 60, 600, 'mean', 'end')
                        except Exception as err:
                            errors.append([labelled_metric, 'downsample_timeseries', str(err)])
                            downsampled_timeseries = list(timeseries)
                        downsample_timings.append(time() - d_start)

                        # Calculate the derivate AFTER downsampling
                        if check_for_anomalous and calculate_derivative:
                            if downsampled_timeseries:
                                try:
                                    downsampled_timeseries = nonNegativeDerivative(downsampled_timeseries)
                                except Exception as err:
                                    errors.append([labelled_metric, 'nonNegativeDerivative downsampled_timeseries', str(err)])

                        t_start = time()
                        try:
                            anomalous, ensemble, datapoint, negatives_found, algorithms_run = run_selected_algorithm(downsampled_timeseries, base_name, metric_airgaps, metric_airgaps_filled, run_negatives_present, check_for_airgaps_only, custom_stale_metrics_dict)
                            try:
                                del negatives_found
                            except:
                                pass
                        # It could have been deleted by the Roomba
                        except TypeError:
                            exceptions['DeletedByRoomba'] += 1
                        except EmptyTimeseries:
                            exceptions['EmptyTimeseries'] += 1
                            empty_timeseries_list.append(labelled_metric)
                        except TooShort:
                            exceptions['TooShort'] += 1
                            # @added 20230309 - Feature #4864: analyzer_labelled_metrics - exceptions metrics set
                            # @modified 20230401 - Feature #4886: analyzer - operation_timings
                            # Only sadd once
                            identified_tooshort_metrics.append(str(metric_id))
                            # try:
                            #     self.redis_conn_decoded.sadd('analyzer_labelled_metrics.tooshort', str(metric_id))
                            # except Exception as err:
                            #    redis_set_errors.append(['analyzer_labelled_metrics.tooshort', str(err)])
                        except Stale:
                            exceptions['Stale'] += 1
                            # @added 20230309 - Feature #4864: analyzer_labelled_metrics - exceptions metrics set
                            # @modified 20230401 - Feature #4886: analyzer - operation_timings
                            # Only sadd once
                            identified_stale_metrics.append(str(metric_id))
                            # try:
                            #     self.redis_conn_decoded.sadd('analyzer_labelled_metrics.stale', str(metric_id))
                            # except Exception as err:
                            #     redis_set_errors.append(['analyzer_labelled_metrics.stale', str(err)])
                        except Boring:
                            exceptions['Boring'] += 1
                            # @added 20230309 - Feature #4864: analyzer_labelled_metrics - exceptions metrics set
                            # @modified 20230401 - Feature #4886: analyzer - operation_timings
                            # Only sadd once
                            identified_boring_metrics.append(str(metric_id))
                            # try:
                            #     self.redis_conn_decoded.sadd('analyzer_labelled_metrics.boring', str(metric_id))
                            # except Exception as err:
                            #     redis_set_errors.append(['analyzer_labelled_metrics.boring', str(err)])
                        except Exception as err:
                            exceptions['Other'] += 1
                            errors.append([labelled_metric, 'run_selected_algorithm - Other', str(err)])
                        threesigma_timings.append(time() - t_start)

                    # @added 20220919 - Feature #4676: analyzer - illuminance.all key
                    if ensemble.count(True) > 0:
                        triggered_algorithms = []
                        for index, value in enumerate(ensemble):
                            if value:
                                algorithm = algorithms_run[index]
                                try:
                                    algorithm_id = int(algorithms[algorithm])
                                except:
                                    algorithm_id = 0
                                triggered_algorithms.append(algorithm_id)
                        try:
                            illuminance_all_dict[str(metric_id)] = {
                                # 't': int(metric_timestamp),
                                't': int(last_timeseries_timestamp),
                                'v': float(datapoint),
                                'a': triggered_algorithms}
                        except Exception as err:
                            errors.append([labelled_metric, 'failed to add illuminance_all_dict', str(err)])

                    if not anomalous:
                        no_anomalies += 1
                        try:
                            data = [metric_id, last_timeseries_timestamp]
                            not_anomalous_list.append(str(data))
                        except Exception as err:
                            errors.append([labelled_metric, 'not_anomalous_list', str(err)])
                        continue

                    anomalies += 1

                    threesigma_anomalous_dict[base_name] = str(timeseries)

                    if calculate_derivative:
                        try:
                            timeseries = nonNegativeDerivative(timeseries)
                        except Exception as err:
                            errors.append([labelled_metric, 'nonNegativeDerivative timeseries', str(err)])

                    # Handle single value timeseries. The second timestamp in the
                    # timeseries was used because the first timestamp in a
                    # nonNegativeDerivative timeseries has no value.
                    try:
                        from_timestamp = int(timeseries[1][0])
                    except:
                        try:
                            from_timestamp = int(timeseries[0][0])
                        except:
                            # Failover to last_timeseries_timestamp
                            from_timestamp = last_timeseries_timestamp
                    metric_timestamp = last_timeseries_timestamp

                    triggered_algorithms_for_waterfall_alert = []
                    for index, value in enumerate(ensemble):
                        if value:
                            algorithm = algorithms_run[index]
                            triggered_algorithms_for_waterfall_alert.append(algorithm)
                    waterfall_panorama_data_added_at = until_timestamp
                    waterfall_panorama_data_source = 'redistimeseries'
                    waterfall_panorama_data = [
                        base_name, datapoint, from_timestamp, metric_timestamp,
                        algorithms_run, triggered_algorithms_for_waterfall_alert,
                        skyline_app, waterfall_panorama_data_source, this_host,
                        waterfall_panorama_data_added_at
                    ]

                    metric = [datapoint, base_name, metric_timestamp]
                    redis_set = 'analyzer_labelled_metrics.real_anomalous_metrics'
                    data = str(metric)
                    try:
                        self.redis_conn.sadd(redis_set, data)
                        self.redis_conn.expire(redis_set, 120)
                    except Exception as err:
                        errors.append([labelled_metric, 'sadd analyzer_labelled_metrics.real_anomalous_metrics', str(err)])

                    # Get the anomaly breakdown - who returned True?
                    triggered_algorithms = []
                    for index, value in enumerate(ensemble):
                        if value:
                            algorithm = algorithms_run[index]
                            anomaly_breakdown[algorithm] += 1
                            triggered_algorithms.append(algorithm)
                    metric_data = [datapoint, base_name, metric_timestamp, triggered_algorithms, algorithms_run]
                    metric_data = {
                        'metric': base_name,
                        'metric_id': int(metric_id),
                        'metric_dict': metrics_dict[base_name],
                        'from_timestamp': from_timestamp,
                        'timestamp': metric_timestamp,
                        'value': datapoint,
                        'triggered_algorithms': triggered_algorithms,
                        'algorithms_run': algorithms_run,
                        'snab_only_check': False,
                        'processed_by': {
                            'analyzer_labelled_metrics': {
                                'timestamp': int(time()),
                                'triggered_algorithms': triggered_algorithms,
                                'algorithms_run': algorithms_run,
                                'waterfall_panorama_data': waterfall_panorama_data,
                                'process_number': i_process,
                            },
                        },
                    }
                    redis_set = 'analyzer_labelled_metrics.anomalous_metrics'
                    try:
                        self.redis_conn.sadd(redis_set, str(metric_data))
                        self.redis_conn.expire(redis_set, 120)
                    except Exception as err:
                        errors.append([labelled_metric, 'sadd analyzer_labelled_metrics.anomalous_metrics', str(err)])

                    # Add ionosphere data
                    # timeseries_dir = str(metric_id)
                    # timeseries_dir = base_name.replace('.', '/')
                    timeseries_dir = labelled_metric.replace('.', '/')

                    training_dir = '%s/%s/%s' % (
                        settings.IONOSPHERE_DATA_FOLDER, str(metric_timestamp),
                        str(timeseries_dir))
                    if not os.path.exists(training_dir):
                        mkdir_p(training_dir)
                    full_duration_in_hours = int(settings.FULL_DURATION) / 3600
                    ionosphere_json_file = '%s/%s.mirage.redis.%sh.json' % (
                        training_dir, str(labelled_metric),
                        str(int(full_duration_in_hours)))
                    if not os.path.isfile(ionosphere_json_file):
                        # Do not use the downsample_timeseries as there are only
                        # ~144 data points which is not sufficient for echo
                        # features profiles or motif matching
                        # timeseries_json = str(downsampled_timeseries).replace('[', '(').replace(']', ')')
                        timeseries_json = str(timeseries).replace('[', '(').replace(']', ')')
                        try:
                            write_data_to_file(skyline_app, ionosphere_json_file, 'w', timeseries_json)
                            if VERBOSE_LOGGING:
                                logger.info('labelled_metrics_spin_process :: added Ionosphere Mirage %sh Redis data timeseries json file :: %s' % (
                                    str(int(full_duration_in_hours)), ionosphere_json_file))
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: labelled_metrics_spin_process :: failed to add Ionosphere Mirage Redis data timeseries json file - %s - %s' % (
                                ionosphere_json_file, err))

                    # @added 20200904 - Feature #3734: waterfall alerts
                    added_to_waterfall_timestamp = int(time())
                    # [metric, timestamp, value, added_to_waterfall_timestamp, waterfall_panorama_data]
                    waterfall_data = [labelled_metric, metric_timestamp, datapoint, added_to_waterfall_timestamp, waterfall_panorama_data]
                    redis_set = 'analyzer.waterfall_alerts.sent_to_mirage'
                    # Only add to waterfall_alerts if it is not
                    # a snab_only_check
                    snab_only_check = False
                    if not snab_only_check:
                        try:
                            self.redis_conn.sadd(redis_set, str(waterfall_data))
                            # if VERBOSE_LOGGING:
                            logger.info('labelled_metrics_spin_process :: added to Redis set %s - %s' % (redis_set, str(waterfall_data)))
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: labelled_metrics_spin_process :: failed to add %s to Redis set %s - %s' % (
                                str(waterfall_data), str(redis_set), e))

                    redis_hash = 'analyzer_labelled_metrics.mirage_check'
                    hash_key = '%s.%s' % (str(metric_id), str(int(time())))
                    try:
                        self.redis_conn.hset(redis_hash, hash_key, str(metric_data))
                        logger.info('labelled_metrics_spin_process :: added mirage check for %s with metric_data: %s' % (
                            str(labelled_metric), str(metric_data)))
                    except Exception as err:
                        errors.append([labelled_metric, 'hset analyzer_labelled_metrics.to_mirage', metric_data, str(err)])

                    # @added 20220504 - Feature #2580: illuminance
                    try:
                        illuminance_dict[str(metric_id)] = {
                            'timestamp': int(metric_timestamp),
                            'value': float(datapoint),
                            'triggered_algorithms_count': len(triggered_algorithms)}
                    except Exception as err:
                        errors.append([labelled_metric, 'failed to add illuminance_dict', str(err)])
                    # FINISH HERE FOR NOW
                    continue

                if not_anomalous_list:
                    redis_set = 'analyzer_labelled_metrics.not_anomalous_metrics'
                    logger.info('labelled_metrics_spin_process :: adding %s metric ids to %s' % (
                        str(len(not_anomalous_list)), redis_set))
                    try:
                        items_added = self.redis_conn.sadd(redis_set, *not_anomalous_list)
                        logger.info('labelled_metrics_spin_process :: added %s metric ids to %s' % (
                            str(items_added), redis_set))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to sadd to %s Redis set - %s' % (
                            redis_set, err))
                        errors.append(['labelled_metrics_spin_process', 'sadd analyzer_labelled_metrics.not_anomalous_metrics', str(err)])
                if empty_timeseries_list:
                    redis_set = 'redistimeseries_roomba.metrics_to_remove'
                    logger.info('labelled_metrics_spin_process :: adding %s metrics to %s' % (
                        str(len(empty_timeseries_list)), redis_set))
                    try:
                        items_added = self.redis_conn.sadd(redis_set, *empty_timeseries_list)
                        logger.info('labelled_metrics_spin_process :: added %s metrics to %s' % (
                            str(items_added), redis_set))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to sadd to %s Redis set - %s' % (
                            redis_set, err))
                        errors.append(['labelled_metrics_spin_process', 'sadd redistimeseries_roomba.metrics_to_remove', str(err)])

                if boring_dict:
                    redis_set = 'analyzer_labelled_metrics.boring.%s' % str(current_aligned_ts)
                    logger.info('labelled_metrics_spin_process :: adding %s metrics to %s' % (
                        str(len(boring_dict)), redis_set))
                    try:
                        items_added = self.redis_conn.hset(redis_set, mapping=boring_dict)
                        logger.info('labelled_metrics_spin_process :: added %s metrics to %s' % (
                            str(items_added), redis_set))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to hset to %s Redis set - %s' % (
                            redis_set, err))
                        errors.append(['labelled_metrics_spin_process', 'hset analyzer_labelled_metrics.boring', str(err)])
                    try:
                        self.redis_conn.expire(redis_set, 120)
                    except Exception as err:
                        logger.error('error :: labelled_metrics_spin_process :: failed to expire to %s Redis set - %s' % (
                            redis_set, err))

                logger.info('labelled_metrics_spin_process :: metric_subtypes_added: %s' % str(metric_subtypes_added))
                logger.info('labelled_metrics_spin_process :: metric_subtypes_queried: %s' % str(metric_subtypes_queried))
                logger.info('labelled_metrics_spin_process :: metric_types_known: %s' % str(metric_types_known))

                stats = {
                    'checked': metrics_checked,
                    'analysed': metrics_analysed,
                    'skipped': metrics_skipped,
                    'anomalous': anomalies,
                    'not anomalous': no_anomalies,
                    'redis timings': sum(redis_timings),
                    'redis_timings_count': len(redis_timings),
                    'redis full_duration timings': sum(redis_full_duration_timings),
                    'stationary analysed': len(stationary_timings),
                    'stationary': stationary,
                    'not stationary': not_stationary,
                    'stationary timing': sum(stationary_timings),
                    'stationary not expired': stationary_not_expired,
                    'mad analysed': mad_analysed,
                    'mad anomalous': mad_anomalies,
                    'mad timings': sum(mad_timings),
                    'downsampled': len(downsample_timings),
                    'downsample timings': sum(downsample_timings),
                    '3sigma analysed': run_selected_algorithm_count,
                    '3sigma timings': sum(threesigma_timings),
                    'monotonicity_checked': monotonicity_checked,
                    'monotonicity_changed': monotonicity_changed,
                    'resolution_and_sparsity_timings': sum(resolution_and_sparsity_timings),
                    # @added 20230426 - Feature #4724: custom_algorithms - anomalous_daily_peak
                    # Added expiry to record metrics identified as normal by anomalous_daily_peaks
                    'normal_daily_peak_metrics_skipped': len(normal_daily_peak_metrics_skipped),
                    # @added 20230425 - Feature #4894: labelled_metrics - SKYLINE_FEEDBACK_NAMESPACES
                    'feedback_metrics_skipped': len(feedback_metrics_skipped),
                }
                logger.info('labelled_metrics_spin_process :: complete: %s' % str(stats))
                for kkey, value in stats.items():
                    self.stats_q.put((kkey, value))

                logger.info('labelled_metrics_spin_process :: used %s metric id Redis ts keys' % str(used_metric_id_keys))

                logger.info('labelled_metrics_spin_process :: completed with %s errors' % str(len(errors)))
                if errors:
                    logger.info('labelled_metrics_spin_process :: sample error: %s' % str(errors[0]))

                # @added 20230309 - Feature #4864: analyzer_labelled_metrics - exceptions metrics set
                if redis_set_errors:
                    errors = redis_set_errors + errors

                sadd_errors = []
                if errors:
                    error_set = 'analyzer_labelled_metrics.errors.%s' % str(current_aligned_ts)
                    errors_strs = []
                    for error_list in errors:
                        errors_strs.append(str(error_list))
                    try:
                        self.redis_conn_decoded.sadd(error_set, *errors_strs)
                    except Exception as err:
                        sadd_errors.append(['errors_strs', 'sadd analyzer_labelled_metrics.errors', str(err)])
                    if not sadd_errors:
                        logger.info('labelled_metrics_spin_process :: updated %s Redis set with %s errors' % (
                            len(errors), error_set))
                    else:
                        logger.error('error :: labelled_metrics_spin_process :: failed to sadd to %s Redis set with %s errors, last err - %s' % (
                            error_set, str(len(sadd_errors)), err))
                    try:
                        self.redis_conn_decoded.expire(error_set, 120)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to expire %s Redis set - %s' % (
                            error_set, err))

                logger.info('labelled_metrics_spin_process :: %s metrics run through run_selected_algorithm' % (
                    str(run_selected_algorithm_count)))

                # @added 20220420 - Feature #4530: namespace.analysed_events
                namespace_analysed = defaultdict(int)
                for tenant_id in tenant_ids:
                    parent_namespace = tenant_id
                    namespace_analysed[parent_namespace] += 1
                date_string = str(strftime('%Y-%m-%d', gmtime()))
                namespace_analysed_events_hash = 'namespace.analysed_events.%s.%s' % (skyline_app, date_string)
                for namespace in list(namespace_analysed.keys()):
                    try:
                        self.redis_conn.hincrby(namespace_analysed_events_hash, namespace, namespace_analysed[namespace])
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to increment %s Redis hash - %s' % (
                            namespace_analysed_events_hash, err))
                try:
                    self.redis_conn.expire(namespace_analysed_events_hash, (86400 * 15))
                    logger.info('labelled_metrics_spin_process :: updated %s Redis hash' % namespace_analysed_events_hash)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: labelled_metrics_spin_process :: failed to set expire %s Redis hash - %s' % (
                        namespace_analysed_events_hash, err))

                # Add values to the queue so the parent process can collate
                for kkey, value in anomaly_breakdown.items():
                    self.anomaly_breakdown_q.put((kkey, value))
                for kkey, value in exceptions.items():
                    self.exceptions_q.put((kkey, value))

                if len(illuminance_dict) > 0:
                    logger.info('labelled_metrics_spin_process :: calling add_illuminance_entries with %s entries to add' % (
                        str(len(illuminance_dict))))
                    current_illuminance_dict = {}
                    try:
                        current_illuminance_dict = add_illuminance_entries(self, skyline_app, int(spin_start), illuminance_dict)
                    except Exception as err:
                        logger.error('error :: labelled_metrics_spin_process :: add_illuminance_entries failed - %s' % (
                            err))
                    logger.info('labelled_metrics_spin_process :: illuminance Redis hash now has %s entries' % (
                        str(len(current_illuminance_dict))))

                # @added 20220919 - Feature #4676: analyzer - illuminance.all key
                if len(illuminance_all_dict) > 0:
                    logger.info('labelled_metrics_spin_process :: calling add_illuminance_entries (all) with %s entries to add' % (
                        str(len(illuminance_all_dict))))
                    current_illuminance_all_dict = {}
                    try:
                        current_illuminance_all_dict = add_illuminance_entries(self, skyline_app, int(spin_start), illuminance_all_dict)
                    except Exception as err:
                        logger.error('error :: labelled_metrics_spin_process :: add_illuminance_entries (all) failed - %s' % (
                            err))
                    logger.info('labelled_metrics_spin_process :: illuminance_all Redis hash now has %s entries' % (
                        str(len(current_illuminance_all_dict))))

                if metrics_last_timeseries_timestamp_update_dict:
                    hash_key = metrics_last_timeseries_timestamp_hash_key
                    hash_dict = metrics_last_timeseries_timestamp_update_dict
                    logger.info('labelled_metrics_spin_process :: updating %s Redis hash with %s metrics' % (
                        hash_key, str(len(hash_dict))))
                    try:
                        self.redis_conn.hset(hash_key, mapping=hash_dict)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to update %s Redis hash - %s' % (
                            hash_key, err))

                if metrics_stationary_update_dict:
                    hash_key = metrics_stationary_hash_key
                    hash_dict = metrics_stationary_update_dict
                    logger.info('labelled_metrics_spin_process :: updating %s Redis hash with %s metrics' % (
                        hash_key, str(len(hash_dict))))
                    try:
                        self.redis_conn.hset(hash_key, mapping=hash_dict)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to update %s Redis hash - %s' % (
                            hash_key, err))

                if last_analysed_timeseries and i_process == 1:
                    last_analysed_timeseries_data = [last_analysed_metric, last_analysed_timeseries]
                    try:
                        self.redis_conn.setex('analyzer_labelled_metrics.last_analysed_timeseries', 300, str(last_analysed_timeseries_data))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to set analyzer_labelled_metrics.last_analysed_timeseries Redis key - %s' % (
                            err))

                if mad_anomalous_dict and i_process == 1:
                    hash_key = 'analyzer_labelled_metrics.last_mad_anomalous.%s' % str(current_aligned_ts)
                    try:
                        self.redis_conn.hset(hash_key, mapping=mad_anomalous_dict)
                        self.redis_conn.expire(hash_key, 300)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to set %s Redis hash - %s' % (
                            hash_key, err))

                if threesigma_anomalous_dict:
                    hash_key = 'analyzer_labelled_metrics.last_3sigma_anomalous.%s.%s' % (str(i_process), str(current_aligned_ts))
                    try:
                        self.redis_conn.hset(hash_key, mapping=threesigma_anomalous_dict)
                        self.redis_conn.expire(hash_key, 1800)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to set %s Redis hash - %s' % (
                            hash_key, err))

                if empty_metrics:
                    hash_key = 'analyzer_labelled_metrics.empty_metrics.%s' % str(current_aligned_ts)
                    try:
                        self.redis_conn.sadd(hash_key, *empty_metrics)
                        self.redis_conn.expire(hash_key, 120)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to sadd %s Redis set - %s' % (
                            hash_key, err))

                record_stale_metrics = True
                if record_stale_metrics:
                    if stale_metrics:
                        hash_key = 'analyzer_labelled_metrics.stale_metrics.%s' % str(current_aligned_ts)
                        try:
                            self.redis_conn.hset(hash_key, mapping=stale_metrics)
                            self.redis_conn.expire(hash_key, 120)
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: labelled_metrics_spin_process :: failed to set %s Redis hash - %s' % (
                                hash_key, err))
                    if tooshort_metrics:
                        hash_key = 'analyzer_labelled_metrics.tooshort_metrics.%s' % str(current_aligned_ts)
                        try:
                            self.redis_conn.hset(hash_key, mapping=tooshort_metrics)
                            self.redis_conn.expire(hash_key, 120)
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: labelled_metrics_spin_process :: failed to set %s Redis hash - %s' % (
                                hash_key, err))

                record_no_new_data_metrics = True
                if record_no_new_data_metrics:
                    if no_new_data_metrics:
                        hash_key = 'analyzer_labelled_metrics.no_new_data_metrics.%s' % str(current_aligned_ts)
                        try:
                            self.redis_conn.hset(hash_key, mapping=no_new_data_metrics)
                            self.redis_conn.expire(hash_key, 120)
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: labelled_metrics_spin_process :: failed to set %s Redis hash - %s' % (
                                hash_key, err))

                logger.info('labelled_metrics_spin_process :: adding %s metrics to skyline.labelled_metrics.id.type Redis hash' % (
                    str(len(metrics_type_dict))))
                if metrics_type_dict:
                    hash_key = 'skyline.labelled_metrics.id.type'
                    try:
                        self.redis_conn.hset(hash_key, mapping=metrics_type_dict)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to set %s Redis hash - %s' % (
                            hash_key, err))

                # @added 20230517 - Bug #4766: analyzer_labelled_metrics - monotonicity checked incorrect classification
                # If any metrics have changed monotonicity set a key to update the metric_type
                # in the parent process
                if monotonicity_changed:
                    try:
                        self.redis_conn_decoded.setex('skyline.labelled_metrics.id.type.changed', 60, monotonicity_changed)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: analyzer_labelled_metrics :: failed to create Redis key skyline.labelled_metrics.id.type.changed - %s' % (
                            err))

                # @added 20230329 - Feature #4882: labelled_metrics - resolution and data sparsity
                #                   Feature #3870: metrics_manager - check_data_sparsity
                if labelled_metrics_resolution_sparsity_checked_dict:
                    logger.info('labelled_metrics_spin_process :: checked resolution and sparsity for %s metrics' % (
                        str(len(labelled_metrics_resolution_sparsity_checked_dict))))
                if labelled_metrics_resolutions:
                    hash_key = 'labelled_metrics.metric_resolutions'
                    try:
                        self.redis_conn.hset(hash_key, mapping=labelled_metrics_resolutions)
                        logger.info('labelled_metrics_spin_process :: updated %s metrics in %s Redis hash' % (
                            str(len(labelled_metrics_resolutions)), hash_key))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to set %s Redis hash - %s' % (
                            hash_key, err))
                if labelled_metrics_sparsity:
                    hash_key = 'labelled_metrics.data_sparsity'
                    try:
                        self.redis_conn.hset(hash_key, mapping=labelled_metrics_sparsity)
                        logger.info('labelled_metrics_spin_process :: updated %s metrics in %s Redis hash' % (
                            str(len(labelled_metrics_sparsity)), hash_key))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to set %s Redis hash - %s' % (
                            hash_key, err))
                if labelled_metrics_resolution_sparsity_recently_checked:
                    logger.info('labelled_metrics_spin_process :: resolution and sparsity not checked on %s metrics that have been recently checked' % (
                        str(labelled_metrics_resolution_sparsity_recently_checked)))
                if labelled_metrics_resolution_sparsity_checked_dict:
                    hash_key = str(labelled_metrics_resolution_sparsity_last_checked_hash_key)
                    try:
                        self.redis_conn.hset(hash_key, mapping=labelled_metrics_resolution_sparsity_checked_dict)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to set %s Redis hash - %s' % (
                            hash_key, err))

                # @modified 20230401 - Feature #4886: analyzer - operation_timings
                # Only sadd once
                if identified_boring_metrics:
                    logger.info('labelled_metrics_spin_process :: sadding %s identified_boring_metrics to analyzer_labelled_metrics.boring Redis set' % (
                        str(len(identified_boring_metrics))))
                    try:
                        self.redis_conn_decoded.sadd('analyzer_labelled_metrics.boring', *set(identified_boring_metrics))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to sadd identified_boring_metrics to analyzer_labelled_metrics.boring Redis set - %s' % (
                            err))
                if identified_stale_metrics:
                    logger.info('labelled_metrics_spin_process :: sadding %s identified_stale_metrics to analyzer_labelled_metrics.stale Redis set' % (
                        str(len(identified_stale_metrics))))
                    try:
                        self.redis_conn_decoded.sadd('analyzer_labelled_metrics.stale', *set(identified_stale_metrics))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to sadd identified_stale_metrics to analyzer_labelled_metrics.stale Redis set - %s' % (
                            err))
                if identified_tooshort_metrics:
                    logger.info('labelled_metrics_spin_process :: sadding %s identified_tooshort_metrics to analyzer_labelled_metrics.tooshort Redis set' % (
                        str(len(identified_tooshort_metrics))))
                    try:
                        self.redis_conn_decoded.sadd('analyzer_labelled_metrics.tooshort', *set(identified_tooshort_metrics))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: labelled_metrics_spin_process :: failed to sadd identified_tooshort_metrics to analyzer_labelled_metrics.tooshort Redis set - %s' % (
                            err))

                # @added 20230405 - Feature #4890: analyzer_labelled_metrics - use mrange
                # Added filters and all_timeseries
                if timeseries_not_present_in_all_timeseries:
                    current_aligned_ts = int(int(spin_start) // 60 * 60)
                    hash_key = 'analyzer_labelled_metrics.timeseries_not_present_in_all_timeseries.%s' % str(current_aligned_ts)
                    logger.info('labelled_metrics_spin_process :: adding %s metrics to Redis %s hash key' % (
                        str(len(timeseries_not_present_in_all_timeseries)),
                        hash_key))
                    try:
                        self.redis_conn_decoded.hset(hash_key, mapping=timeseries_not_present_in_all_timeseries)
                        self.redis_conn_decoded.expire(hash_key, 180)
                        logger.info('labelled_metrics_spin_process :: added %s metrics to Redis %s hash key' % (
                            str(len(timeseries_not_present_in_all_timeseries)),
                            hash_key))
                    except Exception as err:
                        logger.error('error :: labelled_metrics_spin_process :: failed to set %s hash key - %s' % (
                            hash_key, err))

                # @added 20230404 - Feature #4888: analyzer - load_shedding
                if load_shedding_active or activating_load_shedding or load_shedding_hash_exists:
                    if metrics_last_analysis_dict:
                        try:
                            self.redis_conn_decoded.hset(metrics_last_analysis_hash_key, mapping=metrics_last_analysis_dict)
                            logger.info('labelled_metrics_spin_process :: load shedding - updated %s analysis timestamps in Redis analyzer.metrics.last_analysis hash key' % (
                                str(len(metrics_last_analysis_dict))))
                            if load_shedding_active:
                                # The load_shedding hash only has the expiry set if load
                                # shedding is active, not if the hash is only being updated.
                                self.redis_conn_decoded.expire(metrics_last_analysis_hash_key, 300)
                                logger.info('labelled_metrics_spin_process :: load_shedding_active - set expire to 300 on Redis analyzer.metrics.last_analysis hash key')
                        except Exception as err:
                            logger.error('error :: labelled_metrics_spin_process :: failed to update analysis timestamp in Redis analyzer.metrics.last_analysis hash key - %s' % err)
                        try:
                            key_ttl = self.redis_conn_decoded.ttl(metrics_last_analysis_hash_key)
                            if key_ttl == -1:
                                self.redis_conn_decoded.expire(metrics_last_analysis_hash_key, 300)
                                logger.info('labelled_metrics_spin_process :: set the unset expire TTL on Redis analyzer.metrics.last_analysis hash key to 300')
                        except Exception as err:
                            logger.error('error :: labelled_metrics_spin_process :: failed to set the unset expire TTL on Redis analyzer.metrics.last_analysis hash key - %s' % err)

        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: labelled_metrics_spin_process :: error in memray with block - %s' % (
                err))

        logger.info('labelled_metrics_spin_process :: %s metrics had monotonicity_checked and %s changed' % (
            str(monotonicity_checked), str(monotonicity_changed)))

        spin_end = time() - spin_start
        logger.info('labelled_metrics_spin_process :: process %s took %.2f seconds' % (str(i_process), spin_end))
        return

    def run(self):
        """
        - Called when the process intializes.

        - Determine if Redis is up and discover the number of `unique metrics`.

        - Divide the `unique_labelled_metrics` between the number of `ANALYZER_LABELLED_METRICS_PROCESSES`
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
        now = time()
        log_wait_for = now + 5
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time()
            else:
                now = log_wait_for + 1

        logger.info('analyzer_labelled_metrics :: starting')
        if os.path.isfile(skyline_app_loglock):
            logger.error('error :: analyzer_labelled_metrics :: bin/%s.d log management seems to have failed, continuing' % skyline_app)
        else:
            logger.info('analyzer_labelled_metrics :: bin/%s.d log management done' % skyline_app)

        if not os.path.exists(settings.SKYLINE_TMP_DIR):
            # @modified 20160803 - Adding additional exception handling to Analyzer
            try:
                mkdir_p(settings.SKYLINE_TMP_DIR)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: analyzer_labelled_metrics :: failed to create %s' % settings.SKYLINE_TMP_DIR)

        def smtp_trigger_alert(alert, metric, context):
            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            try:
                p = Process(target=self.spawn_alerter_process, args=(alert, metric, context))
                pids.append(p)
                pid_count += 1
                p.start()
                spawned_pids.append(p.pid)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: analyzer_labelled_metrics :: failed to spawn_alerter_process')
            p_starts = time()
            while time() - p_starts <= 15:
                if any(p.is_alive() for p in pids):
                    # Just to avoid hogging the CPU
                    sleep(.1)
                else:
                    # All the processes are done, break now.
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('analyzer_labelled_metrics :: timed out, killing the spawn_trigger_alert process')
                for p in pids:
                    p.terminate()
                    # p.join()

            for p in pids:
                if p.is_alive():
                    logger.info('analyzer_labelled_metrics :: stopping spawn_trigger_alert - %s' % (str(p.is_alive())))
                    p.join()

        # Discover unique labelled_metrics
        logger.info('analyzer_labelled_metrics :: memory usage before loading unique labelled_metrics - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
        unique_labelled_metrics = []
        try:
            unique_labelled_metrics = list(self.redis_conn_decoded.smembers('labelled_metrics.unique_labelled_metrics'))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: analyzer_labelled_metrics :: could not get the unique_labelled_metrics list from labelled_metrics.unique_labelled_metrics Redis set - %s' % err)
        logger.info('analyzer_labelled_metrics :: memory usage after loading unique labelled_metrics - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))

        logger.info('analyzer_labelled_metrics :: memory usage before loading aet.metrics_manager.metric_names_with_ids - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
        metric_names_with_ids_key = 'aet.metrics_manager.metric_names_with_ids'
        metric_names_with_ids = {}
        try:
            metric_names_with_ids = self.redis_conn_decoded.hgetall(metric_names_with_ids_key)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: analyzer_labelled_metrics :: failed to hgetall aet.metrics_manager.metric_names_with_ids Redis hash key %s - %s' % (
                err))
        logger.info('analyzer_labelled_metrics :: memory usage after loading aet.metrics_manager.metric_names_with_ids - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))

        metric_ids_with_metric_key = 'aet.metrics_manager.active_labelled_ids_with_metric'
        metric_ids_with_name = {}
        try:
            metric_ids_with_name = self.redis_conn_decoded.hgetall(metric_ids_with_metric_key)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: analyzer_labelled_metrics :: failed to hgetall aet.metrics_manager.active_labelled_ids_with_metric Redis hash key %s - %s' % (
                err))
        if not metric_ids_with_name:
            for base_name in metric_names_with_ids:
                metric_id = metric_names_with_ids[base_name]
                metric_ids_with_name[metric_id] = base_name
        logger.info('analyzer_labelled_metrics :: memory usage after loading aet.metrics_manager.active_labelled_ids_with_metric - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))

        logger.info('analyzer_labelled_metrics :: memory usage before creating labelled_metrics_id_types - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
        labelled_metrics_id_types = {}
        try:
            labelled_metrics_id_types = self.redis_conn_decoded.hgetall('skyline.labelled_metrics.id.type')
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: analyzer_labelled_metrics :: failed to hgetall skyline.labelled_metrics.id.type Redis hash key %s - %s' % (
                err))
        logger.info('analyzer_labelled_metrics :: memory usage after creating labelled_metrics_id_types - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))

        # @added 20230404 - Feature #4890: analyzer_labelled_metrics - use mrange
        # Added filters
        tenant_ids_with_count = {}
        tenant_ids_base_names = {}
        tenant_ids_labelled_metrics = {}

        logger.info('analyzer_labelled_metrics :: memory usage before creating metrics_and_labels_dict - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
        metrics_and_labels_dict = {}
        metric_type_redis_keys_list = []
        errors = []
        for labelled_metric in unique_labelled_metrics:
            metric_id = labelled_metric.replace('labelled_metrics.', '', 1)
            try:
                base_name = metric_ids_with_name[str(metric_id)]
            except:
                continue
            metric_dict = {}
            try:
                # metric_dict = metric_name_labels_parser(skyline_app, base_name)
                metric_dict = self.metric_name_labels_parser(base_name)
                if metric_dict:
                    metrics_and_labels_dict[base_name] = metric_dict
                    try:
                        tenant_id = metric_dict['labels']['_tenant_id']
                        server_id = metric_dict['labels']['_server_id']
                        metric_type_redis_key = 'metrics_manager.prometheus.metrics_type.%s.%s' % (str(tenant_id), str(server_id))
                        metric_type_redis_keys_list.append(metric_type_redis_key)
                        # @added 20230404 - Feature #4890: analyzer_labelled_metrics - use mrange
                        # Added filters
                        try:
                            tenant_ids_with_count[tenant_id] += 1
                            tenant_ids_base_names[tenant_id].append(base_name)
                            tenant_ids_labelled_metrics[tenant_id].append(labelled_metric)
                        except:
                            tenant_ids_with_count[tenant_id] = 1
                            tenant_ids_base_names[tenant_id] = [base_name]
                            tenant_ids_labelled_metrics[tenant_id] = [labelled_metric]
                    except:
                        pass
            except Exception as err:
                err_msg = '%s - %s' % (err, traceback.format_exc())
                errors.append([labelled_metric, 'metric_name_labels_parser', str(err_msg)])
            metric_type = None
            in_labelled_metrics_id_types = False
            try:
                metric_type_id_str = labelled_metrics_id_types[str(metric_id)]
                if metric_type_id_str:
                    in_labelled_metrics_id_types = True
                    if metric_type_id_str == '1':
                        metric_type = 'COUNTER'
                    if metric_type_id_str == '0':
                        metric_type = 'GAUGE'
            except:
                metric_type = None
            try:
                metrics_and_labels_dict[base_name]['type'] = metric_type
            except Exception as err:
                err_msg = '%s - %s' % (err, traceback.format_exc())
                errors.append([labelled_metric, 'adding metric type to metrics_and_labels_dict', str(err_msg)])
            try:
                metrics_and_labels_dict[base_name]['in_labelled_metrics_id_types'] = in_labelled_metrics_id_types
            except Exception as err:
                err_msg = '%s - %s' % (err, traceback.format_exc())
                errors.append([labelled_metric, 'adding in_labelled_metrics_id_types to metrics_and_labels_dict', str(err_msg)])

        if errors:
            logger.error('error :: analyzer_labelled_metrics :: metric_name_labels_parser encountered %s errors, sample: %s' % (
                str(len(errors)), str(errors[0])))
        logger.info('analyzer_labelled_metrics :: memory usage after creating metrics_and_labels_dict - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))

        logger.info('analyzer_labelled_metrics :: memory usage before creating metric_type_redis_keys - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
        metric_type_redis_keys = {}
        metric_type_redis_keys_list = list(set(metric_type_redis_keys_list))
        errors = []
        for metric_type_redis_key in metric_type_redis_keys_list:
            if metric_type_redis_key not in list(metric_type_redis_keys.keys()):
                metric_type_redis_key_dict = None
                try:
                    metric_type_redis_key_dict = self.redis_conn_decoded.hgetall(metric_type_redis_key)
                except Exception as err:
                    err_msg = 'hgetall %s' % metric_type_redis_key
                    errors.append([metric_type_redis_key, err_msg, str(err)])
                metric_type_redis_keys[metric_type_redis_key] = metric_type_redis_key_dict
        if errors:
            logger.error('error :: analyzer_labelled_metrics :: metric_name_labels_parser encountered %s errors, sample: %s' % (
                str(len(errors)), str(errors[0])))
        for base_name in list(metrics_and_labels_dict.keys()):
            metric_type = None
            try:
                metric_type = metrics_and_labels_dict[base_name]['type']
            except:
                metric_type = None
            if metric_type:
                continue
            try:
                metric_type = metric_type_redis_keys[metric_type_redis_key][metrics_and_labels_dict[base_name]['metric_name']]
                if metric_type:
                    metrics_and_labels_dict[base_name]['type'] = metric_type
            except:
                # Do not default to COUNTER
                # metrics_and_labels_dict[base_name]['type'] = 'COUNTER'
                metrics_and_labels_dict[base_name]['type'] = None
        logger.info('analyzer_labelled_metrics :: memory usage after determining metric types from metric_type_redis_keys - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))

        fetch_new_metrics = False
        no_id_metrics = []

        while 1:
            now = time()

            logger.info('analyzer_labelled_metrics :: memory usage at beginning of run - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: analyzer_labelled_metrics :: cannot ping Redis at socket path %s, reconnect will be attempted in 10 seconds - %s' % (settings.REDIS_SOCKET_PATH, err))
                sleep(10)
                try:
                    self.redis_conn = get_redis_conn(skyline_app)
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: analyzer_labelled_metrics :: cannot connect to get_redis_conn - %s' % err)
                continue

            # Report app up
            try:
                # Report app AND Redis as up
                redis_is_up = self.redis_conn.setex(skyline_app_thunder_key, 120, now)
                if redis_is_up:
                    try:
                        self.redis_conn.setex('redis', 120, now)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: analyzer_labelled_metrics :: could not update the Redis redis key - %s' % (
                            err))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: analyzer_labelled_metrics :: could not update the Redis %s key - %s' % (
                    skyline_app, err))

            # Discover unique labelled_metrics
            try:
                unique_labelled_metrics = list(self.redis_conn_decoded.smembers('labelled_metrics.unique_labelled_metrics'))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: analyzer_labelled_metrics :: could not get the unique_labelled_metrics list from labelled_metrics.unique_labelled_metrics Redis set - %s' % err)
                sleep(10)
                continue

            unique_labelled_metrics_count = len(unique_labelled_metrics)
            if unique_labelled_metrics_count == 0:
                logger.info('analyzer_labelled_metrics :: no labelled_metrics in redis try adding some')
                sleep(10)
                continue

            # If there are ANALYZER_SKIP metrics declare shuffle the metrics
            # so that the load is distributed evenly between the processes
            # rather than 1 process getting all the metrics to skip
            if ANALYZER_SKIP and ANALYZER_LABELLED_METRICS_PROCESSES > 1:
                logger.info('analyzer_labelled_metrics :: ANALYZER_SKIP set shuffling unique_labelled_metrics')
                shuffle(unique_labelled_metrics)

            logger.info('analyzer_labelled_metrics :: memory usage before creating metrics_last_timeseries_timestamp_dict - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
            metrics_last_timeseries_timestamp_dict = {}
            metrics_last_timeseries_timestamp_hash_key = 'analyzer_labelled_metrics.last_timeseries_timestamp'
            try:
                metrics_last_timeseries_timestamp_dict = self.redis_conn_decoded.hgetall(metrics_last_timeseries_timestamp_hash_key)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: analyzer_labelled_metrics :: failed to create metrics_last_timeseries_timestamp_dict from Redis hash key %s - %s' % (
                    metrics_last_timeseries_timestamp_hash_key, err))
            logger.info('analyzer_labelled_metrics :: memory usage after creating metrics_last_timeseries_timestamp_dict - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))

            # @added 20230517 - Bug #4766: analyzer_labelled_metrics - monotonicity checked incorrect classification
            # If any metrics have changed monotonicity update the metric_type
            update_labelled_metrics_id_type = False
            try:
                update_labelled_metrics_id_type = self.redis_conn_decoded.exists('skyline.labelled_metrics.id.type.changed')
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: analyzer_labelled_metrics :: exists failed on Redis key skyline.labelled_metrics.id.type.changed - %s' % (
                    err))
            if update_labelled_metrics_id_type:
                logger.info('analyzer_labelled_metrics :: skyline.labelled_metrics.id.type.changed exists reloading skyline.labelled_metrics.id.type to use new metric_type from monotonicity changes')
                try:
                    self.redis_conn_decoded.delete('skyline.labelled_metrics.id.type.changed')
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: analyzer_labelled_metrics :: failed to delete skyline.labelled_metrics.id.type.changed key from Redis - %s' % (
                        err))
                logger.info('analyzer_labelled_metrics :: memory usage before updating labelled_metrics_id_types because skyline.labelled_metrics.id.type.changed exists - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
                try:
                    labelled_metrics_id_types = self.redis_conn_decoded.hgetall('skyline.labelled_metrics.id.type')
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: analyzer_labelled_metrics :: failed to hgetall skyline.labelled_metrics.id.type Redis hash key - %s' % (
                        err))
                logger.info('analyzer_labelled_metrics :: memory usage after creating labelled_metrics_id_types - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
                errors = []
                for metric_id_str in list(labelled_metrics_id_types.keys()):
                    base_name = None
                    try:
                        base_name = metric_ids_with_name[metric_id_str]
                    except:
                        continue
                    metric_type = None
                    in_labelled_metrics_id_types = False
                    try:
                        metric_type_id_str = labelled_metrics_id_types[str(metric_id_str)]
                        if metric_type_id_str:
                            in_labelled_metrics_id_types = True
                            if metric_type_id_str == '1':
                                metric_type = 'COUNTER'
                            if metric_type_id_str == '0':
                                metric_type = 'GAUGE'
                    except:
                        metric_type = None
                    if metric_type:
                        try:
                            metrics_and_labels_dict[base_name]['type'] = metric_type
                        except Exception as err:
                            err_msg = '%s - %s' % (err, traceback.format_exc())
                            errors.append([labelled_metric, 'adding metric type to metrics_and_labels_dict', str(err_msg)])
                    try:
                        metrics_and_labels_dict[base_name]['in_labelled_metrics_id_types'] = in_labelled_metrics_id_types
                    except Exception as err:
                        err_msg = '%s - %s' % (err, traceback.format_exc())
                        errors.append([labelled_metric, 'adding in_labelled_metrics_id_types to metrics_and_labels_dict', str(err_msg)])

            if len(list(metrics_and_labels_dict.keys())) < unique_labelled_metrics_count:
                fetch_new_metrics = True

            if no_id_metrics:
                fetch_new_metrics = True

            if fetch_new_metrics:
                try:
                    metric_names_with_ids = self.redis_conn_decoded.hgetall(metric_names_with_ids_key)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: analyzer_labelled_metrics :: failed to hgetall aet.metrics_manager.metric_names_with_ids Redis hash key %s - %s' % (
                        err))
                try:
                    metric_ids_with_name = self.redis_conn_decoded.hgetall(metric_ids_with_metric_key)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: analyzer_labelled_metrics :: failed to hgetall aet.metrics_manager.active_labelled_ids_with_metric Redis hash key %s - %s' % (
                        err))
                if not metric_ids_with_name:
                    for base_name in list(metric_names_with_ids.keys()):
                        metric_id = metric_names_with_ids[base_name]
                        metric_ids_with_name[metric_id] = base_name

            if fetch_new_metrics:
                new_metrics = []
                metric_type_redis_keys_list = []
                errors = []
                for labelled_metric in unique_labelled_metrics:
                    metric_id = labelled_metric.replace('labelled_metrics.', '', 1)
                    try:
                        base_name = metric_ids_with_name[str(metric_id)]
                    except:
                        continue
                    # base_name = labelled_metric.replace('labelled_metrics.', '', 1)
                    metric_dict = {}
                    try:
                        metric_dict = metrics_and_labels_dict[base_name]
                    except:
                        metric_dict = {}
                    metric_type = None
                    try:
                        metric_type = metric_dict['type']
                    except:
                        metric_type = None
                    if not metric_type:
                        try:
                            # metric_dict = metric_name_labels_parser(skyline_app, base_name)
                            metric_dict = self.metric_name_labels_parser(base_name)
                            if metric_dict:
                                try:
                                    metrics_and_labels_dict[base_name] = {}
                                    metrics_and_labels_dict[base_name] = metric_dict
                                    tenant_id = metric_dict['labels']['_tenant_id']
                                    server_id = metric_dict['labels']['_server_id']
                                    metric_type_redis_key = 'metrics_manager.prometheus.metrics_type.%s.%s' % (str(tenant_id), str(server_id))
                                    metric_type_redis_keys_list.append(metric_type_redis_key)
                                    new_metrics.append(base_name)
                                    # @added 20230404 - Feature #4890: analyzer_labelled_metrics - use mrange
                                    # Added filters
                                    try:
                                        tenant_ids_with_count[tenant_id] += 1
                                        tenant_ids_base_names[tenant_id].append(base_name)
                                        tenant_ids_labelled_metrics[tenant_id].append(labelled_metric)
                                    except:
                                        tenant_ids_with_count[tenant_id] = 1
                                        tenant_ids_base_names[tenant_id] = [base_name]
                                        tenant_ids_labelled_metrics[tenant_id] = [labelled_metric]
                                except:
                                    pass
                        except Exception as err:
                            err_msg = '%s - %s' % (err, traceback.format_exc())
                            errors.append([labelled_metric, 'metric_name_labels_parser', str(err_msg)])
                if errors:
                    logger.error('error :: analyzer_labelled_metrics :: metric_name_labels_parser encountered %s errors, sample: %s' % (
                        str(len(errors)), str(errors[0])))

                metric_type_redis_keys_list = list(set(metric_type_redis_keys_list))
                errors = []
                for metric_type_redis_key in metric_type_redis_keys_list:
                    if metric_type_redis_key not in metric_type_redis_keys:
                        metric_type_redis_key_dict = None
                        try:
                            metric_type_redis_key_dict = self.redis_conn_decoded.hgetall(metric_type_redis_key)
                        except Exception as err:
                            err_msg = 'hgetall %s' % metric_type_redis_key
                            errors.append([metric_type_redis_key, err_msg, str(err)])
                        metric_type_redis_keys[metric_type_redis_key] = metric_type_redis_key_dict
                if errors:
                    logger.error('error :: analyzer_labelled_metrics :: metric_name_labels_parser encountered %s errors, sample: %s' % (
                        str(len(errors)), str(errors[0])))
                for base_name in new_metrics:
                    try:
                        metric_type = metric_type_redis_keys[metric_type_redis_key][metrics_and_labels_dict[base_name]['metric_name']]
                        if metric_type:
                            metrics_and_labels_dict[base_name]['type'] = metric_type
                    except:
                        # metrics_and_labels_dict[base_name]['type'] = 'COUNTER'
                        metrics_and_labels_dict[base_name]['type'] = None

            logger.info('analyzer_labelled_metrics :: memory usage before creating stationary_metrics_dict - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
            stationary_metrics_dict = {}
            stationary_metrics_hash_key = 'analyzer_labelled_metrics.stationary_metrics'
            try:
                stationary_metrics_dict = self.redis_conn_decoded.hgetall(stationary_metrics_hash_key)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: analyzer_labelled_metrics :: failed to hgetall %s Redis hash key - %s' % (
                    stationary_metrics_hash_key, err))
            logger.info('analyzer_labelled_metrics :: memory usage after creating stationary_metrics_dict - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))

            # Discover assigned metrics
            keys_per_processor = int(ceil(float(unique_labelled_metrics_count) / float(ANALYZER_LABELLED_METRICS_PROCESSES)))

            # @added 20230404 - Feature #4890: analyzer_labelled_metrics - use mrange
            # Added filters
            try:
                filters_dict = get_tenant_id_mrange_split(self, ANALYZER_LABELLED_METRICS_PROCESSES, tenant_ids_with_count)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: analyzer_labelled_metrics :: get_tenant_id_mrange_split failed - %s' % err)
                filters_dict = {}
            logger.info('analyzer_labelled_metrics :: filters_dict: %s' % str(filters_dict))

            no_id_metrics = []
            # @added 20230123 - Task #2732: Prometheus to Skyline
            #                   Branch #4300: prometheus
            # all_db_base_names = {}
            # all_db_base_names_with_ids = {}
            # all_db_ids_with_base_names = {}

            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            for i_process in range(1, ANALYZER_LABELLED_METRICS_PROCESSES + 1):
                if i_process > unique_labelled_metrics_count:
                    logger.warning('warning :: analyzer_labelled_metrics :: skyline is set for more cores than needed.')

                logger.info('analyzer_labelled_metrics :: memory usage before starting process - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
                if i_process == ANALYZER_LABELLED_METRICS_PROCESSES:
                    assigned_max = unique_labelled_metrics_count
                else:
                    assigned_max = min(unique_labelled_metrics_count, i_process * keys_per_processor)
                # Fix analyzer worker metric assignment #94
                # https://github.com/etsy/skyline/pull/94 @languitar:worker-fix
                assigned_min = (i_process - 1) * keys_per_processor
                assigned_keys = range(assigned_min, assigned_max)

                # Compile assigned metrics
                error_logged = False
                assigned_metrics = [unique_labelled_metrics[index] for index in assigned_keys]

                # @added 20230404 - Feature #4890: analyzer_labelled_metrics - use mrange
                # Added filters
                assigned_filter_metrics = []
                filters_list = []
                if filters_dict:
                    filter_key = i_process - 1
                    try:
                        filters_list = filters_dict[filter_key]
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: analyzer_labelled_metrics :: failed to determine filters_dict[%s] from filters_dict - %s' % (
                            str(filter_key), err))
                        filters_list = []
                if filters_list:
                    for tenant_id in filters_list:
                        try:
                            assigned_filter_metrics = assigned_filter_metrics + tenant_ids_labelled_metrics[tenant_id]
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: analyzer_labelled_metrics :: failed metrics for tenant_id %s from tenant_ids_labelled_metrics - %s' % (
                                str(tenant_id), err))
                if assigned_filter_metrics:
                    assigned_metrics = list(set(assigned_filter_metrics))
                    del assigned_filter_metrics

                del assigned_keys
                assigned_metrics_dict = {}
                for labelled_metric in assigned_metrics:
                    try:
                        # base_name = labelled_metric.replace('labelled_metrics.', '', 1)
                        # metric_id = None
                        metric_id = labelled_metric.replace('labelled_metrics.', '', 1)
                        base_name = None
                        no_metric_id = True
                        try:
                            base_name = metric_ids_with_name[metric_id]
                            no_metric_id = False
                        except:
                            # no_id_metrics.append(metric_id)
                            # continue
                            no_metric_id = True
                        # try:
                        #     metric_id = metric_names_with_ids[base_name]
                        #     no_metric_id = False
                        # except:
                            # no_id_metrics.append(metric_id)
                            # continue
                        #    no_metric_id = True
                        if no_metric_id:
                            no_id_metrics.append(metric_id)
                            continue

                        try:
                            last_timestamp = int(str(metrics_last_timeseries_timestamp_dict[metric_id]))
                        except:
                            last_timestamp = 0
                        try:
                            stationary_str = stationary_metrics_dict[str(metric_id)]
                            if stationary_str:
                                stationary_str_elements = stationary_str.split(',')
                                stationary_value = False
                                if str(stationary_str_elements[0]) == 'True':
                                    stationary_value = True
                                stationary_dict = {'v': stationary_value, 'ts': int(stationary_str_elements[1])}
                        except:
                            stationary_dict = {'v': None, 'last_ts': 0}
                        metric_dict = {}
                        try:
                            metric_dict = metrics_and_labels_dict[base_name]
                        except:
                            metric_dict = {}
                        assigned_metrics_dict[base_name] = {
                            'id': metric_id,
                            'last_ts': last_timestamp,
                            'stationary': stationary_dict,
                            'metric_dict': metric_dict,
                        }
                    except Exception as err:
                        if not error_logged:
                            logger.error(traceback.format_exc())
                            logger.error('error :: analyzer_labelled_metrics :: failed to add %s details to assigned_metrics_dict - %s' % (
                                labelled_metric, err))
                        error_logged = True
                del assigned_metrics
                try:
                    # @modified 20230404 - Feature #4890: analyzer_labelled_metrics - use mrange
                    # Added filters
                    # p = Process(target=self.labelled_metrics_spin_process, args=(i_process, assigned_metrics_dict))
                    p = Process(target=self.labelled_metrics_spin_process, args=(i_process, assigned_metrics_dict, filters_list))
                    pids.append(p)
                    pid_count += 1
                    logger.info('analyzer_labelled_metrics :: starting %s of %s labelled_metrics_spin_process/es with filters_list: %s' % (
                        str(pid_count), str(ANALYZER_LABELLED_METRICS_PROCESSES),
                        str(filters_list)))
                    p.start()
                    started_pid = p.pid
                    spawned_pids.append(started_pid)
                    logger.info('analyzer_labelled_metrics :: started labelled_metrics_spin_process with pid %s' % (
                        str(started_pid)))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: analyzer_labelled_metrics :: failed to spawn process - %s' % err)
                del assigned_metrics_dict
                logger.info('analyzer_labelled_metrics :: memory usage after starting process - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))

            del unique_labelled_metrics
            del metrics_last_timeseries_timestamp_dict
#            del metric_names_with_ids
            del stationary_metrics_dict
            logger.info('analyzer_labelled_metrics :: memory usage after starting processes - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))

            if no_id_metrics:
                logger.info('analyzer_labelled_metrics :: no_id_metrics count: %s' % (
                    str(len(no_id_metrics))))
                no_id_metrics_key = 'analyzer_labelled_metrics.no_id_metrics.%s' % str(now)
                try:
                    self.redis_conn.sadd(no_id_metrics_key, *set(no_id_metrics))
                    self.redis_conn.expire(no_id_metrics_key, 300)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: analyzer_labelled_metrics :: failed to sadd %s Redis set - %s' % (
                        no_id_metrics_key, err))

            # Send wait signal to zombie processes
            logger.info('analyzer_labelled_metrics :: started pids: %s' % (
                str(spawned_pids)))
            p_starts = time()
            while time() - p_starts <= MAX_ANALYZER_LABELLED_METRICS_PROCESS_RUNTIME:
                if any(p.is_alive() for p in pids):
                    # Just to avoid hogging the CPU
                    sleep(.1)
                else:
                    # All the processes are done, break now.
                    time_to_run = time() - p_starts
                    logger.info('analyzer_labelled_metrics :: %s labelled_metrics_spin_process/es completed in %.2f seconds' % (
                        str(ANALYZER_LABELLED_METRICS_PROCESSES), time_to_run))
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('analyzer_labelled_metrics :: timed out, killing all labelled_metrics_spin_process processes')
                for p in pids:
                    logger.info('analyzer_labelled_metrics :: killing labelled_metrics_spin_process process')
                    p.terminate()
                    # p.join()
                    logger.info('analyzer_labelled_metrics :: killed labelled_metrics_spin_process process')
            for p in pids:
                if p.is_alive():
                    logger.info('analyzer_labelled_metrics :: sending SIGKILL to pid %s labelled_metrics_spin_process - %s' % (
                        str(p.pid), str(p.is_alive())))
                    try:
                        os.kill(p.pid, SIGKILL)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: analyzer_labelled_metrics :: SIGKILL failed - %s' % err)
                    # p.join()

            # Log the last reported error by any algorithms that errored in the
            # spawned processes from algorithms.py
            for completed_pid in spawned_pids:
                logger.info('analyzer_labelled_metrics :: labelled_metrics_pin_process with pid %s completed' % (str(completed_pid)))

            logger.info('analyzer_labelled_metrics :: memory usage after completed processes - %s' % str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))

            # Grab data from the queue and populate dictionaries
            exceptions = {}
            anomaly_breakdown = {}
            while 1:
                try:
                    key, value = self.anomaly_breakdown_q.get_nowait()
                    if key not in anomaly_breakdown:
                        anomaly_breakdown[key] = value
                    else:
                        anomaly_breakdown[key] += value
                except Empty:
                    break

            while 1:
                try:
                    key, value = self.exceptions_q.get_nowait()
                    if key not in exceptions:
                        exceptions[key] = value
                    else:
                        exceptions[key] += value
                except Empty:
                    break

            exceptions_metrics = ['Boring', 'Stale', 'TooShort', 'Other', 'EmptyTimeseries', 'NoNewData']
            for i_exception in exceptions_metrics:
                if i_exception not in exceptions:
                    exceptions[i_exception] = 0
                    # @added 20230309 - Feature #4864: analyzer_labelled_metrics - exceptions metrics set
                    if i_exception == 'TooShort':
                        try:
                            self.redis_conn_decoded.delete('aet.analyzer_labelled_metrics.tooshort')
                            logger.info('deleted Redis set aet.analyzer_labelled_metrics.tooshort as no tooshort exceptions')
                        except Exception as err:
                            logger.error('error :: labelled_metrics_spin_process :: failed to delete Redis set aet.analyzer_labelled_metrics.tooshort - %s' % (
                                err))
                    if i_exception == 'Stale':
                        try:
                            self.redis_conn_decoded.delete('aet.analyzer_labelled_metrics.stale')
                            logger.info('deleted Redis set aet.analyzer_labelled_metrics.stale as no stale exceptions')
                        except Exception as err:
                            logger.error('error :: labelled_metrics_spin_process :: failed to delete Redis set aet.analyzer_labelled_metrics.stale - %s' % (
                                err))
                    if i_exception == 'Boring':
                        try:
                            self.redis_conn_decoded.delete('aet.analyzer_labelled_metrics.boring')
                            logger.info('deleted Redis set aet.analyzer_labelled_metrics.boring as no boring exceptions')
                        except Exception as err:
                            logger.error('error :: labelled_metrics_spin_process :: failed to delete Redis set aet.analyzer_labelled_metrics.boring - %s' % (
                                err))
                else:
                    # @added 20230309 - Feature #4864: analyzer_labelled_metrics - exceptions metrics set
                    if i_exception == 'TooShort':
                        try:
                            self.redis_conn_decoded.rename('analyzer_labelled_metrics.tooshort', 'aet.analyzer_labelled_metrics.tooshort')
                            logger.info('renamed Redis set analyzer_labelled_metrics.tooshort to aet.analyzer_labelled_metrics.tooshort')
                        except Exception as err:
                            logger.error('error :: labelled_metrics_spin_process :: failed to rename Redis set analyzer_labelled_metrics.tooshort to aet.analyzer_labelled_metrics.tooshort - %s' % (
                                err))
                    if i_exception == 'Stale':
                        try:
                            self.redis_conn_decoded.rename('analyzer_labelled_metrics.stale', 'aet.analyzer_labelled_metrics.stale')
                            logger.info('renamed Redis set analyzer_labelled_metrics.stale to aet.analyzer_labelled_metrics.stale')
                        except Exception as err:
                            logger.error('error :: labelled_metrics_spin_process :: failed to rename Redis set analyzer_labelled_metrics.stale to aet.analyzer_labelled_metrics.stale - %s' % (
                                err))
                    if i_exception == 'Boring':
                        try:
                            self.redis_conn_decoded.rename('analyzer_labelled_metrics.boring', 'aet.analyzer_labelled_metrics.boring')
                            logger.info('renamed Redis set analyzer_labelled_metrics.boring to aet.analyzer_labelled_metrics.boring')
                        except Exception as err:
                            logger.error('error :: labelled_metrics_spin_process :: failed to rename Redis set analyzer_labelled_metrics.boring to aet.analyzer_labelled_metrics.boring - %s' % (
                                err))

            # @added 20200603 - Feature #3566: custom_algorithms
            anomaly_breakdown_algorithms = list(settings.ALGORITHMS)
            if CUSTOM_ALGORITHMS:
                for custom_algorithm in settings.CUSTOM_ALGORITHMS:
                    anomaly_breakdown_algorithms.append(custom_algorithm)

            stats = {}
            while 1:
                try:
                    key, value = self.stats_q.get_nowait()
                    if key not in stats:
                        stats[key] = value
                    else:
                        stats[key] += value
                except Empty:
                    break

            # @modified 20200603 - Feature #3566: custom_algorithms
            # for i_anomaly_breakdown in settings.ALGORITHMS:
            for i_anomaly_breakdown in anomaly_breakdown_algorithms:
                if i_anomaly_breakdown not in anomaly_breakdown:
                    anomaly_breakdown[i_anomaly_breakdown] = 0

            # Set the anomaly_end_timestamp
            not_anomalous_metric_ids = []
            not_anomalous_metrics_data = []
            redis_set = 'current.anomalies'
            try:
                current_anomalies = list(self.redis_conn_decoded.smembers(redis_set))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: analyzer_labelled_metrics :: failed to get Redis set %s - %s' % (redis_set, err))
                current_anomalies = []
            if current_anomalies:
                try:
                    redis_set = 'analyzer_labelled_metrics.not_anomalous_metrics'
                    not_anomalous_metrics = self.redis_conn_decoded.smembers(redis_set)
                except:
                    not_anomalous_metrics = []
                for item in not_anomalous_metrics:
                    try:
                        list_data = literal_eval(item)
                        not_anomalous_metrics_data.append(list_data)
                        not_anomalous_metric_ids.append(int(list_data[0]))
                    except:
                        pass
                for item in current_anomalies:
                    try:
                        list_data = literal_eval(str(item))
                        anomalous_metric = str(list_data[0])
                        if '_tenant_id="' not in anomalous_metric:
                            continue
                        anomaly_timestamp = int(list_data[1])
                        # @added 20200608 - Feature #3306: Record anomaly_end_timestamp
                        # Remove entries from the current.anomalies set if the
                        # timestamp is older than FULL_DURATION
                        if anomaly_timestamp < (now - settings.FULL_DURATION):
                            redis_set = 'current.anomalies'
                            try:
                                self.redis_conn.srem(redis_set, str(list_data))
                                logger.info('analyzer_labelled_metrics :: removed %s from Redis set %s as the anomaly_timestamp is older than FULL_DURATION - %s' % (
                                    anomalous_metric, redis_set, str(list_data)))
                                continue
                            except Exception as err:
                                logger.error(traceback.format_exc())
                                logger.error('error :: analyzer_labelled_metrics :: failed to remove %s for Redis set %s - %s' % (
                                    str(list_data), redis_set, err))
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
                        logger.error('error :: analyzer_labelled_metrics :: failed to determine current_anomalies item')
                    if anomaly_end_timestamp:
                        continue
                    if not anomaly_id:
                        continue
                    anomalous_metric_id = 0
                    try:
                        anomalous_metric_id = get_metric_id_from_base_name(skyline_app, anomalous_metric)
                    except Exception as err:
                        logger.error('error :: analyzer_labelled_metrics :: get_metric_id_from_base_name failed for %s - %s' % (
                            str(anomalous_metric), err))

                    if int(anomalous_metric_id) in not_anomalous_metric_ids:
                        for metric_id, anomaly_end_timestamp in not_anomalous_metrics_data:
                            if int(anomalous_metric_id) == int(metric_id):
                                update_item = False
                                redis_set = 'current.anomalies'
                                try:
                                    self.redis_conn.srem(redis_set, str(list_data))
                                    update_item = True
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: analyzer_labelled_metrics :: failed to remove %s for Redis set %s' % (str(list_data), redis_set))
                                if update_item:
                                    new_list_data = [anomalous_metric, anomaly_timestamp, anomaly_id, anomaly_end_timestamp]
                                    try:
                                        redis_set = 'current.anomalies'
                                        self.redis_conn.sadd(redis_set, str(new_list_data))
                                        logger.info('analyzer_labelled_metrics :: set anomaly_end_timestamp to %s for %s in Redis set %s' % (
                                            str(anomaly_end_timestamp), anomalous_metric, redis_set))
                                    except Exception as err:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: analyzer_labelled_metrics :: failed to add %s to Redis set %s - %s' % (str(new_list_data), redis_set, err))

            not_anomalous_count = 0
            total_anomalies = 0
            redis_set = 'analyzer_labelled_metrics.not_anomalous_metrics'
            try:
                not_anomalous_count = self.redis_conn_decoded.scard(redis_set)
            except Exception as err:
                logger.error('error :: analyzer_labelled_metrics :: failed to determine not_anomalous_count from %s - %s' % (redis_set, err))
                not_anomalous_count = 0
            try:
                self.redis_conn.delete('aet.analyzer_labelled_metrics.not_anomalous_metrics')
            except:
                pass
            try:
                self.redis_conn.rename('analyzer_labelled_metrics.not_anomalous_metrics', 'aet.analyzer_labelled_metrics.not_anomalous_metrics')
            except:
                pass
            redis_set = 'analyzer_labelled_metrics.anomalous_metrics'
            try:
                total_anomalies = self.redis_conn_decoded.scard(redis_set)
            except Exception as err:
                logger.error('error :: analyzer_labelled_metrics :: failed to determine anomalous_count from %s - %s' % (redis_set, err))
                total_anomalies = 0
            try:
                self.redis_conn.delete('aet.analyzer_labelled_metrics.anomalous_metrics')
            except:
                pass
            try:
                self.redis_conn.rename('analyzer_labelled_metrics.anomalous_metrics', 'aet.analyzer_labelled_metrics.anomalous_metrics')
            except:
                pass

            # @added 20230425 - Feature #4894: labelled_metrics - SKYLINE_FEEDBACK_NAMESPACES
            # Create busy key to enable feedback_labelled_metrics
            if total_anomalies >= 50:
                try:
                    self.redis_conn_decoded.setex('analyzer_labelled_metrics.busy', 120, total_anomalies)
                    logger.info('analyzer_labelled_metrics :: created analyzer_labelled_metrics.busy key')
                except Exception as err:
                    logger.error('error :: analyzer_labelled_metrics :: failed to setex analyzer_labelled_metrics.busy - %s' % err)
                    not_anomalous_count = 0

            total_errors = 0
            current_aligned_ts = int(int(p_starts) // 60 * 60)
            error_set = 'analyzer_labelled_metrics.errors.%s' % str(current_aligned_ts)
            try:
                total_errors = self.redis_conn_decoded.scard(error_set)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: labelled_metrics_spin_process :: failed to scard %s Redis set - %s' % (
                    error_set, err))

            total_analyzed = not_anomalous_count + total_anomalies
            run_time = time() - p_starts
            # Log progress
            logger.info('analyzer_labelled_metrics :: seconds to run     :: %.2f' % run_time)
            logger.info('analyzer_labelled_metrics :: total metrics      :: %s' % str(unique_labelled_metrics_count))
            logger.info('analyzer_labelled_metrics :: total analyzed     :: %s' % total_analyzed)
            logger.info('analyzer_labelled_metrics :: total anomalies    :: %s' % total_anomalies)
            logger.info('analyzer_labelled_metrics :: total errors       :: %s' % total_errors)
            logger.info('analyzer_labelled_metrics :: exception stats    :: %s' % exceptions)
            logger.info('analyzer_labelled_metrics :: anomaly breakdown  :: %s' % anomaly_breakdown)
            logger.info('analyzer_labelled_metrics :: stats              :: %s' % stats)

            send_metric_name = '%s.labelled_metrics.run_time' % (skyline_app_graphite_namespace)
            run_time_str = '%.2f' % run_time
            try:
                send_graphite_metric(self, skyline_app, send_metric_name, run_time_str)
            except Exception as err:
                logger.error('error :: analyzer_labelled_metrics :: could not send send_graphite_metric %s %s: %s' % (
                    send_metric_name, run_time_str, err))

            # @added 20230404 - Feature #4888: analyzer - load_shedding
            cache_key = 'analyzer_labelled_metrics.run_time'
            try:
                self.redis_conn_decoded.hset('analyzer_labelled_metrics.run_time', 'value', float(run_time))
                self.redis_conn_decoded.hset('analyzer_labelled_metrics.run_time', 'timestamp', int(time()))
            except Exception as err:
                logger.error('error :: Analyzer could not update the Redis analyzer_labelled_metrics.run_time hash - %s' % (
                    err))


            for stat in list(stats.keys()):
                stat_name = stat.replace(' ', '_')
                send_metric_name = '%s.labelled_metrics.%s' % (
                    skyline_app_graphite_namespace, stat_name)
                try:
                    send_graphite_metric(self, skyline_app, send_metric_name, str(stats[stat]))
                except Exception as err:
                    logger.error('error :: analyzer_labelled_metrics :: could not send send_graphite_metric %s %s: %s' % (
                        send_metric_name, str(stats[stat]), err))
            logger.info('analyzer_labelled_metrics :: sent Graphite %s stats metrics' % str(len(stats)))
            for exception in list(exceptions.keys()):
                send_metric_name = '%s.labelled_metrics.exceptions.%s' % (
                    skyline_app_graphite_namespace, exception)
                try:
                    send_graphite_metric(self, skyline_app, send_metric_name, str(exceptions[exception]))
                except Exception as err:
                    logger.error('error :: analyzer_labelled_metrics :: could not send send_graphite_metric %s %s: %s' % (
                        send_metric_name, str(exceptions[exception]), err))
            logger.info('analyzer_labelled_metrics :: sent Graphite %s exceptions metrics' % str(len(exceptions)))

            unique_labelled_metrics = []

            mem_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            logger.info('analyzer_labelled_metrics :: memory usage after completed run - %s' % str(mem_usage))
            send_metric_name = '%s.labelled_metrics.mem_usage' % skyline_app_graphite_namespace
            try:
                send_graphite_metric(self, skyline_app, send_metric_name, str(mem_usage))
            except Exception as err:
                logger.error('error :: analyzer_labelled_metrics :: could not send send_graphite_metric %s %s: %s' % (
                    send_metric_name, str(mem_usage), err))

            process_runtime = time() - now
            analyzer_optimum_run_duration = settings.ANALYZER_OPTIMUM_RUN_DURATION
            if process_runtime < analyzer_optimum_run_duration:
                sleep_for = (analyzer_optimum_run_duration - process_runtime)
                logger.info('analyzer_labelled_metrics :: completed in %.2f seconds, sleeping for %.2f seconds due to low run time...' % (
                    (time() - now), sleep_for))
                sleep(sleep_for)
                try:
                    del sleep_for
                except Exception as err:
                    logger.error('error :: analyzer_labelled_metrics :: failed to del sleep_for - %s' % err)
            try:
                del process_runtime
            except Exception as err:
                logger.error('error :: analyzer_labelled_metrics :: failed to del process_runtime - %s' % err)
