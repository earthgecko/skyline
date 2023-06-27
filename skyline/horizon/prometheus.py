"""
prometheus.py
"""
from os import kill, getpid
# try:
#     from Queue import Full
# except ImportError:
#     from queue import Full
from multiprocessing import Process
import sys
from time import time, sleep
from timeit import default_timer as timer
from ast import literal_eval
import traceback
import logging
import os.path
from os import remove as os_remove
import json
import gzip
# @added 20220722 - Task #4624: Change all dict copy to deepcopy
import copy
from contextlib import nullcontext

import zlib
import requests
import memray

import settings
# @modified 20220726 - Task #2732: Prometheus to Skyline
#                      Branch #4300: prometheus
# Moved send_graphite_metric
# from skyline_functions import (get_redis_conn_decoded, send_graphite_metric)
from skyline_functions import get_redis_conn_decoded
from functions.prometheus.metric_name_labels_parser import metric_name_labels_parser
# @added 20220726 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.graphite.send_graphite_metric import send_graphite_metric

# @added 20230123 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.database.queries.get_all_db_metric_names import get_all_db_metric_names

parent_skyline_app = 'horizon'
child_skyline_app = 'prometheus'
skyline_app_logger = '%sLog' % parent_skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app = '%s.%s' % (parent_skyline_app, child_skyline_app)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, parent_skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

try:
    HORIZON_PROMETHEUS_METRIC_LIMIT = settings.HORIZON_PROMETHEUS_METRIC_LIMIT
except AttributeError:
    HORIZON_PROMETHEUS_METRIC_LIMIT = settings.CHUNK_SIZE

try:
    # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
    # HORIZON_SHARDS = settings.HORIZON_SHARDS.copy()
    HORIZON_SHARDS = copy.deepcopy(settings.HORIZON_SHARDS)
except:
    HORIZON_SHARDS = {}
try:
    HORIZON_SHARD_DEBUG = settings.HORIZON_SHARD_DEBUG
except:
    HORIZON_SHARD_DEBUG = True

try:
    VICTORIAMETRICS_ENABLED = settings.VICTORIAMETRICS_ENABLED
except:
    VICTORIAMETRICS_ENABLED = False
vm_url = None
if VICTORIAMETRICS_ENABLED:
    # For cluster path, default to VictoriaMetrics standalone path
    try:
        jsonl_insert_path = settings.VICTORIAMETRICS_OPTS['jsonl_insert_path']
    except:
        jsonl_insert_path = '/api/v1/import'
    try:
        if settings.VICTORIAMETRICS_OPTS['scheme'] == 'https':
            vm_url = '%s://%s%s' % (
                settings.VICTORIAMETRICS_OPTS['scheme'],
                settings.VICTORIAMETRICS_OPTS['host'],
                jsonl_insert_path
            )
        else:
            vm_url = '%s://%s:%s%s' % (
                settings.VICTORIAMETRICS_OPTS['scheme'],
                settings.VICTORIAMETRICS_OPTS['host'],
                str(settings.VICTORIAMETRICS_OPTS['port']),
                jsonl_insert_path
            )
    except:
        vm_url = None

try:
    MEMRAY_ENABLED = settings.MEMRAY_ENABLED
except:
    MEMRAY_ENABLED = False

number_of_horizon_shards = 0
this_host = str(os.uname()[1])

if this_host == 'skyline-test-1-fra1':
    DEVELOPMENT = True
    HORIZON_SHARDS = {
        'skyline-test-1-fra1': 0,
        'another-test-node-1': 1,
        'another-test-node-2': 2,
    }
else:
    DEVELOPMENT = False


HORIZON_SHARD = 0
if HORIZON_SHARDS:
    number_of_horizon_shards = len(HORIZON_SHARDS)
    HORIZON_SHARD = HORIZON_SHARDS[this_host]

skyline_app_graphite_namespace = 'skyline.%s%s.%s' % (
    parent_skyline_app, SERVER_METRIC_PATH, child_skyline_app)

LOCAL_DEBUG = False


def create_global_reload_skip_dict():
    """ skip_dict in shared memory """
    global global_reload_skip_dict
    global_reload_skip_dict = False


def get_global_reload_skip_dict():
    """ Reload shared memory skip_dict """
    return global_reload_skip_dict


def update_global_reload_skip_dict(value):
    """ Update shared memory skip_dict """
    global global_reload_skip_dict
    global_reload_skip_dict = value
    return global_reload_skip_dict


create_global_reload_skip_dict()


def create_global_skip_dict():
    global global_skip_dict
    global_skip_dict = {}


def get_global_skip_dict():
    return global_skip_dict


def update_global_skip_dict(skip_dict):
    global global_skip_dict
    global_skip_dict = skip_dict
    return global_skip_dict


create_global_skip_dict()

def create_global_prom_to_vm_metrics_dict():
    global global_prom_to_vm_metrics_dict
    global_prom_to_vm_metrics_dict = {}


def get_global_prom_to_vm_metrics_dict():
    return global_prom_to_vm_metrics_dict


def update_global_prom_to_vm_metrics_dict(prom_to_vm_metrics_dict):
    global global_prom_to_vm_metrics_dict
    global_prom_to_vm_metrics_dict = prom_to_vm_metrics_dict
    return global_prom_to_vm_metrics_dict


create_global_prom_to_vm_metrics_dict()


def create_global_shard_metrics_dict():
    global global_shard_metrics_dict
    global_shard_metrics_dict = {}


def get_global_shard_metrics_dict():
    return global_shard_metrics_dict


def update_global_shard_metrics_dict(prom_to_vm_metrics_dict):
    global global_shard_metrics_dict
    global_shard_metrics_dict = prom_to_vm_metrics_dict
    return global_shard_metrics_dict


create_global_shard_metrics_dict()


def create_global_metrics_type_dict():
    global global_metrics_type_dict
    global_metrics_type_dict = {}


def get_global_metrics_type_dict():
    return global_metrics_type_dict


def update_global_metrics_type_dict(metrics_type_dict):
    global global_metrics_type_dict
    global_metrics_type_dict = metrics_type_dict
    return global_metrics_type_dict


create_global_metrics_type_dict()


def create_global_metrics_with_id_dict():
    global global_metrics_with_id_dict
    global_metrics_with_id_dict = {}


def get_global_metrics_with_id_dict():
    return global_metrics_with_id_dict


def update_global_metrics_with_id_dict(metrics_with_id_dict):
    global global_metrics_with_id_dict
    global_metrics_with_id_dict = metrics_with_id_dict
    return global_metrics_with_id_dict


create_global_metrics_with_id_dict()


class PrometheusMetrics(Process):
    """
    The PrometheusMetrics process is responsible for getting metrics submitted
    to flux from the flux.prometheus_metrics Redis hash set and adding them to
    the Horizon worker queue to be submitted to Redis.
    """
    def __init__(self, parent_pid):
        # super(PrometheusMetrics, self).__init__()
        super().__init__()
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            # Log warning
            logger.warning('warning :: parent or current process dead')
            sys.exit(0)

    def metric_in_shard(self, metric_name):
        """
        Check if the metric belongs to the Horizon instance shard.
        """
        shard_number = 0
        if not HORIZON_SHARDS:
            return shard_number
        metric_as_bytes = str(metric_name).encode()
        value = zlib.adler32(metric_as_bytes)
        for shard_node in list(HORIZON_SHARDS.keys()):
            modulo_result = value % number_of_horizon_shards
            shard_number = HORIZON_SHARDS[shard_node]
            if modulo_result == shard_number:
                return shard_number

    def submit_PrometheusMetrics(self):
        """
        Get Prometheus metrics from the Redis flux.prometheus_metrics set every
        60 seconds, aggregated them to 60 seconds and submit them to Redis.
        """

        now_ts = int(time())
        next_run_ts = (now_ts // 60 * 60) + 60

        test = True

        untyped_metrics_key = 'horizon.prometheus.untyped_metrics'

        metrics_with_no_id_key = 'horizon.labelled_metrics.data.no_id'

        # @added 20220730 - Info #4620: memray
        memray_file = '%s/horizon.prometheus.bin' % settings.SKYLINE_TMP_DIR
        memray_file_last = '%s.last' % memray_file
        if MEMRAY_ENABLED:
            logger.info('horizon.prometheus :: memray Tracker enabled')
        else:
            logger.info('horizon.prometheus :: memray Tracker NOT enabled')

        # @added 20230107 - Task #4778: v4.0.0 - update dependencies
        # Address bandit B108:hardcoded_tmp_directory, do not hardcoded tmp
        # Probable insecure usage of temp file/directory (CWE-377)
        vm_send_data_jsonl_file = '%s/horizon.vm_send_data.jsonl' % settings.SKYLINE_TMP_DIR

        running = True
        while running:

            # @added 20220730 - Info #4620: memray
            if os.path.isfile(memray_file_last):
                os.remove(memray_file_last)
                logger.info('horizon.prometheus :: removed %s' % str(memray_file_last))
            if os.path.isfile(memray_file):
                os.rename(memray_file, memray_file_last)
                logger.info('horizon.prometheus :: ranamed %s to %s' % (str(memray_file), str(memray_file_last)))

            if MEMRAY_ENABLED:
                memray_tracker = memray.Tracker(memray_file)
            else:
                memray_tracker = nullcontext()

            try:
                with memray_tracker:
                    self.check_if_parent_is_alive()
                    now_ts = int(time())
                    sleep_for = next_run_ts - now_ts
                    logger.info('horizon.prometheus :: sleeping for %s seconds until next run' % str(sleep_for))
                    while now_ts < next_run_ts:
                        # self.check_if_parent_is_alive()
                        now_ts = int(time())
        #                if not now_ts % 10:
        #                    logger.info('horizon.prometheus :: sleeping for %s seconds until next run' % str(sleep_for))
                        sleep_for = next_run_ts - now_ts
                        if sleep_for < 0:
                            sleep_for = 0.01
                        logger.info('horizon.prometheus :: sleeping for %s seconds until next run' % str(sleep_for))
                        sleep(sleep_for)
                    current_run_ts = int(next_run_ts)
                    last_run_ts = next_run_ts - 60
                    next_run_ts = (now_ts // 60 * 60) + 60

                    metrics_with_no_id_data_list = []

                    # @added 20230123 - Task #2732: Prometheus to Skyline
                    #                   Branch #4300: prometheus
                    all_db_base_names = {}
                    all_db_base_names_with_ids = {}

                    # @added 20230205 - Task #2732: Prometheus to Skyline
                    #                   Branch #4300: prometheus
                    if not VICTORIAMETRICS_ENABLED or not vm_url:
                        logger.error('error :: horizon.prometheus :: no victoriametrics store to write to is known, will drop data')

                    logger.info('horizon.prometheus :: checking for prometheus metrics submitted to flux')
                    new_key = 'flux.prometheus_metrics.%s' % str(current_run_ts)
                    try:
                        self.redis_conn_decoded.rename('flux.prometheus_metrics', new_key)
                    except Exception as err:
                        if str(err) == 'no such key':
                            logger.info('horizon.prometheus :: flux.prometheus_metrics set does not exist')
                        else:
                            logger.error('error :: horizon.prometheus :: failed to rename Redis set flux.prometheus_metrics to %s - %s' % (
                                new_key, str(err)))

                    # Manage global skip dicts
                    # global skip dict variables are created and updated when necessary
                    # meaning when a metric arrives that is not in the skip_dict or
                    # do_not_skip_dict, the metric will be dropped and the global
                    # reload_skip_sets variable will be set to true, so the subsequent
                    # run will reload the skip dicts and when the metric is present,
                    # which might only be after a number of runs until it is updated in
                    # metrics manager, at which point it will be processed accordingly.
                    # This minimises the amount of hgetall requests needed to be made for
                    # skip dicts.
                    check_skip = False
                    if check_skip:
                        skipped_metrics = []
                        skip_dict = {}
                        do_reload_skip_dict = False
                        try:
                            do_reload_skip_dict = get_global_reload_skip_dict()
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: get_global_reload_skip_dict failed - %s' % (
                                str(err)))
                        if do_reload_skip_dict:
                            logger.info('horizon.prometheus :: get_global_reload_skip_dict returned True reloading skip_dict')

                        updated_prometheus_skip_dict = 0
                        try:
                            updated_prometheus_skip_dict = self.redis_conn_decoded.get('metrics_manager.updated.prometheus_skip_dict')
                        except Exception as err:
                            logger.error('error :: metrics_manager :: failed to set metrics_manager.updated.prometheus_skip_dict - %s' % (
                                err))
                        if updated_prometheus_skip_dict:
                            do_reload_skip_dict = True
                            logger.info('horizon.prometheus :: metrics_manager has updated metrics_manager.prometheus_skip_dict reloading skip_dict')
                            try:
                                self.redis_conn_decoded.expire('metrics_manager.updated.prometheus_skip_dict', 50)
                            except Exception as err:
                                logger.error('error :: metrics_manager :: failed to expire metrics_manager.updated.prometheus_skip_dict - %s' % (
                                    err))
                        if not do_reload_skip_dict:
                            try:
                                skip_dict = get_global_skip_dict()
                            except Exception as err:
                                logger.error('error :: horizon.prometheus :: get_global_skip_dict failed - %s' % (
                                    str(err)))
                                skip_dict = {}
                        if not skip_dict:
                            logger.info('horizon.prometheus :: skip_dict empty reloading skip_dict')
                            do_reload_skip_dict = True
                        if do_reload_skip_dict:
                            # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
                            # previous_skip_dict = skip_dict.copy()
                            previous_skip_dict = copy.deepcopy(skip_dict)
                            try:
                                skip_dict = self.redis_conn_decoded.hgetall('metrics_manager.prometheus_skip_dict')
                            except Exception as err:
                                logger.error('error :: horizon.prometheus :: failed to hgetall Redis hash metrics_manager.prometheus_skip_dict - %s' % (
                                    str(err)))
                            if not skip_dict and previous_skip_dict:
                                skip_dict = copy.deepcopy(previous_skip_dict)
                                del previous_skip_dict
                            if skip_dict:
                                try:
                                    new_global_skip_dict = update_global_skip_dict(skip_dict)
                                    if new_global_skip_dict:
                                        skip_dict = new_global_skip_dict
                                except Exception as err:
                                    logger.error('error :: horizon.prometheus :: update_global_skip_dict failed - %s' % (
                                        str(err)))
                                try:
                                    do_reload_skip_dict = update_global_reload_skip_dict(False)
                                except Exception as err:
                                    logger.error('error :: horizon.prometheus :: update_global_reload_skip_dict failed - %s' % (
                                        str(err)))

                    # Create the flux.prometheus_namespace_cardinality.last with counts
                    last_cardinality_key = 'flux.prometheus_namespace_cardinality.%s' % str(last_run_ts)
                    prometheus_namespace_cardinality_dict = {}
                    try:
                        prometheus_namespace_cardinality_dict = self.redis_conn_decoded.hgetall(last_cardinality_key)
                    except Exception as err:
                        logger.error('error :: horizon.prometheus :: failed to hgetall Redis hash %s - %s' % (
                            last_cardinality_key, str(err)))
                    namespace_cardinality_dict = {}
                    for namespace in list(prometheus_namespace_cardinality_dict.keys()):
                        try:
                            namespace_cardinality_dict[namespace] = len(literal_eval(prometheus_namespace_cardinality_dict[namespace]))
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: failed to determine last cardinality count for %s - %s' % (
                                namespace, str(err)))
                    del prometheus_namespace_cardinality_dict
                    if namespace_cardinality_dict:
                        try:
                            self.redis_conn_decoded.hset('flux.prometheus_namespace_cardinality.last', mapping=namespace_cardinality_dict)
                            logger.info('horizon.prometheus :: hset flux.prometheus_namespace_cardinality.last Redis hash with %s namespace counts' % str(len(namespace_cardinality_dict)))
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: failed to hset flux.prometheus_namespace_cardinality.last Redis hash - %s' % (
                                str(err)))

                    # Create a dict for metric types
                    metrics_type_dict = {}
                    update_metrics_type_dict = 0
                    try:
                        metrics_type_dict = get_global_metrics_type_dict()
                    except Exception as err:
                        logger.error('error :: horizon.prometheus :: get_metrics_type_dict failed - %s' % (
                            str(err)))
                        metrics_type_dict = {}
                    logger.info('horizon.prometheus :: got metric types for %s tenant_ids from get_global_metrics_type_dict' % str(len(metrics_type_dict)))

                    # Create a dict for metric types
                    metrics_with_id_dict = {}
                    update_metrics_with_id_dict = False
                    try:
                        metrics_with_id_dict = get_global_metrics_with_id_dict()
                    except Exception as err:
                        logger.error('error :: horizon.prometheus :: get_metrics_with_id_dict failed - %s' % (
                            str(err)))
                        metrics_with_id_dict = {}
                    logger.info('horizon.prometheus :: got %s metrics in metrics_with_id_dict from get_global_metrics_with_id_dict' % str(len(metrics_with_id_dict)))
                    if not metrics_with_id_dict:
                        logger.info('horizon.prometheus :: getting metrics_with_id_dict from Redis aet.metrics_manager.active_labelled_metrics_with_id')
                        try:
                            metrics_with_id_dict = self.redis_conn_decoded.hgetall('aet.metrics_manager.active_labelled_metrics_with_id')
                            logger.info('horizon.prometheus :: got %s metrics in metrics_with_id_dict from Redis aet.metrics_manager.active_labelled_metrics_with_id' % str(len(metrics_with_id_dict)))
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: hgetall aet.metrics_manager.active_labelled_metrics_with_id failed - %s' % (
                                str(err)))
                        if metrics_with_id_dict:
                            update_metrics_with_id_dict = True
                    if update_metrics_with_id_dict:
                        try:
                            new_metrics_with_id_dict = update_global_metrics_with_id_dict(metrics_with_id_dict)
                            if new_metrics_with_id_dict:
                                metrics_with_id_dict = new_metrics_with_id_dict
                                logger.info('horizon.prometheus :: updated global_metrics_with_id_dict with %s metrics' % str(len(metrics_with_id_dict)))
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: update_global_metrics_with_id_dict failed - %s' % (
                                str(err)))
                    update_metrics_with_id_dict = False

                    aggregated_metrics_data_list = []
                    metrics_added = 0
                    metrics_submitted = 0
                    metrics_dropped = 0
                    metric_data_dropped = []

                    metric_data_accepted = []

                    metrics_data_list = []
                    try:
                        metrics_data_list = list(self.redis_conn_decoded.smembers(new_key))
                    except Exception as err:
                        logger.error('error :: horizon.prometheus :: failed to query Redis set %s - %s' % (
                            new_key, str(err)))

                    flux_prometheus_received = len(metrics_data_list)
                    logger.info('horizon.prometheus :: flux received %s prometheus entries' % str(flux_prometheus_received))

                    if metrics_data_list:
                        try:
                            self.redis_conn_decoded.expire(new_key, 120)
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: failed to expire Redis set %s - %s' % (
                                new_key, str(err)))

                    # @added 20230203
                    # We have to fetch the flux.horizon_prometheus_metrics.<ALIGNED_TIMESTAMPS>
                    # and add them to the metrics_data_list
                    if number_of_horizon_shards:
                        current_aligned_ts = int(int(time()) // 60 * 60)
                        check_flux_timestamps = [current_aligned_ts, (current_aligned_ts - 60)]
                        for check_timestamp in check_flux_timestamps:
                            flux_metrics_data_list = []
                            check_key = 'flux.horizon_prometheus_metrics.%s' % str(check_timestamp)
                            try:
                                flux_metrics_data_list = list(self.redis_conn_decoded.smembers(check_key))
                            except Exception as err:
                                logger.error('error :: horizon.prometheus :: failed to query Redis set %s - %s' % (
                                    check_key, str(err)))
                            if flux_metrics_data_list:
                                logger.info('horizon.prometheus :: adding an additional %s metrics received via flux from another horizon shard' % (
                                    str(len(flux_metrics_data_list))))
                                metrics_data_list = metrics_data_list + flux_metrics_data_list
                                try:
                                    self.redis_conn_decoded.delete(check_key)
                                except Exception as err:
                                    logger.error('error :: horizon.prometheus :: failed to delete Redis set %s - %s' % (
                                        check_key, str(err)))

                    # Create a dict for victoriametrics json to submit
                    victoriametrics_data = {}
                    try:
                        prom_to_vm_metrics_dict = get_global_prom_to_vm_metrics_dict()
                    except Exception as err:
                        logger.error('error :: horizon.prometheus :: get_global_prom_to_vm_metrics_dict failed - %s' % (
                            str(err)))
                        prom_to_vm_metrics_dict = {}
                    logger.info('horizon.prometheus :: got %s vm_metrics from get_global_prom_to_vm_metrics_dict' % str(len(prom_to_vm_metrics_dict)))

                    # TODO - route to correct shard/vmagent - as quick as possible
                    shard_metrics_dict = {}
                    other_shard_metrics = {}
                    update_shard_metrics_dict = False
                    if number_of_horizon_shards:
                        for shard_node in list(HORIZON_SHARDS.keys()):
                            if shard_node == this_host:
                                continue
                            other_shard_metrics[HORIZON_SHARDS[shard_node]] = []
                            logger.info('horizon.prometheus :: created other_shard_metrics: %s' % str(other_shard_metrics))
                        try:
                            shard_metrics_dict = get_global_shard_metrics_dict()
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: get_global_shard_metrics_dict failed - %s' % (
                                str(err)))
                            shard_metrics_dict = {}
                        logger.info('horizon.prometheus :: got %s shard metrics from get_global_shard_metrics_dict' % str(len(shard_metrics_dict)))

                    metrics_data_dict = {}
                    metrics_metadata_dict = {}

                    error_logged = False
                    for metric_data_str in metrics_data_list:
                        data_list = []
                        try:
                            data_list = metric_data_str.split(' ')
                        except Exception as err:
                            if not error_logged:
                                logger.error('error :: horizon.prometheus :: interpolate data from - %s - %s' % (
                                    str(metric_data_str), str(err)))
                                error_logged = True
                        if not data_list:
                            continue
                        try:
                            # Handle spaces in metric name
                            # metric_name_list = data_list[2:]
                            # if len(metric_name_list) == 1:
                            #    metric = data_list[2]
                            # else:
                            #    metric = ''.join(data_list[2:])
                            #    # metric = metric.replace('\\', ' ')
                            # Convert millisecond timestamp to unix timestamp
                            tenant_id = str(data_list[0])
                            server_id = str(data_list[1])
                            timestamp_ms = int(data_list[2])
                            timestamp = int(int(data_list[2]) / 1000)
                            value = float(data_list[3])
                            timestamp_str = data_list[2]
                            value_str = data_list[3]
                            if value_str == 'inf':
                                continue
                            remove_data = '%s %s %s %s ' % (tenant_id, server_id, timestamp_str, value_str)
                            # Handle spaces in metric name
                            metric = metric_data_str.replace(remove_data, '')
                            # skip_metric = 1
                            skip_metric = 0
                            if check_skip:
                                try:
                                    skip_metric = int(str(skip_dict[metric]))
                                except:
                                    # metric unknown as not in skip_dict so skip until known
                                    skip_metric = 1
                                    # Set so set global reload_skip_dict to True
                                    try:
                                        do_reload_skip_dict = update_global_reload_skip_dict(True)
                                    except Exception as err:
                                        logger.error('error :: horizon.prometheus :: update_reload_skip_dict failed - %s' % (
                                            str(err)))
                            if skip_metric:
                                skipped_metrics.append(metric)
                                continue
                            # TODO - route to correct shard/vmagent
                            if number_of_horizon_shards:
                                metric_shard = None
                                try:
                                    metric_shard = shard_metrics_dict[metric]
                                except:
                                    try:
                                        metric_shard = self.metric_in_shard(metric)
                                        shard_metrics_dict[metric] = metric_shard
                                        update_shard_metrics_dict = True
                                    except Exception as err:
                                        if not error_logged:
                                            logger.error(traceback.format_exc())
                                            logger.error('error :: horizon.prometheus :: self.metric_in_shard failed for %s - %s' % (
                                                str(metric), str(err)))
                                            # error_logged = True
                                        continue
                                # @modified 20230303 - Task #2732: Prometheus to Skyline
                                #                      Branch #4300: prometheus
                                # Handle the 0 shard
                                append_to_other_shard_metrics = False
                                if metric_shard and metric_shard != HORIZON_SHARD:
                                    append_to_other_shard_metrics = True
                                if metric_shard == 0 and metric_shard != HORIZON_SHARD:
                                    append_to_other_shard_metrics = True
                                if append_to_other_shard_metrics:
                                    # try:
                                    #     other_shard_metric_data = other_shard_metrics[metric_shard][metric]
                                    # except:
                                    #     other_shard_metric_data = []
                                    other_shard_metric_data = {
                                        'metric': metric, 'server_id': server_id,
                                        'tenant_id': tenant_id,
                                        'timestamp': timestamp_ms, 'value': value}
                                    try:
                                        # other_shard_metric_data.append([timestamp_ms, value])
                                        other_shard_metrics[metric_shard].append(other_shard_metric_data)
                                    except Exception as err:
                                        if not error_logged:
                                            logger.error(traceback.format_exc())
                                            logger.error('error :: horizon.prometheus :: failed appending to other_shard_metrics[%s] - %s %s - %s' % (
                                                str(metric_shard), str(metric), str(other_shard_metric_data), str(err)))
                                            logger.debug('debug :: horizon.prometheus :: failed with metric_data_str: %s' % str(metric_data_str))
                                            error_logged = True
                                    if not DEVELOPMENT:
                                        continue

                            try:
                                metric_data = metrics_data_dict[metric]
                            except KeyError:
                                metric_data = []

                            metrics_metadata_dict[metric] = {'tenant_id': tenant_id, 'server_id': server_id}

                        except Exception as err:
                            if not error_logged:
                                logger.error(traceback.format_exc())
                                logger.error('error :: horizon.prometheus :: interpolate metric_data from - %s - %s' % (
                                    str(metric_data_str), str(err)))
                                error_logged = True
                            continue

                        # Deduplicate
                        if [timestamp, value] in metric_data:
                            continue

                        metric_data.append([timestamp, value])
                        metrics_data_dict[metric] = metric_data
                        vm_metric = None
                        vm_metric_name = None
                        try:
                            vm_metric = prom_to_vm_metrics_dict[metric]['metric']  # dict
                            vm_metric_name = prom_to_vm_metrics_dict[metric]['metric_name']  # str
                        except:
                            vm_metric = None
                            vm_metric_name = None
                        update_prom_to_vm_metrics_dict = False
                        if not vm_metric:
                            try:
                                metric_dict = metric_name_labels_parser('horizon', metric)
                                metric_name = metric_dict['metric']
                                vm_metric = {}
                                vm_metric['__name__'] = metric_name
                                vm_metric_labels = metric_dict['labels']
                                labels = ''
                                for key in list(metric_dict['labels'].keys()):
                                    vm_metric[key] = metric_dict['labels'][key]
                                    if labels == '':
                                        labels = '"%s":"%s"' % (key, metric_dict['labels'][key])
                                    else:
                                        labels = '%s,"%s":"%s"' % (labels, key, metric_dict['labels'][key])
                                vm_metric_name = '{"__name__":"%s",%s}' % (metric_name, labels)
                                update_prom_to_vm_metrics_dict = True
                            except Exception as err:
                                if not error_logged:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: horizon.prometheus :: could not determine vm_metric name for metric %s - %s' % (
                                        str(metric), str(err)))
                                    error_logged = True
                                continue
                        if update_prom_to_vm_metrics_dict:
                            prom_to_vm_metrics_dict[metric] = {}
                            prom_to_vm_metrics_dict[metric]['metric'] = vm_metric
                            prom_to_vm_metrics_dict[metric]['metric_name'] = vm_metric_name
                            prom_to_vm_metrics_dict[metric]['labels'] = vm_metric_labels
                            prom_to_vm_metrics_dict[metric]['name'] = metric_name
                            try:
                                new_prom_to_vm_metrics_dict = update_global_prom_to_vm_metrics_dict(prom_to_vm_metrics_dict)
                                if new_prom_to_vm_metrics_dict:
                                    prom_to_vm_metrics_dict = new_prom_to_vm_metrics_dict
                            except Exception as err:
                                logger.error('error :: horizon.prometheus :: update_global_prom_to_vm_metrics_dict failed - %s' % (
                                    str(err)))

                        if update_shard_metrics_dict:
                            try:
                                new_shard_metrics_dict = update_global_shard_metrics_dict(shard_metrics_dict)
                                if new_shard_metrics_dict:
                                    shard_metrics_dict = new_shard_metrics_dict
                            except Exception as err:
                                logger.error('error :: horizon.prometheus :: update_global_shard_metrics_dict failed - %s' % (
                                    str(err)))

                        # victoriametrics json parser does not handle Infinity, null or
                        # false
                        # VictoriaMetrics/lib/protoparser/vmimport/parser.go error with
                        # cannot unmarshal json line \"values\": [null, null]}": cannot
                        # unmarshal value at position 0: value doesn't contain number;
                        # it contains null; skipping it
                        # Same for Infinity and false
                        if value == float('inf'):
                            continue
                        try:
                            victoriametrics_data[vm_metric_name]['timestamps'].append(timestamp_ms)
                            victoriametrics_data[vm_metric_name]['values'].append(value)
                        except:
                            try:
                                victoriametrics_data[vm_metric_name] = {}
                                victoriametrics_data[vm_metric_name]['metric'] = vm_metric
                                victoriametrics_data[vm_metric_name]['timestamps'] = [timestamp_ms]
                                victoriametrics_data[vm_metric_name]['values'] = [value]
                            except Exception as err:
                                logger.error('error :: horizon.prometheus :: failed to add %s to victoriametrics_data - %s' % (
                                    str(vm_metric), str(err)))

                    del metrics_data_list

                    # All prometheus metrics need to be aggregated to 60 seconds as
                    # there is no gaurantee of what interval they are submitted at
                    error_logged = False
                    untyped_metrics = {}

                    # Infer type from metric name as per
                    # https://docs.newrelic.com/docs/infrastructure/prometheus-integrations/install-configure-remote-write/set-your-prometheus-remote-write-integration/#override-mapping
                    counter_metricname_patterns = ['_bucket{', '_count{', '_total{', '_sum{']

                    for metric in list(metrics_data_dict.keys()):
                        metrics_added += 1
                        metric_timeseries = metrics_data_dict[metric]
                        if len(metric_timeseries) == 1:
                            aggregated_metrics_data_list.append([metric, metric_timeseries])
                            continue

                        metric_type = None
                        namespace = metric.split('{', maxsplit=1)[0]
                        query_redis_hash = False
                        try:
                            metric_type = metrics_type_dict[tenant_id][server_id][namespace]
                        except:
                            query_redis_hash = True
                        if query_redis_hash:
                            tenant_id = metrics_metadata_dict[metric]['tenant_id']
                            server_id = metrics_metadata_dict[metric]['server_id']
                            metrics_type_key = 'metrics_manager.prometheus.metrics_type.%s.%s' % (
                                str(tenant_id), str(server_id))
                            try:
                                raw_metric_type = self.redis_conn_decoded.hget(metrics_type_key, namespace)
                                if raw_metric_type:
                                    metric_type = str(raw_metric_type)
                            except Exception as err:
                                if not error_logged:
                                    logger.error('error :: horizon.prometheus :: failed to determine metric_type for %s from Redis hash %s - %s' % (
                                        str(metric), metrics_type_key, str(err)))
                                    error_logged = True
                            if not metric_type:
                                for counter_metricname_pattern in counter_metricname_patterns:
                                    if counter_metricname_pattern in metric:
                                        metric_type = 'COUNTER'
                                        try:
                                            self.redis_conn_decoded.hset(metrics_type_key, namespace, metric_type)
                                        except Exception as err:
                                            if not error_logged:
                                                logger.error('error :: horizon.prometheus :: failed to add metric_type for %s to Redis hash %s - %s' % (
                                                    str(namespace), metrics_type_key, str(err)))
                                                error_logged = True
                                        untyped_metric_key = '%s.%s.%s' % (
                                            str(tenant_id), str(server_id), metric)
                                        try:
                                            self.redis_conn_decoded.hdel(untyped_metrics_key, untyped_metric_key)
                                        except Exception as err:
                                            if not error_logged:
                                                logger.error('error :: horizon.prometheus :: failed to hdel %s from Redis set %s - %s' % (
                                                    str(untyped_metric_key), untyped_metrics_key, str(err)))
                                                error_logged = True
                                        break
                            if metric_type:
                                if tenant_id not in metrics_type_dict:
                                    metrics_type_dict[tenant_id] = {}
                                if server_id not in metrics_type_dict[tenant_id]:
                                    metrics_type_dict[tenant_id][server_id] = {}
                                if metric not in metrics_type_dict[tenant_id][server_id]:
                                    metrics_type_dict[tenant_id][server_id][namespace] = metric_type
                                    update_metrics_type_dict += 1
                        if not metric_type:
                            metric_type = 'UNTYPED'
                            untyped_metric_key = '%s.%s.%s' % (
                                str(tenant_id), str(server_id), metric)
                            untyped_metric_dict = {'metric': metric, 'tenant_id': tenant_id, 'server_id': server_id}
                            untyped_metrics[untyped_metric_key] = str(untyped_metric_dict)

                        # @modified 20220810 - Do not aggregate the timeseries
                        # data because at times this results in counter data
                        # going down instead of up.
                        aggregate_timeseries = False
                        if aggregate_timeseries:
                            # Group aggregates by 60 second period so that if lag/delay or
                            # multiple submissions are added in a single period, they are
                            # submitted for the relevant 60 seconds period, rather than
                            # aggregating multiple periods into the current period.
                            timestamps = [ts for ts, value in metric_timeseries]
                            all_aligned_timestamps = [int(ts // 60 * 60) for ts in timestamps]
                            del timestamps
                            aligned_timestamps = list(set(all_aligned_timestamps))
                            for timestamp in aligned_timestamps:
                                metric_values = []
                                for ts, value in metric_timeseries:
                                    if int(ts // 60 * 60) == timestamp:
                                        last_ts = ts
                                        metric_values.append(value)
                                if metric_values:
                                    try:
                                        if metric_type == 'GAUGE':
                                            aggregate_value = sum(metric_values) / len(metric_values)
                                        else:
                                            aggregate_value = sum(metric_values)
                                        aggregated_metrics_data_list.append([metric, [last_ts, aggregate_value]])
                                    except Exception as err:
                                        logger.error('error :: horizon.prometheus :: failed to calculate add aggregate_value for %s - %s' % (
                                            str(metric), str(err)))
                            del aligned_timestamps
                        else:
                            for ts, value in metric_timeseries:
                                aggregated_metrics_data_list.append([metric, [ts, value]])

                    del metrics_data_dict

                    if update_metrics_type_dict:
                        try:
                            new_metrics_type_dict = update_global_metrics_type_dict(metrics_type_dict)
                            if new_metrics_type_dict:
                                metrics_type_dict = new_metrics_type_dict
                            logger.info('horizon.prometheus :: updated global metrics_type_dict with %s metric namespaces' % str(update_metrics_type_dict))
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: update_global_metrics_type_dict failed - %s' % (
                                str(err)))
                    if untyped_metrics:
                        try:
                            self.redis_conn_decoded.hset(untyped_metrics_key, mapping=untyped_metrics)
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: could hset to Redis key %s - %s' % (
                                untyped_metrics_key, str(err)))
                        logger.info('horizon.prometheus :: recorded %s metrics due to no known metric type in %s' % (
                            str(len(untyped_metrics)), untyped_metrics_key))

                    logger.info('horizon.prometheus :: flux prometheus entries aggregrated to %s metrics' % str(len(aggregated_metrics_data_list)))

                    if metric_data_dropped:
                        dropped_key = 'horizon.dropped.prometheus_metrics.%s' % str(current_run_ts)
                        logger.warning('horizon.prometheus :: adding %s dropped metrics to the %s Redis set' % (
                            str(len(metric_data_dropped)), dropped_key))
                        try:
                            self.redis_conn_decoded.setex(dropped_key, 86400, str(metric_data_dropped))
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: could add metric_data_dropped to Redis key %s - %s' % (
                                dropped_key, str(err)))
                    del metric_data_dropped

                    if test and metric_data_accepted:
                        accepted_key = 'horizon.accepted.prometheus_metrics.%s' % str(current_run_ts)
                        try:
                            self.redis_conn_decoded.setex(accepted_key, 600, str(metric_data_accepted))
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: could add metric_data_accepted to Redis key %s - %s' % (
                                accepted_key, str(err)))

                    # logger.info('horizon.prometheus :: skipped %s metrics' % str(len(skipped_metrics)))

                    logger.info('horizon.prometheus :: submitted %s metrics to redis in the last 60 seconds' % str(metrics_submitted))
                    submitted_metrics_metric_name = '%s.submitted' % skyline_app_graphite_namespace

                    victoriametrics_data_list = []
                    error_logged = False
                    if VICTORIAMETRICS_ENABLED:
                        for metric in list(victoriametrics_data.keys()):
                            try:
                                data_dict = {
                                    'metric': victoriametrics_data[metric]['metric'],
                                    'timestamps': victoriametrics_data[metric]['timestamps'],
                                    'values': victoriametrics_data[metric]['values'],
                                }
                                victoriametrics_data_list.append(data_dict)
                            except Exception as err:
                                if not error_logged:
                                    logger.error('error :: horizon.prometheus :: could add data to victoriametrics_data_list - %s - %s' % (
                                        victoriametrics_data[metric], str(err)))
                                    error_logged = True
                    vm_metrics_key = 'horizon.prometheus_metrics.to.victoriametrics.%s' % str(current_run_ts)
                    if victoriametrics_data_list:
                        try:
                            self.redis_conn_decoded.sadd(vm_metrics_key, str(victoriametrics_data_list))
                            self.redis_conn_decoded.expire(vm_metrics_key, 120)
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: could add victoriametrics_data_list to Redis key %s - %s' % (
                                vm_metrics_key, str(err)))
                        # To send victoriametrics
                        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
                        # Limit to 1000 metrics per request
                        victoriametrics_submissions = []
                        for i in range(0, len(victoriametrics_data_list), 1000):
                            victoriametrics_submissions.append(victoriametrics_data_list[i:(i + 1000)])
                        for victoriametrics_data in victoriametrics_submissions:
                            try:
                                # Handle cluster import url
                                # url = '%s/api/v1/import' % vm_url
                                url = str(vm_url)
                                # @modified 20230107 - Task #4778: v4.0.0 - update dependencies
                                # Address bandit B108:hardcoded_tmp_directory
                                # with open('/tmp/skyline/horizon.vm_send_data.jsonl', 'w') as outfile:
                                with open(vm_send_data_jsonl_file, 'w') as outfile:
                                    for entry in victoriametrics_data:
                                        json.dump(entry, outfile)
                                        outfile.write('\n')
                            except Exception as err:
                                logger.error('error :: horizon.prometheus :: failed to write data to %s - %s' % (
                                    vm_send_data_jsonl_file, str(err)))
                            try:
                                # @modified 20230107 - Task #4778: v4.0.0 - update dependencies
                                # Address bandit B108:hardcoded_tmp_directory
                                # r = requests.post(url, data=open('/tmp/skyline/horizon.vm_send_data.jsonl', 'rb'), headers=headers)
                                r = requests.post(url, data=open(vm_send_data_jsonl_file, 'rb'), headers=headers)
                                logger.info('horizon.prometheus :: sent %s metrics to victoriametrics with response code: %s' % (str(len(victoriametrics_data)), str(r.status_code)))
                            except Exception as err:
                                logger.error(traceback.format_exc())
                                logger.error('error :: horizon.prometheus :: failed to send data to victoriametrics - %s' % (
                                    str(err)))
                        del victoriametrics_submissions
                    logger.info('horizon.prometheus :: %s prometheus/victoriametrics in prom_to_vm_metrics_dict' % str(len(prom_to_vm_metrics_dict)))

                    del victoriametrics_data
                    del victoriametrics_data_list

                    logger.info('horizon.prometheus :: %s metrics in shard_metrics_dict' % str(len(shard_metrics_dict)))
                    other_shard_submissions = []
                    # if other_shard_metrics and DEVELOPMENT:
                    if other_shard_metrics:
                        # What would you do here, how to send to other shards
                        # send to other flux on flux/prometheus/write as json with
                        # horizon-shard header
                        logger.info('horizon.prometheus :: %s metrics in other_shard_metrics' % str(len(other_shard_metrics)))
                        other_shard_metrics_key = 'horizon.prometheus_metrics.other_shard_metrics.%s' % str(current_run_ts)
                        for shard_number in list(other_shard_metrics.keys()):
                            # @modified 20230303 - Task #2732: Prometheus to Skyline
                            #                      Branch #4300: prometheus
                            # Handle the 0 shard
                            # if other_shard_metrics[shard_number]:
                            if other_shard_metrics[shard_number] or other_shard_metrics[shard_number] == 0:
                                try:
                                    self.redis_conn_decoded.hset(other_shard_metrics_key, shard_number, str(other_shard_metrics[shard_number]))
                                    self.redis_conn_decoded.expire(other_shard_metrics_key, 120)
                                    logger.info('horizon.prometheus :: added to Redis hash %s' % other_shard_metrics_key)
                                except Exception as err:
                                    logger.error('error :: horizon.prometheus :: could not add other_shard_metrics_key to Redis key %s - %s' % (
                                        other_shard_metrics_key, str(err)))
                                # What would you do here, how to send to other shards
                                # send to other flux on flux/prometheus/write as json with
                                # horizon-shard header and determine flux URL from
                                # REMOTE_SKYLINE_INSTANCES = [
                                #     ['https://skyline-test-2.example.org', 'admin', 'xxxx', 'skyline-test-2'],
                                #     ['https://skyline-test-3.example.org', 'admin', 'xxxx', 'skyline-test-3'],
                                # ]
                                for shard_host in list(HORIZON_SHARDS.keys()):
                                    if shard_number == HORIZON_SHARDS[shard_host]:
                                        other_shard_host = shard_host
                                        for item in settings.REMOTE_SKYLINE_INSTANCES:
                                            if item[3] == other_shard_host:
                                                url = '%s/flux/prometheus/write' % item[0]
                                # TODO - handle large payload and split into smaller chunks so that 413 is not
                                #        returned
                                other_shard_submissions = []
                                for i in range(0, len(other_shard_metrics[shard_number]), 2500):
                                    other_shard_submissions.append(other_shard_metrics[shard_number][i:(i + 2500)])
                                for other_shard_submission in other_shard_submissions:
                                    try:
                                        # flux_horizon_dict = {
                                        #     "key": settings.FLUX_SELF_API_KEY,
                                        #     "metrics": other_shard_metrics[shard_number]
                                        # }
                                        flux_horizon_dict = {
                                            "key": settings.FLUX_SELF_API_KEY,
                                            "metrics": other_shard_submission
                                        }
                                        logger.info('horizon.prometheus :: flux_horizon_dict size: %s bytes, string length: %s' % (
                                            str(sys.getsizeof(flux_horizon_dict)), str(len(str(flux_horizon_dict)))))
                                        payload = gzip.compress(json.dumps(flux_horizon_dict).encode('utf-8'))
                                        logger.info('horizon.prometheus :: flux_horizon_dict gzipped size: %s' % str(sys.getsizeof(payload)))
                                        headers = {
                                            'Accept': 'application/json',
                                            'Content-Type': 'application/json',
                                            'Content-Encoding': 'gzip',
                                            'horizon-shard': this_host
                                        }
                                        # Compress payload
                                        # r = requests.post(url, data=json.dumps(flux_horizon_dict), headers=headers)
                                        r = requests.post(url, data=payload, headers=headers)
                                        logger.info('horizon.prometheus :: sent %s metrics to %s for shard %s with response code: %s' % (
                                            str(len(other_shard_submission)),
                                            url, str(shard_number),
                                            str(r.status_code)))
                                    except Exception as err:
                                        logger.error('error :: horizon.prometheus :: failed to send data to %s - %s' % (
                                            url, str(err)))
                                logger.info('horizon.prometheus :: sent %s metrics shard %s' % (
                                    str(len(other_shard_metrics[shard_number])),
                                    str(shard_number)))

                    del other_shard_metrics
                    del other_shard_submissions

                    previous_metrics_with_no_id_data_strs = []
                    try:
                        previous_metrics_with_no_id_data_strs = list(self.redis_conn_decoded.smembers(metrics_with_no_id_key))
                    except Exception as err:
                        logger.error('error :: horizon.prometheus :: could not smembers to Redis set %s - %s' % (
                            metrics_with_no_id_key, str(err)))
                    previous_metrics_with_no_id_data = []
                    error_logged = False
                    removed_previous_metrics_with_no_id_data = 0
                    if previous_metrics_with_no_id_data_strs:
                        logger.info('horizon.prometheus :: prefixing aggregated_metrics_data_list with %s entries for metrics without known ids' % (
                            str(len(previous_metrics_with_no_id_data_strs))))
                        for item in previous_metrics_with_no_id_data_strs:
                            try:
                                previous_data = literal_eval(item)
                                if isinstance(previous_data[1][0], list):
                                    previous_data_timestamp = int(previous_data[1][0][0])
                                else:
                                    previous_data_timestamp = int(previous_data[1][0])
                                if previous_data_timestamp >= int(int(now_ts) - 300):
                                    previous_metrics_with_no_id_data.append(previous_data)
                                else:
                                    removed_previous_metrics_with_no_id_data += 1
                            except Exception as err:
                                if not error_logged:
                                    logger.error('error :: horizon.prometheus :: failed to add to previous_metrics_with_no_id_data %s - %s' % (
                                        str(item), str(err)))
                                error_logged = True
                        del previous_metrics_with_no_id_data_strs
                    logger.info('horizon.prometheus :: removing %s metrics with no id entries from %s' % (
                        str(removed_previous_metrics_with_no_id_data),
                        metrics_with_no_id_key))

                    if previous_metrics_with_no_id_data:
                        try:
                            self.redis_conn_decoded.delete(metrics_with_no_id_key)
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: could delete Redis set %s - %s' % (
                                metrics_with_no_id_key, str(err)))
                        aggregated_metrics_data_list = previous_metrics_with_no_id_data + aggregated_metrics_data_list

                    global_metrics_with_id_dict_updated = False
                    labelled_metrics = []
                    retention_msecs = int(settings.FULL_DURATION) * 1000
                    redis_timeseries_errors = []
                    redis_timeseries_failed_to_insert = []
                    logger.info('horizon.prometheus :: inserting %s entries into redistimeseries' % (
                        str(len(aggregated_metrics_data_list))))
                    start_insert = timer()
                    inserted_count = 0

                    time_now = int(time())
                    no_id_keys = 0
                    unique_labelled_metrics_key = 'labelled_metrics.unique_labelled_metrics'

                    metrics_with_unknown_id = []
                    metrics_with_id_found_in_db = {}
                    updated_global_metrics_with_id_dict_count = 0

                    for item in aggregated_metrics_data_list:
                        try:
                            metric = item[0]
                            metric_id = 0
                            if metrics_with_id_dict:
                                try:
                                    metric_id = int(float(metrics_with_id_dict[metric]))
                                except:
                                    metric_id = 0

                            if not metric_id and not global_metrics_with_id_dict_updated:
                                logger.info('horizon.prometheus :: getting metrics_with_id_dict from Redis aet.metrics_manager.active_labelled_metrics_with_id to update global_metrics_with_id_dict')
                                try:
                                    metrics_with_id_dict = self.redis_conn_decoded.hgetall('aet.metrics_manager.active_labelled_metrics_with_id')
                                    logger.info('horizon.prometheus :: got %s metrics in metrics_with_id_dict from Redis aet.metrics_manager.active_labelled_metrics_with_id' % str(len(metrics_with_id_dict)))
                                    global_metrics_with_id_dict_updated = True
                                except Exception as err:
                                    logger.error('error :: horizon.prometheus :: hgetall aet.metrics_manager.active_labelled_metrics_with_id failed - %s' % (
                                        str(err)))
                                if metrics_with_id_dict:
                                    try:
                                        new_metrics_with_id_dict = update_global_metrics_with_id_dict(metrics_with_id_dict)
                                        if new_metrics_with_id_dict:
                                            metrics_with_id_dict = new_metrics_with_id_dict
                                            # Logs every metric when service restarts
                                            # logger.info('horizon.prometheus :: updated global_metrics_with_id_dict with %s metrics' % str(len(metrics_with_id_dict)))
                                    except Exception as err:
                                        logger.error('error :: horizon.prometheus :: update_global_metrics_with_id_dict failed - %s' % (
                                            str(err)))
                                try:
                                    metric_id = int(float(metrics_with_id_dict[metric]))
                                except:
                                    metric_id = 0
                            if not metric_id:
                                try:
                                    metric_id = int(float(self.redis_conn_decoded.hget('aet.metrics_manager.metric_names_with_ids', metric)))
                                except:
                                    pass

                                # @added 20230123 - Task #2732: Prometheus to Skyline
                                #                   Branch #4300: prometheus
                                # Check metric if metric/s are in DB and not in
                                # Redis for metrrics_manager to reset as active
                                if not metric_id:
                                    if not all_db_base_names_with_ids:
                                        try:
                                            with_ids = True
                                            all_db_base_names, all_db_base_names_with_ids = get_all_db_metric_names(skyline_app, with_ids)
                                            logger.info('horizon.prometheus :: got %s metrics in all_db_base_names_with_ids with get_all_db_metric_names' % str(len(all_db_base_names_with_ids)))
                                        except Exception as err:
                                            logger.error('error :: horizon.prometheus :: get_all_db_metric_names failed - %s' % (
                                                str(err)))
                                    metrics_with_unknown_id.append(metric)
                                    try:
                                        metric_id = int(all_db_base_names_with_ids[metric])
                                        metrics_with_id_found_in_db[metric_id] = metric
                                    except:
                                        pass

                                if metric_id:
                                    metrics_with_id_dict[metric] = metric_id
                                    try:
                                        new_metrics_with_id_dict = update_global_metrics_with_id_dict(metrics_with_id_dict)
                                        updated_global_metrics_with_id_dict_count += 1
                                        if new_metrics_with_id_dict:
                                            metrics_with_id_dict = new_metrics_with_id_dict
                                            # Logs every metric when service restarts
                                            # logger.info('horizon.prometheus :: updated global_metrics_with_id_dict with %s metrics' % str(len(metrics_with_id_dict)))
                                    except Exception as err:
                                        logger.error('error :: horizon.prometheus :: update_global_metrics_with_id_dict failed - %s' % (
                                            str(err)))

                            if not metric_id:
                                metrics_with_no_id_data_list.append(item)
                                no_id_keys += 1
                                continue

                            labels = {}
                            try:
                                labels_ = prom_to_vm_metrics_dict[metric]['labels']
                                labels = {}
                                labels['__name__'] = prom_to_vm_metrics_dict[metric]['name']
                                # @added 20230626 - Bug #4968: horizon.prometheus - handle TSDB not being able to parse LABELS
                                #                   Task #4962: Build and test skyline v4.0.0
                                # https://github.com/RedisTimeSeries/RedisTimeSeries/issues/1478
                                warn_on_labels = False

                                for key in list(labels_.keys()):
                                    # @added 20230626 - Bug #4968: horizon.prometheus - handle TSDB not being able to parse LABELS
                                    #                   Task #4962: Build and test skyline v4.0.0
                                    # https://github.com/RedisTimeSeries/RedisTimeSeries/issues/1478
                                    if key == 'tags':
                                        if ',' in labels_[key]:
                                            value_str = labels_[key]
                                            new_value_str = value_str.replace(',', ' ')
                                            logger.warning('warning :: horizon.prometheus :: changing tags key value from %s to %s on %s' % (
                                                str(value_str), new_value_str, str(labels)))
                                            labels_[key] = new_value_str
                                            warn_on_labels = True

                                    labels[key] = labels_[key]

                                # @added 20230626 - Bug #4968: horizon.prometheus - handle TSDB not being able to parse LABELS
                                #                   Task #4962: Build and test skyline v4.0.0
                                # https://github.com/RedisTimeSeries/RedisTimeSeries/issues/1478
                                if warn_on_labels:
                                    logger.warning('warning :: horizon.prometheus :: changing tags on %s' % (
                                        str(labels_)))

                            except Exception as err:
                                err_str = 'labels err - %s' % str(err)
                                redis_timeseries_errors.append([item, err_str])
                                continue

                            labelled_metric_by_id = 'labelled_metrics.%s' % str(metric_id)
                            redis_ts_key = labelled_metric_by_id

                            item_list = list(item[1])
                            if isinstance(item[1][0], int):
                                item_list = [item[1]]
                            for ts_item in item_list:
                                value = ts_item[1]
                                timestamp = ts_item[0]
                                timestamp_ms = int(timestamp) * 1000
                                inserted = 0
                                try:
                                    inserted = self.redis_conn_decoded.ts().add(redis_ts_key, timestamp_ms, value, retention_msecs=retention_msecs, labels=labels, chunk_size=128, duplicate_policy='first')
                                    labelled_metrics.append(redis_ts_key)
                                except Exception as err:
                                    if "TSDB: Couldn't parse LABELS" in str(err):
                                        redis_timeseries_errors.append([item, {'labels': labels}, err])
                                    else:
                                        redis_timeseries_errors.append(['insert', item, err])
                                    continue
                                if not inserted:
                                    redis_timeseries_failed_to_insert.append(item)
                                else:
                                    inserted_count += 1
                        except Exception as err:
                            redis_timeseries_errors.append(['process error', item, err])
                    end_insert = timer()
                    logger.info('horizon.prometheus :: inserted %s entries into redistimeseries with %s errors, %s warnings in %.6f seconds' % (
                        str(inserted_count), str(len(redis_timeseries_errors)),
                        str(len(redis_timeseries_failed_to_insert)),
                        (end_insert - start_insert)))
                    if redis_timeseries_errors:
                        logger.error('error :: horizon.prometheus :: last 3 redistimeseries errors: %s' % (
                            str(redis_timeseries_errors[-3:])))

                    logger.info('horizon.prometheus :: dropped %s timeseries due to no metric id known' % (
                        str(no_id_keys)))

                    # @added 20230123 - Task #2732: Prometheus to Skyline
                    #                   Branch #4300: prometheus
                    # Check metric if metric/s are in DB and not in
                    # Redis for metrrics_manager to reset as active
                    logger.info('horizon.prometheus :: updated_global_metrics_with_id_dict_count: %s' % (
                        str(updated_global_metrics_with_id_dict_count)))
                    if metrics_with_id_found_in_db:
                        logger.info('horizon.prometheus :: looked up %s metric ids from all_db_base_names_with_ids and found ids for %s' % (
                            str(len(metrics_with_unknown_id)), str(len(metrics_with_id_found_in_db))))
                        try:
                            self.redis_conn_decoded.sadd('horizon.active_metrics_not_in_metric_names_with_ids', *set(list(metrics_with_id_found_in_db.keys())))
                            logger.info('horizon.prometheus :: set horizon.active_metrics_not_in_metric_names_with_ids with %s metric ids for metrics_manager to set as active' % str(len(metrics_with_id_found_in_db)))
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: sadd on horizon.active_metrics_not_in_metric_names_with_ids failed - %s' % (
                                str(err)))
                        active_key = 'horizon.active_metrics_not_in_metric_names_with_ids.%s' % str(current_run_ts)
                        try:
                            self.redis_conn_decoded.hset(active_key, mapping=metrics_with_id_found_in_db)
                            self.redis_conn_decoded.expire(active_key, 300)
                            logger.info('horizon.prometheus :: set %s with 300 second expiry' % active_key)
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: hest on %s failed - %s' % (
                                active_key, str(err)))

                    insert_results = {
                        'total': len(aggregated_metrics_data_list),
                        'inserted': inserted_count,
                        'errors': redis_timeseries_errors,
                        'errors_count': len(redis_timeseries_errors),
                        'failed_to_insert': redis_timeseries_failed_to_insert,
                        'failed_to_insert_count': len(redis_timeseries_failed_to_insert),
                        'execution_time': (end_insert - start_insert),
                        'dropped_no_id': no_id_keys,
                    }
                    del aggregated_metrics_data_list
                    insert_results_key = 'horizon.prometheus_metrics.insert_results.redistimeseries.%s' % str(current_run_ts)
                    expiry_time = 120
                    if len(redis_timeseries_errors) > 0:
                        expiry_time = 300
                    if len(redis_timeseries_failed_to_insert) > 0:
                        expiry_time = 300
                    for key in list(insert_results.keys()):
                        try:
                            self.redis_conn_decoded.hset(insert_results_key, key, str(insert_results[key]))
                            self.redis_conn_decoded.expire(insert_results_key, expiry_time)
                            # logger.info('horizon.prometheus :: added %s to Redis hash %s' % (key, insert_results_key))
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: could not add %s to Redis hash %s - %s' % (
                                key, insert_results_key, str(err)))

                    if labelled_metrics:
                        try:
                            self.redis_conn_decoded.sadd(unique_labelled_metrics_key, *set(labelled_metrics))
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: could not sadd to Redis set %s - %s' % (
                                unique_labelled_metrics_key, str(err)))

                    if metrics_with_no_id_data_list:
                        metrics_with_no_id = []
                        logger.info('horizon.prometheus :: adding %s entries to Redis set %s for metrics without ids' % (
                            str(len(metrics_with_no_id_data_list)), metrics_with_no_id_key))
                        metrics_with_no_id_data_strings_list = [str(item) for item in metrics_with_no_id_data_list]
                        try:
                            self.redis_conn_decoded.sadd(metrics_with_no_id_key, *metrics_with_no_id_data_strings_list)
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: could not sadd %s metric data for metrics without ids to Redis set %s - %s' % (
                                str(len(metrics_with_no_id_data_list)), metrics_with_no_id_key, str(err)))
                        metrics_with_no_id = [item[0] for item in metrics_with_no_id_data_list]
                        try:
                            self.redis_conn_decoded.sadd('panorama.horizon.metrics_with_no_id', *metrics_with_no_id)
                        except Exception as err:
                            logger.error('error :: horizon.prometheus :: could not sadd %s metrics without ids to Redis set panorama.horizon.metrics_with_no_id - %s' % (
                                str(len(metrics_with_no_id_data_list)), str(err)))
                        del metrics_with_no_id
                    del metrics_with_no_id_data_list

                    send_graphite_metric(self, skyline_app, submitted_metrics_metric_name, metrics_submitted)
                    dropped_metrics_metric_name = '%s.prometheus_metrics.dropped' % skyline_app_graphite_namespace
                    send_graphite_metric(self, skyline_app, dropped_metrics_metric_name, metrics_dropped)

                    flux_prometheus_received_metric_name = '%s.flux_received' % skyline_app_graphite_namespace
                    send_graphite_metric(self, skyline_app, flux_prometheus_received_metric_name, flux_prometheus_received)

                    logger.info('horizon.prometheus :: flux received %s prometheus entries' % str(flux_prometheus_received))

                    logger.info('horizon.prometheus :: took %.6f seconds to process flux.prometheus_metrics' % (
                        (time() - now_ts)))

                    continue
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: horizon.prometheus :: error in memray tracker block - %s' % (
                    str(err)))

    def run(self):
        """
        Called when process intializes.
        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        if os.path.isfile(skyline_app_logwait):
            try:
                os_remove(skyline_app_logwait)
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

        logger.info('starting %s Prometheus' % skyline_app)
        if os.path.isfile(skyline_app_loglock):
            logger.error('error - bin/%s.d log management seems to have failed, continuing' % skyline_app)
            try:
                os_remove(skyline_app_loglock)
                logger.info('log lock file removed')
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_loglock)
        else:
            logger.info('bin/%s.d log management done' % skyline_app)

        if not VICTORIAMETRICS_ENABLED or not vm_url:
            logger.error('error :: %s :: starting submit_PrometheusMetrics but no victoriametrics store to write to is known' % skyline_app)

        logger.info('%s :: starting submit_PrometheusMetrics' % skyline_app)
        self.submit_PrometheusMetrics()
