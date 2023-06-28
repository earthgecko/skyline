"""
prometheus.py
"""
import sys
import os
from time import time
import base64
import re
from timeit import default_timer as timer
import traceback
from ast import literal_eval
import json
import gzip
import resource
# @added 20220722 - Task #4624: Change all dict copy to deepcopy
import copy

from logger import set_up_logging

import snappy
from google.protobuf import json_format as pb_json_format

import falcon

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# This prevents flake8 E402 - module level import not at top of file
if True:
    import settings
    import flux
    from skyline_functions import get_redis_conn, get_redis_conn_decoded
    if settings.MEMCACHE_ENABLED:
        from functions.memcache.get_memcache_key import get_memcache_key
    else:
        get_memcache_key = None
#    try:
#        import prometheus_pb2
#    except Exception as outer_err:
#        print(traceback.format_exc())
#        print('prometheus :: error failed to import prometheus_pb2 - %s' % outer_err)
    # @modified 20230227 - Bug #4856: flux - prometheus - no 0s
    # from proto import remote_pb2
    from protoc import remote_pb2

    from functions.flux.prometheus_horizon_request import prometheus_horizon_request
    from matched_or_regexed_in_list import matched_or_regexed_in_list

# Wrap per metric logging in if FLUX_VERBOSE_LOGGING
try:
    FLUX_VERBOSE_LOGGING = settings.FLUX_VERBOSE_LOGGING
except:
    FLUX_VERBOSE_LOGGING = True

# @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
try:
    FLUX_PERSIST_QUEUE = settings.FLUX_PERSIST_QUEUE
except:
    FLUX_PERSIST_QUEUE = False

# Consolidate flux logging
logger = set_up_logging(None)

LOCAL_DEBUG = False
# LOCAL_DEBUG = True

# @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
redis_conn = get_redis_conn('flux')
redis_conn_decoded = get_redis_conn_decoded('flux')

flux_keys = {}
valid_keys = []
try:
    FLUX_SELF_API_KEY = settings.FLUX_SELF_API_KEY
    valid_keys.append(FLUX_SELF_API_KEY)
    flux_keys[FLUX_SELF_API_KEY] = None
except:
    pass

flux_keys = {}
FLUX_API_KEYS = {}
try:
    # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
    # FLUX_API_KEYS = settings.FLUX_API_KEYS.copy()
    FLUX_API_KEYS = copy.deepcopy(settings.FLUX_API_KEYS)
except:
    pass
if FLUX_API_KEYS:
    for flux_api_key in FLUX_API_KEYS:
        try:
            valid_keys.append(str(flux_api_key))
            flux_keys[flux_api_key] = FLUX_API_KEYS[flux_api_key]
        except:
            pass

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
this_host = str(os.uname()[1])

DEVELOPMENT = False
if this_host == 'skyline-test-1-fra1':
    DEVELOPMENT = True
    HORIZON_SHARDS = {
        'skyline-test-1-fra1': 0,
        'another-test-node-1': 1,
        'another-test-node-2': 2,
    }

# @added 20230213 - Feature #4840: flux - prometheus - drop bucket metrics
# Drop _bucket metrics
try:
    FLUX_DROP_BUCKET_METRICS = settings.FLUX_DROP_BUCKET_METRICS
except:
    FLUX_DROP_BUCKET_METRICS = True

skyline_external_settings = {}

try:
    skyline_external_settings_raw = redis_conn_decoded.get('skyline.external_settings')
    if skyline_external_settings_raw:
        skyline_external_settings = literal_eval(skyline_external_settings_raw)
except Exception as outer_err:
    logger.error('error :: prometheus :: could not get Redis set skyline.external_settings - %s' % str(outer_err))
    skyline_external_settings = {}
    # @added 20220426 - Feature #4536: Handle Redis failure
    # Add flux required data to memcache as well
    if get_memcache_key:
        try:
            skyline_external_settings = get_memcache_key('flux', 'skyline.external_settings')
            if skyline_external_settings:
                logger.info('prometheus :: there are %s items in the skyline.external_settings memcache dict' % (
                    str(len(skyline_external_settings))))
            else:
                skyline_external_settings = {}
                logger.warning('warning :: prometheus :: failed to get skyline.external_settings memcache dict')
        except Exception as outer_err2:
            logger.error('error :: prometheus :: could not get memcache set skyline.external_settings - %s' % str(outer_err2))
            skyline_external_settings = {}

external_settings_namespaces = []
if skyline_external_settings:
    for settings_key in list(skyline_external_settings.keys()):
        external_setting_keys = list(skyline_external_settings[settings_key].keys())
        if 'flux_token' in external_setting_keys:
            flux_token = skyline_external_settings[settings_key]['flux_token']
            if flux_token:
                valid_keys.append(str(flux_token))
                flux_keys[flux_token] = skyline_external_settings[settings_key]['namespace']
        if 'namespace' in external_setting_keys:
            try:
                external_settings_namespaces.append(skyline_external_settings[settings_key]['namespace'])
            except Exception as outer_err:
                logger.error('error :: prometheus :: could not append skyline_external_settings[\'%s\'][\'namespace\'] to external_settings_namespaces - %s' % (
                    str(settings_key), str(outer_err)))

ALL_SKIP_LIST = []
try:
    ALL_SKIP_LIST = list(settings.SKIP_LIST)
except AttributeError:
    ALL_SKIP_LIST = []
except:
    ALL_SKIP_LIST = []
ALL_DO_NOT_SKIP_LIST = []
try:
    ALL_DO_NOT_SKIP_LIST = list(settings.DO_NOT_SKIP_LIST)
except AttributeError:
    ALL_DO_NOT_SKIP_LIST = []
except:
    ALL_DO_NOT_SKIP_LIST = []

external_namespace_settings = {}
if skyline_external_settings:
    for settings_key in list(skyline_external_settings.keys()):
        external_setting_keys = list(skyline_external_settings[settings_key].keys())
        skip_metrics = []
        external_namespace = None
        try:
            external_namespace = skyline_external_settings[settings_key]['namespace']
        except KeyError:
            external_namespace = None
        except:
            external_namespace = None
        if external_namespace:
            external_namespace_settings[external_namespace] = skyline_external_settings[settings_key]
        if 'skip_metrics' in external_setting_keys:
            skip_metrics = skyline_external_settings[settings_key]['skip_metrics']
            if skip_metrics:
                if isinstance(skip_metrics, list):
                    if external_namespace:
                        for ex_namespace in skip_metrics:
                            ex_metric_namespace = '%s.%s' % (external_namespace, ex_namespace)
                            ALL_SKIP_LIST.append(ex_metric_namespace)
                    else:
                        ALL_SKIP_LIST = ALL_SKIP_LIST + skip_metrics
        else:
            skyline_external_settings[settings_key]['skip_metrics'] = []
        do_not_skip_metrics = []
        if 'do_not_skip_metrics' in external_setting_keys:
            do_not_skip_metrics = skyline_external_settings[settings_key]['do_not_skip_metrics']
            if do_not_skip_metrics:
                if isinstance(do_not_skip_metrics, list):
                    if external_namespace:
                        for s_namespace in do_not_skip_metrics:
                            s_metric_namespace = '%s.%s' % (external_namespace, s_namespace)
                            ALL_DO_NOT_SKIP_LIST.append(s_metric_namespace)
                    else:
                        ALL_DO_NOT_SKIP_LIST = ALL_DO_NOT_SKIP_LIST + do_not_skip_metrics
        else:
            skyline_external_settings[settings_key]['do_not_skip_metrics'] = []

known_prometheus_metrics = {}
try:
    # known_prometheus_metrics = redis_conn_decoded.hgetall('skyline.prometheus_to_dotted')
    known_metric_names_with_ids = redis_conn_decoded.hgetall('metrics_manager.metric_names_with_ids')
    if known_metric_names_with_ids:
        for metric_id in list(known_metric_names_with_ids.keys()):
            known_metric = known_metric_names_with_ids[metric_id]
            if '{' in known_metric and '}' in known_metric and '=' in known_metric:
                known_prometheus_metrics[known_metric] = metric_id
    del known_metric_names_with_ids
except Exception as outer_err:
    logger.error('error :: prometheus :: could not get Redis hgetall metrics_manager.metric_names_with_ids - %s' % str(outer_err))
    known_prometheus_metrics = {}

namespace_quotas_dict = {}
try:
    namespace_quotas_dict = redis_conn_decoded.hgetall('metrics_manager.flux.namespace_quotas')
except Exception as outer_err:
    logger.error(traceback.format_exc())
    logger.error('error :: prometheus :: failed to hgetall metrics_manager.flux.namespace_quotas Redis hash key - %s' % (
        outer_err))
    namespace_quotas_dict = {}
    # @added 20220426 - Feature #4536: Handle Redis failure
    # Add flux required data to memcache as well
    if get_memcache_key:
        try:
            namespace_quotas_dict = get_memcache_key('flux', 'metrics_manager.flux.namespace_quotas')
            if namespace_quotas_dict:
                logger.info('prometheus :: there are %s items in the namespace_quotas_dict memcache dict' % (
                    str(len(namespace_quotas_dict))))
            else:
                logger.warning('warning :: prometheus :: failed to get namespace_quotas_dict memcache dict')
                namespace_quotas_dict = {}
        except Exception as outer_err2:
            logger.error('error :: prometheus :: could not get memcache dict metrics_manager.flux.namespace_quotas - %s' % str(outer_err2))
            namespace_quotas_dict = {}

namespaces_with_quotas = []
for quota_namespace in list(namespace_quotas_dict.keys()):
    namespaces_with_quotas.append(quota_namespace)
if namespaces_with_quotas:
    from functions.thunder.send_event import thunder_send_event
else:
    thunder_send_event = None


def create_namespace_cardinality_count():
    global namespace_cardinality_count
    namespace_cardinality_count = {}


def get_namespace_cardinality_count():
    return namespace_cardinality_count


def update_namespace_cardinality_count(data):
    global namespace_cardinality_count
    namespace_cardinality_count = data
    return namespace_cardinality_count


create_namespace_cardinality_count()


class PrometheusMetricDataPost(object):

    def on_post(self, req, resp):
        """
        The /flux/prometheus/write endpoint is called via a POST with the
        Prometheus metric data.

        """

        start = timer()

        key = None

        now = time()
        logger.info('prometheus :: %s :: handling request with mem_usage of %s kb' % (
            str(now), str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)))

        current_ts = int(now)
        current_aligned_ts = int(current_ts // 60 * 60)
        last_aligned_ts = current_aligned_ts - 60

        dropped_too_long = []
        cardinality_breached_metric_count = 0

        # A dictionary to record metrics for cardinality
        metric_namespaces_dict = {}
        cardinality_metric_namespaces_dict = {}

        data = None
        try:
            data = req.stream.read()
        except Exception as err:
            logger.error('error :: prometheus :: req.stream.read() failed - %s' % str(err))

        # The prometheus remote_storage_adapter is started with the
        # --influxdb.database=<database> option, when the remote_storage_adapter
        # is used to send data to Skyline, Skyline uses this as the internal
        # metric prefix
        request_params = {}
        for request_param_key, request_param_value in req.params.items():
            request_params[request_param_key] = request_param_value

        status_request = False
        try:
            status_request = request_params['status']
        except KeyError:
            status_request = False

        if status_request:
            try:
                # Report app up
                redis_conn.setex('flux.prometheus', 120, int(time()))
                body = {"status": "ok"}
                resp.text = json.dumps(body)
                logger.info('prometheus :: %s :: status request - OK' % str(now))
                resp.status = falcon.HTTP_200
                return
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: prometheus :: status request failed - %s' % err)
                resp.status = falcon.HTTP_500
                return

        all_headers = None
        try:
            all_headers = str(req.headers)
        except Exception as err:
            logger.error('prometheus :: req.headers() error - %s' % str(err))
        if LOCAL_DEBUG:
            logger.debug('debug :: prometheus :: %s :: headers - %s' % (str(now), str(all_headers)))

        request_type = 'remote_storage_adapter'

        content_encoding = None
        try:
            content_encoding = req.get_header('content-encoding')
        except Exception as err:
            logger.error('prometheus :: get_header(\'content-encoding\') error - %s' % str(err))
        # Decompress gzip data
        if content_encoding == 'gzip':
            logger.info('prometheus :: %s :: ungzipping content' % str(now))
            try:
                data = gzip.decompress(data)
                logger.info('prometheus :: %s :: ungzipped' % str(now))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: prometheus :: gzip.decompress(data) failed - %s' % err)

        # Shard request
        shard_request_node = None
        horizonPostData = None
        try:
            shard_request_node = req.get_header('horizon-shard')
        except Exception as err:
            logger.error('prometheus :: get_header(\'shard_request\') error - %s' % str(err))
        if shard_request_node:
            if shard_request_node not in list(HORIZON_SHARDS.keys()):
                logger.warning('warning :: prometheus :: invalid horizon-shard request - %s' % (
                    str(shard_request_node)))
                resp.status = falcon.HTTP_403
                return
            request_type = 'horizon'
            try:
                horizonPostData = json.loads(data)
            except json.decoder.JSONDecodeError:
                message = 'invalid json data'
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.info('prometheus :: %s, returning 400' % message)
                resp.status = falcon.HTTP_400
                return
            key = None
            invalid_key = False
            try:
                key = str(horizonPostData['key'])
            except KeyError:
                message = 'no key passed'
                invalid_key = True
                key = None
            if str(key) != settings.FLUX_SELF_API_KEY:
                message = 'invalid key passed'
                invalid_key = True
            if invalid_key:
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.info('prometheus :: %s, returning 400' % message)
                resp.status = falcon.HTTP_400
                return

        # @added 20230202 - Feature #4840: flux - prometheus - x-test-only header
        test_only = False
        test_only_metrics_submitted = []
        test_metrics_with_no_values = []
        try:
            test_only_str = req.get_header('x-test-only')
            if test_only_str:
                test_only = True
        except Exception as err:
            logger.error('prometheus :: get_header(\'x-test-only\') error - %s' % str(err))
        if test_only:
            logger.info('prometheus :: %s :: test_only request' % str(now))

        # Require the tenant_id, server_id and server_url headers to map metrics
        # and metadata to the correct servers.
        return_400 = False
        tenant_id = None
        try:
            tenant_id = req.get_header('x-tenant-id')
            if not tenant_id and DEVELOPMENT:
                tenant_id = '123'
            if DEVELOPMENT:
                external_namespace_settings[tenant_id] = {
                    'skip_metrics': ['.*_info{', '.*le="0'],
                    'do_not_skip_metrics': [],
                    'metric_limit': 30000,
                }
            if DEVELOPMENT and tenant_id == 'org_124':
                external_namespace_settings[tenant_id] = {
                    'skip_metrics': ['.*_info{'],
                    'do_not_skip_metrics': [],
                    'metric_limit': 1000,
                }
        except Exception as err:
            logger.error('prometheus :: get_header(\'x-tenant-id\') error - %s' % str(err))
            return_400 = True
            message = 'no tenant id passed'
        server_id = None
        try:
            server_id = req.get_header('x-server-id')
            if not server_id and DEVELOPMENT:
                server_id = '1'
        except Exception as err:
            logger.error('prometheus :: get_header(\'x-server-id\') error - %s' % str(err))
            return_400 = True
            message = 'no server id passed'

        if tenant_id and server_id:
            logger.info('prometheus :: %s :: tenant_id: %s, server_id: %s' % (
                str(now), str(tenant_id), str(server_id)))

        server_url = None
        try:
            server_url = req.get_header('x-server-url')
            if not server_url and DEVELOPMENT:
                server_url = 'http://localhost:9090'
        except Exception as err:
            logger.error('prometheus :: get_header(\'x-server-url\') error - %s' % str(err))
            return_400 = True
            message = 'no server url passed'
        if return_400:
            body = {"code": 400, "message": message}
            resp.text = json.dumps(body)
            logger.info('prometheus :: %s, returning 400' % message)
            resp.status = falcon.HTTP_400
            return

        # dropMetrics
        dropMetrics = []

        # @added 20230202 - Feature #4840: flux - prometheus - x-test-only header
        #                   Feature #4842: webapp - api - get_flux_test_metrics
        #                   Task #4828: EXTERNAL_SETTINGS - handle key value in external_settings
        # Handle a list of strings and a list of lists added evaled_dropMetrics
        evaled_dropMetrics = None

        try:
            dropMetrics_str = req.get_header('dropMetrics')
            if dropMetrics_str:
                dropMetrics = dropMetrics_str.split(',')
                # @added 20230202 - Feature #4840: flux - prometheus - x-test-only header
                #                   Feature #4842: webapp - api - get_flux_test_metrics
                #                   Task #4828: EXTERNAL_SETTINGS - handle key value in external_settings
                # Handle a list of strings and a list of lists using evaled_dropMetrics
                try:
                    evaled_dropMetrics = literal_eval(dropMetrics_str)
                    if test_only:
                        logger.info('prometheus :: %s :: test_only evaled_dropMetrics: %s' % (
                            str(now), str(evaled_dropMetrics)))
                except:
                    evaled_dropMetrics = None
                if isinstance(evaled_dropMetrics, list):
                    dropMetrics = []
                    for item in evaled_dropMetrics:
                        if isinstance(item, list):
                            for i_item in item:
                                dropMetrics.append(i_item)
                        if isinstance(item, str):
                            dropMetrics.append(item)

        except Exception as err:
            logger.error('prometheus :: get_header(\'dropMetrics\') error - %s' % str(err))
        if dropMetrics:
            logger.info('prometheus :: %s :: dropMetrics: %s' % (
                str(now), str(dropMetrics)))

        # @added 20230213 - Feature #4840: flux - prometheus - drop bucket metrics
        # Drop _bucket metrics
        if FLUX_DROP_BUCKET_METRICS:
            dropMetrics.append('.*_bucket{.*')

        # @added 20230214 - Feature #4840: flux - prometheus - x-test-only header
        #                   Feature #4842: webapp - api - get_flux_test_metrics
        #                   Task #4828: EXTERNAL_SETTINGS - handle key value in external_settings
        # Handle a list of strings and a list of lists added evaled_doNotDropMetrics
        evaled_doNotDropMetrics = None

        # doNotDropMetrics
        doNotDropMetrics = []
        try:
            doNotDropMetrics_str = req.get_header('doNotDropMetrics')
            if doNotDropMetrics_str:
                doNotDropMetrics = doNotDropMetrics_str.split(',')

                # @added 20230202 - Feature #4840: flux - prometheus - x-test-only header
                #                   Feature #4842: webapp - api - get_flux_test_metrics
                #                   Task #4828: EXTERNAL_SETTINGS - handle key value in external_settings
                # Handle a list of strings and a list of lists using evaled_doNotDropMetrics
                try:
                    evaled_doNotDropMetrics = literal_eval(doNotDropMetrics_str)
                    if test_only:
                        logger.info('prometheus :: %s :: test_only evaled_doNotDropMetrics: %s' % (
                            str(now), str(evaled_doNotDropMetrics)))
                except:
                    evaled_doNotDropMetrics = None
                if isinstance(evaled_doNotDropMetrics, list):
                    doNotDropMetrics = []
                    for item in evaled_doNotDropMetrics:
                        if isinstance(item, list):
                            for i_item in item:
                                doNotDropMetrics.append(i_item)
                        if isinstance(item, str):
                            doNotDropMetrics.append(item)

        except Exception as err:
            logger.error('prometheus :: get_header(\'doNotDropMetrics\') error - %s' % str(err))
        if doNotDropMetrics:
            logger.info('prometheus :: %s :: doNotDropMetrics: %s' % (str(now), str(doNotDropMetrics)))

        # dropLabels
        dropLabels = []

        # labels_dropped = []
        labels_dropped = 0
        try:
            dropLabels_str = req.get_header('dropLabels')
            if dropLabels_str:
                dropLabels = literal_eval(dropLabels_str)
        except Exception as err:
            dropLabels_str = None
            try:
                dropLabels_str = req.get_header('dropLabels')
            except:
                dropLabels_str = 'failed to parse dropLabels'
            logger.error('error :: prometheus :: %s :: get_header(\'dropLabels\') error dropLabels_str: %s - %s' % (
                str(now), str(dropLabels_str), str(err)))
            message = 'Malformed dropLabels header'
            body = {"code": 400, "message": message}
            resp.text = json.dumps(body)
            logger.info('prometheus :: %s :: %s, returning 400' % (str(now), message))
            resp.status = falcon.HTTP_400
            return
        if dropLabels:
            logger.info('prometheus :: %s :: dropLabels: %s' % (str(now), str(dropLabels)))
            # added 20230210
            # Handle list and list of lists
            if isinstance(dropLabels, list):
                if len(dropLabels) == 1:
                    if isinstance(dropLabels, str):
                        dropLabels = [dropLabels]

        # Decompress and decode Prometheus WriteRequest data
        prometheus_data = {}
        prometheus_metadata = {}
        if content_encoding == 'snappy':
            try:
                key = req.get_header('key')
            except Exception as err:
                logger.error('prometheus :: get_header(\'key\') error - %s' % str(err))
            if not key:
                message = "no key header"
                body = {"code": 401, "message": "no key header"}
                resp.text = json.dumps(body)
                logger.info('prometheus :: %s, returning 401' % message)
                resp.status = falcon.HTTP_401
                return
            if key not in valid_keys:
                message = "invalid_key - %s" % str(key)
                body = {"code": 403, "message": "invalid key"}
                resp.text = json.dumps(body)
                logger.info('prometheus :: %s, returning 401' % message)
                resp.status = falcon.HTTP_403
                return

            try:
                prom_data = snappy.decompress(data)
                # logger.debug('debug :: prometheus :: data: %s' % str(data))
                if LOCAL_DEBUG:
                    logger.debug('debug :: prometheus :: %s :: prom_data available' % str(now))

                if LOCAL_DEBUG:
                    debug_key = 'flux.prom_data.debug.%s' % str(current_aligned_ts)
                    try:
                        redis_conn_decoded.hset(debug_key, time(), str(prom_data))
                        redis_conn_decoded.expire(debug_key, 300)
                    except Exception as err:
                        logger.error('error :: prometheus :: could not add prom_data to %s - %s' % (debug_key, str(err)))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: prometheus :: snappy.decompress(data) failed - %s' % err)
            try:
                prom_dataP_WriteRequest = remote_pb2.WriteRequest()
                prom_dataP_WriteRequest.ParseFromString(prom_data)
                debug_key = 'flux.prom_dataP_WriteRequest.debug.%s' % str(current_aligned_ts)
                prometheus_data = pb_json_format.MessageToDict(prom_dataP_WriteRequest)
                request_type = 'remote_write'
                if LOCAL_DEBUG:
                    logger.debug('debug :: prometheus :: %s :: request_type: %s' % (str(now), str(request_type)))

            except Exception as err:
                # logger.error(traceback.format_exc())
                logger.error('error :: prometheus :: could not parse prometheus write data - %s' % str(err))
                prometheus_data = None

            prometheus_metadata = {}
            try:
                prom_dataP_Metadata = remote_pb2.WriteRequest()
                prom_dataP_Metadata.ParseFromString(prom_data)
                if str(prom_dataP_Metadata).startswith('metadata'):
                    if LOCAL_DEBUG:
                        logger.debug('debug :: prometheus :: %s :: metadata requests' % (str(now)))
                    prometheus_metadata = pb_json_format.MessageToDict(prom_dataP_Metadata)
                    prometheus_metadata['tenant_id'] = '%s' % str(tenant_id)
                    prometheus_metadata['server_id'] = '%s' % str(server_id)
                    prometheus_metadata['server_url'] = '%s' % str(server_url)
                    metadata_key = 'flux.prometheus_metadata'

                    # @modified 20230202 - Feature #4840: flux - prometheus - x-test-only header
                    # Only submit metadata if it is not a test_only request
                    if test_only:
                        logger.info('prometheus :: %s :: test_only not processing metadata request' % str(now))
                        resp.status = falcon.HTTP_204
                        return

                    if not test_only:
                        try:
                            redis_conn_decoded.hset(metadata_key, time(), str(prometheus_metadata))
                            redis_conn_decoded.expire(metadata_key, 360)
                        except Exception as err:
                            logger.error('error :: prometheus :: could not add prom_data to %s - %s' % (metadata_key, str(err)))
            except Exception as err:
                logger.error('error :: prometheus :: could not parse prometheus write data Metadata - %s' % str(err))

        if LOCAL_DEBUG and prometheus_data:
            debug_key = 'flux.prometheus_data.debug.%s' % str(current_aligned_ts)
            try:
                redis_conn_decoded.hset(debug_key, time(), str(prometheus_data))
                redis_conn_decoded.expire(debug_key, 300)
            except Exception as err:
                logger.error('error :: prometheus :: could not add prometheus_data to %s - %s' % (debug_key, str(err)))
        #     logger.debug('debug :: prometheus :: WriteRequest prometheus_data: %s' % str(prometheus_data))

        prefix_metric = False
        influxdb_db = False

        if request_type == 'remote_storage_adapter':
            try:
                influxdb_db = request_params['db']
            except KeyError:
                influxdb_db = False
            except Exception as err:
                logger.error('error :: prometheus :: error determining request_params[\'db\'] - %s' % str(err))
                influxdb_db = False
            if influxdb_db:
                prefix_metric = str(influxdb_db)

        if request_type == 'remote_write':
            try:
                prefix_metric = req.get_header('prefix')
            except KeyError:
                prefix_metric = False
            except Exception as err:
                logger.error('error :: prometheus :: error determining request_params[\'prefix\'] - %s' % str(err))
                prefix_metric = False

        logger.info('prometheus :: %s :: %s request, prefix_metric: %s' % (str(now), request_type, str(prefix_metric)))

        auth_u = None
        auth_p = None
        if influxdb_db:
            try:
                authorization_str = req.get_header('authorization')
                if authorization_str:
                    auth = authorization_str.split(' ')[1]
                    auth_bytes = base64.b64decode(auth)
                    auth_str = auth_bytes.decode('utf-8')
                    auth_u = auth_str.split(':')[0]
                    auth_p = auth_str.split(':')[1]
                    if auth_p:
                        key = auth_p
            except Exception as err:
                logger.error('prometheus :: get_header(\'authorization\') error - %s' % str(err))
            if not key:
                body = {"code": 401, "message": "no key header"}
                resp.text = json.dumps(body)
                logger.info('prometheus :: %s, returning 401' % message)
                resp.status = falcon.HTTP_401
                return
            if key not in valid_keys:
                body = {"code": 403, "message": "invalid key"}
                resp.text = json.dumps(body)
                logger.info('prometheus :: %s, returning 401' % message)
                resp.status = falcon.HTTP_403
                return
        key_prefix = None
        if auth_p:
            try:
                key_prefix = flux_keys[auth_p]
            except KeyError:
                key_prefix = None
            except:
                key_prefix = None
        if key_prefix:
            logger.info('prometheus :: key_prefix found from authorization, prefixing with: %s' % str(key_prefix))
            if prefix_metric:
                if prefix_metric != key_prefix:
                    prefix_metric = '%s.%s' % (key_prefix, prefix_metric)
                else:
                    logger.info('prometheus :: key_prefix and prefix_metric the same, not adding key_prefix')
            else:
                prefix_metric = '%s' % key_prefix

        # Mitigate a Prometheus cardinality explosion
        metric_namespaces = []
        cardinality_metric_namespaces = []

        # metric name to dotted translation

        prometheus_metrics_list = []
        error_logged = False

        sample_logged = False

        # @added 20220617 - Task #2732: Prometheus to Skyline
        #                   Branch #4300: prometheus
        if horizonPostData:
            logger.info('prometheus :: %s :: processing prometheus_horizon_request' % str(now))
            try:
                horizon_prometheus_data = {
                    'prometheus_metrics_list': [],
                    'metric_namespaces': [],
                    'prometheus_metrics': {},
                }
                try:
                    horizon_prometheus_data = prometheus_horizon_request('flux', now, horizonPostData)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: prometheus :: prometheus_horizon_request failed - %s' % (
                        str(err)))
                prometheus_metrics_list = horizon_prometheus_data['prometheus_metrics_list']
                metric_namespaces = horizon_prometheus_data['metric_namespaces']
                prometheus_metrics = horizon_prometheus_data['prometheus_metrics']
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: prometheus :: processing prometheus_horizon_request failed - %s' % (
                    str(err)))
            logger.info('prometheus :: %s :: processed prometheus_horizon_request determined prometheus_metrics_list with %s metrics' % (
                str(now), str(len(prometheus_metrics_list))))

            logger.info('prometheus :: %s :: processing metric namespaces from prometheus_horizon_request' % str(now))
            error_logged = False
            for metric in list(prometheus_metrics.keys()):
                try:
                    metric_namespace = prometheus_metrics[metric]['metric_namespace']
                    try:
                        metric_namespaces_dict[metric_namespace].append(metric)
                        cardinality_metric_namespaces_dict[metric_namespace].append(metric)
                    except KeyError:
                        metric_namespaces_dict[metric_namespace] = [metric]
                        cardinality_metric_namespaces_dict[metric_namespace] = [metric]
                except Exception as err:
                    if not error_logged:
                        logger.error(traceback.format_exc())
                        try:
                            key_data = str(prometheus_metrics[metric])
                        except:
                            key_data = None
                        logger.error('error :: prometheus :: failed to determine metric_namespace from prometheus_metrics[%s]: %s - %s' % (
                            str(metric), str(key_data), str(err)))
                        error_logged = True
            logger.info('prometheus :: %s :: processed metric namespaces from prometheus_horizon_request' % str(now))

        timeseries_data = False
        prometheus_data_keys = []
        if prometheus_data:
            if LOCAL_DEBUG:
                logger.debug('debug :: prometheus :: %s :: prometheus_data data available' % (str(now)))

            if isinstance(prometheus_data, dict):
                prometheus_data_keys = list(prometheus_data.keys())
                if 'timeseries' in prometheus_data_keys:
                    timeseries_data = True
        if prometheus_data and not timeseries_data:
            logger.info('prometheus :: %s :: prometheus_data_keys: %s' % (
                str(now), str(prometheus_data_keys)))
        error_logged = False
        labels_checked_to_drop = 0
        sample_logged = False
        if prometheus_data and timeseries_data:
            if LOCAL_DEBUG:
                logger.debug('debug :: prometheus :: %s :: prometheus_data and timeseries_data available' % (str(now)))

            try:
                for index, prometheus_item_data in enumerate(prometheus_data['timeseries']):
                    if not sample_logged:
                        logger.debug('prometheus :: %s :: sample prometheus_item_data: %s' % (
                            str(now), str(prometheus_item_data)))
                        sample_logged = True

                    first_element_done = False
                    metric = None

                    # @added 20220819
                    # Some metrics do not pass the __name__ label first so
                    # determine metric name first before iterating the labels
                    for item in prometheus_data['timeseries'][index]['labels']:
                        try:
                            name = item['name']
                            if name == '__name__':
                                metric_namespace = str(item['value'])
                                metric = metric_namespace + '{'
                                metric = '%s{_tenant_id="%s",_server_id="%s",' % (metric_namespace, str(tenant_id), str(server_id))
                        except Exception as err:
                            if not error_logged:
                                logger.error(traceback.format_exc())
                                logger.error('error :: prometheus :: could not determine the __name__ label from WriteRequest item %s - %s' % (
                                    str(prometheus_data['timeseries'][index]), str(err)))
                                error_logged = True
                            metric = None
                            break

                    for item in prometheus_data['timeseries'][index]['labels']:
                        if not metric:
                            break
                        try:
                            name = item['name']
                            if name == 'monitor' and item['value'] == 'master':
                                labels_dropped += 1
                                continue
                            value = item['value']
                            if name == '__name__':
                                # @modified 20220819
                                # The __name__ label is not always passed first
                                # sometimes some determine metric name first and
                                # then iterate the labels
                                # metric_namespace = value
                                # metric = metric_namespace + '{'
                                # metric = '%s{_tenant_id="%s",_server_id="%s",' % (metric_namespace, str(tenant_id), str(server_id))
                                continue
                            else:
                                drop_label = False
                                if dropLabels:
                                    labels_checked_to_drop += 1
                                    for dropLabel in dropLabels:
                                        if name == dropLabel[0]:
                                            if dropLabel[1] in ['*', '.*']:
                                                # labels_dropped.append(value)
                                                labels_dropped += 1
                                                drop_label = True
                                                break
                                            pattern_match = False
                                            try:
                                                pattern_match, metric_matched_by = matched_or_regexed_in_list('flux', value, [dropLabel[1]])
                                                del metric_matched_by
                                            except Exception as err:
                                                if not error_logged:
                                                    logger.error(traceback.format_exc())
                                                    logger.error('error :: prometheus :: %s :: dropLabels - matched_or_regexed_in_list failed checking %s in %s - %s' % (
                                                        str(now), str(item), str(dropLabel[1]), err))
                                                    error_logged = True
                                            if pattern_match:
                                                # labels_dropped.append(value)
                                                labels_dropped += 1
                                                drop_label = True
                                                break
                                if drop_label:
                                    continue
                                if not first_element_done:
                                    metric = metric + item['name'] + '="' + item['value'] + '"'
                                else:
                                    metric = metric + ',' + item['name'] + '="' + item['value'] + '"'
                                if not first_element_done:
                                    first_element_done = True
                        except Exception as err:
                            if not error_logged:
                                logger.error(traceback.format_exc())
                                logger.error('error :: prometheus :: could not decode interpolate WriteRequest metric name %s, prometheus_item_data: %s - %s' % (
                                    str(prometheus_data['timeseries'][index]),
                                    str(prometheus_item_data), str(err)))
                                error_logged = True
                            metric = None
                            break
                    if not metric:
                        continue
                    metric = metric + '}'
                    if not metric:
                        continue

                    if len(metric) > 4096:
                        dropped_too_long.append(metric)
                        continue

                    # @modified 20230202 - Feature #4840: flux - prometheus - x-test-only header
                    # Only process cardinality if not test_only
                    if not test_only:
                        metric_namespaces.append(metric_namespace)
                        cardinality_metric_namespace = '%s.%s' % (str(tenant_id), metric_namespace)
                        cardinality_metric_namespaces.append(cardinality_metric_namespace)

                        try:
                            metric_namespaces_dict[metric_namespace].append(metric)
                            cardinality_metric_namespaces_dict[cardinality_metric_namespace].append(metric)
                        except KeyError:
                            metric_namespaces_dict[metric_namespace] = [metric]
                            cardinality_metric_namespaces_dict[cardinality_metric_namespace] = [metric]

                    for item in prometheus_data['timeseries'][index]['samples']:
                        value_present = False
                        try:
                            value_str = item['value']
                            value_present = True
                            # if value_str:
                            #     value = float(value_str)
                        except KeyError:
                            # @modified 20230224 - Bug #4856: flux - prometheus - no 0s
                            # The python protocol buffer does not deserialise a
                            # value key if it is 0. Prometheus does send a value
                            # key when the datapoint is 0, but after deserialising
                            # there is only a timestamp key, so ...
                            # if test_only:
                            #     test_metrics_with_no_values.append(metric)
                            # continue
                            value_str = '0.0'
                            value_present = True
                        except:
                            continue

                        value = None
                        if value_present:
                            try:
                                value = float(value_str)
                            except:
                                if test_only:
                                    test_metrics_with_no_values.append(metric)
                                continue

                        if str(value) == 'nan':
                            continue
                        try:
                            timestamp = int(item['timestamp'])
                        except:
                            continue
                        metric_list_data = '%s %s %s %s %s' % (
                            str(tenant_id), str(server_id), str(timestamp),
                            str(value), metric)
                        prometheus_metrics_list.append(metric_list_data)

                        # @added 20230202 - Feature #4840: flux - prometheus - x-test-only header
                        if test_only:
                            test_only_metrics_submitted.append(metric)

                logger.info('prometheus :: %s :: got %s metrics in %s request' % (
                    str(now), str(len(prometheus_metrics_list)), request_type))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: prometheus :: could not parse prometheus_data dict - %s' % str(err))

        remote_storage_adapter_data = []
        if request_type == 'remote_storage_adapter':
            try:
                metric_str = data.decode('utf-8')
                remote_storage_adapter_data = metric_str.split('\n')
            except Exception as err:
                logger.error('error :: prometheus :: could not decode remote_storage_adapter data - %s' % str(err))

        for raw_metric_data in remote_storage_adapter_data:
            # Skip blank lines
            if raw_metric_data == '':
                continue
            # influxdb format
            # metric_name = 'go_gc_duration_seconds,alias=Prometheus,instance=localhost:9090,job=prometheus,monitor=master,quantile=0.5'
            # However seeing as metric names can have spaces in them the string
            # must be reverse because the value and timestamp never have spaces
            try:
                # metric_name = raw_metric_data.split(' ')[0]
                reversed_metric_data = ''.join(reversed(raw_metric_data))
                r_timestamp_str = reversed_metric_data.split(' ')[0]
                timestamp = ''.join(reversed(r_timestamp_str))
                r_value_tag = reversed_metric_data.split(' ')[1]
                value_tag = ''.join(reversed(r_value_tag))
                value = value_tag.split('=')[1]
                remove_data = ' value=%s %s' % (str(value), str(timestamp))
                metric_name = raw_metric_data.replace(remove_data, '')

                metric_name_elements = metric_name.split(',')
                metric_namespace = metric_name_elements[0]
                prometheus_metric_name = '%s{' % metric_namespace
                remove_str = '%s,' % metric_name_elements[0]
                metric_labels_str = metric_name.replace(remove_str, '')
                labels_list = re.split(r"[\w]+=", metric_labels_str)
                label_names_list = metric_labels_str.split('=')
                label_names = []
                # Drop the last item
                for item in label_names_list[0:-1]:
                    item_list = item.split(',')
                    label_names.append(item_list[-1])
                labels = []
                for i in labels_list:
                    if i == '':
                        continue
                    labels.append(i)
                for index, label_name in enumerate(label_names):
                    try:
                        label_str = labels[index]
                        label = label_str.rstrip(',')
                        if index == 0:
                            prometheus_metric_name = '%s%s="%s"' % (prometheus_metric_name, label_name, label)
                        else:
                            # Drop the monitor="master" label as it does not exist
                            # on the source
                            if label_name == 'monitor':
                                if label == 'master':
                                    labels_dropped += 1
                                    continue
                            prometheus_metric_name = '%s,%s="%s"' % (prometheus_metric_name, label_name, label)
                    except:
                        continue
                prometheus_metric_name = prometheus_metric_name.replace('\\', '')

            except Exception as err:
                logger.warning('warning :: prometheus :: skipping, failed to determine name from raw_metric_data: %s - %s' % (
                    str(raw_metric_data), str(err)))
                prometheus_metric_name = None

            if not prometheus_metric_name:
                continue

            try:
                prometheus_metric_name = '%s}' % prometheus_metric_name

                # if prefix_metric:
                #     prometheus_metric_name = '%s.%s' % (prefix_metric, prometheus_metric_name)
                #     metric_namespace = '%s.%s' % (prefix_metric, metric_namespace)

                if len(metric) > 4096:
                    dropped_too_long.append(metric)
                    continue

                # @modified 20230202 - Feature #4840: flux - prometheus - x-test-only header
                # Only process cardinality if not test_only
                if not test_only:
                    metric_namespaces.append(metric_namespace)
                    cardinality_metric_namespace = '%s.%s' % (str(tenant_id), metric_namespace)
                    cardinality_metric_namespaces.append(cardinality_metric_namespace)

                    try:
                        metric_namespaces_dict[metric_namespace].append(prometheus_metric_name)
                        cardinality_metric_namespaces_dict[cardinality_metric_namespace].append(prometheus_metric_name)

                    except KeyError:
                        metric_namespaces_dict[metric_namespace] = [prometheus_metric_name]
                        cardinality_metric_namespaces_dict[cardinality_metric_namespace] = [prometheus_metric_name]

                # Use a Redis list and rpush entires
                # metric_data = '%s,%s' % (str(timestamp), str(value))
                # prometheus_metrics_list.append(metric_list_data)
                # Add to flux.prometheus_metrics Redis hash key in the following
                # format for Horizon prometheus to add to the Horizon worker queue
                # to submit to Redis, hence the strict and usual data format for the
                # hash key
                # redis_conn_decoded.hset('flux.prometheus_metrics', metric, str(timestamp,value))
                # The data is stored by flux in the Redis hash key as
                # 'metric': 'timestamp,value' to reduce the overhead of
                # needing to use literal_eval on every call in Horizon prometheus,
                # instead a simple str split is used
                metric_list_data = '%s %s %s %s %s' % (
                    str(tenant_id), str(server_id), str(timestamp), str(value),
                    prometheus_metric_name)
                prometheus_metrics_list.append(metric_list_data)

                # @added 20230202 - Feature #4840: flux - prometheus - x-test-only header
                if test_only:
                    test_only_metrics_submitted.append(prometheus_metric_name)

                # Add to flux.prometheus_metrics Redis list key in the following
                # format for Horizon prometheus to add to the Horizon worker queue
                # to submit to Redis, hence the strict and usual data format for the
                # hash key
                # The data is stored by flux in the Redis list key as
                # 'metric,timestamp,value' to reduce the overhead of
                # needing to use literal_eval on every call in Horizon prometheus,
                # instead a simple str split is used
            except Exception as err:
                logger.error('error :: prometheus :: failed to write data: %s - %s' % (
                    str(raw_metric_data), str(err)))
                # logger.debug('debug :: prometheus :: data: %s' % str(data))

        if request_type == 'remote_storage_adapter':
            logger.info('prometheus :: %s :: got %s metrics in %s request' % (
                str(now), str(len(prometheus_metrics_list)), request_type))

        logger.info('prometheus :: %s :: dropped %s labels, checked %s labels to drop' % (
            # str(now), str(len(labels_dropped)), str(labels_checked_to_drop)))
            str(now), str(labels_dropped), str(labels_checked_to_drop)))

        # Monitor for and mitigate against a Prometheus cardinality explosion
        logger.info('prometheus :: %s :: checking cardinality %s request data' % (str(now), request_type))
        max_cardinality = 1000
        max_cardinality = 5000
        removed_count = 0
        if prefix_metric and prefix_metric in external_settings_namespaces:
            try:
                for config_id in list(skyline_external_settings.keys()):
                    try:
                        current_namespace = skyline_external_settings[config_id]['namespace']
                        if current_namespace == prefix_metric:
                            max_cardinality = skyline_external_settings[config_id]['prometheus']['max_cardinality']
                    except:
                        pass
            except:
                pass

        # TODO: Deal with cardinality per tenant_id

        # Get namespace_cardinality_count
        current_namespace_cardinality_count = {}
        try:
            # global_namespace_cardinality_count = flux.get_namespace_cardinality_count()
            global_namespace_cardinality_count = get_namespace_cardinality_count()
            # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
            # current_namespace_cardinality_count = global_namespace_cardinality_count.copy()
            current_namespace_cardinality_count = copy.deepcopy(global_namespace_cardinality_count)
        except Exception as err:
            logger.error('error :: prometheus :: %s :: get_namespace_cardinality_count failed - %s' % (
                str(now), str(err)))
            current_namespace_cardinality_count = {}
        cardinality_timestamp = 0
        try:
            cardinality_timestamp = list(current_namespace_cardinality_count.keys())[-1]
        except:
            cardinality_timestamps = 0
        try:
            cardinality_timestamps = list(current_namespace_cardinality_count.keys())
        except:
            cardinality_timestamps = []
        # logger.debug('debug :: prometheus :: %s :: current_namespace_cardinality_count has timestamps: %s' % (
        #     str(now), str(cardinality_timestamps)))

        previous_namespace_cardinality_dict = {}
        try:
            previous_namespace_cardinality_dict = current_namespace_cardinality_count[last_aligned_ts]
        except:
            previous_namespace_cardinality_dict = {}
        previous_cardinality_dict = {}
        for namespace in list(previous_namespace_cardinality_dict.keys()):
            try:
                previous_cardinality_dict[namespace] = len(previous_namespace_cardinality_dict[namespace])
            except Exception as err:
                logger.error('error :: prometheus :: %s :: failed to determine last cardinality count for %s - %s' % (
                    str(now), namespace, str(err)))

        updated_global_cardinality_var = False
        if current_aligned_ts not in cardinality_timestamps:
            try:
                logger.info('prometheus :: %s :: creating new namespace_cardinality_count for %s last one was for %s with %s metric namespaces' % (
                    str(now), str(current_aligned_ts), str(cardinality_timestamp),
                    str(len(current_namespace_cardinality_count[cardinality_timestamp]))))
            except:
                logger.info('prometheus :: %s :: creating new namespace_cardinality_count for %s last one was for %s' % (
                    str(now), str(current_aligned_ts), str(cardinality_timestamp)))
            cardinality_timestamp = current_aligned_ts
            current_namespace_cardinality_count[cardinality_timestamp] = {}
            updated_global_cardinality_var = True
            if previous_cardinality_dict:
                try:
                    redis_conn_decoded.hset('flux.prometheus_namespace_cardinality.last', mapping=previous_cardinality_dict)
                    logger.info('prometheus :: %s :: hset flux.prometheus_namespace_cardinality.last Redis hash with %s namespace counts' % (
                        str(now), str(len(previous_cardinality_dict))))
                except Exception as err:
                    logger.error('error :: prometheus :: %s :: failed to hset flux.prometheus_namespace_cardinality.last Redis hash - %s' % (
                        str(now), str(err)))
        else:
            logger.info('prometheus :: %s :: got existing namespace_cardinality_count for %s with %s namespaces' % (
                str(now), str(current_aligned_ts), str(len(current_namespace_cardinality_count[cardinality_timestamp]))))

        processed_namespace_metrics_dict = {}
        try:
            processed_namespace_metrics_dict = current_namespace_cardinality_count[cardinality_timestamp]
        except:
            processed_namespace_metrics_dict = {}

        previous_namespace_cardinality_dict = {}
        if last_aligned_ts != current_aligned_ts:
            try:
                previous_namespace_cardinality_dict = current_namespace_cardinality_count[last_aligned_ts]
            except:
                previous_namespace_cardinality_dict = {}

        # Get Redis hashes once
        logger.info('prometheus :: %s :: getting cardinality Redis hashes' % str(now))
        previous_namespace_cardinality_counts_dict = {}
        if not previous_namespace_cardinality_dict:
            # The flux.prometheus_namespace_cardinality.last is created by horizon
            redis_key = 'flux.prometheus_namespace_cardinality.last'
            try:
                previous_namespace_cardinality_counts_dict = redis_conn_decoded.hgetall(redis_key)
            except Exception as err:
                logger.error('error :: prometheus :: %s :: could not hgetall Redis %s - %s' % (
                    str(now), redis_key, str(err)))
                previous_namespace_cardinality_counts_dict = {}
            if LOCAL_DEBUG:
                logger.debug('debug :: prometheus :: %s :: got %s metric namespaces from %s' % (
                    str(now), str(len(previous_namespace_cardinality_counts_dict)), redis_key))

            logger.debug('debug :: prometheus :: %s :: got %s metric namespaces from %s' % (
                str(now), str(len(previous_namespace_cardinality_counts_dict)), redis_key))

        get_redis_data = False

        exploding_namespaces = {}
        cardinality_breached_namespaces = []
        metric_namespaces = list(set(metric_namespaces))
        cardinality_metric_namespaces = list(set(cardinality_metric_namespaces))

        trace_logged = False
        namespaces_cardinality = {}
        try:
            for cardinality_namespace in cardinality_metric_namespaces:

                # @added 20230202 - Feature #4840: flux - prometheus - x-test-only header
                if test_only:
                    logger.info('prometheus :: %s :: test_only not processing cardinality' % str(now))
                    break

                try:
                    previous_namespace_cardinality = 0
                    try:
                        previous_namespace_cardinality = len(previous_namespace_cardinality_dict[cardinality_namespace])
                    except Exception as err:
                        if previous_namespace_cardinality_counts_dict:
                            try:
                                previous_namespace_cardinality = int(float(previous_namespace_cardinality_counts_dict[cardinality_namespace]))
                            except Exception as new_err:
                                if not trace_logged:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: prometheus :: %s :: could not determine previous_namespace_cardinality for %s - 1st err: %s, 2nd err: %s' % (
                                        str(now), str(cardinality_namespace), str(err), str(new_err)))
                                    trace_logged = True
                                previous_namespace_cardinality = 0
                        else:
                            previous_namespace_cardinality = 0

#                        previous_namespace_cardinality = 0
                    metric_names_list = []
                    try:
                        # metric_names_list = list(set(metric_namespaces_dict[namespace]))
                        metric_names_list = list(set(cardinality_metric_namespaces_dict[cardinality_namespace]))
                    except Exception as err:
                        logger.error('error :: prometheus :: %s :: could not determine metric_names_list for %s - %s' % (
                            str(now), str(cardinality_namespace), str(err)))
                        metric_names_list = []

                    processed_namespace_metrics = []
                    try:
                        processed_namespace_metrics = processed_namespace_metrics_dict[cardinality_namespace]
                    except:
                        processed_namespace_metrics = []

                    if processed_namespace_metrics:
                        total_metrics = len(list(set(metric_names_list + processed_namespace_metrics)))
                    else:
                        total_metrics = len(metric_names_list)
                    unrecorded_metrics = []
                    if processed_namespace_metrics:
                        try:
                            current_set = set(metric_names_list)
                            processed_set = set(processed_namespace_metrics)
                            set_difference = current_set.difference(processed_set)
                            unrecorded_metrics = list(set_difference)
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: prometheus :: %s :: failed to determine unrecorded_metrics - %s' % (
                                str(now), err))
                    else:
                        unrecorded_metrics = metric_names_list
                    if LOCAL_DEBUG:
                        logger.debug('debug :: prometheus :: %s :: there are %s unrecorded metrics for %s in this period' % (
                            str(now), str(len(unrecorded_metrics)), cardinality_namespace))
                    namespaces_cardinality[cardinality_namespace] = {}
                    namespaces_cardinality[cardinality_namespace]['current'] = len(metric_names_list)
                    namespaces_cardinality[cardinality_namespace]['processed_this_minute'] = len(processed_namespace_metrics)
                    namespaces_cardinality[cardinality_namespace]['total'] = total_metrics
                    namespaces_cardinality[cardinality_namespace]['unrecorded_metrics'] = unrecorded_metrics
                    namespaces_cardinality[cardinality_namespace]['processed_namespace_metrics'] = processed_namespace_metrics
                    namespaces_cardinality[cardinality_namespace]['kept_metrics'] = []
                    namespaces_cardinality[cardinality_namespace]['removed_metrics'] = []

                    cardinality_breached = False
                    if total_metrics > max_cardinality:
                        cardinality_breached = True
                        exploding_namespaces[cardinality_namespace] = {
                            'new_this_minute': len(metric_names_list),
                            'processed_this_minute': len(processed_namespace_metrics),
                            'previous_minute': previous_namespace_cardinality,
                            'breach': cardinality_breached,
                        }
                    else:
                        if previous_namespace_cardinality:
                            if total_metrics > (previous_namespace_cardinality * 20):
                                exploding_namespaces[cardinality_namespace] = {
                                    'current_minute': total_metrics,
                                    'previous_minute': previous_namespace_cardinality,
                                    'breach': cardinality_breached,
                                }
                    namespaces_cardinality[cardinality_namespace]['cardinality_breached'] = cardinality_breached
                    if cardinality_breached:
                        cardinality_breached_namespaces.append(cardinality_namespace)
                except Exception as err:
                    logger.error('error :: prometheus :: failed to process %s cardinality - %s' % (
                        cardinality_namespace, str(err)))
        except Exception as err:
            logger.error('error :: prometheus :: failed to process cardinality - %s' % err)

        prometheus_metrics_list_removed_breached = []
        removed_count = 0
        kept_count = 0
        trace_logged = False
        try:
            for item in prometheus_metrics_list:

                # @added 20230202 - Feature #4840: flux - prometheus - x-test-only header
                if test_only:
                    logger.info('prometheus :: %s :: test_only not processing cardinality breached' % str(now))
                    break

                items = item.split(' ')
                # Handle spaces in label values
                # metric = str(items[4])
                metric_list = items[4:]
                metric = ' '.join(metric_list)
                namespace = metric.split('{', maxsplit=1)[0]
                remove = False
                cardinality_namespace = '%s.%s' % (str(tenant_id), namespace)
                # if namespace in cardinality_breached_namespaces:
                if cardinality_namespace in cardinality_breached_namespaces:
                    remove = True
                    # Retain known metrics and drop unknown metrics
                    try:
                        current_metric_id = known_prometheus_metrics[metric]
                        if current_metric_id:
                            del current_metric_id
                            if kept_count < max_cardinality:
                                remove = False
                                kept_count += 1
                    except:
                        remove = True
                if cardinality_namespace not in list(namespaces_cardinality.keys()):
                    continue
                try:
                    if remove:
                        removed_count += 1
                        try:
                            del known_prometheus_metrics[metric]
                        except:
                            pass
                        # namespaces_cardinality[namespace]['removed_metrics'].append(metric)
                        namespaces_cardinality[cardinality_namespace]['removed_metrics'].append(metric)
                    else:
                        prometheus_metrics_list_removed_breached.append(item)
                        # namespaces_cardinality[namespace]['kept_metrics'].append(metric)
                        namespaces_cardinality[cardinality_namespace]['kept_metrics'].append(metric)
                except Exception as err:
                    if not trace_logged:
                        logger.error(traceback.format_exc())
                        trace_logged = True
                        try:
                            redis_key = 'flux.prometheus_namespaces_cardinality_errors.%s' % cardinality_namespace
                            redis_conn_decoded.setex(redis_key, 120, str(namespaces_cardinality))
                            logger.info('prometheus :: %s :: created %s' % (
                                str(now), str(redis_key)))
                        except:
                            pass
        except Exception as err:
            logger.error('error :: prometheus :: %s :: failed to determine cardinality breach metrics to remove - %s' % (
                str(now), str(err)))

        if removed_count:
            prometheus_metrics_list = list(prometheus_metrics_list_removed_breached)

        for cardinality_namespace in cardinality_metric_namespaces:

            # @added 20230202 - Feature #4840: flux - prometheus - x-test-only header
            if test_only:
                logger.info('prometheus :: %s :: test_only not processing cardinality' % str(now))
                break

            namespace_metrics = list(set(namespaces_cardinality[cardinality_namespace]['kept_metrics'] + namespaces_cardinality[cardinality_namespace]['processed_namespace_metrics']))
            redis_key = 'flux.prometheus_namespace_cardinality.%s' % str(current_aligned_ts)
            if get_redis_data:
                try:
                    redis_conn_decoded.hset(redis_key, cardinality_namespace, str(namespace_metrics))
                except Exception as err:
                    logger.error('error :: prometheus :: could not hset %s in Redis hash %s - %s' % (cardinality_namespace, redis_key, str(err)))
                try:
                    redis_conn_decoded.expire(redis_key, 300)
                except Exception as err:
                    logger.error('error :: prometheus :: could not expire to Redis %s - %s' % (redis_key, str(err)))

            current_namespace_cardinality_count[cardinality_timestamp][cardinality_namespace] = namespace_metrics

            c_error_logged = False
            if cardinality_namespace in cardinality_breached_namespaces:
                try:
                    removed_count = len(namespaces_cardinality[cardinality_namespace]['removed_metrics'])
                    exploding_namespaces[cardinality_namespace]['removed_count'] = removed_count
                    exploding_namespaces[cardinality_namespace]['kept_count'] = len(namespaces_cardinality[cardinality_namespace]['kept_metrics'])
                    exploding_namespaces[cardinality_namespace]['max_cardinality'] = max_cardinality
                    cardinality_breached_metric_count += removed_count
                    logger.warning('warning :: prometheus :: cardinality_breached on %s: %s' % (
                        namespace, str(exploding_namespaces[cardinality_namespace])))
                except Exception as err:
                    if not c_error_logged:
                        logger.error(traceback.format_exc())
                        logger.error('error :: prometheus :: could not manage exploding_namespaces for %s - %s' % (cardinality_namespace, str(err)))
                        c_error_logged = True

        # namespace_cardinality_count = {}
        # namespace_cardinality_count[current_aligned_ts] = {}

        # @added 20230202 - Feature #4840: flux - prometheus - x-test-only header
        if test_only:
            logger.info('prometheus :: %s :: test_only not updating global_cardinality' % str(now))

        # @added 20230202 - Feature #4840: flux - prometheus - x-test-only header
        # Only process if not test
        if not test_only:
            try:
                if updated_global_cardinality_var:
                    cardinality_timestamps = list(current_namespace_cardinality_count.keys())
                    for timestamp in sorted(cardinality_timestamps):
                        if len(list(current_namespace_cardinality_count.keys())) <= 2:
                            break
                        if timestamp == (current_aligned_ts - 60):
                            break
                        if timestamp == current_aligned_ts:
                            break
                        try:
                            logger.info('prometheus :: %s :: deleting %s entry from namespace_cardinality_count' % (
                                str(now), str(timestamp)))
                            del current_namespace_cardinality_count[timestamp]
                        except Exception as err:
                            logger.error('error :: prometheus :: %s :: could not delete %s entry from current_namespace_cardinality_count - %s' % (
                                str(now), str(timestamp), str(err)))
            except Exception as err:
                logger.error('error :: prometheus :: failed to prune namespace_cardinality_count - %s' % str(err))

            try:
                if current_namespace_cardinality_count:
                    current_namespace_cardinality_count = update_namespace_cardinality_count(current_namespace_cardinality_count)
                else:
                    logger.warning('warning :: prometheus :: %s :: no data to updated namespace_cardinality_count for %s' % (
                        str(now), str(current_aligned_ts)))
            except Exception as err:
                logger.error('error :: prometheus :: update_namespace_cardinality_count failed - %s' % str(err))
            logger.info('prometheus :: %s :: updated namespace_cardinality_count for %s with %s namespaces for tenant_id %s' % (
                str(now), str(current_aligned_ts), str(len(current_namespace_cardinality_count[cardinality_timestamp])),
                str(tenant_id)))

        # logger.debug('debug :: prometheus :: current_namespace_cardinality_count: %s' % (
        #     str(current_namespace_cardinality_count)))

        # Handle metric quota
        namespace_quota = 0
        namespace_metrics = []
        update_namespace_quota_metrics = False
        quota_namespace_metrics_redis_key = 'flux.quota.namespace_metrics.%s' % str(tenant_id)
        if tenant_id in namespaces_with_quotas:
            # namespace_quota = namespaces_with_quotas[tenant_id]
            try:
                namespace_quota = int(namespace_quotas_dict[tenant_id])
            except Exception as err:
                logger.error('error :: prometheus :: %s :: failed to determine namespace_quota - %s' % (
                    str(now), str(err)))
        if DEVELOPMENT and tenant_id == '123':
            # namespace_quota = 800
            namespace_quota = 30000

        # @added 20230202 - Feature #4840: flux - prometheus - x-test-only header
        if test_only:
            namespace_quota = 0
            logger.info('prometheus :: %s :: test_only not processing namespace_quota' % str(now))

        if namespace_quota:
            try:
                try:
                    namespace_metrics = list(redis_conn_decoded.smembers(quota_namespace_metrics_redis_key))
                except:
                    namespace_metrics = []
                    if get_memcache_key:
                        try:
                            namespace_metrics = get_memcache_key('flux', quota_namespace_metrics_redis_key)
                            if not namespace_metrics:
                                namespace_metrics = []
                        except Exception as err:
                            logger.error('error :: prometheus :: could get memcache set %s - %s' % (
                                quota_namespace_metrics_redis_key, err))
                            namespace_metrics = []
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: prometheus :: error determining namespace_metrics for quota, %s' % err)

        logger.info('prometheus :: %s :: checking skip metrics' % str(now))

        prometheus_metrics_list_removed_dropMetrics = []

        # Remove skip metrics
        skip_list = ALL_SKIP_LIST + dropMetrics
        do_not_skip_list = ALL_DO_NOT_SKIP_LIST + doNotDropMetrics
        # If there is external_settings skip_list use that
        if tenant_id in external_namespace_settings:
            skip_list = dropMetrics
            if 'skip_metrics' in external_namespace_settings[tenant_id]:

                # @modified 20230126 - Task #4828: EXTERNAL_SETTINGS - handle key value in external_settings
                #                      Feature #4000: EXTERNAL_SETTINGS
                skip_metrics_enabled = False
                skip_metrics_key = external_namespace_settings[tenant_id]['skip_metrics']
                if isinstance(skip_metrics_key, dict):
                    skip_metrics_enabled = False
                    try:
                        skip_metrics_enabled = external_namespace_settings[tenant_id]['skip_metrics']['enabled']
                    except KeyError:
                        skip_metrics_enabled = False
                    except Exception as err:
                        logger.error('error :: prometheus :: failed to determine if skip_metrics for %s is enabled - %s' % (
                            str(tenant_id), err))
                    if skip_metrics_enabled:
                        if 'exclude' in external_namespace_settings[tenant_id]['skip_metrics']:
                            skip_metrics_key = external_namespace_settings[tenant_id]['skip_metrics']['exclude']
                if isinstance(skip_metrics_key, list):
                    if len(skip_metrics_key) > 0:
                        skip_metrics_enabled = True

                # @modified 20230126 - Task #4828: EXTERNAL_SETTINGS - handle key value in external_settings
                #                      Feature #4000: EXTERNAL_SETTINGS
                # try:
                #    skip_list = list(set(external_namespace_settings[tenant_id]['skip_metrics'][] + dropMetrics))
                if skip_metrics_enabled:
                    try:
                        # @modified 20230202 - Feature #4840: flux - prometheus - x-test-only header
                        #                      Feature #4842: webapp - api - get_flux_test_metrics
                        #                      Task #4828: EXTERNAL_SETTINGS - handle key value in external_settings
                        # Handle a list of strings and a list of lists
                        # skip_metrics_list = [pattern for item in skip_metrics_key for pattern in item]
                        skip_metrics_list = []
                        for item in skip_metrics_key:
                            if isinstance(item, list):
                                for i_item in item:
                                    skip_metrics_list.append(i_item)
                            if isinstance(item, str):
                                skip_metrics_list.append(item)

                        skip_list = list(set(skip_metrics_list + dropMetrics))
                    except Exception as err:
                        logger.error('error :: prometheus :: failed to created skip_list from %s and %s - %s' % (
                            str(skip_metrics_key), str(dropMetrics), err))

            # @added 20230126 - Task #4828: EXTERNAL_SETTINGS - handle key value in external_settings
            #                   Feature #4000: EXTERNAL_SETTINGS
            # Added drop_metrics which is synonumous with skip_metrics
            if 'drop_metrics' in external_namespace_settings[tenant_id]:
                drop_metrics_enabled = False
                drop_metrics_key = external_namespace_settings[tenant_id]['drop_metrics']
                if isinstance(drop_metrics_key, dict):
                    drop_metrics_enabled = False
                    try:
                        drop_metrics_enabled = external_namespace_settings[tenant_id]['drop_metrics']['enabled']
                    except KeyError:
                        drop_metrics_enabled = False
                    except Exception as err:
                        logger.error('error :: prometheus :: failed to determine if drop_metrics for %s is enabled - %s' % (
                            str(tenant_id), err))
                    if drop_metrics_enabled:
                        if 'exclude' in external_namespace_settings[tenant_id]['drop_metrics']:
                            drop_metrics_key = external_namespace_settings[tenant_id]['drop_metrics']['exclude']
                if isinstance(drop_metrics_key, list):
                    if len(drop_metrics_key) > 0:
                        drop_metrics_enabled = True
                if drop_metrics_enabled:
                    try:
                        drop_metrics_list = [pattern for item in drop_metrics_key for pattern in item]
                        skip_list = list(set(skip_list + drop_metrics_list))
                    except Exception as err:
                        logger.error('error :: prometheus :: failed to created skip_list from %s and %s - %s' % (
                            str(drop_metrics_key), str(skip_list), err))

            do_not_skip_list = doNotDropMetrics
            if 'do_not_skip_metrics' in external_namespace_settings[tenant_id]:
                try:
                    do_not_skip_list = list(set(external_namespace_settings[tenant_id]['do_not_skip_metrics'] + doNotDropMetrics))
                except Exception as err:
                    logger.error('error :: prometheus :: failed to created do_not_skip_list from %s and %s - %s' % (
                        str(external_namespace_settings[tenant_id]['do_not_skip_metrics']),
                        str(doNotDropMetrics), err))

        if skip_list:
            # @added 20230202 - Feature #4840: flux - prometheus - x-test-only header
            if test_only:
                logger.info('prometheus :: %s :: test_only skip_list: %s' % (
                    str(now), str(skip_list)))

            error_logged = False
            # logger.info('prometheus :: %s :: removing metrics matching dropMetrics and skip_list: %s' % (str(now), str(skip_list)))
            # if do_not_skip_list:
            #     logger.info('prometheus :: %s :: not removing metrics matching doNotDropMetrics and do_not_skip_list: %s' % (
            #         str(now), str(do_not_skip_list)))

            prometheus_metrics_list_removed_dropMetrics = []
            prometheus_metrics_list_not_dropped = []
            for item in prometheus_metrics_list:
                items = item.split(' ')
                # Handle spaces in label values
                # metric = str(items[4])
                metric_list = items[4:]
                metric = ' '.join(metric_list)
                remove = False
                pattern_match = False
                try:
                    pattern_match, metric_matched_by = matched_or_regexed_in_list('flux', metric, skip_list)
                    del metric_matched_by
                except Exception as err:
                    if not error_logged:
                        logger.error(traceback.format_exc())
                        logger.error('error :: prometheus :: matched_or_regexed_in_list failed checking %s in %s - %s' % (
                            metric, str(skip_list), err))
                    pattern_match = False
                    error_logged = True
                if pattern_match:
                    # Check if the metric is in the do_not_skip_list
                    remove = True
                    if do_not_skip_list:
                        pattern_match = False
                        try:
                            pattern_match, metric_matched_by = matched_or_regexed_in_list('flux', metric, do_not_skip_list)
                            del metric_matched_by
                        except Exception as err:
                            if not error_logged:
                                logger.error(traceback.format_exc())
                                logger.error('error :: prometheus :: matched_or_regexed_in_list failed checking %s in %s - %s' % (
                                    metric, str(do_not_skip_list), err))
                            pattern_match = False
                            error_logged = True
                        if pattern_match:
                            remove = False
                if remove:
                    prometheus_metrics_list_removed_dropMetrics.append(metric)
                    try:
                        del known_prometheus_metrics[metric]
                    except:
                        pass
                    if namespace_quota and namespace_metrics:
                        # If a metric is added to be skipped and it is part of
                        # the namespace quota metrics remove it
                        if metric in namespace_metrics:
                            namespace_metrics.remove(metric)
                            update_namespace_quota_metrics = True
                    continue
                prometheus_metrics_list_not_dropped.append(item)
            prometheus_metrics_list = list(prometheus_metrics_list_not_dropped)
            logger.info('prometheus :: %s :: dropped %s metrics matching skip_list/dropMetrics patterns' % (
                str(now), str(len(prometheus_metrics_list_removed_dropMetrics))))
            if prometheus_metrics_list_removed_dropMetrics:
                redis_key = 'flux.prometheus_metrics.dropMetrics.%s' % str(current_aligned_ts)
                try:
                    redis_conn.sadd(redis_key, *prometheus_metrics_list_removed_dropMetrics)
                    redis_conn.expire(redis_key, 120)
                    logger.info('prometheus :: %s :: added %s metrics %s Redis set' % (
                        str(now), str(len(prometheus_metrics_list_removed_dropMetrics)),
                        redis_key))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    end = timer()
                    logger.error('error :: prometheus :: %s :: failed to add metrics to %s Redis set  - %s' % (
                        str(now), redis_key, str(err)))

        logger.info('prometheus :: %s :: checking namespace_quota: %s, len(namespace_metrics): %s, for %s' % (
            str(now), str(namespace_quota), str(len(namespace_metrics)),
            str(tenant_id)))

        # Check quota
        if namespace_quota:
            logger.debug('debug :: prometheus :: %s :: checking if metrics are in namespace_metrics' % (
                str(now)))

            prometheus_metrics_list_removed_over_quota = []
            prometheus_metrics_list_in_quota = []
            try:
                for item in prometheus_metrics_list:
                    # @modified 20230322 - Feature #4468: flux - remove_namespace_quota_metrics
                    #                      Feature #4464: flux - quota - cluster_sync
                    # Wrapped each item in try and except
                    try:
                        items = item.split(' ')
                        # Handle spaces in label values
                        # metric = str(items[4])
                        metric_list = items[4:]
                        metric = ' '.join(metric_list)
                        remove = False
                        if len(namespace_metrics) < namespace_quota:
                            namespace_metrics.append(metric)
                            update_namespace_quota_metrics = True
                            prometheus_metrics_list_in_quota.append(item)
                        # @modified 20230322 - Feature #4468: flux - remove_namespace_quota_metrics
                        #                      Feature #4464: flux - quota - cluster_sync
                        # if len(namespace_metrics) == namespace_quota:
                        if len(namespace_metrics) >= namespace_quota:
                            if metric in namespace_metrics:
                                prometheus_metrics_list_in_quota.append(item)
                            else:
                                remove = True
                        if remove:
                            prometheus_metrics_list_removed_over_quota.append(metric)
                            try:
                                del known_prometheus_metrics[metric]
                            except:
                                pass
                    except Exception as err:
                        logger.error('error :: prometheus :: %s :: error parsing item in prometheus_metrics_list - %s' % (
                            str(now), err))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: prometheus :: %s :: error determining which submitted metrics are in namespace_metrics quota, %s' % (
                    str(now), err))
            if prometheus_metrics_list_in_quota:
                logger.info('prometheus :: %s :: accepted %s metrics for tenant_id %s which are in quota' % (
                    str(now), str(len(prometheus_metrics_list_in_quota)),
                    str(tenant_id)))
                prometheus_metrics_list = list(prometheus_metrics_list_in_quota)
            if prometheus_metrics_list_removed_over_quota:
                logger.warning('prometheus :: %s :: will not accept %s metrics for tenant_id %s which are over quota' % (
                    str(now), str(len(prometheus_metrics_list_removed_over_quota)),
                    str(tenant_id)))
                redis_key = 'flux.prometheus_metrics.over_quota.%s.%s' % (tenant_id, str(current_aligned_ts))
                try:
                    redis_conn.sadd(redis_key, *set(prometheus_metrics_list_removed_over_quota))
                    redis_conn.expire(redis_key, 300)
                    logger.info('prometheus :: %s :: added %s metrics %s Redis set' % (
                        str(now), str(len(prometheus_metrics_list_removed_over_quota)),
                        redis_key))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: prometheus :: %s :: failed to add metrics to %s Redis set  - %s' % (
                        str(now), redis_key, str(err)))

        if update_namespace_quota_metrics:
            try:
                redis_conn_decoded.sadd(quota_namespace_metrics_redis_key, *set(namespace_metrics))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: prometheus :: %s :: failed to append to Redis set %s - %s' % (
                    str(now), quota_namespace_metrics_redis_key, err))

        if dropped_too_long:
            redis_key = 'flux.prometheus_metrics.dropped_too_long.%s' % str(current_aligned_ts)
            try:
                redis_conn.sadd(redis_key, *dropped_too_long)
                redis_conn.expire(redis_key, 300)
                logger.info('prometheus :: %s :: added %s metrics to %s Redis set' % (
                    str(now), str(len(dropped_too_long)), redis_key))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: prometheus :: %s :: failed to add metrics to %s Redis set from %s request - %s' % (
                    str(now), redis_key, request_type, str(err)))

        if request_type == 'horizon':
            redis_key = 'flux.horizon_prometheus_metrics.%s' % str(current_aligned_ts)
            logger.info('prometheus :: %s :: %s metrics from %s to add to %s' % (
                str(now), str(len(prometheus_metrics_list)), request_type, redis_key))
            if prometheus_metrics_list:
                try:
                    redis_conn.sadd(redis_key, *prometheus_metrics_list)
                    redis_conn.expire(redis_key, 120)
                    end = timer()
                    logger.info('prometheus :: %s :: added %s metrics from %s metric namespaces to %s Redis set from %s request in %.6f seconds' % (
                        str(now), str(len(prometheus_metrics_list)),
                        str(len(metric_namespaces)), redis_key, request_type, (end - start)))
                    prometheus_metrics_list = []
                except Exception as err:
                    logger.error(traceback.format_exc())
                    end = timer()
                    logger.error('error :: prometheus :: %s :: failed to add metrics to %s Redis set from %s request in %.6f seconds - %s' % (
                        str(now), redis_key, request_type, (end - start), str(err)))
                    prometheus_metrics_list = []
                    resp.status = falcon.HTTP_500

        # @added 20230202 - Feature #4840: flux - prometheus - x-test-only header
        if test_only:
            test_only_metrics_that_would_be_ingested = []
            for item in prometheus_metrics_list:
                items = item.split(' ')
                # Handle spaces in label values
                # metric = str(items[4])
                metric_list = items[4:]
                metric = ' '.join(metric_list)
                test_only_metrics_that_would_be_ingested.append(metric)
            test_only_metrics_that_would_be_ingested = list(set(test_only_metrics_that_would_be_ingested))
            test_only_redis_key = 'flux.prometheus.test_only.%s.%s' % (str(tenant_id), str(current_aligned_ts))
            test_only_data = {}
            try:
                test_only_data = redis_conn_decoded.hgetall(test_only_redis_key)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: prometheus :: %s :: failed to hgetall %s Redis hash - %s' % (
                    str(now), test_only_redis_key, err))
            if test_only_data:
                existing_test_metrics = []
                try:
                    existing_test_metrics = literal_eval(test_only_data['metrics'])
                except:
                    existing_test_metrics = []
                all_test_metrics = test_only_metrics_that_would_be_ingested + existing_test_metrics
                test_metrics_that_would_be_ingested = list(set(all_test_metrics))

                existing_dropped_metrics = []
                try:
                    existing_dropped_metrics = literal_eval(test_only_data['dropped'])
                except:
                    existing_dropped_metrics = []
                all_dropped_metrics = prometheus_metrics_list_removed_dropMetrics + existing_dropped_metrics
                test_metrics_that_would_be_dropped = list(set(all_dropped_metrics))
            else:
                test_metrics_that_would_be_ingested = list(test_only_metrics_that_would_be_ingested)
                test_metrics_that_would_be_dropped = list(set(prometheus_metrics_list_removed_dropMetrics))
            ingest_count = len(test_metrics_that_would_be_ingested)
            drop_count = len(test_metrics_that_would_be_dropped)
            test_only_data = {
                'tenant_id': str(tenant_id),
                'message': 'metric ingestion test, no metrics were ingested only counts are reported',
                'ingest_count': ingest_count,
                'drop_count': drop_count,
                'metrics': str(test_metrics_that_would_be_ingested),
                'dropped': str(test_metrics_that_would_be_dropped),
                'metrics_with_no_values': str(test_metrics_with_no_values),
                'no_values_count': len(test_metrics_with_no_values),
            }
            logger.info('prometheus :: %s :: test_only creating Redis hash %s with ingest_count: %s, drop_count: %s' % (
                str(now), test_only_redis_key, str(ingest_count),
                str(drop_count)))
            try:
                redis_conn_decoded.hset(test_only_redis_key, mapping=test_only_data)
                redis_conn.expire(test_only_redis_key, 300)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: prometheus :: %s :: failed to hset %s Redis hash - %s' % (
                    str(now), test_only_redis_key, err))
            end = timer()
            logger.info('prometheus :: %s :: test_only request completed in %.6f seconds' % (str(now), (end - start)))
            resp.text = json.dumps(test_only_data)
            resp.status = falcon.HTTP_200
            return

        if prometheus_metrics_list:
            try:
                # Use a Redis set
                # redis_conn.hset('test.flux.prometheus_metrics', mapping=prometheus_metrics)
                # logger.info('flux :: prometheus :: added %s metrics to test.flux.prometheus_metrics Redis hash' % str(len(list(prometheus_metrics.keys()))))
                redis_conn.sadd('flux.prometheus_metrics', *prometheus_metrics_list)
                logger.info('prometheus :: %s :: added %s metrics to flux.prometheus_metrics Redis hash from %s request' % (
                    str(now), str(len(prometheus_metrics_list)), request_type))
            except Exception as err:
                logger.error(traceback.format_exc())
                end = timer()
                logger.error('error :: prometheus :: %s :: failed to add metrics to flux.prometheus_metrics_list Redis hash from %s request in %.6f seconds - %s' % (
                    str(now), request_type, (end - start), str(err)))
                resp.status = falcon.HTTP_500
            tenant_key = '%s.%s' % (str(tenant_id), str(server_id))
            try:
                redis_conn.hset('flux.prometheus_metrics.servers', tenant_key, server_url)
            except Exception as err:
                logger.error('error :: prometheus :: %s :: failed to server_url flux.prometheus_metrics.servers Redis hash - %s' % (
                    str(now), str(err)))

        logger.info('prometheus :: %s :: handled request with mem_usage of %s kb' % (
            str(now), str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)))

        end = timer()
        logger.info('prometheus :: %s :: request completed in %.6f seconds' % (str(now), (end - start)))
        resp.status = falcon.HTTP_204
        return
