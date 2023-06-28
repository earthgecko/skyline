"""
listen.py
"""
import sys
import os
from time import time, sleep
import json
import traceback
# from multiprocessing import Queue
import string
from ast import literal_eval

# @added 20220209 - Feature #4284: flux - telegraf
from timeit import default_timer as timer
import gzip

# @added 20220722 - Task #4624: Change all dict copy to deepcopy
import copy

# @added 20221118 - Feature #4732: flux vortex
#                   Feature #4734: mirage_vortex
import uuid

import requests

from logger import set_up_logging

import falcon

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# @modified 20191115 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
if True:
    import settings
    import flux
    # @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
    # @modified 20210406 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
    # Added get_redis_conn_decoded
    from skyline_functions import get_redis_conn, get_redis_conn_decoded
    # @added 20220208 - Feature #4432: functions.metrics.skip_metric
    from functions.metrics.skip_metric import skip_metric

    # @added 20220405 - Feature #4516: flux - opentelemetry traces
    FLUX_OTEL_ENABLED = False
    try:
        FLUX_OTEL_ENABLED = settings.FLUX_OTEL_ENABLED
    except AttributeError:
        FLUX_OTEL_ENABLED = False
    except Exception as outer_err:
        FLUX_OTEL_ENABLED = False
    if FLUX_OTEL_ENABLED:
        from opentelemetry.proto.trace.v1 import trace_pb2
        from google.protobuf import json_format as pb_json_format

    # @added 20220426 - Feature #4536: Handle Redis failure
    if settings.MEMCACHE_ENABLED:
        from functions.memcache.get_memcache_key import get_memcache_key
        from functions.memcache.set_memcache_key import set_memcache_key
        from functions.memcache.delete_memcache_key import delete_memcache_key
        from functions.memcache.incr_memcache_key import incr_memcache_key
        from functions.memcache.append_memcache_key import append_memcache_key
    else:
        get_memcache_key = None
        set_memcache_key = None
        delete_memcache_key = None
        incr_memcache_key = None
        append_memcache_key = None

# @added 20200818 - Feature #3694: flux - POST multiple metrics
# Added validation of FLUX_API_KEYS
valid_keys = []
try:
    FLUX_SELF_API_KEY = settings.FLUX_SELF_API_KEY
    valid_keys.append(FLUX_SELF_API_KEY)
except:
    pass
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
        except:
            pass

# @added 20201006 - Feature #3764: flux validate metric name
try:
    FLUX_GRAPHITE_WHISPER_PATH = settings.FLUX_GRAPHITE_WHISPER_PATH
except:
    FLUX_GRAPHITE_WHISPER_PATH = '/opt/graphite/storage/whisper'

# added 20201016 - Feature #3788: snab_flux_load_test
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

# @added 20210406 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
try:
    # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
    # FLUX_AGGREGATE_NAMESPACES = settings.FLUX_AGGREGATE_NAMESPACES.copy()
    FLUX_AGGREGATE_NAMESPACES = copy.deepcopy(settings.FLUX_AGGREGATE_NAMESPACES)
    # aggregate_namespaces = list(FLUX_AGGREGATE_NAMESPACES.keys())
except:
    FLUX_AGGREGATE_NAMESPACES = {}
    # aggregate_namespaces = []
try:
    FLUX_EXTERNAL_AGGREGATE_NAMESPACES = settings.FLUX_EXTERNAL_AGGREGATE_NAMESPACES
except:
    FLUX_EXTERNAL_AGGREGATE_NAMESPACES = False

# @added 20221129 - Feature #4732: flux vortex
#                   Feature #4734: mirage_vortex
try:
    VORTEX_TIMESERIES_JSON_TO_DISK = settings.VORTEX_TIMESERIES_JSON_TO_DISK
except:
    VORTEX_TIMESERIES_JSON_TO_DISK = True
try:
    HORIZON_SHARDS = copy.deepcopy(settings.HORIZON_SHARDS)
except:
    HORIZON_SHARDS = {}
try:
    HORIZON_SHARD_DEBUG = settings.HORIZON_SHARD_DEBUG
except:
    HORIZON_SHARD_DEBUG = True
number_of_horizon_shards = 0
this_host = str(os.uname()[1])
HORIZON_SHARD = 0
if HORIZON_SHARDS:
    number_of_horizon_shards = len(HORIZON_SHARDS)
    HORIZON_SHARD = HORIZON_SHARDS[this_host]
number_of_horizon_test_shards = 0
HORIZON_TEST_SHARD = 0
try:
    HORIZON_TEST_SHARDS = copy.deepcopy(settings.HORIZON_TEST_SHARDS)
except:
    HORIZON_TEST_SHARDS = {}
if HORIZON_TEST_SHARDS:
    number_of_horizon_shards = len(HORIZON_TEST_SHARDS)
    HORIZON_TEST_SHARD = HORIZON_TEST_SHARDS[this_host]
if HORIZON_SHARDS or HORIZON_TEST_SHARDS:
    import zlib
try:
    VORTEX_ALLOWED_SHARD_TEST_KEYS = settings.VORTEX_ALLOWED_SHARD_TEST_KEYS
except:
    VORTEX_ALLOWED_SHARD_TEST_KEYS = ['URG3U7IrNo18VheSZwP7Jyvg0jWs55Sn']
try:
    VORTEX_MAX_ALGORITHMS = settings.VORTEX_MAX_ALGORITHMS
except:
    VORTEX_MAX_ALGORITHMS = 3

# @modified 20191129 - Branch #3262: py3
# Consolidate flux logging
# logger = set_up_logging('listen')
logger = set_up_logging(None)

LOCAL_DEBUG = False
# @added 20230531 - SAVE_DEBUG_INFO
SAVE_DEBUG_INFO = False

# @added 20220210 - Feature #4284: flux - telegraf
# This is a debug feature that allows to enable timing of functions to ensure
# performance is maintained as much as possible
TIMINGS = False

# @modified 20220209 - Feature #4284: flux - telegraf
# Allow :
# ALLOWED_CHARS = ['+', '-', '%', '.', '_', '/', '=']
ALLOWED_CHARS = ['+', '-', '%', '.', '_', '/', '=', ':']
for char in string.ascii_lowercase:
    ALLOWED_CHARS.append(char)
for char in string.ascii_uppercase:
    ALLOWED_CHARS.append(char)
for char in string.digits:
    ALLOWED_CHARS.append(char)

# @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
# @modified 20220426 - Feature #4536: Handle Redis failure
try:
    redis_conn = get_redis_conn('flux')
except:
    redis_conn = None
try:
    redis_conn_decoded = get_redis_conn_decoded('flux')
except:
    redis_conn_decoded = None

# @added 20220208 - Feature #4432: functions.metrics.skip_metric
#                   Feature #4400: flux - quota
external_settings_namespaces = []

# @added 20210406 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
aggregate_metrics = []
if FLUX_AGGREGATE_NAMESPACES or FLUX_EXTERNAL_AGGREGATE_NAMESPACES:
    # redis_conn_decoded = get_redis_conn_decoded('flux')
    try:
        aggregate_metrics = list(redis_conn_decoded.smembers('metrics_manager.flux.aggregate_metrics'))
        logger.info('listen :: there are %s metrics in the metrics_manager.flux.aggregate_metrics Redis set' % (
            str(len(aggregate_metrics))))
    except Exception as outer_err:
        logger.error('error :: listen :: could not get Redis set metrics_manager.flux.aggregate_metrics - %s' % str(outer_err))
        aggregate_metrics = []
        # @added 20220426 - Feature #4536: Handle Redis failure
        # Add flux required data to memcache as well
        if get_memcache_key:
            try:
                aggregate_metrics = get_memcache_key('flux', 'metrics_manager.flux.aggregate_metrics')
                if aggregate_metrics:
                    logger.info('listen :: there are %s metrics in the metrics_manager.flux.aggregate_metrics memcache set' % (
                        str(len(aggregate_metrics))))
                else:
                    aggregate_metrics = []
                    logger.warning('warning :: listen :: failed to get metrics_manager.flux.aggregate_metrics memcache list')
            except Exception as outer_err2:
                logger.error('error :: listen :: could not get memcache set metrics_manager.flux.aggregate_metrics - %s' % str(outer_err2))
                aggregate_metrics = []

external_flux_keys = {}
skyline_external_settings = {}

try:
    skyline_external_settings_raw = redis_conn_decoded.get('skyline.external_settings')
    if skyline_external_settings_raw:
        skyline_external_settings = literal_eval(skyline_external_settings_raw)
except Exception as outer_err:
    logger.error('error :: listen :: could not get Redis set skyline.external_settings - %s' % str(outer_err))
    skyline_external_settings = {}
    # @added 20220426 - Feature #4536: Handle Redis failure
    # Add flux required data to memcache as well
    if get_memcache_key:
        try:
            skyline_external_settings = get_memcache_key('flux', 'skyline.external_settings')
            if skyline_external_settings:
                logger.info('listen :: there are %s items in the skyline.external_settings memcache dict' % (
                    str(len(skyline_external_settings))))
            else:
                skyline_external_settings = {}
                logger.warning('warning :: listen :: failed to get skyline.external_settings memcache dict')
        except Exception as outer_err2:
            logger.error('error :: listen :: could not get memcache set skyline.external_settings - %s' % str(outer_err2))
            skyline_external_settings = {}

if skyline_external_settings:
    for settings_key in list(skyline_external_settings.keys()):
        external_setting_keys = list(skyline_external_settings[settings_key].keys())
        if 'flux_token' in external_setting_keys:
            flux_token = skyline_external_settings[settings_key]['flux_token']
            if flux_token:
                valid_keys.append(str(flux_token))
                external_flux_keys[flux_token] = skyline_external_settings[settings_key]['namespace']
        # @added 20220208 - Feature #4432: functions.metrics.skip_metric
        #                   Feature #4400: flux - quota
        if 'namespace' in external_setting_keys:
            try:
                external_settings_namespaces.append(skyline_external_settings[settings_key]['namespace'])
            except Exception as outer_err:
                logger.error('error :: listen :: could not append skyline_external_settings[\'%s\'][\'namespace\'] to external_settings_namespaces - %s' % (
                    str(settings_key), str(outer_err)))

# @added 20220208 - Feature #4432: functions.metrics.skip_metric
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
        if 'skip_metrics' in external_setting_keys:
            skip_metrics = skyline_external_settings[settings_key]['skip_metrics']
            if skip_metrics:
                if isinstance(skip_metrics, list):
                    if external_namespace:
                        for namespace in skip_metrics:
                            metric_namespaces = '%s.%s' % (external_namespace, namespace)
                            ALL_SKIP_LIST.append(metric_namespaces)
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
                        for namespace in do_not_skip_metrics:
                            metric_namespaces = '%s.%s' % (external_namespace, namespace)
                            ALL_DO_NOT_SKIP_LIST.append(metric_namespaces)
                    else:
                        ALL_DO_NOT_SKIP_LIST = ALL_DO_NOT_SKIP_LIST + do_not_skip_metrics
        else:
            skyline_external_settings[settings_key]['do_not_skip_metrics'] = []

# @added 20220126 - Feature #4400: flux - quota
namespace_quotas_dict = {}
try:
    namespace_quotas_dict = redis_conn_decoded.hgetall('metrics_manager.flux.namespace_quotas')
except Exception as outer_err:
    logger.error(traceback.format_exc())
    logger.error('error :: listen :: failed to hgetall metrics_manager.flux.namespace_quotas Redis hash key - %s' % (
        outer_err))
    namespace_quotas_dict = {}
    # @added 20220426 - Feature #4536: Handle Redis failure
    # Add flux required data to memcache as well
    if get_memcache_key:
        try:
            namespace_quotas_dict = get_memcache_key('flux', 'metrics_manager.flux.namespace_quotas')
            if namespace_quotas_dict:
                logger.info('listen :: there are %s items in the namespace_quotas_dict memcache dict' % (
                    str(len(namespace_quotas_dict))))
            else:
                logger.warning('warning :: listen :: failed to get namespace_quotas_dict memcache dict')
                namespace_quotas_dict = {}
        except Exception as outer_err2:
            logger.error('error :: listen :: could not get memcache dict metrics_manager.flux.namespace_quotas - %s' % str(outer_err2))
            namespace_quotas_dict = {}

namespaces_with_quotas = []
for namespace in list(namespace_quotas_dict.keys()):
    namespaces_with_quotas.append(namespace)

# @added 20220202 - Feature #4412: flux - quota - thunder alert
skyline_app = 'flux'
if namespaces_with_quotas:
    from functions.thunder.send_event import thunder_send_event
else:
    thunder_send_event = None

# @added 20220128 - Feature #4404: flux - external_settings - aggregation
#                   Feature #4324: flux - reload external_settings
#                   Feature #4376: webapp - update_external_settings
aggregate_namespaces_dict = {}
try:
    aggregate_namespaces_dict = redis_conn_decoded.hgetall('metrics_manager.flux.aggregate_namespaces')
except Exception as outer_err:
    logger.error(traceback.format_exc())
    logger.error('error :: listen :: failed to hgetall metrics_manager.flux.aggregate_namespaces Redis hash key - %s' % (
        outer_err))
    aggregate_namespaces_dict = {}
    # @added 20220426 - Feature #4536: Handle Redis failure
    # Add flux required data to memcache as well
    if get_memcache_key:
        try:
            aggregate_namespaces_dict = get_memcache_key('flux', 'metrics_manager.flux.aggregate_namespaces')
            if aggregate_namespaces_dict:
                logger.info('listen :: there are %s items in the aggregate_namespaces_dict memcache dict' % (
                    str(len(aggregate_namespaces_dict))))
            else:
                aggregate_namespaces_dict = {}
                logger.warning('warning :: listen :: failed to get metrics_manager.flux.aggregate_namespaces memcache dict')
        except Exception as outer_err2:
            logger.error('error :: listen :: could not get memcache dict metrics_manager.flux.aggregate_namespaces - %s' % str(outer_err2))
            aggregate_namespaces_dict = {}

aggregate_namespaces_list = []
for namespace in list(aggregate_namespaces_dict.keys()):
    aggregate_namespaces_list.append(namespace)


# @added 20220117 - Feature #4324: flux - reload external_settings
#                   Feature #4376: webapp - update_external_settings
def check_validate_key_update(caller):

    updated_valid_keys = []
    try:
        UPDATED_FLUX_SELF_API_KEY = settings.FLUX_SELF_API_KEY
        updated_valid_keys.append(UPDATED_FLUX_SELF_API_KEY)
    except:
        pass
    UPDATED_FLUX_API_KEYS = {}
    try:
        # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
        # UPDATED_FLUX_API_KEYS = settings.FLUX_API_KEYS.copy()
        UPDATED_FLUX_API_KEYS = copy.deepcopy(settings.FLUX_API_KEYS)
    except:
        pass
    if UPDATED_FLUX_API_KEYS:
        for updated_flux_api_key in UPDATED_FLUX_API_KEYS:
            try:
                updated_valid_keys.append(str(updated_flux_api_key))
            except:
                pass
    updated_external_flux_keys = {}
    updated_skyline_external_settings = {}
    try:
        updated_skyline_external_settings_raw = redis_conn_decoded.get('skyline.external_settings')
        if updated_skyline_external_settings_raw:
            updated_skyline_external_settings = literal_eval(updated_skyline_external_settings_raw)
    except Exception as e:
        logger.error('error :: listen :: %s - could get Redis set skyline.external_settings - %s' % (caller, str(e)))
        updated_skyline_external_settings = {}
        # @added 20220426 - Feature #4536: Handle Redis failure
        # Add flux required data to memcache as well
        if get_memcache_key:
            try:
                updated_skyline_external_settings = get_memcache_key('flux', 'skyline.external_settings')
                if not updated_skyline_external_settings:
                    updated_skyline_external_settings = {}
            except Exception as err:
                logger.error('error :: listen :: could get memcache set skyline.external_settings - %s' % str(err))
                updated_skyline_external_settings = {}

    if updated_skyline_external_settings:
        for updated_setting in list(updated_skyline_external_settings.keys()):
            if 'flux_token' in list(updated_skyline_external_settings[updated_setting].keys()):
                new_flux_token = updated_skyline_external_settings[updated_setting]['flux_token']
                if new_flux_token:
                    updated_valid_keys.append(str(new_flux_token))
                    updated_external_flux_keys[new_flux_token] = skyline_external_settings[updated_setting]['namespace']
    return (updated_valid_keys, updated_external_flux_keys)


def validate_key(caller, apikey):

    # @added 20200818 - Feature #3694: flux - POST multiple metrics
    # Added metric_namespace_prefix which is declared via the FLUX_API_KEYS
    metric_namespace_prefix = None

    keyValid = False

    try:
        isAlNum = False
        isAlNum = apikey.isalnum()
        if isAlNum:
            keyLength = len(apikey)
            if keyLength == 32:
                # Check to determine if it is a valid API key
                keyValid = True
            else:
                # @modified 20210421 - Task #4030: refactoring
                # semgrep - python-logger-credential-disclosure
                # logger.error('error :: %s :: invalid api key length of %s - %s' % (
                #     caller, str(keyLength), str(apikey)))
                logger.error('error :: %s :: invalid api key length' % (
                    caller))
        # @added 20200818 - Feature #3694: flux - POST multiple metrics
        # Added validation of FLUX_API_KEYS
        if valid_keys and keyValid:
            if apikey not in valid_keys:
                # @modified 20210421 - Task #4030: refactoring
                # semgrep - python-logger-credential-disclosure
                logger.error('error :: %s :: invalid api key - not known' % (
                    # caller, str(keyLength), str(apikey)))
                    caller))
                keyValid = False
            if keyValid:
                try:
                    metric_namespace_prefix = settings.FLUX_API_KEYS[apikey]
                except:
                    metric_namespace_prefix = None
            if apikey in list(external_flux_keys.keys()):
                try:
                    metric_namespace_prefix = external_flux_keys[apikey]
                except:
                    metric_namespace_prefix = None

        # @added 20220117 - Feature #4324: flux - reload external_settings
        #                   Feature #4376: webapp - update_external_settings
        update_keys = False
        if not keyValid:
            last_valid_keys_update = None
            update_external_settings_key = None
            try:
                last_valid_keys_update = redis_conn_decoded.get('flux.last_valid_keys_update')
            except Exception as err:
                if LOCAL_DEBUG:
                    logger.error('error :: listen :: could get flux.last_valid_keys_update from Redis - %s' % str(err))
                last_valid_keys_update = None
            if last_valid_keys_update:
                if int(last_valid_keys_update) < (int(time()) - 30):
                    logger.info('%s :: last valid key update check was more than 30 seconds ago, checking update' % (
                        caller))
                    update_keys = True
            if not update_keys:
                try:
                    update_external_settings_key = redis_conn_decoded.get('skyline.external_settings.update.flux')
                except Exception as err:
                    if LOCAL_DEBUG:
                        logger.error('error :: listen :: could get skyline.external_settings.update.flux from Redis - %s' % str(err))
                    last_valid_keys_update = None
                if update_external_settings_key:
                    logger.info('%s :: skyline.external_settings.update.flux, updating keys' % (
                        caller))
                    update_keys = True
        updated_valid_keys = []
        updated_external_flux_keys = []
        if update_keys:
            try:
                updated_valid_keys, updated_external_flux_keys = check_validate_key_update(caller)
            except Exception as err:
                if LOCAL_DEBUG:
                    logger.error('error :: listen :: check_validate_key_update failed - %s' % str(err))
                updated_valid_keys = []
                updated_external_flux_keys = []
        if updated_valid_keys:
            for uv_key in updated_valid_keys:
                valid_keys.append(uv_key)
        if updated_external_flux_keys:
            for uv_key in updated_external_flux_keys:
                valid_keys.append(uv_key)
        if update_keys and valid_keys:
            if apikey not in valid_keys:
                logger.error('error :: %s :: invalid api key - not known' % (
                    caller))
                keyValid = False
            if keyValid:
                try:
                    metric_namespace_prefix = settings.FLUX_API_KEYS[apikey]
                except:
                    metric_namespace_prefix = None
            if apikey in list(external_flux_keys.keys()):
                try:
                    metric_namespace_prefix = external_flux_keys[apikey]
                except:
                    metric_namespace_prefix = None

        if not keyValid:
            # @modified 20210421 - Task #4030: refactoring
            # semgrep - python-logger-credential-disclosure
            # logger.error('error :: %s :: invalid api key - %s' % (
            #    caller, str(apikey)))
            logger.error('error :: %s :: invalid api key' % (
                caller))
            # resp.status = falcon.HTTP_400
            # @modified 20200818 - Feature #3694: flux - POST multiple metrics
            # Added metric_namespace_prefix
            return False, metric_namespace_prefix
    except:
        logger.error(traceback.format_exc())
        # @modified 20210421 - Task #4030: refactoring
        # semgrep - python-logger-credential-disclosure
        # logger.error('error :: %s :: failed to validate api key - %s' % (
        #     caller, str(apikey)))
        logger.error('error :: %s :: failed to validate api key' % (
            caller))
        # resp.status = falcon.HTTP_400
        # @modified 20200818 - Feature #3694: flux - POST multiple metrics
        # Added metric_namespace_prefix
        return False, metric_namespace_prefix

    # @modified 20200818 - Feature #3694: flux - POST multiple metrics
    # Added metric_namespace_prefix
    return True, metric_namespace_prefix


def validate_timestamp(caller, timestamp):

    timestampInvalidReason = None

    try:
        timestamp = int(timestamp)
    except:
        logger.error('error :: %s :: the timestamp is not an int - %s' % (
            caller, str(timestamp)))
        timestampInvalidReason = 'timestamp is not an int'
        return False, timestampInvalidReason
    try:
        if len(str(timestamp)) != 10:
            logger.error('error :: %s :: the timestamp value is not 10 digits - %s' % (
                caller, str(timestamp)))
            timestampInvalidReason = 'timestamp is not a valid current unix timestamp'
            return False, timestampInvalidReason
        now = int(time())

        # @added 20200107 - Task #3376: Enable vista and flux to deal with lower frequency data
        try:
            flux_max_age = settings.FLUX_MAX_AGE
        except Exception as e:
            logger.error('error :: %s :: validate_timestamp FLUX_MAX_AGE is not set in settings.py, set to the default 3600 - %s' % (
                caller, e))
            flux_max_age = 3600

        # @modified 20200107 - Task #3376: Enable vista and flux to deal with lower frequency data
        # tooOld = now - settings.STALE_PERIOD
        tooOld = now - flux_max_age

        timestampValid = True
        timestampInvalidReason = 'None'
        if timestamp < tooOld:
            timestampInvalidReason = 'timestamp is too old, older than %s seconds' % str(flux_max_age)
            timestampValid = False
        # @modified 20200828 - Task #3718: Allow flux to handle timestamp in the same minute period
        # if timestamp > now:
        if timestamp > (now + 60):
            # @modified 20200828 - Task #3718: Allow flux to handle timestamp in the same minute period
            # timestampInvalidReason = 'timestamp is in the future'
            timestampInvalidReason = 'timestamp is more than 1 minute in the future'
            timestampValid = False
        if not timestampValid:
            logger.error('error :: %s :: timestamp in request data is not a valid timestamp - %s - %s' % (
                caller, str(timestampInvalidReason), str(timestamp)))
            return False, timestampInvalidReason
    except:
        logger.error('error :: %s :: validating timestamp argument value - %s' % (
            caller, str(timestamp)))
        return False, timestampInvalidReason
    return True, timestampInvalidReason


# @added 20201006 - Feature #3764: flux validate metric name
def validate_metric_name(caller, metric):

    # @modified 20220211 - Feature #4380: flux - return reason with 400
    # Added reason
    reason = None

    for vchar in metric:
        if vchar not in ALLOWED_CHARS:
            logger.error('error :: %s :: invalid char in metric - %s' % (
                caller, str(metric)))
            # @modified 20220211 - Feature #4380: flux - return reason with 400
            # Added reason
            reason = 'invalid characters in name'
            return False, reason
    # Check for prohibited chars
    # prohibitedTagChars = ';!^='
    # for char in metric:
    #     if char in prohibitedTagChars:
    #         return False

    # Linux has a maximum filename length of 255 characters for most filesystems
    # and a maximum path of 4096 characters.
    # @modified 20201020 - Feature #3764: flux validate metric name
    # By elements not by metric name
    # if (len(metric) + 4) > 255:   # +4 for .wsp
    metric_elements = metric.split('.')
    if (len(metric_elements[-1]) + 4) > 255:
        logger.error('error :: %s :: metric longer than 255 chars - %s' % (
            caller, str(metric)))
        # @modified 20220211 - Feature #4380: flux - return reason with 400
        # Added reason
        reason = 'longer than 255 characters'
        return False, reason
    metric_dir = metric.replace('.', '/')
    full_whisper_path = '%s/%s.wsp' % (FLUX_GRAPHITE_WHISPER_PATH, metric_dir)
    if len(full_whisper_path) > 4096:
        logger.error('error :: %s :: metric path longer than 4096 chars - %s' % (
            caller, str(full_whisper_path)))
        # @modified 20220211 - Feature #4380: flux - return reason with 400
        # Added reason
        reason = 'name too long'
        return False, reason
    return True, reason


# @added 20210406 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
# To reduce the overhead of making a redis query for every metric
# submitted, the metric name is checked against the initial loaded
# aggregate_metrics list, if not found in there then the elements of the
# metric name are checked to see if element could any possible match an
# aggregation namespace, only then is Redis queried.
# @modified 20220128 - Feature #4404: flux - external_settings - aggregation
#                      Feature #4324: flux - reload external_settings
#                     Feature #4376: webapp - update_external_settings
# def add_to_aggregate_metric_queue(metric, metric_data, aggregate_metrics_list):
def add_to_aggregate_metric_queue(
        metric, metric_data, aggregate_metrics_list, aggregate_namespaces_lst):
    added_to_aggregation_queue = False
    return_status_code = 204
    aggregate_metric = False
    # First check the list of aggregate_metrics created at startup from
    # the analyzer/metrics_manager metrics_manager.flux.aggregate_metrics Redis
    # set and if not in there try the Redis metrics_manager.flux.aggregate_metrics.hash
    if aggregate_metrics_list:
        if metric in aggregate_metrics_list:
            aggregate_metric = True

    # @modified 20220128 - Feature #4404: flux - external_settings - aggregation
    #                      Feature #4324: flux - reload external_settings
    #                     Feature #4376: webapp - update_external_settings
    # Do not use query Redis, use lists
    if not aggregate_metric and aggregate_namespaces_lst:
        for agg_namespace in aggregate_namespaces_lst:
            if metric.startswith(agg_namespace):
                aggregate_metric = True
                break

    aggregate_metric_timestamp = None

    # @modified 20220128 - Feature #4404: flux - external_settings - aggregation
    #                      Feature #4324: flux - reload external_settings
    #                     Feature #4376: webapp - update_external_settings
    # Do not use query Redis, use lists
    # if not aggregate_metric:
    if aggregate_metric_timestamp:
        try:
            # current_redis_conn_decoded = get_redis_conn_decoded('flux')
            aggregate_metric_timestamp = int(float(redis_conn_decoded.hget('metrics_manager.flux.aggregate_metrics.hash', metric)))
        except Exception as err:
            if LOCAL_DEBUG:
                logger.error('error :: listen :: could get metric %s from Redis hash metrics_manager.flux.aggregate_metrics.hash - %s' % (
                    metric, str(err)))
            aggregate_metric_timestamp = None
    if aggregate_metric_timestamp:
        try:
            if aggregate_metric_timestamp > (time() - 3600):
                aggregate_metric = True
            else:
                if aggregate_metric_timestamp < (time() - (3600 * 12)):
                    try:
                        redis_conn_decoded.hdel('metrics_manager.flux.aggregate_metrics.hash', metric)
                        logger.info('listen :: deleted metric %s from Redis hash metrics_manager.flux.aggregate_metrics.hash as entry is 12 hours old' % metric)
                    except Exception as e:
                        if LOCAL_DEBUG:
                            logger.error('error :: listen :: could not delete metric %s from Redis hash metrics_manager.flux.aggregate_metrics.hash - %s' % (metric, str(e)))
                        aggregate_metric_timestamp = None
        except:
            pass

    # @added 20210718
    # Added aggregate
    aggregate_passed = False
    try:
        aggregate_passed = metric_data[4]
        if aggregate_passed:
            aggregate_metric = True
    except IndexError:
        aggregate_passed = False
    except Exception as e:
        if LOCAL_DEBUG:
            logger.error('error :: listen :: add_to_aggregate_metric_queue could determine aggregate from metric_data for %s - %s' % (metric, str(e)))
        aggregate_passed = False

    # Add the metric to the flux/aggregator queue
    if aggregate_metric:
        try:
            redis_conn.sadd('flux.aggregator.queue', str(metric_data))
            added_to_aggregation_queue = True
            return_status_code = 204
            # if FLUX_VERBOSE_LOGGING:
            #     logger.info('listen :: data added to flux.aggregator.queue Redis set - %s' % str(metric_data))
        except Exception as err:
            # logger.error(traceback.format_exc())
            # logger.error('error :: listen :: failed to add data to flux.aggregator.queue Redis set - %s' % str(metric_data))
            logger.error('error :: listen :: failed to add data to flux.aggregator.queue Redis set - %s' % err)
            return_status_code = 500
            added_to_aggregation_queue = False
            # return added_to_aggregation_queue, return_status_code
            # @added 20220429 - Feature #4536: Handle Redis failure
            if settings.MEMCACHE_ENABLED:
                success = False
                memcache_data = '%s, ' % str(metric_data)
                try:
                    success = append_memcache_key('flux', 'flux.aggregator.queue', memcache_data)
                except Exception as err:
                    logger.error('error :: listen :: failed to append to memcache flux.aggregator.queue key - %s' % (str(err)))
                if success:
                    added_to_aggregation_queue = True
                    return_status_code = 204
                    if FLUX_VERBOSE_LOGGING:
                        logger.info('listen :: data added to flux.aggregator.queue memcache key - %s' % str(memcache_data))

    # @added 20220429 - Feature #4536: Handle Redis failure
    if added_to_aggregation_queue:
        return_status_code = 204
        if FLUX_VERBOSE_LOGGING:
            logger.info('listen :: data added to flux.aggregator.queue Redis set - %s' % str(metric_data))

    return added_to_aggregation_queue, return_status_code


# @added 20220126 - Feature #4400: flux - quota
def get_namespace_quota(metric_namespace_prefix):
    quota = 0
    try:
        quota_str = namespace_quotas_dict[metric_namespace_prefix]
        if quota_str:
            quota = int(quota_str)
    except KeyError:
        quota = 0
    except:
        quota = 0
    return quota


class MetricData(object):

    def on_get(self, req, resp):
        """
        The /flux/metric_data endpoint is called via a GET with the URI
        parameters as defined below, with the optional fill parameter to allow
        flux to backfill airgap data for a metric:
        /flux/metric_data?metric=<metric|str>&timestamp=<timestamp|int>&value=<value|float>&key=<key|str>[&fill=true]
        For example:
        /flux/metric_data?metric=vista.nodes.skyline-1.cpu.user&timestamp=1478021700&value=1.0&key=YOURown32charSkylineAPIkeySecret
        /flux/metric_data?metric=vista.nodes.skyline-1.cpu.user&timestamp=1478021700&value=1.0&key=YOURown32charSkylineAPIkeySecret&fill=true
        """

        if LOCAL_DEBUG:
            logger.info('listen :: GET request - %s' % str(req.query_string))

        # @added 20220210 - Feature #4284: flux - telegraf
        start_request_timer = timer()

        # @modified 20200115 - Feature #3394: flux health check
        # Added status parameter so that the flux listen process can be monitored
        # @modified 20200206 - Feature #3444: Allow flux to backfill
        # Added the fill parameter
        # @modified 20230223 - Feature #4840: flux - prometheus - x-test-only header
        # Added test argument
        validGetArguments = ['key', 'metric', 'value', 'timestamp', 'status', 'fill', 'test']

        payload = {}
        payload['query_string'] = str(req.query_string)

        apikey = False
        metric = False
        value = None
        valid_value = False
        timestamp = None

        # @added 20200206 - Feature #3444: Allow flux to backfill
        # This needs to be iterated amd determined before the timestamp is
        # checked
        backfill = False
        for request_param_key, request_param_value in req.params.items():
            try:
                if str(request_param_key) == 'fill':
                    fill = str(request_param_value)
                    if fill == 'true':
                        backfill = True
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: could not validate the fill key GET request argument - %s' % (
                    str(req.query_string)))
                resp.status = falcon.HTTP_400
                return

        for request_param_key, request_param_value in req.params.items():
            try:
                if str(request_param_key) not in validGetArguments:
                    logger.error('error :: listen :: invalid key in request arguments - %s - %s' % (
                        str(request_param_key), str(req.query_string)))

                    # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                    unique_value = '%s' % str(time())
                    try:
                        redis_conn.sadd('flux.listen.discarded.invalid_parameters', str(unique_value))
                        if FLUX_VERBOSE_LOGGING:
                            logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_parameters' % str(unique_value))
                    except Exception as err:
                        logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_parameters - %s' % err)

                    # @added 20220118 - Feature #4380: flux - return reason with 400
                    message = 'Invalid parameter received - %s' % str(request_param_key)
                    body = {"code": 400, "message": message}
                    resp.text = json.dumps(body)
                    logger.info('listen :: %s, returning 400' % message)

                    resp.status = falcon.HTTP_400
                    return
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: validating request arguments - %s' % (
                    str(req.query_string)))

                # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                unique_value = '%s' % str(time())
                try:
                    redis_conn.sadd('flux.listen.discarded.invalid_parameters', str(unique_value))
                    if FLUX_VERBOSE_LOGGING:
                        logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_parameters' % str(unique_value))
                except Exception as e:
                    logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_parameters - %s' % e)

                # @added 20220118 - Feature #4380: flux - return reason with 400
                message = 'Invalid parameters'
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.info('listen :: %s, returning 400' % message)

                resp.status = falcon.HTTP_400
                return

            # @added 20200115 - Feature #3394: flux health check
            # Added status parameter
            try:
                if str(request_param_key) == 'status':
                    logger.info('listen :: GET %s - ok' % str(req.query_string))

                    # @added 20220329 - Feature #4018: thunder - skyline.errors
                    # Report app up
                    try:
                        redis_conn.setex('flux.listen', 120, int(time()))
                    except Exception as err:
                        logger.error('error :: listen :: set flux.listen failed - %s' % err)

                    body = {"status": "ok"}
                    resp.text = json.dumps(body)
                    resp.status = falcon.HTTP_200
                    return
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: could not validate the status key GET request argument - %s' % (
                    str(req.query_string)))
                resp.status = falcon.HTTP_400
                return

            # @modified 20200818 - Feature #3694: flux - POST multiple metrics
            # Added metric_namespace_prefix
            metric_namespace_prefix = None

            try:
                if str(request_param_key) == 'key':
                    key = str(request_param_value)
                    # @modified 20200818 - Feature #3694: flux - POST multiple metrics
                    # Added metric_namespace_prefix
                    keyValid, metric_namespace_prefix = validate_key('listen :: MetricData GET', key)
                    if not keyValid:
                        logger.error('error :: listen :: invalid key in GET request arguments - %s - %s' % (
                            str(request_param_value), str(req.query_string)))

                        # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                        unique_value = '%s' % str(time())
                        try:
                            redis_conn.sadd('flux.listen.discarded.invalid_key', str(unique_value))
                            if FLUX_VERBOSE_LOGGING:
                                logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_key' % str(unique_value))
                        except Exception as e:
                            logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_key - %s' % e)

                        # @added 20220118 - Feature #4380: flux - return reason with 400
                        message = 'invalid key'
                        body = {"code": 401, "message": message}
                        resp.text = json.dumps(body)
                        logger.info('listen :: %s, returning 400' % message)

                        # @modified 20210909
                        # Return 401 if bad key
                        # resp.status = falcon.HTTP_400
                        resp.status = falcon.HTTP_401
                        return
            except:
                logger.error(traceback.format_exc())

                # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                unique_value = '%s' % str(time())
                try:
                    redis_conn.sadd('flux.listen.discarded.invalid_key', str(unique_value))
                    if FLUX_VERBOSE_LOGGING:
                        logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_key' % str(unique_value))
                except Exception as e:
                    logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_key - %s' % e)

                # @added 20220118 - Feature #4380: flux - return reason with 400
                message = 'invalid key'
                body = {"code": 401, "message": message}
                resp.text = json.dumps(body)
                logger.info('listen :: %s, returning 400' % message)

                # @modified 20210909 - Feature #4380: flux - return reason with 400
                # Return 401 if bad key
                # resp.status = falcon.HTTP_400
                resp.status = falcon.HTTP_401
                return

            try:
                if str(request_param_key) == 'timestamp':
                    # @modified 20200206 - Feature #3444: Allow flux to backfill
                    # Only valid_timestamp is this is not a backfill request
                    if not backfill:
                        # @modified 20220114 - lint message
                        valid_timestamp_message = 'listen :: MetricData GET - parameter - %s' % str(request_param_value)
                        # valid_timestamp = validate_timestamp('listen :: MetricData GET - parameter - %s', str(request_param_value))
                        # @modified 20220211 - Feature #4380: flux - return reason with 400
                        # Added reason
                        valid_timestamp, reason = validate_timestamp(valid_timestamp_message, str(request_param_value))
                    else:
                        valid_timestamp = True
                    if valid_timestamp:
                        timestamp = int(request_param_value)
                    else:
                        # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                        unique_value = None
                        for r_param_key, r_param_value in req.params.items():
                            if str(r_param_key) == 'metric':
                                metric = str(r_param_value)
                        if not metric:
                            metric = str(time())
                        try:
                            unique_value = '%s.%s' % (str(metric), str(request_param_value))
                            redis_conn.sadd('flux.listen.discarded.invalid_timestamp', str(unique_value))
                            if FLUX_VERBOSE_LOGGING:
                                logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_timestamp' % str(unique_value))
                        except Exception as e:
                            logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_timestamp - %s' % e)

                        # @added 20220118 - Feature #4380: flux - return reason with 400
                        # @modified 20220211 - Feature #4380: flux - return reason with 400
                        # Added reason
                        message = 'invalid timestamp - %s - %s' % (str(request_param_value), str(reason))
                        body = {"code": 400, "message": message}
                        resp.text = json.dumps(body)
                        logger.info('listen :: %s, returning 400' % message)

                        resp.status = falcon.HTTP_400
                        return
            except:
                logger.error('error :: listen :: invalid timestamp value from the GET request argument - %s - %s' % (
                    str(request_param_value), str(req.query_string)))

                # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                unique_value = None
                if not metric:
                    for r_param_key, r_param_value in req.params.items():
                        if str(r_param_key) == 'metric':
                            metric = str(r_param_value)
                if not metric:
                    metric = str(time())
                try:
                    unique_value = '%s.%s' % (str(metric), str(request_param_value))
                    redis_conn.sadd('flux.listen.discarded.invalid_timestamp', str(unique_value))
                    if FLUX_VERBOSE_LOGGING:
                        logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_timestamp' % str(unique_value))
                except Exception as e:
                    logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_timestamp - %s' % e)

                # @added 20220118 - Feature #4380: flux - return reason with 400
                message = 'invalid timestamp'
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.info('listen :: %s, returning 400' % message)

                resp.status = falcon.HTTP_400
                return

            if str(request_param_key) == 'metric':
                metric = str(request_param_value)
                # @added 20201006 - Feature #3764: flux validate metric name
                # @modified 20201207 - Task #3864: flux - try except everything
                try:
                    # @modified 20220211 - Feature #4380: flux - return reason with 400
                    # Added reason
                    valid_metric_name, reason = validate_metric_name('listen :: MetricData GET', str(request_param_key))
                except Exception as e:
                    logger.error('error :: listen :: could validate_metric_name - %s - %s' % (str(request_param_key), str(e)))
                    valid_metric_name = False

                if not valid_metric_name:

                    # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                    if not metric:
                        metric = str(time())
                    try:
                        redis_conn.sadd('flux.listen.discarded.metric_name', str(metric))
                        if FLUX_VERBOSE_LOGGING:
                            logger.info('listen :: added %s to Redis set flux.listen.discarded.metric_name' % str(metric))
                    except Exception as e:
                        logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.metric_name - %s' % e)

                    # @added 20220118 - Feature #4380: flux - return reason with 400
                    # @modified 20220211 - Feature #4380: flux - return reason with 400
                    # Added reason
                    message = 'invalid metric - %s - %s' % (str(request_param_value), str(reason))
                    body = {"code": 400, "message": message}
                    resp.text = json.dumps(body)
                    logger.info('listen :: %s, returning 400' % message)

                    resp.status = falcon.HTTP_400
                    return

            if str(request_param_key) == 'value':
                try:
                    value = float(request_param_value)
                    # valid_value is used as in terms of Python if value evalatuion
                    # if value was 0.0 (or 0) they evaluate as False
                    valid_value = True
                except:
                    logger.error('error :: listen :: invalid value from GET request argument - %s - %s' % (
                        str(request_param_value), str(req.query_string)))

                    # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                    if not metric:
                        metric = str(time())
                    unique_value = '%s.%s' % (str(metric), str(request_param_value))
                    try:
                        redis_conn.sadd('flux.listen.discarded.invalid_value', str(unique_value))
                        if FLUX_VERBOSE_LOGGING:
                            logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_value' % str(unique_value))
                    except Exception as e:
                        logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_value - %s' % e)

                    # @added 20220118 - Feature #4380: flux - return reason with 400
                    message = 'invalid value - %s' % str(request_param_value)
                    body = {"code": 400, "message": message}
                    resp.text = json.dumps(body)
                    logger.info('listen :: %s, returning 400' % message)

                    resp.status = falcon.HTTP_400
                    return

            request_param_key_str = str(request_param_key)
            payload[request_param_key_str] = str(request_param_value)

        if not key:
            logger.error('error :: listen :: no key in the GET request arguments - %s - returning 400' % (
                str(req.query_string)))

            # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
            unique_value = '%s' % str(time())
            try:
                redis_conn.sadd('flux.listen.discarded.invalid_key', str(unique_value))
                if FLUX_VERBOSE_LOGGING:
                    logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_key' % str(unique_value))
            except Exception as e:
                logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_key - %s' % e)

            # @added 20220118 - Feature #4380: flux - return reason with 400
            message = 'no key parameter passed'
            body = {"code": 400, "message": message}
            resp.text = json.dumps(body)
            logger.info('listen :: %s, returning 400' % message)

            resp.status = falcon.HTTP_400
            return
        if not metric:
            logger.error('error :: listen :: no metric in the GET request arguments - %s - returning 400' % (
                str(req.query_string)))

            # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
            metric = str(time())
            try:
                redis_conn.sadd('flux.listen.discarded.metric_name', str(metric))
                if FLUX_VERBOSE_LOGGING:
                    logger.info('listen :: added %s to Redis set flux.listen.discarded.metric_name' % str(metric))
            except Exception as e:
                logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.metric_name - %s' % e)

            # @added 20220118 - Feature #4380: flux - return reason with 400
            message = 'no metric parameter passed'
            body = {"code": 400, "message": message}
            resp.text = json.dumps(body)
            logger.info('listen :: %s, returning 400' % message)

            resp.status = falcon.HTTP_400
            return
        if not valid_value:
            logger.error('error :: listen :: no valid value in the GET request arguments - %s - returning 400' % (
                str(req.query_string)))

            # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
            if not metric:
                metric = str(time())
            unique_value = '%s.%s' % (str(metric), str(time()))
            try:
                redis_conn.sadd('flux.listen.discarded.invalid_value', str(unique_value))
                if FLUX_VERBOSE_LOGGING:
                    logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_value' % str(unique_value))
            except Exception as e:
                logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_value - %s' % e)

            # @added 20220118 - Feature #4380: flux - return reason with 400
            message = 'no value parameter passed'
            body = {"code": 400, "message": message}
            resp.text = json.dumps(body)
            logger.info('listen :: %s, returning 400' % message)

            resp.status = falcon.HTTP_400
            return

        # TODO
        # token? in request header Authorization
        # Validate key and token
        # API keys and tokens stored in memcache and populated by worker?
        # Fetch key token
        token = None
        try:
            # token = redis_conn.get(key)
            token = False
        except:
            token = False

        if not timestamp:
            timestamp = int(time())

        # @added 20220128 - Feature #4400: flux - quota
        namespace_quota = 0
        current_namespace_metric_count = 0
        namespace_metrics = []
        namespace_metrics_count = 0
        rejected_metric_over_quota = None
        check_namespace_quota = None

        # @added 20220426 - Feature #4536: Handle Redis failure
        # Add flux required data to memcache as well
        quota_metrics_from_memcache = False

        try:
            if metric_namespace_prefix:
                check_namespace_quota = str(metric_namespace_prefix)
            else:
                check_namespace_quota = metric.split('.', maxsplit=1)[0]
            if check_namespace_quota in namespaces_with_quotas:
                namespace_quota = get_namespace_quota(check_namespace_quota)
            if namespace_quota:
                namespace_quota_key_reference_timestamp = (int(time()) // 60 * 60)
                namespace_quota_redis_key = 'flux.namespace_quota.%s.%s' % (
                    check_namespace_quota, str(namespace_quota_key_reference_timestamp))
                current_namespace_metric_count = 0
                try:
                    current_namespace_metric_count = redis_conn_decoded.scard(namespace_quota_redis_key)
                except:
                    current_namespace_metric_count = 0
                quota_namespace_metrics_redis_key = 'flux.quota.namespace_metrics.%s' % check_namespace_quota
                try:
                    namespace_metrics = list(redis_conn_decoded.smembers(quota_namespace_metrics_redis_key))
                except:
                    namespace_metrics = []
                    # @added 20220426 - Feature #4536: Handle Redis failure
                    # Add flux required data to memcache as well
                    if get_memcache_key:
                        try:
                            namespace_metrics = get_memcache_key('flux', quota_namespace_metrics_redis_key)
                            if not namespace_metrics:
                                namespace_metrics = []
                            quota_metrics_from_memcache = True
                        except Exception as err:
                            logger.error('error :: listen :: could get memcache set %s - %s' % (
                                quota_namespace_metrics_redis_key, err))
                            namespace_metrics = []

                namespace_metrics_count = len(namespace_metrics)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: listen :: error determining namespace_quota, %s' % err)
        if namespace_quota:
            if current_namespace_metric_count >= namespace_quota:
                logger.info('listen :: will not accept new metric in this minute, over quota')
                rejected_metric_over_quota = metric
            if not rejected_metric_over_quota:
                try:
                    if metric in namespace_metrics:
                        new_metric_count = namespace_metrics_count + 0
                    else:
                        new_metric_count = namespace_metrics_count + 1
                    if new_metric_count > namespace_quota:
                        logger.info('listen :: will not accept new metric, over quota')
                        rejected_metric_over_quota = metric
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: error determining new metric for namespace_quota, %s' % err)
        if rejected_metric_over_quota:
            body = {
                'code': 400,
                'error': 'over metric quota',
                'rejected metric': rejected_metric_over_quota
            }
            try:
                message = 'over quota - this account is allowed %s metrics and an additional metric was posted that has been rejected' % (
                    str(namespace_quota))
                body = {
                    'code': 400,
                    'error': message,
                    'rejected metric': rejected_metric_over_quota
                }
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: error building 400 body, %s' % err)
            resp.text = json.dumps(body)
            logger.info('listen :: %s, returning 400 on metric_namespace_prefix: %s' % (
                message, str(check_namespace_quota)))
            resp.status = falcon.HTTP_400
            return

        # @added 20220128 - Feature #4400: flux - quota
        if namespace_quota:
            try:
                if metric not in namespace_metrics:
                    try:
                        redis_conn.sadd(quota_namespace_metrics_redis_key, metric)
                    except Exception as err:
                        logger.error('error :: listen :: failed to add %s to Redis set %s, %s' % (
                            metric, quota_namespace_metrics_redis_key, err))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: error adding metric to quota_namespace_metrics_redis_key, %s' % err)
            try:
                redis_conn.sadd(namespace_quota_redis_key, metric)
                redis_conn.expire(namespace_quota_redis_key, 60)
            except Exception as err:
                logger.error('error :: listen :: failed to add %s to Redis set %s, %s' % (
                    metric, namespace_quota_redis_key, err))

        # @added 20210430 - Bug #4046: flux - metric_namespace_prefix on FLUX_SELF_API_KEY conflicting with FLUX_API_KEYS
        # If metrics are being submitted to flux internally (vista) using
        # the FLUX_SELF_API_KEY for a namespace if that namespace is added
        # to FLUX_API_KEYS, flux will begin to also append the namespace
        # prefix from the FLUX_API_KEYS for metrics submitted with the
        # FLUX_SELF_API_KEY
        if metric_namespace_prefix:
            if metric.startswith(metric_namespace_prefix):
                metric_namespace_prefix = None

        # @modified 20200818 - Feature #3694: flux - POST multiple metrics
        # Added metric_namespace_prefix which is declared via the FLUX_API_KEYS
        if metric_namespace_prefix:
            metric = '%s.%s' % (str(metric_namespace_prefix), metric)

        # metric to add to queue
        # @modified 20200206 - Feature #3444: Allow flux to backfill
        # Added backfill
        # @modified 20201207 - Task #3864: flux - try except everything
        try:
            metric_data = [metric, value, timestamp, backfill]
            payload['metric_data'] = str(metric_data)
        except Exception as e:
            logger.error('error :: listen :: failed to add metric_data to payload - %s' % str(e))

        # @added 20230223 - Feature #4840: flux - prometheus - x-test-only header
        test_only = False
        test_only_metrics_submitted = []
        try:
            test_only_str = req.get_header('x-test-only')
            if test_only_str:
                test_only = True
        except Exception as err:
            logger.error('listen :: get_header(\'x-test-only\') error - %s' % str(err))
        for request_param_key, request_param_value in req.params.items():
            if not test_only:
                try:
                    if str(request_param_key) == 'test':
                        test_only_str = str(request_param_value)
                        if test_only_str == 'true':
                            test_only = True
                except Exception as err:
                    logger.error('listen :: get_header(\'x-test-only\') error - %s' % str(err))
        if test_only:
            logger.info('listen :: test_only request')
            test_only_metrics_submitted.append(metric)
            current_aligned_ts = int(int(time()) // 60 * 60)
            test_only_redis_key = 'flux.prometheus.test_only.%s.%s' % (str(metric_namespace_prefix), str(current_aligned_ts))
            test_only_data = {}
            try:
                test_only_data = redis_conn_decoded.hgetall(test_only_redis_key)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: failed to hgetall %s Redis hash - %s' % (
                    test_only_redis_key, err))
            if test_only_data:
                existing_test_metrics = []
                try:
                    existing_test_metrics = literal_eval(test_only_data['metrics'])
                except:
                    existing_test_metrics = []
                all_test_metrics = test_only_metrics_submitted + existing_test_metrics
                test_metrics_that_would_be_ingested = list(set(all_test_metrics))
                existing_dropped_metrics = []
                try:
                    existing_dropped_metrics = literal_eval(test_only_redis_key['dropped'])
                except:
                    existing_dropped_metrics = []
                all_dropped_metrics = existing_dropped_metrics
                test_metrics_that_would_be_dropped = list(set(all_dropped_metrics))
            else:
                test_metrics_that_would_be_ingested = test_only_metrics_submitted
                test_metrics_that_would_be_dropped = []
            ingest_count = len(test_metrics_that_would_be_ingested)
            drop_count = len(test_metrics_that_would_be_dropped)
            test_only_data = {
                'namespace': str(metric_namespace_prefix),
                'message': 'metric ingestion test, no metrics were ingested only counts are reported',
                'ingest_count': ingest_count,
                'drop_count': drop_count,
                'metrics': str(test_metrics_that_would_be_ingested),
                'dropped': str(test_metrics_that_would_be_dropped),
            }
            logger.info('listen :: test_only creating Redis hash %s with ingest_count: %s, drop_count: %s' % (
                test_only_redis_key, str(ingest_count),
                str(drop_count)))
            try:
                redis_conn_decoded.hset(test_only_redis_key, mapping=test_only_data)
                redis_conn.expire(test_only_redis_key, 300)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: failed to hset %s Redis hash - %s' % (
                    test_only_redis_key, err))
            logger.info('listen :: test_only request completed')
            resp.text = json.dumps(test_only_data)
            resp.status = falcon.HTTP_200
            return

        # @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
        # Add to data to the flux.queue Redis set
        if FLUX_PERSIST_QUEUE and metric_data:
            try:
                redis_conn.sadd('flux.queue', str(metric_data))
            except:
                pass

        # @added 20210406 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
        # To reduce the overhead of making a redis query for every metric
        # submitted, the metric name is checked against the initial loaded
        # aggregate_metrics list, if not found in there then the elements of the
        # metric name are checked to see if element could any possible match an
        # aggregation namespace, only then is Redis queried.
        if aggregate_metrics or aggregate_namespaces_list:
            added_to_aggregation_queue, return_status_code = add_to_aggregate_metric_queue(metric, metric_data, aggregate_metrics, aggregate_namespaces_list)
            if added_to_aggregation_queue:
                if return_status_code == 500:
                    resp.status = falcon.HTTP_500
                if return_status_code == 204:
                    resp.status = falcon.HTTP_204
                if LOCAL_DEBUG:
                    try:
                        queue_size = flux.httpMetricDataAggregatorQueue.qsize()
                        logger.info('listen :: flux.httpMetricDataAggregatorQueue.qsize - %s' % str(queue_size))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: listen :: failed to determine flux.httpMetricDataAggregatorQueue.qsize')
                return

        # Queue the metric
        try:
            flux.httpMetricDataQueue.put(metric_data, block=False)
            # modified 20201016 - Feature #3788: snab_flux_load_test
            if FLUX_VERBOSE_LOGGING:
                logger.info('listen :: GET request data added to flux.httpMetricDataQueue - %s' % str(metric_data))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: listen :: failed to add GET request data to flux.httpMetricDataQueue - %s' % str(metric_data))
            resp.status = falcon.HTTP_500
            return
        # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
        try:
            redis_conn.incr('flux.listen.added_to_queue')
        except Exception as e:
            # logger.error('error :: listen :: failed to increment to Redis key flux.listen.added_to_queue - %s' % e)
            # @added 20220428 - Feature #4536: Handle Redis failure
            if settings.MEMCACHE_ENABLED:
                try:
                    incr_memcache_key('flux', 'flux.listen.added_to_queue', 1)
                except:
                    pass

        if LOCAL_DEBUG:
            try:
                queue_size = flux.httpMetricDataQueue.qsize()
                logger.info('listen :: flux.httpMetricDataQueue.qsize - %s' % str(queue_size))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: failed to determine flux.httpMetricDataQueue.qsize')

        # @added 20220210 - Feature #4284: flux - telegraf
        end_request_timer = timer()
        if FLUX_VERBOSE_LOGGING:
            try:
                logger.info('listen :: GET request took %.6f seconds to process' % (
                    (end_request_timer - start_request_timer)))
            except Exception as err:
                logger.error('error :: listen :: failed to calculate request time - %s' % err)

        if LOCAL_DEBUG:

            # @modified 20201207 - Task #3864: flux - try except everything
            try:
                resp.text = json.dumps(payload)
                resp.status = falcon.HTTP_200
            except Exception as e:
                logger.error('error :: listen :: failed to json.dumps(payload) - %s' % str(e))
                resp.status = falcon.HTTP_500
        else:
            resp.status = falcon.HTTP_204
        return


class MetricDataPost(object):

    def on_post(self, req, resp):
        """
        The /flux/metric_data_post endpoint is called via a POST with a json
        object as defined below, with the optional fill parameter to allow
        flux to backfill airgap data for a metric:

        For example::

            {
                "metric": "metric|str",
                "timestamp": timestamp|int,
                "value": value|float,
                "key": "api_key|str",
                "fill": "boolean|optional"
            }

        For example::

            {
                "metric": "vista.nodes.skyline-1.cpu.user",
                "timestamp": 1478021700,
                "value": 1.0,
                "key": "YOURown32charSkylineAPIkeySecret"
            }

            {
                "metric": "vista.nodes.skyline-1.cpu.user",
                "timestamp": 1478021700,
                "value": 1.0,
                "key": "YOURown32charSkylineAPIkeySecret",
                "fill": "true"
            }

        """
        try:
            postData = None

            post_req_start = int(time())

            # @added 20220210 - Feature #4284: flux - telegraf
            start_request_timer = timer()

            # @added 20210512 - Feature #4060: skyline.flux.worker.discarded metrics
            # Preserve the request for debugging
            postData_obj = None
            try:
                postData_obj = req.stream.read()
            except Exception as err:
                logger.error('error :: listen :: req.stream.read() failed - %s' % err)

            # @added 20220208 - Feature #4284: flux - telegraf
            # Allow for gzip data
            # DEBUG
            # all_headers = None
            # try:
            #     all_headers = str(req.headers)
            # except Exception as err:
            #     logger.error('listen :: req.headers() error - %s' % str(err))
            # logger.info('debug :: listen :: headers - %s' % str(all_headers))
            content_encoding = None
            try:
                content_encoding = req.get_header('content-encoding')
            except Exception as err:
                logger.error('listen :: get_header(\'content-encoding\') error - %s' % str(err))
            # if content_encoding:
            #     logger.info('debug :: listen :: content_encoding: %s' % str(content_encoding))
            # else:
            #     logger.info('debug :: listen :: no content_encoding: %s' % str(content_encoding))
            if content_encoding == 'gzip':
                if TIMINGS:
                    logger.info('debug :: listen :: content_encoding: %s, decompressing' % str(content_encoding))
                    start_gzip_timer = timer()
                try:
                    postData_obj = gzip.decompress(postData_obj)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: gzip.decompress(postData_obj) failed - %s' % err)
                if TIMINGS:
                    end_gzip_timer = timer()
                    logger.debug('debug :: listen :: gzip decompression took %.6f seconds' % (end_gzip_timer - start_gzip_timer))

            try:
                # @added 20210512 - Feature #4060: skyline.flux.worker.discarded metrics
                # postData = json.loads(req.stream.read())
                postData = json.loads(postData_obj)
            except json.decoder.JSONDecodeError:
                # @added 20220118 - Feature #4380: flux - return reason with 400
                message = 'invalid json data'
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.info('listen :: %s, returning 400' % message)

                resp.status = falcon.HTTP_400
                return

            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: invalid post data - %s' % err)
            if LOCAL_DEBUG:
                logger.debug('debug :: listen :: request arguments and POST data - %s - %s' % (
                    str(req.query_string), str(postData)))

            if not postData:
                if LOCAL_DEBUG:
                    logger.debug('debug :: listen :: no POST data recieved')

                # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                unique_value = '%s' % str(time())
                try:
                    redis_conn.sadd('flux.listen.discarded.invalid_parameters', str(unique_value))
                    if FLUX_VERBOSE_LOGGING:
                        logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_parameters no POST data' % str(unique_value))
                except Exception as err:
                    logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_parameters no POST data- %s' % err)

                try:
                    redis_conn.set('flux.listen.discarded.invalid_post_data', str(postData_obj))
                except Exception as err:
                    logger.error('error :: listen :: failed to data to Redis set flux.listen.discarded.invalid_post_data - %s' % err)

                # @added 20220118 - Feature #4380: flux - return reason with 400
                message = 'no POST data received'
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.info('listen :: %s, returning 400' % message)

                resp.status = falcon.HTTP_400
                return

            # Add metric to add to queue
            metric = None
            timestamp = None
            value = None
            valid_value = None
            key = None
            # @added 20200206 - Feature #3444: Allow flux to backfill
            backfill = False

            # @added 20210718
            aggregate = False

            # @added 20220208 - Feature #4284: flux - telegraf
            telegraf = False
            telegraf_metric_prefix = None

            # @added 20220222 - Feature #4284: flux - telegraf
            # Drop telegraf metrics with a string/boolean value
            dropped_telegraf_non_numeric_value_metrics = {}
            dropped_telegraf_non_numeric_value_base_names = {}
            metrics_added_to_aggregation = []

            # @added 20211018 - Feature #4284: flux - telegraf
            # Allow the key to be passed as a HTTP header rather than in the json
            # payload
            header_apikey = None
            try:
                header_apikey = req.get_header('key')
            except Exception as err:
                logger.error('listen :: get_header(\'key\') error - %s' % str(err))
            if header_apikey:
                # FOR DEBUG
                # logger.info('debug :: listen :: get_header(\'key\') - %s' % str(header_apikey))
                # @added 20220208 - Feature #4284: flux - telegraf
                try:
                    telegraf = req.get_header('telegraf')
                except Exception as err:
                    logger.error('listen :: get_header(\'telegraf\') error - %s' % str(err))
                if telegraf:
                    logger.info('listen :: telegraf header - %s' % str(telegraf))
                    if LOCAL_DEBUG:
                        logger.debug('debug :: listen :: get_header(\'telegraf\') - %s' % str(telegraf))
                try:
                    telegraf_metric_prefix = req.get_header('prefix')
                except Exception as err:
                    logger.error('listen :: get_header(\'prefix\') error - %s' % str(err))

            # @added 20200115 - Feature #3394: flux health check
            # Added status parameter so that the flux listen process can be monitored
            status = None
            try:
                status = str(postData['status'])
            except KeyError:
                status = False
            if status:

                status_code = None
                try:
                    status_code = postData['status_code']
                    logger.info('listen :: status status_code test, %s' % (
                        str(status_code)))
                except KeyError:
                    status_code = None
                if status_code:
                    status_code = int(status_code)
                    data = 'Status code test - %s' % str(status_code)
                    if status_code == 207:
                        rejected_metrics_over_quota = ['server-1.unknown.metric', 'server-1.some-other-unknown.metric']
                        processed_metrics = ['server-1.known.metric', 'server-1.some-other-known.metric']
                        data = []
                        for metric in rejected_metrics_over_quota:
                            data.append({'metric': metric, 'status': 400})
                        for metric in processed_metrics:
                            data.append({'metric': metric, 'status': 204})
                        message = 'TEST - over quota - this account is allowed x metrics and an additional y new metrics were posted that have been rejected'
                        notice_msg = 'TEST - 2 metrics were processed and 2 metrics were rejected'
                        body = {
                            'code': status_code,
                            'data': data,
                            'error': message,
                            'message': notice_msg,
                            'rejected metrics': rejected_metrics_over_quota,
                            'rejected metrics count': 2,
                            'processed metrics': processed_metrics,
                            'processed metrics count': 2,
                            'submitted metrics count': 4
                        }
                    else:
                        body = {
                            'code': status_code,
                            'data': data,
                        }
                    resp.text = json.dumps(body)
                    logger.info('listen :: status - status_code test: %s' % (
                        str(body)))
                    # logger.debug('debug :: listen :: %s' % str(body))
                    if status_code == 207:
                        resp.status = falcon.HTTP_207
                    elif status_code == 400:
                        resp.status = falcon.HTTP_400
                    elif status_code == 401:
                        resp.status = falcon.HTTP_401
                    elif status_code == 200:
                        resp.status = falcon.HTTP_200
                    else:
                        resp.status = falcon.HTTP_204
                    return

                try:
                    logger.info('listen :: POST status - ok')

                    # @added 20220329 - Feature #4018: thunder - skyline.errors
                    # Report app up
                    try:
                        redis_conn.setex('flux.listen', 120, int(time()))
                    except Exception as err:
                        logger.error('error :: listen :: setex on flux.listen failed - %s' % err)

                    body = {"status": "ok"}
                    resp.text = json.dumps(body)
                    resp.status = falcon.HTTP_200
                    return
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: could not validate the status key POST request argument - %s - %s' % (
                        str(req.query_string), err))
                    resp.status = falcon.HTTP_500
                    return

            # @added 20200818 - Feature #3694: flux - POST multiple metrics
            metric_namespace_prefix = None

            # @added 20211018 - Feature #4284: flux - telegraf
            # Allow the key to be passed as a HTTP header rather than in the json
            # payload
            if header_apikey:
                key = header_apikey
            else:
                key = None
                try:
                    key = str(postData['key'])
                except KeyError:
                    key = None
                    # @added 20220118 - Feature #4380: flux - return reason with 400
                    message = 'no key passed'
                    body = {"code": 400, "message": message}
                    resp.text = json.dumps(body)
                    logger.info('listen :: %s, returning 400' % message)
                    resp.status = falcon.HTTP_400
                    return

                except Exception as err:
                    logger.error(traceback.format_exc())
                    # @modified 20220222 - Feature #4284: flux - telegraf
                    # Never log POST data as it can be massive
                    # logger.error('error :: listen :: could not determine the key from POST data - %s - %s' % (
                    #     str(postData), err))
                    logger.error('error :: listen :: could not determine the key from POST data - %s' % (
                        err))

            try:
                # @modified 20211018 - Feature #4284: flux - telegraf
                # Allow the key to be passed as a HTTP header or in the json payload
                # key = str(postData['key'])

                # @modified 20200818 - Feature #3694: flux - POST multiple metrics
                # Added metric_namespace_prefix
                keyValid, metric_namespace_prefix = validate_key('listen :: MetricDataPOST POST', key)
                if not keyValid:
                    # @modified 20220222 - Feature #4284: flux - telegraf
                    # Never log POST data as it can be massive
                    # logger.error('error :: listen :: invalid key in POST data - %s - %s' % (
                    #     key, str(postData)))
                    logger.error('error :: listen :: invalid key in POST data - %s' % (
                        key))

                    # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                    unique_value = '%s' % str(time())
                    try:
                        redis_conn.sadd('flux.listen.discarded.invalid_key', str(unique_value))
                        if FLUX_VERBOSE_LOGGING:
                            logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_key' % str(unique_value))
                    except Exception as e:
                        logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_key - %s' % e)

                    # @added 20220118 - Feature #4380: flux - return reason with 400
                    message = 'invalid key'
                    body = {"code": 401, "message": message}
                    resp.text = json.dumps(body)
                    logger.warning('warning :: listen :: %s, returning 401' % message)

                    # @modified 20210909
                    # Return 401 if bad key
                    # resp.status = falcon.HTTP_400
                    resp.status = falcon.HTTP_401
                    return

                if LOCAL_DEBUG:
                    logger.debug('debug :: listen :: valid key, metric_namespace_prefix set to %s' % str(metric_namespace_prefix))
            except Exception as err:
                logger.error(traceback.format_exc())
                # @modified 20220222 - Feature #4284: flux - telegraf
                # Never log POST data as it can be massive
                # logger.error('error :: listen :: could not validate the key from POST data - %s - %s' % (
                #     str(postData), err))
                logger.error('error :: listen :: could not validate the key from POST data - %s' % (
                    err))

                # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                unique_value = '%s' % str(time())
                try:
                    redis_conn.sadd('flux.listen.discarded.invalid_key', str(unique_value))
                    if FLUX_VERBOSE_LOGGING:
                        logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_key' % str(unique_value))
                except Exception as e:
                    logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_key - %s' % e)

                # @added 20220118 - Feature #4380: flux - return reason with 400
                message = 'invalid key'
                body = {"code": 401, "message": message}
                resp.text = json.dumps(body)
                logger.warning('warning :: listen :: %s, returning 401' % message)

                # @modified 20210909
                # Return 401 if bad key
                # resp.status = falcon.HTTP_400
                resp.status = falcon.HTTP_401
                return

            # @added 20211103 - Feature #4316: flux - validate_key
            validated_key = False
            try:
                validated_key = postData['validate_key']
                logger.info('listen :: validate_key: %s' % str(validated_key))
            except KeyError:
                validated_key = False
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: failed to determine validated_key - %s' % str(err))
            if validated_key:
                try:
                    body = {"status": "key valid"}
                    resp.text = json.dumps(body)
                    logger.info('listen :: validate_key returning 200')
                    resp.status = falcon.HTTP_200
                    return
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: failed to response to validate_key request - %s' % str(err))

            # @added 20230223 - Feature #4840: flux - prometheus - x-test-only header
            test_only = False
            test_only_metrics_submitted = []
            try:
                test_only_str = req.get_header('x-test-only')
                if test_only_str:
                    test_only = True
            except Exception as err:
                logger.error('listen :: get_header(\'x-test-only\') error - %s' % str(err))
            if not test_only:
                try:
                    test_only = postData['test']
                    logger.info('listen :: test: %s' % str(test_only))
                except KeyError:
                    test_only = False
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: failed to determine test - %s' % str(err))

            # @added 20200818 - Feature #3694: flux - POST multiple metrics
            # Determine if the POST data is for a single metric or multiple metrics
            metrics = []
            try:
                metrics = postData['metrics']
                if LOCAL_DEBUG:
                    logger.debug('debug :: listen :: metrics is set in the POST data - %s' % str(metrics))
                    for item in metrics:
                        logger.debug('debug :: listen :: metrics item - %s' % str(item))
                        logger.debug('debug :: listen :: metrics item - metric - %s' % str(item['metric']))
            except KeyError:
                metrics = []
            except Exception as err:
                # @modified 20220222 - Feature #4284: flux - telegraf
                # Never log POST data as it can be massive
                # logger.error('error :: listen :: metrics was passed in the request POST data but an error was encountered - returned 400 - %s - %s' % (
                #     str(postData), err))
                logger.error('error :: listen :: metrics was passed in the request POST data but an error was encountered - returned 400 - %s' % (
                    err))

                # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                unique_value = '%s' % str(time())
                try:
                    redis_conn.sadd('flux.listen.discarded.invalid_parameters', str(unique_value))
                    if FLUX_VERBOSE_LOGGING:
                        logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_parameters' % str(unique_value))
                except Exception as err:
                    logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_parameters - %s' % err)

                # @added 20220118 - Feature #4380: flux - return reason with 400
                message = 'invalid metrics element'
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.info('listen :: %s, returning 400' % message)

                resp.status = falcon.HTTP_400
                return

            # @added 20220208 - Feature #4284: flux - telegraf
            # Convert a single metric post to a multiple metric post.  This
            # replaces and deprecates the handling of a single metric POST format in
            # duplicated code.
            if not metrics:
                try:
                    metric_data = {
                        'metric': str(postData['metric']),
                        'timestamp': postData['timestamp'],
                        'value': postData['value']
                    }
                    # @added 20230208 - Feature #3444: Allow flux to backfill
                    # Add the fill key if it is passed on a single metric
                    try:
                        if 'fill' in metric_data:
                            fill = str(metric_data['fill'])
                            if fill in ['true', 'True']:
                                metric_data['fill'] = 'true'
                    except:
                        pass

                    metrics.append(metric_data)
                except Exception as err:
                    logger.error('error :: listen :: failed to get metric data from POST to create metric_data and add to metrics - %s' % err)
                    # @added 20230531 - SAVE_DEBUG_INFO
                    if SAVE_DEBUG_INFO:
                        debug_time_now = time()
                        current_aligned_ts = int(int(debug_time_now) // 60 * 60)
                        hash_key = 'flux.debug.bad.posts.%s' % str(current_aligned_ts)
                        try:
                            redis_conn_decoded.hset(hash_key, str(debug_time_now), str(postData))
                            redis_conn_decoded.expire(hash_key, 900)
                            logger.debug('debug :: listen :: postData from failed POST saved to Redis hash %s, postData: %s' % (
                                str(hash_key), str(postData)))
                        except Exception as err:
                            logger.error('error :: listen :: postData from failed POST saved to Redis hash %s, postData: %s' % (
                                str(hash_key), str(postData), err))

            # @added 20211018 - Feature #4284: flux - telegraf
            # Allow the key to be passed as a HTTP header rather than in the json
            # payload
            telegraf_payload = False
            telegraf_keys_list = ['fields', 'name', 'tags', 'timestamp']
            if metrics and telegraf:
                if isinstance(metrics, list):
                    if len(metrics) > 0:
                        if isinstance(metrics[0], dict):
                            if sorted(list(metrics[0].keys())) == telegraf_keys_list:
                                telegraf_payload = True
            telegraf_metrics = []
            telegraf_metrics_list = []
            if telegraf_payload:

                # @added 20220210 - Feature #4284: flux - telegraf
                # Added TIMINGS
                if TIMINGS:
                    start_telegraf_conversion_timer = timer()

                for metric_dict in metrics:
                    # {'fields': {'accepts': 17819,
                    #    'active': 4,
                    #    'handled': 17819,
                    #    'reading': 0,
                    #    'requests': 218369,
                    #    'waiting': 3,
                    #    'writing': 1},
                    #   'name': 'nginx',
                    #   'tags': {'a_0_tag': 'nginx',
                    #    'host': 'nginx-server-1',
                    #    'port': '81',
                    #    'server': '127.0.0.1'},
                    #   'timestamp': 1644320940}
                    # Using the same as the telegraf Graphite template pattern
                    # template = "host.tags.measurement.field"
                    try:
                        if telegraf_metric_prefix:
                            metric_parent_namespace = '%s.%s' % (
                                str(telegraf_metric_prefix), metric_dict['tags']['host'])
                        else:
                            metric_parent_namespace = '%s' % metric_dict['tags']['host']
                        for key in list(metric_dict['tags'].keys()):
                            if key == 'host':
                                continue
                            value = metric_dict['tags'][key].replace('.', '_')
                            value = value.replace('/', '-')
                            metric_parent_namespace = '%s.%s' % (metric_parent_namespace, str(value))
                        metric_parent_namespace = '%s.%s' % (metric_parent_namespace, str(metric_dict['name']))
                        for key in list(metric_dict['fields'].keys()):
                            key_str = key.replace('.', '_')
                            key_str = key_str.replace('/', '-')
                            metric = '%s.%s' % (metric_parent_namespace, str(key_str))
                            timestamp = metric_dict['timestamp']
                            value = metric_dict['fields'][key]

                            # @added 20220303 - Feature #4284: flux - telegraf
                            # Drop telegraf metrics with a string or boolean value
                            try:
                                add_dropped_metric = False
                                if isinstance(value, str):
                                    add_dropped_metric = True
                                if isinstance(value, bool):
                                    add_dropped_metric = True
                                if add_dropped_metric:
                                    if metric_namespace_prefix:
                                        metric_name = '%s.%s' % (metric_namespace_prefix, metric)
                                    else:
                                        metric_name = str(metric)
                                    dropped_dict = {'timestamp': timestamp, 'value': value}
                                    dropped_telegraf_non_numeric_value_metrics[metric_name] = str(dropped_dict)
                                    dropped_telegraf_non_numeric_value_base_names[metric] = str(dropped_dict)
                                    continue
                            except Exception as err:
                                logger.error('error :: listen :: failed to determine if telegraf non-numeric metric - %s' % err)
                                continue

                            telegraf_metrics.append(metric)
                            telegraf_metrics_list.append({'metric': metric, 'value': value, 'timestamp': timestamp})
                    except Exception as err:
                        logger.error('error :: listen :: failed to determine metric from telegraf data - %s' % err)
                try:
                    redis_conn.set('test.flux.telegraf.metrics_data', str(metrics))
                except Exception as err:
                    logger.error('error :: listen :: set test.flux.telegraf.metrics_data failed - %s' % err)

                if TIMINGS:
                    end_telegraf_conversion_timer = timer()
                    logger.debug('debug :: listen :: telegraf conversion took %.6f seconds' % (end_telegraf_conversion_timer - start_telegraf_conversion_timer))

            if telegraf_metrics_list and telegraf_payload:
                metrics = list(telegraf_metrics_list)

            # @added 20220208 - Feature #4284: flux - telegraf
            # After convert a single metric post to a multiple metric post, if there
            # are no metrics in the POST data return a 400
            if not metrics:
                message = 'no metric data passed'
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.info('listen :: %s, returning 400' % message)
                resp.status = falcon.HTTP_400
                return

            # @added 20220202 - Feature #4412: flux - quota - thunder alert
            test_metric_quota_exceeded_alert = False

            # @added 20220208 - Feature #4432: functions.metrics.skip_metric
            #                   Feature #4400: flux - quota
            not_skipped_metrics_dict = {}
            original_redis_not_skipped_metrics_dict = {}
            skipped_metrics_dict = {}
            original_redis_skipped_metrics_dict = {}
            skipped_metrics_query_redis = False
            original_base_names_in_post = []
            metrics_in_post = []
            skipped_metrics = []
            not_skipped_metrics = []
            received_namespaces = []
            try:
                for metric_data in metrics:
                    try:
                        metric = str(metric_data['metric'])
                        original_base_names_in_post.append(metric)
                        if metric_namespace_prefix:
                            metric_name = '%s.%s' % (metric_namespace_prefix, metric)
                        else:
                            metric_name = str(metric)
                        metrics_in_post.append(metric_name)
                        try:
                            received_namespaces.append(metric_name.split('.')[0])
                        except:
                            continue
                    except:
                        continue
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: could not determine metrics in post - %s' % err)
            received_namespaces = list(set(received_namespaces))

            if ALL_SKIP_LIST:
                namespace_not_skipped_metrics_dict = {}
                if len(metrics) == 1:
                    skipped_metrics_query_redis = True
                if not skipped_metrics_query_redis:
                    # @added 20220210 - Feature #4284: flux - telegraf
                    # Added TIMINGS
                    if TIMINGS:
                        start_not_skipped_metrics_dict_timer = timer()
                    for received_namespace in received_namespaces:
                        flux_not_skipped_metrics_key = 'flux.not_skipped_metrics.%s' % str(received_namespace)
                        try:
                            namespace_not_skipped_metrics_dict = redis_conn_decoded.hgetall(flux_not_skipped_metrics_key)
                        except Exception as err:
                            if not get_memcache_key:
                                # logger.error(traceback.format_exc())
                                logger.error('error :: listen :: could not hgetall %s from Redis - %s' % (
                                    flux_not_skipped_metrics_key, err))
                            # @added 20220426 - Feature #4536: Handle Redis failure
                            # Add flux required data to memcache as well
                            if get_memcache_key:
                                try:
                                    namespace_not_skipped_metrics_dict = get_memcache_key('flux', flux_not_skipped_metrics_key)
                                    if not namespace_not_skipped_metrics_dict:
                                        namespace_not_skipped_metrics_dict = {}
                                except Exception as err:
                                    logger.error('error :: listen :: could get memcache set %s - %s' % (
                                        flux_not_skipped_metrics_key, err))
                                    namespace_not_skipped_metrics_dict = {}
                        if namespace_not_skipped_metrics_dict:
                            for metric in list(namespace_not_skipped_metrics_dict.keys()):
                                not_skipped_metrics_dict[metric] = namespace_not_skipped_metrics_dict[metric]
                                original_redis_not_skipped_metrics_dict[metric] = namespace_not_skipped_metrics_dict[metric]
                                not_skipped_metrics.append(metric)

                    if TIMINGS:
                        end_not_skipped_metrics_dict_timer = timer()
                        logger.debug('debug :: listen :: not_skipped_metrics_dict from Redis took %.6f seconds' % (end_not_skipped_metrics_dict_timer - start_not_skipped_metrics_dict_timer))
                if skipped_metrics_query_redis:
                    flux_not_skipped_metrics_key = 'flux.not_skipped_metrics.%s' % str(received_namespaces[0])
                    metric = metrics_in_post[0]
                    not_skipped_metric = False
                    try:
                        not_skipped_metric = redis_conn_decoded.hget(flux_not_skipped_metrics_key, metric)
                    except Exception as err:
                        if not get_memcache_key:
                            logger.error('error :: listen :: could not check %s in %s Redis hash - %s' % (
                                metric, flux_not_skipped_metrics_key, err))
                        # @added 20220426 - Feature #4536: Handle Redis failure
                        # Add flux required data to memcache as well
                        if get_memcache_key:
                            try:
                                namespace_not_skipped_metrics_dict = get_memcache_key('flux', flux_not_skipped_metrics_key)
                                if not namespace_not_skipped_metrics_dict:
                                    namespace_not_skipped_metrics_dict = {}
                            except Exception as err:
                                logger.error('error :: listen :: could get memcache set %s - %s' % (
                                    flux_not_skipped_metrics_key, err))
                                namespace_not_skipped_metrics_dict = {}
                            if namespace_not_skipped_metrics_dict:
                                try:
                                    not_skipped_metric = namespace_not_skipped_metrics_dict[metric]
                                except KeyError:
                                    not_skipped_metric = False
                    if not_skipped_metric:
                        not_skipped_metrics_dict[metric] = not_skipped_metric
                        original_redis_not_skipped_metrics_dict[metric] = not_skipped_metric
                        not_skipped_metrics.append(metric)

            # @added 20220126 - Feature #4400: flux - quota
            namespace_quota = 0
            current_namespace_metric_count = 0
            namespace_metrics = []
            namespace_metrics_count = 0
            over_quota = 0
            rejected_metrics_over_quota = []
            processed_metrics = []
            newly_added_metrics = []
            check_namespace_quota = None
            # @added 20220426 - Feature #4536: Handle Redis failure
            # Add flux required data to memcache as well
            quota_metrics_from_memcache = False

            try:
                if metric_namespace_prefix:
                    logger.info('listen :: data for metric_namespace_prefix: %s' % str(metric_namespace_prefix))
                    check_namespace_quota = str(metric_namespace_prefix)
                else:
                    if len(received_namespaces) == 1:
                        check_namespace_quota = received_namespaces[0]
                if check_namespace_quota in namespaces_with_quotas:
                    namespace_quota = get_namespace_quota(check_namespace_quota)

                # @added 20230223 - Feature #4840: flux - prometheus - x-test-only header
                # Do not check namespace_quota for test metrics
                if test_only:
                    namespace_quota = 0

                if namespace_quota:
                    namespace_quota_key_reference_timestamp = (int(time()) // 60 * 60)
                    namespace_quota_redis_key = 'flux.namespace_quota.%s.%s' % (
                        check_namespace_quota, str(namespace_quota_key_reference_timestamp))
                    current_namespace_metric_count = 0
                    try:
                        current_namespace_metric_count = redis_conn_decoded.scard(namespace_quota_redis_key)
                    except:
                        current_namespace_metric_count = 0
                    quota_namespace_metrics_redis_key = 'flux.quota.namespace_metrics.%s' % check_namespace_quota
                    try:
                        namespace_metrics = list(redis_conn_decoded.smembers(quota_namespace_metrics_redis_key))
                    except:
                        namespace_metrics = []
                        # @added 20220426 - Feature #4536: Handle Redis failure
                        # Add flux required data to memcache as well
                        if get_memcache_key:
                            try:
                                namespace_metrics = get_memcache_key('flux', quota_namespace_metrics_redis_key)
                                if not namespace_metrics:
                                    namespace_metrics = []
                                quota_metrics_from_memcache = True
                            except Exception as err:
                                logger.error('error :: listen :: could get memcache set %s - %s' % (
                                    quota_namespace_metrics_redis_key, err))
                                namespace_metrics = []

                    namespace_metrics_count = len(namespace_metrics)

                    # @added 20220202 - Feature #4412: flux - quota - thunder alert
                    try:
                        test_metric_quota_exceeded_alert = postData['metric_quota_exceeded_test']
                        if LOCAL_DEBUG:
                            if test_metric_quota_exceeded_alert:
                                logger.debug('debug :: listen :: metric_quota_exceeded_test is set in the POST data')
                    except KeyError:
                        test_metric_quota_exceeded_alert = False
                    except Exception as err:
                        if LOCAL_DEBUG:
                            logger.error('error :: listen :: checking metric_quota_exceeded_test in the POST data - %s' % (
                                err))
                        test_metric_quota_exceeded_alert = False
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: error determining namespace_quota, %s' % err)

            if metrics:
                # added 20201211 - Feature #3694: flux - POST multiple metrics
                # Added metric count
                if FLUX_VERBOSE_LOGGING:
                    try:
                        logger.info('listen :: POST mulitple metric data received with %s entries' % str(len(metrics)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: listen :: could not report number of entries submitted in POST mulitple metric data')

                # @added 20220209 - Feature #4432: functions.metrics.skip_metric
                metrics_found_in_redis = {}
                namespace_skipped_metrics_dict = {}
                if ALL_SKIP_LIST:

                    not_found_in_not_skipped = []
                    # @added 20220210 - Feature #4284: flux - telegraf
                    # Added TIMINGS
                    if TIMINGS:
                        start_skip_metrics_redis_resources_timer = timer()

                    for base_name in list(set(metrics_in_post)):
                        not_skipped_metric = False
                        if not_skipped_metrics_dict:
                            try:
                                not_skipped_metric = not_skipped_metrics_dict[base_name]
                            except KeyError:
                                not_skipped_metric = False
                        if not_skipped_metric:
                            metrics_found_in_redis[base_name] = 'do_not_skip'
                            not_skipped_metrics.append(base_name)
                        else:
                            not_found_in_not_skipped.append(base_name)
                    if TIMINGS:
                        logger.debug('debug :: listen :: metrics_found_in_redis count after not_skipped_metrics: %s' % str(len(metrics_found_in_redis)))
                        logger.debug('debug :: listen :: not_found_in_not_skipped count after not_skipped_metrics: %s' % str(len(not_found_in_not_skipped)))

                    if not_found_in_not_skipped:
                        if not skipped_metrics_query_redis:
                            if TIMINGS:
                                start_skipped_metrics_dict_redis_resources_timer = timer()
                            for received_namespace in received_namespaces:
                                flux_skipped_metrics_key = 'flux.skipped_metrics.%s' % str(received_namespace)
                                try:
                                    namespace_skipped_metrics_dict = redis_conn_decoded.hgetall(flux_skipped_metrics_key)
                                except Exception as err:
                                    if not get_memcache_key:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: listen :: could not hgetall flux.skipped_metrics from Redis - %s' % err)
                                    namespace_skipped_metrics_dict = {}
                                    # @added 20220426 - Feature #4536: Handle Redis failure
                                    # Add flux required data to memcache as well
                                    if get_memcache_key:
                                        try:
                                            namespace_skipped_metrics_dict = get_memcache_key('flux', flux_skipped_metrics_key)
                                            if not namespace_skipped_metrics_dict:
                                                namespace_skipped_metrics_dict = {}
                                        except Exception as err:
                                            logger.error('error :: listen :: could get memcache set %s - %s' % (
                                                flux_skipped_metrics_key, err))
                                            namespace_skipped_metrics_dict = {}
                                if namespace_skipped_metrics_dict:
                                    # This is used to only update the flux.skipped_metrics
                                    # periodically and not every time, otherwise
                                    # updating all the skipped metrics key every
                                    # submission would incur unnecessary Redis
                                    # overhead
                                    # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
                                    # original_redis_skipped_metrics_dict[received_namespace] = namespace_skipped_metrics_dict.copy()
                                    original_redis_skipped_metrics_dict[received_namespace] = copy.deepcopy(namespace_skipped_metrics_dict)
                                    if not skipped_metrics_dict:
                                        skipped_metrics_dict = {}
                                    for metric in list(namespace_skipped_metrics_dict.keys()):
                                        skipped_metrics_dict[metric] = namespace_skipped_metrics_dict[metric]

                            if TIMINGS:
                                end_skipped_metrics_dict_redis_resources_timer = timer()
                                if skipped_metrics_dict:
                                    logger.debug('debug :: listen :: skipped_metrics_dict with %s metrics from Redis took %.6f seconds' % (
                                        str(len(skipped_metrics_dict)), (end_skipped_metrics_dict_redis_resources_timer - start_skipped_metrics_dict_redis_resources_timer)))
                                else:
                                    logger.debug('debug :: listen :: no skipped_metrics_dict from Redis took %.6f seconds' % (
                                        (end_skipped_metrics_dict_redis_resources_timer - start_skipped_metrics_dict_redis_resources_timer)))
                        else:
                            received_namespace = received_namespaces[0]
                            base_name = metrics_in_post[0]
                            flux_skipped_metrics_key = 'flux.not_skipped_metrics.%s' % str(received_namespace)
                            namespace_skipped_metric = None
                            try:
                                namespace_skipped_metric = redis_conn_decoded.hget(flux_skipped_metrics_key, base_name)
                            except Exception as err:
                                if not get_memcache_key:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: listen :: could not hget flux.skipped_metrics from Redis - %s' % err)
                                namespace_skipped_metric = None
                                # @added 20220426 - Feature #4536: Handle Redis failure
                                # Add flux required data to memcache as well
                                if get_memcache_key:
                                    try:
                                        namespace_skipped_metrics_dict = get_memcache_key('flux', flux_skipped_metrics_key)
                                        if not namespace_skipped_metrics_dict:
                                            namespace_skipped_metrics_dict = {}
                                    except Exception as err:
                                        logger.error('error :: listen :: could get memcache set %s - %s' % (
                                            flux_skipped_metrics_key, err))
                                        namespace_skipped_metrics_dict = {}
                                    if namespace_skipped_metrics_dict:
                                        try:
                                            namespace_skipped_metric = namespace_skipped_metrics_dict[base_name]
                                        except KeyError:
                                            namespace_skipped_metric = None
                            if namespace_skipped_metric:
                                if not skipped_metrics_dict:
                                    skipped_metrics_dict = {}
                                skipped_metrics_dict[base_name] = namespace_skipped_metric
                                if skipped_metrics_dict:
                                    # This is used to only update the flux.skipped_metrics
                                    # periodically and not every time, otherwise
                                    # updating all the skipped metrics key every
                                    # submission would incur unnecessary Redis
                                    # overhead
                                    # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
                                    # original_redis_skipped_metrics_dict[received_namespace] = skipped_metrics_dict.copy()
                                    original_redis_skipped_metrics_dict[received_namespace] = copy.deepcopy(skipped_metrics_dict)

                        if skipped_metrics_dict:
                            for base_name in not_found_in_not_skipped:
                                skipped = False
                                try:
                                    skipped = skipped_metrics_dict[base_name]
                                except KeyError:
                                    skipped = False
                                if skipped:
                                    metrics_found_in_redis[base_name] = 'skip'
                                    skipped_metrics.append(base_name)
                    if TIMINGS:
                        end_skip_metrics_redis_resources_timer = timer()
                        logger.debug('debug :: listen :: skip_metrics_redis_resources took %.6f seconds' % (end_skip_metrics_redis_resources_timer - start_skip_metrics_redis_resources_timer))
                        logger.debug('debug :: listen :: metrics_found_in_redis count after skipped_metrics: %s' % str(len(metrics_found_in_redis)))

                # @added 20220208 - Feature #4432: functions.metrics.skip_metric
                # Performance of functions.metrics.skip_metric on a POST with
                # 932 metrics and substantial SKIP_LIST and DO_NOT_SKIP_LIST
                # took 0.322734 seconds
                # metrics:  932 , skipped:  681 , not_skipped:  251 , SKIP_LIST:  83 , DO_NOT_SKIP_LIST: 32
                if not skipped_metrics_dict:
                    skipped_metrics_dict = {}

                if ALL_SKIP_LIST:

                    # @added 20220210 - Feature #4284: flux - telegraf
                    # Added TIMINGS
                    if TIMINGS:
                        start_skip_metrics_lists_timer = timer()

                    # Minimise the SKIP_LIST and DO_NOT_SKIP_LIST lengths to reduce
                    # runtime, only use the appropriate skip lists
                    try:
                        SKIP_LIST = list(settings.SKIP_LIST)
                        DO_NOT_SKIP_LIST = list(settings.DO_NOT_SKIP_LIST)
                        if check_namespace_quota in external_settings_namespaces:
                            for conf_id in list(skyline_external_settings.keys()):
                                try:
                                    if skyline_external_settings[conf_id]['namespace'] == check_namespace_quota:
                                        SKIP_LIST = skyline_external_settings[conf_id]['skip_metrics']
                                except:
                                    continue
                                try:
                                    if skyline_external_settings[conf_id]['namespace'] == check_namespace_quota:
                                        DO_NOT_SKIP_LIST = skyline_external_settings[conf_id]['do_not_skip_metrics']
                                except:
                                    continue
                        skip_exceptions = []
                        if SKIP_LIST:
                            metrics_found_in_redis_count = 0
                            metrics_not_found_in_redis_count = 0
                            if TIMINGS:
                                metrics_in_post_count = len(list(set(metrics_in_post)))
                            for base_name in list(set(metrics_in_post)):
                                found_in_redis = False
                                try:
                                    found_in_redis = metrics_found_in_redis[base_name]
                                except KeyError:
                                    found_in_redis = False
                                if found_in_redis:
                                    metrics_found_in_redis_count += 1
                                    continue
                                # Now do the performance heavy check
                                metrics_not_found_in_redis_count += 1
                                skip = False
                                try:
                                    skip = skip_metric(base_name, SKIP_LIST, DO_NOT_SKIP_LIST)
                                except:
                                    skip = False
                                if skip:
                                    skipped_metrics.append(base_name)
                                    try:
                                        skipped_metrics_dict[base_name] = post_req_start
                                    except Exception as err:
                                        skip_exceptions = ['skipped_metrics_dict', err, traceback.format_exc()]
                                    # If it is Explicitly skipped remove it from the
                                    # flux.not_skipped_metrics if it exists
                                    try:
                                        received_namespace = base_name.split('.')[0]
                                        flux_not_skipped_metrics_key = 'flux.not_skipped_metrics.%s' % str(received_namespace)
                                        redis_conn_decoded.hdel(flux_not_skipped_metrics_key, base_name)
                                    except Exception as err:
                                        skip_exceptions = ['skipped_metrics_dict', err, traceback.format_exc()]
                                else:
                                    not_skipped_metrics_dict[base_name] = post_req_start
                                    not_skipped_metrics.append(base_name)
                            if not_skipped_metrics:
                                try:
                                    metrics_in_post = list(set(not_skipped_metrics))
                                except Exception as err:
                                    skip_exceptions = ['metrics_in_post', err, traceback.format_exc()]
                        if skip_exceptions:
                            logger.error('error :: listen :: if ALL_SKIP_LIST skip_exceptions: %s' % str(skip_exceptions))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: listen :: could not determine metrics to skip - %s' % err)
                    if TIMINGS:
                        end_skip_metrics_lists_timer = timer()
                        logger.debug('debug :: listen :: skip_metrics_lists took %.6f seconds' % (end_skip_metrics_lists_timer - start_skip_metrics_lists_timer))
                        logger.debug('debug :: listen :: skip_metrics_lists total unique metrics in post: %s, found_in_redis: %s, skip_checked: %s, not_skipped_metrics: %s' % (
                            str(metrics_in_post_count),
                            str(metrics_found_in_redis_count),
                            str(metrics_not_found_in_redis_count),
                            str(len(not_skipped_metrics))))

                # @added 20220126 - Feature #4400: flux - quota
                if namespace_quota:

                    # @added 20220210 - Feature #4284: flux - telegraf
                    # Added TIMINGS
                    if TIMINGS:
                        start_namespace_quota_timer = timer()

                    try:
                        known_metrics = []
                        new_metrics = []
                        skipped_metrics_dict_list = []
                        if isinstance(skipped_metrics_dict, dict):
                            skipped_metrics_dict_list = list(skipped_metrics_dict.keys())
                        for base_name in list(set(metrics_in_post)):
                            if metric_name in namespace_metrics:
                                known_metrics.append(metric_name)
                            elif metric_name in skipped_metrics_dict_list:
                                known_metrics.append(metric_name)
                            else:
                                new_metrics.append(metric_name)

                        # @added 20220202 - Feature #4412: flux - quota - thunder alert
                        if test_metric_quota_exceeded_alert:
                            logger.info('listen :: TEST - test_metric_quota_exceeded_alert set, not accepting data testing')
                            namespace_quota = 0
                            known_metrics = []
                            new_metrics = list(metrics_in_post)
                            namespace_metrics = []

                        new_metric_count = namespace_metrics_count + len(new_metrics)
                        if new_metric_count > namespace_quota:
                            over_quota = new_metric_count - namespace_quota
                            logger.info('listen :: will not accept some new metrics, over quota by %s' % str(over_quota))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: listen :: error determining known and new metrics for namespace_quota, %s' % err)

                    if TIMINGS:
                        end_namespace_quota_timer = timer()
                        logger.debug('debug :: listen :: namespace_quota checks per metric took %.6f seconds' % (end_namespace_quota_timer - start_namespace_quota_timer))

                # @added 20220210 - Feature #4284: flux - telegraf
                # Added TIMINGS
                if TIMINGS:
                    start_metrics_processing_timer = timer()

                for metric_data in metrics:

                    # Add metric to add to queue
                    metric = None
                    timestamp = None
                    value = None
                    valid_value = None
                    backfill = False
                    try:
                        metric = str(metric_data['metric'])

                        # @added 20201006 - Feature #3764: flux validate metric name
                        # @modified 20220211 - Feature #4380: flux - return reason with 400
                        # Added reason
                        valid_metric_name, reason = validate_metric_name('listen :: MetricDataPOST POST multiple metrics', str(metric))
                        if not valid_metric_name:

                            # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                            if not metric:
                                metric = str(time())
                            try:
                                redis_conn.sadd('flux.listen.discarded.metric_name', str(metric))
                                if FLUX_VERBOSE_LOGGING:
                                    logger.info('listen :: added %s to Redis set flux.listen.discarded.metric_name' % str(metric))
                            except Exception as err:
                                logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.metric_name - %s' % err)

                            # @added 20220118 - Feature #4380: flux - return reason with 400
                            # @modified 20220211 - Feature #4380: flux - return reason with 400
                            # Added reason
                            # @modified 20220222 - Feature #4380: flux - return reason with 400
                            # Add the metric allowing for more user debug
                            # message = 'invalid metric - %s - %s' % (str(metric_data['metric']), str(reason))
                            message = 'invalid metric - %s - %s' % (str(metric_data), str(reason))
                            body = {"code": 400, "message": message}
                            resp.text = json.dumps(body)
                            logger.info('listen :: %s, returning 400' % message)

                            resp.status = falcon.HTTP_400
                            return

                        if LOCAL_DEBUG:
                            logger.debug('debug :: listen :: metric from postData set to %s' % metric)
                    except Exception as err:
                        # @modified 20220222 - Feature #4284: flux - telegraf
                        # Never log POST data as it can be massive
                        # logger.error('error :: listen :: no valid metric in the request POST metrics data - returned 400 - %s' % (
                        #     str(postData)))
                        logger.error('error :: listen :: no valid metric in the request POST metrics data - returned 400 - %s' % (
                            err))

                        # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                        metric = 'none.%s' % str(time())
                        try:
                            redis_conn.sadd('flux.listen.discarded.metric_name', str(metric))
                            if FLUX_VERBOSE_LOGGING:
                                logger.info('listen :: added %s to Redis set flux.listen.discarded.metric_name' % str(metric))
                        except Exception as e:
                            logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.metric_name - %s' % e)

                        # @added 20220118 - Feature #4380: flux - return reason with 400
                        # @modified 20220222 - Feature #4380: flux - return reason with 400
                        # Add the metric allowing for more user debug
                        # message = 'invalid metric'
                        message = 'invalid metric - %s' % str(metric_data)
                        body = {"code": 400, "message": message}
                        resp.text = json.dumps(body)
                        logger.info('listen :: %s, returning 400' % message)

                        resp.status = falcon.HTTP_400
                        return

                    original_base_name = str(metric)

                    # @added 20210430 - Bug #4046: flux - metric_namespace_prefix on FLUX_SELF_API_KEY conflicting with FLUX_API_KEYS
                    # If metrics are being submitted to flux internally (vista) using
                    # the FLUX_SELF_API_KEY for a namespace if that namespace is added
                    # to FLUX_API_KEYS, flux will begin to also append the namespace
                    # prefix from the FLUX_API_KEYS for metrics submitted with the
                    # FLUX_SELF_API_KEY
                    if metric_namespace_prefix:
                        if metric.startswith(metric_namespace_prefix):
                            metric_namespace_prefix = None

                    # Added metric_namespace_prefix which is declared via the FLUX_API_KEYS
                    if metric_namespace_prefix:
                        metric = '%s.%s' % (str(metric_namespace_prefix), metric)

                    # @added 20220208 - Feature #4432: functions.metrics.skip_metric
                    skip_base_name = False
                    try:
                        skip_base_name = skipped_metrics_dict[metric]
                    except KeyError:
                        skip_base_name = False
                    if skip_base_name:
                        try:
                            skipped_metrics_dict[metric] = post_req_start
                            skipped_metrics.append(metric)
                            continue
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: listen :: error updating metric in skipped_metrics_dict - %s' % err)

                    # @added 20220126 - Feature #4400: flux - quota
                    if over_quota:
                        try:
                            new_namespace_metrics_count = len(namespace_metrics)
                            if new_namespace_metrics_count >= namespace_quota:
                                if metric not in namespace_metrics:
                                    rejected_metrics_over_quota.append(original_base_name)
                                    continue
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: listen :: error adding to rejected_metrics_over_quota, %s' % err)

                    if namespace_quota:
                        if metric not in namespace_metrics:
                            newly_added_metrics.append(metric)

                    try:
                        fill = str(metric_data['fill'])
                        if fill in ['true', 'True']:
                            backfill = True
                    except:
                        backfill = False
                    try:
                        timestamp_present = str(metric_data['timestamp'])
                    except:
                        timestamp_present = False
                    if timestamp_present:
                        try:
                            if not backfill:
                                # @modified 20220211 - Feature #4380: flux - return reason with 400
                                # Added reason
                                valid_timestamp, reason = validate_timestamp('listen :: MetricDataPOST POST multiple metrics', str(timestamp_present))
                            else:
                                valid_timestamp = True
                            if valid_timestamp:
                                timestamp = int(timestamp_present)
                            else:
                                # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                                unique_value = None
                                if not metric:
                                    metric = str(time())
                                try:
                                    unique_value = '%s.%s' % (str(metric), str(timestamp_present))
                                    redis_conn.sadd('flux.listen.discarded.invalid_timestamp', str(unique_value))
                                    if FLUX_VERBOSE_LOGGING:
                                        logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_timestamp' % str(unique_value))
                                except Exception as e:
                                    logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_timestamp - %s' % e)

                                # @added 20220118 - Feature #4380: flux - return reason with 400
                                # @modified 20220222 - Feature #4380: flux - return reason with 400
                                # Add the metric allowing for more user debug
                                # message = 'invalid timestamp - %s - %s' % (str(timestamp_present), str(reason))
                                message = 'invalid timestamp - %s - %s - %s' % (str(timestamp_present), str(reason), str(metric_data))
                                body = {"code": 400, "message": message}
                                resp.text = json.dumps(body)
                                logger.info('listen :: %s, returning 400' % message)

                                resp.status = falcon.HTTP_400
                                return
                        except:
                            logger.warning('warning :: listen :: invalid timestamp value found in POST data - %s' % (
                                str(timestamp_present)))

                            # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                            if not metric:
                                metric = str(time())
                            try:
                                unique_value = '%s.%s' % (str(metric), str(timestamp_present))
                                redis_conn.sadd('flux.listen.discarded.invalid_timestamp', str(unique_value))
                                if FLUX_VERBOSE_LOGGING:
                                    logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_timestamp' % str(unique_value))
                            except Exception as e:
                                logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_timestamp - %s' % e)

                            # @added 20220118 - Feature #4380: flux - return reason with 400
                            # @modified 20220222 - Feature #4380: flux - return reason with 400
                            # Add the metric allowing for more user debug
                            # message = 'invalid timestamp'
                            message = 'invalid timestamp - %s' % str(metric_data)
                            body = {"code": 400, "message": message}
                            resp.text = json.dumps(body)
                            logger.info('listen :: %s, returning 400' % message)

                            resp.status = falcon.HTTP_400
                            return
                    try:
                        value_present = str(metric_data['value'])
                    except:
                        value_present = False
                    if value_present:
                        try:
                            value = float(metric_data['value'])
                            # valid_value is used as in terms of Python if value evalatuion
                            # if value was 0.0 (or 0) they evaluate as False
                            valid_value = True
                        except:
                            # @added 20220222 - Feature #4284: flux - telegraf
                            # Drop telegraf metrics with a string or boolean value
                            if telegraf:
                                try:
                                    add_dropped_metric = False
                                    if isinstance(metric_data['value'], str):
                                        add_dropped_metric = True
                                    if isinstance(metric_data['value'], bool):
                                        add_dropped_metric = True
                                    if add_dropped_metric:
                                        dropped_dict = {'timestamp': metric_data['timestamp'], 'value': metric_data['value']}
                                        dropped_telegraf_non_numeric_value_metrics[metric] = str(dropped_dict)
                                        dropped_telegraf_non_numeric_value_base_names[original_base_name] = str(dropped_dict)
                                        continue
                                except Exception as err:
                                    logger.error('error :: listen :: failed to determine if telegraf non-numeric metric - %s' % err)
                                    continue
                            logger.error('error :: listen :: invalid value from POST data - %s' % (
                                # @modified 20220222 - Feature #4284: flux - telegraf
                                # Never log POST data as it can be massive
                                # str(postData)))
                                str(metric_data)))

                            # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                            if not metric:
                                metric = str(time())
                            unique_value = '%s.%s.%s' % (str(metric), str(timestamp_present), str(value_present))
                            try:
                                redis_conn.sadd('flux.listen.discarded.invalid_value', str(unique_value))
                                if FLUX_VERBOSE_LOGGING:
                                    logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_value' % str(unique_value))
                            except Exception as e:
                                logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_value - %s' % e)

                            # @added 20220118 - Feature #4380: flux - return reason with 400
                            # @modified 20220222 - Feature #4380: flux - return reason with 400
                            # Add the metric allowing for more user debug
                            # message = 'invalid value - %s' % str(metric_data['value'])
                            message = 'invalid value - %s' % str(metric_data)
                            body = {"code": 400, "message": message}
                            resp.text = json.dumps(body)
                            logger.info('listen :: %s, returning 400' % message)

                            resp.status = falcon.HTTP_400
                            return
                    if not metric:
                        logger.error('error :: listen :: no metric in the POST data - %s - returning 400' % (
                            # @modified 20220222 - Feature #4284: flux - telegraf
                            # Never log POST data as it can be massive
                            # str(postData)))
                            str(metric_data)))

                        # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                        metric = 'none.%s' % str(time())
                        try:
                            redis_conn.sadd('flux.listen.discarded.metric_name', str(metric))
                            if FLUX_VERBOSE_LOGGING:
                                logger.info('listen :: added %s to Redis set flux.listen.discarded.metric_name' % str(metric))
                        except Exception as e:
                            logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.metric_name - %s' % e)

                        # @added 20220118 - Feature #4380: flux - return reason with 400
                        # @modified 20220222 - Feature #4380: flux - return reason with 400
                        # Add the metric allowing for more user debug
                        # message = 'invalid metric'
                        message = 'invalid metric - %s' % str(metric_data)
                        body = {"code": 400, "message": message}
                        resp.text = json.dumps(body)
                        logger.info('listen :: %s, returning 400' % message)

                        resp.status = falcon.HTTP_400
                        return
                    if not valid_value:
                        logger.error('error :: listen :: no valid value in the POST data - %s - returning 400' % (
                            # @modified 20220222 - Feature #4284: flux - telegraf
                            # Never log POST data as it can be massive
                            # str(postData)))
                            str(metric_data)))

                        # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                        if not metric:
                            metric = str(time())
                        unique_value = '%s.%s.%s' % (str(metric), str(timestamp_present), str(value_present))
                        try:
                            redis_conn.sadd('flux.listen.discarded.invalid_value', str(unique_value))
                            if FLUX_VERBOSE_LOGGING:
                                logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_value' % str(unique_value))
                        except Exception as e:
                            logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_value - %s' % e)

                        # @added 20220118 - Feature #4380: flux - return reason with 400
                        if not value_present:
                            # @modified 20220222 - Feature #4380: flux - return reason with 400
                            # Add the metric allowing for more user debug
                            # message = 'no value parameter present'
                            message = 'no value parameter present - %s' % str(metric_data)
                        else:
                            # message = 'invalid value'
                            message = 'invalid value - %s' % str(metric_data)
                        body = {"code": 400, "message": message}
                        resp.text = json.dumps(body)
                        logger.info('listen :: %s, returning 400' % message)

                        resp.status = falcon.HTTP_400
                        return
                    if not timestamp:
                        timestamp = int(time())

                    # @added 20230223 - Feature #4840: flux - prometheus - x-test-only header
                    if test_only:
                        test_only_metrics_submitted.append(original_base_name)
                        continue

                    # Queue the metric
                    try:
                        metric_data = [metric, value, timestamp, backfill]

                        # @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
                        # Add to data to the flux.queue Redis set
                        if FLUX_PERSIST_QUEUE and metric_data:
                            try:
                                redis_conn.sadd('flux.queue', str(metric_data))
                            except Exception as e:
                                logger.error('error :: listen :: failed adding data to Redis set flux.queue - %s' % e)

                        # @added 20220126 - Feature #4400: flux - quota
                        if namespace_quota:
                            try:
                                if metric not in namespace_metrics:
                                    namespace_metrics.append(metric)
                                    try:
                                        redis_conn.sadd(quota_namespace_metrics_redis_key, metric)
                                    except Exception as err:
                                        logger.error('error :: listen :: failed to add %s to Redis set %s, %s' % (
                                            metric, quota_namespace_metrics_redis_key, err))
                            except Exception as err:
                                logger.error(traceback.format_exc())
                                logger.error('error :: listen :: error adding metric to quota_namespace_metrics_redis_key, %s' % err)

                            try:
                                redis_conn.sadd(namespace_quota_redis_key, metric)
                                redis_conn.expire(namespace_quota_redis_key, 60)
                            except Exception as err:
                                logger.error('error :: listen :: failed to add %s to Redis set %s, %s' % (
                                    metric, namespace_quota_redis_key, err))

                        # @added 20210406 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                        if aggregate_metrics or aggregate_namespaces_list:
                            try:
                                added_to_aggregation_queue, return_status_code = add_to_aggregate_metric_queue(metric, metric_data, aggregate_metrics, aggregate_namespaces_list)
                                if added_to_aggregation_queue:
                                    if return_status_code == 500:
                                        resp.status = falcon.HTTP_500
                                        return
                                    if return_status_code == 204:
                                        resp.status = falcon.HTTP_204
                                        processed_metrics.append(original_base_name)
                                        metrics_added_to_aggregation.append(original_base_name)
                                    continue
                            except Exception as err:
                                logger.error(traceback.format_exc())
                                logger.error('error :: listen :: error add_to_aggregate_metric_queue failed, returning 500 - %s' % err)
                                resp.status = falcon.HTTP_500
                                return

                        flux.httpMetricDataQueue.put(metric_data, block=False)

                        processed_metrics.append(original_base_name)

                        # modified 20201016 - Feature #3788: snab_flux_load_test
                        if FLUX_VERBOSE_LOGGING:
                            logger.info('listen :: POST mulitple metric data added to flux.httpMetricDataQueue - %s' % str(metric_data))

                        # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                        try:
                            redis_conn.incr('flux.listen.added_to_queue')
                        except Exception as e:
                            # logger.error('error :: listen :: failed to increment to Redis key flux.listen.added_to_queue - %s' % e)
                            # @added 20220428 - Feature #4536: Handle Redis failure
                            if settings.MEMCACHE_ENABLED:
                                try:
                                    incr_memcache_key('flux', 'flux.listen.added_to_queue', 1)
                                except:
                                    pass

                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: listen :: adding POST metric_data to the flux.httpMetricDataQueue queue - %s - %s' % (
                            str(metric_data), err))
                        resp.status = falcon.HTTP_500
                        return

                # @added 20220210 - Feature #4284: flux - telegraf
                # Added TIMINGS
                if TIMINGS:
                    end_metrics_processing_timer = timer()
                    logger.debug('debug :: listen :: metrics_processing took %.6f seconds' % (end_metrics_processing_timer - start_metrics_processing_timer))

            # @added 20230223 - Feature #4840: flux - prometheus - x-test-only header
            if test_only:
                logger.info('listen :: test_only request')
                current_aligned_ts = int(int(time()) // 60 * 60)
                test_only_redis_key = 'flux.prometheus.test_only.%s.%s' % (str(metric_namespace_prefix), str(current_aligned_ts))
                test_only_data = {}
                try:
                    test_only_data = redis_conn_decoded.hgetall(test_only_redis_key)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: failed to hgetall %s Redis hash - %s' % (
                        test_only_redis_key, err))
                if test_only_data:
                    existing_test_metrics = []
                    try:
                        existing_test_metrics = literal_eval(test_only_data['metrics'])
                    except:
                        existing_test_metrics = []
                    all_test_metrics = test_only_metrics_submitted + existing_test_metrics
                    test_metrics_that_would_be_ingested = list(set(all_test_metrics))
                    existing_dropped_metrics = []
                    try:
                        existing_dropped_metrics = literal_eval(test_only_redis_key['dropped'])
                    except:
                        existing_dropped_metrics = []
                    all_dropped_metrics = existing_dropped_metrics
                    test_metrics_that_would_be_dropped = list(set(all_dropped_metrics))
                else:
                    test_metrics_that_would_be_ingested = test_only_metrics_submitted
                    test_metrics_that_would_be_dropped = []
                ingest_count = len(test_metrics_that_would_be_ingested)
                drop_count = len(test_metrics_that_would_be_dropped)
                test_only_data = {
                    'namespace': str(metric_namespace_prefix),
                    'message': 'metric ingestion test, no metrics were ingested only counts are reported',
                    'ingest_count': ingest_count,
                    'drop_count': drop_count,
                    'metrics': str(list(set(test_metrics_that_would_be_ingested))),
                    'dropped': str(test_metrics_that_would_be_dropped),
                }
                logger.info('listen :: test_only creating Redis hash %s with ingest_count: %s, drop_count: %s' % (
                    test_only_redis_key, str(ingest_count),
                    str(drop_count)))
                try:
                    redis_conn_decoded.hset(test_only_redis_key, mapping=test_only_data)
                    redis_conn.expire(test_only_redis_key, 300)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: failed to hset %s Redis hash - %s' % (
                        test_only_redis_key, err))
                logger.info('listen :: test_only request completed')
                resp.text = json.dumps(test_only_data)
                resp.status = falcon.HTTP_200
                return

            if rejected_metrics_over_quota:
                try:
                    message = 'over quota - this account is allowed %s metrics and an additional %s new metrics were posted that have been rejected' % (
                        str(namespace_quota), str(len(rejected_metrics_over_quota)))
                    notice_msg = '%s metrics were processed and %s metrics were rejected' % (
                        str(len(processed_metrics)), str(len(rejected_metrics_over_quota)))

                    # @added 20220202 - Feature #4412: flux - quota - thunder alert
                    if test_metric_quota_exceeded_alert:
                        message = 'TEST - over quota - this account is allowed %s metrics and an additional %s new metrics were posted that have been rejected' % (
                            str(namespace_quota), str(len(rejected_metrics_over_quota)))
                        notice_msg = 'TEST - %s metrics were processed and %s metrics were rejected' % (
                            str(len(processed_metrics)), str(len(rejected_metrics_over_quota)))

                    # @added 20220302 - Feature #4400: flux - quota
                    namespace_over_quota_redis_key = 'flux.quota.namespace_rejected_metrics.%s' % (
                        check_namespace_quota)
                    try:
                        rejected_metrics_dict = {}
                        for rejected_metric in rejected_metrics_over_quota:
                            rejected_metrics_dict[rejected_metric] = namespace_quota_key_reference_timestamp
                        redis_conn.hset(namespace_over_quota_redis_key, mapping=rejected_metrics_dict)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: listen :: failed to add %s rejected metrics to %s Redis hash - %s' % (
                            str(len(rejected_metrics_over_quota)), str(namespace_over_quota_redis_key), err))

                    # @added 20220302 - Feature #4400: flux - quota
                    try:
                        redis_conn.sadd('flux.listen.over_quota.rejected_metrics', *set(rejected_metrics_over_quota))
                    except Exception as err:
                        logger.error('error :: listen :: failed to sadd to Redis set flux.listen.over_quota.rejected_metrics - %s' % err)

                    # @added 20220428 - Feature #4536: Handle Redis failure
                    if settings.MEMCACHE_ENABLED:
                        success = False
                        try:
                            success = set_memcache_key('flux', 'flux.listen.over_quota.rejected_metrics', rejected_metrics_over_quota)
                        except Exception as err:
                            logger.error('error :: worker :: failed to set memcache flux.listen.over_quota.rejected_metrics - %s' % (str(err)))
                        if success:
                            logger.info('worker :: set memcache flux.listen.over_quota.rejected_metrics key - %s')

                    data = []
                    for metric in rejected_metrics_over_quota:
                        data.append({'metric': metric, 'status': 400})
                    for metric in processed_metrics:
                        data.append({'metric': metric, 'status': 204})
                    return_code = 207
                    if len(metrics) == 1:
                        return_code = 400

                    body = {
                        'code': return_code,
                        'data': data,
                        'error': message,
                        'message': notice_msg,
                        'rejected metrics': rejected_metrics_over_quota,
                        'rejected metrics count': len(rejected_metrics_over_quota),
                        'processed metrics': processed_metrics,
                        'processed metrics count': len(processed_metrics),
                        'submitted metrics count': len(metrics),
                        # Do not report skipped_metrics or the skipped_metrics_count
                        # 'skipped_metrics': skipped_metrics,
                        'defined skipped_metrics count': len(skipped_metrics)
                    }
                    # Create a log_body for debugging because logging at
                    # telegraf 5s flush_interval is VERB-OVERISTY and
                    # overservability
                    # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
                    # log_body = body.copy()
                    log_body = copy.deepcopy(body)
                    try:
                        log_body['data'] = 'redacted'
                        log_body['rejected metrics'] = 'redacted'
                        log_body['processed metrics'] = 'redacted'
                        # log_body['skipped_metrics'] = 'redacted'
                    except Exception as err:
                        devnull = str(err)
                        del devnull

                    # @added 20220222 - Feature #4284: flux - telegraf
                    # Drop telegraf metrics with a string or boolean value
                    if telegraf and len(dropped_telegraf_non_numeric_value_base_names) > 0:
                        # @modified 20220303 - Feature #4284: flux - telegraf
                        # Do not add non-numeric dropped metrics to the client
                        # response
                        # body['dropped non-numeric metrics'] = dropped_telegraf_non_numeric_value_base_names
                        # body['dropped non-numeric metrics count'] = len(dropped_telegraf_non_numeric_value_base_names)
                        len_dropped_telegraf_non_numeric_value_base_names = len(dropped_telegraf_non_numeric_value_base_names)
                        log_body['dropped non-numeric metrics count'] = len_dropped_telegraf_non_numeric_value_base_names
                        log_body['dropped non-numeric metrics'] = 'redacted'
                        # @added 20220302 - Feature #4400: flux - quota
                        try:
                            redis_conn.sadd('flux.listen.dropped_non_numeric_metrics', *set(dropped_telegraf_non_numeric_value_metrics))
                        except Exception as err:
                            logger.error('error :: listen :: failed to sadd to Redis set flux.listen.dropped_non_numeric_metrics - %s' % err)

                    # @added 20220202 - Feature #4412: flux - quota - thunder alert
                    if settings.THUNDER_ENABLED:
                        # Note expiry is hardcoded here but thunder will override it
                        # if there is an expiry declared in an external_settings
                        # context
                        expiry = 3600
                        level = 'alert'
                        event_type = 'metric_quota_exceeded'
                        message = '%s - %s - %s metrics rejected as over quota' % (
                            level, check_namespace_quota, str(len(rejected_metrics_over_quota)))
                        status = 'metric quota exceeded'
                        if test_metric_quota_exceeded_alert:
                            message = '%s - %s - rejected %s metrics rejected as over quota - TEST' % (
                                level, check_namespace_quota, str(len(rejected_metrics_over_quota)))
                            status = 'metric quota exceeded - TEST'
                        thunder_event = {
                            'level': level,
                            'event_type': event_type,
                            'message': message,
                            'app': skyline_app,
                            'metric': None,
                            'source': skyline_app,
                            'timestamp': time(),
                            'expiry': expiry,
                            'data': {
                                'namespace': check_namespace_quota,
                                'dropped_non_numeric_metrics': dropped_telegraf_non_numeric_value_base_names,
                                'processed_metrics': processed_metrics,
                                'processed_metrics_count': len(processed_metrics),
                                'rejected_metrics': rejected_metrics_over_quota,
                                'rejected_metrics_count': len(rejected_metrics_over_quota),
                                # 'skipped_metrics': skipped_metrics,
                                'skipped_metrics_count': len(skipped_metrics),
                                'submitted_metrics_count': len(metrics),
                                'status': 'over quota'
                            },
                        }
                        submitted = False
                        try:
                            # Do not log overly verbose
                            # submitted = thunder_send_event(skyline_app, thunder_event, log=False)
                            submitted = thunder_send_event(skyline_app, thunder_event, log=False)
                        except Exception as err:
                            logger.error('error :: listen :: error encounterd with thunder_send_event - %s' % (
                                err))
                        if submitted:
                            logger.info('listen :: send thunder event metrics_qouta_exceeded for %s metrics on namespace %s' % (
                                str(len(rejected_metrics_over_quota)), check_namespace_quota))

                    # @added 20220222 - Feature #4284: flux - telegraf
                    # Drop telegraf metrics with a string or boolean value
                    if telegraf and len(dropped_telegraf_non_numeric_value_metrics) > 0:
                        try:
                            redis_conn.sadd('flux.listen.dropped_non_numeric_metrics', *set(dropped_telegraf_non_numeric_value_metrics))
                        except Exception as err:
                            logger.error('error :: listen :: failed to sadd to Redis set flux.listen.dropped_non_numeric_metrics - %s' % err)
                    # @modified 20220303 - Feature #4284: flux - telegraf
                    # Do not add non-numeric dropped metrics to the client
                    # response
                    #     try:
                    #         redis_conn_decoded.hset('flux.telegraf.dropped_non_numeric_metrics', mapping=dropped_telegraf_non_numeric_value_metrics)
                    #     except Exception as err:
                    #         logger.error(traceback.format_exc())
                    #         logger.error('error :: listen :: failed to update flux.telegraf.dropped_non_numeric_metrics Redis hash - %s' % (
                    #             err))
                    # if telegraf and len(dropped_telegraf_non_numeric_value_base_names) > 0:
                    #     try:
                    #         redis_conn_decoded.hset('flux.telegraf.dropped_non_numeric_base_names', mapping=dropped_telegraf_non_numeric_value_base_names)
                    #     except Exception as err:
                    #         logger.error(traceback.format_exc())
                    #         logger.error('error :: listen :: failed to update flux.telegraf.dropped_non_numeric_base_names Redis hash - %s' % (
                    #             err))
                    #     for dropped_metric in list(dropped_telegraf_non_numeric_value_base_names.keys()):
                    #         data.append({'metric': dropped_metric, 'status': 400, 'reason': 'non-numeric value'})
                    #     body['data'] = data

                    # @added 20220303 - Feature #4400: flux - quota
                    if metrics_added_to_aggregation:
                        try:
                            redis_conn.incrby('flux.listen.added_to_aggregation_queue', len(metrics_added_to_aggregation))
                        except Exception as err:
                            logger.error('error :: listen :: failed to incrby to Redis key flux.listen.added_to_aggregation_queue - %s' % err)

                    resp.text = json.dumps(body)
                    logger.info('listen :: %s, returning %s on metric_namespace_prefix: %s' % (
                        message, str(return_code), str(check_namespace_quota)))
                    # logger.debug('debug :: listen :: %s' % str(body))
                    if return_code == 207:
                        resp.status = falcon.HTTP_207
                        logger.info('listen :: returning %s on metric_namespace_prefix: %s with sample of response: %s' % (
                            str(return_code), str(check_namespace_quota),
                            str(log_body)))
                    else:
                        resp.status = falcon.HTTP_400
                    return
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: error building 207 body, %s' % err)
                    resp.status = falcon.HTTP_500
                    return

            # @modified 20220208 - Feature #4284: flux - telegraf
            # Converted a single metric post to a multiple metric post.  Removed the
            # handling of a single metric POST format.
            # if not metrics:

            # @added 20220209 - Feature #4432: functions.metrics.skip_metric
            if not_skipped_metrics:
                # @added 20220210 - Feature #4284: flux - telegraf
                for received_namespace in received_namespaces:
                    try:
                        if TIMINGS:
                            start_flux_not_skipped_metrics_timer = timer()
                            logger.debug('debug :: listen :: checking %s not_skipped_metrics to update flux.not_skipped_metrics keys for %s received_namespaces, checking %s' % (
                                str(len(not_skipped_metrics)),
                                str(len(received_namespaces)),
                                received_namespace))
                            logger.debug('debug :: listen :: not_skipped_metrics sample: %s' % (
                                str(not_skipped_metrics[0:3])))
                            logger.debug('debug :: listen :: not_skipped_metrics_dict count: %s' % (
                                str(len(not_skipped_metrics_dict))))
                        update_not_skipped_metrics = {}
                        for metric in list(set(not_skipped_metrics)):
                            if metric.startswith(received_namespace):
                                update_entry = True
                                if original_redis_not_skipped_metrics_dict:
                                    try:
                                        # Only update not_skipped metrics every hour to save on
                                        # Redis overhead
                                        last_update_timestamp_str = original_redis_not_skipped_metrics_dict[metric]
                                        if last_update_timestamp_str:
                                            last_update_timestamp = int(float(last_update_timestamp_str))
                                            if (post_req_start - last_update_timestamp) >= 3600:
                                                update_entry = True
                                            else:
                                                update_entry = False
                                    except KeyError:
                                        update_entry = True
                                    except:
                                        update_entry = True
                                else:
                                    update_entry = True
                                if update_entry:
                                    update_not_skipped_metrics[metric] = post_req_start
                        if update_not_skipped_metrics:
                            flux_not_skipped_metrics_key = 'flux.not_skipped_metrics.%s' % received_namespace
                            try:
                                redis_conn_decoded.hset(flux_not_skipped_metrics_key, mapping=update_not_skipped_metrics)
                            except Exception as err:
                                logger.error(traceback.format_exc())
                                logger.error('error :: listen :: failed to update %s Redis hash - %s' % (
                                    flux_not_skipped_metrics_key, err))
                        if TIMINGS:
                            end_flux_not_skipped_metrics_timer = timer()
                            if update_not_skipped_metrics:
                                logger.debug('debug :: listen :: updating %s took %.6f seconds to update with %s metrics' % (
                                    flux_not_skipped_metrics_key,
                                    (end_flux_not_skipped_metrics_timer - start_flux_not_skipped_metrics_timer),
                                    str(len(update_not_skipped_metrics))))
                            else:
                                logger.debug('debug :: listen :: no metrics to update in %s check took %.6f seconds' % (
                                    flux_not_skipped_metrics_key, (end_flux_not_skipped_metrics_timer - start_flux_not_skipped_metrics_timer)))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: listen :: an error occurred check flux.not_skipped_metrics to update - %s' % (
                            err))

            if skipped_metrics:
                # @added 20220210 - Feature #4284: flux - telegraf
                for received_namespace in received_namespaces:
                    try:
                        flux_skipped_metrics_key = 'flux.skipped_metrics.%s' % received_namespace
                        if TIMINGS:
                            start_flux_skipped_metrics_timer = timer()
                            skipped_metrics_count = len(list(set(skipped_metrics)))
                            logger.debug('debug :: listen :: checking which of the %s skipped metrics need updating in %s' % (
                                str(skipped_metrics_count), flux_skipped_metrics_key))
                        update_skipped_metrics = {}
                        for metric in list(set(skipped_metrics)):
                            if metric.startswith(received_namespace):
                                update_entry = False
                                if original_redis_skipped_metrics_dict:
                                    try:
                                        # Only update skipped metrics every hour to save on
                                        # Redis overhead
                                        last_update_timestamp_str = original_redis_skipped_metrics_dict[received_namespace][metric]
                                        if last_update_timestamp_str:
                                            last_update_timestamp = int(float(last_update_timestamp_str))
                                            if (post_req_start - last_update_timestamp) >= 3600:
                                                update_entry = True
                                    except KeyError:
                                        update_entry = True
                                    except:
                                        update_entry = True
                                else:
                                    update_entry = True
                                if update_entry:
                                    update_skipped_metrics[metric] = post_req_start
                        if update_skipped_metrics:
                            if TIMINGS:
                                logger.debug('debug :: listen :: %s skipped metrics need updating in %s' % (
                                    str(len(update_skipped_metrics)), flux_skipped_metrics_key))
                            try:
                                redis_conn_decoded.hset(flux_skipped_metrics_key, mapping=update_skipped_metrics)
                            except Exception as err:
                                logger.error(traceback.format_exc())
                                logger.error('error :: listen :: failed to update %s Redis hash - %s' % (
                                    flux_skipped_metrics_key, err))
                        else:
                            if TIMINGS:
                                logger.debug('debug :: listen :: no skipped metrics need updating in %s' % flux_skipped_metrics_key)
                        if TIMINGS:
                            end_flux_skipped_metrics_timer = timer()
                            logger.debug('debug :: listen :: updating %s took %.6f seconds to update with %s metrics' % (
                                flux_skipped_metrics_key,
                                (end_flux_skipped_metrics_timer - start_flux_skipped_metrics_timer),
                                str(len(update_skipped_metrics))))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: listen :: an error occurred check flux.skipped_metrics to update - %s' % (
                            err))

            # @added 20220210 - Feature #4284: flux - telegraf
            end_request_timer = timer()
            if FLUX_VERBOSE_LOGGING:
                try:
                    logger.info('listen :: POST request took %.6f seconds to process %s metrics' % (
                        (end_request_timer - start_request_timer), str(len(metrics))))
                except Exception as err:
                    logger.error('error :: listen :: failed to calculate request time - %s' % err)

            # @added 20220222 - Feature #4284: flux - telegraf
            # Drop telegraf metrics with a string or boolean value, respond with a
            # 207
            if telegraf and len(dropped_telegraf_non_numeric_value_base_names) > 0:

                # @added 20220302 - Feature #4284: flux - telegraf
                # Drop telegraf metrics with a string or boolean value
                # @modified 20220303 - Feature #4284: flux - telegraf
                # Do not add non-numeric dropped metrics to the client
                # response
                respond_with_dropped_telegraf_non_numeric = False
                # try:
                #     redis_conn_decoded.hset('flux.telegraf.dropped_non_numeric_metrics', mapping=dropped_telegraf_non_numeric_value_metrics)
                # except Exception as err:
                #     logger.error(traceback.format_exc())
                #     logger.error('error :: listen :: failed to update flux.telegraf.dropped_non_numeric_metrics Redis hash - %s' % (
                #         err))
                # try:
                #     redis_conn_decoded.hset('flux.telegraf.dropped_non_numeric_base_names', mapping=dropped_telegraf_non_numeric_value_base_names)
                # except Exception as err:
                #     logger.error(traceback.format_exc())
                #     logger.error('error :: listen :: failed to update flux.telegraf.dropped_non_numeric_base_names Redis hash - %s' % (
                #         err))
                try:
                    redis_conn.sadd('flux.listen.dropped_non_numeric_metrics', *set(dropped_telegraf_non_numeric_value_metrics))
                except Exception as err:
                    logger.error('error :: listen :: failed to sadd to Redis set flux.listen.dropped_non_numeric_metrics - %s' % err)

                # @modified 20220303 - Feature #4284: flux - telegraf
                # Do not add non-numeric dropped metrics to the client
                # response
                respond_with_dropped_telegraf_non_numeric = False
                if respond_with_dropped_telegraf_non_numeric:
                    try:
                        return_code = 207
                        data = []
                        for dropped_metric in list(dropped_telegraf_non_numeric_value_base_names.keys()):
                            data.append({'metric': dropped_metric, 'status': 400, 'reason': 'non-numeric value'})
                        notice_msg = '%s non-numeric value metrics were dropped' % str(len(dropped_telegraf_non_numeric_value_base_names))
                        body = {
                            'code': return_code,
                            'data': data,
                            'message': notice_msg,
                            'dropped non-numeric metrics': list(dropped_telegraf_non_numeric_value_base_names.keys()),
                            'dropped non-numeric metrics count': len(dropped_telegraf_non_numeric_value_base_names),
                            'processed metrics count': len(processed_metrics),
                            'submitted metrics count': len(metrics),
                            # 'skipped_metrics': skipped_metrics,
                            'defined skipped_metrics_count': len(skipped_metrics)
                        }
                        # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
                        # log_body = body.copy()
                        log_body = copy.deepcopy(body)
                        try:
                            log_body['data'] = 'redacted'
                            log_body['dropped non-numeric metrics'] = 'redacted'
                        except Exception as err:
                            devnull = str(err)
                            del devnull

                        try:
                            resp.text = json.dumps(body)
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: listen :: failed to json.dumps body: %s - %s' % (
                                str(body), err))
                            resp.text = {"dropped string value metrics count": len(dropped_telegraf_non_numeric_value_base_names)}
                            return_code = 200
                        if return_code == 200:
                            resp.status = falcon.HTTP_200
                        if return_code == 207:
                            resp.status = falcon.HTTP_207
                        logger.debug('listen :: debug :: returning %s on metric_namespace_prefix: %s with for dropped_telegraf_non_numeric_value_base_names with sample of the response: %s' % (
                            str(return_code), str(check_namespace_quota),
                            str(log_body)))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: listen :: failed to respond with non-numeric value metrics were dropped response: %s - %s' % (
                            str(body), err))

            # @added 20220303 - Feature #4400: flux - quota
            if metrics_added_to_aggregation:
                try:
                    redis_conn.incrby('flux.listen.added_to_aggregation_queue', len(metrics_added_to_aggregation))
                except Exception as err:
                    logger.error('error :: listen :: failed to incrby to Redis key flux.listen.added_to_aggregation_queue - %s' % err)

            if LOCAL_DEBUG:
                try:
                    resp.text = json.dumps(postData)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    # @modified 20220222 - Feature #4284: flux - telegraf
                    # Never log POST data as it can be massive
                    # logger.error('error :: listen :: failed to json.dumps postData - %s' % str(postData))
                    logger.error('error :: listen :: failed to json.dumps postData - %s' % err)
                    resp.text = {"postData": "None"}
                resp.status = falcon.HTTP_200
                logger.info('listen :: returning 200 on metric_data_post request for metric_namespace_prefix: %s' % str(metric_namespace_prefix))
            else:
                resp.status = falcon.HTTP_204
                logger.info('listen :: metric_data_post return 204 - metrics: %s, processed: %s (to aggregate %s), skipped: %s, dropped non-numeric: %s, metric_namespace_prefix: %s' % (
                    str(len(metrics)), str(len(processed_metrics)),
                    str(len(metrics_added_to_aggregation)),
                    str(len(list(set(skipped_metrics)))),
                    str(len(dropped_telegraf_non_numeric_value_metrics)),
                    str(metric_namespace_prefix)))
            return
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: listen :: metric_data_post failed, returning 500 - %s' % err)
            resp.status = falcon.HTTP_500


# @added 20220405 - Feature #4516: flux - opentelemetry traces
class OTLPTracePost(object):

    def on_post(self, req, resp):
        """
        The /flux/otlp/v1/trace endpoint is called via a POST with a content
        type of application/x-protobuf
        """

        if not FLUX_OTEL_ENABLED:
            message = 'otel traces not enabled'
            body = {"code": 400, "message": message}
            resp.text = json.dumps(body)
            logger.info('listen :: OTLPTracePost :: %s, returning 400' % message)
            resp.status = falcon.HTTP_400
            return

        trace_post_start = int(time())
        start_request_timer = timer()
        metrics_added_to_aggregation = []
        total_trace_count = 0
        total_timings_count = 0
        total_metrics = []

        try:
            status = None
            try:
                status = req.get_header('status')
            except Exception as err:
                logger.error('listen :: OTLPTracePost :: get_header(\'status\') error - %s' % str(err))
            if status:
                message = 'OK'
                body = {"code": 200, "message": message}
                resp.text = json.dumps(body)
                logger.info('listen :: OTLPTracePost :: %s, returning 200' % message)
                resp.status = falcon.HTTP_200
                return

            key = None
            try:
                key = req.get_header('key')
            except Exception as err:
                logger.error('listen :: OTLPTracePost :: get_header(\'key\') error - %s' % str(err))
            if not key:
                message = 'no key passed'
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.info('listen :: OTLPTracePost :: %s, returning 400' % message)
                resp.status = falcon.HTTP_400
                return

            keyValid = False
            metric_namespace_prefix = None
            try:
                keyValid, metric_namespace_prefix = validate_key('listen :: OTLPTracePost', key)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: OTLPTracePost :: could not validate the key from header - %s' % (
                    err))
            if not keyValid:
                logger.error('error :: listen :: OTLPTracePost :: invalid key in header - %s' % (
                    key))
                message = 'invalid key'
                body = {"code": 401, "message": message}
                resp.text = json.dumps(body)
                logger.warning('warning :: listen :: OTLPTracePost :: %s, returning 401' % message)
                resp.status = falcon.HTTP_401
                return

            # Preserve the request for debugging
            traceData_obj = None
            try:
                traceData_obj = req.stream.read()
            except Exception as err:
                logger.error('error :: listen :: OTLPTracePost :: req.stream.read() failed - %s' % err)
            content_encoding = None
            try:
                content_encoding = req.get_header('content-encoding')
            except Exception as err:
                logger.error('listen :: OTLPTracePost :: get_header(\'content-encoding\') error - %s' % str(err))
            if content_encoding == 'gzip':
                try:
                    traceData_obj = gzip.decompress(traceData_obj)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: OTLPTracePost :: gzip.decompress(traceData_obj) failed - %s' % err)
            trace = {}
            try:
                traceP = trace_pb2.TracesData()
                traceP.ParseFromString(traceData_obj)
                trace = pb_json_format.MessageToDict(traceP)
            except Exception as err:
                # logger.error(traceback.format_exc())
                logger.error('error :: listen :: OTLPTracePost :: could not parse OLTP trace data - %s' % str(err))
                trace = None
            if not trace:
                message = 'invalid OLTP trace data'
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.warning('warning :: listen :: OTLPTracePost :: %s, returning 400' % message)
                resp.status = falcon.HTTP_400
                return
            resourceSpans = []
            try:
                resourceSpans = trace['resourceSpans']
            except KeyError:
                resourceSpans = []
            if not resourceSpans:
                logger.error('error :: listen :: OTLPTracePost :: could not parse trace[\'resourceSpans\']')
                message = 'invalid OLTP trace data no resourceSpans'
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.warning('warning :: listen :: OTLPTracePost :: %s, returning 400' % message)
                resp.status = falcon.HTTP_400
                return
            timings = {}
            for i in resourceSpans:
                # The service name, name and resulting metric is questionable in
                # terms of achieveing consistency across all types of naming
                # conventions
                try:
                    service_name = i['resource']['attributes'][0]['value']['stringValue']
                    # @modified 20220422 - Bug #4532: opentelemetry - rename InstrumentationLibrary to InstrumentationScope
                    #                      Task #4452: Update dependencies
                    # instrumentationLibrarySpans was deprecated and changed to
                    # scopeSpans between 1.10.0 and 1.11.0 wtf?
                    use_scope = 'instrumentationLibrarySpans'
                    try:
                        trace_count = len(i['instrumentationLibrarySpans'][0]['spans'])
                    except:
                        trace_count = len(i['scopeSpans'][0]['spans'])
                        if trace_count:
                            use_scope = 'scopeSpans'
                    total_trace_count = total_trace_count + trace_count
                    # for span_dict in i['instrumentationLibrarySpans'][0]['spans']:
                    for span_dict in i[use_scope][0]['spans']:
                        db_system = None
                        for attribute_dict in span_dict['attributes']:
                            if attribute_dict['key'] == 'service.name':
                                service_name = attribute_dict['value']['stringValue'].replace('/', '.')
                            if attribute_dict['key'] == 'db.system':
                                db_system = attribute_dict['value']['stringValue']
                        name = span_dict['name'].replace('/', '.')
                        name = name.replace(' ', '.')
                        kind = 'client'
                        if span_dict['kind'] == 'SPAN_KIND_SERVER':
                            kind = 'server'
                        if db_system:
                            kind = '%s.%s' % (kind, db_system)
                        if metric_namespace_prefix:
                            metric = '%s.otel.traces.%s.%s.%s' % (
                                metric_namespace_prefix, service_name,
                                kind, name)
                        else:
                            metric = 'otel.traces.%s.%s.%s' % (
                                service_name, kind, name)
                        metric = metric.replace('..', '.')
                        sanitised_metric = ''
                        for c in metric:
                            if c in ALLOWED_CHARS:
                                sanitised_metric = sanitised_metric + c
                        metric = str(sanitised_metric)
                        metric = metric.replace('..', '.')
                        total_metrics.append(metric)
                        timing_metric = '%s.seconds' % metric
                        timing_metric = timing_metric.replace('..', '.')
                        try:
                            valid_metric_name, reason = validate_metric_name('listen :: OTLPTracePost', metric)
                        except Exception as err:
                            logger.warning('warning :: listen :: OTLPTracePost :: could validate_metric_name - %s - %s - %s' % (
                                reason, metric, str(err)))
                            valid_metric_name = False
                        if not valid_metric_name:
                            continue
                        took = (int(span_dict['endTimeUnixNano']) - int(span_dict['startTimeUnixNano'])) / 1000000000
                        total_timings_count += 1
                        metric_times = []
                        try:
                            metric_times = timings[timing_metric]
                        except KeyError:
                            metric_times = []
                        metric_times.append(took)
                        timings[timing_metric] = metric_times
                        if span_dict['kind'] == 'SPAN_KIND_SERVER':
                            trace_count_metric = '%s.trace_count' % metric
                            trace_count_metric = trace_count_metric.replace('..', '.')
                            trace_counts = []
                            try:
                                trace_counts = timings[trace_count_metric]
                            except KeyError:
                                trace_counts = []
                            trace_counts.append(trace_count)
                            timings[trace_count_metric] = trace_counts
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: OTLPTracePost :: failed to iterate resourceSpans - %s' % err)
                    logger.error('error :: listen :: OTLPTracePost :: resourceSpan: %s' % str(i))
                    resp.status = falcon.HTTP_500
                    return
            agg_timings = {}
            for metric in list(timings.keys()):
                values = timings[metric]
                value = sum(values) / len(values)
                agg_timings[metric] = value
            status_codes = []
            for metric in list(agg_timings.keys()):
                try:
                    value = agg_timings[metric]
                    backfill = False
                    aggregate = True
                    metric_data = [metric, value, trace_post_start, backfill, aggregate]
                    added_to_aggregation_queue, return_status_code = add_to_aggregate_metric_queue(metric, metric_data, aggregate_metrics, aggregate_namespaces_list)
                    status_codes.append(return_status_code)
                    if added_to_aggregation_queue:
                        if return_status_code == 500:
                            resp.status = falcon.HTTP_500
                            return
                        if return_status_code == 204:
                            resp.status = falcon.HTTP_204
                            metrics_added_to_aggregation.append(metric)
                        continue
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: OTLPTracePost :: error add_to_aggregate_metric_queue failed, returning 500 - %s' % err)
                    resp.status = falcon.HTTP_500
                    return
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: listen :: OTLPTracePost :: error add_to_aggregate_metric_queue failed, returning 500 - %s' % err)
            resp.status = falcon.HTTP_500
            return

        if metrics_added_to_aggregation:
            try:
                redis_conn.incrby('flux.listen.added_to_aggregation_queue', len(metrics_added_to_aggregation))
            except Exception as err:
                logger.error('error :: listen :: OTLPTracePost :: failed to incrby to Redis key flux.listen.added_to_aggregation_queue - %s' % err)
        try:
            redis_conn.incrby('flux.listen.otlp_traces.trace_count', total_trace_count)
        except Exception as err:
            logger.error('error :: listen :: failed to incrby to Redis key flux.listen.otlp_traces.trace_count - %s' % err)
        try:
            redis_conn.incrby('flux.listen.otlp_traces.timings_count', total_timings_count)
        except Exception as err:
            logger.error('error :: listen :: failed to incrby to Redis key flux.listen.otlp_traces.timings_count - %s' % err)
        try:
            redis_conn.incrby('flux.listen.otlp_traces.metrics', len(total_metrics))
        except Exception as err:
            logger.error('error :: listen :: failed to incrby to Redis key flux.listen.otlp_traces.metrics - %s' % err)

        resp.status = falcon.HTTP_204
        return


# @added 20221118 - Feature #4732: flux vortex
#                   Feature #4734: mirage_vortex
class VortexDataPost(object):

    def on_post(self, req, resp):
        """
        The /flux/vortex endpoint is called via a POST with a json
        object as defined below, with the optional fill parameter to allow
        flux to backfill airgap data for a metric:

        For example::

            {
                "key": "apikey|str",                        # required
                "metric": "metric|str",                     # required
                "timeout": seconds|int,                     # required
                "timeseries": timeseries|[list,dict],       # required
                "algorithms": {
                    "default": {},
                    "sigma": {"consensus": 6},
                    "matrixprofile": {"window": },
                    "spectral_residual": {},
                    "dbscan": {},
                    "lof": {},
                },                                          # optional
                "consensus": [["sigma", "sr"], ["sigma", "matrixprofile"]],
                "consensus_count": 0,                       # optional
                "check_all_consensuses": false,
                "reference": "a reference id for you|str"   # optional
            }

        For example::

            {
                "algorithms": {"default": {}},
                "key": "1234567890abcdefghijklmnop",
                "metric": "prometheus_http_requests_total{alias=\\"Prometheus\\", code=\\"200\\", handler=\\"/api/v1/query_range\\", instance=\\"localhost:9090\\", job=\\"prometheus\\"}",
                "timeout": 45,
                "timeseries": [[1668689060, 23.3], ..., [1668689120, 20.9]],
            }

        """

        # @modified 20230502 Feature #4732: flux vortex
        #                    Feature #4734: mirage_vortex
        # Handle cluster fqdn being different from the hostname
        # by determining the URL FQDN from REMOTE_SKYLINE_INSTANCES
        # def shard_host(metric, check_horizon_shards):
        def shard_host(metric, check_horizon_shards, return_fqdn=False):
            """
            Return the shard host to which a metric belongs.
            """
            if not check_horizon_shards:
                return this_host
            metric_as_bytes = str(metric).encode()
            value = zlib.adler32(metric_as_bytes)
            modulo_result = value % len(check_horizon_shards)
            for shost in check_horizon_shards:
                if modulo_result == check_horizon_shards[shost]:

                    # @added 20230502 - Feature #4732: flux vortex
                    #                   Feature #4734: mirage_vortex
                    # Handle cluster fqdn being different from the hostname
                    # by determining the URL FQDN from REMOTE_SKYLINE_INSTANCES
                    if return_fqdn:
                        for item in settings.REMOTE_SKYLINE_INSTANCES:
                            if shost in item[3]:
                                return item[0]

                    return shost

        try:
            postData = None

            post_req_start = time()
            request_id = '%s.%s' % (str(post_req_start), str(uuid.uuid4()))

            # @added 20220210 - Feature #4284: flux - telegraf
            start_request_timer = timer()

            # @added 20210512 - Feature #4060: skyline.flux.worker.discarded metrics
            # Preserve the request for debugging
            postData_obj = None
            try:
                postData_obj = req.stream.read()
            except Exception as err:
                logger.error('error :: listen :: vortex :: req.stream.read() failed - %s' % err)

            # Allow for gzip data
            # DEBUG
            # all_headers = None
            # try:
            #     all_headers = str(req.headers)
            # except Exception as err:
            #     logger.error('listen :: req.headers() error - %s' % str(err))
            # logger.info('debug :: listen :: headers - %s' % str(all_headers))
            content_encoding = None
            try:
                content_encoding = req.get_header('content-encoding')
            except Exception as err:
                logger.error('listen :: get_header(\'content-encoding\') error - %s' % str(err))
            # if content_encoding:
            #     logger.info('debug :: listen :: content_encoding: %s' % str(content_encoding))
            # else:
            #     logger.info('debug :: listen :: no content_encoding: %s' % str(content_encoding))
            if content_encoding == 'gzip':
                if TIMINGS:
                    logger.info('debug :: listen :: vortex :: content_encoding: %s, decompressing' % str(content_encoding))
                    start_gzip_timer = timer()
                try:
                    postData_obj = gzip.decompress(postData_obj)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: vortex :: gzip.decompress(postData_obj) failed - %s' % err)
                if TIMINGS:
                    end_gzip_timer = timer()
                    logger.debug('debug :: listen :: vortex :: gzip decompression took %.6f seconds' % (end_gzip_timer - start_gzip_timer))

            try:
                postData = json.loads(postData_obj)
            except json.decoder.JSONDecodeError:
                logger.debug('debug :: listen :: vortex :: postData_obj: %s' % str(postData_obj))
                message = 'invalid json data'
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.info('listen :: vortex :: %s, returning 400' % message)
                resp.status = falcon.HTTP_400
                return
            except Exception as err:
                logger.error('error :: listen :: vortex :: invalid post data - %s' % err)
                # logger.debug('debug :: listen :: vortex :: postData_obj: %s' % str(postData_obj))

            if LOCAL_DEBUG:
                logger.debug('debug :: listen :: vortex :: request arguments and POST data - %s - %s' % (
                    str(req.query_string), str(postData)))

            if not postData:
                if LOCAL_DEBUG:
                    logger.debug('debug :: listen :: vortex :: no POST data recieved')

                # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                try:
                    redis_conn.sadd('flux.listen.discarded.invalid_parameters', request_id)
                    if FLUX_VERBOSE_LOGGING:
                        logger.info('listen :: vortex :: added %s to Redis set flux.listen.discarded.invalid_parameters no POST data' % request_id)
                except Exception as err:
                    logger.error('error :: listen :: vortex :: failed to add entry to Redis set flux.listen.discarded.invalid_parameters no POST data- %s' % err)

                try:
                    redis_conn.set('flux.listen.discarded.invalid_post_data', str(postData_obj))
                except Exception as err:
                    logger.error('error :: listen :: vortex :: failed to data to Redis set flux.listen.discarded.invalid_post_data - %s' % err)

                # @added 20220118 - Feature #4380: flux - return reason with 400
                message = 'no POST data received'
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.info('listen :: vortex :: %s, returning 400' % message)

                resp.status = falcon.HTTP_400
                return

            key = None

            # @added 20211018 - Feature #4284: flux - telegraf
            # Allow the key to be passed as a HTTP header rather than in the json
            # payload
            header_apikey = None
            try:
                header_apikey = req.get_header('key')
            except Exception as err:
                logger.error('listen :: vortex :: get_header(\'key\') error - %s' % str(err))

            status = None
            try:
                status = str(postData['status'])
            except KeyError:
                status = False
            if status:
                try:
                    logger.info('listen :: vortex :: POST status - ok')
                    try:
                        redis_conn.setex('flux.listen.vortex', 120, int(time()))
                    except Exception as err:
                        logger.error('error :: listen :: vortex :: setex on flux.listen failed - %s' % err)
                    body = {"status": "ok"}
                    resp.text = json.dumps(body)
                    resp.status = falcon.HTTP_200
                    return
                except Exception as err:
                    logger.error('error :: listen :: vortex :: could not validate the status key POST request argument - %s - %s' % (
                        str(req.query_string), err))
                    resp.status = falcon.HTTP_500
                    return

            # @added 20200818 - Feature #3694: flux - POST multiple metrics
            metric_namespace_prefix = None

            # Allow the key to be passed as a HTTP header rather than in the json
            # payload
            if header_apikey:
                key = header_apikey
            else:
                key = None
                try:
                    key = str(postData['key'])
                except KeyError:
                    key = None
                    # @added 20220118 - Feature #4380: flux - return reason with 400
                    message = 'no key passed'
                    body = {"code": 400, "message": message}
                    resp.text = json.dumps(body)
                    logger.info('listen :: vortex :: %s, returning 400' % message)
                    resp.status = falcon.HTTP_400
                    return
                except Exception as err:
                    # Never log POST data as it can be massive
                    logger.error('error :: listen :: could not determine the key from POST data - %s' % (
                        err))

            try:
                # Added metric_namespace_prefix
                keyValid, metric_namespace_prefix = validate_key('listen :: MetricDataPOST POST', key)
                if not keyValid:
                    logger.error('error :: listen :: vortex :: invalid key in POST data - %s' % (
                        str(key)))
                    unique_value = '%s' % str(time())
                    try:
                        redis_conn.sadd('flux.listen.discarded.invalid_key', str(unique_value))
                        if FLUX_VERBOSE_LOGGING:
                            logger.info('listen :: vortex :: added %s to Redis set flux.listen.discarded.invalid_key' % request_id)
                    except Exception as e:
                        logger.error('error :: listen :: vortex :: failed to add entry to Redis set flux.listen.discarded.invalid_key - %s' % e)
                    message = 'invalid key'
                    body = {"code": 401, "message": message}
                    resp.text = json.dumps(body)
                    logger.warning('warning :: listen :: vortex :: %s, returning 401' % message)
                    # Return 401 if bad key
                    resp.status = falcon.HTTP_401
                    return

                if LOCAL_DEBUG:
                    logger.debug('debug :: listen :: vortex :: valid key, metric_namespace_prefix set to %s' % str(metric_namespace_prefix))
            except Exception as err:
                logger.error('error :: listen :: could not validate the key from POST data - %s' % (
                    err))
                unique_value = '%s' % str(time())
                try:
                    redis_conn.sadd('flux.listen.discarded.invalid_key', str(unique_value))
                    if FLUX_VERBOSE_LOGGING:
                        logger.info('listen :: vortex :: added %s to Redis set flux.listen.discarded.invalid_key' % request_id)
                except Exception as e:
                    logger.error('error :: listen :: vortex :: failed to add entry to Redis set flux.listen.discarded.invalid_key - %s' % e)
                message = 'invalid key'
                body = {"code": 401, "message": message}
                resp.text = json.dumps(body)
                logger.warning('warning :: listen :: vortex :: %s, returning 401' % message)
                resp.status = falcon.HTTP_401
                return

            # Allow FLUX_SELF_API_KEY requests to set the metric_namespace_prefix
            if key == FLUX_SELF_API_KEY:
                try:
                    metric_namespace_prefix = postData['metric_namespace_prefix']
                except:
                    pass

            request_id = '%s.%s' % (request_id, str(metric_namespace_prefix))

            validated_key = False
            try:
                validated_key = postData['validate_key']
                logger.info('listen :: validate_key: %s' % str(validated_key))
            except KeyError:
                validated_key = False
            except Exception as err:
                logger.error('error :: listen :: vortex :: failed to determine validated_key - %s' % str(err))
            if validated_key:
                try:
                    body = {"status": "key valid"}
                    resp.text = json.dumps(body)
                    logger.info('listen :: validate_key returning 200')
                    resp.status = falcon.HTTP_200
                    return
                except Exception as err:
                    logger.error('error :: listen :: vortex :: failed to response to validate_key request - %s' % str(err))

            valid_post_keys = [
                'algorithms', 'key', 'metric', 'timeout', 'timeseries',
                'reference', 'consensus', 'no_downsample',
                # @added 20230129
                'override_7_day_limit',
                # @added 20230613 - Feature #4948: vortex - adtk algorithms
                # Changed the adtk algorithms to return a results dict
                # like other custom algorithms that vortex can run
                'realtime_analysis', 'return_results', 'check_all_consensuses',
                # @added 20230616 - Feature #4952: vortex - consensus_count
                'consensus_count',
            ]
            # Allow FLUX_SELF_API_KEY requests to pass additional parameters
            if key == FLUX_SELF_API_KEY:
                valid_post_keys.append('save_training_data_on_false')
                valid_post_keys.append('shard_test')
                valid_post_keys.append('metric_namespace_prefix')
                valid_post_keys.append('send_to_ionosphere')
                valid_post_keys.append('return_image_urls')
                valid_post_keys.append('trigger_anomaly')
                valid_post_keys.append('algorithms_test_only')
                valid_post_keys.append('processing_shard_url')

            metric = 'none'
            invalid_post_key = None
            for post_key in list(postData.keys()):
                invalid = False
                if post_key not in valid_post_keys:
                    invalid = True
                    invalid_post_key = post_key
                    continue
                if not invalid:
                    if post_key == 'algorithms':
                        if not isinstance(postData[post_key], dict):
                            invalid = True
                            invalid_post_key = post_key
                            break
                    if post_key in ['key', 'metric', 'reference']:
                        if not isinstance(postData[post_key], str):
                            invalid = True
                            invalid_post_key = post_key
                            break
                        if post_key == 'metric':
                            metric = str(postData[post_key])
                    if post_key == 'timeseries':
                        if not isinstance(postData[post_key], list):
                            if not isinstance(postData[post_key], dict):
                                invalid = True
                                invalid_post_key = post_key
                                break

            if invalid:
                try:
                    redis_conn.sadd('flux.listen.discarded.invalid_parameters', request_id)
                    if FLUX_VERBOSE_LOGGING:
                        logger.info('listen :: vortex :: added %s to Redis set flux.listen.discarded.invalid_parameters' % request_id)
                except Exception as err:
                    logger.error('error :: listen :: vortex :: failed to add entry to Redis set flux.listen.discarded.invalid_parameters - %s' % err)
                message = 'invalid POST element passed - %s' % str(invalid_post_key)
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.info('listen :: vortex :: %s, returning 400' % message)
                resp.status = falcon.HTTP_400
                return

            # Determine if the number of algorithms declared breaches the
            # maximum
            algorithms_err = None
            algorithms_count = 0
            try:
                algorithms_count = len(postData['algorithms'])
            except:
                algorithms_count = 0
            if not algorithms_count:
                algorithms_err = 'no algorithms passed in POST data'
            if algorithms_count > VORTEX_MAX_ALGORITHMS:
                algorithms_err = '%s algorithms passed in POST data, max allowed %s' % (
                    str(algorithms_count), str(VORTEX_MAX_ALGORITHMS))
            # Allow FLUX_SELF_API_KEY requests to breach maximum
            if key == FLUX_SELF_API_KEY:
                algorithms_err = None
            if algorithms_err:
                try:
                    redis_conn.sadd('flux.listen.discarded.invalid_parameters', request_id)
                    if FLUX_VERBOSE_LOGGING:
                        logger.info('listen :: vortex :: added %s to Redis set flux.listen.discarded.invalid_parameters' % request_id)
                except Exception as err:
                    logger.error('error :: listen :: vortex :: failed to add entry to Redis set flux.listen.discarded.invalid_parameters - %s' % err)
                message = algorithms_err
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.info('listen :: vortex :: request_id: %s, %s, returning 400' % (request_id, message))
                resp.status = falcon.HTTP_400
                return

            # Determine if the POST data has a metric and timeseries element
            invalid_timestamp_err_message = None
            for post_key in ['metric', 'timeseries']:
                bad_element = False
                bad_format = False
                invalid_time_format = False
                timeseries_list = False
                timeseries_dict = False
                try:
                    value = postData[post_key]
                    if LOCAL_DEBUG:
                        logger.debug('debug :: listen :: vortex :: %s is set in the POST data' % str(post_key))
                except Exception as err:
                    logger.error('error :: listen :: vortex :: %s was not passed in the request POST data - returning 400 - %s' % (
                        post_key, err))
                    bad_element = True
                if not bad_element and post_key == 'timeseries':
                    if not isinstance(value, list):
                        if not isinstance(value, dict):
                            bad_element = True
                            logger.error('error :: listen :: vortex :: %s was not passed as a list or dict - returning 400' % (
                                post_key))
                    if isinstance(value, list):
                        timeseries_list = True
                    if isinstance(value, dict):
                        timeseries_dict = True
                    if not bad_element:
                        try:
                            if timeseries_list:
                                first_timestamp = int(value[0][0])
                            if timeseries_dict:
                                first_timestamp = int(list(value.keys())[0])
                        except Exception as err:
                            invalid_time_format = True
                            bad_element = True
                            invalid_timestamp_err_message = 'invalid timestamps in %s, expected unix timestamps' % (
                                str(post_key))
                            logger.error('error :: listen :: vortex :: %s does not contain unix timestamps - returning 400 - %s' % (
                                post_key, err))
                if bad_element:
                    unique_value = '%s' % str(time())
                    try:
                        redis_conn.sadd('flux.listen.discarded.invalid_parameters', request_id)
                        if FLUX_VERBOSE_LOGGING:
                            logger.info('listen :: vortex :: added %s to Redis set flux.listen.discarded.invalid_parameters' % str(unique_value))
                    except Exception as err:
                        logger.error('error :: listen :: vortex :: failed to add entry to Redis set flux.listen.discarded.invalid_parameters - %s' % err)
                    message = 'invalid %s element' % str(post_key)
                    if invalid_time_format:
                        message = str(invalid_timestamp_err_message)
                    body = {"code": 400, "message": message}
                    resp.text = json.dumps(body)
                    logger.info('listen :: vortex :: %s, returning 400' % message)
                    resp.status = falcon.HTTP_400
                    return

            logger.info('listen :: vortex :: handling request_id %s' % request_id)
            original_request_id = None
            try:
                original_request_id = postData['request_id']
            except:
                pass
            postData['request_id'] = request_id
            postData['metric_namespace_prefix'] = metric_namespace_prefix
            added_at = time()

            shard_test = False
            if key in VORTEX_ALLOWED_SHARD_TEST_KEYS:
                try:
                    shard_test = postData['shard_test']
                    if shard_test:
                        logger.info('listen :: vortex :: shard_test: %s' % str(shard_test))
                        postData['shard_test'] = shard_test
                except KeyError:
                    shard_test = False
                except:
                    shard_test = False

            postData['added'] = added_at
            postData['source_app'] = 'flux'

            wait_for = 60
            try:
                wait_for = postData['timeout']
            except KeyError:
                wait_for = 60
            except:
                wait_for = 60

            local_metric = True
            shost = this_host

            authorized_shard_request = False
            shard_request = False
            try:
                shard_request = postData['shard_request']
            except:
                shard_request = False
            if shard_request:
                if shard_request == FLUX_SELF_API_KEY:
                    authorized_shard_request = True
                else:
                    try:
                        del postData['timeseries']
                    except:
                        pass
                    logger.warning('warning :: listen :: vortex :: unauthorized shard request made - postData (excl. timeseries):  %s' % str(postData))
                    resp.status = falcon.HTTP_403
                    return
                try:
                    del postData['shard_request']
                except Exception as err:
                    resp.status = falcon.HTTP_500
                    return
            if authorized_shard_request and original_request_id:
                logger.info('listen :: vortex :: authorized_shard_request so switching back to the original_request_id: %s' % str(original_request_id))
                request_id = str(original_request_id)
                postData['request_id'] = request_id

            processing_shard = 0
            shost = this_host
            if not authorized_shard_request:
                if HORIZON_SHARDS or HORIZON_TEST_SHARDS or shard_test:
                    if HORIZON_SHARDS:
                        postData['originating_shard'] = HORIZON_SHARD
                        shost = shard_host(metric, HORIZON_SHARDS)
                        processing_shard = HORIZON_SHARDS[shost]
                    if HORIZON_TEST_SHARDS and shard_test:
                        postData['horizon_test_shard'] = True
                        postData['originating_shard'] = HORIZON_TEST_SHARD
                        shost = shard_host(metric, HORIZON_TEST_SHARDS)
                        processing_shard = HORIZON_TEST_SHARDS[shost]
                    if shost != this_host:
                        local_metric = False
                    postData['processing_shard'] = processing_shard
                    postData['shard_host'] = shost
                    postData['origin_host'] = req.host
                    postData['origin_scheme'] = req.scheme
                    postData['shard_request'] = FLUX_SELF_API_KEY

#            postData['local_metric'] = local_metric
#            postData['origin_host'] = req.host
#            postData['origin_scheme'] = req.scheme
#            postData['origin_uri'] = req.uri
#            postData['origin_url'] = req.url

            if VORTEX_TIMESERIES_JSON_TO_DISK and local_metric:
                jsonfile = '%s/flux/vortex/data/%s.json' % (settings.SKYLINE_DIR, request_id)
                try:
                    # Using a JSON string
                    with open(jsonfile, 'w') as outfile:
                        json.dump(postData, outfile)
                    try:
                        postData['timeseries'] = jsonfile
                    except:
                        pass
                except Exception as err:
                    logger.error('error :: listen :: vortex :: failed to save request json to %s - %s' % (
                        jsonfile, err))

            shard_results_url = None
            added = False
            if local_metric:
                try:
                    added = redis_conn_decoded.hset('flux.vortex', request_id, str(postData))
                    if added:
                        logger.info('listen :: vortex :: added %s to flux.vortex' % request_id)
                except Exception as err:
                    try:
                        del postData['timeseries']
                    except:
                        pass
                    logger.error('error :: listen :: vortex :: failed to add entry to Redis hash flux.vortex - %s, postData (excl timeseries): %s' % (
                        err, str(postData)))
                    logger.info('listen :: vortex :: returning 500')
                    resp.status = falcon.HTTP_500
                    return
            else:
                # Send request to shardOffload to shard
                origin_host = str(req.host)
                origin_hostname = origin_host.split('.', maxsplit=1)[0]
                origin_fqdn = origin_host.split('.', maxsplit=1)[1]
                original_url = str(req.url)
                processing_shard_url = original_url.replace(origin_hostname, shost)

                # @added 20230502 - Feature #4732: flux vortex
                #                   Feature #4734: mirage_vortex
                # Handle cluster fqdn being different from the hostname
                # by determining the URL FQDN from REMOTE_SKYLINE_INSTANCES
                if HORIZON_SHARDS:
                    processing_shard_fqdn_url = shard_host(metric, HORIZON_SHARDS, return_fqdn=True)
                    processing_shard_url = '%s/flux/vortex' % processing_shard_fqdn_url
                if HORIZON_TEST_SHARDS and shard_test:
                    processing_shard_fqdn_url = shard_host(metric, HORIZON_TEST_SHARDS, return_fqdn=True)
                    processing_shard_url = '%s/flux/vortex' % processing_shard_fqdn_url

                # Handle reverse proxy stripping flux
                if 'flux/vortex' not in processing_shard_url:
                    processing_shard_url = processing_shard_url.replace('vortex', 'flux/vortex')
                postData['shard_host'] = shost
                postData['processing_shard_url'] = processing_shard_url
                r = None
                logger.info('listen :: vortex :: making shard processing request to %s' % str(processing_shard_url))
                try:
                    headers = {'content-encoding': 'gzip', "content-type": "application/json"}
                    request_body = gzip.compress(json.dumps(postData).encode('utf-8'))
                    r_timeout = wait_for - 5
                    r = requests.post(processing_shard_url, data=request_body, headers=headers, timeout=r_timeout)
                    logger.info('listen :: vortex :: shard processing request got response with code %s' % str(r.status_code))
                except Exception as err:
                    logger.error('error :: listen :: vortex :: failed to make shard request to %s - %s' % (
                        str(processing_shard_url), err))
                    logger.info('listen :: vortex :: returning 500')
                    resp.status = falcon.HTTP_500
                    return
                if r:
                    try:
                        response_json = r.json()
                        shard_results_url = response_json['results_url']
                        retries = -1
                        logger.info('listen :: vortex :: got shard_results_url: %s' % str(shard_results_url))
                        sleep(10)
                    except Exception as err:
                        logger.error('error :: listen :: vortex :: failed to get results_url from shard response - %s' % (
                            err))
                        logger.info('listen :: vortex :: returning 500')
                        resp.status = falcon.HTTP_500
                        return
                    if shard_results_url:
                        added = True

            if not added:
                try:
                    del postData['timeseries']
                except:
                    pass
                if local_metric:
                    logger.error('error :: listen :: vortex :: failed to add entry to Redis hash flux.vortex, postData (excl timeseries): %s' % (
                        str(postData)))
                else:
                    logger.error('error :: listen :: vortex :: failed to add shard processing request, postData (excl timeseries): %s' % (
                        str(postData)))
                logger.info('listen :: vortex :: returning 500')
                resp.status = falcon.HTTP_500
                return

            time_now = int(time())
            results = {}
            errors = []

            if local_metric and authorized_shard_request:
                end_request_timer = timer()
                try:
                    logger.info('listen :: vortex :: accepted shard request took %.6f seconds' % (
                        (end_request_timer - start_request_timer)))
                except Exception as err:
                    logger.error('error :: listen :: vortex :: failed to calculate request time - %s' % err)
                try:
                    results_url = '%s://%s/flux/vortex_results?request_id=%s&retries=-1&timeout=%s&shard=%s' % (
                        req.scheme, req.host, request_id, str(wait_for - 5),
                        str(postData['processing_shard']))
                    results = {'results_url': results_url}
                    resp.text = json.dumps(results)
                    resp.status = falcon.HTTP_200
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: vortex :: failed to return results_url - %s' % err)
                    resp.status = falcon.HTTP_500
                    return
                return

            # Return immediately, do not hold the request open waiting for the
            # result as this causes all the workers to be unavailable.  Return
            # the 303 redirect to /flux/vortex_results
            end_request_timer = timer()
            try:
                logger.info('listen :: vortex :: request took %.6f seconds to process' % (
                    (end_request_timer - start_request_timer)))
            except Exception as err:
                logger.error('error :: listen :: vortex :: failed to calculate request time - %s' % err)
            uri = '/flux/vortex_results?request_id=%s&retries=1&timeout=%s' % (request_id, str(wait_for))
            if authorized_shard_request:
                uri = '%s&shard=%s' % (uri, str(postData['processing_shard']))
            if shard_results_url:
                if 'shard=' not in uri:
                    uri = '%s&shard=%s' % (uri, str(postData['processing_shard']))

            logger.info('listen :: vortex :: returning request_id: %s, redirecting to %s' % (
                request_id, uri))
            try:
                resp.request_id = request_id
                resp.retries = 1
                resp.timeout = wait_for
                resp.location = uri
                resp.status = falcon.HTTP_303
                # raise falcon.HTTPSeeOther('/flux/vortex_results', headers={'request_id': request_id, 'retries': 1, 'timeout': wait_for})
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: vortex :: raise falcon.HTTPSeeOther - %s' % err)
                resp.status = falcon.HTTP_500
                return
            return

            while time_now < (post_req_start + wait_for - 2):
                result_str = None
                if local_metric:
                    try:
                        result_str = redis_conn_decoded.hget('mirage.vortex', request_id)
                    except Exception as err:
                        errors.append(err)
                    if result_str:
                        try:
                            results = literal_eval(result_str)
                        except Exception as err:
                            logger.error('error :: listen :: vortex :: failed to literal_eval %s data from mirage.vortex - %s' % (
                                request_id, err))
                            try:
                                del postData['timeseries']
                            except:
                                pass
                            logger.error('error :: listen :: vortex :: failed to get results for %s, postData (excl timeseries): %s' % (
                                request_id, str(postData)))
                            logger.info('listen :: vortex :: returning 500')
                            resp.status = falcon.HTTP_500
                            return
                    if results:
                        break
                    sleep(1)
                    time_now = int(time())
                else:
                    r = None
                    try:
                        retries += 1
                        url = '%s://%s%s/flux/vortex_results?request_id=%s&retries=-1&timeout=%s&shard=%s' % (
                            req.scheme, shost, origin_fqdn, request_id, str(wait_for - 5),
                            str(processing_shard))
                        new_shard_results_url = '%s&retries=%s' % (shard_results_url, str(retries))
                        r = requests.get(new_shard_results_url, timeout=r_timeout)
                    except Exception as err:
                        logger.error('error :: listen :: vortex :: failed to request shard_results_url: %s - %s' % (
                            str(shard_results_url), err))
                        try:
                            del postData['timeseries']
                        except:
                            pass
                        logger.error('error :: listen :: vortex :: failed to get results for %s, postData (excl timeseries): %s' % (
                            request_id, str(postData)))
                        logger.info('listen :: vortex :: returning 500')
                        resp.status = falcon.HTTP_500
                        return
                    if r:
                        try:
                            results = r.json()
                            if 'results' in results:
                                break
                        except Exception as err:
                            logger.error('error :: listen :: vortex :: failed to get results from response from shard_results_url: %s - %s' % (
                                str(shard_results_url), err))
                    sleep(1)
                    time_now = int(time())

            if not results:
                end_request_timer = timer()
                try:
                    logger.info('listen :: vortex :: request took %.6f seconds to process' % (
                        (end_request_timer - start_request_timer)))
                except Exception as err:
                    logger.error('error :: listen :: vortex :: failed to calculate request time - %s' % err)
                uri = '/flux/vortex_results?request_id=%s&retries=1&timeout=%s' % (request_id, str(wait_for))
                if authorized_shard_request:
                    uri = '%s&shard=%s' % (uri, str(postData['processing_shard']))
                logger.info('listen :: vortex :: no results for request_id: %s, redirecting to %s' % (
                    request_id, uri))
                try:
                    resp.request_id = request_id
                    resp.retries = 1
                    resp.timeout = wait_for
                    resp.location = uri
                    resp.status = falcon.HTTP_303
                    # raise falcon.HTTPSeeOther('/flux/vortex_results', headers={'request_id': request_id, 'retries': 1, 'timeout': wait_for})
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: vortex :: raise falcon.HTTPSeeOther - %s' % err)
                    resp.status = falcon.HTTP_500
                    return
                return

            try:
                del postData['timeseries']
            except:
                pass
            postData['results'] = results['results']

            if results:
                # Remove the key
                try:
                    redis_conn_decoded.hdel('mirage.vortex', request_id)
                except Exception as err:
                    logger.error('error :: vortex :: failed to hdel %s from mirage.vortex - %s' % (request_id, err))

                postData_keys = list(postData.keys())
                for key in list(results.keys()):
                    if key not in postData_keys:
                        postData[key] = copy.deepcopy(results[key])

                remove_keys = [
                    'shard_request', 'processing_shard_url', 'shard_host',
                    'origin_host', 'origin_scheme', 'local_metric', 'origin_uri',
                    'origin_url', 'shard_test', 'key',
                ]
                for remove_key in remove_keys:
                    try:
                        del postData[remove_key]
                    except:
                        pass
                    try:
                        del postData['results'][remove_key]
                    except:
                        pass

            resp.text = json.dumps(postData)

            end_request_timer = timer()
            try:
                logger.info('listen :: vortex :: request_id: %s, took %.6f seconds to process' % (
                    request_id, (end_request_timer - start_request_timer)))
            except Exception as err:
                logger.error('error :: vortex :: failed to calculate request time - %s' % err)

            resp.status = falcon.HTTP_200
            return
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: listen :: vortex :: request failed - %s' % (
                err))
            logger.info('listen :: vortex :: returning 500')
            resp.status = falcon.HTTP_500
            return

        resp.status = falcon.HTTP_204
        return


# @added 20221118 - Feature #4732: flux vortex
#                   Feature #4734: mirage_vortex
class VortexResults(object):

    def on_get(self, req, resp):
        """
        The /flux/vortex_results endpoint is called via a GET with headers and a
        URI parameter of retries.
        """

        # @modified 20230510 Feature #4732: flux vortex
        #                    Feature #4734: mirage_vortex
        # Handle cluster fqdn being different from the hostname
        # by determining the URL FQDN from REMOTE_SKYLINE_INSTANCES
        # def shard_host(metric, check_horizon_shards):
        def shard_fqdn(shard):
            """
            Return the fqdn of the shard
            """
            for shost in list(HORIZON_SHARDS.keys()):
                if shard == HORIZON_SHARDS[shost]:
                    break
            
            for item in settings.REMOTE_SKYLINE_INSTANCES:
                if shost in item[3]:
                    return item[0]

        start_request_timer = timer()
        start_request = int(time())

        validGetArguments = ['request_id', 'retries', 'timeout', 'shard']
        request_id = None
        retries = 0
        timeout = 29
        shard = None

        try:
            payload = {}
            payload['query_string'] = str(req.query_string)

            for request_param_key, request_param_value in req.params.items():
                try:
                    if str(request_param_key) not in validGetArguments:
                        logger.error('error :: listen :: vortex_results :: invalid key in request arguments - %s - %s' % (
                            str(request_param_key), str(req.query_string)))
                        unique_value = '%s' % str(time())
                        try:
                            redis_conn.sadd('flux.listen.discarded.invalid_parameters', str(unique_value))
                            if FLUX_VERBOSE_LOGGING:
                                logger.info('listen :: vortex_results :: added %s to Redis set flux.listen.discarded.invalid_parameters' % str(unique_value))
                        except Exception as err:
                            logger.error('error :: listen :: vortex_results :: failed to add entry to Redis set flux.listen.discarded.invalid_parameters - %s' % err)

                        # @added 20220118 - Feature #4380: flux - return reason with 400
                        message = 'Invalid parameter received - %s' % str(request_param_key)
                        body = {"code": 400, "message": message}
                        resp.text = json.dumps(body)
                        logger.info('listen :: vortex_results :: %s, returning 400' % message)

                        resp.status = falcon.HTTP_400
                        return
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: vortex_results :: validating request arguments - %s' % (
                        str(req.query_string)))

                    # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                    unique_value = '%s' % str(time())
                    try:
                        redis_conn.sadd('flux.listen.discarded.invalid_parameters', str(unique_value))
                        if FLUX_VERBOSE_LOGGING:
                            logger.info('listen :: vortex_results :: added %s to Redis set flux.listen.discarded.invalid_parameters' % str(unique_value))
                    except Exception as e:
                        logger.error('error :: listen :: vortex_results :: failed to add entry to Redis set flux.listen.discarded.invalid_parameters - %s' % e)

                    # @added 20220118 - Feature #4380: flux - return reason with 400
                    message = 'Invalid parameters'
                    body = {"code": 400, "message": message}
                    resp.text = json.dumps(body)
                    logger.info('listen :: vortex_results :: %s, returning 400' % message)

                    resp.status = falcon.HTTP_400
                    return

                try:
                    if str(request_param_key) == 'request_id':
                        request_id = str(request_param_value)
                    if str(request_param_key) == 'retries':
                        retries = int(str(request_param_value[-1]))
                    if str(request_param_key) == 'timeout':
                        wait_for = int(str(request_param_value))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: vortex_results :: could not validate the %s key in the GET request argument - %s' % (
                        str(request_param_key), str(req.query_string)))
                    resp.status = falcon.HTTP_400
                    return
                try:
                    if str(request_param_key) == 'shard':
                        shard = int(str(request_param_value))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: vortex_results :: could not validate the %s key in the GET request argument - %s' % (
                        str(request_param_key), str(req.query_string)))
                    resp.status = falcon.HTTP_400
                    return

            if not request_id:
                logger.info('listen :: vortex_results :: request - %s' % str(req.query_string))
                message = 'no request_id'
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.info('listen :: vortex_results :: %s, returning 400' % message)
                resp.status = falcon.HTTP_400
                return

            retries += 1

            shard_request = False
            local_shard_number = None
            shost = this_host
            if isinstance(shard, int):
                shard_request = True
            if shard_request:
                if HORIZON_SHARDS or HORIZON_TEST_SHARDS:
                    if HORIZON_SHARDS:
                        local_shard_number = HORIZON_SHARDS[this_host]
                        if local_shard_number != shard:
                            for hshost in HORIZON_SHARDS:
                                if HORIZON_SHARDS[hshost] == shard:
                                    shost = hshost
                                    break
                    if HORIZON_TEST_SHARDS:
                        local_shard_number = HORIZON_TEST_SHARDS[this_host]
                        if local_shard_number != shard:
                            for hshost in HORIZON_TEST_SHARDS:
                                if HORIZON_TEST_SHARDS[hshost] == shard:
                                    shost = hshost
                                    break
                        if shost != this_host:
                            shost = this_host

            if not shard_request:
                logger.info('listen :: vortex_results :: request_id: %s, retries: %s, timeout: %s' % (
                    str(request_id), str(retries), str(wait_for)))
            else:
                logger.info('listen :: vortex_results :: request_id: %s, retries: %s, timeout: %s, shard: %s' % (
                    str(request_id), str(retries), str(wait_for), str(shard)))

            time_now = int(time())
            results = {}
            errors = []
            while time_now < (start_request + wait_for - 2):
                result_str = None
                if shost == this_host:
                    try:
                        result_str = redis_conn_decoded.hget('mirage.vortex', request_id)
                    except Exception as err:
                        errors.append(err)
                    if result_str:
                        try:
                            results = literal_eval(result_str)
                        except Exception as err:
                            logger.error('error :: listen :: vortex_results :: failed to literal_eval %s data from mirage.vortex - %s' % (
                                request_id, err))
                            logger.info('listen :: vortex_results :: returning 500')
                            resp.status = falcon.HTTP_500
                            return
                else:
                    origin_host = str(req.host)
                    origin_hostname = origin_host.split('.', maxsplit=1)[0]
                    # origin_fqdn = origin_host.split('.', maxsplit=1)[1]
                    original_url = str(req.url)
                    processing_shard_url = original_url.replace(origin_hostname, shost)

                    # @added 20230502 - Feature #4732: flux vortex
                    #                   Feature #4734: mirage_vortex
                    # Handle cluster fqdn being different from the hostname
                    # by determining the URL FQDN from REMOTE_SKYLINE_INSTANCES
                    origin_host_url = '%s://%s' % (req.scheme, req.host)
                    if shard_request:
                        processing_shard_fqdn_url = shard_fqdn(shard)
                        processing_shard_url = original_url.replace(origin_host_url, processing_shard_fqdn_url)

                    if 'flux/vortex' not in processing_shard_url:
                        processing_shard_url = processing_shard_url.replace('vortex', 'flux/vortex')
                    if retries == 1 and HORIZON_TEST_SHARDS:
                        r_timeout = 3
                    else:
                        r_timeout = wait_for - 5
                    r = None
                    try:
                        r = requests.get(processing_shard_url, timeout=r_timeout)
                    except Exception as err:
                        logger.error('error :: listen :: vortex_results :: failed to request processing_shard_url: %s - %s' % (
                            str(processing_shard_url), err))
                        logger.info('listen :: vortex_results :: returning 500')
                        resp.status = falcon.HTTP_500
                        return
                    if r:
                        try:
                            results = r.json()
                            if 'results' in results:
                                break
                        except Exception as err:
                            logger.error('error :: listen :: vortex :: failed to get results from response from processing_shard_url: %s - %s' % (
                                str(processing_shard_url), err))

                if results:
                    # Remove the key
                    try:
                        redis_conn_decoded.hdel('mirage.vortex', request_id)
                    except Exception as err:
                        errors.append(err)

                    remove_keys = [
                        'shard_request', 'processing_shard_url', 'shard_host',
                        'origin_host', 'origin_scheme', 'local_metric', 'origin_uri',
                        'origin_url', 'shard_test', 'key',
                    ]
                    for remove_key in remove_keys:
                        try:
                            del results[remove_key]
                        except:
                            pass
                        try:
                            del results['results'][remove_key]
                        except:
                            pass

                    break
                sleep(1)
                time_now = int(time())

            if not results:
                end_request_timer = timer()
                try:
                    logger.info('listen :: vortex_results :: request took %.6f seconds to process before redirection' % (
                        (end_request_timer - start_request_timer)))
                except Exception as err:
                    logger.error('error :: listen :: vortex_results :: failed to calculate request time - %s' % err)
                if retries >= 3:
                    retry_url = '%s://%s/flux/vortex_results?request_id=%s&timeout=%s' % (
                        req.scheme, req.host, request_id, str(wait_for))
                    results = {
                        'request_id': request_id,
                        'results': None,
                        'reason': 'No results were returned . The request can be retried later, results will be available for 1 hour.',
                        'retry_url': retry_url}
                    resp.text = json.dumps(results)
                    end_request_timer = timer()
                    try:
                        logger.info('listen :: vortex_results :: request_id: %s no results returned after 3 retries, took %.6f seconds to process' % (
                            request_id, (end_request_timer - start_request_timer)))
                    except Exception as err:
                        logger.error('error :: listen :: failed to calculate request time - %s' % err)
                    resp.status = falcon.HTTP_200
                    return

                uri = '/flux/vortex_results?request_id=%s&retries=%s&timeout=%s' % (request_id, str(retries), str(wait_for))
                if shard_request:
                    uri = '%s&shard=%s' % (uri, str(shard))
                logger.info('listen :: vortex_results :: no results for request_id: %s, redirecting to %s' % (
                    request_id, uri))
                resp.request_id = request_id
                resp.retries = 1
                resp.timeout = wait_for
                resp.location = uri
                resp.status = falcon.HTTP_303
                return

            status_code = 200
            try:
                status_code = results['status_code']
            except:
                status_code = 200

            resp.text = json.dumps(results)
            end_request_timer = timer()
            try:
                logger.info('listen :: vortex_results :: request_id: %s results returned, took %.6f seconds to process' % (
                    request_id, (end_request_timer - start_request_timer)))
            except Exception as err:
                logger.error('error :: listen :: failed to calculate request time - %s' % err)

            if status_code == 200:
                resp.status = falcon.HTTP_200
            elif status_code == 400:
                resp.status = falcon.HTTP_400
            else:
                resp.status = falcon.HTTP_500
            return

        except Exception as err:
            logger.error('error :: listen :: vortex_results :: request failed - %s' % (
                err))
            logger.info('listen :: vortex_results :: returning 500')
            resp.status = falcon.HTTP_500
            return
