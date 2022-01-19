import sys
import os
from time import time
import json
import traceback
from multiprocessing import Queue
from logger import set_up_logging
import string
from ast import literal_eval

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
    FLUX_API_KEYS = settings.FLUX_API_KEYS.copy()
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
    FLUX_AGGREGATE_NAMESPACES = settings.FLUX_AGGREGATE_NAMESPACES.copy()
    # aggregate_namespaces = list(FLUX_AGGREGATE_NAMESPACES.keys())
except:
    FLUX_AGGREGATE_NAMESPACES = {}
    # aggregate_namespaces = []
try:
    FLUX_EXTERNAL_AGGREGATE_NAMESPACES = settings.FLUX_EXTERNAL_AGGREGATE_NAMESPACES
except:
    FLUX_EXTERNAL_AGGREGATE_NAMESPACES = False

# @modified 20191129 - Branch #3262: py3
# Consolidate flux logging
# logger = set_up_logging('listen')
logger = set_up_logging(None)

LOCAL_DEBUG = False

ALLOWED_CHARS = ['+', '-', '%', '.', '_', '/', '=']
for char in string.ascii_lowercase:
    ALLOWED_CHARS.append(char)
for char in string.ascii_uppercase:
    ALLOWED_CHARS.append(char)
for char in string.digits:
    ALLOWED_CHARS.append(char)

# @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
redis_conn = get_redis_conn('flux')
redis_conn_decoded = None

# @added 20210406 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
aggregate_metrics = []
if FLUX_AGGREGATE_NAMESPACES or FLUX_EXTERNAL_AGGREGATE_NAMESPACES:
    redis_conn_decoded = get_redis_conn_decoded('flux')
    try:
        aggregate_metrics = list(redis_conn_decoded.smembers('metrics_manager.flux.aggregate_metrics'))
        logger.info('listen :: there are %s metrics in the metrics_manager.flux.aggregate_metrics Redis set' % (
            str(len(aggregate_metrics))))
    except Exception as e:
        logger.error('error :: listen :: could get Redis set metrics_manager.flux.aggregate_metrics - %s' % str(e))
        aggregate_metrics = []

external_flux_keys = {}
skyline_external_settings = {}
if not redis_conn_decoded:
    redis_conn_decoded = get_redis_conn_decoded('flux')
try:
    skyline_external_settings_raw = redis_conn_decoded.get('skyline.external_settings')
    if skyline_external_settings_raw:
        skyline_external_settings = literal_eval(skyline_external_settings_raw)
except Exception as e:
    logger.error('error :: listen :: could get Redis set skyline.external_settings - %s' % str(e))
    skyline_external_settings = {}
if skyline_external_settings:
    for settings_key in list(skyline_external_settings.keys()):
        if 'flux_token' in list(skyline_external_settings[settings_key].keys()):
            flux_token = skyline_external_settings[settings_key]['flux_token']
            if flux_token:
                valid_keys.append(str(flux_token))
                external_flux_keys[flux_token] = skyline_external_settings[settings_key]['namespace']


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
        UPDATED_FLUX_API_KEYS = settings.FLUX_API_KEYS.copy()
    except:
        pass
    if UPDATED_FLUX_API_KEYS:
        for flux_api_key in UPDATED_FLUX_API_KEYS:
            try:
                updated_valid_keys.append(str(flux_api_key))
            except:
                pass
    updated_external_flux_keys = {}
    updated_skyline_external_settings = {}
    if not redis_conn_decoded:
        redis_conn_decoded = get_redis_conn_decoded('flux')
    try:
        updated_skyline_external_settings_raw = redis_conn_decoded.get('skyline.external_settings')
        if updated_skyline_external_settings_raw:
            updated_skyline_external_settings = literal_eval(updated_skyline_external_settings_raw)
    except Exception as e:
        logger.error('error :: listen :: could get Redis set skyline.external_settings - %s' % str(e))
        updated_skyline_external_settings = {}
    if updated_skyline_external_settings:
        for settings_key in list(updated_skyline_external_settings.keys()):
            if 'flux_token' in list(updated_skyline_external_settings[settings_key].keys()):
                flux_token = updated_skyline_external_settings[settings_key]['flux_token']
                if flux_token:
                    updated_valid_keys.append(str(flux_token))
                    updated_external_flux_keys[flux_token] = skyline_external_settings[settings_key]['namespace']
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
                if int(last_valid_keys_update) < (int(time()) - 60):
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
                    if int(update_external_settings_key):
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
    try:
        timestamp = int(timestamp)
    except:
        logger.error('error :: %s :: the timestamp is not an int - %s' % (
            caller, str(timestamp)))
        return False
    try:
        if len(str(timestamp)) != 10:
            logger.error('error :: %s :: the timestamp value is not 10 digits - %s' % (
                caller, str(timestamp)))
            return False
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
            timestampInvalidReason = 'timestamp is too old'
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
            return False
    except:
        logger.error('error :: %s :: validating timestamp argument value - %s' % (
            caller, str(timestamp)))
        return False
    return True


# @added 20201006 - Feature #3764: flux validate metric name
def validate_metric_name(caller, metric):
    for vchar in metric:
        if vchar not in ALLOWED_CHARS:
            logger.error('error :: %s :: invalid char in metric - %s' % (
                caller, str(metric)))
            return False
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
        return False
    metric_dir = metric.replace('.', '/')
    full_whisper_path = '%s/%s.wsp' % (FLUX_GRAPHITE_WHISPER_PATH, metric_dir)
    if len(full_whisper_path) > 4096:
        logger.error('error :: %s :: metric path longer than 4096 chars - %s' % (
            caller, str(full_whisper_path)))
        return False
    return True


# @added 20210406 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
# To reduce the overhead of making a redis query for every metric
# submitted, the metric name is checked against the initial loaded
# aggregate_metrics list, if not found in there then the elements of the
# metric name are checked to see if element could any possible match an
# aggregation namespace, only then is Redis queried.
def add_to_aggregate_metric_queue(metric, metric_data, aggregate_metrics):
    added_to_aggregation_queue = False
    return_status_code = 204
    aggregate_metric = False
    # First check the list of aggregate_metrics created at startup from
    # the analyzer/metrics_manager metrics_manager.flux.aggregate_metrics Redis
    # set and if not in there try the Redis metrics_manager.flux.aggregate_metrics.hash
    if aggregate_metrics:
        if metric in aggregate_metrics:
            aggregate_metric = True
    aggregate_metric_timestamp = None
    if not aggregate_metric:
        try:
            # current_redis_conn_decoded = get_redis_conn_decoded('flux')
            aggregate_metric_timestamp = int(redis_conn_decoded.hget('metrics_manager.flux.aggregate_metrics.hash', metric))
        except Exception as e:
            if LOCAL_DEBUG:
                logger.error('error :: listen :: could get metric %s from Redis hash metrics_manager.flux.aggregate_metrics.hash - %s' % (metric, str(e)))
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
                            logger.error('error :: listen :: could delete metric %s from Redis hash metrics_manager.flux.aggregate_metrics.hash - %s' % (metric, str(e)))
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
            if FLUX_VERBOSE_LOGGING:
                logger.debug('debug :: listen :: data added to flux.aggregator.queue Redis set - %s' % str(metric_data))
            logger.info('listen :: data added to flux.aggregator.queue Redis set - %s' % str(metric_data))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: listen :: failed to add data to flux.aggregator.queue Redis set - %s' % str(metric_data))
            return_status_code = 500
            return added_to_aggregation_queue, return_status_code
    return added_to_aggregation_queue, return_status_code


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

        # @modified 20200115 - Feature #3394: flux health check
        # Added status parameter so that the flux listen process can be monitored
        # @modified 20200206 - Feature #3444: Allow flux to backfill
        # Added the fill parameter
        validGetArguments = ['key', 'metric', 'value', 'timestamp', 'status', 'fill']

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
                    except Exception as e:
                        logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_parameters - %s' % e)

                    # @added 20220118 - Feature #4380: flux - return reason with 400
                    message = 'Invalid parameter received - %s' % str(request_param_key)
                    body = {"code": 400, "message": message}
                    resp.body = json.dumps(body)
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
                resp.body = json.dumps(body)
                logger.info('listen :: %s, returning 400' % message)

                resp.status = falcon.HTTP_400
                return

            # @added 20200115 - Feature #3394: flux health check
            # Added status parameter
            try:
                if str(request_param_key) == 'status':
                    logger.info('listen :: GET %s - ok' % str(req.query_string))
                    body = {"status": "ok"}
                    resp.body = json.dumps(body)
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
                        resp.body = json.dumps(body)
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
                resp.body = json.dumps(body)
                logger.info('listen :: %s, returning 400' % message)

                # @modified 20210909
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
                        valid_timestamp = validate_timestamp(valid_timestamp_message, str(request_param_value))
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
                        message = 'invalid timestamp - %s' % str(request_param_value)
                        body = {"code": 400, "message": message}
                        resp.body = json.dumps(body)
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
                resp.body = json.dumps(body)
                logger.info('listen :: %s, returning 400' % message)

                resp.status = falcon.HTTP_400
                return

            if str(request_param_key) == 'metric':
                metric = str(request_param_value)
                # @added 20201006 - Feature #3764: flux validate metric name
                # @modified 20201207 - Task #3864: flux - try except everything
                try:
                    valid_metric_name = validate_metric_name('listen :: MetricData GET', str(request_param_key))
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
                    message = 'invalid metric - %s' % str(request_param_value)
                    body = {"code": 400, "message": message}
                    resp.body = json.dumps(body)
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
                    resp.body = json.dumps(body)
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
            resp.body = json.dumps(body)
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
            resp.body = json.dumps(body)
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
            resp.body = json.dumps(body)
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
            # TODO
            # for now pass
            pass

        if not timestamp:
            timestamp = int(time())

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
        if aggregate_metrics:
            added_to_aggregation_queue, return_status_code = add_to_aggregate_metric_queue(metric, metric_data, aggregate_metrics)
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
            logger.error('error :: listen :: failed to increment to Redis key flux.listen.added_to_queue - %s' % e)

        if LOCAL_DEBUG:
            try:
                queue_size = flux.httpMetricDataQueue.qsize()
                logger.info('listen :: flux.httpMetricDataQueue.qsize - %s' % str(queue_size))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: failed to determine flux.httpMetricDataQueue.qsize')

        if LOCAL_DEBUG:

            # @modified 20201207 - Task #3864: flux - try except everything
            try:
                resp.body = json.dumps(payload)
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
        postData = None

        # @added 20210512 - Feature #4060: skyline.flux.worker.discarded metrics
        # Preserve the request for debugging
        postData_obj = None
        try:
            postData_obj = req.stream.read()
        except Exception as e:
            logger.error('error :: listen :: req.stream.read() falied - %s' % e)

        try:
            # @added 20210512 - Feature #4060: skyline.flux.worker.discarded metrics
            # postData = json.loads(req.stream.read())
            postData = json.loads(postData_obj)
        except json.decoder.JSONDecodeError:
            # @added 20220118 - Feature #4380: flux - return reason with 400
            message = 'invalid json data'
            body = {"code": 400, "message": message}
            resp.body = json.dumps(body)
            logger.info('listen :: %s, returning 400' % message)

            resp.status = falcon.HTTP_400
            return

        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: listen :: invalid post data - %s' % e)
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
            except Exception as e:
                logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_parameters no POST data- %s' % e)

            try:
                redis_conn.set('flux.listen.discarded.invalid_post_data', str(postData_obj))
            except Exception as e:
                logger.error('error :: listen :: failed to data to Redis set flux.listen.discarded.invalid_post_data - %s' % e)

            # @added 20220118 - Feature #4380: flux - return reason with 400
            message = 'no POST date recieved'
            body = {"code": 400, "message": message}
            resp.body = json.dumps(body)
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

        # @added 20211018 - Feature #4284: flux - telegraf
        # Allow the key to be passed as a HTTP header rather than in the json
        # payload
        header_apikey = None
        try:
            header_apikey = req.get_header('apikey')
        except Exception as err:
            logger.error('listen :: get_header(\'apikey\') error - %s' % str(err))
        if header_apikey:
            logger.info('debug :: listen :: get_header(\'apikey\') - %s' % str(header_apikey))

        # @added 20200115 - Feature #3394: flux health check
        # Added status parameter so that the flux listen process can be monitored
        status = None
        try:
            status = str(postData['status'])
        except KeyError:
            status = False
        if status:
            try:
                logger.info('listen :: POST status - ok')
                body = {"status": "ok"}
                resp.body = json.dumps(body)
                resp.status = falcon.HTTP_200
                return
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: could not validate the status key POST request argument - %s - %s' % (
                    str(req.query_string), e))
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
                resp.body = json.dumps(body)
                logger.info('listen :: %s, returning 400' % message)
                resp.status = falcon.HTTP_400
                return

            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: could not determine the key from POST data - %s - %s' % (
                    str(postData), e))

        try:
            # @modified 20211018 - Feature #4284: flux - telegraf
            # Allow the key to be passed as a HTTP header or in the json payload
            # key = str(postData['key'])

            # @modified 20200818 - Feature #3694: flux - POST multiple metrics
            # Added metric_namespace_prefix
            keyValid, metric_namespace_prefix = validate_key('listen :: MetricDataPOST POST', key)
            if not keyValid:
                logger.error('error :: listen :: invalid key in POST data - %s - %s' % (
                    key, str(postData)))

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
                resp.body = json.dumps(body)
                logger.warning('warning :: listen :: %s, returning 401' % message)

                # @modified 20210909
                # Return 401 if bad key
                # resp.status = falcon.HTTP_400
                resp.status = falcon.HTTP_401
                return

            if LOCAL_DEBUG:
                logger.debug('debug :: listen :: valid key, metric_namespace_prefix set to %s' % str(metric_namespace_prefix))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: listen :: could not validate the key from POST data - %s - %s' % (
                str(postData), e))

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
            resp.body = json.dumps(body)
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
            logger.info('debug :: listen :: validate_key: %s' % str(validated_key))
        except KeyError:
            validated_key = False
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: listen :: failed to determine validated_key - %s' % str(err))
        if validated_key:
            try:
                body = {"status": "key valid"}
                resp.body = json.dumps(body)
                logger.info('debug :: listen :: validate_key returning 200')
                resp.status = falcon.HTTP_200
                return
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: failed to response to validate_key request - %s' % str(err))

        # @added 20200818 - Feature #3694: flux - POST multiple metrics
        # Determine if the POST data is for a single metric or multiple metrics
        metrics = {}
        try:
            metrics = postData['metrics']
            if LOCAL_DEBUG:
                logger.debug('debug :: listen :: metrics is set in the POST data - %s' % str(metrics))
                for item in metrics:
                    logger.debug('debug :: listen :: metrics item - %s' % str(item))
                    logger.debug('debug :: listen :: metrics item - metric - %s' % str(item['metric']))
        except KeyError:
            metrics = {}
        except Exception as e:
            logger.error('error :: listen :: metrics was passed in the request POST data but an error was encountered - returned 400 - %s - %s' % (
                str(postData), e))

            # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
            unique_value = '%s' % str(time())
            try:
                redis_conn.sadd('flux.listen.discarded.invalid_parameters', str(unique_value))
                if FLUX_VERBOSE_LOGGING:
                    logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_parameters' % str(unique_value))
            except Exception as e:
                logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_parameters - %s' % e)

            # @added 20220118 - Feature #4380: flux - return reason with 400
            message = 'invalid metrics element'
            body = {"code": 400, "message": message}
            resp.body = json.dumps(body)
            logger.info('listen :: %s, returning 400' % message)

            resp.status = falcon.HTTP_400
            return

        # @added 20211018 - Feature #4284: flux - telegraf
        # Allow the key to be passed as a HTTP header rather than in the json
        # payload
        telegraf_payload = False
        telegraf_keys_list = ['fields', 'name', 'tags', 'timestamp']
        if metrics:
            if isinstance(metrics, list):
                if len(metrics) > 0:
                    if isinstance(metrics[0], dict):
                        if sorted(list(metrics[0].keys())) == telegraf_keys_list:
                            telegraf_payload = True
        telegraf_metrics = []
        if telegraf_payload:
            for telegraf_metric_dict in metrics:
                # Create metric name
                # TODO
                # metric = '%s.'  % (
                #     telegraf_metric_dict['tags']['host'],
                #     telegraf_metric_dict['tags']['host'],
                # )
                metric = None
                telegraf_metrics.append(metric)

        if metrics:
            # added 20201211 - Feature #3694: flux - POST multiple metrics
            # Added metric count
            if FLUX_VERBOSE_LOGGING:
                try:
                    logger.info('listen :: POST mulitple metric data received with %s entries' % str(len(metrics)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: could not report number of entries submitted in POST mulitple metric data')

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
                    valid_metric_name = validate_metric_name('listen :: MetricDataPOST POST multiple metrics', str(metric))
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
                        message = 'invalid metric - %s' % str(request_param_value)
                        body = {"code": 400, "message": message}
                        resp.body = json.dumps(body)
                        logger.info('listen :: %s, returning 400' % message)

                        resp.status = falcon.HTTP_400
                        return

                    if LOCAL_DEBUG:
                        logger.debug('debug :: listen :: metric from postData set to %s' % metric)
                except:
                    logger.error('error :: listen :: no valid metric in the request POST metrics data - returned 400 - %s' % (
                        str(postData)))

                    # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                    metric = 'none.%s' % str(time())
                    try:
                        redis_conn.sadd('flux.listen.discarded.metric_name', str(metric))
                        if FLUX_VERBOSE_LOGGING:
                            logger.info('listen :: added %s to Redis set flux.listen.discarded.metric_name' % str(metric))
                    except Exception as e:
                        logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.metric_name - %s' % e)

                    # @added 20220118 - Feature #4380: flux - return reason with 400
                    message = 'invalid metric'
                    body = {"code": 400, "message": message}
                    resp.body = json.dumps(body)
                    logger.info('listen :: %s, returning 400' % message)

                    resp.status = falcon.HTTP_400
                    return
                try:
                    fill = str(metric_data['fill'])
                    if fill == 'true':
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
                            valid_timestamp = validate_timestamp('listen :: MetricDataPOST POST multiple metrics', str(timestamp_present))
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
                            message = 'invalid timestamp - %s' % str(timestamp_present)
                            body = {"code": 400, "message": message}
                            resp.body = json.dumps(body)
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
                        message = 'invalid timestamp'
                        body = {"code": 400, "message": message}
                        resp.body = json.dumps(body)
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
                        logger.error('error :: listen :: invalid value from POST data - %s' % (
                            str(postData)))

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
                        message = 'invalid value - %s' % str(metric_data['value'])
                        body = {"code": 400, "message": message}
                        resp.body = json.dumps(body)
                        logger.info('listen :: %s, returning 400' % message)

                        resp.status = falcon.HTTP_400
                        return
                if not metric:
                    logger.error('error :: listen :: no metric in the POST data - %s - returning 400' % (
                        str(postData)))

                    # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                    metric = 'none.%s' % str(time())
                    try:
                        redis_conn.sadd('flux.listen.discarded.metric_name', str(metric))
                        if FLUX_VERBOSE_LOGGING:
                            logger.info('listen :: added %s to Redis set flux.listen.discarded.metric_name' % str(metric))
                    except Exception as e:
                        logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.metric_name - %s' % e)

                    # @added 20220118 - Feature #4380: flux - return reason with 400
                    message = 'invalid metric'
                    body = {"code": 400, "message": message}
                    resp.body = json.dumps(body)
                    logger.info('listen :: %s, returning 400' % message)

                    resp.status = falcon.HTTP_400
                    return
                if not valid_value:
                    logger.error('error :: listen :: no valid value in the POST data - %s - returning 400' % (
                        str(postData)))

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
                        message = 'no value parameter present'
                    else:
                        message = 'invalid value'
                    body = {"code": 400, "message": message}
                    resp.body = json.dumps(body)
                    logger.info('listen :: %s, returning 400' % message)

                    resp.status = falcon.HTTP_400
                    return
                if not timestamp:
                    timestamp = int(time())

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

                    # @added 20210406 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                    if aggregate_metrics:
                        added_to_aggregation_queue, return_status_code = add_to_aggregate_metric_queue(metric, metric_data, aggregate_metrics)
                        if added_to_aggregation_queue:
                            if return_status_code == 500:
                                resp.status = falcon.HTTP_500
                                return
                            if return_status_code == 204:
                                resp.status = falcon.HTTP_204
                            continue

                    flux.httpMetricDataQueue.put(metric_data, block=False)
                    # modified 20201016 - Feature #3788: snab_flux_load_test
                    if FLUX_VERBOSE_LOGGING:
                        logger.info('listen :: POST mulitple metric data added to flux.httpMetricDataQueue - %s' % str(metric_data))

                    # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                    try:
                        redis_conn.incr('flux.listen.added_to_queue')
                    except Exception as e:
                        logger.error('error :: listen :: failed to increment to Redis key flux.listen.added_to_queue - %s' % e)

                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: adding POST metric_data to the flux.httpMetricDataQueue queue - %s' % str(metric_data))
                    resp.status = falcon.HTTP_500
                    return

        if not metrics:
            try:
                metric = str(postData['metric'])

                # @added 20201006 - Feature #3764: flux validate metric name
                valid_metric_name = validate_metric_name('listen :: MetricDataPOST POST', str(metric))
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
                    message = 'invalid metric'
                    body = {"code": 400, "message": message}
                    resp.body = json.dumps(body)
                    logger.info('listen :: %s, returning 400' % message)

                    resp.status = falcon.HTTP_400
                    return

                if LOCAL_DEBUG:
                    logger.debug('debug :: listen :: metric from postData set to %s' % metric)
            except:
                logger.error('error :: listen :: no valid metric in request POST data - returned 400 - %s' % (
                    str(postData)))

                # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                try:
                    metric = str(postData['metric'])
                except KeyError:
                    if FLUX_VERBOSE_LOGGING:
                        logger.info('listen :: no metric key in POST data using time() for flux.listen.discarded.metric_name entry')
                    metric = str(time())
                try:
                    redis_conn.sadd('flux.listen.discarded.metric_name', str(metric))
                    if FLUX_VERBOSE_LOGGING:
                        logger.info('listen :: added %s to Redis set flux.listen.discarded.metric_name' % str(metric))
                except Exception as e:
                    logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.metric_name - %s' % e)

                # @added 20220118 - Feature #4380: flux - return reason with 400
                message = 'invalid metric'
                body = {"code": 400, "message": message}
                resp.body = json.dumps(body)
                logger.info('listen :: %s, returning 400' % message)

                resp.status = falcon.HTTP_400
                return

            # @added 20200206 - Feature #3444: Allow flux to backfill
            try:
                fill = str(postData['fill'])
                if fill == 'true':
                    backfill = True
            except:
                backfill = False

            # @added 20210718
            try:
                aggregate = postData['aggregate']
                if aggregate == 'true':
                    aggregate = True
            except KeyError:
                aggregate = False
            except:
                aggregate = False

            try:
                timestamp_present = str(postData['timestamp'])
            except:
                timestamp_present = False
            if timestamp_present:
                try:
                    # @modified 20200206 - Feature #3444: Allow flux to backfill
                    # Only valid_timestamp is this is not a backfill request
                    if not backfill:
                        # @modified 20220114 - lint message
                        valid_timestamp_message = 'listen :: MetricDataPOST POST - timestamp - %s' % str(postData['timestamp'])
                        # valid_timestamp = validate_timestamp('listen :: MetricDataPOST POST - timestamp - %s', str(postData['timestamp']))
                        valid_timestamp = validate_timestamp(valid_timestamp_message, str(postData['timestamp']))
                    else:
                        valid_timestamp = True
                    if valid_timestamp:
                        timestamp = int(postData['timestamp'])
                    else:
                        # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                        try:
                            redis_conn.sadd('flux.listen.discarded.invalid_timestamp', str(metric))
                            if FLUX_VERBOSE_LOGGING:
                                logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_timestamp' % str(metric))
                        except Exception as e:
                            logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_timestamp - %s' % e)

                        # @added 20220118 - Feature #4380: flux - return reason with 400
                        try:
                            message = 'invalid timestamp - %s' % str(postData['timestamp'])
                        except Exception as err:
                            message = 'invalid timestamp'
                        body = {"code": 400, "message": message}
                        resp.body = json.dumps(body)
                        logger.info('listen :: %s, returning 400' % message)

                        resp.status = falcon.HTTP_400
                        return
                except:
                    logger.error('error :: listen :: invalid timestamp value in POST data - %s' % (
                        str(postData)))

                    # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                    try:
                        redis_conn.sadd('flux.listen.discarded.invalid_timestamp', str(metric))
                        if FLUX_VERBOSE_LOGGING:
                            logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_timestamp' % str(metric))
                    except Exception as e:
                        logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_timestamp - %s' % e)

                    # @added 20220118 - Feature #4380: flux - return reason with 400
                    message = 'invalid timestamp'
                    body = {"code": 400, "message": message}
                    resp.body = json.dumps(body)
                    logger.info('listen :: %s, returning 400' % message)

                    resp.status = falcon.HTTP_400
                    return

            value_present = None
            try:
                value_present = str(postData['value'])
            except KeyError:
                value_present = False
            if value_present:
                try:
                    value = float(postData['value'])
                    # valid_value is used as in terms of Python if value evalatuion
                    # if value was 0.0 (or 0) they evaluate as False
                    valid_value = True
                except:
                    logger.error('error :: listen :: invalid value from POST data - %s' % (
                        str(postData)))

                    # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                    if not metric:
                        try:
                            metric = str(postData['metric'])
                        except KeyError:
                            if FLUX_VERBOSE_LOGGING:
                                logger.info('listen :: no metric key in POST data using time() for flux.listen.discarded.invalid_value entry')
                            metric = str(time())
                    unique_value = '%s.%s.%s' % (str(metric), str(timestamp_present), str(value_present))
                    try:
                        redis_conn.sadd('flux.listen.discarded.invalid_value', str(unique_value))
                        if FLUX_VERBOSE_LOGGING:
                            logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_value' % str(unique_value))
                    except Exception as e:
                        logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_value - %s' % e)

                    # @added 20220118 - Feature #4380: flux - return reason with 400
                    message = 'invalid value'
                    body = {"code": 400, "message": message}
                    resp.body = json.dumps(body)
                    logger.info('listen :: %s, returning 400' % message)

                    resp.status = falcon.HTTP_400
                    return

            if not key:
                logger.error('error :: listen :: no key in the POST data - %s - returning 401' % (
                    str(postData)))

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
                body = {"code": 401, "message": message}
                resp.body = json.dumps(body)
                logger.warning('warning :: listen :: %s, returning 401' % message)

                # @modified 20210909
                # Return 401 if bad key
                # resp.status = falcon.HTTP_400
                resp.status = falcon.HTTP_401
                return
            if not metric:
                logger.error('error :: listen :: no metric in the POST data - %s - returning 400' % (
                    str(postData)))

                # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                unique_value = str(time())
                try:
                    redis_conn.sadd('flux.listen.discarded.metric_name', str(unique_value))
                    if FLUX_VERBOSE_LOGGING:
                        logger.info('listen :: added %s (no metric passed) to Redis set flux.listen.discarded.metric_name' % str(unique_value))
                except Exception as e:
                    logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.metric_name - %s' % e)

                # @added 20220118 - Feature #4380: flux - return reason with 400
                message = 'no metric data passed'
                body = {"code": 400, "message": message}
                resp.body = json.dumps(body)
                logger.info('listen :: %s, returning 400' % message)

                resp.status = falcon.HTTP_400
                return
            if not valid_value:
                logger.error('error :: listen :: no valid value in the POST data - %s - returning 400' % (
                    str(postData)))

                # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                unique_value = '%s.%s.%s' % (str(metric), str(timestamp_present), str(value_present))
                try:
                    redis_conn.sadd('flux.listen.discarded.invalid_value', str(unique_value))
                    if FLUX_VERBOSE_LOGGING:
                        logger.info('listen :: added %s to Redis set flux.listen.discarded.invalid_value' % str(unique_value))
                except Exception as e:
                    logger.error('error :: listen :: failed to add entry to Redis set flux.listen.discarded.invalid_value - %s' % e)

                # @added 20220118 - Feature #4380: flux - return reason with 400
                message = 'invalid value data passed'
                body = {"code": 400, "message": message}
                resp.body = json.dumps(body)
                logger.info('listen :: %s, returning 400' % message)

                resp.status = falcon.HTTP_400
                return

            if not timestamp:
                timestamp = int(time())

            # @added 20210430 - Bug #4046: flux - metric_namespace_prefix on FLUX_SELF_API_KEY conflicting with FLUX_API_KEYS
            # If metrics are being submitted to flux internally (vista) using
            # the FLUX_SELF_API_KEY for a namespace if that namespace is added
            # to FLUX_API_KEYS, flux will begin to also append the namespace
            # prefix from the FLUX_API_KEYS for metrics submitted with the
            # FLUX_SELF_API_KEY
            if metric_namespace_prefix:
                if metric.startswith(metric_namespace_prefix):
                    metric_namespace_prefix = None

            # @added 20200818 - Feature #3694: flux - POST multiple metrics
            # Added metric_namespace_prefix which is declared via the FLUX_API_KEYS
            if metric_namespace_prefix:
                metric = '%s.%s' % (str(metric_namespace_prefix), metric)

            # Queue the metric
            try:
                # @modified 20200206 - Feature #3444: Allow flux to backfill
                # Added backfill
                # @modified 20210718
                # Added aggregate
                metric_data = [metric, value, timestamp, backfill, aggregate]
                # @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
                # Add to data to the flux.queue Redis set
                if FLUX_PERSIST_QUEUE and metric_data:
                    try:
                        redis_conn.sadd('flux.queue', str(metric_data))
                    except Exception as e:
                        logger.error('error :: listen :: failed adding data to Redis set flux.queue - %s' % e)

                # @added 20210406 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                # To reduce the overhead of making a redis query for every metric
                # submitted, the metric name is checked against the initial loaded
                # aggregate_metrics list, if not found in there then the elements of the
                # metric name are checked to see if element could any possible match an
                # aggregation namespace, only then is Redis queried.
                # @modified 20210718
                # Added aggregate
                # if aggregate_metrics:
                if aggregate_metrics or aggregate:
                    added_to_aggregation_queue, return_status_code = add_to_aggregate_metric_queue(metric, metric_data, aggregate_metrics)
                    if added_to_aggregation_queue:
                        if return_status_code == 500:
                            resp.status = falcon.HTTP_500
                        if return_status_code == 204:
                            resp.status = falcon.HTTP_204
                        return

                flux.httpMetricDataQueue.put(metric_data, block=False)
                # modified 20201016 - Feature #3788: snab_flux_load_test
                if FLUX_VERBOSE_LOGGING:
                    logger.info('listen :: POST data added to flux.httpMetricDataQueue - %s' % str(metric_data))

                # @added 20210511 - Feature #4060: skyline.flux.worker.discarded metrics
                try:
                    redis_conn.incr('flux.listen.added_to_queue')
                except Exception as e:
                    logger.error('error :: listen :: failed to increment to Redis key flux.listen.added_to_queue - %s' % e)

            except Exception as err:
                logger.error(traceback.format_exc())
                # logger.error('error :: listen :: adding POST metric_data to the flux.httpMetricDataQueue queue - %s' % str(metric_data))
                logger.error('error :: listen :: adding POST metric_data to the flux.httpMetricDataQueue queue - [%s, %s, %s, %s, %s] - %s' % (
                    str(metric), str(value), str(timestamp), str(backfill),
                    str(aggregate), err))
                resp.status = falcon.HTTP_500
                return

        if LOCAL_DEBUG:
            try:
                resp.body = json.dumps(postData)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: failed to json.dumps postData - %s' % str(postData))
                resp.body = {"postData": "None"}
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_204
        return
