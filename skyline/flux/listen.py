import sys
import os
from time import time
import json
import traceback
from multiprocessing import Queue
from logger import set_up_logging

import falcon

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# @modified 20191115 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
if True:
    import settings
    import flux

# @modified 20191129 - Branch #3262: py3
# Consolidate flux logging
# logger = set_up_logging('listen')
logger = set_up_logging(None)

LOCAL_DEBUG = False


def validate_key(caller, apikey):
    try:
        isAlNum = False
        isAlNum = apikey.isalnum()
        if isAlNum:
            keyLength = len(apikey)
            if keyLength == 32:
                # Check to determine if it is a valid API key
                keyValid = True
            else:
                logger.error('error :: %s :: invalid api key length of %s - %s' % (
                    caller, str(keyLength), str(apikey)))
        if not keyValid:
            logger.error('error :: %s :: invalid api key - %s' % (
                caller, str(apikey)))
            # resp.status = falcon.HTTP_400
            return False
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to validate api key - %s' % (
            caller, str(apikey)))
        # resp.status = falcon.HTTP_400
        return False
    return True


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
        except:
            logger.error('error :: %s :: validate_timestamp FLUX_MAX_AGE is not set in settings.py, set to the default 3600' % (
                caller))
            flux_max_age = 3600

        # @modified 20200107 - Task #3376: Enable vista and flux to deal with lower frequency data
        # tooOld = now - settings.STALE_PERIOD
        tooOld = now - flux_max_age

        timestampValid = True
        timestampInvalidReason = 'None'
        if timestamp < tooOld:
            timestampInvalidReason = 'timestamp is too old'
            timestampValid = False
        if timestamp > now:
            timestampInvalidReason = 'timestamp is in the future'
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


class MetricData(object):

    def on_get(self, req, resp):
        """
        The /flux/metric_data endpoint is called via a GET with the URI
        parameters as defined below:
        /flux/metric_data?metric=<metric|str>&timestamp=<timestamp|int>&value=<value|float>&key=<key|str>
        For example:
        /flux/metric_data?metric=vista.nodes.skyline-1.cpu.user&timestamp=1478021700&value=1.0&key=YOURown32charSkylineAPIkeySecret
        """

        if LOCAL_DEBUG:
            logger.info('listen :: GET request - %s' % str(req.query_string))

        # @modified 20200115 - Feature #3394: flux health check
        # Added status parameter so that the flux listen process can be monitored
        validGetArguments = ['key', 'metric', 'value', 'timestamp', 'status']

        payload = {}
        payload['query_string'] = str(req.query_string)

        apikey = False
        metric = False
        value = None
        valid_value = False
        timestamp = None

        for request_param_key, request_param_value in req.params.items():
            try:
                if str(request_param_key) not in validGetArguments:
                    logger.error('error :: listen :: invalid key in request arguments - %s - %s' % (
                        str(request_param_key), str(req.query_string)))
                    resp.status = falcon.HTTP_400
                    return
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: validating request arguments - %s' % (
                    str(req.query_string)))
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

            try:
                if str(request_param_key) == 'key':
                    key = str(request_param_value)
                    keyValid = validate_key('listen :: MetricData GET', key)
                    if not keyValid:
                        logger.error('error :: listen :: invalid key in GET request arguments - %s - %s' % (
                            str(request_param_value), str(req.query_string)))
                        resp.status = falcon.HTTP_400
                        return
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: could not validate the key GET request argument - %s' % (
                    str(req.query_string)))
                resp.status = falcon.HTTP_400
                return

            try:
                if str(request_param_key) == 'timestamp':
                    valid_timestamp = validate_timestamp('listen :: MetricData GET', str(request_param_value))
                    if valid_timestamp:
                        timestamp = int(request_param_value)
                    else:
                        resp.status = falcon.HTTP_400
                        return
            except:
                logger.error('error :: listen :: invalid timestamp value from the GET request argument - %s - %s' % (
                    str(request_param_value), str(req.query_string)))
                resp.status = falcon.HTTP_400
                return

            if str(request_param_key) == 'metric':
                metric = str(request_param_value)

            if str(request_param_key) == 'value':
                try:
                    value = float(request_param_value)
                    # valid_value is used as in terms of Python if value evalatuion
                    # if value was 0.0 (or 0) they evaluate as False
                    valid_value = True
                except:
                    logger.error('error :: listen :: invalid value from GET request argument - %s - %s' % (
                        str(request_param_value), str(req.query_string)))
                    resp.status = falcon.HTTP_400
                    return

            request_param_key_str = str(request_param_key)
            payload[request_param_key_str] = str(request_param_value)

        if not key:
            logger.error('error :: listen :: no key in the GET request arguments - %s - returning 400' % (
                str(req.query_string)))
            resp.status = falcon.HTTP_400
            return
        if not metric:
            logger.error('error :: listen :: no metric in the GET request arguments - %s - returning 400' % (
                str(req.query_string)))
            resp.status = falcon.HTTP_400
            return
        if not valid_value:
            logger.error('error :: listen :: no valid value in the GET request arguments - %s - returning 400' % (
                str(req.query_string)))
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

        # metric to add to queue
        metric_data = [metric, value, timestamp]
        payload['metric_data'] = str(metric_data)

        # Queue the metric
        try:
            flux.httpMetricDataQueue.put(metric_data, block=False)
            logger.info('listen :: GET request data added to flux.httpMetricDataQueue - %s' % str(metric_data))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: listen :: failed to add GET request data to flux.httpMetricDataQueue - %s' % str(metric_data))
            resp.status = falcon.HTTP_500
            return

        if LOCAL_DEBUG:
            try:
                queue_size = flux.httpMetricDataQueue.qsize()
                logger.info('listen :: flux.httpMetricDataQueue.qsize - %s' % str(queue_size))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: listen :: failed to determine flux.httpMetricDataQueue.qsize')

        if LOCAL_DEBUG:
            resp.body = json.dumps(payload)
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_204
        return


class MetricDataPost(object):

    def on_post(self, req, resp):
        """
        The /flux/metric_data_post endpoint is called via a POST with a json
        object as defined below
        For example::

            {
                "metric": "metric|str",
                "timestamp": "timestamp|int",
                "value": "value|float",
                "key": "api_key|str"
            }

        For example::

            {
                "metric": "vista.nodes.skyline-1.cpu.user",
                "timestamp": "1478021700",
                "value": "1.0",
                "key": "YOURown32charSkylineAPIkeySecret"
            }

        """
        postData = None
        try:
            postData = json.loads(req.stream.read())
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: listen :: invalid post data')

        if LOCAL_DEBUG:
            logger.info('listen :: request arguments and POST data - %s - %s' % (
                str(req.query_string), str(postData)))

        if not postData:
            if LOCAL_DEBUG:
                logger.info('listen :: no POST data recieved')
            resp.status = falcon.HTTP_400
            return

        # Add metric to add to queue
        metric = None
        timestamp = None
        value = None
        valid_value = None
        key = None

        # @added 20200115 - Feature #3394: flux health check
        # Added status parameter so that the flux listen process can be monitored
        status = None
        try:
            status = str(postData['status'])
        except:
            pass
        if status:
            try:
                logger.info('worker :: POST status - ok')
                body = {"status": "ok"}
                resp.body = json.dumps(body)
                resp.status = falcon.HTTP_200
                return
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: worker :: could not validate the status key POST request argument - %s' % (
                    str(req.query_string)))
                resp.status = falcon.HTTP_400
                return

        try:
            key = str(postData['key'])
            keyValid = validate_key('listen :: MetricDataPOST POST', key)
            if not keyValid:
                logger.error('error :: listen :: invalid key in POST data - %s - %s' % (
                    key, str(postData)))
                resp.status = falcon.HTTP_400
                return
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: listen :: could not validate the key from POST data - %s' % (
                str(postData)))
            resp.status = falcon.HTTP_400
            return

        try:
            metric = str(postData['metric'])
            if LOCAL_DEBUG:
                logger.info('listen :: metric from postData set to %s' % metric)
        except:
            logger.error('error :: listen :: no valid metric in request POST data - returned 400 - %s' % (
                str(postData)))
            resp.status = falcon.HTTP_400
            return

        try:
            timestamp_present = str(postData['timestamp'])
        except:
            timestamp_present = False
        if timestamp_present:
            try:
                valid_timestamp = validate_timestamp('listen :: MetricDataPOST POST', str(postData['timestamp']))
                if valid_timestamp:
                    timestamp = int(postData['timestamp'])
                else:
                    resp.status = falcon.HTTP_400
                    return
            except:
                logger.error('error :: listen :: invalid timestamp value in POST data - %s' % (
                    str(postData)))
                resp.status = falcon.HTTP_400
                return

        try:
            value_present = str(postData['value'])
        except:
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
                resp.status = falcon.HTTP_400
                return

        if not key:
            logger.error('error :: listen :: no key in the POST data - %s - returning 400' % (
                str(postData)))
            resp.status = falcon.HTTP_400
            return
        if not metric:
            logger.error('error :: listen :: no metric in the POST data - %s - returning 400' % (
                str(postData)))
            resp.status = falcon.HTTP_400
            return
        if not valid_value:
            logger.error('error :: listen :: no valid value in the POST data - %s - returning 400' % (
                str(postData)))
            resp.status = falcon.HTTP_400
            return

        if not timestamp:
            timestamp = int(time())

        # Queue the metric
        try:
            metric_data = [metric, value, timestamp]
            flux.httpMetricDataQueue.put(metric_data, block=False)
            logger.info('listen :: POST data added to flux.httpMetricDataQueue - %s' % str(metric_data))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: listen :: adding POST metric_data to the flux.httpMetricDataQueue queue - %s' % str(metric_data))
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
