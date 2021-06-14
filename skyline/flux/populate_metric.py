import sys
import os
import json
import traceback
from ast import literal_eval

# from redis import StrictRedis
import falcon
import graphyte

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

if True:
    import settings
    # @added 20191111 - Bug #3266: py3 Redis binary objects not strings
    #                   Branch #3262: py3
    from skyline_functions import get_redis_conn

    from logger import set_up_logging
    import flux

# @modified 20191129 - Branch #3262: py3
# Consolidate flux logging
# logger = set_up_logging('populate_metric')
logger = set_up_logging(None)

# URI arguments are solely used for identifying requests in the log, all the
# required metric data is submitted via a POST with a json payload.
validArguments = ['remote_target', 'metric', 'namespace_prefix', 'key', 'user']

skyline_app = 'flux'

LOCAL_DEBUG = False

GRAPHITE_METRICS_PREFIX = None
CARBON_HOST = settings.FLUX_CARBON_HOST
CARBON_PORT = settings.FLUX_CARBON_PORT

if settings.FLUX_SEND_TO_CARBON:
    try:
        graphyte.init(CARBON_HOST, port=CARBON_PORT, prefix=None, timeout=5)
        logger.info('populate_metric :: Succeeded to graphyte.init with host: %s, port: %s, prefix: %s' % (
            str(CARBON_HOST), str(CARBON_PORT),
            str(GRAPHITE_METRICS_PREFIX)))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: failed to run graphyte.init with host: %s, port: %s, prefix: %s' % (
            str(CARBON_HOST), str(CARBON_PORT),
            str(GRAPHITE_METRICS_PREFIX)))

# @modified 20191111 - Bug #3266: py3 Redis binary objects not strings
#                      Branch #3262: py3
# if settings.REDIS_PASSWORD:
#     redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
# else:
#     redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
# @added 20191111 - Bug #3266: py3 Redis binary objects not strings
#                   Branch #3262: py3
# Added a single functions to deal with Redis connection and the
# charset='utf-8', decode_responses=True arguments required in py3
redis_conn = get_redis_conn(skyline_app)


class PopulateMetric(object):

    def on_post(self, req, resp):
        """
        The endpoint is called via a POST with a json payload as defined in the
        payload variable in skyline/vista/fetcher.py

        """
        postData = None
        try:
            # postData = req.media
            postData = json.loads(req.stream.read())
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: populate_metric :: invalid post data')

        if LOCAL_DEBUG:
            logger.info('populate_metric :: request arguments and data - %s - %s' % (
                str(req.query_string), str(postData)))

        if not postData:
            resp.status = falcon.HTTP_400
            return

        apiKey = False
        remote_target = False
        namespace_prefix = ''

        # TODO
        # token? in request header Authorization
        # Validate apiKey and token
        # API keys and tokens stored in memcache and populated by worker?
        # Fetch key token
        token = None
        try:
            token = redis_conn.get(apiKey)
        except:
            # TODO
            # for now pass
            pass

        # Add metric to add to queue
        metricDict = postData
        try:
            remote_target = str(metricDict['remote_target'])
            if LOCAL_DEBUG:
                logger.info('populate_metric :: target from metricData set to %s' % remote_target)
            metric = str(metricDict['metric'])
            if LOCAL_DEBUG:
                logger.info('populate_metric :: metric from metricData set to %s' % metric)
            namespace_prefix = str(metricDict['namespace_prefix'])
            if LOCAL_DEBUG:
                logger.info('populate_metric :: namespace_prefix from metricData set to %s' % namespace_prefix)
            key = str(metricDict['key'])
            if LOCAL_DEBUG:
                logger.info('populate_metric :: key from metricData set to %s' % key)
            # @modified 20210421 - Task #4030: refactoring
            # semgrep - python-logger-credential-disclosure
            # token = str(metricDict['token'])
            # if LOCAL_DEBUG:
            #     logger.info('populate_metric :: token from metricData set to %s' % token)
            user = str(metricDict['user'])
            if LOCAL_DEBUG:
                logger.info('populate_metric :: user from metricData set to %s' % user)
            password = str(metricDict['password'])
            if LOCAL_DEBUG:
                logger.info('populate_metric :: password from metricData set to %s' % password)
            if str(metricDict['fetch_resolution_urls']) == 'None':
                if LOCAL_DEBUG:
                    logger.info('No fetch_resolution_urls declared for %s, nothing to do' % remote_target)
                resp.status = falcon.HTTP_400
                return
            else:
                try:
                    fetch_resolution_urls = literal_eval(metricDict['fetch_resolution_urls'])
                    fetch_resolution_urls_type = type(fetch_resolution_urls)
                    if LOCAL_DEBUG:
                        logger.info('populate_metric :: fetch_resolution_urls type %s' % str(fetch_resolution_urls_type))
                        logger.info('populate_metric :: fetch_resolution_urls from metricData is set to %s' % str(fetch_resolution_urls))
                    # for fetch_url in fetch_resolution_urls:
                    #    logger.info('populate_metric :: a fetch_url from metricData is set to %s' % fetch_url)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: populate_metric :: failed to read fetch_resolution_urls list from metricData')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: populate_metric :: failed to read from metricData')

        try:
            apiKeyValid = False
            apiKey = key
            if apiKey == 'None':
                apiKeyValid = True
            if not apiKeyValid:
                isAlNum = False
                isAlNum = apiKey.isalnum()
                if isAlNum:
                    apiKeyLength = len(apiKey)
                    if apiKeyLength == 32:
                        # Check to determine if it is a valid API key
                        apiKeyValid = True
                    else:
                        # @modified 20210421 - Task #4030: refactoring
                        # semgrep - python-logger-credential-disclosure
                        # logger.error('error :: populate_metric :: invalid api key length key in POST data - %s - %s' % (
                        #     str(key), str(apiKeyLength)))
                        logger.error('error :: populate_metric :: invalid api key length key in POST data')
            if not apiKeyValid:
                # @modified 20210421 - Task #4030: refactoring
                # semgrep - python-logger-credential-disclosure
                # logger.error('error :: populate_metric :: invalid api key in POST data - %s - %s' % (
                #     str(key), str(postData)))
                logger.error('error :: populate_metric :: invalid api key in POST data')
                resp.status = falcon.HTTP_400
                return
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: populate_metric :: invalid key in POST data - %s' % (
                str(postData)))
            resp.status = falcon.HTTP_400
            return

        # Queue the metric
        try:
            flux.populateMetricQueue.put(postData, block=False)
            logger.info('populate_metric :: added %s to flux.populateMetricQueue' % str(remote_target))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: populate_metric :: adding target to queue - %s' % str(remote_target))
            resp.status = falcon.HTTP_500
            return

        if LOCAL_DEBUG:
            resp.body = json.dumps(postData)
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_204
        return
