"""
tornado.py

flux/tornado provides pre-loaded njit algorithms such as stump.
"""
import sys
import os
import gzip
from time import time, sleep
import json
import traceback
import uuid

from timeit import default_timer as timer

from logger import set_up_logging

import falcon

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# This prevents flake8 E402 - module level import not at top of file
if True:
    import settings
    import flux
    from skyline_functions import get_redis_conn_decoded
    from tornadoes.tornado_stump import tornado_stump

FLUX_SELF_API_KEY = settings.FLUX_SELF_API_KEY

this_host = str(os.uname()[1])

logger = set_up_logging(None)

LOCAL_DEBUG = False
TIMINGS = True
init_timings_dict = {}

try:
    redis_conn_decoded = get_redis_conn_decoded('flux')
except:
    redis_conn_decoded = None


class TornadoPost(object):

    def on_post(self, req, resp):
        """
        The /flux/tornado?algorithm=[algorithm] endpoint is called via a POST
        with a json object as shown below in the example.


        For example::
            {
                "key": FLUX_SELF_API_KEY,
                "algorithm": "stump",
                "ts": [timeseries_values_list],
                "m": [windows],
            }

        """
        try:
            postData = None

            post_req_start = int(time())

            timings_dict = {}

            request_id = '%s.%s' % (str(post_req_start), str(uuid.uuid4()))
            timings_dict['request_id'] = request_id
            if TIMINGS:
                logger.debug('debug :: tornado :: request_id: %s - handling POST tornado request' % request_id)

            start_request_timer = timer()

            # Preserve the request for debugging
            postData_obj = None
            try:
                postData_obj = req.stream.read()
            except Exception as err:
                logger.error('error :: tornado :: req.stream.read() failed - %s' % err)

            content_encoding = None
            try:
                content_encoding = req.get_header('content-encoding')
            except Exception as err:
                logger.error('tornado :: get_header(\'content-encoding\') error - %s' % str(err))
            if content_encoding == 'gzip':
                if TIMINGS:
                    logger.info('tornado :: content_encoding: %s, decompressing' % str(content_encoding))
                    start_gzip_timer = timer()
                try:
                    postData_obj = gzip.decompress(postData_obj)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: tornado :: gzip.decompress(postData_obj) failed - %s' % err)
                if TIMINGS:
                    end_gzip_timer = timer()
                    logger.debug('debug :: tornado :: gzip decompression took %.6f seconds' % (end_gzip_timer - start_gzip_timer))

            try:
                postData = json.loads(postData_obj)
            except json.decoder.JSONDecodeError:
                message = 'invalid json data'
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.info('tornado :: %s, returning 400' % message)
                resp.status = falcon.HTTP_400
                return

            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: tornado :: invalid post data - %s' % err)
            if LOCAL_DEBUG:
                logger.debug('debug :: tornado :: request arguments and POST data - %s - %s' % (
                    str(req.query_string), str(postData)))

            if not postData:
                logger.warning('warning :: tornado :: no POST data recieved')
                resp.status = falcon.HTTP_400
                return

            if TIMINGS:
                timings_dict['read_object_timing'] = '%.6f' % round((timer() - start_request_timer), 6)

            # status parameter so that the flux tornado process can be monitored
            status = None
            try:
                status = str(postData['status'])
            except KeyError:
                status = False
            if status:
                try:
                    status_timing_start = time()
                    status_set = False
                    status_redis_times_checked = 0
                    while not status_set and status_redis_times_checked < 10:
                        status_redis_times_checked += 1
                        try:
                            redis_conn_decoded = get_redis_conn_decoded('flux')
                            redis_conn_decoded.setex('flux.tornado', 120, int(time()))
                            status_set = True
                        except Exception as err:
                            logger.error('error :: tornado :: setex flux.tornado failed - %s' % err)
                        if not status_set:
                            if not status_timing_start:
                                status_timing_start = time()
                            sleep(0.1)
                    if status_timing_start:
                        logger.info('tornado :: POST status - ok - setex flux.tornado set %s after %s attempts, took %s seconds' % (
                            str(status_set), str(status_redis_times_checked),
                            str((time() - status_timing_start))))
                    # Try a few times if there is a failure and report
                    if status_set:
                        body = {"status": "ok"}
                        resp.text = json.dumps(body)
                        resp.status = falcon.HTTP_200
                    else:
                        logger.error('error :: tornado :: could not set flux.tornado Redis key for POST status')
                        resp.status = falcon.HTTP_500
                    logger.info('tornado :: POST status - took %s seconds' % (
                        str((timer() - start_request_timer))))

                    if TIMINGS:
                        timings_dict['status'] = 1
                        timings_dict['request_time'] = '%.6f' % round((timer() - start_request_timer), 6)
                        logger.debug('debug :: tornado :: request_id: %s, timings_dict: %s' % (
                            request_id, str(timings_dict)))
                    return
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: tornado :: could not validate the status key POST request argument - %s - %s' % (
                        str(req.query_string), err))
                    resp.status = falcon.HTTP_500
                    return

            key = None
            try:
                key = str(postData['key'])
            except KeyError:
                key = None
                message = 'no key passed'
                body = {"code": 400, "message": message}
                resp.text = json.dumps(body)
                logger.info('tornado :: %s, returning 400' % message)
                resp.status = falcon.HTTP_400
                return

            if key != FLUX_SELF_API_KEY:
                message = 'no key passed'
                body = {"code": 401, "message": message}
                resp.text = json.dumps(body)
                logger.info('tornado :: %s, returning 401' % message)
                resp.status = falcon.HTTP_401
                return

            profile = []
            run_stump = False
            custom_algorithm_modules = ['stumpy','stump','stumpy.stamp','custom_algorithm_sources.stumpy','custom_algorithm_sources.stumpy.stump']
            if 'algorithm' in postData:
                if postData['algorithm'] == 'stump':
                    try:
                        algorithm_modules_loaded = [i for i in list(sys.modules.keys()) if i in custom_algorithm_modules]
                        logger.info('tornado :: %s :: algorithm_modules_loaded (before): %s' % (
                            postData['algorithm'], str(algorithm_modules_loaded)))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: tornado :: determine and log algorithm_modules_loaded, err: %s' % (
                            err))

                    run_stump = True
                    try:
                        profile = tornado_stump(postData)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: tornado :: tornado_stump failed, err: %s' % (
                            err))
                        resp.status = falcon.HTTP_500
                        return
                    algorithm_modules_loaded = [i for i in list(sys.modules.keys()) if i in custom_algorithm_modules]
                    logger.debug('debug :: tornado :: %s :: algorithm_modules_loaded (after): %s' % (
                        postData['algorithm'], str(algorithm_modules_loaded)))

            request_time = round((timer() - start_request_timer), 6)
            timings_dict['request_time'] = round((timer() - start_request_timer), 6)
            if TIMINGS:
                logger.info('tornado :: request_id: %s, timings_dict: %s' % (
                    request_id, str(timings_dict)))
            if run_stump:
                body = {"status": {"code": 200, "request_id": request_id, "request_time": request_time}, "data": {"profile": profile}}
                resp.text = json.dumps(body)
                logger.info('tornado :: returning stump profile of length: %s' % str(len(profile)))
                resp.status = falcon.HTTP_200
                return

            return
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: tornado :: tornado failed, returning 500 - %s' % err)
            resp.status = falcon.HTTP_500
