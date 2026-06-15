"""
tornado.py

flux/tornado provides pre-loaded njit algorithms such as stump.
"""
import sys
import os
import gzip
from contextlib import nullcontext
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

    # @added 20251108 - Feature #5665: custom_algorithm - mirage_nirvana
    # Determine what tornadoes to load based on the tornado algorithms
    # used in CUSTOM_ALGORITHMS and load the others as nullcontext
    try:
        CUSTOM_ALGORITHMS = settings.CUSTOM_ALGORITHMS
    except:
        CUSTOM_ALGORITHMS = {}

    # @added 20260422 - Feature #5665: custom_algorithm - mirage_nirvana
    # stumpy is required by many
    try:
        from tornadoes.tornado_stump import tornado_stump
    except:
        tornado_stump = nullcontext()

    # @added 20260422 - Feature #5563: mirage_vortex - use tornado
    #                   Feature #5198: flux - tornado
    #                   Feature #4734: mirage_vortex
    # Handle all algortihms defined in VORTEX_ALGORITHMS as well
    try:
        VORTEX_ALGORITHMS = settings.VORTEX_ALGORITHMS
    except Exception as outer_err:
        VORTEX_ALGORITHMS = {}
    tornado_algorithms = []
    for key in VORTEX_ALGORITHMS.keys():
        if key != 'default':
            tornado_algorithms.append(key)
    for key in CUSTOM_ALGORITHMS.keys():
        if key != 'default':
            tornado_algorithms.append(key)
    tornado_mirages_nirvana_matrixprofile = nullcontext()
    tornado_mirage_nirvana = nullcontext()
    tornado_laoccfdlpnc = nullcontext()
    # @added 20260613 - Feature #5751: custom_algorithm - skyline_fast
    tornado_skyline_fast = nullcontext()

    # @modified 20260422 - Feature #5563: mirage_vortex - use tornado
    #                      Feature #5198: flux - tornado
    #                      Feature #4734: mirage_vortex
    # Handle all algortihms defined in VORTEX_ALGORITHMS as well
    #for c_algo in CUSTOM_ALGORITHMS.keys():
    for c_algo in tornado_algorithms:

        # @modified 20260422 - Feature #5665: custom_algorithm - mirage_nirvana
        # stumpy is required by many
        #if c_algo == 'skyline_matrixprofile':
        #    from tornadoes.tornado_stump import tornado_stump
        #else:
        #    tornado_stump = nullcontext()

        if c_algo == 'mirages_nirvana_matrixprofile':
            try:
                from tornadoes.tornado_mirages_nirvana_matrixprofile import tornado_mirages_nirvana_matrixprofile
            except:
                tornado_mirages_nirvana_matrixprofile = nullcontext()
        #else:
        #    tornado_mirages_nirvana_matrixprofile = nullcontext()
        if 'laoccfdlpnc' in c_algo:
            try:
                # @added 20241116 - Feature #5553: custom_algorithm - laoccfdlpnc
                from tornadoes.tornado_laoccfdlpnc import tornado_laoccfdlpnc
            except:
                tornado_laoccfdlpnc = nullcontext()
        if c_algo == 'mirage_nirvana':
            # @added 20251108 - Feature #5665: custom_algorithm - mirage_nirvana
            #                   Feature #4994: custom_algorithm - mirages
            try:
                from tornadoes.tornado_mirage_nirvana import tornado_mirage_nirvana
            except:
                tornado_mirage_nirvana = nullcontext()
        #else:
        #    tornado_mirage_nirvana = nullcontext()
        # @added 20260613 - Feature #5751: custom_algorithm - skyline_fast
        if c_algo == 'skyline_fast':
            try:
                from tornadoes.tornado_skyline_fast import tornado_skyline_fast
            except:
                tornado_skyline_fast = nullcontext()

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
                logger.info('warning :: tornado :: no POST data recieved')
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
            run_mirages_nirvana_matrixprofile = False

            # @added 20241116 - Feature #5553: custom_algorithm - laoccfdlpnc
            run_laoccfdlpnc = False

            # @added 20251108 - Feature #5665: custom_algorithm - mirage_nirvana
            #                   Feature #4994: custom_algorithm - mirages
            run_mirage_nirvana = False

            # @added 20260613 - Feature #5751: custom_algorithm - skyline_fast
            run_skyline_fast = False

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

                if postData['algorithm'] == 'mirages_nirvana_matrixprofile':

                    if isinstance(tornado_mirages_nirvana_matrixprofile, nullcontext):
                        logger.error('error :: mirages_nirvana_matrixprofile is not available')
                        body = {"code": 400, "message": 'mirages_nirvana_matrixprofile is not available'}
                        resp.text = json.dumps(body)
                        resp.status = falcon.HTTP_400
                        return

                    try:
                        algorithm_modules_loaded = [i for i in list(sys.modules.keys()) if i in custom_algorithm_modules]
                        logger.debug('debug :: tornado :: %s :: algorithm_modules_loaded (before): %s' % (
                            postData['algorithm'], str(algorithm_modules_loaded)))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: tornado :: determine and log algorithm_modules_loaded, err: %s' % (
                            err))
                    run_mirages_nirvana_matrixprofile = True
                    response_dict = {
                        'anomalous': None,
                        'anomalyScore': None,
                        'results': None,
                    }
                    try:
                        response_dict = tornado_mirages_nirvana_matrixprofile(postData)
                        logger.info('tornado :: request_id: %s, tornado_mirages_nirvana_matrixprofile run' % (
                            request_id))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: tornado :: tornado_mirages_nirvana_matrixprofile failed, err: %s' % (
                            err))
                        resp.status = falcon.HTTP_500
                        return
                    algorithm_modules_loaded = [i for i in list(sys.modules.keys()) if i in custom_algorithm_modules]
                    logger.debug('debug :: tornado :: %s :: algorithm_modules_loaded (after): %s' % (
                        postData['algorithm'], str(algorithm_modules_loaded)))

                if postData['algorithm'] == 'laoccfdlpnc':
                    custom_algorithm_modules = ['custom_algorithms.laoccfdlpnc']
                    try:
                        algorithm_modules_loaded = [i for i in list(sys.modules.keys()) if i in custom_algorithm_modules]
                        logger.debug('debug :: tornado :: %s :: algorithm_modules_loaded (before): %s' % (
                            postData['algorithm'], str(algorithm_modules_loaded)))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: tornado :: determine and log algorithm_modules_loaded, err: %s' % (
                            err))
                    run_laoccfdlpnc = True
                    response_dict = {
                        'anomalous': None,
                        'anomalyScore': None,
                        'results': {},
                    }
                    try:
                        response_dict = tornado_laoccfdlpnc(postData)
                        logger.info('tornado :: request_id: %s, tornado_laoccfdlpnc run' % (
                            request_id))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: tornado :: tornado_laoccfdlpnc failed, err: %s' % (
                            err))
                        resp.status = falcon.HTTP_500
                        return
                    algorithm_modules_loaded = [i for i in list(sys.modules.keys()) if i in custom_algorithm_modules]
                    logger.debug('debug :: tornado :: %s :: algorithm_modules_loaded (after): %s' % (
                        postData['algorithm'], str(algorithm_modules_loaded)))

                # @added 20251108 - Feature #5665: custom_algorithm - mirage_nirvana
                #                   Feature #4994: custom_algorithm - mirages
                if postData['algorithm'] == 'mirage_nirvana':

                    if isinstance(tornado_mirage_nirvana, nullcontext):
                        logger.error('error :: mirage_nirvana is not available')
                        body = {"code": 400, "message": 'mirage_nirvana is not available'}
                        resp.text = json.dumps(body)
                        resp.status = falcon.HTTP_400
                        return

                    try:
                        algorithm_modules_loaded = [i for i in list(sys.modules.keys()) if i in custom_algorithm_modules]
                        logger.debug('debug :: tornado :: %s :: algorithm_modules_loaded (before): %s' % (
                            postData['algorithm'], str(algorithm_modules_loaded)))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: tornado :: determine and log algorithm_modules_loaded, err: %s' % (
                            err))
                    run_mirage_nirvana = True
                    response_dict = {
                        'anomalous': None,
                        'anomalyScore': None,
                        'results': None,
                    }
                    try:
                        response_dict = tornado_mirage_nirvana(postData)
                        logger.info('tornado :: request_id: %s, tornado_mirage_nirvana run' % (
                            request_id))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: tornado :: tornado_mirage_nirvana failed, err: %s' % (
                            err))
                        resp.status = falcon.HTTP_500
                        return
                    algorithm_modules_loaded = [i for i in list(sys.modules.keys()) if i in custom_algorithm_modules]
                    logger.debug('debug :: tornado :: %s :: algorithm_modules_loaded (after): %s' % (
                        postData['algorithm'], str(algorithm_modules_loaded)))

                # @added 20260613 - Feature #5751: custom_algorithm - skyline_fast
                if postData['algorithm'] == 'skyline_fast':
                    if isinstance(tornado_skyline_fast, nullcontext):
                        logger.error('error :: skyline_fast is not available')
                        body = {"code": 400, "message": 'skyline_fast is not available'}
                        resp.text = json.dumps(body)
                        resp.status = falcon.HTTP_400
                        return
                    try:
                        algorithm_modules_loaded = [i for i in list(sys.modules.keys()) if i in custom_algorithm_modules]
                        logger.debug('debug :: tornado :: %s :: algorithm_modules_loaded (before): %s' % (
                            postData['algorithm'], str(algorithm_modules_loaded)))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: tornado :: determine and log algorithm_modules_loaded, err: %s' % (
                            err))
                    run_skyline_fast = True
                    response_dict = {
                        'anomalous': None,
                        'anomalyScore': None,
                        'results': None,
                    }
                    try:
                        response_dict = tornado_skyline_fast(postData)
                        logger.info('tornado :: request_id: %s, tornado_skyline_fast run' % (
                            request_id))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: tornado :: tornado_skyline_fast failed, err: %s' % (
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

            if run_mirages_nirvana_matrixprofile:
                try:
                    body = {
                        "status": {"code": 200, "request_id": request_id, "request_time": request_time},
                        "data": {
                            "anomalous": response_dict['anomalous'],
                            "anomalyScore": response_dict['anomalyScore'],
                            "results": response_dict['results'],
                        }
                    }
                    resp.text = json.dumps(body)
                    logger.info('tornado :: mirages_nirvana_matrixprofile - anomalous: %s, base_name: %s' % (
                        str(response_dict['anomalous']), postData['base_name']))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: tornado :: failed to create response from mirages_nirvana_matrixprofile, err: %s' % (
                        err))
                resp.status = falcon.HTTP_200
                return

            # @added 20241116 - Feature #5553: custom_algorithm - laoccfdlpnc
            if run_laoccfdlpnc:
                try:
                    body = {
                        "status": {"code": 200, "request_id": request_id, "request_time": request_time},
                        "data": response_dict,
                    }
                    resp.text = json.dumps(body)
                    logger.info('tornado :: laoccfdlpnc - anomalous: %s, base_name: %s' % (
                        str(response_dict['anomalous']), postData['base_name']))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: tornado :: failed to create response from laoccfdlpnc, err: %s' % (
                        err))
                resp.status = falcon.HTTP_200
                return

            # @added 20251108 - Feature #5665: custom_algorithm - mirage_nirvana
            #                   Feature #4994: custom_algorithm - mirages
            if run_mirage_nirvana:
                try:
                    body = {
                        "status": {"code": 200, "request_id": request_id, "request_time": request_time},
                        "data": {
                            "anomalous": response_dict['anomalous'],
                            "anomalyScore": response_dict['anomalyScore'],
                            "results": response_dict['results'],
                        }
                    }
                    resp.text = json.dumps(body)
                    logger.info('tornado :: mirage_nirvana - anomalous: %s, base_name: %s' % (
                        str(response_dict['anomalous']), postData['base_name']))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: tornado :: failed to create response from mirage_nirvana, err: %s' % (
                        err))
                resp.status = falcon.HTTP_200
                return

            # @added 20260613 - Feature #5751: custom_algorithm - skyline_fast
            if run_skyline_fast:
                try:
                    body = {
                        "status": {"code": 200, "request_id": request_id, "request_time": request_time},
                        "data": {
                            "anomalous": response_dict['anomalous'],
                            "anomalyScore": response_dict['anomalyScore'],
                            "results": response_dict['results'],
                        }
                    }
                    resp.text = json.dumps(body)
                    logger.info('tornado :: skyline_fast - anomalous: %s, base_name: %s' % (
                        str(response_dict['anomalous']), postData['base_name']))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: tornado :: failed to create response from skyline_fast, err: %s' % (
                        err))
                resp.status = falcon.HTTP_200
                return

            return
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: tornado :: tornado failed, returning 500 - %s' % err)
            resp.status = falcon.HTTP_500
