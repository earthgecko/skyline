"""
tornado_skyline_fast.py
"""
import os
from os import getpid

import sys
import traceback

from contextlib import nullcontext
from timeit import default_timer as timer

import numpy as np

from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
try:
    from custom_algorithms.skyline_fast import skyline_fast
except:
    skyline_fast = nullcontext()

from logger import set_up_logging
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))


import settings

logger = set_up_logging(None)

this_host = str(os.uname()[1])

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

parent_skyline_app = 'flux'
skyline_app = 'flux'

LOCAL_DEBUG = False

# @added 20260613 - Feature #5751: custom_algorithm - skyline_fast
def tornado_skyline_fast(postData):
    """
    The tornado_skyline_fast function runs the algorithm and returns
    the normal tuple of (anomalous, anomalyScore, results)

    :param current_skyline_app: the Skyline app executing the algorithm.  This
        will be passed to the algorithm by Skyline.  This is **required** for
        error handling and logging.  You do not have to worry about handling the
        argument in the scope of the custom algorithm itself,  but the algorithm
        must accept it as the first agrument.
    :param parent_pid: the parent pid which is executing the algorithm, this is
        **required** for error handling and logging.  You do not have to worry
        about handling this argument in the scope of algorithm, but the
        algorithm must accept it as the second argument.
    :param timeseries: the time series as a list e.g. ``[[1780495200, 1.0], 
        [1780495800, 1.0], ... [1781098800, 4.0], [1781099400, 8.9]]``
    :param algorithm_parameters: a dictionary of required parameters for the
        skyline_fast custom algorithm and algorithm itself.
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: True, False or Non
    :rtype: boolean

    """
    func_name = 'flux.tornadoes.tornado_skyline_fast'
    custom_algorithm = 'skyline_fast'
    anomalous = None
    anomalyScore = 0.0
    results = {}
    response_dict = {
        'anomalous': anomalous,
        'anomalyScore': anomalyScore,
        'results': results,
    }
    if isinstance(skyline_fast, nullcontext):
        logger.info('%s :: %s is not available' % (
            func_name, custom_algorithm))
        response_dict['error'] = 'skyline_fast is not available'
        return response_dict

    requesting_skyline_app = 'mirage'
    postItems = {}
    for key in list(postData.keys()):
        if key == 'timeseries':
            continue
        if 'api_key' in key:
            continue
        postItems[key] = postData[key]
    logger.info('%s :: postData (excl. timeseries and api_keys): %s' % (
        func_name, str(postItems)))
    try:
        requesting_skyline_app = postData['skyline_app']
    except Exception as err:
        logger.error('error :: %s :: failed to get data from postData[\'skyline_app\'], err: %s' % (
            func_name, err))
        requesting_skyline_app = 'mirage'

    base_name = None
    try:
        base_name = postData['base_name']
    except Exception as err:
        logger.error('error :: %s :: failed to get data from postData[\'base_name\'], err: %s' % (
            func_name, err))
        base_name = None

    timeseries = []
    try:
        timeseries = postData['timeseries']
    except Exception as err:
        logger.error('error :: %s :: failed to get data from postData[\'timeseries\'], err: %s' % (
            func_name, err))
        timeseries = []

    algorithm_parameters = {}
    try:
        algorithm_parameters = postData['algorithm_parameters']
    except Exception as err:
        logger.error('error :: %s :: failed to get algorithm_parameters from postData[\'algorithm_parameters\'], err: %s' % (
            func_name, err))
        algorithm_parameters = {}

    debug_logging = False
    try:
        debug_logging = postData['debug_logging']
    except Exception as err:
        try:
            debug_logging = postData['algorithm_parameters']['debug_logging']
        except Exception as err2:
            debug_logging = False

    algorithm_parameters['metric'] = base_name

    labelled_metric_name = None
    # Allow for another Skyline node to send metrics for analysis and pass the
    # labelled_metric_name so it does not have to be looked up
    try:
        labelled_metric_name = algorithm_parameters['labelled_metric_name']
    except:
        labelled_metric_name = None

    if '_tenant_id=' in base_name and not labelled_metric_name:
        metric_id = 0
        try:
            metric_id = get_metric_id_from_base_name(skyline_app, base_name)
        except Exception as err:
            logger.error('error :: get_metric_id_from_base_name failed with base_name: %s - %s' % (str(base_name), err))
        if metric_id:
            labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
    algorithm_parameters['labelled_metric_name'] = labelled_metric_name

    start_compute = timer()
    try:
        if debug_logging:
            logger.debug('debug :: %s :: calling skyline_fast with algorithm_parameters: %s' % (
                func_name, str(algorithm_parameters)))
        anomalous, anomalyScore, results = skyline_fast(skyline_app, getpid(), timeseries, algorithm_parameters)
        if debug_logging:
            logger.debug('debug :: %s :: skyline_fast returned anomalous: %s' % (
                func_name, str(anomalous)))
    except Exception as err:
        traceback_msg = traceback.format_exc()
        if debug_logging:
            logger.error(traceback_msg)
            logger.error('error :: debug_logging :: %s :: failed to run skyline_fast, err: %s' % (
                func_name, err))
    end_compute = timer()
    compute_runtime = end_compute - start_compute
    logger.info('%s :: skyline_fast took %.6f seconds run with result anomalous: %s' % (
        func_name, compute_runtime, str(anomalous)))

    response_dict = {
        'anomalous': anomalous,
        'anomalyScore': anomalyScore,
        'results': results,
    }

    return response_dict