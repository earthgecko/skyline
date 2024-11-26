"""
tornado_laoccfdlpnc.py
"""
import os
from os import getpid

import sys
from timeit import default_timer as timer
import traceback

import numpy as np

from custom_algorithms.laoccfdlpnc import laoccfdlpnc
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name

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


# @added 20241116 - Feature #5553: custom_algorithm - laoccfdlpnc
def tornado_laoccfdlpnc(postData):
    """
    The tornado_laoccfdlpnc function runs the algorithm returns the normal tuple
    of (anomalous, anomalyScore, all_results)

    :param current_skyline_app: the Skyline app executing the algorithm.  This
        will be passed to the algorithm by Skyline.  This is **required** for
        error handling and logging.  You do not have to worry about handling the
        argument in the scope of the custom algorithm itself,  but the algorithm
        must accept it as the first agrument.
    :param parent_pid: the parent pid which is executing the algorithm, this is
        **required** for error handling and logging.  You do not have to worry
        about handling this argument in the scope of algorithm, but the
        algorithm must accept it as the second argument.
    :param timeseries: the time series as a list e.g. ``[[1578916800.0, 29.0],
        [1578920400.0, 55.0], ... [1580353200.0, 55.0]]``
    :param algorithm_parameters: a dictionary of any required parameters for the
        custom_algorithm and algorithm itself.  For the matrixprofile custom
        algorithm the following parameters are required, example:
        ``algorithm_parameters={
            'check_details': {<empty_dict|check_details dict>},
            'full_duration': full_duration,
            'windows': int
        }``
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: True, False or Non
    :rtype: boolean

    """
    func_name = 'flux.tornadoes.tornado_laoccfdlpnc'
    custom_algorithm = 'laoccfdlpnc'
    anomalous = None
    anomalyScore = 0.0
    results = {}
    anomalies = {}
    response_dict = {
        'anomalous': anomalous,
        'anomalyScore': anomalyScore,
        'results': results,
    }
    
    requesting_skyline_app = 'mirage'
    postItems = {}
    for key in list(postData.keys()):
        if key == 'timeseries':
            continue
        postItems[key] = postData[key]
    logger.info('%s :: postData (excl. timeseries): %s' % (
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
        except Exception as err:
            debug_logging = False

    algorithm_parameters['metric'] = base_name
    labelled_metric_name = None
    if '_tenant_id=' in base_name:
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
            logger.debug('debug :: %s :: calling laoccfdlpnc with algorithm_parameters: %s' % (
                func_name, str(algorithm_parameters)))
        anomalous, anomalyScore, results = laoccfdlpnc(skyline_app, getpid(), timeseries, algorithm_parameters)
        if debug_logging:
            logger.debug('debug :: %s :: laoccfdlpnc returned anomalous: %s' % (
                func_name, str(anomalous)))
    except Exception as err:
        traceback_msg = traceback.format_exc()
        if debug_logging:
            logger.error(traceback_msg)
            logger.error('error :: debug_logging :: %s :: failed to run laoccfdlpnc, err: %s' % (
                func_name, err))
    end_compute = timer()
    compute_runtime = end_compute - start_compute
    logger.info('%s :: laoccfdlpnc took %.6f seconds run with result anomalous: %s' % (
        func_name, compute_runtime, str(anomalous)))

    response_dict = {
        'anomalous': anomalous,
        'anomalyScore': anomalyScore,
        'results': results,
    }

    return response_dict