"""
tornado_stump.py
"""
import os
import sys
from timeit import default_timer as timer
import traceback

import numpy as np

from custom_algorithm_sources import stumpy

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


def tornado_stump(postData):
    """
    The tornado_stump function runs the jit cached stump function and returns
    a profile.

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
    func_name = 'flux.tornadoes.tornado_stump'
    profile = []
    debug_logging = True

    if debug_logging:
        custom_algorithm_modules = ['stumpy','stump','stumpy.stamp','custom_algorithm_sources.stumpy','custom_algorithm_sources.stumpy.stump']
        algorithm_modules_loaded = [i for i in list(sys.modules.keys()) if i in custom_algorithm_modules]
        logger.debug('debug :: %s :: algorithm_modules_loaded (before): %s' % (
            func_name, str(algorithm_modules_loaded)))

        if len(algorithm_modules_loaded) > 0:
            logger.debug('debug :: %s :: stump is loaded in sys.modules, will not be loaded' % (
                func_name))
        else:
            logger.debug('debug :: %s :: stump is not loaded in sys.modules, will be loaded' % (
                func_name))

    dataset = []
    try:
        dataset = postData['ts']
    except Exception as err:
        logger.error('error :: %s :: failed to get dataset from postData[\'algorithm\'][\'ts\'], err: %s' % (
            func_name, err))
        return profile

    windows = None
    try:
        windows = postData['windows']
    except Exception as err:
        logger.error('error :: %s :: failed to get window from postData[\'algorithm\'][\'windows\'], err: %s' % (
            func_name, err))
        return profile

    ts = np.array(dataset)

    start_compute = timer()
    try:
        if debug_logging:
            logger.debug('debug :: %s :: calculating profile with stump' % (
                func_name))
        profile = stumpy.stump(ts, m=windows)
    except Exception as err:
        traceback_msg = traceback.format_exc()
        if debug_logging:
            logger.error(traceback_msg)
            logger.error('error :: debug_logging :: %s :: failed to run stump on ts - %s' % (
                func_name, err))
    end_compute = timer()
    compute_runtime = end_compute - start_compute
    if debug_logging:
        logger.info('%s :: stumpy.stump took %.6f seconds to generate a profile of length: %s' % (
            func_name, compute_runtime, str(len(profile))))

    if debug_logging:
        if isinstance(profile, np.ndarray):
            algorithm_modules_loaded = [i for i in list(sys.modules.keys()) if i in custom_algorithm_modules]
            logger.debug('debug :: %s :: algorithm_modules_loaded (after): %s' % (
                func_name, str(algorithm_modules_loaded)))

    # Coerce to json friendly
    if isinstance(profile, np.ndarray):
        profile_list = []
        # start_list = timer()
        for item in profile:
            # Coerce values from numpy.int64, etc to float, int or None
            profile_item = []
            for i in item:
                if isinstance(i, int):
                    profile_item.append(int(i))
                elif isinstance(i, float):
                    profile_item.append(float(i))
                elif np.isnan(i):
                    profile_item.append(None)
                else:
                    profile_item.append(float(i))
            profile_list.append(profile_item)
        profile = list(profile_list)

    return profile