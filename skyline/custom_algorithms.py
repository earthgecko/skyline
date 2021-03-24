from __future__ import division
import logging
import traceback
import os.path
from os import getpid
import sys
import importlib.util
import time
import timeout_decorator
from timeit import default_timer as timer
from settings import (
    SKYLINE_TMP_DIR,
)


def get_function_name():
    """
    This is a utility function is used to determine what algorithm is reporting
    an algorithm error when the record_algorithm_error is used.
    """
    return traceback.extract_stack(None, 2)[0][2]


def record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_format_exc_string):
    """
    This utility function is used to facilitate the traceback from any algorithm
    errors.  The algorithm functions themselves we want to run super fast and
    without fail in terms of stopping the function returning and not reporting
    anything to the log, so the pythonic except is used to "sample" any
    algorithm errors to a tmp file and report once per run rather than spewing
    tons of errors into the log.

    .. note::
        algorithm errors tmp file clean up
            the algorithm error tmp files are handled and cleaned up in
            :class:`Analyzer` after all the spawned processes are completed.

    :param current_skyline_app: the Skyline app
    :param algorithm_name: the algoritm function name
    :param traceback_format_exc_string: the traceback_format_exc string
    :param parent_pid: the pid of the parent process that will be used to in
        error file naming
    :type current_skyline_app: str
    :type algorithm_name: str
    :type traceback_format_exc_string: str
    :type parent_pid: int
    :return:
        - ``True`` the error string was written to the algorithm_error_file
        - ``False`` the error string was not written to the algorithm_error_file

    :rtype:
        - boolean

    """

    # current_process_pid = getpid()
    algorithm_error_file = '%s/%s.%s.%s.algorithm.error' % (
        SKYLINE_TMP_DIR, current_skyline_app, str(parent_pid), algorithm_name)
    try:
        with open(algorithm_error_file, 'w') as f:
            f.write(str(traceback_format_exc_string))
        return True
    except:
        return False


# @added 20200603 - Feature #3566: custom_algorithms
def run_custom_algorithm_on_timeseries(current_skyline_app, parent_pid, base_name, timeseries, custom_algorithm, custom_algorithm_dict, debug_custom_algortihms):
    """
    Return a dictionary of custom algoritms to run on a metric determined from
    the :mod:`settings.CUSTOM_ALGORITHMS` dictionary.
    """

    func_name = 'run_custom_algorithm_on_timeseries'
    anomalous = None
    anomalyScore = None
    current_logger = None
    myPid = getpid()

    def get_log(current_skyline_app):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        return current_logger

    def str_to_class(classname, current_logger):
        try:
            return getattr(sys.modules[classname], classname)
        except:
            if not current_logger:
                current_logger = get_log(current_skyline_app)
            current_logger.error(traceback.format_exc())
            current_logger.error(
                'error :: %s :: with pid %s failed load custom algorithm module - %s' % (
                    func_name, str(myPid), str(classname)))
            return (None, None)

    debug_logging = False
    try:
        debug_logging = custom_algorithm_dict['debug_logging']
    except:
        debug_logging = False
    if debug_logging or debug_custom_algortihms:
        current_logger = get_log(current_skyline_app)
        current_logger.debug(
            'debug :: %s :: pid %s, running custom algorithm - %s with debug_logging for %s' % (
                func_name, str(myPid), custom_algorithm, base_name))
    try:
        algorithm_source = custom_algorithm_dict['algorithm_source']
        if debug_logging or debug_custom_algortihms:
            current_logger.debug(
                'debug :: %s :: pid %s, %s algorithm_source - %s' % (
                    func_name, str(myPid), custom_algorithm, str(algorithm_source)))
    except:
        if not current_logger:
            current_logger = get_log(current_skyline_app)
        current_logger.error(traceback.format_exc())
        current_logger.error(
            'error :: %s :: pid %s, failed determine algorithm_source for custom algorithm - %s' % (
                func_name, str(myPid), custom_algorithm))
        return (None, None)

    if algorithm_source:
        if not os.path.isfile(str(algorithm_source)):
            if not current_logger:
                current_logger = get_log(current_skyline_app)
            current_logger.error(
                'error :: %s :: pid %s, failed to find custom algorithm - %s - algorithm_source file - %s' % (
                    func_name, str(myPid), custom_algorithm,
                    str(algorithm_source)))
            return (None, None)

    try:
        if debug_logging or debug_custom_algortihms:
            current_logger.debug(
                'debug :: %s :: pid %s, importing %s' % (func_name, str(myPid), custom_algorithm))
        sys.path.append(os.path.join(os.path.dirname(os.path.realpath(algorithm_source)), os.pardir))
        sys.path.insert(0, os.path.dirname(algorithm_source))
        module_name = custom_algorithm
        spec = importlib.util.spec_from_file_location(module_name, algorithm_source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        sys.modules[module_name] = module
    except:
        if not current_logger:
            current_logger = get_log(current_skyline_app)
        current_logger.error(traceback.format_exc())
        current_logger.error(
            'error :: %s :: pid %s, failed to load custom algorithm - %s - from algorithm_source file - %s' % (
                func_name, str(myPid), custom_algorithm, str(algorithm_source)))
        return (None, None)

    use_custom_algorithm = None
    try:
        use_custom_algorithm = str_to_class(custom_algorithm, current_logger)
    except:
        if not current_logger:
            current_logger = get_log(current_skyline_app)
        current_logger.error(traceback.format_exc())
        current_logger.error(
            'error :: %s :: pid %s, failed to interpolate module name for custom algorithm - %s' % (
                func_name, str(myPid), custom_algorithm))
        return (None, None)

    if not use_custom_algorithm:
        if not current_logger:
            current_logger = get_log(current_skyline_app)
        current_logger.error(traceback.format_exc())
        current_logger.error(
            'error :: %s :: pid %s, failed to get module name for custom algorithm - %s' % (
                func_name, str(myPid), custom_algorithm))
        return (None, None)

    algorithm_parameters = {}
    try:
        algorithm_parameters = custom_algorithm_dict['algorithm_parameters']
        if debug_logging or debug_custom_algortihms:
            current_logger.debug(
                'debug :: %s :: pid %s, %s algorithm_parameters - %s' % (
                    func_name, str(myPid), custom_algorithm,
                    str(algorithm_parameters)))
    except:
        algorithm_parameters = {}

    # @added 20210226 - Feature #3970: custom_algorithm - adtk_level_shift
    # Add the metric name to the algorithm_parameters
    algorithm_parameters['base_name'] = base_name

    max_execution_time = 0.05
    try:
        max_execution_time = float(custom_algorithm_dict['max_execution_time'])
        if debug_logging or debug_custom_algortihms:
            current_logger.debug(
                'debug :: %s :: pid %s, %s max_execution_time - %s' % (
                    func_name, str(myPid), custom_algorithm,
                    str(max_execution_time)))
    except:
        max_execution_time = 0.05
    if debug_logging or debug_custom_algortihms:
        current_logger.debug(
            'debug :: %s :: pid %s, %s on %s with max_execution_time - %s, algorithm_parameters - %s' % (
                func_name, str(myPid), custom_algorithm, base_name,
                str(max_execution_time), str(algorithm_parameters)))

    @timeout_decorator.timeout(max_execution_time, timeout_exception=StopIteration, use_signals=False)
    def run_custom_algorithm_with_timeout(current_logger, custom_algorithm, use_custom_algorithm, current_skyline_app, parent_pid, timeseries, algorithm_parameters, debug_custom_algortihms, max_execution_time):
        funcPid = getpid()
        if debug_custom_algortihms:
            start_debug_timer = timer()
        anomalous = None
        anomalyScore = None

        # @aded 20210308 - Feature #3978: luminosity - classify_metrics
        #                  Feature #3642: Anomaly type classification
        # Allow for anomalies to be returned
        anomalies = []
        return_anomalies = False
        try:
            return_anomalies = algorithm_parameters['return_anomalies']
        except:
            return_anomalies = False

        try:
            if not return_anomalies:
                anomalous, anomalyScore = use_custom_algorithm(current_skyline_app, parent_pid, timeseries, algorithm_parameters)
            else:
                anomalous, anomalyScore, anomalies = use_custom_algorithm(current_skyline_app, parent_pid, timeseries, algorithm_parameters)

            if debug_custom_algortihms:
                end_debug_timer = timer()
                if not current_logger:
                    current_logger = get_log(current_skyline_app)
                current_logger.debug(
                    'debug :: %s :: pid %s, analysed %s (with run_custom_algorithm_with_timeout with max_execution_time set to %s, in %.6f seconds) - anomalous - %s, anomalyScore - %s' % (
                        func_name, str(funcPid), custom_algorithm,
                        str(max_execution_time),
                        (end_debug_timer - start_debug_timer), str(anomalous),
                        str(anomalyScore)))
        except SystemExit:
            if debug_custom_algortihms:
                end_debug_timer = timer()
                if not current_logger:
                    current_logger = get_log(current_skyline_app)
                current_logger.debug(
                    'debug :: %s :: pid %s, %s was timed out by max_execution_time of %s, after running for %.6f seconds with returning a result' % (
                        func_name, str(funcPid), custom_algorithm,
                        str(max_execution_time),
                        (end_debug_timer - start_debug_timer)))
            if not return_anomalies:
                return (None, None)
            else:
                return (None, None, [])
        except:
            if not current_logger:
                current_logger = get_log(current_skyline_app)
            current_logger.error(traceback.format_exc())
            end_debug_timer = timer()
            current_logger.error(
                'error :: run_custom_algorithm_with_timeout :: pid %s, failed to evaluate time series with custom algorithm - %s' % (
                    str(funcPid), custom_algorithm))
            if not return_anomalies:
                return (None, None)
            else:
                return (None, None, [])

        # return (anomalous, anomalyScore)
        if not return_anomalies:
            return (anomalous, anomalyScore)
        else:
            return (anomalous, anomalyScore, anomalies)

    if debug_logging or debug_custom_algortihms:
        current_logger.debug(
            'debug :: %s :: pid %s, running %s with run_custom_algorithm_with_timeout set to %s' % (
                func_name, str(myPid), custom_algorithm, str(max_execution_time)))
        debug_custom_algortihms = True

    return_anomalies = False
    try:
        return_anomalies = algorithm_parameters['return_anomalies']
    except:
        return_anomalies = False

    try:
        # anomalous, anomalyScore = use_custom_algorithm(current_skyline_app, timeseries, algorithm_parameters)
        if not return_anomalies:
            anomalous, anomalyScore = run_custom_algorithm_with_timeout(current_logger, custom_algorithm, use_custom_algorithm, current_skyline_app, parent_pid, timeseries, algorithm_parameters, debug_custom_algortihms, max_execution_time)
        else:
            anomalous, anomalyScore, anomalies = run_custom_algorithm_with_timeout(current_logger, custom_algorithm, use_custom_algorithm, current_skyline_app, parent_pid, timeseries, algorithm_parameters, debug_custom_algortihms, max_execution_time)
    except StopIteration:
        if not current_logger:
            current_logger = get_log(current_skyline_app)
        # @modified 20201007 - Branch #3068: SNAB
        #                      Task #3744: POC matrixprofile
        #                      Feature #3566: custom_algorithms
        # This should not be recorded as an error, it is simply a warning
        # current_logger.error(
        #     'error :: %s :: pid %s, terminated evaluation of time series after max_execution_time %s was reached (timeout) with custom algorithm - %s - loaded from algorithm_source file - %s' % (
        current_logger.warn(
            'warning :: %s :: pid %s, terminated evaluation of time series after max_execution_time %s was reached (timeout) with custom algorithm - %s - loaded from algorithm_source file - %s' % (
                func_name, str(myPid), str(max_execution_time),
                custom_algorithm, str(algorithm_source)))
        if not return_anomalies:
            return (None, None)
        else:
            return (None, None, [])

        if not current_logger:
            current_logger = get_log(current_skyline_app)
        current_logger.error(traceback.format_exc())
        current_logger.error(
            'error :: %s :: pid %s, failed to evaluate time series with custom algorithm - %s - loaded from algorithm_source file - %s' % (
                func_name, str(myPid), custom_algorithm, str(algorithm_source)))
        if not return_anomalies:
            return (None, None)
        else:
            return (None, None, [])

    if debug_logging or debug_custom_algortihms:
        current_logger.debug(
            'debug :: %s :: pid %s, %s with results (%s, %s)' % (
                func_name, str(myPid), custom_algorithm, str(anomalous),
                str(anomalyScore)))

    if not return_anomalies:
        return (anomalous, anomalyScore)
    else:
        return (anomalous, anomalyScore, anomalies)
