"""
sigma.py
"""
# REQUIRED Skyline imports.  All custom algorithms MUST have the following two
# imports.  These are required for exception handling and to record algorithm
# errors regardless of debug_logging setting for the custom_algorithm
import logging
import traceback
from custom_algorithms import record_algorithm_error

# Import ALL modules that the custom algorithm requires.  Remember that if a
# requirement is not one that is provided by the Skyline requirements.txt you
# must ensure it is installed in the Skyline virtualenv
from custom_algorithm_sources.sigma.sigma import run_sigma_algorithms

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20221127 - Feature #4736: custom_algorithms - sigma
#                   Feature #4734: mirage_vortex
def sigma(current_skyline_app, parent_pid, timeseries, algorithm_parameters):
    """
    This is an implementation of the original Skyline 3sigma algorithms as a
    single custom algorithm.  It has been extended to allow for the sigma value
    to be passed as a parameter.

    :param current_skyline_app: the Skyline app executing the algorithm.  This
        will be passed to the algorithm by Skyline.  This is **required** for
        error handling and logging.  You do not have to worry about handling the
        argument in the scope of the custom algorithm itself,  but the algorithm
        must accept it as the first agrument.
    :param parent_pid: the parent pid which is executing the algorithm, this is
        **required** for error handling and logging.  You do not have to worry
        about handling this argument in the scope of algorithm, but the
        algorithm must accept it as the second argument.
    :param timeseries: the time series as a list e.g. ``[[1667608854, 1269121024.0],
        [1667609454, 1269174272.0], [1667610054, 1269174272.0]]``
    :param algorithm_parameters: a dictionary of any required parameters for the
        custom_algorithm and algorithm itself.  For the anomalous_daily_peak
        custom algorithm no specific algorithm_parameters are required apart
        from an empty dict, example: ``algorithm_parameters={}``
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: anomalous, anomalyScore
    :rtype: tuple(boolean, float)

    """

    # You MUST define the algorithm_name
    algorithm_name = 'sigma'

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = None
    anomalies = {}

    current_logger = None

    # If you wanted to log, you can but this should only be done during
    # testing and development
    def get_log(current_skyline_app):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        return current_logger

    # Use the algorithm_parameters to determine the sample_period
    debug_logging = None
    try:
        debug_logging = algorithm_parameters['debug_logging']
    except:
        debug_logging = False
    if debug_logging:
        try:
            current_logger = get_log(current_skyline_app)
            current_logger.debug('debug :: %s :: debug_logging enabled with algorithm_parameters - %s' % (
                algorithm_name, str(algorithm_parameters)))
        except:
            # This except pattern MUST be used in ALL custom algortihms to
            # facilitate the traceback from any errors.  The algorithm we want to
            # run super fast and without spamming the log with lots of errors.
            # But we do not want the function returning and not reporting
            # anything to the log, so the pythonic except is used to "sample" any
            # algorithm errors to a tmp file and report once per run rather than
            # spewing tons of errors into the log e.g. analyzer.log
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
            # Return None and None as the algorithm could not determine True or False
            return (anomalous, anomalyScore, anomalies)

    # Use the algorithm_parameters to determine variables
    debug_print = None
    try:
        debug_print = algorithm_parameters['debug_print']
    except:
        debug_print = False
    sigma_value = 3
    try:
        sigma_value = int(algorithm_parameters['sigma'])
    except:
        sigma_value = 3
    consensus = 6
    try:
        consensus = algorithm_parameters['consensus']
    except:
        consensus = 6
    anomaly_window = 1
    try:
        anomaly_window = int(algorithm_parameters['anomaly_window'])
    except:
        anomaly_window = 1
    return_anomalies = False
    try:
        return_anomalies = algorithm_parameters['return_anomalies']
    except:
        return_anomalies = False

    try:
        anomalous, anomalies = run_sigma_algorithms(current_skyline_app, timeseries, sigma_value, consensus, anomaly_window)
        if anomalous:
            anomalyScore = 1.0
        else:
            anomalyScore = 0.0
    except StopIteration:
        # This except pattern MUST be used in ALL custom algortihms to
        # facilitate the traceback from any errors.  The algorithm we want to
        # run super fast and without spamming the log with lots of errors.
        # But we do not want the function returning and not reporting
        # anything to the log, so the pythonic except is used to "sample" any
        # algorithm errors to a tmp file and report once per run rather than
        # spewing tons of errors into the log e.g. analyzer.log
        if not return_anomalies:
            return (anomalous, anomalyScore)
        return (anomalous, anomalyScore, anomalies)
    except:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        # Return None and None as the algorithm could not determine True or False
        return (anomalous, anomalyScore, anomalies)

    return (anomalous, anomalyScore, anomalies)
