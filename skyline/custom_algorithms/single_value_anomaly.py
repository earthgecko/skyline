"""
single_value_anomaly.py
"""
# REQUIRED Skyline imports.  All custom algorithms MUST have the following two
# imports.  These are required for exception handling and to record algorithm
# errors regardless of debug_logging setting for the custom_algorithm
import logging
import traceback
from time import time
from custom_algorithms import record_algorithm_error

# Import ALL modules that the custom algorithm requires.  Remember that if a
# requirement is not one that is provided by the Skyline requirements.txt you
# must ensure it is installed in the Skyline virtualenv
import numpy as np

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20230717 - Feature #4994: custom_algorithm - mirages
def single_value_anomaly(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

    """
    A time series in anomalous if all the values are equal apart from the last
    data point.

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
    :param algorithm_parameters: only an empty dictionary is required.
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: anomalous, anomalyScore, results
    :rtype: tuple(bool, float, dict)

    """

    # You MUST define the algorithm_name
    algorithm_name = 'single_value_anomaly'

    # If you wanted to log, you can but this should only be done during
    # testing and development
    def get_log(current_skyline_app):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        return current_logger

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = False
    anomalyScore = 0.0
    anomalies = {}
    anomalyScore_list = []
    results = {
        'anomalous': anomalous,
        'anomalies': anomalies,
        'anomalyScore_list': anomalyScore_list,
    }

    return_results = False
    try:
        return_results = algorithm_parameters['return_results']
    except:
        return_results = False

    # Use the algorithm_parameters to determine whether to log
    debug_logging = False
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
            if return_results:
                return (anomalous, anomalyScore, results)
            return (None, None)

    print_debug = False
    try:
        print_debug = algorithm_parameters['print_debug']
    except:
        print_debug = False

    try:
        start = time()
        non_none_timeseries = [v for t, v in timeseries if v is not None]
        unique_array = np.unique([v for v in non_none_timeseries], return_counts=True)
        single_value = False
        if len(unique_array[0]) == 2:
            if print_debug:
                print('unique_array:', unique_array)
            values = unique_array[0].tolist()
            counts = unique_array[1].tolist()
            for index, value in enumerate(values):
                if counts[index] == 1:
                    if print_debug:
                        print('counts[index]:', counts[index], 'value:', value, 'non_none_timeseries[-1]:', non_none_timeseries[-1])
                    if value == non_none_timeseries[-1]:
                        single_value = True
                        if print_debug:
                            print('value == non_none_timeseries[-1]', single_value)
                    else:
                        if print_debug:
                            print('value != non_none_timeseries[-1]', single_value)

            # if timeseries[-1][1] in unique_array[0].tolist():
            if single_value:
                anomalous = True
                anomalyScore = 1.0
                last_timestamp = int(timeseries[-1][0])
                anomaly_index = len(timeseries) - 1
                anomalyScore_list = [0] * anomaly_index
                anomalyScore_list.append(1)
                results['anomalous'] = anomalous
                results['anomalyScore'] = anomalyScore
                results['total_anomalies'] = 1
                results['anomalies_in_window'] = 1
                results['success'] = True
                results['unreliable'] = False
                results['anomalies'] = {last_timestamp: {'value': timeseries[-1][1], 'index': anomaly_index, 'score': 1}}
                results['anomalyScore_list'] = anomalyScore_list
                results['scores'] = anomalyScore_list
                results['analysis_runtime'] = time() - start
        else:
            anomalous = False
            anomalyScore = 0.0
    except StopIteration:
        # This except pattern MUST be used in ALL custom algortihms to
        # facilitate the traceback from any errors.  The algorithm we want to
        # run super fast and without spamming the log with lots of errors.
        # But we do not want the function returning and not reporting
        # anything to the log, so the pythonic except is used to "sample" any
        # algorithm errors to a tmp file and report once per run rather than
        # spewing tons of errors into the log e.g. analyzer.log
        if return_results:
            return (anomalous, anomalyScore, results)
        return (None, None)
    except Exception as err:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if print_debug:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error - on single_value_anomaly - %s' % err)
            current_logger.debug(traceback.format_exc())

    if return_results:
        return (anomalous, anomalyScore, results)
    return (anomalous, anomalyScore)
