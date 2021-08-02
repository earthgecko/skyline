"""
THIS IS A MORE FEATUREFUL CUSTOM ALGORITHM to provide a skeleton to develop your
own custom algorithms.  The algorithm itself, although viable, is not
recommended for production or general use, it is simply a toy algorithm here to
demonstrate the structure of a more complex custom algorithm that has
``algorithm_parameters`` passed and can also log if enabled.
It is documented via comments #
"""

# REQUIRED Skyline imports.  All custom algorithms MUST have the following two
# imports.  These are required for exception handling and to record algorithm
# errors regardless of debug_logging setting for the custom_algorithm
import traceback
from custom_algorithms import record_algorithm_error

import logging

# Import ALL modules that the custom algorithm requires.  Remember that if a
# requirement is not one that is provided by the Skyline requirements.txt you
# must ensure it is installed in the Skyline virtualenv
import numpy as np

# To test max_execution_time import sleep
# from time import sleep


# Define your simple algorithm.
# The name of the fucntion MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except
def last_same_hours(current_skyline_app, parent_pid, timeseries, algorithm_parameters):
    """
    The last_same_hours algorithm determines the data points for the same hour
    and minute as the current timestamp from the last x days and calculates the
    mean of those values and determines whether the current data point is within
    3 standard deviations of the mean.

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
    :param algorithm_parameters: a dictionary of any parameters and their
        arguments you wish to pass to the algorithm.
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: True, False or Non
    :rtype: boolean

    """

    # You MUST define the algorithm_name
    algorithm_name = 'last_same_hours'

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = None

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
            return (None, None)

    # Use the algorithm_parameters to determine the sample_period
    try:
        sample_period = algorithm_parameters['sample_period']
        if debug_logging:
            current_logger.debug('debug :: %s :: sample_period - %s' % (
                algorithm_name, str(sample_period)))
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
        return (None, None)

    # To test max_execution_time enable a sleep
    # sleep(1)

    # ALWAYS WRAP YOUR ALGORITHM IN try and the BELOW except
    try:
        sorted_timeseries = sorted(timeseries, key=lambda x: x[0])
        if debug_logging:
            current_logger.debug('debug :: %s :: sorted_timeseries of length - %s' % (
                algorithm_name, str(len(sorted_timeseries))))
        # In compute terms, think lite, remember there could be multiple
        # processes running the algorithm, try keeping its footprint as small as
        # possible
        try:
            del timeseries
        except:
            pass

        # Test error handling
        # make_an_error = 2 * UNDEFINED_VARIABLE

        # Think about testing the data to ensure it meets any requirements
        try:
            start_timestamp = int(sorted_timeseries[0][0])
            end_timestamp = int(sorted_timeseries[-1][0])
            # If the time series does not have 3 days of data it does not have
            # sufficient data to sample.
            if (end_timestamp - start_timestamp) < 259200:
                return (anomalous, anomalyScore)
        except:
            sorted_timeseries = []
        if not sorted_timeseries:
            return (anomalous, anomalyScore)

        reversed_timeseries = sorted_timeseries[::-1]
        try:
            del sorted_timeseries
        except:
            pass
        datapoint = reversed_timeseries[0][1]
        for timestamp, value in reversed_timeseries:
            if int(timestamp) < end_timestamp:
                break
            oldest_timestamp_in_window = int(timestamp) - (int(sample_period))
            if int(timestamp) < oldest_timestamp_in_window:
                continue
            same_hour_data_points = []
            last_same_hour = int(timestamp) - 86400
            for sh_ts, sh_val in reversed_timeseries:
                if int(sh_ts) < oldest_timestamp_in_window:
                    break
                if int(sh_ts) > last_same_hour:
                    continue
                if int(sh_ts) < last_same_hour:
                    continue
                if int(sh_ts) == last_same_hour:
                    same_hour_data_points.append(sh_val)
                    last_same_hour = int(sh_ts) - 86400
                    continue
        if len(same_hour_data_points) > 1:
            mean_of_previous_hours = np.mean(same_hour_data_points)
            stdDev = np.std(same_hour_data_points)
            upper = (mean_of_previous_hours) + (3 * stdDev)
            lower = (mean_of_previous_hours) - (3 * stdDev)
            if debug_logging:
                current_logger.debug('debug :: %s :: data point - %s, mean - %s, upper - %s, lower - %s, same_hour_data_points - %s' % (
                    algorithm_name, str(datapoint), str(mean_of_previous_hours),
                    str(upper), str(lower), str(same_hour_data_points)))
            if value > upper:
                anomalous = True
                anomalyScore = 1.0
            if value < lower:
                anomalous = True
                anomalyScore = 1.0
        if not anomalous:
            anomalous = False
            anomalyScore = 0.0
        if debug_logging:
            current_logger.debug('debug :: %s :: anomalous - %s, anomalyScore - %s' % (
                algorithm_name, str(anomalous), str(anomalyScore)))
        try:
            del reversed_timeseries
        except:
            pass
        return (anomalous, anomalyScore)
    except StopIteration:
        # This except pattern MUST be used in ALL custom algortihms to
        # facilitate the traceback from any errors.  The algorithm we want to
        # run super fast and without spamming the log with lots of errors.
        # But we do not want the function returning and not reporting
        # anything to the log, so the pythonic except is used to "sample" any
        # algorithm errors to a tmp file and report once per run rather than
        # spewing tons of errors into the log e.g. analyzer.log
        return (None, None)
    except:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        # Return None and None as the algorithm could not determine True or False
        return (None, None)

    return (anomalous, anomalyScore)
