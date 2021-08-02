"""
THIS IS A SAMPLE CUSTOM ALGORITHM to provide a skeleton to develop your own
custom algorithms.  The algorithm itself, although viable, is not recommended for
production or general use, it is simply a toy algorithm here to demonstrate the
structure of a simple custom algorithm that has ``algorithm_parameters`` passed
as an empty dict {}.
It is documented via comments #
"""

# REQUIRED Skyline imports.  All custom algorithms MUST have the following two
# imports.  These are required for exception handling and to record algorithm
# errors regardless of debug_logging setting for the custom_algorithm
import traceback
from custom_algorithms import record_algorithm_error

# Import ALL modules that the custom algorithm requires.  Remember that if a
# requirement is not one that is provided by the Skyline requirements.txt you
# must ensure it is installed in the Skyline virtualenv
import pandas

# To test max_execution_time import sleep
# from time import sleep


# Define your simple algorithm.
# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except
def abs_stddev_from_median(current_skyline_app, parent_pid, timeseries, algorithm_parameters):
    """
    A data point is anomalous if its absolute value is greater than three
    standard deviations of the median.

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
    algorithm_name = 'abs_stddev_from_median'

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = None

    # To test max_execution_time enable a sleep
    # sleep(1)

    # ALWAYS WRAP YOUR ALGORITHM IN try and the BELOW except
    try:
        value = timeseries[-1][1]
        series = pandas.Series([x[1] for x in timeseries])
        median = series.median()
        stdDev = series.std()
        upper = median + (3 * stdDev)
        lower = median - (3 * stdDev)
        # make_error = median * UNDEFINED_VARIABLE
        if value > upper:
            anomalous = True
            anomalyScore = 1.0
        if value < lower:
            anomalous = True
            anomalyScore = 1.0
        if not anomalous:
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
        return (None, None)
    except:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        # Return None and None as the algorithm could not determine True or False
        return (None, None)
    return (anomalous, anomalyScore)
