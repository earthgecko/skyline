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
import logging
import traceback
from custom_algorithms import record_algorithm_error

# Import ALL modules that the custom algorithm requires.  Remember that if a
# requirement is not one that is provided by the Skyline requirements.txt you
# must ensure it is installed in the Skyline virtualenv
import pandas as pd

# To test max_execution_time import sleep
# from time import sleep

from functions.numpy.percent_different import get_percent_different


# Define your simple algorithm.
# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except
def significant_change_window_percent_sustained(
        current_skyline_app, parent_pid, timeseries, algorithm_parameters):
    """
    A data point is anomalous if it is x percent different from the median of the
    window (seconds resample) of the last p period (seconds). A few examples,

    If the value is 10% different from the median value of the 10min windows of
    the last hour.
    algorithm_parameters: {'window': 600, 'percent': 10.0, 'period': 3600}

    If the value is 50% different from the median value of the 10min windows of
    the last day.
    algorithm_parameters: {'window': 600, 'percent': 50.0, 'period': 86400}

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
    algorithm_name = 'significant_change_window_percent_sustained'

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
    anomalous = None
    anomalyScore = None

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
            return (anomalous, anomalyScore)

    # window in seconds
    window = 600
    try:
        window = algorithm_parameters['window']
    except:
        pass
    resample_window = '%sS' % str(window)

    # Allow the LevelShiftAD c parameter to be passed in the
    # algorithm_parameters
    percent = 10.0
    try:
        percent = algorithm_parameters['percent']
    except:
        pass

    period = 3600
    try:
        period = algorithm_parameters['period']
    except:
        pass

    return_percent_as_anomalyScore = False
    try:
        return_percent_as_anomalyScore = algorithm_parameters['return_percent_as_anomalyScore']
    except:
        pass

    times_in_a_row = 0
    try:
        times_in_a_row = algorithm_parameters['times_in_a_row']
    except:
        pass

    # To test max_execution_time enable a sleep
    # sleep(1)

    # ALWAYS WRAP YOUR ALGORITHM IN try and the BELOW except
    try:
        timestamp = int(timeseries[-1][0])
        value = timeseries[-1][1]
        values = []
        if times_in_a_row:
            values = [item[1] for item in timeseries[-times_in_a_row:]]
        from_timestamp = timestamp - period
        applicable_timeseries = [item for item in timeseries if int(item[0] >= from_timestamp)]
        del timeseries
        df = pd.DataFrame(applicable_timeseries, columns=['date', 'value'])
        df['date'] = pd.to_datetime(df['date'], unit='s')
        datetime_index = pd.DatetimeIndex(df['date'].values)
        df = df.set_index(datetime_index)
        df.drop('date', axis=1, inplace=True)
        resampled_df = df.resample(resample_window, origin='epoch').median()
        del df
        resampled_values = resampled_df['value'].values.tolist()
        series = pd.Series([x for x in resampled_values])
        del resampled_df
        median = series.median()
        percent_different = get_percent_different(median, value, False)
        if percent_different > percent:
            anomalous = True
            anomalyScore = 1.0
        else:
            anomalous = False
            anomalyScore = 0.0
        if return_percent_as_anomalyScore:
            anomalyScore = percent_different
        anomalous_values = []
        anomalous_count = 0
        if values:
            for v in values:
                percent_different = get_percent_different(median, v, False)
                if percent_different > percent:
                    anomalous = True
                    anomalyScore = 1.0
                    anomalous_count += 1
                else:
                    anomalous = False
                    anomalyScore = 0.0
                if return_percent_as_anomalyScore:
                    anomalyScore = percent_different
                anomalous_values.append([anomalous, anomalyScore])
        if anomalous_values and anomalous_count:
            if anomalous_count == times_in_a_row:
                anomalous = True
                anomalyScores = [item[1] for item in anomalous_values]
                anomalyScore = sum(anomalyScores) / len(anomalyScores)
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
