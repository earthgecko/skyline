"""
macd.py
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
from time import time
import pandas as pd
import numpy as np

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20221114 - Feature #4960: custom_algorithms - macd
# https://en.wikipedia.org/wiki/MACD
def macd(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

    """
    Outlier detection for time-series data using Moving Average Convergence/Divergence
    https://en.wikipedia.org/wiki/MACD - EXPERIMENTAL

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
        custom_algorithm and algorithm itself.  Example:
        "algorithm_parameters": {
            "anomaly_window": 1,
            "fast_window": 12,
            "slow_window": 26,
            "signal_window": 9,
            "feature": "macd",  # This can be macd, macd_signal or macd_histogram
            "return_results": True,
        }
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: anomalous, anomalyScore, results
    :rtype: tuple(boolean, float, dict)

    """

    # You MUST define the algorithm_name
    algorithm_name = 'macd'

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = None
    results = {}

    current_logger = None

    # If you wanted to log, you can but this should only be done during
    # testing and development
    def get_log(current_skyline_app):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        return current_logger

    start = time()

    return_results = False
    try:
        return_results = algorithm_parameters['return_results']
    except:
        return_results = False

    if not return_results:
        try:
            return_results = algorithm_parameters['return_anomalies']
        except:
            return_results = False

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
            if return_results:
                return (None, None, None)
            return (None, None)

    # Use the algorithm_parameters to determine variables
    debug_print = None
    try:
        debug_print = algorithm_parameters['debug_print']
    except:
        debug_print = False

    # If any data points in the last anomaly_window data points are
    # anomalous, the instance is classed as anomalous.  For example
    # if set to 1 the instance would only be anomalous if the last
    # data point was anomalous.  If it is set to 5, the instance
    # would be classed as anomalous if ANY one of the last 5 data
    # points were classed as anomalous.
    # How many data points?
    anomaly_window = 1
    try:
        anomaly_window = int(algorithm_parameters['anomaly_window'])
    except:
        anomaly_window = 1

    # The slow and fast EMA windows to use (how many data points)
    fast_window = 12
    try:
        fast_window = int(algorithm_parameters['fast_window'])
    except:
        fast_window = 12
    slow_window = 26
    try:
        slow_window = int(algorithm_parameters['slow_window'])
    except:
        slow_window = 26
    signal_window = 9
    try:
        signal_window = int(algorithm_parameters['signal_window'])
    except:
        signal_window = 9

    # What is macd feature to use: macd, macd_signal or macd_histogram
    feature = 'macd'
    try:
        feature = algorithm_parameters['feature']
    except:
        feature = 'macd'

    if debug_print:
        print('running macd with fast_window: %s, slow_window: %s, signal_window: %s' % (
            str(fast_window), str(slow_window), str(signal_window)))
    if debug_logging:
        current_logger.debug('debug :: running macd with fast_window: %s, slow_window: %s, signal_window: %s' % (
            str(fast_window), str(slow_window), str(signal_window)))

    if debug_print:
        print('running macd on timeseries with %s datapoints' % str(len(timeseries)))
    if debug_logging:
        current_logger.debug('debug :: running macd on timeseries with %s datapoints' % str(len(timeseries)))

    try:

        Y = np.array([item[1] for item in timeseries])
        df = pd.DataFrame(data=Y, columns=['value'])

        # Determine the EMAs
        ema12 = df['value'].ewm(span=fast_window, adjust=False, min_periods=fast_window).mean()
        ema26 = df['value'].ewm(span=slow_window, adjust=False, min_periods=slow_window).mean()

        macd_signal = None
        macd_histogram = None

        macd = ema12 - ema26
        if feature in ['macd_signal', 'macd_histogram']:
            # Get the medium EMA of the MACD for the Trigger line
            macd_signal = macd.ewm(span=signal_window, adjust=False, min_periods=signal_window).mean()
        if feature == 'macd_histogram':
            # Calculate the difference between the MACD - Trigger for the Convergence/Divergence value
            macd_histogram = macd - macd_signal
        # Add all of our new values for the MACD to the dataframe
        df['macd'] = df.index.map(macd)
        if feature in ['macd_signal', 'macd_histogram']:
            df['macd_signal'] = df.index.map(macd_signal)
        if feature == 'macd_histogram':
            df['macd_histogram'] = df.index.map(macd_histogram)
        raw_scores = list(df[feature])
        macd_scores = []
        for index, score in enumerate(raw_scores):
            # Replace nan with 0.0.  The nans pad the first window
            # of the df, that is just how it rolls.
            if str(score) == 'nan':
                score = 0.0
            macd_scores.append([index, score])
        # Sort the scores, lowest to highest
        sorted_scores = sorted(macd_scores, key=lambda x: x[1])
        scores = [s for i, s in sorted_scores]
        # bin the 0.0 to 1.0 scores
        h = np.histogram(scores, bins=15)
        # Example of the score distribution
        # (array([  11,  414, 1504, 1851,  263,   89,   28,   24,   15,   18,   24,
        #         16,   24,   21,   18]),
        # array([-90.16033348, -63.07251163, -35.98468978,  -8.89686793,
        #         18.19095393,  45.27877578,  72.36659763,  99.45441948,
        #         126.54224133, 153.63006318, 180.71788503, 207.80570688,
        #         234.89352873, 261.98135058, 289.06917243, 316.15699428]))

        # Determine the lowest value bin
        bins = h[1]
        lowest_value_bin = bins[0]  # e.g. -90.16033348
        second_lowest_value_bin = bins[1]  # e.g. -63.07251163
        highest_value_bin = bins[-1]  # e.g. 316.15699428
        second_highest_value_bin = bins[-2]  # e.g. 289.06917243

        # In some cases the bin with the highest values will incorporate
        # most of the datapoints signifying that the upper value bins
        # do not contain any anomalous values
        if h[0][-1] >= len(timeseries) / 3:
            highest_value_bin = None
            second_highest_value_bin = None

        # In some cases the bin with the lowest values will incorporate
        # most of the datapoints signifying that the lower value bins
        # do not contain any anomalous values
        if h[0][0] >= len(timeseries) / 3:
            lowest_value_bin = None
            second_lowest_value_bin = None

        anomalyScore_list = []
        anomalies = {}
        errors = []
        for index, item in enumerate(timeseries):
            ts = int(item[0])
            a_score = 0
            try:
                score = macd_scores[index][1]
                if second_lowest_value_bin:
                    if score <= second_lowest_value_bin and score < 0:
                        anomalies[ts] = {'value': item[1], 'index': index, 'score': score}
                        a_score = 1
                if second_highest_value_bin:
                    if score >= second_highest_value_bin and score > 1:
                        anomalies[ts] = {'value': item[1], 'index': index, 'score': score}
                        a_score = 1
                anomalyScore_list.append(a_score)
            except Exception as err:
                errors.append([err])
                anomalyScore_list.append(0)
        if errors:
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
            if debug_logging:
                current_logger.debug('debug :: errors, sample: %s' % str(errors[-3:]))
                current_logger.debug(traceback.format_exc())

        anomaly_sum = sum(anomalyScore_list[-anomaly_window:])
        if anomaly_sum > 0:
            anomalous = True
        else:
            anomalous = False
        results = {
            'anomalous': anomalous,
            'anomalies': anomalies,
            'anomalyScore_list': anomalyScore_list,
            'scores': [s for i, s in macd_scores],
            'feature': feature,
        }
        if debug_print:
            print('ran macd OK in %.6f seconds' % (time() - start))
        if debug_logging:
            current_logger.debug('debug :: ran macd OK in %.6f seconds' % (time() - start))
        if results:
            if results['anomalous']:
                anomalous = True
                anomalyScore = 1.0
            else:
                anomalous = False
                anomalyScore = 0.0
            if debug_print:
                print('anomalous: %s' % str(anomalous))
            if debug_logging:
                current_logger.debug('debug :: anomalous: %s' % str(anomalous))
        else:
            if debug_print:
                print('error - no results')
            if debug_logging:
                current_logger.debug('debug :: error - no results')

    except StopIteration:
        if debug_print:
            print('warning - StopIteration called on macd')
        if debug_logging:
            current_logger.debug('debug :: warning - StopIteration called on macd')

        # This except pattern MUST be used in ALL custom algortihms to
        # facilitate the traceback from any errors.  The algorithm we want to
        # run super fast and without spamming the log with lots of errors.
        # But we do not want the function returning and not reporting
        # anything to the log, so the pythonic except is used to "sample" any
        # algorithm errors to a tmp file and report once per run rather than
        # spewing tons of errors into the log e.g. analyzer.log
        if return_results:
            return (None, None, None)
        return (None, None)
    except Exception as err:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if debug_print:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error - on macd - %s' % err)
            current_logger.debug(traceback.format_exc())

        # Return None and None as the algorithm could not determine True or False
        if return_results:
            return (None, None, None)
        return (None, None)

    if return_results:
        return (anomalous, anomalyScore, results)

    return (anomalous, anomalyScore)
