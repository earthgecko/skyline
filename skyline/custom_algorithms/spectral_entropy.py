"""
spectral_entropy.py
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
from scipy.signal import periodogram

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20230614 - Feature #4950: custom_algorithm - spectral_entropy
# https://gist.githubusercontent.com/ningja1/4cce99b29657bb19079faf3b2a550639/raw/c8b0b77c154d48328c94fd449d015950c53aefa4/spectral_entropy.py
def spectral_entropy(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

    """
    Outlier detection for time-series data using Spectral Entropy.  EXPERIMENTAL
    The outlier method

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
            "window": 60,
            "frequency": 100,
            "max_low_entropy": 0.6,
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
    algorithm_name = 'spectral_entropy'

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

    # https://gist.githubusercontent.com/ningja1/4cce99b29657bb19079faf3b2a550639/raw/c8b0b77c154d48328c94fd449d015950c53aefa4/spectral_entropy.py
    # https://towardsdatascience.com/anomaly-detection-in-univariate-stochastic-time-series-with-spectral-entropy-834b63ec9343
    # In favour of the antropy spectral_residual implementation
    # because this method is quicker and both generate very
    # similar (probably the same) values.
    def get_spectral_entropy(x, freq, nfft=None):   
        _, psd = periodogram(x, freq, nfft=nfft)   
        # calculate shannon entropy of normalized psd
        psd_norm = psd / np.sum(psd)
        entropy = np.nansum(psd_norm * np.log2(psd_norm))
        return -(entropy / np.log2(psd_norm.size))

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
    # The sampling frequency of the x time series. Defaults to 100.
    frequency = 100
    try:
        frequency = float(algorithm_parameters['frequency'])
    except:
        frequency = 100
    # The rolling window to use (how many data points)
    window = 60
    try:
        window = int(algorithm_parameters['window'])
    except:
        window = 60

    # What is the maximum value that can be considered as
    # low entropy?  If the spectral_entropy value is not
    # close to 0 then it is not anomalous, but the range
    # which could be considered low varies somewhat.
    # Through experimentation and testing a general
    # value of 0.6 has been determined to represent
    # not anomalous behaviour in most cases.  In fact
    # it does very well at limiting the number of
    # anomalies that can be reported.
    max_low_entropy = 0.6
    try:
        max_low_entropy = float(algorithm_parameters['max_low_entropy'])
    except:
        max_low_entropy = 0.6

    if debug_print:
        print('running spectral_entropy with frequency: %s, window: %s' % (
            str(frequency), str(window)))
    if debug_logging:
        current_logger.debug('debug :: running spectral_entropy with frequency: %s, window: %s' % (
            str(frequency), str(window)))

    if debug_print:
        print('running spectral_entropy on timeseries with %s datapoints' % str(len(timeseries)))
    if debug_logging:
        current_logger.debug('debug :: running spectral_entropy on timeseries with %s datapoints' % str(len(timeseries)))

    try:
        series = [v for ts, v in timeseries]
        nfft = None
        df = pd.DataFrame(data=series, columns=['x'])
        df['x_roll_se'] = df['x'].rolling(window).apply(lambda x: get_spectral_entropy(x, freq=frequency, nfft=nfft))

        # Automatically determine the lowest possible spectral_entropy
        # values using histrgram bins, because the Shannon entropy
        # will not always be close to 0 but will still be smaller than
        # the "normal" windows.
        raw_values = list(df['x_roll_se'])
        se_values = []
        for index, se_value in enumerate(raw_values):
            # Replace nan with 1.0 which is the highest
            # entropy implying uncertainty and randomness,
            # not anomalous.  The nans pad the first window
            # of the df['x_roll_se'], that is just how it
            # rolls :)
            if str(se_value) == 'nan':
                se_value = 1.0
            se_values.append([index, se_value])
        # Sort the values, lowest to highest
        sorted_values = sorted(se_values, key=lambda x: x[1])
        values = [s for i, s in sorted_values]
        # bin the 0.0 to 1.0 values
        h = np.histogram(values, bins=15)
        # Example of the se value distribution
        # (array([  20,   19,   61,   71,  235,  375,  563,  607, 1038, 1522, 2144,
        #    2152,  945,  258,   62]),
        # array([0.39736418, 0.43753991, 0.47771563, 0.51789135, 0.55806707,
        #   0.59824279, 0.63841851, 0.67859423, 0.71876995, 0.75894567,
        #   0.79912139, 0.83929712, 0.87947284, 0.91964856, 0.95982428,
        #   1.        ]))
        if debug_print:
            print('spectral_entropy values histogram: %s' % str(h))
        if debug_logging:
            current_logger.debug('debug :: spectral_entropy values histogram: %s' % str(h))

        # Determine the lowest value bin
        bins = h[1]
        # array([0.39736418, 0.43753991, 0.47771563, 0.51789135, 0.55806707,
        #   0.59824279, 0.63841851, 0.67859423, 0.71876995, 0.75894567,
        #   0.79912139, 0.83929712, 0.87947284, 0.91964856, 0.95982428,
        # 1.        ])
        lowest_value_bin = bins[0]  # e.g. 0.39736418
        second_lowest_value_bin = bins[1]  # e.g. 0.43753991
        # Estimate the low entropy value based on the difference
        # between the bins and then some.  This gives a low
        # value to use as 
        low_entropy_value = lowest_value_bin + (second_lowest_value_bin - lowest_value_bin) / 3  # e.g. 0.41075609107379757
        # Given the above example distribution this would
        # result in the most 39 "outliers" in the 10072
        # datapoints, so 0.387212073% would be flagged as
        # anomalous.

        if debug_print:
            print('spectral_entropy low_entropy_value: %s' % str(low_entropy_value))
        if debug_logging:
            current_logger.debug('debug :: spectral_entropy low_entropy_value: %s' % str(low_entropy_value))

        # However limit the max value that should be considered
        # as low entropy
        if low_entropy_value > max_low_entropy:
            low_entropy_value = float(max_low_entropy)
            if debug_logging:
                current_logger.debug('debug :: spectral_entropy low_entropy_value set to max_low_entropy: %s' % str(max_low_entropy))

        anomalyScore_list = []
        anomalies = {}
        errors = []
        for index, item in enumerate(timeseries):
            try:
                se_value = se_values[index][1]
                if se_value <= low_entropy_value:
                    ts = int(item[0])
                    anomalies[ts] = {'value': item[1], 'index': index, 'score': se_value}
                    anomalyScore_list.append(1)
                else:
                    anomalyScore_list.append(0)
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
            'low_entropy_value': low_entropy_value,
            'anomalyScore_list': anomalyScore_list,
            'scores': [s for i, s in se_values],
        }
        if debug_print:
            print('ran spectral_entropy OK in %.6f seconds' % (time() - start))
        if debug_logging:
            current_logger.debug('debug :: ran spectral_entropy OK in %.6f seconds' % (time() - start))
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
            print('warning - StopIteration called on spectral_entropy')
        if debug_logging:
            current_logger.debug('debug :: warning - StopIteration called on spectral_entropy')

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
            current_logger.debug('debug :: error - on spectral_entropy - %s' % err)
            current_logger.debug(traceback.format_exc())

        # Return None and None as the algorithm could not determine True or False
        if return_results:
            return (None, None, None)
        return (None, None)

    if return_results:
        return (anomalous, anomalyScore, results)

    return (anomalous, anomalyScore)
