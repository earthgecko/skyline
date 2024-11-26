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
        custom_algorithm and algorithm itself.  For the spectral_entropy
        custom algorithm no specific algorithm_parameters are required apart
        from an empty dict but the spectral_entropy algorithm_parameters
        that can be passed are:

        - ``'anomaly_window'`` (int): The anomaly_window value.
            This specifies how many of the last data points should be considered
            when determining if the metric is anomalous. Only the last
            ``anomaly_window`` data points in the time series will be used to
            determine if the metric is anomalous.  Default is ``1``.
        - ``'frequency'`` (int): The sampling frequency of the x time series.
            Default is ``100``.
        - ``'window'`` (int): The rolling window.
            How many data points in the rolling window.  Default is ``60``.
        - ``'max_low_entropy'`` (float): The maximum low entropy value.
            What is the maximum value that can be considered as low entropy?  If
            the spectral_entropy value is not close to 0 then it is not
            anomalous, but the range which could be considered low varies
            somewhat.  Through experimentation and testing a general value of
            0.6 has been determined to represent not anomalous behaviour in most
            cases.  In fact it does very well at limiting the number of
            anomalies that can be reported.  Default is ``0.6``.
        - ``'determine_frequency'`` (bool): Optional.
            If ``True`` the frequency is determined automatically from the time
            series data. Default is ``False``.
        - ``'return_results'`` (bool): Optional.
            If ``True``, returns the results dict in addition to anomalous and
            anomalyScore.  Default is ``False``.
        - ``'debug_logging'`` (bool): Optional.
            If ``True``, enables debug logging.
        - ``'debug_print'`` (bool): Optional.
            If ``True``, enables debug printing  (for Jupyter testing). Default
            is ``False``.

        Example usage:
        
            algorithm_parameters={
                'anomaly_window': 1,
                'window': 60,
                'determine_frequency': True,
                'max_low_entropy': 0.6,
                'debug_logging': True,
                'return_results': True,
            }

    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: anomalous, anomalyScore, results
    :rtype: tuple(bool, float, dict)

    """

    # You MUST define the algorithm_name
    algorithm_name = 'spectral_entropy'

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = False
    # A dict to add results to
    results = {}
    # A list of binary results for each data point, either 1 == anomalous or
    # 0 == not_anomalous.  This is not the algorithm score, just whether the
    # algorithm determined the data point as anomalous or not.
    anomalyScore_list = []
    # A dict for anomalies
    anomalies = {}
    # @added 20230707 - custom_algorithms - handle unreliable results
    # To test the reliability of the result
    unreliable = False
    # A reason if unreliabile
    unreliable_reason = 'None'

    current_logger = None

    # If you wanted to log, you can but this should only be done during
    # testing and development
    def get_log(current_skyline_app):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        return current_logger

    # @added 20230717 - Feature #4950: custom_algorithm - spectral_entropy
    def get_resolution(np_timestamps_array):
        ts_diffs = np.diff(np_timestamps_array)
        resolution_counts = np.unique(ts_diffs, return_counts=True)
        resolution = resolution_counts[0][np.argmax(resolution_counts[1])]
        return resolution

    # https://gist.githubusercontent.com/ningja1/4cce99b29657bb19079faf3b2a550639/raw/c8b0b77c154d48328c94fd449d015950c53aefa4/spectral_entropy.py
    # https://towardsdatascience.com/anomaly-detection-in-univariate-stochastic-time-series-with-spectral-entropy-834b63ec9343
    # In favour of the antropy spectral_residual implementation
    # because this method is quicker and both generate very
    # similar (probably the same) values.
    def get_spectral_entropy(x, freq, nfft=None):   
        _, psd = periodogram(x, freq, nfft=nfft)   
        # calculate shannon entropy of normalized power spectrum density
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
    # The sampling frequency of the x time series. Defaults to 100.  Which in
    # terms of the fs argument in scipy.signal.periodogram a time series with
    # 1 data point per 60 seconds should be 1/60 (0.016666666666666666) and
    # 1/600 (0.0016666666666666668) for 10 minutely data.
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

    # @added 20230717 - Feature #4950: custom_algorithm - spectral_entropy
    determine_frequency = False
    try:
        determine_frequency = algorithm_parameters['determine_frequency']
    except:
        determine_frequency = False

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

    algorithm_parameters_used = {
        'anomaly_window': anomaly_window, 'frequency': frequency,
        'window': window, 'max_low_entropy': max_low_entropy
    }

    # @added 20230717 - Feature #4950: custom_algorithm - spectral_entropy
    passed_frequency = float(frequency)
    if determine_frequency:
        resolution = 0
        try:
            np_timestamps_array = np.array([int(t) for t, _ in timeseries])
            resolution = get_resolution(np_timestamps_array)
            if resolution:
                frequency = 1/resolution
                if debug_logging:
                    current_logger.debug('debug :: spectral_entropy - determined frequency: %s' % str(frequency))
        except Exception as err:
            if debug_logging:
                current_logger.debug('error :: get_resolution failed in spectral_entropy, err: %s' % err)

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
            # Coerce np.int64 to a float
            se_values.append([index, float(se_value)])
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
        lowest_value_bin = float(bins[0])  # e.g. 0.39736418
        second_lowest_value_bin = float(bins[1])  # e.g. 0.43753991
        # Estimate the low entropy value based on the difference
        # between the bins and then some.  This gives a low
        # value to use as 
        low_entropy_value = float(lowest_value_bin + (second_lowest_value_bin - lowest_value_bin) / 3)  # e.g. 0.41075609107379757
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

        # @added 20230707 - debug
        # Add histogram to the results in JSON friendly format
        results['histogram_bins'] = {}
        # Coerce to ints and floats for JSON results
        #histogram_bins_counts = list(h[0])
        #histogram_bins_values = list(h[1])
        histogram_bins_counts = [int(v) for v in h[0]]
        histogram_bins_values = [float(v) for v in h[1]]

        results['histogram_bins']['counts'] = histogram_bins_counts
        results['histogram_bins']['values'] = histogram_bins_values

        errors = []
        for index, item in enumerate(timeseries):
            try:
                se_value = se_values[index][1]
                # @modified 20230717
                # Handle 0s - ignore 0s
                # https://stats.stackexchange.com/questions/57069/alternative-to-shannons-entropy-when-probability-equal-to-zero
                # if se_value <= low_entropy_value:
                if se_value <= low_entropy_value and se_value != 0.0:
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

        # @added 20230707 - custom_algorithms - handle unreliable results
        success = True
        total_anomaly_sum = sum(anomalyScore_list)
        if total_anomaly_sum > (len(timeseries) / 5):
            # None is not json friendly
            # anomalous = None
            anomalous = False
            unreliable = True
            unreliable_reason = '%s triggered %s data points of the total %s data points as anomalous, discarding as this algorithm does not suit this data.' % (
                algorithm_name, str(total_anomaly_sum), str(len(timeseries))            )

        results['anomalous'] = anomalous
        results['anomalies'] = anomalies
        results['low_entropy_value'] = float(low_entropy_value)
        results['anomalyScore_list'] = anomalyScore_list
        results['scores'] = [s for i, s in se_values]
        results['unreliable'] = unreliable
        results['unreliable_reason'] = unreliable_reason
        results['success'] = success
        results['algorithm_parameters'] = algorithm_parameters
        results['algorithm_parameters_used'] = algorithm_parameters_used
        # @added 20230717 - Feature #4950: custom_algorithm - spectral_entropy
        results['frequency'] = float(frequency)
        results['passed_frequency'] = passed_frequency

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

        # @added 20230707 - custom_algorithms - handle unreliable results
        if unreliable:
            anomalous = False
            anomalyScore = 0.0

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
