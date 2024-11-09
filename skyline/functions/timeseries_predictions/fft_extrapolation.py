"""
fft_extrapolation.py
"""
import logging
import traceback

import numpy as np
from numpy import fft


# @added 20230731 - Feature #5040: functions.timeseries_prediction.fft_extrapolation
# Predicting timeseries using Fourier Extrapolation
# https://stackoverflow.com/a/28163549
# https://gist.github.com/tartakynov/83f3cd8f44208a1856ce
# Based on fork:
# https://gist.github.com/endolith/bf60a63453c927b04db29cecebcc4efa
# With sort on amplitude as per https://gist.github.com/tartakynov/83f3cd8f44208a1856ce?permalink_comment_id=2760760#gistcomment-2760760
def fft_extrapolation(current_skyline_app, timeseries, n_predict=10, n_harmonics=4, log=False):
    """
    Predict the next n values in the timeseries based on Fourier Extrapolation.
    This can be used for padding timeseries with probable values for algorithms
    that are window based and need more data to analyse the final window such as
    m66 and matrixprofile.  This is a utility algorithm ONLY and the predictions
    do not need to be accurate, just within a reasonable probability and
    providing data to the algorithms that will allow for the data points in the
    final window to be evaluated with the windowed algorithm in question.

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param timeseries: the timeseries as a list of lists e.g.
        [[1618761782, 123],...,[1618848182, 78]]
    :param n_predict: the number of predictions to return, this should be a
        value greater than the window size of the algorithm which is to be used
        for analysis, e.g. if the predictions are being made to pad data for the
        m66 algorithm which is using a window of 60, then n_predict should be
        > 60.
    :param n_harmonics: the number of harmonics to use, 
    :param log: whether to log or not, optional, defaults to False
    :type current_skyline_app: str
    :type timeseries: list
    :type n_predict: int
    :type n_harmonics: int
    :type log: boolean
    :return: predicted_timeseries
    :rtype:  list
    """
    function_str = 'functions.timeseries_prediction.fft_extrapolation'

    predicted_timeseries = list(timeseries)

    metric_resolution = None
    timestamp_resolutions_count = {}
    current_logger = None
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        np_timestamps_array = np.array([t for t, v in timeseries])
        x = np.array([v for t, v in timeseries])

        # Remove None and nan values as the algorithm is not robust to missing
        # values, however just removing these changes indices so rather use last
        # value
        none_indices = [index for index, item in enumerate(timeseries) if item[1] is None]
        nan_indices = [index for index, item in enumerate(timeseries) if index not in none_indices and np.isnan(item[1])]

        nan_indices = none_indices + nan_indices
        nan_indices.sort()

        if nan_indices:
            # x = np.array([item[1] for index, item in enumerate(timeseries) if index not in nan_indices])
            last_value = None
            # @modified 20240601 - Feature #5352: vista - bigquery
            #                      Feature #5040: functions.timeseries_prediction.fft_extrapolation
            # Use nanmean which is the mean of non nans
            # mean_value = np.mean(x)
            mean_value = np.nanmean(x)

            # @added 20240223 - Feature #5040: functions.timeseries_prediction.fft_extrapolation
            # Handle no mean
            no_mean = False
            if np.isnan(mean_value):
                no_mean = True
            if not isinstance(mean_value, float):
                no_mean = True
            if no_mean:
                if log:
                    current_logger.warning('warning :: %s :: cannot predict values - no mean, mean_value: %s' % (
                        function_str, str(mean_value)))
                predicted_timeseries = list(timeseries)
                return predicted_timeseries

            timeseries_no_nans = []
            for index, item in enumerate(timeseries):
                # if isinstance(v, float) and not np.isnan(v):
                if index not in nan_indices:
                    timeseries_no_nans.append(item)
                    last_value = item[1]
                else:
                    if last_value:
                        timeseries_no_nans.append([item[0], last_value])
                    else:
                        timeseries_no_nans.append([item[0], mean_value])
            x = np.array([v for t, v in timeseries_no_nans])
            # @modified 20240601 - Feature #5352: vista - bigquery
            #                      Feature #5040: functions.timeseries_prediction.fft_extrapolation
            # Use nanmean which is the mean of non nans
            # mean_value = np.mean(x)
            mean_value = np.nanmean(x)

        ts_diffs = np.diff(np_timestamps_array)
        resolution_counts = np.unique(ts_diffs, return_counts=True)
        resolution = resolution_counts[0][np.argmax(resolution_counts[1])]
        n = x.size
        n_harm = n_harmonics              # number of harmonics in model
        t = np.arange(0, n)
        p = np.polyfit(t, x, 1)         # find linear trend in x
        x_notrend = x - p[0] * t        # detrended x
        x_freqdom = fft.fft(x_notrend)  # detrended x in frequency domain
        f = fft.fftfreq(n)              # frequencies
        indexes = list(range(n))
        # sort indexes by frequency, lower -> higher
        #indexes.sort(key=lambda i: np.absolute(f[i]))
        # sort indexes by amplitude as per https://gist.github.com/tartakynov/83f3cd8f44208a1856ce?permalink_comment_id=2760760#gistcomment-2760760
        indexes.sort(key=lambda i: np.absolute(x_freqdom[i]))
        indexes.reverse()
        t = np.arange(0, n + n_predict)
        restored_sig = np.zeros(t.size)
        for i in indexes[:1 + n_harm * 2]:
            ampli = np.absolute(x_freqdom[i]) / n   # amplitude
            phase = np.angle(x_freqdom[i])          # phase
            restored_sig += ampli * np.cos(2 * np.pi * f[i] * t + phase)
        extrapolation = restored_sig + p[0] * t
        extrapolation_values = extrapolation.tolist()
        last_ts = timeseries[-1][0]
        last_timeseries_index = len(timeseries) - 1
        for index, v in enumerate(extrapolation_values):
            if index <= last_timeseries_index:
                continue
            next_ts = int(last_ts + resolution)
            last_ts = int(next_ts)
            predicted_timeseries.append([next_ts, v])
    except Exception as err:
        if log:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to predict values - %s' % (
                function_str, err))
        predicted_timeseries = list(timeseries)

    return predicted_timeseries

