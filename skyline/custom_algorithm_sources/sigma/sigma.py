"""
sigma.py
"""
from os import getpid
import traceback

import pandas
import numpy as np
import scipy
import statsmodels.api as sm

# @added 20221019 - Feature #4702: numba optimisations
from numba import jit

from settings import (
    ALGORITHMS,
    MAX_TOLERABLE_BOREDOM,
    MIN_TOLERABLE_LENGTH,
    BOREDOM_SET_SIZE,
    SKYLINE_TMP_DIR,
)

USE_NUMBA = True


# @added 20221127 - Feature #4736: custom_algorithms - sigma
#                   Feature #4734: mirage_vortex
def tail_avg(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value):
    """
    This is a utility function used to calculate the average of the last three
    datapoints in the series as a measure, instead of just the last datapoint.
    It reduces noise, but it also reduces sensitivity and increases the delay
    to detection.
    """
    try:
        # t = (timeseries[-1][1] + timeseries[-2][1] + timeseries[-3][1]) / 3
        t = sum(Y[-sigma_value:]) / sigma_value
        return t
    except IndexError:
        return timeseries[-1][1]


# @added 20221020 - Feature #4702: numba optimisations
# Added the numba_median_absolute_deviation function
@jit(nopython=True, cache=True)
def numba_median_absolute_deviation(y_np_array, sigma_value):
    """
    This is a numba implementation of median_absolute_deviation, it speeds up
    the computation on a 1000 timeseries from 1.593684 seconds to
    0.029051 seconds.
    """
    median = np.median(y_np_array)
    demedianed = np.abs(y_np_array - median)
    median_deviation = np.median(demedianed)

    # The test statistic is infinite when the median is zero,
    # so it becomes super sensitive. We play it safe and skip when this happens.
    if median_deviation == 0:
        return False
    test_statistic = demedianed[-1] / median_deviation
    # Completely arbitary...triggers if the median deviation is
    # 6 times bigger than the median
    if test_statistic > (sigma_value * 2):
        return True
    return False


# @modified 20221019 - Feature #4700: algorithms - single series
# def median_absolute_deviation(timeseries):
def median_absolute_deviation(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value):
    """
    A timeseries is anomalous if the deviation of its latest datapoint with
    respect to the median is X times larger than the median of deviations.
    """
    # logger.info('Running ' + str(get_function_name()))

    # @added 20221020 - Feature #4702: numba optimisations
    if USE_NUMBA:
        try:
            result = numba_median_absolute_deviation(Y, sigma_value)
            return result
        except:
            traceback_format_exc_string = traceback.format_exc()
            algorithm_name = str(get_function_name())
            record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
            return None

    try:
        # @modified 20221019 - Feature #4700: algorithms - single series
        # series = pandas.Series([x[1] for x in timeseries])
        median = series.median()
        demedianed = np.abs(series - median)
        median_deviation = demedianed.median()
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
        return None

    # The test statistic is infinite when the median is zero,
    # so it becomes super sensitive. We play it safe and skip when this happens.
    if median_deviation == 0:
        return False

    try:
        test_statistic = demedianed.iat[-1] / median_deviation
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
        return None

    # Completely arbitary...triggers if the median deviation is
    # 6 times bigger than the median
    if test_statistic > (sigma_value * 2):
        return True

    # As per https://github.com/etsy/skyline/pull/104 by @rugger74
    # Although never seen this should return False if not > arbitary_value
    # 20160523 @earthgecko
    return False


def grubbs(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value):
    """
    A timeseries is anomalous if the Z score is greater than the Grubb's score.
    """

    try:
        # @modified 20191011 - Update least_squares & grubbs algorithms by using sample standard deviation PR #124
        #                      Task #3256: Review and test PR 124
        # Change from using scipy/numpy std which calculates the population
        # standard deviation to using pandas.std which calculates the sample
        # standard deviation which is more appropriate for time series data
        # series = scipy.array([x[1] for x in timeseries])
        # stdDev = scipy.std(series)
        # @modified 20221019 - Feature #4700: algorithms - single series
        # series = pandas.Series(x[1] for x in timeseries)

        stdDev = series.std()

        # Issue #27 - Handle z_score agent.py RuntimeWarning - https://github.com/earthgecko/skyline/issues/27
        # This change avoids spewing warnings on agent.py tests:
        # RuntimeWarning: invalid value encountered in double_scalars
        # If stdDev is 0 division returns nan which is not > grubbs_score so
        # return False here
        if stdDev == 0:
            return False

        mean = np.mean(series)
        tail_average = tail_avg(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value)
        z_score = (tail_average - mean) / stdDev
        len_series = len(series)
        threshold = scipy.stats.t.isf(.05 / (2 * len_series), len_series - 2)
        threshold_squared = threshold * threshold
        grubbs_score = ((len_series - 1) / np.sqrt(len_series)) * np.sqrt(threshold_squared / (len_series - 2 + threshold_squared))

        return z_score > grubbs_score
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
        return None


def first_hour_average(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value):
    """
    Calcuate the simple average over one hour, FULL_DURATION seconds ago.
    A timeseries is anomalous if the average of the last three datapoints
    are outside of three standard deviations of this value.
    """

    try:
        last_hour_threshold = timeseries[-1][0] - 86400
        last_hour_threshold_end = last_hour_threshold + 3600
        series = pandas.Series([x[1] for x in timeseries if x[0] > last_hour_threshold and x[0] < last_hour_threshold_end])
        mean = (series).mean()
        stdDev = (series).std()
        t = tail_avg(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value)

        return abs(t - mean) > sigma_value * stdDev
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
        return None


@jit(nopython=True, cache=True)
def numba_stddev_from_average(series, t):
    """
    """
    mean = np.mean(series)
    stdDev = np.std(series)
    return abs(t - mean) > 3 * stdDev


def stddev_from_average(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value):
    """
    A timeseries is anomalous if the absolute value of the average of the latest
    three datapoint minus the moving average is greater than three standard
    deviations of the average. This does not exponentially weight the MA and so
    is better for detecting anomalies with respect to the entire series.
    """

    try:
        # @modified 20221019 - Feature #4700: algorithms - single series
        # series = pandas.Series([x[1] for x in timeseries])
        mean = series.mean()
        stdDev = series.std()
        t = tail_avg(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value)

        return abs(t - mean) > sigma_value * stdDev
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
        return None


def stddev_from_moving_average(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value):
    """
    A timeseries is anomalous if the absolute value of the average of the latest
    three datapoint minus the moving average is greater than three standard
    deviations of the moving average. This is better for finding anomalies with
    respect to the short term trends.
    """
    try:
        expAverage = pandas.Series.ewm(series, ignore_na=False, min_periods=0, adjust=True, com=50).mean()
        stdDev = pandas.Series.ewm(series, ignore_na=False, min_periods=0, adjust=True, com=50).std(bias=False)
        # @added 20221019 - Feature #4702: numba optimisations
        # Evaluated some numba implementations of ewm.mean() and created a .std()
        # one and they were slower that pandas ewm
        return abs(series.iat[-1] - expAverage.iat[-1]) > sigma_value * stdDev.iat[-1]
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
        return None


# @added 20221019 - Feature #4702: numba optimisations
# Added the numba_mean_subtraction_cumulation function
@jit(nopython=True, cache=True)
def numba_mean_subtraction_cumulation(y_np_array, sigma_value):
    """
    This is a numba implementation of mean_subtraction_cumulation, it speeds up
    the computation on a 1000 timeseries from 7.042794 seconds to
    0.041275 seconds.
    """
    series_out = y_np_array - np.mean(y_np_array[0:len(y_np_array) - 1])
    stdDev = np.std(series_out[0:len(series_out) - 1])
    return abs(series_out[-1]) > 3 * stdDev


def mean_subtraction_cumulation(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value):
    """
    A timeseries is anomalous if the value of the next datapoint in the
    series is farther than three standard deviations out in cumulative terms
    after subtracting the mean from each data point.
    """

    try:

        if not USE_NUMBA:
            series = pandas.Series([x[1] if x[1] else 0 for x in timeseries])
            series = series - series[0:len(series) - 1].mean()
            stdDev = series[0:len(series) - 1].std()
            # @modified 20161228 - Feature #1828: ionosphere - mirage Redis data features
            # This expAverage is unused
            # if PANDAS_VERSION < '0.18.0':
            #     expAverage = pandas.stats.moments.ewma(series, com=15)
            # else:
            #     expAverage = pandas.Series.ewm(series, ignore_na=False, min_periods=0, adjust=True, com=15).mean()

            return abs(series.iat[-1]) > sigma_value * stdDev
        else:
            y_np_array = np.array([x[1] if x[1] else 0 for x in timeseries], dtype=np.float64)
            result = numba_mean_subtraction_cumulation(y_np_array, sigma_value)
            return result

    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
        return None


# @added 20221019 - Feature #4702: numba optimisations
# Added the numba_projected_errors function
@jit(nopython=True, cache=True)
def numba_projected_errors(x, y, m, c):
    """
    This is a numba implementation of the original calculation of the errors
    loop.  It speeds up the loop on a 1000 timeseries from 8.727679 seconds to
    2.785311 seconds.
    """
    errors = []
    # Evaluate append once, not every time in the loop - this gains ~0.020 s on
    # every timeseries potentially @earthgecko #1310
    append_error = errors.append
    for i, value in enumerate(y):
        projected = m * x[i] + c
        error = value - projected
        # errors.append(error) # @earthgecko #1310
        append_error(error)
    return errors


def least_squares(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value):
    """
    A timeseries is anomalous if the average of the last three datapoints
    on a projected least squares model is greater than three sigma.
    """

    try:
        A = np.vstack([X, np.ones(len(X))]).T
        # @modified 20161228 - Feature #1828: ionosphere - mirage Redis data features
        # This results and residual are unused
        # results = np.linalg.lstsq(A, y)
        # residual = results[1]

        # @modified 20180910 - Task #2588: Update dependencies
        # Changed in version numpy 1.14.0: If not set, a FutureWarning is given.
        # The previous default of -1 will use the machine precision as rcond
        # parameter, the new default will use the machine precision times
        # max(M, N). To silence the warning and use the new default, use
        # rcond=None, to keep using the old behavior, use rcond=-1.
        # Tested with time series - /opt/skyline/ionosphere/features_profiles/stats/statsd/processing_time/1491468474/stats.statsd.processing_time.mirage.redis.24h.json
        # new rcond=None resulted in:
        # np.linalg.lstsq(A, y, rcond=None)[0]
        # >>> array([3.85656116e-11, 2.58582310e-20])
        # Original default results in:
        # np.linalg.lstsq(A, y, rcond=-1)[0]
        # >>> array([ 4.10251589e-07, -6.11801949e+02])
        # Changed to pass rcond=-1
        # m, c = np.linalg.lstsq(A, y)[0]
        m, c = np.linalg.lstsq(A, Y, rcond=-1)[0]

        # @added 20221019 - Feature #4702: numba optimisations
        # Although numba supports np.linalg testing converting the above function
        # to use a jit function resulted in a massive slowdown.  Firstly there
        # are problems with using np.vstack without converting the np array lists
        # to a tuple (which is "reflection" in numba speak and is to be deprecated
        # soon).  Secondly this implemented on a 1000 loop takes
        # least_squares 1000 loop run took 8.727679 seconds
        # The numba implementation takes
        # nb_least_squares_and_nb_errors 1000 loop run took 64.184817 seconds!!!
        # A vast improvement was found in creating a numba function to replace
        # the errors function/iteration below
        # least_squares_and_nb_errors_only 1000 loop run took 2.785311 seconds

        # @modified 20221019 - Feature #4702: numba optimisations
        if not USE_NUMBA:
            errors = []
            # Evaluate append once, not every time in the loop - this gains ~0.020 s on
            # every timeseries potentially @earthgecko #1310
            append_error = errors.append

            # Further a question exists related to performance and accruracy with
            # regards to how many datapoints are in the sample, currently all datapoints
            # are used but this may not be the ideal or most efficient computation or
            # fit for a timeseries... @earthgecko is checking graphite...
            for i, value in enumerate(Y):
                projected = m * X[i] + c
                error = value - projected
                # errors.append(error) # @earthgecko #1310
                append_error(error)
        else:
            errors = numba_projected_errors(X, Y, m, c)

        if len(errors) < sigma_value:
            return False

        # @modified 20191011 - Update least_squares & grubbs algorithms by using sample standard deviation PR #124
        #                      Task #3256: Review and test PR 124
        # Change from using scipy/numpy std which calculates the population
        # standard deviation to using pandas.std which calculates the sample
        # standard deviation which is more appropriate for time series data
        # std_dev = scipy.std(errors)
        series = pandas.Series(x for x in errors)
        std_dev = series.std()

        # t = (errors[-1] + errors[-2] + errors[-3]) / 3
        t = sum(errors[-sigma_value:]) / sigma_value

        return abs(t) > std_dev * sigma_value and round(std_dev) != 0 and round(t) != 0
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
        return None


# @added 20221019 - Feature #4702: numba optimisations
# Added the numba get_bin_edges, compute_bin and numba_histogram functions
# based on https://numba.pydata.org/numba-examples/examples/density_estimation/histogram/results.html
@jit(nopython=True, cache=True)
def get_bin_edges(a, bins):
    """
    https://numba.pydata.org/numba-examples/examples/density_estimation/histogram/results.html
    """
    bin_edges = np.zeros((bins + 1,), dtype=np.float64)
    a_min = a.min()
    a_max = a.max()
    delta = (a_max - a_min) / bins
    for i in range(bin_edges.shape[0]):
        bin_edges[i] = a_min + i * delta

    bin_edges[-1] = a_max  # Avoid roundoff error on last point
    return bin_edges


@jit(nopython=True, cache=True)
def compute_bin(x, bin_edges):
    """
    https://numba.pydata.org/numba-examples/examples/density_estimation/histogram/results.html
    """
    # assuming uniform bins for now
    n = bin_edges.shape[0] - 1
    a_min = bin_edges[0]
    a_max = bin_edges[-1]

    # special case to mirror NumPy behavior for last bin
    if x == a_max:
        return n - 1  # a_max always in last bin

    n_bin = int(n * (x - a_min) / (a_max - a_min))

    if n_bin < 0 or n_bin >= n:
        return None
    return n_bin


@jit(nopython=True, cache=True)
def numba_histogram(a, bins):
    """
    https://numba.pydata.org/numba-examples/examples/density_estimation/histogram/results.html
    """
    hist = np.zeros((bins,), dtype=np.intp)
    bin_edges = get_bin_edges(a, bins)

    for x in a.flat:
        n_bin = compute_bin(x, bin_edges)
        if n_bin is not None:
            hist[int(n_bin)] += 1

    return hist, bin_edges


# @added 20221019 - Feature #4702: numba optimisations
# Added the numba_histogram_bins to iterate the bins generated by numba_histogram
# The original histogram_bins 1000 loop run took 1.790279 seconds
# The numba_histogram_bins 1000 loop run took 0.021249 seconds
@jit(nopython=True, cache=True)
def numba_histogram_bins(t, y_np_array):
    """
    Pass the tail average and the y np array
    """
    h = numba_histogram(y_np_array, bins=15)
    bins = h[1]
    for index, bin_size in enumerate(h[0]):
        if bin_size <= 20:
            # Is it in the first bin?
            if index == 0:
                if t <= bins[0]:
                    return True
            # Is it in the current bin?
            elif t >= bins[index] and t < bins[index + 1]:
                return True
    return False


def histogram_bins(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value):
    """
    A timeseries is anomalous if the average of the last three datapoints falls
    into a histogram bin with less than 20 other datapoints (you'll need to tweak
    that number depending on your data)

    Returns: the size of the bin which contains the tail_avg. Smaller bin size
    means more anomalous.
    """

    try:

        # @modified 20210420 - Support #4026: Change from scipy array to numpy array
        # Deprecation of scipy.array
        # series = scipy.array([x[1] for x in timeseries])
        # @modified 20221019 - Feature #4702: numba optimisations
        # Added dtype for numba
        # series = np.array([x[1] for x in timeseries])
        # series = np.array([x[1] for x in timeseries], dtype=np.float64)

        t = tail_avg(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value)

        if not USE_NUMBA:
            h = np.histogram(Y, bins=15)
            bins = h[1]
            for index, bin_size in enumerate(h[0]):
                if bin_size <= 20:
                    # Is it in the first bin?
                    if index == 0:
                        if t <= bins[0]:
                            return True
                    # Is it in the current bin?
                    elif t >= bins[index] and t < bins[index + 1]:
                        return True
        else:
            # @added 20221019 - Feature #4702: numba optimisations
            # Added the numba_histogram_bins to iterate the bins generated by
            # numba_histogram
            result = numba_histogram_bins(t, Y)
            return result

        return False
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
        return None


def ks_test(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value):
    """
    A timeseries is anomalous if 2 sample Kolmogorov-Smirnov test indicates
    that data distribution for last 10 minutes is different from last hour.
    It produces false positives on non-stationary series so Augmented
    Dickey-Fuller test applied to check for stationarity.
    """

    try:
        hour_ago = timeseries[-1][0] - 3600
        ten_minutes_ago = timeseries[-1][0] - 600
        # Adjust to resolution if not 60 seconds
        if second_order_resolution_seconds > 60:
            hour_ago = timeseries[-1][0] - (second_order_resolution_seconds * 60)
            ten_minutes_ago = timeseries[-1][0] - (second_order_resolution_seconds * 10)

        # @modified 20210420 - Support #4026: Change from scipy array to numpy array
        # Deprecation of scipy.array
        # reference = scipy.array([x[1] for x in timeseries if x[0] >= hour_ago and x[0] < ten_minutes_ago])
        # probe = scipy.array([x[1] for x in timeseries if x[0] >= ten_minutes_ago])
        reference = np.array([x[1] for x in timeseries if x[0] >= hour_ago and x[0] < ten_minutes_ago])
        probe = np.array([x[1] for x in timeseries if x[0] >= ten_minutes_ago])

        if reference.size < 20 or probe.size < 20:
            return False

        ks_d, ks_p_value = scipy.stats.ks_2samp(reference, probe)

        if ks_p_value < 0.05 and ks_d > 0.5:
            adf = sm.tsa.stattools.adfuller(reference, 10)
            if adf[1] < 0.05:
                return True

        return False
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
        return None

    return False


# THE END of NO MAN'S LAND
# THE START of UTILITY FUNCTIONS
def get_function_name():
    """
    This is a utility function is used to determine what algorithm is reporting
    an algorithm error when the record_algorithm_error is used.
    """
    return traceback.extract_stack(None, 2)[0][2]


def record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string):
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

    :param algorithm_name: the algoritm function name
    :type algorithm_name: str
    :param traceback_format_exc_string: the traceback_format_exc string
    :type traceback_format_exc_string: str
    :return:
        - ``True`` the error string was written to the algorithm_error_file
        - ``False`` the error string was not written to the algorithm_error_file

    :rtype:
        - boolean

    """

    current_process_pid = getpid()
    algorithm_error_file = '%s/%s.%s.%s.algorithm.error' % (
        SKYLINE_TMP_DIR, current_skyline_app, str(current_process_pid), algorithm_name)
    try:
        with open(algorithm_error_file, 'w') as f:
            f.write(str(traceback_format_exc_string))
        return True
    except:
        return False


# @added 20221127 - Feature #4736: custom_algorithms - sigma
#                   Feature #4734: mirage_vortex
def run_sigma_algorithms(current_skyline_app, timeseries, sigma_value, consensus, anomaly_window):
    """
    Run the sigma algorithms on the timeseries

    :param current_skyline_app: the Skyline app calling the function
    :param timeseries: the time series data
    :param metric_name: the full Redis metric name
    :param sigma_value: the sigma value to use
    :type current_skyline_app: str
    :type timeseries: list
    :type metric_name: str
    :type sigma_value: int
    :return: anomalous, ensemble, datapoint, algorithms_run
    :rtype: (boolean, list, float, list)

    """

    anomalous = None
    anomalyScore = 0.0
    anomalies = {}

    if len(timeseries) == 0:
        return anomalous, anomalyScore, anomalies

    if len(timeseries) < MIN_TOLERABLE_LENGTH:
        return anomalous, anomalies

    # Get rid of boring series
    if len(set(item[1] for item in timeseries[-MAX_TOLERABLE_BOREDOM:])) == BOREDOM_SET_SIZE:
        return anomalous, anomalies

    ensemble = []
    number_of_algorithms_triggered = 0
    number_of_algorithms_run = 0
    number_of_algorithms = len(ALGORITHMS)
    maximum_false_count = number_of_algorithms - consensus + 1
    consensus_possible = True

    # This optimisation is added to save microseconds on interpolating the
    # pandas series individually in the median_absolute_deviation,
    # mean_subtraction_cumulation, stddev_from_moving_average and
    # stddev_from_average algorithms.  The pandas series is interpolated once
    # here and passed to all the algorithms.
    all_series = pandas.Series(x[1] for x in timeseries)

    # numba optimisations
    all_X = np.array([x[0] for x in timeseries], dtype=np.float64)
    all_Y = np.array([x[1] for x in timeseries], dtype=np.float64)

    ts_diffs = np.diff(all_X)
    resolution_counts = np.unique(ts_diffs, return_counts=True)
    second_order_resolution_seconds = resolution_counts[0][np.argmax(resolution_counts[1])]

    try:
        if anomaly_window > 1:
            for i in list(range((-anomaly_window), 0)):
                algorithms_run = {}
                ensemble = []
                number_of_algorithms_triggered = 0
                number_of_algorithms_run = 0
                consensus_possible = True
                # series = pandas.Series(x[1] for x in timeseries[0:i])
                # X = np.array([x[0] for x in timeseries[0:i]], dtype=np.float64)
                # Y = np.array([x[1] for x in timeseries[0:i]], dtype=np.float64)
                series = all_series[0:i]
                current_timeseries = timeseries[0:i]
                X = all_X[0:i]
                Y = all_Y[0:i]
                for algorithm in ALGORITHMS:
                    if consensus_possible:
                        run_algorithm = []
                        run_algorithm.append(algorithm)
                        algorithm_none_result = False
                        try:
                            algorithm_result = [globals()[test_algorithm](current_skyline_app, X, Y, current_timeseries, second_order_resolution_seconds, series, sigma_value) for test_algorithm in run_algorithm]
                        except:
                            # logger.error('%s failed' % (algorithm))
                            algorithm_result = [None]
                            algorithm_none_result = True

                        # Coerce numpy.bool_ to bool as numpy.bool_ is not JSON serializable
                        if algorithm_result[0]:
                            bool_algorithm_result = True
                        else:
                            bool_algorithm_result = False
                        if algorithm_none_result:
                            bool_algorithm_result = None

                        algorithms_run[algorithm] = bool_algorithm_result
#                    else:
#                        algorithm_result = [None]
#                        algorithms_run.append(algorithm)
                        if algorithm_result.count(True) == 1:
                            result = True
                            number_of_algorithms_triggered += 1
                        elif algorithm_result.count(False) == 1:
                            result = False
                        elif algorithm_result.count(None) == 1:
                            result = None
                        else:
                            result = False
                        ensemble.append(result)
                        if ensemble.count(False) >= maximum_false_count:
                            consensus_possible = False
                threshold = len(ensemble) - consensus
                anomaly = False
                if ensemble.count(False) <= threshold and ensemble.count(False) >= 0:
                    anomaly = True
                if ensemble.count(True) >= consensus:
                    anomaly = True
                if ensemble.count(True) < consensus:
                    anomaly = False
                if anomaly:
                    anomaly_index = (len(timeseries) - 1 + i)
                    ts = int(timeseries[anomaly_index][0])
                    anomalies[ts] = {
                        'anomalous': True,
                        'anomalyScore': ensemble.count(True) / len(algorithms_run),
                        'index': anomaly_index,
                        'value': timeseries[anomaly_index][1],
                        'algorithms_results': algorithms_run,
                        'score': 1,
                    }

        ensemble = []
        number_of_algorithms_triggered = 0
        number_of_algorithms_run = 0
        consensus_possible = True
        algorithms_run = {}
        # @added 20221019 - Feature #4702: numba optimisations
        # Y = np.array([x[1] for x in timeseries], dtype=np.float64)
        for algorithm in ALGORITHMS:
            if consensus_possible:
                run_algorithm = []
                run_algorithm.append(algorithm)
                number_of_algorithms_run += 1
                algorithm_none_result = False
                try:
                    algorithm_result = [globals()[test_algorithm](current_skyline_app, all_X, all_Y, timeseries, second_order_resolution_seconds, all_series, sigma_value) for test_algorithm in run_algorithm]
                except:
                    # logger.error('%s failed' % (algorithm))
                    algorithm_result = [None]
                    algorithm_none_result = True

                # Coerce numpy.bool_ to bool as numpy.bool_ is not JSON serializable
                if algorithm_result[0]:
                    bool_algorithm_result = True
                else:
                    bool_algorithm_result = False
                if algorithm_none_result:
                    bool_algorithm_result = None

                algorithms_run[algorithm] = bool_algorithm_result
#            else:
#                algorithm_result = [None]
#                algorithms_run.append(algorithm)
                if algorithm_result.count(True) == 1:
                    result = True
                    number_of_algorithms_triggered += 1
                elif algorithm_result.count(False) == 1:
                    result = False
                elif algorithm_result.count(None) == 1:
                    result = None
                else:
                    result = False
                ensemble.append(result)
                if ensemble.count(False) >= maximum_false_count:
                    consensus_possible = False

        threshold = len(ensemble) - consensus
        anomaly = False
        if ensemble.count(False) <= threshold and ensemble.count(False) >= 0:
            anomaly = True
        if ensemble.count(True) >= consensus:
            anomaly = True
        if ensemble.count(True) < consensus:
            anomaly = False
        if anomaly:
            anomaly_index = len(timeseries) - 1
            ts = int(timeseries[anomaly_index][0])
            anomalies[ts] = {
                'anomalous': True,
                'anomalyScore': ensemble.count(True) / len(algorithms_run),
                'index': anomaly_index,
                'value': timeseries[anomaly_index][1],
                'algorithms_results': algorithms_run,
                'score': 1,
            }
    except:
        traceback_format_exc_string = traceback.format_exc()
        record_algorithm_error(current_skyline_app, 'sigma', traceback_format_exc_string)
        return None, anomalies

    if len(anomalies) == 0:
        return False, anomalies
    else:
        return True, anomalies
