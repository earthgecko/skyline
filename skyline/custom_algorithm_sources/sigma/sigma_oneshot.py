"""
sigma_oneshot.py
"""
from os import getpid
from time import time
import traceback

import pandas
import numpy as np
import scipy

# @added 20221019 - Feature #4702: numba optimisations
from numba import jit

import bottleneck as bn

from settings import (
    MAX_TOLERABLE_BOREDOM,
    MIN_TOLERABLE_LENGTH,
    BOREDOM_SET_SIZE,
    SKYLINE_TMP_DIR,
)


# @added 20260608 - Feature #4736: custom_algorithms - sigma
# Added this oneshot implementation that is used if the sigma anomaly_window is
# > 20

@jit(nopython=True, cache=True)
def numba_median_absolute_deviation(y_np_array, sigma_value):
    """
    This is a numba implementation of median_absolute_deviation, it speeds up
    the computation on a 1000 timeseries from 1.593684 seconds to
    0.029051 seconds.
    """
    scores = np.zeros((len(y_np_array), 0), dtype=np.float64)

    median = np.median(y_np_array)
    demedianed = np.abs(y_np_array - median)
    median_deviation = np.median(demedianed)

    # The test statistic is infinite when the median is zero,
    # so it becomes super sensitive. We play it safe and skip when this happens.
    if median_deviation == 0:
        return scores

    threshold = sigma_value * 2
    for index in range(0, len(y_np_array)):
        score = 0
        if not np.isnan(demedianed[index]) and demedianed[index] != 0 and np.isfinite(demedianed[index]):
            test_statistic = demedianed[index] / median_deviation
            # Completely arbitary...triggers if the median deviation is
            # 6 times bigger than the median
            if test_statistic > threshold:
                score = 1
        scores[index] = score

    return scores


def median_absolute_deviation(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value, tail_avgs):
    """
    A timeseries is anomalous if the deviation of its latest datapoint with
    respect to the median is X times larger than the median of deviations.
    """
    # logger.info('Running ' + str(get_function_name()))

    scores = []
    usenumba = False
    if usenumba:
        try:
            scores = numba_median_absolute_deviation(Y, sigma_value)
            if len(scores) > 0:
                scores = [float(score[0]) for score in scores]
        except:
            traceback_format_exc_string = traceback.format_exc()
            algorithm_name = str(get_function_name())
            record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
    #        return scores
    if not scores:
        try:
            median = series.median()
            demedianed = np.abs(series - median)
            median_deviation = demedianed.median()
        except:
            traceback_format_exc_string = traceback.format_exc()
            algorithm_name = str(get_function_name())
            record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
        # The test statistic is infinite when the median is zero,
        # so it becomes super sensitive. We play it safe and skip when this happens.
        if median_deviation == 0:
            return scores

        threshold = sigma_value * 2
        for index in range(0, len(Y)):
            score = 0
            if not np.isnan(demedianed[index]) and demedianed[index] != 0:
                test_statistic = demedianed[index] / median_deviation
                # Completely arbitary...triggers if the median deviation is
                # 6 times bigger than the median
                if test_statistic > threshold:
                    score = 1
            scores.append(score)

    return scores


def grubbs(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value, tail_avgs):
    """
    A timeseries is anomalous if the Z score is greater than the Grubb's score.
    """
    scores = []
    try:
        stdDev = series.std()
        if stdDev == 0:
            return scores
        mean = np.mean(series)
        len_series = len(series)
        threshold = scipy.stats.t.isf(.05 / (2 * len_series), len_series - 2)
        threshold_squared = threshold * threshold
        grubbs_score = ((len_series - 1) / np.sqrt(len_series)) * np.sqrt(threshold_squared / (len_series - 2 + threshold_squared))
        for tail_average in tail_avgs:
            score = 0
            z_score = (tail_average - mean) / stdDev
            if z_score > grubbs_score:
                score = 1
            scores.append(score)
        return scores
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
        return scores


@jit(nopython=True, cache=True)
def numba_stddev_from_average(series, t):
    """
    """
    mean = np.mean(series)
    stdDev = np.std(series)
    return abs(t - mean) > 3 * stdDev


def stddev_from_average(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value, tail_avgs):
    """
    A timeseries is anomalous if the absolute value of the average of the latest
    three datapoint minus the moving average is greater than three standard
    deviations of the average. This does not exponentially weight the MA and so
    is better for detecting anomalies with respect to the entire series.
    """
    scores = []
    try:
        mean = np.mean(Y)
        stdDev = np.std(Y)
        for t in tail_avgs:
            score = 0
            if abs(t - mean) > (sigma_value * stdDev):
                score = 1
            scores.append(score)
        return scores
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
        return scores


def stddev_from_moving_average(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value, tail_avgs):
    """
    A timeseries is anomalous if the absolute value of the average of the latest
    three datapoint minus the moving average is greater than three standard
    deviations of the moving average. This is better for finding anomalies with
    respect to the short term trends.
    """
    scores = []
    try:
        expAverage = pandas.Series.ewm(series, ignore_na=False, min_periods=0, adjust=True, com=50).mean()
        stdDev = pandas.Series.ewm(series, ignore_na=False, min_periods=0, adjust=True, com=50).std(bias=False)
        for index, y in enumerate(Y):
            score = 0
            if abs(series.iat[index] - expAverage.iat[index]) > (sigma_value * stdDev.iat[index]):
                score = 1
            scores.append(score)
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
        return scores
    return scores


@jit(nopython=True, cache=True)
def numba_mean_subtraction_cumulation(y_np_array, sigma_value):
    """
    This is a numba implementation of mean_subtraction_cumulation, it speeds up
    the computation on a 1000 timeseries from 7.042794 seconds to
    0.041275 seconds.
    """
    scores = np.zeros((len(y_np_array), 1), dtype=np.float64)

    series_out = y_np_array - np.mean(y_np_array[0:len(y_np_array) - 1])
    stdDev = np.std(series_out[0:len(series_out) - 1])
    for index in range(0, len(y_np_array)):
        score = 0
        if abs(series_out[index]) > (sigma_value * stdDev):
            score = 1
        scores[index] = score
    return scores

def mean_subtraction_cumulation(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value, tail_avgs):
    """
    A timeseries is anomalous if the value of the next datapoint in the
    series is farther than three standard deviations out in cumulative terms
    after subtracting the mean from each data point.
    """
    scores = []
    try:

#        y_np_array = np.array([x[1] if x[1] else 0 for x in timeseries], dtype=np.float64)
#        scores_array = numba_mean_subtraction_cumulation(y_np_array, sigma_value)
#        if len(scores_array) > 0:
#            score_array = scores_array[0].tolist()
#            scores = [float(i[0]) for i in score_array]

        use_series = pandas.Series([x[1] if x[1] else 0 for x in timeseries])
        use_series = use_series - use_series[0:len(use_series) - 1].mean()
        stdDev = use_series[0:len(use_series) - 1].std()
        for index, y in enumerate(Y):
            score = 0
            if abs(series.iat[index]) > (sigma_value * stdDev):
                score = 1
            scores.append(score)

    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
        return scores
    return scores


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


def least_squares(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value, tail_avgs):
    """
    A timeseries is anomalous if the average of the last three datapoints
    on a projected least squares model is greater than three sigma.
    """
    scores = []
    try:
        A = np.vstack([X, np.ones(len(X))]).T
        m, c = np.linalg.lstsq(A, Y, rcond=-1)[0]

        errors = numba_projected_errors(X, Y, m, c)

        if len(errors) < sigma_value:
            return [0 for item in timeseries]

        # Change from using scipy/numpy std which calculates the population
        # standard deviation to using pandas.std which calculates the sample
        # standard deviation which is more appropriate for time series data
        # std_dev = scipy.std(errors)
        series = pandas.Series(x for x in errors)
        std_dev = series.std()

        # t = (errors[-1] + errors[-2] + errors[-3]) / 3
        for index, y in enumerate(Y):
            score = 0
            start_index = index - sigma_value
            t = sum(errors[start_index:index]) / sigma_value
            if ((abs(t) > std_dev) * sigma_value) and (round(std_dev) != 0) and (round(t) != 0):
                score = 1
            scores.append(score)
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
        return scores
    return scores


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


def histogram_bins(current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value, tail_avgs):
    """
    A timeseries is anomalous if the average of the last three datapoints falls
    into a histogram bin with less than 20 other datapoints (you'll need to tweak
    that number depending on your data)

    Returns: the size of the bin which contains the tail_avg. Smaller bin size
    means more anomalous.
    """
    scores = []
    try:
        h = numba_histogram(Y, bins=15)
        bins = h[1]
        for index, value in enumerate(Y.tolist()):
            t = tail_avgs[index]
            score = 0
            for bin_index, bin_size in enumerate(h[0]):
                if bin_size <= 20:
                    # Is it in the first bin?
                    if bin_index == 0:
                        if t <= bins[0]:
                            score = 1
                    # Is it in the current bin?
                    elif t >= bins[bin_index] and t < bins[bin_index + 1]:
                        score = 1
            scores.append(score)
        return scores
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(current_skyline_app, algorithm_name, traceback_format_exc_string)
        return []



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


# @added 20260608 - Feature #4736: custom_algorithms - sigma
# Added this oneshot implementation that is used if the sigma anomaly_window is
# > 20
def run_sigma_oneshot_algorithms(current_skyline_app, timeseries, sigma_value, consensus, anomaly_window):
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
    :return: anomalous, anomalies
    :rtype: (boolean, dict)

    """

    anomalous = None
    anomalyScore = 0.0
    consensus_scores = []
    anomalies = {}

    timings = {}
    begin = time()

    if len(timeseries) == 0:
        return anomalous, anomalyScore, anomalies

    if len(timeseries) < MIN_TOLERABLE_LENGTH:
        return anomalous, anomalyScore, anomalies

    # Get rid of boring series
    if len(set(item[1] for item in timeseries[-MAX_TOLERABLE_BOREDOM:])) == BOREDOM_SET_SIZE:
        return anomalous, anomalyScore, anomalies

    USE_ALGORITHMS = [
        'histogram_bins',
#        'first_hour_average',
        'stddev_from_average',
        'grubbs',
#        'ks_test',
        'mean_subtraction_cumulation',
        'median_absolute_deviation',
        'stddev_from_moving_average',
        'least_squares'
    ]

    # This optimisation is added to save microseconds on interpolating the
    # pandas series individually in the median_absolute_deviation,
    # mean_subtraction_cumulation, stddev_from_moving_average and
    # stddev_from_average algorithms.  The pandas series is interpolated once
    # here and passed to all the algorithms.
    series = pandas.Series(x[1] for x in timeseries)

    # numba optimisations
    X = np.array([x[0] for x in timeseries], dtype=np.float64)
    Y = np.array([x[1] for x in timeseries], dtype=np.float64)

    tail_avgs =  bn.move_mean(Y, window=3, min_count=1)

    ts_diffs = np.diff(X)
    resolution_counts = np.unique(ts_diffs, return_counts=True)
    second_order_resolution_seconds = resolution_counts[0][np.argmax(resolution_counts[1])]

    algorithms_results = {}
    try:
        for algorithm in USE_ALGORITHMS:
            scores = []
            run_algorithm = []
            run_algorithm.append(algorithm)
            start = time()
            try:
                scores = [globals()[test_algorithm](current_skyline_app, X, Y, timeseries, second_order_resolution_seconds, series, sigma_value, tail_avgs) for test_algorithm in run_algorithm]
            except:
                # logger.error('%s failed' % (algorithm))
                scores = []
            #print(f"algorithm: {algorithm}, type(scores): {type(scores)}, scores: {scores}")
            if scores:
                try:
                    scores = [float(score) for score in scores[0]]
                except TypeError:
                    print('TypeError on scores', scores)
                    continue
    #                try:
    #                    scores = [float(score[0]) for score in scores]
    #                expect:
    #                    pass
            #print(f"algorithm: {algorithm}, type(scores): {type(scores)}, scores: {scores}")
            algorithms_results[algorithm] = scores
            timings[algorithm] = time() - start

        start = time()
        for index, item in enumerate(timeseries):
            triggered_algorithms = []
            for algorithm in USE_ALGORITHMS:
                score = 0
                try:
                    score = algorithms_results[algorithm][index]
                except IndexError:
                    pass                        
                if score > 0:
                    triggered_algorithms.append(algorithm)
            score = 0
            if triggered_algorithms:
                score = len(triggered_algorithms) / len(USE_ALGORITHMS)
            consensus_scores.append(score)
            algorithms_run = {}
            for algorithm in USE_ALGORITHMS:
                bool_algorithm_result = False
                if algorithm in triggered_algorithms:
                    bool_algorithm_result = True
                algorithms_run[algorithm] = bool_algorithm_result

            if len(triggered_algorithms) >= consensus:
                ts = int(item[0])
                anomalies[ts] = {
                    'anomalous': True,
                    'anomalyScore': score,
                    'index': index,
                    'value': item[1],
                    'algorithms_results': algorithms_run,
                    'score': 1,
                }
        timings['consensus'] = time() - start

        anomalies_in_window = len([x for x in consensus_scores[-anomaly_window:] if x >= 1])
        if anomalies_in_window:
            anomalous = True
            anomalyScore = 1.0
        else:
            anomalous = False
            anomalyScore = 0.0
        timings['total'] = time() - begin

        #print(f"timings: {timings}")

    except:
        traceback_format_exc_string = traceback.format_exc()
        record_algorithm_error(current_skyline_app, 'sigma_oneshot', traceback_format_exc_string)
        return anomalous, anomalyScore, anomalies

    return anomalous, anomalyScore, anomalies
