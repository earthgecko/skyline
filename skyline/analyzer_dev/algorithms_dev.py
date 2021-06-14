from __future__ import division
import pandas
import numpy as np
import scipy
import statsmodels.api as sm
import traceback
import logging
from time import time
import os.path
import sys
from timeit import default_timer as timer
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

try:
    from settings import (
        ALGORITHMS,
        CONSENSUS,
        FULL_DURATION,
        MAX_TOLERABLE_BOREDOM,
        MIN_TOLERABLE_LENGTH,
        STALE_PERIOD,
        REDIS_SOCKET_PATH,
        ENABLE_SECOND_ORDER,
        BOREDOM_SET_SIZE,
        PANDAS_VERSION,
        RUN_OPTIMIZED_WORKFLOW,
        SKYLINE_TMP_DIR,
        ENABLE_ALGORITHM_RUN_METRICS,
        ENABLE_ALL_ALGORITHMS_RUN_METRICS,
        REDIS_PASSWORD,
    )
except:
    print('failed to import expected settings' + traceback.format_exc())
    print(traceback.format_exc())

from algorithm_exceptions import *

skyline_app = 'analyzer_dev'
skyline_app_logger = skyline_app + 'Log'
logger = logging.getLogger(skyline_app_logger)

if ENABLE_SECOND_ORDER:
    from redis import StrictRedis
    from msgpack import unpackb, packb
    # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
    if REDIS_PASSWORD:
        redis_conn = StrictRedis(password=REDIS_PASSWORD, unix_socket_path=REDIS_SOCKET_PATH)
    else:
        redis_conn = StrictRedis(unix_socket_path=REDIS_SOCKET_PATH)

try:
    send_algorithm_run_metrics = ENABLE_ALGORITHM_RUN_METRICS
except:
    send_algorithm_run_metrics = False


def get_function_name():
    return traceback.extract_stack(None, 2)[0][2]

"""
This is no man's land. Do anything you want in here,
as long as you return a boolean that determines whether the input
timeseries is anomalous or not.

To add an algorithm, define it here, and add its name to settings.ALGORITHMS.
"""


def tail_avg(timeseries):
    """
    This is a utility function used to calculate the average of the last three
    datapoints in the series as a measure, instead of just the last datapoint.
    It reduces noise, but it also reduces sensitivity and increases the delay
    to detection.
    """
    try:
        t = (timeseries[-1][1] + timeseries[-2][1] + timeseries[-3][1]) / 3
        return t
    except IndexError:
        return timeseries[-1][1]


def median_absolute_deviation(timeseries):
    """
    A timeseries is anomalous if the deviation of its latest datapoint with
    respect to the median is X times larger than the median of deviations.
    """
    # logger.info('Running ' + str(get_function_name()))
    try:
        series = pandas.Series([x[1] for x in timeseries])
        median = series.median()
        demedianed = np.abs(series - median)
        median_deviation = demedianed.median()
    except:
        return None

    # The test statistic is infinite when the median is zero,
    # so it becomes super sensitive. We play it safe and skip when this happens.
    if median_deviation == 0:
        return False

    if PANDAS_VERSION < '0.17.0':
        try:
            test_statistic = demedianed.iget(-1) / median_deviation
        except:
            return None
    else:
        try:
            test_statistic = demedianed.iat[-1] / median_deviation
        except:
            return None

    # Completely arbitary...triggers if the median deviation is
    # 6 times bigger than the median
    if test_statistic > 6:
        return True

    # As per https://github.com/etsy/skyline/pull/104 by @rugger74
    # Although never seen this should return False if not > arbitary_value
    # 20160523 @earthgecko
    return False


def grubbs(timeseries):
    """
    A timeseries is anomalous if the Z score is greater than the Grubb's score.
    """
    # logger.info('Running ' + str(get_function_name()))

    try:
        # @modified 20210420 - Support #4026: Change from scipy array to numpy array
        # Deprecation of scipy.array
        # series = scipy.array([x[1] for x in timeseries])
        series = np.array([x[1] for x in timeseries])
        stdDev = scipy.std(series)

        # Issue #27 - Handle z_score agent.py RuntimeWarning - https://github.com/earthgecko/skyline/issues/27
        # This change avoids spewing warnings on agent.py tests:
        # RuntimeWarning: invalid value encountered in double_scalars
        # If stdDev is 0 division returns nan which is not > grubbs_score so
        # return False here
        if stdDev == 0:
            return False

        mean = np.mean(series)
        tail_average = tail_avg(timeseries)
        z_score = (tail_average - mean) / stdDev
        len_series = len(series)
        threshold = scipy.stats.t.isf(.05 / (2 * len_series), len_series - 2)
        threshold_squared = threshold * threshold
        grubbs_score = ((len_series - 1) / np.sqrt(len_series)) * np.sqrt(threshold_squared / (len_series - 2 + threshold_squared))

        return z_score > grubbs_score
    except:
        return None


def first_hour_average(timeseries):
    """
    Calcuate the simple average over one hour, FULL_DURATION seconds ago.
    A timeseries is anomalous if the average of the last three datapoints
    are outside of three standard deviations of this value.
    """
    # logger.info('Running ' + str(get_function_name()))

    try:
        last_hour_threshold = time() - (FULL_DURATION - 3600)
        series = pandas.Series([x[1] for x in timeseries if x[0] < last_hour_threshold])
        mean = (series).mean()
        stdDev = (series).std()
        t = tail_avg(timeseries)

        return abs(t - mean) > 3 * stdDev
    except:
        return None


def stddev_from_average(timeseries):
    """
    A timeseries is anomalous if the absolute value of the average of the latest
    three datapoint minus the moving average is greater than three standard
    deviations of the average. This does not exponentially weight the MA and so
    is better for detecting anomalies with respect to the entire series.
    """
    # logger.info('Running ' + str(get_function_name()))

    try:
        series = pandas.Series([x[1] for x in timeseries])
        mean = series.mean()
        stdDev = series.std()
        t = tail_avg(timeseries)

        return abs(t - mean) > 3 * stdDev
    except:
        return None


def stddev_from_moving_average(timeseries):
    """
    A timeseries is anomalous if the absolute value of the average of the latest
    three datapoint minus the moving average is greater than three standard
    deviations of the moving average. This is better for finding anomalies with
    respect to the short term trends.
    """
    # logger.info('Running ' + str(get_function_name()))
    try:
        series = pandas.Series([x[1] for x in timeseries])
        if PANDAS_VERSION < '0.18.0':
            expAverage = pandas.stats.moments.ewma(series, com=50)
            stdDev = pandas.stats.moments.ewmstd(series, com=50)
        else:
            expAverage = pandas.Series.ewm(series, ignore_na=False, min_periods=0, adjust=True, com=50).mean()
            stdDev = pandas.Series.ewm(series, ignore_na=False, min_periods=0, adjust=True, com=50).std(bias=False)

        if PANDAS_VERSION < '0.17.0':
            return abs(series.iget(-1) - expAverage.iget(-1)) > 3 * stdDev.iget(-1)
        else:
            return abs(series.iat[-1] - expAverage.iat[-1]) > 3 * stdDev.iat[-1]
# http://stackoverflow.com/questions/28757389/loc-vs-iloc-vs-ix-vs-at-vs-iat
    except:
        return None


def mean_subtraction_cumulation(timeseries):
    """
    A timeseries is anomalous if the value of the next datapoint in the
    series is farther than three standard deviations out in cumulative terms
    after subtracting the mean from each data point.
    """
    # logger.info('Running ' + str(get_function_name()))

    try:
        series = pandas.Series([x[1] if x[1] else 0 for x in timeseries])
        series = series - series[0:len(series) - 1].mean()
        stdDev = series[0:len(series) - 1].std()
        if PANDAS_VERSION < '0.18.0':
            expAverage = pandas.stats.moments.ewma(series, com=15)
        else:
            expAverage = pandas.Series.ewm(series, ignore_na=False, min_periods=0, adjust=True, com=15).mean()

        if PANDAS_VERSION < '0.17.0':
            return abs(series.iget(-1)) > 3 * stdDev
        else:
            return abs(series.iat[-1]) > 3 * stdDev
    except:
        return None


def least_squares(timeseries):
    """
    A timeseries is anomalous if the average of the last three datapoints
    on a projected least squares model is greater than three sigma.
    """
    # logger.info('Running ' + str(get_function_name()))

    try:
        x = np.array([t[0] for t in timeseries])
        y = np.array([t[1] for t in timeseries])
        A = np.vstack([x, np.ones(len(x))]).T
        results = np.linalg.lstsq(A, y)
        residual = results[1]
        m, c = np.linalg.lstsq(A, y)[0]
        errors = []
        # Evaluate append once, not every time in the loop - this gains ~0.020 s on
        # every timeseries potentially @earthgecko #1310
        append_error = errors.append

        # Further a question exists related to performance and accruracy with
        # regards to how many datapoints are in the sample, currently all datapoints
        # are used but this may not be the ideal or most efficient computation or
        # fit for a timeseries... @earthgecko is checking graphite...
        for i, value in enumerate(y):
            projected = m * x[i] + c
            error = value - projected
            # errors.append(error) # @earthgecko #1310
            append_error(error)

        if len(errors) < 3:
            return False

        std_dev = scipy.std(errors)
        t = (errors[-1] + errors[-2] + errors[-3]) / 3

        return abs(t) > std_dev * 3 and round(std_dev) != 0 and round(t) != 0
    except:
        return None


def histogram_bins(timeseries):
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
        series = np.array([x[1] for x in timeseries])

        t = tail_avg(timeseries)
        h = np.histogram(series, bins=15)
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
    except:
        return None


def ks_test(timeseries):
    """
    A timeseries is anomalous if 2 sample Kolmogorov-Smirnov test indicates
    that data distribution for last 10 minutes is different from last hour.
    It produces false positives on non-stationary series so Augmented
    Dickey-Fuller test applied to check for stationarity.
    """

    try:
        hour_ago = time() - 3600
        ten_minutes_ago = time() - 600
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
        return None


def determine_median(timeseries):
    """
    Determine the median of the values in the timeseries
    """

    # logger.info('Running ' + str(get_function_name()))
    try:
        np_array = pandas.Series([x[1] for x in timeseries])
    except:
        return False
    try:
        array_median = np.median(np_array)
        return array_median
    except:
        return False

    return False


def is_anomalously_anomalous(metric_name, ensemble, datapoint):
    """
    This method runs a meta-analysis on the metric to determine whether the
    metric has a past history of triggering. TODO: weight intervals based on datapoint
    """
    # We want the datapoint to avoid triggering twice on the same data
    new_trigger = [time(), datapoint]

    # Get the old history
    raw_trigger_history = redis_conn.get('trigger_history.' + metric_name)
    if not raw_trigger_history:
        redis_conn.set('trigger_history.' + metric_name, packb([(time(), datapoint)]))
        return True

    trigger_history = unpackb(raw_trigger_history)

    # Are we (probably) triggering on the same data?
    if (new_trigger[1] == trigger_history[-1][1] and
            new_trigger[0] - trigger_history[-1][0] <= 300):
                return False

    # Update the history
    trigger_history.append(new_trigger)
    redis_conn.set('trigger_history.' + metric_name, packb(trigger_history))

    # Should we surface the anomaly?
    trigger_times = [x[0] for x in trigger_history]
    intervals = [
        trigger_times[i + 1] - trigger_times[i]
        for i, v in enumerate(trigger_times)
        if (i + 1) < len(trigger_times)
    ]

    series = pandas.Series(intervals)
    mean = series.mean()
    stdDev = series.std()

    return abs(intervals[-1] - mean) > 3 * stdDev


def run_selected_algorithm(timeseries, metric_name):
    """
    Filter timeseries and run selected algorithm.
    """
    # Get rid of short series
    if len(timeseries) < MIN_TOLERABLE_LENGTH:
        raise TooShort()

    # Get rid of stale series
    if time() - timeseries[-1][0] > STALE_PERIOD:
        raise Stale()

    # Get rid of boring series
    if len(set(item[1] for item in timeseries[-MAX_TOLERABLE_BOREDOM:])) == BOREDOM_SET_SIZE:
        raise Boring()

    final_ensemble = []
    number_of_algorithms_triggered = 0
    number_of_algorithms_run = 0
    number_of_algorithms = len(ALGORITHMS)
    maximum_false_count = number_of_algorithms - CONSENSUS + 1
    # logger.info('the maximum_false_count is %s, above which CONSENSUS cannot be achieved' % (str(maximum_false_count)))
    consensus_possible = True
    if skyline_app == 'analyzer_dev':
        time_all_algorithms = True
    else:
        time_all_algorithms = False

    algorithm_tmp_file_prefix = '%s/%s.' % (SKYLINE_TMP_DIR, skyline_app)

    for algorithm in ALGORITHMS:
        if consensus_possible:

            if send_algorithm_run_metrics:
                algorithm_count_file = '%s%s.count' % (algorithm_tmp_file_prefix, algorithm)
                algorithm_timings_file = '%s%s.timings' % (algorithm_tmp_file_prefix, algorithm)

            run_algorithm = []
            run_algorithm.append(algorithm)
            number_of_algorithms_run += 1
            if send_algorithm_run_metrics:
                start = timer()
            try:
                ensemble = [globals()[test_algorithm](timeseries) for test_algorithm in run_algorithm]
            except:
                # logger.error('%s failed' % (algorithm))
                ensemble = [None]

            if send_algorithm_run_metrics:
                end = timer()
                with open(algorithm_count_file, 'a') as f:
                    f.write('1\n')
                with open(algorithm_timings_file, 'a') as f:
                    f.write('%.6f\n' % (end - start))
        else:
            ensemble = [None]
            # logger.info('CONSENSUS NOT ACHIEVABLE - skipping %s' % (str(algorithm)))

        if ensemble.count([True]) == 1:
            result = True
            number_of_algorithms_triggered += 1
            # logger.info('algorithm %s triggerred' % (str(algorithm)))
        elif ensemble.count([False]) == 1:
            result = False
        elif ensemble.count([None]) == 1:
            ensemble = None
        else:
            result = False

        final_ensemble.append(result)

        false_count = final_ensemble.count(False)
        # logger.info('current false_count %s' % (str(false_count)))
        if not time_all_algorithms:
            if final_ensemble.count(False) >= maximum_false_count:
                consensus_possible = False
                # logger.info('CONSENSUS cannot be reached as %s algorithms have already not been triggered' % (str(false_count)))
                skip_algorithms_count = number_of_algorithms - number_of_algorithms_run
                # logger.info('skipping %s algorithms' % (str(skip_algorithms_count)))

    # logger.info('final_ensemble: %s' % (str(final_ensemble)))

    if not consensus_possible:
        return False, final_ensemble, timeseries[-1][1]

    try:
        # ensemble = [globals()[algorithm](timeseries) for algorithm in ALGORITHMS]
        ensemble = final_ensemble

        threshold = len(ensemble) - CONSENSUS
        if ensemble.count(False) <= threshold:
            if ENABLE_SECOND_ORDER:
                if is_anomalously_anomalous(metric_name, ensemble, timeseries[-1][1]):
                    return True, ensemble, timeseries[-1][1]
            else:
                return True, ensemble, timeseries[-1][1]

        return False, ensemble, timeseries[-1][1]
    except:
        logger.error("Algorithm error: " + traceback.format_exc())
        return False, [], 1
