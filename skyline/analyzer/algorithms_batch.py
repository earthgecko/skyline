from __future__ import division
import logging
from time import time
from os import getpid
from timeit import default_timer as timer

import pandas
import numpy as np
import scipy
import statsmodels.api as sm
import traceback

from settings import (
    ALGORITHMS,
    CONSENSUS,
    FULL_DURATION,
    MAX_TOLERABLE_BOREDOM,
    MIN_TOLERABLE_LENGTH,
    BOREDOM_SET_SIZE,
    PANDAS_VERSION,
    RUN_OPTIMIZED_WORKFLOW,
    SKYLINE_TMP_DIR,
    ENABLE_ALGORITHM_RUN_METRICS,
    ENABLE_ALL_ALGORITHMS_RUN_METRICS,
)

from algorithm_exceptions import TooShort, Stale, Boring

skyline_app = 'analyzer_batch'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

try:
    send_algorithm_run_metrics = ENABLE_ALGORITHM_RUN_METRICS
except:
    send_algorithm_run_metrics = False


"""
This is no man's land. Do anything you want in here,
as long as you return a boolean that determines whether the input timeseries is
anomalous or not.

The key here is to return a True or False boolean.

You should use the pythonic except mechanism to ensure any excpetions do not
cause things to halt and the record_algorithm_error utility can be used to
sample any algorithm errors to log.

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
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(algorithm_name, traceback_format_exc_string)
        return None

    # The test statistic is infinite when the median is zero,
    # so it becomes super sensitive. We play it safe and skip when this happens.
    if median_deviation == 0:
        return False

    if PANDAS_VERSION < '0.17.0':
        try:
            test_statistic = demedianed.iget(-1) / median_deviation
        except:
            traceback_format_exc_string = traceback.format_exc()
            algorithm_name = str(get_function_name())
            record_algorithm_error(algorithm_name, traceback_format_exc_string)
            return None
    else:
        try:
            test_statistic = demedianed.iat[-1] / median_deviation
        except:
            traceback_format_exc_string = traceback.format_exc()
            algorithm_name = str(get_function_name())
            record_algorithm_error(algorithm_name, traceback_format_exc_string)
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

    try:
        # @modified 20191011 - Update least_squares & grubbs algorithms by using sample standard deviation PR #124
        #                      Task #3256: Review and test PR 124
        # Change from using scipy/numpy std which calculates the population
        # standard deviation to using pandas.std which calculates the sample
        # standard deviation which is more appropriate for time series data
        # series = scipy.array([x[1] for x in timeseries])
        # stdDev = scipy.std(series)
        series = pandas.Series(x[1] for x in timeseries)
        stdDev = series.std()

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
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(algorithm_name, traceback_format_exc_string)
        return None


def first_hour_average(timeseries):
    """
    Calcuate the simple average over one hour, FULL_DURATION seconds ago.
    A timeseries is anomalous if the average of the last three datapoints
    are outside of three standard deviations of this value.
    """

    try:
        last_hour_threshold = time() - (FULL_DURATION - 3600)
        series = pandas.Series([x[1] for x in timeseries if x[0] < last_hour_threshold])
        mean = (series).mean()
        stdDev = (series).std()
        t = tail_avg(timeseries)

        return abs(t - mean) > 3 * stdDev
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(algorithm_name, traceback_format_exc_string)
        return None


def stddev_from_average(timeseries):
    """
    A timeseries is anomalous if the absolute value of the average of the latest
    three datapoint minus the moving average is greater than three standard
    deviations of the average. This does not exponentially weight the MA and so
    is better for detecting anomalies with respect to the entire series.
    """

    try:
        series = pandas.Series([x[1] for x in timeseries])
        mean = series.mean()
        stdDev = series.std()
        t = tail_avg(timeseries)

        return abs(t - mean) > 3 * stdDev
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(algorithm_name, traceback_format_exc_string)
        return None


def stddev_from_moving_average(timeseries):
    """
    A timeseries is anomalous if the absolute value of the average of the latest
    three datapoint minus the moving average is greater than three standard
    deviations of the moving average. This is better for finding anomalies with
    respect to the short term trends.
    """
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
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(algorithm_name, traceback_format_exc_string)
        return None


def mean_subtraction_cumulation(timeseries):
    """
    A timeseries is anomalous if the value of the next datapoint in the
    series is farther than three standard deviations out in cumulative terms
    after subtracting the mean from each data point.
    """

    try:
        series = pandas.Series([x[1] if x[1] else 0 for x in timeseries])
        series = series - series[0:len(series) - 1].mean()
        stdDev = series[0:len(series) - 1].std()
        # @modified 20161228 - Feature #1828: ionosphere - mirage Redis data features
        # This expAverage is unused
        # if PANDAS_VERSION < '0.18.0':
        #     expAverage = pandas.stats.moments.ewma(series, com=15)
        # else:
        #     expAverage = pandas.Series.ewm(series, ignore_na=False, min_periods=0, adjust=True, com=15).mean()

        if PANDAS_VERSION < '0.17.0':
            return abs(series.iget(-1)) > 3 * stdDev
        else:
            return abs(series.iat[-1]) > 3 * stdDev
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(algorithm_name, traceback_format_exc_string)
        return None


def least_squares(timeseries):
    """
    A timeseries is anomalous if the average of the last three datapoints
    on a projected least squares model is greater than three sigma.
    """

    try:
        x = np.array([t[0] for t in timeseries])
        y = np.array([t[1] for t in timeseries])
        A = np.vstack([x, np.ones(len(x))]).T
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
        m, c = np.linalg.lstsq(A, y, rcond=-1)[0]

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

        # @modified 20191011 - Update least_squares & grubbs algorithms by using sample standard deviation PR #124
        #                      Task #3256: Review and test PR 124
        # Change from using scipy/numpy std which calculates the population
        # standard deviation to using pandas.std which calculates the sample
        # standard deviation which is more appropriate for time series data
        # std_dev = scipy.std(errors)
        series = pandas.Series(x for x in errors)
        std_dev = series.std()

        t = (errors[-1] + errors[-2] + errors[-3]) / 3

        return abs(t) > std_dev * 3 and round(std_dev) != 0 and round(t) != 0
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(algorithm_name, traceback_format_exc_string)
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
        series = scipy.array([x[1] for x in timeseries])
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
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(algorithm_name, traceback_format_exc_string)
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
        reference = scipy.array([x[1] for x in timeseries if x[0] >= hour_ago and x[0] < ten_minutes_ago])
        probe = scipy.array([x[1] for x in timeseries if x[0] >= ten_minutes_ago])

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
        record_algorithm_error(algorithm_name, traceback_format_exc_string)
        return None

    return False


"""
THE END of NO MAN'S LAND


THE START of UTILITY FUNCTIONS

"""


def get_function_name():
    """
    This is a utility function is used to determine what algorithm is reporting
    an algorithm error when the record_algorithm_error is used.
    """
    return traceback.extract_stack(None, 2)[0][2]


def record_algorithm_error(algorithm_name, traceback_format_exc_string):
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
        SKYLINE_TMP_DIR, skyline_app, str(current_process_pid), algorithm_name)
    try:
        with open(algorithm_error_file, 'w') as f:
            f.write(str(traceback_format_exc_string))
        return True
    except:
        return False


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


def determine_array_median(array):
    """
    Determine the median of the values in an array
    """
    try:
        np_array = np.array(array)
    except:
        return False

    # logger.info('Running ' + str(get_function_name()))
    try:
        array_median = np.median(np_array)
        return array_median
    except:
        return False

    return False


# @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
def negatives_present(timeseries):
    """
    Determine if there are negative number present in a time series
    """

    try:
        np_array = pandas.Series([x[1] for x in timeseries])
    except:
        return False
    try:
        lowest_value = np.min(np_array)
    except:
        return False
    if lowest_value < 0:
        negatives = []
        try:
            for ts, v in timeseries:
                try:
                    if v < 0:
                        negatives.append((ts, v))
                except:
                    pass
        except:
            pass
        return negatives
    return False


def run_selected_batch_algorithm(timeseries, metric_name, run_negatives_present):
    """
    Filter timeseries and run selected algorithm.
    """

    try:
        from settings import BATCH_PROCESSING_STALE_PERIOD
        STALE_PERIOD = BATCH_PROCESSING_STALE_PERIOD
    except:
        STALE_PERIOD = 86400

    # Get rid of short series
    if len(timeseries) < MIN_TOLERABLE_LENGTH:
        raise TooShort()

    # Get rid of stale series
    if time() - timeseries[-1][0] > STALE_PERIOD:
        raise Stale()

    # Get rid of boring series
    if len(set(item[1] for item in timeseries[-MAX_TOLERABLE_BOREDOM:])) == BOREDOM_SET_SIZE:
        raise Boring()

    # RUN_OPTIMIZED_WORKFLOW - replaces the original ensemble method:
    # ensemble = [globals()[algorithm](timeseries) for algorithm in ALGORITHMS]
    # which runs all timeseries through all ALGORITHMS
    final_ensemble = []
    number_of_algorithms_triggered = 0
    number_of_algorithms_run = 0
    number_of_algorithms = len(ALGORITHMS)
    maximum_false_count = number_of_algorithms - CONSENSUS + 1
    # logger.info('the maximum_false_count is %s, above which CONSENSUS cannot be achieved' % (str(maximum_false_count)))
    consensus_possible = True

    time_all_algorithms = False

    algorithm_tmp_file_prefix = '%s/%s.' % (SKYLINE_TMP_DIR, skyline_app)

    # @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
    # Added negatives_found
    negatives_found = False

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
                algorithm_result = [globals()[test_algorithm](timeseries) for test_algorithm in run_algorithm]
            except:
                # logger.error('%s failed' % (algorithm))
                algorithm_result = [None]

            if send_algorithm_run_metrics:
                end = timer()
                with open(algorithm_count_file, 'a') as f:
                    f.write('1\n')
                with open(algorithm_timings_file, 'a') as f:
                    f.write('%.6f\n' % (end - start))
        else:
            algorithm_result = [False]
            # logger.info('CONSENSUS NOT ACHIEVABLE - skipping %s' % (str(algorithm)))

        if algorithm_result.count(True) == 1:
            result = True
            number_of_algorithms_triggered += 1
            # logger.info('algorithm %s triggerred' % (str(algorithm)))
        elif algorithm_result.count(False) == 1:
            result = False
        elif algorithm_result.count(None) == 1:
            result = None
        else:
            result = False

        final_ensemble.append(result)

        if not RUN_OPTIMIZED_WORKFLOW:
            continue

        if time_all_algorithms:
            continue

        if ENABLE_ALL_ALGORITHMS_RUN_METRICS:
            continue

        # true_count = final_ensemble.count(True)
        # false_count = final_ensemble.count(False)
        # logger.info('current false_count %s' % (str(false_count)))

        if final_ensemble.count(False) >= maximum_false_count:
            consensus_possible = False
            # logger.info('CONSENSUS cannot be reached as %s algorithms have already not been triggered' % (str(false_count)))
            # skip_algorithms_count = number_of_algorithms - number_of_algorithms_run
            # logger.info('skipping %s algorithms' % (str(skip_algorithms_count)))

    # logger.info('final_ensemble: %s' % (str(final_ensemble)))

    try:
        # ensemble = [globals()[algorithm](timeseries) for algorithm in ALGORITHMS]
        ensemble = final_ensemble

        threshold = len(ensemble) - CONSENSUS
        if ensemble.count(False) <= threshold:

            # @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
            # Only run a negatives_present check if it is anomalous, there
            # is no need to check unless it is related to an anomaly
            if run_negatives_present:
                try:
                    negatives_found = negatives_present(timeseries)
                except:
                    logger.error('Algorithm error: negatives_present :: %s' % traceback.format_exc())
                    negatives_found = False

            # @modified 20200425 - Feature #3508: ionosphere.untrainable_metrics
            # return True, ensemble, timeseries[-1][1]
            return True, ensemble, timeseries[-1][1], negatives_found

        # @modified 20200425 - Feature #3508: ionosphere.untrainable_metrics
        # return False, ensemble, timeseries[-1][1]
        return False, ensemble, timeseries[-1][1], negatives_found
    except:
        logger.error('Algorithm error: %s' % traceback.format_exc())
        # @modified 20200425 - Feature #3508: ionosphere.untrainable_metrics
        # return False, [], 1
        return False, [], 1, negatives_found
