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
    # @added 20200607 - Feature #3566: custom_algorithms
    FULL_NAMESPACE,
)

from algorithm_exceptions import TooShort, Stale, Boring

# @added 20200607 - Feature #3566: custom_algorithms
try:
    from settings import CUSTOM_ALGORITHMS
except:
    CUSTOM_ALGORITHMS = None
try:
    from settings import DEBUG_CUSTOM_ALGORITHMS
except:
    DEBUG_CUSTOM_ALGORITHMS = False

run_custom_algorithm_on_timeseries = None
get_custom_algorithms_to_run = None
if CUSTOM_ALGORITHMS:
    try:
        from custom_algorithms_to_run import get_custom_algorithms_to_run
    except:
        get_custom_algorithms_to_run = None
    try:
        from custom_algorithms import run_custom_algorithm_on_timeseries
    except:
        run_custom_algorithm_on_timeseries = None

# @added 20200817 - Feature #3684: ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS
#                   Feature #3650: ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
#                   Feature #3480: batch_processing
# Allow for custom durations on namespaces
ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS = None
try:
    from settings import ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
except:
    ROOMBA_DO_NOT_PROCESS_BATCH_METRICS = False
if ROOMBA_DO_NOT_PROCESS_BATCH_METRICS:
    try:
        from settings import ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS
    except:
        ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS = None

# @added 20211127 - Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
BATCH_METRICS_CUSTOM_FULL_DURATIONS = {}
try:
    from settings import BATCH_METRICS_CUSTOM_FULL_DURATIONS
except:
    BATCH_METRICS_CUSTOM_FULL_DURATIONS = {}

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


# @modified 20200817 - Feature #3684: ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS
# Added use_full_duration to all algorithms
def tail_avg(timeseries, use_full_duration):
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


def median_absolute_deviation(timeseries, use_full_duration):
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


def grubbs(timeseries, use_full_duration):
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
        # @modified 20200904 - Feature #3684: ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS
        # Added use_full_duration
        tail_average = tail_avg(timeseries, use_full_duration)
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


def first_hour_average(timeseries, use_full_duration):
    """
    Calcuate the simple average over one hour, use_full_duration seconds ago.
    A timeseries is anomalous if the average of the last three datapoints
    are outside of three standard deviations of this value.
    """

    try:
        # @modified 20200817 - Feature #3684: ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS
        # Use use_full_duration
        # last_hour_threshold = time() - (FULL_DURATION - 3600)
        # @modified 20211127 - Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
        # Calculate the "equivalent" of the last hour and handle daily frequency
        # data
        # last_hour_threshold = time() - (use_full_duration - 3600)
        last_hour_threshold = timeseries[-1][0] - (use_full_duration - 3600)
        # Handle daily data
        resolution = (timeseries[-1][0] - timeseries[-2][0])
        if resolution > 80000 and resolution < 90000:
            last_hour_threshold = timeseries[-1][0] - ((resolution * 7) - resolution)

        series = pandas.Series([x[1] for x in timeseries if x[0] < last_hour_threshold])
        mean = (series).mean()
        stdDev = (series).std()
        # @modified 20200904 - Feature #3684: ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS
        # Added use_full_duration
        t = tail_avg(timeseries, use_full_duration)

        return abs(t - mean) > 3 * stdDev
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(algorithm_name, traceback_format_exc_string)
        return None


def stddev_from_average(timeseries, use_full_duration):
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
        # @modified 20200904 - Feature #3684: ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS
        # Added use_full_duration
        t = tail_avg(timeseries, use_full_duration)

        return abs(t - mean) > 3 * stdDev
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(algorithm_name, traceback_format_exc_string)
        return None


def stddev_from_moving_average(timeseries, use_full_duration):
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


def mean_subtraction_cumulation(timeseries, use_full_duration):
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


def least_squares(timeseries, use_full_duration):
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


def histogram_bins(timeseries, use_full_duration):
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

        # @modified 20200904 - Feature #3684: ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS
        # Added use_full_duration
        t = tail_avg(timeseries, use_full_duration)
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


def ks_test(timeseries, use_full_duration):
    """
    A timeseries is anomalous if 2 sample Kolmogorov-Smirnov test indicates
    that data distribution for last 10 minutes is different from last hour.
    It produces false positives on non-stationary series so Augmented
    Dickey-Fuller test applied to check for stationarity.
    """

    try:

        # @modified 20211127 - Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
        # Calculate the "equivalent" of hour and handle daily frequency data
        # hour_ago = time() - 3600
        # ten_minutes_ago = time() - 600
        hour_ago = timeseries[-1][0] - 3600
        ten_minutes_ago = timeseries[-1][0] - 600
        # Handle daily data
        resolution = (timeseries[-1][0] - timeseries[-2][0])
        if resolution > 80000 and resolution < 90000:
            hour_ago = timeseries[-1][0] - (86400 * 90)
            ten_minutes_ago = timeseries[-1][0] - (86400 * 30)

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


def determine_median(timeseries, use_full_duration):
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
def negatives_present(timeseries, use_full_duration):
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
        # @modified 20200816 - Feature #3678:  SNAB - anomalyScore
        # Renamed to avoid confusion
        # STALE_PERIOD = int(BATCH_PROCESSING_STALE_PERIOD)
        BATCH_PROCESSING_STALE_PERIOD = int(BATCH_PROCESSING_STALE_PERIOD)
    except:
        BATCH_PROCESSING_STALE_PERIOD = 86400

    # Get rid of short series
    if len(timeseries) < MIN_TOLERABLE_LENGTH:
        raise TooShort()

    # Get rid of stale series
    # @modified 20200816 - Feature #3678:  SNAB - anomalyScore
    # Renamed to avoid confusion
    # if time() - timeseries[-1][0] > BATCH_PROCESSING_STALE_PERIOD:
    if time() - timeseries[-1][0] > BATCH_PROCESSING_STALE_PERIOD:
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

    # @added 20200607 - Feature #3566: custom_algorithms
    algorithms_run = []
    custom_consensus_override = False
    custom_consensus_values = []
    run_3sigma_algorithms = True
    run_3sigma_algorithms_overridden_by = []
    custom_algorithm = None

    # @added 20211125 - Feature #3566: custom_algorithms
    custom_algorithms_run = []
    custom_algorithms_run_results = []

    # @modified 20200817 - Bug #3652: Handle multiple metrics in base_name conversion
    # base_name = metric_name.replace(FULL_NAMESPACE, '', 1)
    if metric_name.startswith(FULL_NAMESPACE):
        base_name = metric_name.replace(FULL_NAMESPACE, '', 1)
    else:
        base_name = metric_name
    if CUSTOM_ALGORITHMS:
        custom_algorithms_to_run = {}
        try:
            custom_algorithms_to_run = get_custom_algorithms_to_run(skyline_app, base_name, CUSTOM_ALGORITHMS, DEBUG_CUSTOM_ALGORITHMS)
            if DEBUG_CUSTOM_ALGORITHMS:
                if custom_algorithms_to_run:
                    logger.debug('algorithms :: debug :: custom algorithms ARE RUN on %s' % (str(base_name)))
        except:
            logger.error('error :: get_custom_algorithms_to_run :: %s' % traceback.format_exc())
            custom_algorithms_to_run = {}
        for custom_algorithm in custom_algorithms_to_run:
            if consensus_possible:
                algorithm = custom_algorithm
                debug_logging = False
                try:
                    debug_logging = custom_algorithms_to_run[custom_algorithm]['debug_logging']
                except:
                    debug_logging = False
                if DEBUG_CUSTOM_ALGORITHMS:
                    debug_logging = True

                # @added 20211125 - Feature #3566: custom_algorithms
                run_before_3sigma = False
                try:
                    run_before_3sigma = custom_algorithms_to_run[custom_algorithm]['run_before_3sigma']
                except:
                    run_before_3sigma = False
                if not run_before_3sigma:
                    if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                        logger.debug('debug :: algorithms :: not running custom algorithm %s before 3-sigma' % (
                            str(algorithm)))
                    continue

                if send_algorithm_run_metrics:
                    algorithm_count_file = '%s%s.count' % (algorithm_tmp_file_prefix, algorithm)
                    algorithm_timings_file = '%s%s.timings' % (algorithm_tmp_file_prefix, algorithm)
                run_algorithm = []
                run_algorithm.append(algorithm)
                number_of_algorithms += 1
                number_of_algorithms_run += 1
                if send_algorithm_run_metrics:
                    start = timer()
                if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                    logger.debug('debug :: algorithms :: running custom algorithm %s on %s' % (
                        str(algorithm), str(base_name)))
                    start_debug_timer = timer()
                # @modified 20211124 - linting
                # run_custom_algorithm_on_timeseries = None
                try:
                    # @modified 20211124 - linting
                    # from custom_algorithms import run_custom_algorithm_on_timeseries
                    if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                        logger.debug('debug :: algorithms :: loaded run_custom_algorithm_on_timeseries')
                except:
                    if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                        logger.error(traceback.format_exc())
                        logger.error('error :: algorithms :: failed to load run_custom_algorithm_on_timeseries')
                result = None
                anomalyScore = None
                if run_custom_algorithm_on_timeseries:

                    # @added 20211125 - Feature #3566: custom_algorithms
                    custom_algorithms_run.append(custom_algorithm)

                    try:
                        result, anomalyScore = run_custom_algorithm_on_timeseries(skyline_app, getpid(), base_name, timeseries, custom_algorithm, custom_algorithms_to_run[custom_algorithm], DEBUG_CUSTOM_ALGORITHMS)
                        algorithm_result = [result]
                        if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                            logger.debug('debug :: algorithms :: run_custom_algorithm_on_timeseries run with result - %s, anomalyScore - %s' % (
                                str(result), str(anomalyScore)))
                        # @added 20211125 - Feature #3566: custom_algorithms
                        custom_algorithms_run_results.append(result)
                    except:
                        if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                            logger.error(traceback.format_exc())
                            logger.error('error :: algorithms :: failed to run custom_algorithm %s on %s' % (
                                custom_algorithm, base_name))
                        result = None
                        algorithm_result = [None]
                        # @added 20211125 - Feature #3566: custom_algorithms
                        custom_algorithms_run_results.append(False)
                else:
                    if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                        logger.error('error :: debug :: algorithms :: run_custom_algorithm_on_timeseries was not loaded so was not run')
                if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                    end_debug_timer = timer()
                    logger.debug('debug :: algorithms :: ran custom algorithm %s on %s with result of (%s, %s) in %.6f seconds' % (
                        str(algorithm), str(base_name),
                        str(result), str(anomalyScore),
                        (end_debug_timer - start_debug_timer)))
                algorithms_run.append(algorithm)
                if send_algorithm_run_metrics:
                    end = timer()
                    with open(algorithm_count_file, 'a') as f:
                        f.write('1\n')
                    with open(algorithm_timings_file, 'a') as f:
                        f.write('%.6f\n' % (end - start))
            else:
                algorithm_result = [None]
                algorithms_run.append(algorithm)

            if algorithm_result.count(True) == 1:
                result = True
                number_of_algorithms_triggered += 1
            elif algorithm_result.count(False) == 1:
                result = False
            elif algorithm_result.count(None) == 1:
                result = None
            else:
                result = False
            final_ensemble.append(result)
            custom_consensus = None
            algorithms_allowed_in_consensus = []
            # @added 20200605 - Feature #3566: custom_algorithms
            # Allow only single or multiple custom algorithms to run and allow
            # the a custom algorithm to specify not to run 3sigma aglorithms
            custom_run_3sigma_algorithms = True
            try:
                custom_run_3sigma_algorithms = custom_algorithms_to_run[custom_algorithm]['run_3sigma_algorithms']
            except:
                custom_run_3sigma_algorithms = True
            if not custom_run_3sigma_algorithms and result:
                run_3sigma_algorithms = False
                run_3sigma_algorithms_overridden_by.append(custom_algorithm)
                if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                    logger.debug('debug :: algorithms :: run_3sigma_algorithms is False on %s for %s' % (
                        custom_algorithm, base_name))
            if result:
                try:
                    custom_consensus = custom_algorithms_to_run[custom_algorithm]['consensus']
                    if custom_consensus == 0:
                        custom_consensus = int(CONSENSUS)
                    else:
                        custom_consensus_values.append(custom_consensus)
                except:
                    custom_consensus = int(CONSENSUS)
                try:
                    algorithms_allowed_in_consensus = custom_algorithms_to_run[custom_algorithm]['algorithms_allowed_in_consensus']
                except:
                    algorithms_allowed_in_consensus = []
                if custom_consensus == 1:
                    consensus_possible = False
                    custom_consensus_override = True
                    logger.info('algorithms :: overidding the CONSENSUS as custom algorithm %s overides on %s' % (
                        str(algorithm), str(base_name)))
                # TODO - figure out how to handle consensus overrides if
                #        multiple custom algorithms are used
    if DEBUG_CUSTOM_ALGORITHMS:
        if not run_3sigma_algorithms:
            logger.debug('algorithms :: not running 3 sigma algorithms')
        if len(run_3sigma_algorithms_overridden_by) > 0:
            logger.debug('algorithms :: run_3sigma_algorithms overridden by %s' % (
                str(run_3sigma_algorithms_overridden_by)))

    # @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
    # Added negatives_found
    negatives_found = False

    # @added 20200817 - Feature #3684: ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS
    #                   Feature #3650: ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
    #                   Feature #3480: batch_processing
    #                   Feature #3678:  SNAB - anomalyScore
    # Allow for custom durations on namespaces
    use_full_duration = int(FULL_DURATION) + 0
    if ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS:
        for metric_namespace, custom_full_duration in ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS:
            if metric_namespace in base_name:
                use_full_duration = custom_full_duration
        # @added 20211127 - Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
        if BATCH_METRICS_CUSTOM_FULL_DURATIONS:
            for metric_namespace in list(BATCH_METRICS_CUSTOM_FULL_DURATIONS.keys()):
                if metric_namespace in base_name:
                    use_full_duration = BATCH_METRICS_CUSTOM_FULL_DURATIONS[metric_namespace]

    detect_drop_off_cliff_trigger = False

    # @added 20211125 - Feature #3566: custom_algorithms
    # If custom_algorithms were run and did not trigger reset the consensus
    if consensus_possible and run_3sigma_algorithms:
        if len(custom_algorithms_run) > 0:
            none_count = custom_algorithms_run_results.count(None)
            false_count = custom_algorithms_run_results.count(False)
            not_anomalous_count = none_count + false_count
            if len(custom_algorithms_run) == not_anomalous_count:
                custom_consensus_override = False
                custom_consensus = int(CONSENSUS)
                final_ensemble = []
                if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                    logger.debug('debug :: algorithms :: reset final_ensemble, custom_consensus and custom_consensus_override after custom_algorithms all calcuated False')

    for algorithm in ALGORITHMS:
        # @modified 20200607 - Feature #3566: custom_algorithms
        # Added run_3sigma_algorithms to allow only single or multiple custom
        # algorithms to run and allow the a custom algorithm to specify not to
        # run 3sigma aglorithms.
        # if consensus_possible:
        if consensus_possible and run_3sigma_algorithms:
            if send_algorithm_run_metrics:
                algorithm_count_file = '%s%s.count' % (algorithm_tmp_file_prefix, algorithm)
                algorithm_timings_file = '%s%s.timings' % (algorithm_tmp_file_prefix, algorithm)
            run_algorithm = []
            run_algorithm.append(algorithm)
            number_of_algorithms_run += 1
            if send_algorithm_run_metrics:
                start = timer()
            try:
                # @added 20200817 - Feature #3684: ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS
                #                   Feature #3650: ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
                #                   Feature #3480: batch_processing
                #                   Feature #3678:  SNAB - anomalyScore
                # Allow for custom durations on namespaces
                # algorithm_result = [globals()[test_algorithm](timeseries) for test_algorithm in run_algorithm]
                algorithm_result = [globals()[test_algorithm](timeseries, use_full_duration) for test_algorithm in run_algorithm]
                if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                    logger.debug('debug :: algorithms :: ran 3-sigma algorithms on %s with algorithm_result: %s' % (
                        str(base_name), str(algorithm_result)))
            except:
                # logger.error('%s failed' % (algorithm))
                algorithm_result = [None]

            # @added 20200607 - Feature #3566: custom_algorithms
            algorithms_run.append(algorithm)

            if send_algorithm_run_metrics:
                end = timer()
                with open(algorithm_count_file, 'a') as f:
                    f.write('1\n')
                with open(algorithm_timings_file, 'a') as f:
                    f.write('%.6f\n' % (end - start))
        else:
            algorithm_result = [None]
            algorithms_run.append(algorithm)

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

        # @modified 20200607 - Feature #3566: custom_algorithms
        # threshold = len(ensemble) - CONSENSUS
        if custom_consensus_override:
            threshold = len(ensemble) - 1
        else:
            threshold = len(ensemble) - CONSENSUS

        # @added 20220113 - Feature #3566: custom_algorithms
        #                   Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
        # With the addition of the custom_consensus_override and the possibility
        # of the ensemble list being filled with None value when TooShort, etc
        # handle if the ensemble list is all None and do not return anomalous as
        # True
        if ensemble.count(None) == len(ensemble):
            return False, ensemble, timeseries[-1][1], negatives_found, algorithms_run, number_of_algorithms

        # @modified 20220113 - Feature #3566: custom_algorithms
        #                      Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
        # if ensemble.count(False) <= threshold:
        if ensemble.count(False) <= threshold and ensemble.count(False) > 0:

            # @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
            # Only run a negatives_present check if it is anomalous, there
            # is no need to check unless it is related to an anomaly
            if run_negatives_present:
                try:
                    # @added 20200817 - Feature #3684: ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS
                    #                   Feature #3650: ROOMBA_DO_NOT_PROCESS_BATCH_METRICS
                    #                   Feature #3480: batch_processing
                    #                   Feature #3678:  SNAB - anomalyScore
                    # Allow for custom durations on namespaces
                    # negatives_found = negatives_present(timeseries)
                    negatives_found = negatives_present(timeseries, use_full_duration)
                except:
                    logger.error('Algorithm error: negatives_present :: %s' % traceback.format_exc())
                    negatives_found = False

            # @modified 20200425 - Feature #3508: ionosphere.untrainable_metrics
            # return True, ensemble, timeseries[-1][1]
            # @modified 20200607 - Feature #3566: custom_algorithms
            # Added algorithms_run
            # return True, ensemble, timeseries[-1][1], negatives_found
            # @modified 20200815 - Feature #3678: SNAB - anomalyScore
            # Added the number_of_algorithms to calculate anomalyScore from
            # return True, ensemble, timeseries[-1][1], negatives_found, algorithms_run
            return True, ensemble, timeseries[-1][1], negatives_found, algorithms_run, number_of_algorithms

        # @modified 20200425 - Feature #3508: ionosphere.untrainable_metrics
        # return False, ensemble, timeseries[-1][1]
        # @modified 20200607 - Feature #3566: custom_algorithms
        # Added algorithms_run
        # @modified 20200815 - Feature #3678: SNAB - anomalyScore
        # Added the number_of_algorithms to calculate anomalyScore from
        # return False, ensemble, timeseries[-1][1], negatives_found, algorithms_run
        return False, ensemble, timeseries[-1][1], negatives_found, algorithms_run, number_of_algorithms
    except:
        logger.error('Algorithm error: %s' % traceback.format_exc())
        # @modified 20200425 - Feature #3508: ionosphere.untrainable_metrics
        # return False, [], 1
        # @modified 20200607 - Feature #3566: custom_algorithms
        # Added algorithms_run
        # return False, ensemble, timeseries[-1][1], negatives_found, algorithms_run
        # @modified 20200815 - Feature #3678: SNAB - anomalyScore
        # Added the number_of_algorithms to calculate anomalyScore from
        # return False, [], 1, negatives_found, algorithms_run
        return False, [], 1, negatives_found, algorithms_run, 0
