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
from os import getpid

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# @modified 20191115 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
if True:
    from settings import (
        MIRAGE_ALGORITHMS,
        MIRAGE_CONSENSUS,
        # @modified 20191115 - Branch #3262: py3
        # MIRAGE_DATA_FOLDER,
        MIRAGE_ENABLE_SECOND_ORDER,
        PANDAS_VERSION,
        # @modified 20191115 - Branch #3262: py3
        # RUN_OPTIMIZED_WORKFLOW,
        SKYLINE_TMP_DIR,
        REDIS_SOCKET_PATH,
        REDIS_PASSWORD,
        # @added 20200607 - Feature #3566: custom_algorithms
        FULL_NAMESPACE,
    )
    # from algorithm_exceptions import *

skyline_app = 'mirage'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

# @added 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
if MIRAGE_ENABLE_SECOND_ORDER:
    from redis import StrictRedis
    from msgpack import unpackb, packb
    if REDIS_PASSWORD:
        redis_conn = StrictRedis(password=REDIS_PASSWORD, unix_socket_path=REDIS_SOCKET_PATH)
    else:
        redis_conn = StrictRedis(unix_socket_path=REDIS_SOCKET_PATH)

# @added 20200607 - Feature #3566: custom_algorithms
try:
    from settings import CUSTOM_ALGORITHMS
except:
    CUSTOM_ALGORITHMS = None
try:
    from settings import DEBUG_CUSTOM_ALGORITHMS
except:
    DEBUG_CUSTOM_ALGORITHMS = False
if CUSTOM_ALGORITHMS:
    from timeit import default_timer as timer
    try:
        from custom_algorithms_to_run import get_custom_algorithms_to_run
    except:
        get_custom_algorithms_to_run = None
    try:
        from custom_algorithms import run_custom_algorithm_on_timeseries
    except:
        run_custom_algorithm_on_timeseries = None

"""
This is no man's land. Do anything you want in here,
as long as you return a boolean that determines whether the input timeseries is
anomalous or not.

The key here is to return a True or False boolean.

You should use the pythonic except mechanism to ensure any excpetions do not
cause things to halt and the record_algorithm_error utility can be used to
sample any algorithm errors to log.

To add an algorithm, define it here, and add its name to settings.MIRAGE_ALGORITHMS.
"""


def tail_avg(timeseries, second_order_resolution_seconds):
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


def median_absolute_deviation(timeseries, second_order_resolution_seconds):
    """
    A timeseries is anomalous if the deviation of its latest datapoint with
    respect to the median is X times larger than the median of deviations.
    """

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
    else:
        return False


def grubbs(timeseries, second_order_resolution_seconds):
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
        tail_average = tail_avg(timeseries, second_order_resolution_seconds)
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


def first_hour_average(timeseries, second_order_resolution_seconds):
    """
    Calcuate the simple average over one hour, second order resolution seconds ago.
    A timeseries is anomalous if the average of the last three datapoints
    are outside of three standard deviations of this value.
    """
    try:
        last_hour_threshold = time() - (second_order_resolution_seconds - 3600)
        # @modified 20211127 - Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
        # Calculate the "equivalent" of hour and handle daily frequency data
        # Handle daily data
        resolution = (timeseries[-1][0] - timeseries[-2][0])
        if resolution > 80000 and resolution < 90000:
            last_hour_threshold = timeseries[-1][0] - ((resolution * 7) - resolution)

        series = pandas.Series([x[1] for x in timeseries if x[0] < last_hour_threshold])
        mean = (series).mean()
        stdDev = (series).std()
        t = tail_avg(timeseries, second_order_resolution_seconds)

        return abs(t - mean) > 3 * stdDev
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(algorithm_name, traceback_format_exc_string)
        return None


def stddev_from_average(timeseries, second_order_resolution_seconds):
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
        t = tail_avg(timeseries, second_order_resolution_seconds)

        return abs(t - mean) > 3 * stdDev
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(algorithm_name, traceback_format_exc_string)
        return None


def stddev_from_moving_average(timeseries, second_order_resolution_seconds):
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


def mean_subtraction_cumulation(timeseries, second_order_resolution_seconds):
    """
    A timeseries is anomalous if the value of the next datapoint in the
    series is farther than three standard deviations out in cumulative terms
    after subtracting the mean from each data point.
    """

    try:
        series = pandas.Series([x[1] if x[1] else 0 for x in timeseries])
        series = series - series[0:len(series) - 1].mean()
        stdDev = series[0:len(series) - 1].std()

        # @modified 20180910 - Task #2588: Update dependencies
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


def least_squares(timeseries, second_order_resolution_seconds):
    """
    A timeseries is anomalous if the average of the last three datapoints
    on a projected least squares model is greater than three sigma.
    """

    try:
        x = np.array([t[0] for t in timeseries])
        y = np.array([t[1] for t in timeseries])
        A = np.vstack([x, np.ones(len(x))]).T

        # @modified 20180910 - Task #2588: Update dependencies
        # This results and residual are unused
        # results = np.linalg.lstsq(A, y)
        # residual = results[1]

        # @modified 20180910 - Task #2588: Update dependencies
        # Changed in version numpy 1.14.0 - see full comments in
        # analyzer/algorithms.py under least_squares np.linalg.lstsq
        # m, c = np.linalg.lstsq(A, y)[0]
        m, c = np.linalg.lstsq(A, y, rcond=-1)[0]

        errors = []
        # Evaluate append once, not every time in the loop - this gains ~0.020 s
        # on every timeseries potentially @earthgecko #1310
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


def histogram_bins(timeseries, second_order_resolution_seconds):
    """
    A timeseries is anomalous if the average of the last three datapoints falls
    into a histogram bin with less than 20 other datapoints (you'll need to
    tweak that number depending on your data)

    Returns: the size of the bin which contains the tail_avg. Smaller bin size
    means more anomalous.
    """

    try:
        # @modified 20210420 - Support #4026: Change from scipy array to numpy array
        # Deprecation of scipy.array
        # series = scipy.array([x[1] for x in timeseries])
        series = np.array([x[1] for x in timeseries])

        t = tail_avg(timeseries, second_order_resolution_seconds)
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


def ks_test(timeseries, second_order_resolution_seconds):
    """
    A timeseries is anomalous if 2 sample Kolmogorov-Smirnov test indicates
    that data distribution for last 10 minutes is different from last hour.
    It produces false positives on non-stationary series so Augmented
    Dickey-Fuller test applied to check for stationarity.
    """

    try:
        hour_ago = time() - 3600
        ten_minutes_ago = time() - 600

        # @modified 20211127 - Feature #4328: BATCH_METRICS_CUSTOM_FULL_DURATIONS
        # Calculate the "equivalent" of hour and handle daily frequency data
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
        if lowest_value < 0:
            negatives = []
            for ts, v in timeseries:
                if v < 0:
                    negatives.append((ts, v))
            return negatives
    except:
        traceback_format_exc_string = traceback.format_exc()
        algorithm_name = str(get_function_name())
        record_algorithm_error(algorithm_name, traceback_format_exc_string)
        return None
    return False


def is_anomalously_anomalous(metric_name, ensemble, datapoint):
    """
    This method runs a meta-analysis on the metric to determine whether the
    metric has a past history of triggering. TODO: weight intervals based on datapoint
    """
    # We want the datapoint to avoid triggering twice on the same data
    new_trigger = [time(), datapoint]

    # Get the old history
    raw_trigger_history = redis_conn.get('mirage_trigger_history.' + metric_name)
    if not raw_trigger_history:
        redis_conn.set('mirage_trigger_history.' + metric_name, packb([(time(), datapoint)]))
        return True

    trigger_history = unpackb(raw_trigger_history)

    # Are we (probably) triggering on the same data?
    if (new_trigger[1] == trigger_history[-1][1] and new_trigger[0] - trigger_history[-1][0] <= 300):
        return False

    # Update the history
    trigger_history.append(new_trigger)
    redis_conn.set('mirage_trigger_history.' + metric_name, packb(trigger_history))

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


# @modified 20200423 - Feature #3508: ionosphere.untrainable_metrics
# Added run_negatives_present
# def run_selected_algorithm(timeseries, metric_name, second_order_resolution_seconds):
# @modified 20210304 - Feature #3642: Anomaly type classification
#                      Feature #3970: custom_algorithm - adtk_level_shift
# Added triggered_algorithms
def run_selected_algorithm(timeseries, metric_name, second_order_resolution_seconds, run_negatives_present, triggered_algorithms):
    """
    Run selected algorithms
    """

    # @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
    # Added negatives_found for run_negatives_present
    negatives_found = False

    # @added 20200607 - Feature #3566: custom_algorithms
    final_custom_ensemble = []
    algorithms_run = []
    custom_consensus_override = False
    custom_consensus_values = []
    run_3sigma_algorithms = True
    run_3sigma_algorithms_overridden_by = []
    custom_algorithm = None

    # @added 20201125 - Feature #3848: custom_algorithms - run_before_3sigma parameter
    run_custom_algorithm_after_3sigma = False
    final_after_custom_ensemble = []
    custom_algorithm_not_anomalous = False

    # @added 20211125 - Feature #3566: custom_algorithms
    custom_algorithms_run = []
    custom_algorithms_run_results = []

    # @added 20220111 - Feature #3566: custom_algorithms
    # Set default values
    base_name = metric_name.replace(FULL_NAMESPACE, '', 1)
    custom_algorithms_to_run = {}

    if CUSTOM_ALGORITHMS:
        base_name = metric_name.replace(FULL_NAMESPACE, '', 1)
        custom_algorithms_to_run = {}
        try:
            custom_algorithms_to_run = get_custom_algorithms_to_run(skyline_app, base_name, CUSTOM_ALGORITHMS, DEBUG_CUSTOM_ALGORITHMS)
            if DEBUG_CUSTOM_ALGORITHMS:
                if custom_algorithms_to_run:
                    logger.debug('mirage_algorithms :: debug :: custom algorithms ARE RUN on %s' % (str(base_name)))
        except:
            logger.error('error :: get_custom_algorithms_to_run :: %s' % traceback.format_exc())
            custom_algorithms_to_run = {}
        for custom_algorithm in list(custom_algorithms_to_run.keys()):
            debug_logging = False
            try:
                debug_logging = custom_algorithms_to_run[custom_algorithm]['debug_logging']
            except:
                debug_logging = False
            if DEBUG_CUSTOM_ALGORITHMS:
                debug_logging = True

            # @modified 20210304 - Feature #3642: Anomaly type classification
            #                      Feature #3970: custom_algorithm - adtk_level_shift
            # Added triggered_algorithms
            if custom_algorithm == 'adtk_level_shift':
                if 'adtk_level_shift' not in triggered_algorithms:
                    if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                        logger.debug('debug :: custom_algorithms :: NOT running custom algorithm %s on %s as was not in triggered_algorithms' % (
                            str(custom_algorithm), str(base_name)))
                    continue

            # @added 20201125 - Feature #3848: custom_algorithms - run_before_3sigma parameter
            run_before_3sigma = True
            try:
                run_before_3sigma = custom_algorithms_to_run[custom_algorithm]['run_before_3sigma']
            except:
                run_before_3sigma = True
            if not run_before_3sigma:
                run_custom_algorithm_after_3sigma = True
                if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                    logger.debug('debug :: mirage_algorithms :: NOT running custom algorithm %s on %s BEFORE three-sigma algorithms' % (
                        str(custom_algorithm), str(base_name)))
                continue

            run_algorithm = []
            run_algorithm.append(custom_algorithm)
            if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                logger.debug('debug :: mirage_algorithms :: running custom algorithm %s on %s' % (
                    str(custom_algorithm), str(base_name)))
                start_debug_timer = timer()
            run_custom_algorithm_on_timeseries = None
            try:
                from custom_algorithms import run_custom_algorithm_on_timeseries
                if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                    logger.debug('debug :: mirage_algorithms :: loaded run_custom_algorithm_on_timeseries')
            except:
                if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_algorithms :: failed to load run_custom_algorithm_on_timeseries')
            result = None
            anomalyScore = None

            use_debug_logging = False
            if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                use_debug_logging = True

            if run_custom_algorithm_on_timeseries:

                # @added 20211125 - Feature #3566: custom_algorithms
                custom_algorithms_run.append(custom_algorithm)

                try:
                    result, anomalyScore = run_custom_algorithm_on_timeseries(skyline_app, getpid(), base_name, timeseries, custom_algorithm, custom_algorithms_to_run[custom_algorithm], use_debug_logging)
                    algorithm_result = [result]
                    if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                        logger.debug('debug :: mirage_algorithms :: run_custom_algorithm_on_timeseries run with result - %s, anomalyScore - %s' % (
                            str(result), str(anomalyScore)))
                    # @added 20211125 - Feature #3566: custom_algorithms
                    custom_algorithms_run_results.append(result)

                except:
                    if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_algorithms :: failed to run custom_algorithm %s on %s' % (
                            custom_algorithm, base_name))
                    result = None
                    algorithm_result = [None]
                    # @added 20211125 - Feature #3566: custom_algorithms
                    custom_algorithms_run_results.append(False)

            else:
                if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                    logger.error('error :: debug :: mirage_algorithms :: run_custom_algorithm_on_timeseries was not loaded so was not run')
            if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                end_debug_timer = timer()
                logger.debug('debug :: mirage_algorithms :: ran custom algorithm %s on %s with result of (%s, %s) in %.6f seconds' % (
                    str(custom_algorithm), str(base_name),
                    str(result), str(anomalyScore),
                    (end_debug_timer - start_debug_timer)))
            algorithms_run.append(custom_algorithm)
            if algorithm_result.count(True) == 1:
                result = True
            elif algorithm_result.count(False) == 1:
                result = False
            elif algorithm_result.count(None) == 1:
                result = None
            else:
                result = False
            final_custom_ensemble.append(result)
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
                if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                    logger.debug('debug :: error - algorithms :: could not determine custom_run_3sigma_algorithms - default to True')
            if not custom_run_3sigma_algorithms and result:
                run_3sigma_algorithms = False
                run_3sigma_algorithms_overridden_by.append(custom_algorithm)
                if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                    logger.debug('debug :: mirage_algorithms :: run_3sigma_algorithms is False on %s for %s' % (
                        custom_algorithm, base_name))
            else:
                if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                    logger.debug('debug :: mirage_algorithms :: run_3sigma_algorithms will now be run on %s - %s' % (
                        base_name, str(custom_run_3sigma_algorithms)))
            if result:
                try:
                    custom_consensus = custom_algorithms_to_run[custom_algorithm]['consensus']
                    if custom_consensus == 0:
                        custom_consensus = int(MIRAGE_CONSENSUS)
                    else:
                        custom_consensus_values.append(custom_consensus)
                except:
                    custom_consensus = int(MIRAGE_CONSENSUS)
                try:
                    algorithms_allowed_in_consensus = custom_algorithms_to_run[custom_algorithm]['algorithms_allowed_in_consensus']
                except:
                    algorithms_allowed_in_consensus = []
                if custom_consensus == 1:
                    custom_consensus_override = True
                    logger.info('mirage_algorithms :: overidding the CONSENSUS as custom algorithm %s overides on %s' % (
                        str(custom_algorithm), str(base_name)))
                # TODO - figure out how to handle consensus overrides if
                #        multiple custom algorithms are used
    if DEBUG_CUSTOM_ALGORITHMS:
        if not run_3sigma_algorithms:
            logger.debug('mirage_algorithms :: not running 3 sigma algorithms')
        if len(run_3sigma_algorithms_overridden_by) > 0:
            logger.debug('mirage_algorithms :: run_3sigma_algorithms overridden by %s' % (
                str(run_3sigma_algorithms_overridden_by)))

    # @added 20200607 - Feature #3566: custom_algorithms
    # @modified 20201120 - Feature #3566: custom_algorithms
    # ensemble = []
    ensemble = final_custom_ensemble

    # @added 20211125 - Feature #3566: custom_algorithms
    # If custom_algorithms were run and did not trigger reset the consensus
    if run_3sigma_algorithms:
        if len(custom_algorithms_run) > 0:
            none_count = custom_algorithms_run_results.count(None)
            false_count = custom_algorithms_run_results.count(False)
            not_anomalous_count = none_count + false_count
            if len(custom_algorithms_run) == not_anomalous_count:
                custom_consensus_override = False
                custom_consensus = int(MIRAGE_CONSENSUS)
                ensemble = []
                if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                    logger.debug('debug :: mirage-algorithms :: reset ensemble, custom_consensus and custom_consensus_override after custom_algorithms all calcuated False')

    # @modified 20201125 - Feature #3848: custom_algorithms - run_before_3sigma parameter
    # Check run_3sigma_algorithms as well to the conditional
    # if not custom_consensus_override:
    if run_3sigma_algorithms and not custom_consensus_override:
        if DEBUG_CUSTOM_ALGORITHMS:
            logger.debug('debug :: mirage_algorithms :: running three-sigma algorithms')
        try:
            logger.info('mirage_algorithms :: running three-sigma algorithms')
            ensemble = [globals()[algorithm](timeseries, second_order_resolution_seconds) for algorithm in MIRAGE_ALGORITHMS]
            for algorithm in MIRAGE_ALGORITHMS:
                algorithms_run.append(algorithm)
        except:
            logger.error('Algorithm error: %s' % traceback.format_exc())
            # @modified 20200425 - Feature #3508: ionosphere.untrainable_metrics
            # Added negatives_found
            # return False, [], 1
            # @modified 20200607 - Feature #3566: custom_algorithms
            # Added algorithms_run
            return False, [], 1, False, algorithms_run
        # @added 20201124 - Feature #3566: custom_algorithms
        if final_custom_ensemble:
            ensemble = final_custom_ensemble + ensemble
        logger.info('mirage_algorithms :: %s - True count of %s after 3-sigma algorithms' % (
            str(base_name), str(ensemble.count(True))))

    else:
        for algorithm in MIRAGE_ALGORITHMS:
            ensemble.append(None)

    # @added 20201120 - Feature #3566: custom_algorithms
    # If 3sigma algorithms have not been run discard the MIRAGE_ALGORITHMS
    # added above
    if not run_3sigma_algorithms:
        ensemble = final_custom_ensemble

    # @added 20211104 - Bug #4308: matrixprofile - fN on big drops
    #                   Branch #3068: SNAB
    ensemble_pre_custom_algorithms_true_count = ensemble.count(True)
    ensemble_pre_custom_algorithms = list(ensemble)
    # if ensemble_pre_custom_algorithms_true_count >= 7:
    #     skyline_matrixprofile_override = True
    check_trigger_history = False
    trigger_history_override = 0
    try:
        trigger_history_override = custom_algorithms_to_run[custom_algorithm]['trigger_history_override']
    except KeyError:
        trigger_history_override = 0

    redis_conn_decoded = None
    if ensemble_pre_custom_algorithms_true_count >= MIRAGE_CONSENSUS and trigger_history_override:
        from ast import literal_eval
        from skyline_functions import get_redis_conn_decoded
        try:
            redis_conn_decoded = get_redis_conn_decoded(skyline_app)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: mirage_algorithms :: get_redis_conn_decoded failed - %s' % (
                str(err)))
        check_trigger_history = True

    # @added 20201125 - Feature #3848: custom_algorithms - run_before_3sigma parameter
    if run_custom_algorithm_after_3sigma:
        if DEBUG_CUSTOM_ALGORITHMS:
            logger.debug('debug :: checking custom algorithms to run AFTER three-sigma algorithms')
        for custom_algorithm in list(custom_algorithms_to_run.keys()):
            debug_logging = False
            try:
                debug_logging = custom_algorithms_to_run[custom_algorithm]['debug_logging']
            except:
                debug_logging = False
            if DEBUG_CUSTOM_ALGORITHMS:
                debug_logging = True
            run_before_3sigma = True
            try:
                run_before_3sigma = custom_algorithms_to_run[custom_algorithm]['run_before_3sigma']
            except:
                run_before_3sigma = True
            if run_before_3sigma:
                if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                    logger.debug('debug :: mirage_algorithms :: NOT running custom algorithm %s on %s AFTER three-sigma algorithms as run_before_3sigma is %s' % (
                        str(custom_algorithm), str(base_name),
                        str(run_before_3sigma)))
                continue
            try:
                custom_consensus = custom_algorithms_to_run[custom_algorithm]['consensus']
                if custom_consensus == 0:
                    custom_consensus = int(MIRAGE_CONSENSUS)
                else:
                    custom_consensus_values.append(custom_consensus)
            except:
                custom_consensus = int(MIRAGE_CONSENSUS)
            run_only_if_consensus = False
            try:
                run_only_if_consensus = custom_algorithms_to_run[custom_algorithm]['run_only_if_consensus']
            except:
                run_only_if_consensus = False
            if run_only_if_consensus:
                if ensemble.count(True) < int(MIRAGE_CONSENSUS):
                    if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                        logger.debug('debug :: mirage_algorithms :: NOT running custom algorithm %s on %s AFTER three-sigma algorithms as only %s three-sigma algorithms triggered - MIRAGE_CONSENSUS of %s not achieved' % (
                            str(custom_algorithm), str(base_name),
                            str(ensemble.count(True)), str(MIRAGE_CONSENSUS)))
                    continue
                else:
                    if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                        logger.debug('debug :: mirage_algorithms :: running custom algorithm %s on %s AFTER three-sigma algorithms as %s three-sigma algorithms triggered - MIRAGE_CONSENSUS of %s was achieved' % (
                            str(custom_algorithm), str(base_name),
                            str(ensemble.count(True)), str(MIRAGE_CONSENSUS)))
            run_algorithm = []
            run_algorithm.append(custom_algorithm)
            if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                logger.debug('debug :: mirage_algorithms :: running custom algorithm %s on %s' % (
                    str(custom_algorithm), str(base_name)))
                start_debug_timer = timer()
            run_custom_algorithm_on_timeseries = None
            try:
                from custom_algorithms import run_custom_algorithm_on_timeseries
                if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                    logger.debug('debug :: mirage_algorithms :: loaded run_custom_algorithm_on_timeseries')
            except:
                if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                    logger.error(traceback.format_exc())
                    logger.error('error :: mirage_algorithms :: failed to load run_custom_algorithm_on_timeseries')
            result = None
            anomalyScore = None

            use_debug_logging = False
            if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                use_debug_logging = True

            if run_custom_algorithm_on_timeseries:
                try:
                    result, anomalyScore = run_custom_algorithm_on_timeseries(skyline_app, getpid(), base_name, timeseries, custom_algorithm, custom_algorithms_to_run[custom_algorithm], use_debug_logging)
                    algorithm_result = [result]
                    if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                        logger.debug('debug :: mirage_algorithms :: run_custom_algorithm_on_timeseries run with result - %s, anomalyScore - %s' % (
                            str(result), str(anomalyScore)))
                    logger.info('mirage_algorithms :: metric: %s, custom_algorithm: %s, result: %s, anomalyScore: %s' % (
                        base_name, custom_algorithm, str(result), str(anomalyScore)))
                except Exception as err:
                    if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_algorithms :: failed to run custom_algorithm %s on %s - %s' % (
                            custom_algorithm, base_name, str(err)))
                    result = None
                    algorithm_result = [None]
            else:
                if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                    logger.error('error :: debug :: mirage_algorithms :: run_custom_algorithm_on_timeseries was not loaded so was not run')
            if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                end_debug_timer = timer()
                logger.debug('debug :: mirage_algorithms :: ran custom algorithm %s on %s with result of (%s, %s) in %.6f seconds' % (
                    str(custom_algorithm), str(base_name),
                    str(result), str(anomalyScore),
                    (end_debug_timer - start_debug_timer)))
            algorithms_run.append(custom_algorithm)
            if algorithm_result.count(True) == 1:
                result = True
            elif algorithm_result.count(False) == 1:
                result = False
            elif algorithm_result.count(None) == 1:
                result = None
            else:
                result = False
            final_after_custom_ensemble.append(result)
            algorithms_allowed_in_consensus = []
            # custom_run_3sigma_algorithms = True does not need to be checked
            # here as if three-sigma algorithms have run they have already run
            # at this point, unlike above in the run_before_3sigma custom
            # algorithms run
            if result:
                try:
                    algorithms_allowed_in_consensus = custom_algorithms_to_run[custom_algorithm]['algorithms_allowed_in_consensus']
                except:
                    algorithms_allowed_in_consensus = []
                if custom_consensus == 1:
                    custom_consensus_override = True
                    logger.info('mirage_algorithms :: overriding the CONSENSUS as custom algorithm %s overides on %s' % (
                        str(custom_algorithm), str(base_name)))
            else:
                # @added 20201127 - Feature #3566: custom_algorithms
                # Handle if the result is None
                if result is None:
                    logger.warning('warning :: mirage_algorithms :: %s failed to run on %s' % (
                        str(custom_algorithm), str(base_name)))
                else:
                    if custom_consensus == 1:
                        # hmmm we are required to hack threshold here
                        custom_algorithm_not_anomalous = True
                        if DEBUG_CUSTOM_ALGORITHMS or debug_logging:
                            logger.debug('debug :: mirage_algorithms :: %s did not trigger - custom_algorithm_not_anomalous set to identify as not anomalous' % (
                                str(custom_algorithm)))

                # @added 20211104 - Bug #4308: matrixprofile - fN on big drops
                #                   Branch #3068: SNAB
                if custom_algorithm == 'skyline_matrixprofile' and check_trigger_history:
                    trigger_history = {}
                    try:
                        raw_trigger_history = redis_conn_decoded.hget('mirage.trigger_history', base_name)
                        if raw_trigger_history:
                            trigger_history = literal_eval(raw_trigger_history)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_algorithms :: failed to evaluate data from mirage.trigger_history Redis hash key - %s' % (
                            str(err)))
                    metric_resolution = 60
                    if trigger_history:
                        try:
                            from functions.timeseries.determine_data_frequency import determine_data_frequency
                            metric_resolution = determine_data_frequency(skyline_app, timeseries, False)
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: mirage_algorithms :: determine_data_frequency failed - %s' % (
                                str(err)))
                    recent_trigger_history = {}
                    last_timestamp = int(timeseries[-1][0])
                    oldest_trigger_timestamp = last_timestamp - (metric_resolution * 4)
                    # Self clean trigger_history
                    for trigger_timestamp in list(trigger_history.keys()):
                        if trigger_timestamp < oldest_trigger_timestamp:
                            continue
                        recent_trigger_history[trigger_timestamp] = trigger_history[trigger_timestamp]
                    tmp_final_ensemble = ensemble + final_after_custom_ensemble
                    trigger_dict = {
                        'count': ensemble_pre_custom_algorithms_true_count,
                        'ensemble': ensemble_pre_custom_algorithms,
                        'final_ensemble': tmp_final_ensemble,
                        'final_ensemble_count': tmp_final_ensemble.count(True),
                        'algorithms_run': algorithms_run,
                        'value': timeseries[-1][1],
                        'resolution': metric_resolution
                    }
                    recent_trigger_history[last_timestamp] = trigger_dict
                    try:
                        redis_conn_decoded.hset('mirage.trigger_history', base_name, str(recent_trigger_history))
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: mirage_algorithms :: failed to set key in mirage.trigger_history Redis hash key - %s' % (
                            str(err)))
                    recent_trigger_history_count = len(list(recent_trigger_history.keys()))
                    if recent_trigger_history_count >= trigger_history_override and custom_consensus == 1:
                        logger.info('mirage_algorithms :: %s overriding %s - recent_trigger_history_count breached with %s recent triggers' % (
                            str(base_name), custom_algorithm,
                            str(recent_trigger_history_count)))
                        custom_consensus_override = False
                        custom_algorithm_not_anomalous = False
                        logger.info('mirage_algorithms :: overriding %s result: %s, not anomalous custom_consensus of 1' % (
                            custom_algorithm, str(result)))
                        logger.info('mirage_algorithms :: custom_consensus_override set to False as original 3-sigma True count is %s along with recent_trigger_history_count breach' % (
                            str(ensemble_pre_custom_algorithms_true_count)))
                        new_algorithms_run = []
                        for algorithm_run in algorithms_run:
                            if algorithm_run == custom_algorithm:
                                algorithm_run = '%s (override - %s recent 3-sigma triggers)' % (
                                    custom_algorithm, str(recent_trigger_history_count))
                            new_algorithms_run.append(algorithm_run)
                        algorithms_run = list(new_algorithms_run)

    for item in final_after_custom_ensemble:
        ensemble.append(item)

    # @modified 20200607 - Feature #3566: custom_algorithms
    try:
        # threshold = len(ensemble) - MIRAGE_CONSENSUS
        if custom_consensus_override:
            threshold = len(ensemble) - 1
        else:
            threshold = len(ensemble) - MIRAGE_CONSENSUS

        # @modified 20201125 - Feature #3848: custom_algorithms - run_before_3sigma parameter
        # Added and not custom_algorithm_not_anomalous
        if ensemble.count(False) <= threshold and not custom_algorithm_not_anomalous:

            # @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
            # Only run a negatives_present check if it is anomalous, there
            # is no need to check unless it is related to an anomaly
            if run_negatives_present:
                try:
                    negatives_found = negatives_present(timeseries)
                    if negatives_found:
                        number_of_negatives_found = len(negatives_found)
                    else:
                        number_of_negatives_found = 0
                    logger.info('%s negative values found for %s' % (
                        str(number_of_negatives_found), metric_name))
                except:
                    logger.error('Algorithm error: negatives_present :: %s' % traceback.format_exc())
                    negatives_found = False

            if MIRAGE_ENABLE_SECOND_ORDER:
                if is_anomalously_anomalous(metric_name, ensemble, timeseries[-1][1]):
                    # @modified 20200425 - Feature #3508: ionosphere.untrainable_metrics
                    # Added negatives_found
                    # return True, ensemble, timeseries[-1][1]
                    # @modified 20200607 - Feature #3566: custom_algorithms
                    # Added algorithms_run
                    return True, ensemble, timeseries[-1][1], negatives_found, algorithms_run
            else:
                # @modified 20200425 - Feature #3508: ionosphere.untrainable_metrics
                # Added negatives_found
                # return True, ensemble, timeseries[-1][1]
                # @modified 20200607 - Feature #3566: custom_algorithms
                # Added algorithms_run
                return True, ensemble, timeseries[-1][1], negatives_found, algorithms_run

        # @modified 20200425 - Feature #3508: ionosphere.untrainable_metrics
        # Added negatives_found
        # return False, ensemble, timeseries[-1][1]
        # @modified 20200607 - Feature #3566: custom_algorithms
        # Added algorithms_run
        return False, ensemble, timeseries[-1][1], negatives_found, algorithms_run
    except:
        logger.error('Algorithm error: %s' % traceback.format_exc())
        # @modified 20200425 - Feature #3508: ionosphere.untrainable_metrics
        # Added negatives_found
        # return False, [], 1
        # @modified 20200607 - Feature #3566: custom_algorithms
        # Added algorithms_run
        return False, [], 1, False, algorithms_run
