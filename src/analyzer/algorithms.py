import pandas
import numpy as np
import scipy
import statsmodels.api as sm
import traceback
import logging
from time import time
from msgpack import unpackb, packb
from redis import StrictRedis

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
)

try:
    from settings import (
        DROP_OFF_CLIFF_METRICS,
    )
    DETECT_DROP_OFF_CLIFF_METRICS=True
    import re
except:
    DETECT_DROP_OFF_CLIFF_METRICS=False

from algorithm_exceptions import *

logger = logging.getLogger("AnalyzerLog")
redis_conn = StrictRedis(unix_socket_path=REDIS_SOCKET_PATH)

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

    series = pandas.Series([x[1] for x in timeseries])
    median = series.median()
    demedianed = np.abs(series - median)
    median_deviation = demedianed.median()

    # The test statistic is infinite when the median is zero,
    # so it becomes super sensitive. We play it safe and skip when this happens.
    if median_deviation == 0:
        return False

    test_statistic = demedianed.iget(-1) / median_deviation

    # Completely arbitary...triggers if the median deviation is
    # 6 times bigger than the median
    if test_statistic > 6:
        return True


def grubbs(timeseries):
    """
    A timeseries is anomalous if the Z score is greater than the Grubb's score.
    """

    series = scipy.array([x[1] for x in timeseries])
    stdDev = scipy.std(series)
    mean = np.mean(series)
    tail_average = tail_avg(timeseries)
    z_score = (tail_average - mean) / stdDev
    len_series = len(series)
    threshold = scipy.stats.t.isf(.05 / (2 * len_series), len_series - 2)
    threshold_squared = threshold * threshold
    grubbs_score = ((len_series - 1) / np.sqrt(len_series)) * np.sqrt(threshold_squared / (len_series - 2 + threshold_squared))

    return z_score > grubbs_score


def first_hour_average(timeseries):
    """
    Calcuate the simple average over one hour, FULL_DURATION seconds ago.
    A timeseries is anomalous if the average of the last three datapoints
    are outside of three standard deviations of this value.
    """
    last_hour_threshold = time() - (FULL_DURATION - 3600)
    series = pandas.Series([x[1] for x in timeseries if x[0] < last_hour_threshold])
    mean = (series).mean()
    stdDev = (series).std()
    t = tail_avg(timeseries)

    return abs(t - mean) > 3 * stdDev


def stddev_from_average(timeseries):
    """
    A timeseries is anomalous if the absolute value of the average of the latest
    three datapoint minus the moving average is greater than three standard
    deviations of the average. This does not exponentially weight the MA and so
    is better for detecting anomalies with respect to the entire series.
    """
    series = pandas.Series([x[1] for x in timeseries])
    mean = series.mean()
    stdDev = series.std()
    t = tail_avg(timeseries)

    return abs(t - mean) > 3 * stdDev


def stddev_from_moving_average(timeseries):
    """
    A timeseries is anomalous if the absolute value of the average of the latest
    three datapoint minus the moving average is greater than three standard
    deviations of the moving average. This is better for finding anomalies with
    respect to the short term trends.
    """
    series = pandas.Series([x[1] for x in timeseries])
    expAverage = pandas.stats.moments.ewma(series, com=50)
    stdDev = pandas.stats.moments.ewmstd(series, com=50)

    return abs(series.iget(-1) - expAverage.iget(-1)) > 3 * stdDev.iget(-1)


def mean_subtraction_cumulation(timeseries):
    """
    A timeseries is anomalous if the value of the next datapoint in the
    series is farther than three standard deviations out in cumulative terms
    after subtracting the mean from each data point.
    """

    series = pandas.Series([x[1] if x[1] else 0 for x in timeseries])
    series = series - series[0:len(series) - 1].mean()
    stdDev = series[0:len(series) - 1].std()
    expAverage = pandas.stats.moments.ewma(series, com=15)

    return abs(series.iget(-1)) > 3 * stdDev


def least_squares(timeseries):
    """
    A timeseries is anomalous if the average of the last three datapoints
    on a projected least squares model is greater than three sigma.
    """

    x = np.array([t[0] for t in timeseries])
    y = np.array([t[1] for t in timeseries])
    A = np.vstack([x, np.ones(len(x))]).T
    results = np.linalg.lstsq(A, y)
    residual = results[1]
    m, c = np.linalg.lstsq(A, y)[0]
    errors = []
    for i, value in enumerate(y):
        projected = m * x[i] + c
        error = value - projected
        errors.append(error)

    if len(errors) < 3:
        return False

    std_dev = scipy.std(errors)
    t = (errors[-1] + errors[-2] + errors[-3]) / 3

    return abs(t) > std_dev * 3 and round(std_dev) != 0 and round(t) != 0


def histogram_bins(timeseries):
    """
    A timeseries is anomalous if the average of the last three datapoints falls
    into a histogram bin with less than 20 other datapoints (you'll need to tweak
    that number depending on your data)

    Returns: the size of the bin which contains the tail_avg. Smaller bin size
    means more anomalous.
    """

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


def ks_test(timeseries):
    """
    A timeseries is anomalous if 2 sample Kolmogorov-Smirnov test indicates
    that data distribution for last 10 minutes is different from last hour.
    It produces false positives on non-stationary series so Augmented
    Dickey-Fuller test applied to check for stationarity.
    """

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

def detect_drop_off_cliff(timeseries, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds):
    """
    A timeseries is anomalous if the average of the last 10 datapoints is 
    <trigger> times greater than the last data point AND if has not experienced
    frequent cliff drops in the last 10 datapoints.  If the timeseries has 
    experienced 2 or more datapoints of equal or less values in the last 10 or
    EXPIRATION_TIME datapoints or is less than a MIN_AVERAGE if set the
    algorithm determines the datapoint as NOT anomalous but normal.
    This algorithm is most suited to timeseries with most datapoints being > 100
    (e.g high rate).  The arbitrary <trigger> values become more noisy with
    lower value datapoints, but it still matches drops off cliffs.
    """

    if len(timeseries) < 60:
        return False

    int_end_timestamp = int(timeseries[-1][0])
    # Determine resolution of the data set
    int_second_last_end_timestamp = int(timeseries[-2][0])
    resolution = int_end_timestamp - int_second_last_end_timestamp
    ten_data_point_seconds = resolution * 10
    ten_datapoints_ago = int_end_timestamp - ten_data_point_seconds

    ten_datapoint_array = scipy.array([x[1] for x in timeseries if x[0] <= int_end_timestamp and x[0] > ten_datapoints_ago])
    ten_datapoint_array_len = len(ten_datapoint_array)
    if ten_datapoint_array_len > 3:

        ten_datapoint_min_value = np.amin(ten_datapoint_array)

        # DO NOT handle if negative integers are in the range, where is the
        # bottom of the cliff if a range goes negative?  Testing with a noisy
        # sine wave timeseries that had a drop off cliff introduced to the
        # postive data side, proved that this algorithm does work on timeseries
        # with data values in the negative range
        if ten_datapoint_min_value < 0:
            return False

        ten_datapoint_max_value = np.amax(ten_datapoint_array)

        # The algorithm should have already fired in 10 datapoints if the
        # timeseries dropped off a cliff, these are all zero
        if ten_datapoint_max_value == 0:
            return False

        # If the lowest is equal to the highest, no drop off cliff
        if ten_datapoint_min_value == ten_datapoint_max_value:
            return False

#        if ten_datapoint_max_value < 10:
#            return False

        ten_datapoint_array_sum = np.sum(ten_datapoint_array)
        ten_datapoint_value = int(ten_datapoint_array[-1])
        ten_datapoint_average = ten_datapoint_array_sum / ten_datapoint_array_len
        ten_datapoint_value = int(ten_datapoint_array[-1])

        # if a metric goes up and down a lot and falls off a cliff frequently
        # it is normal, not anomalous
        number_of_similar_datapoints = len(np.where(ten_datapoint_array <= ten_datapoint_min_value))

        # Detect once only - to make this useful and not noisy the first one
        # would have already fired and detected the drop
        if number_of_similar_datapoints > 2:
            return False

        # evaluate against 20 datapoints as well, reduces chatter on peaky ones
        # tested with 60 as well and 20 is sufficient to filter noise
        twenty_data_point_seconds = resolution * 20
        twenty_datapoints_ago = int_end_timestamp - twenty_data_point_seconds
        twenty_datapoint_array = scipy.array([x[1] for x in timeseries if x[0] <= int_end_timestamp and x[0] > twenty_datapoints_ago])
        number_of_similar_datapoints_in_twenty = len(np.where(twenty_datapoint_array <= ten_datapoint_min_value))
        if number_of_similar_datapoints_in_twenty > 2:
            return False

        # Check if there is a similar data point in EXPIRATION_TIME
        # Disabled as redis alert cache will filter on this
#        if metric_expiration_time > twenty_data_point_seconds:
#            expiration_time_data_point_seconds = metric_expiration_time
#            expiration_time_datapoints_ago = int_end_timestamp - metric_expiration_time
#            expiration_time_datapoint_array = scipy.array([x[1] for x in timeseries if x[0] <= int_end_timestamp and x[0] > expiration_time_datapoints_ago])
#            number_of_similar_datapoints_in_expiration_time = len(np.where(expiration_time_datapoint_array <= ten_datapoint_min_value))
#            if number_of_similar_datapoints_in_expiration_time > 2:
#                return False

        if metric_min_average > 0 and metric_min_average_seconds > 0:
            min_average = metric_min_average
            min_average_seconds = metric_min_average_seconds
            min_average_data_point_seconds = resolution * min_average_seconds
            min_average_datapoints_ago = int_end_timestamp - (resolution * min_average_seconds)
            min_average_array = scipy.array([x[1] for x in timeseries if x[0] <= int_end_timestamp and x[0] > min_average_datapoints_ago])
            min_average_array_average = np.sum(min_average_array) / len(min_average_array)
            if min_average_array_average < min_average:
                return False

        if ten_datapoint_max_value < 101:
            trigger = 15
        if ten_datapoint_max_value < 20:
            trigger = ten_datapoint_average / 2
        if ten_datapoint_max_value > 100:
            trigger = 100
        if ten_datapoint_value == 0:
            # Cannot divide by 0, so set to 0.1 to prevent error
            ten_datapoint_value = 0.1
        if ten_datapoint_value == 1:
            trigger = 1
        if ten_datapoint_value == 1 and ten_datapoint_max_value < 10:
            trigger = 0.1
        if ten_datapoint_value == 0.1 and ten_datapoint_average < 1 and ten_datapoint_array_sum < 7:
            trigger = 7

        ten_datapoint_result = ten_datapoint_average / ten_datapoint_value
        if int(ten_datapoint_result) > trigger:
            log_event_string = join("detect_drop_off_cliff - " + str(int_end_timestamp) + ", ten_datapoint_value = " + str(ten_datapoint_value) + ", ten_datapoint_array_sum = " + str(ten_datapoint_array_sum)  + ", ten_datapoint_average = " + str(ten_datapoint_average) + ", trigger = " + str(trigger) + ", ten_datapoint_result = " + str(ten_datapoint_result))
            logger.info(log_event_string)
            return True

    return False


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

    # detect_drop_off_cliff first as CONSENSUS is not required
    if DETECT_DROP_OFF_CLIFF_METRICS == True:
        if DROP_OFF_CLIFF_METRICS:
            for metric in DROP_OFF_CLIFF_METRICS:
                CHECK_MATCH_PATTERN = metric[0]
                check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
                pattern_match = check_match_pattern.match(metric_name)
                if pattern_match:
                    metric_expiration_time = 0
                    metric_min_average = 0
                    metric_min_average_seconds = 1200
                    if metric[2]:
                        metric_expiration_time = metric[2]
                    if metric[3]:
                        metric_min_average = metric[3]
                    if metric[4]:
                        metric_min_average_seconds = metric[4]
                    try:
                        ensemble = [globals()[detect_drop_off_cliff](timeseries, metric[0], metric_expiration_time, metric_min_average, metric_min_average_seconds)]
                        if ensemble.count(True) == 1:
                            return True, ensemble, timeseries[-1][1]
                    except:
                        logging.error("Algorithm error: detect_drop_off_cliff - " + traceback.format_exc())

    try:
        ensemble = [globals()[algorithm](timeseries) for algorithm in ALGORITHMS]
        threshold = len(ensemble) - CONSENSUS
        if ensemble.count(False) <= threshold:
            if ENABLE_SECOND_ORDER:
                if is_anomalously_anomalous(metric_name, ensemble, timeseries[-1][1]):
                    return True, ensemble, timeseries[-1][1]
            else:
                return True, ensemble, timeseries[-1][1]

        return False, ensemble, timeseries[-1][1]
    except:
        logging.error("Algorithm error: " + traceback.format_exc())
        return False, [], 1
