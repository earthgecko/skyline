from __future__ import division
# @modified 20180910 - Task #2588: Update dependencies
# matplotlib.use is now required before statsmodels.api
from matplotlib import use as matplotlib_use
matplotlib_use('Agg')

if True:
    import pandas
    import numpy as np
    import scipy
    import statsmodels.api as sm
    # @modified 20160821 - Issue #23 Test dependency updates
    # Use Agg for matplotlib==1.5.2 upgrade, backwards compatibile
    # @modified 20180910 - Task #2588: Update dependencies
    # import matplotlib
    # matplotlib.use('Agg')
    import matplotlib
    import matplotlib.pyplot as plt
    import traceback
    import logging
    import os
    import time
    from sys import version_info

    from os.path import join

    import sys
    import os.path
    sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
    sys.path.insert(0, os.path.dirname(__file__))

    from settings import (
        ALGORITHMS,
        MIRAGE_ALGORITHMS,
        PANDAS_VERSION,
        # @added 20190611 - Feature #3106: crucible - skyline.consensus.anomalies.png
        CONSENSUS,
    )

    # @added 20200603 - Feature #3566: custom_algorithms
    try:
        CUSTOM_ALGORITHMS = settings.CUSTOM_ALGORITHMS.copy()
    except:
        CUSTOM_ALGORITHMS = None
    try:
        DEBUG_CUSTOM_ALGORITHMS = settings.DEBUG_CUSTOM_ALGORITHMS
    except:
        DEBUG_CUSTOM_ALGORITHMS = False

    from skyline_functions import write_data_to_file

skyline_app = 'crucible'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

python_version = int(version_info[0])

"""
This is no man's land. Do anything you want in here,
as long as you return a boolean that determines whether the input
timeseries is anomalous or not.

To add an algorithm, define it here, and add its name to settings.ALGORITHMS.
It must be defined required parameters (even if your algorithm/function does not
need them), as the run_algorithms function passes them to all ALGORITHMS defined
in settings.ALGORITHMS.
"""


def tail_avg(timeseries, end_timestamp, full_duration):
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


def median_absolute_deviation(timeseries, end_timestamp, full_duration):
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


def grubbs(timeseries, end_timestamp, full_duration):
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
        tail_average = tail_avg(timeseries, end_timestamp, full_duration)
        z_score = (tail_average - mean) / stdDev
        len_series = len(series)
        threshold = scipy.stats.t.isf(.05 / (2 * len_series), len_series - 2)
        threshold_squared = threshold * threshold
        grubbs_score = ((len_series - 1) / np.sqrt(len_series)) * np.sqrt(threshold_squared / (len_series - 2 + threshold_squared))

        return z_score > grubbs_score
    except:
        return None


def first_hour_average(timeseries, end_timestamp, full_duration):
    """
    Calcuate the simple average over 60 datapoints (maybe one hour),
    FULL_DURATION seconds ago.
    A timeseries is anomalous if the average of the last three datapoints
    are outside of three standard deviations of this value.
    """

    try:
        int_end_timestamp = int(timeseries[-1][0])
        int_start_timestamp = int(timeseries[0][0])
        int_full_duration = int_end_timestamp - int_start_timestamp

    # Determine data resolution
    #    last_hour_threshold = int_end_timestamp - (int_full_duration - 3600)
        int_second_last_end_timestamp = int(timeseries[-2][0])
        resolution = int_end_timestamp - int_second_last_end_timestamp
        # @modified 20160814 - pyflaked
        # ten_data_point_seconds = resolution * 10
        sixty_data_point_seconds = resolution * 60
        sixty_datapoints_ago = int_end_timestamp - sixty_data_point_seconds
        last_hour_threshold = int_end_timestamp - (int_full_duration - sixty_datapoints_ago)

        series = pandas.Series([x[1] for x in timeseries if x[0] < last_hour_threshold])
        mean = (series).mean()
        stdDev = (series).std()
        t = tail_avg(timeseries, end_timestamp, full_duration)

        return abs(t - mean) > 3 * stdDev
    except:
        return None

    return False


def stddev_from_average(timeseries, end_timestamp, full_duration):
    """
    A timeseries is anomalous if the absolute value of the average of the latest
    three datapoint minus the moving average is greater than one standard
    deviation of the average. This does not exponentially weight the MA and so
    is better for detecting anomalies with respect to the entire series.
    """
    try:
        series = pandas.Series([x[1] for x in timeseries])
        mean = series.mean()
        stdDev = series.std()
        t = tail_avg(timeseries, end_timestamp, full_duration)

        return abs(t - mean) > 3 * stdDev
    except:
        return None

    return False


def stddev_from_moving_average(timeseries, end_timestamp, full_duration):
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
        return None

    return False


def mean_subtraction_cumulation(timeseries, end_timestamp, full_duration):
    """
    A timeseries is anomalous if the value of the next datapoint in the
    series is farther than three standard deviations out in cumulative terms
    after subtracting the mean from each data point.
    """

    try:
        series = pandas.Series([x[1] if x[1] else 0 for x in timeseries])
        series = series - series[0:len(series) - 1].mean()
        stdDev = series[0:len(series) - 1].std()
        # @modified 20160814 - pyflaked
        # if PANDAS_VERSION < '0.18.0':
        #     expAverage = pandas.stats.moments.ewma(series, com=15)
        # else:
        #     expAverage = pandas.Series.ewm(series, ignore_na=False, min_periods=0, adjust=True, com=15).mean()

        if PANDAS_VERSION < '0.17.0':
            return abs(series.iget(-1)) > 3 * stdDev
        else:
            return abs(series.iat[-1]) > 3 * stdDev
    except:
        return None

    return False


def least_squares(timeseries, end_timestamp, full_duration):
    """
    A timeseries is anomalous if the average of the last three datapoints
    on a projected least squares model is greater than three sigma.
    """

    try:
        x = np.array([t[0] for t in timeseries])
        y = np.array([t[1] for t in timeseries])
        A = np.vstack([x, np.ones(len(x))]).T
        # @modified 20160814 - pyflaked
        # results = np.linalg.lstsq(A, y)
        # residual = results[1]
        # @modified 20200422 - Feature #3500: webapp - crucible_process_metrics
        #                      Task #2588: Update dependencies
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
        return None

    return False


def histogram_bins(timeseries, end_timestamp, full_duration):
    """
    A timeseries is anomalous if the average of the last three datapoints falls
    into a histogram bin with less than 20 other datapoints (you'll need to tweak
    that number depending on your data)

    Returns: the size of the bin which contains the tail_avg. Smaller bin size
    means more anomalous.
    """

    try:
        int_end_timestamp = int(timeseries[-1][0])
        int_start_timestamp = int(timeseries[0][0])
        int_full_duration = int_end_timestamp - int_start_timestamp

        # @modified 20210420 - Support #4026: Change from scipy array to numpy array
        # Deprecation of scipy.array
        # series = scipy.array([x[1] for x in timeseries])
        series = np.array([x[1] for x in timeseries])

        t = tail_avg(timeseries, int_end_timestamp, int_full_duration)
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
    except:
        return None

    return False


def ks_test(timeseries, end_timestamp, full_duration):
    """
    A timeseries is anomalous if 2 sample Kolmogorov-Smirnov test indicates
    that data distribution for last 10 datapoints (might be 10 minutes) is
    different from the last 60 datapoints (might be an hour).
    It produces false positives on non-stationary series so Augmented
    Dickey-Fuller test applied to check for stationarity.
    """

    try:
        int_end_timestamp = int(timeseries[-1][0])

        # @modified 20160814 - pyflaked
        # hour_ago = int_end_timestamp - 3600
        # ten_minutes_ago = int_end_timestamp - 600
    # Determine resolution of the data set
    #    reference = scipy.array([x[1] for x in timeseries if x[0] >= hour_ago and x[0] < ten_minutes_ago])
    #    probe = scipy.array([x[1] for x in timeseries if x[0] >= ten_minutes_ago])
        int_second_last_end_timestamp = int(timeseries[-2][0])
        resolution = int_end_timestamp - int_second_last_end_timestamp
        ten_data_point_seconds = resolution * 10
        ten_datapoints_ago = int_end_timestamp - ten_data_point_seconds
        sixty_data_point_seconds = resolution * 60
        sixty_datapoints_ago = int_end_timestamp - sixty_data_point_seconds
        # @modified 20210420 - Support #4026: Change from scipy array to numpy array
        # Deprecation of scipy.array
        # reference = scipy.array([x[1] for x in timeseries if x[0] >= sixty_datapoints_ago and x[0] < ten_datapoints_ago])
        # probe = scipy.array([x[1] for x in timeseries if x[0] >= ten_datapoints_ago])
        reference = np.array([x[1] for x in timeseries if x[0] >= sixty_datapoints_ago and x[0] < ten_datapoints_ago])
        probe = np.array([x[1] for x in timeseries if x[0] >= ten_datapoints_ago])

        if reference.size < 20 or probe.size < 20:
            return False

        ks_d, ks_p_value = scipy.stats.ks_2samp(reference, probe)

        if ks_p_value < 0.05 and ks_d > 0.5:
            adf = sm.tsa.stattools.adfuller(reference, 10)
            if adf[1] < 0.05:
                return True
    except:
        return None

    return False


def detect_drop_off_cliff(timeseries, end_timestamp, full_duration):
    """
    A timeseries is anomalous if the average of the last ten datapoints is <trigger>
    times greater than the last data point.  This algorithm is most suited to
    timeseries with most datapoints being > 100 (e.g high rate).  The arbitrary
    <trigger> values become more noisy with lower value datapoints, but it still
    matches drops off cliffs.
    """

    try:
        if len(timeseries) < 21:
            return False

        int_end_timestamp = int(timeseries[-1][0])
        # Determine resolution of the data set
        int_second_last_end_timestamp = int(timeseries[-2][0])
        resolution = int_end_timestamp - int_second_last_end_timestamp
        ten_data_point_seconds = resolution * 10
        ten_datapoints_ago = int_end_timestamp - ten_data_point_seconds

        # @modified 20210420 - Support #4026: Change from scipy array to numpy array
        # Deprecation of scipy.array
        # ten_datapoint_array = scipy.array([x[1] for x in timeseries if x[0] <= int_end_timestamp and x[0] > ten_datapoints_ago])
        ten_datapoint_array = np.array([x[1] for x in timeseries if x[0] <= int_end_timestamp and x[0] > ten_datapoints_ago])

        ten_datapoint_array_len = len(ten_datapoint_array)
        if ten_datapoint_array_len > 3:
            # DO NOT handle if negative integers in range, where is the bottom of
            # of the cliff if a range goes negative? The maths does not work either
            ten_datapoint_min_value = np.amin(ten_datapoint_array)
            if ten_datapoint_min_value < 0:
                return False
            ten_datapoint_max_value = np.amax(ten_datapoint_array)
            if ten_datapoint_max_value < 10:
                return False
            ten_datapoint_array_sum = np.sum(ten_datapoint_array)
            ten_datapoint_value = int(ten_datapoint_array[-1])
            ten_datapoint_average = ten_datapoint_array_sum / ten_datapoint_array_len
            ten_datapoint_value = int(ten_datapoint_array[-1])
            ten_datapoint_max_value = np.amax(ten_datapoint_array)
            if ten_datapoint_max_value == 0:
                return False
            if ten_datapoint_max_value < 101:
                trigger = 15
            if ten_datapoint_max_value < 20:
                trigger = ten_datapoint_average / 2
            if ten_datapoint_max_value < 1:
                trigger = 0.1
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
            # Filter low rate and variable between 0 and 100 metrics
            if ten_datapoint_value <= 1 and ten_datapoint_array_sum < 100 and ten_datapoint_array_sum > 1:

                # @modified 20210420 - Support #4026: Change from scipy array to numpy array
                # Deprecation of scipy.array
                # all_datapoints_array = scipy.array([x[1] for x in timeseries])
                all_datapoints_array = np.array([x[1] for x in timeseries])

                all_datapoints_max_value = np.amax(all_datapoints_array)
                if all_datapoints_max_value < 100:
                    # print "max_value for all datapoints at - " + str(int_end_timestamp) + " - " + str(all_datapoints_max_value)
                    return False
            ten_datapoint_result = ten_datapoint_average / ten_datapoint_value
            if int(ten_datapoint_result) > trigger:
                return True
    except:
        return None

    return False


"""
This is no longer no man's land, but feel free to play and try new stuff
"""


# @modified 20200421 - Feature #3500: webapp - crucible_process_metrics
#                      Feature #1448: Crucible web UI
# Added alert_interval and add_to_panaroma
# @modified 20200422 - Feature #3500: webapp - crucible_process_metrics
#                      Feature #1448: Crucible web UI
# Added padded_timeseries
def run_algorithms(
        timeseries, timeseries_name, end_timestamp, full_duration,
        timeseries_file, skyline_app, algorithms, alert_interval,
        add_to_panorama, padded_timeseries, from_timestamp):
    """
    Iteratively run algorithms.
    """

    results_dir = os.path.dirname(timeseries_file)

    if not os.path.exists(results_dir):
        os.makedirs(results_dir, mode=0o755)

    start_analysis = int(time.time())

    triggered_algorithms = []
    anomalous = False
    # @added 20200427 - Feature #3500: webapp - crucible_process_metrics
    #                   Feature #1448: Crucible web UI
    # Added default alert_interval_discarded_anomalies_count so run_algorithms
    # does not return as failed
    alert_interval_discarded_anomalies_count = 0

    check_algorithms = []
    if str(algorithms) == "['all']":
        if skyline_app == 'analyzer':
            check_algorithms = ALGORITHMS
            logger.info('check_algorithms for analyzer - %s' % (str(check_algorithms)))
        if skyline_app == 'mirage':
            check_algorithms = MIRAGE_ALGORITHMS
            logger.info('check_algorithms for mirage - %s' % (str(check_algorithms)))
        if skyline_app == 'boundary':
            check_algorithms = algorithms
            logger.info('check_algorithms for boundary - %s' % (str(check_algorithms)))
        if skyline_app == 'crucible':
            ALGORITHMS.append('detect_drop_off_cliff')
            check_algorithms = ALGORITHMS
            logger.info('check_algorithms for crucible - %s' % (str(check_algorithms)))
    else:
        check_algorithms = algorithms
        logger.info('check_algorithms specified - %s' % (str(check_algorithms)))

    if not check_algorithms:
        logger.info('check_algorithms unknown - %s' % (str(check_algorithms)))
        ALGORITHMS.append('detect_drop_off_cliff')
        check_algorithms = ALGORITHMS
        logger.info('check_algorithms - %s' % (str(check_algorithms)))

    logger.info('checking algorithms - %s on %s' % (str(check_algorithms), str(timeseries_file)))

    # @added 20190611 - Feature #3106: crucible - skyline.consensus.anomalies.png
    # Plot Skyline anomalies if CONSENSUS is achieved
    anomalies = []

    # @added 20200422 - Feature #3500: webapp - crucible_process_metrics
    #                      Feature #1448: Crucible web UI
    # Added padded_timeseries.  If the time series is padded then set
    # the range appropriately so that the padded period data points are not
    # analysed for anomalies
    default_range = 10
    if padded_timeseries:
        default_range = 0
        for ts, value in timeseries:
            if int(ts) < from_timestamp:
                default_range += 1
            else:
                break
        logger.info('padded_timeseries - default range set to %s to %s' % (str(default_range), str(timeseries_file)))

    for algorithm in check_algorithms:
        detected = ''
        try:
            x_vals = np.arange(len(timeseries))
            y_vals = np.array([y[1] for y in timeseries])
            # Match default graphite graph size
            plt.figure(figsize=(5.86, 3.08), dpi=100)
            plt.plot(x_vals, y_vals)

            # Start a couple datapoints in for the tail average
            # @modified 20200422 - Feature #3500: webapp - crucible_process_metrics
            #                      Feature #1448: Crucible web UI
            # If the time series is padded then use the appropriate range so
            # that the padded period data points are not analysed for anomalies
            # for index in range(10, len(timeseries)):
            for index in range(default_range, len(timeseries)):
                sliced = timeseries[:index]
                anomaly = globals()[algorithm](sliced, end_timestamp, full_duration)

                # Point out the datapoint if it's anomalous
                if anomaly:
                    plt.plot([index], [sliced[-1][1]], 'ro')
                    detected = "DETECTED"
                    # @added 20190611 - Feature #3106: crucible - skyline.consensus.anomalies.png
                    # Add the anomaly to the anomalies list to plot Skyline
                    # anomalies if CONSENSUS is achieved
                    anomalies.append([sliced[-1][0], sliced[-1][1], algorithm])

            if detected == "DETECTED":
                results_filename = join(results_dir + "/" + algorithm + "." + detected + ".png")
                logger.info('ANOMALY DETECTED :: with %s on %s' % (algorithm, str(timeseries_file)))
                anomalous = True
                triggered_algorithms.append(algorithm)
            else:
                results_filename = join(results_dir + "/" + algorithm + ".png")

            try:
                plt.savefig(results_filename, dpi=100)
                logger.info('saved %s plot :: %s' % (algorithm, results_filename))
                if python_version == 2:
                    # @modified 20200327 - Branch #3262: py3
                    # os.chmod(results_filename, 0644)
                    os.chmod(results_filename, 0o644)
                if python_version == 3:
                    os.chmod(results_filename, mode=0o644)
            except:
                logger.error('error :: %s' % (traceback.format_exc()))
                logger.error('error :: failed to save %s for %s' % (str(results_filename), str(timeseries_file)))
        except:
            logger.error('error :: %s' % (traceback.format_exc()))
            logger.error('error :: error thrown in algorithm running and plotting - %s on %s' % (str(algorithm), str(timeseries_file)))

    end_analysis = int(time.time())
    # @modified 20160814 - pyflaked
    # seconds_to_run = end_analysis - start_analysis
#    logger.info(
#        'analysis of %s at a full duration of %s took %s seconds' %
#        (timeseries_name, str(full_duration), str(seconds_to_run)))

    # @added 20200421 - Feature #3500: webapp - crucible_process_metrics
    #                   Feature #1448: Crucible web UI
    # Added last_anomaly_timestamp to apply alert_interval against and
    # alert_interval_discarded_anomalies.  If the alert interval is passed
    # Crucible will only report Skyline CONSENSUS anomalies if the time between
    # the last anomaly is not alert_interval less than the specified
    # alert_interval period.  This enables Crucible to mimic Analyzer and Mirage
    # and apply a EXPIRATION_TIME type methodology to identifying anomalies like
    # Analyzer would.  This makes Crucible work SOMEWHAT like Analyzer, however
    # is still a bit different as with Crucible the time series grows, like a
    # new metric would.
    # Set the last_anomaly_timestamp to the appropriate timestamp before the
    # alert_interval if alert_interval is set, if it is not it does not matter
    # as alert_interval and alert_interval_discarded_anomalies will not be
    # applied.
    # @modified 20200427 - Feature #3500: webapp - crucible_process_metrics
    #                      Feature #1448: Crucible web UI
    # Wrap timeseries_start_timestamp variable in try so on fail the process
    # does not hang
    try:
        timeseries_start_timestamp = int(timeseries[0][0])
    except:
        logger.error('error :: %s' % (traceback.format_exc()))
        logger.error('error :: failed to determine timeseries_start_timestamp from %s' % str(timeseries_file))
        timeseries_start_timestamp = 0

    # @modified 20200427 - Feature #3500: webapp - crucible_process_metrics
    #                      Feature #1448: Crucible web UI
    # if alert_interval:
    last_anomaly_timestamp = timeseries_start_timestamp
    if alert_interval and timeseries_start_timestamp:
        last_anomaly_timestamp = timeseries_start_timestamp - (alert_interval + 1)
    else:
        last_anomaly_timestamp = timeseries_start_timestamp
    alert_interval_discarded_anomalies = []
    # To apply alert_interval the anomalies object needs to be sorted by
    # timestamp as the anomalies are added per algorithm so they are not
    # timestamp ordered, but timestamp ordered per algorithm
    if anomalies and alert_interval:
        try:
            logger.info('info :: last_anomaly_timestamp set to %s for alert_interval check on %s' % (str(last_anomaly_timestamp), str(timeseries_file)))
            logger.info('info :: sorting anomalies %s to apply alert_interval check on %s' % (str(len(anomalies)), str(timeseries_file)))
            sorted_anomalies = sorted(anomalies, key=lambda x: x[0])
            anomalies = sorted_anomalies
            del sorted_anomalies
        except:
            logger.error('error :: %s' % (traceback.format_exc()))
            logger.error('error :: falied to create sorted_anomalies on %s' % str(timeseries_file))

    # @added 20200817 - Feature #3682: SNAB - webapp - crucible_process - run_algorithms
    # Allow the user to pass run_algorithms to run
    use_consensus = 6
    try:
        try:
            from settings import CONSENSUS as use_consensus
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: falied to set uSE_CONSENSUS')
            use_consensus = 6
        if len(check_algorithms) <= use_consensus:
            use_consensus = len(check_algorithms)
            logger.info('check_algorithms passed with the number of algorithms less than CONSENSUS, use_consensus set to %s' % (str(use_consensus)))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: falied to set CONSENSUS')

    # @added 20190611 - Feature #3106: crucible - skyline.consensus.anomalies.png
    # Plot Skyline anomalies where CONSENSUS achieved and create file resources
    # skyline.anomalies_score.txt and skyline.anomalies.csv
    anomalies_score = []
    if anomalies:
        for ts, value, algo in anomalies:
            try:
                processed = False
                algorithms_triggered = []
                if anomalies_score:
                    for i in anomalies_score:
                        if i[0] == ts:
                            processed = True
                            continue
                if processed:
                    continue
                for w_ts, w_value, w_algo in anomalies:
                    if w_ts == ts:
                        algorithms_triggered.append(w_algo)
                # @added 20200421 - Feature #3500: webapp - crucible_process_metrics
                #                      Feature #1448: Crucible web UI
                # Added last_anomaly_timestamp to apply alert_interval against and
                # alert_interval_discarded_anomalies.  If the alert interval is passed
                append_anomaly = True

                if algorithms_triggered:
                    consensus = len(algorithms_triggered)

                    # @added 20200421 - Feature #3500: webapp - crucible_process_metrics
                    #                      Feature #1448: Crucible web UI
                    # Added last_anomaly_timestamp to apply alert_interval against and
                    # alert_interval_discarded_anomalies.  If the alert interval is passed
                    # @modified 20200817 - Feature #3682: SNAB - webapp - crucible_process - run_algorithms
                    # if consensus >= CONSENSUS:
                    if consensus >= use_consensus:
                        current_anomaly_timestamp = int(ts)
                        if alert_interval and last_anomaly_timestamp:
                            time_between_anomalies = current_anomaly_timestamp - last_anomaly_timestamp
                            if time_between_anomalies < alert_interval:
                                try:
                                    discard_anomaly = [ts, value, consensus, algorithms_triggered]
                                    # This logs a lot if enabled
                                    # logger.info('debug :: time_between_anomalies %s is less than alert_interval %s, last_anomaly_timestamp set to %s and current_anomaly_timestamp is %s - discarding %s' % (
                                    #     str(time_between_anomalies), str(alert_interval),
                                    #     str(last_anomaly_timestamp),
                                    #     str(current_anomaly_timestamp), str(discard_anomaly)))
                                    alert_interval_discarded_anomalies.append(discard_anomaly)
                                    append_anomaly = False
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: falied to append to alert_interval_discarded_anomalies on %s' % str(timeseries_file))

                    # @modified 20200421 -  Feature #3500: webapp - crucible_process_metrics
                    #                       Feature #1448: Crucible web UI
                    # Only append if append_anomaly
                    # anomalies_score.append([ts, value, consensus, algorithms_triggered])
                    if append_anomaly:
                        anomalies_score.append([ts, value, consensus, algorithms_triggered])
                        # @modified 20200817 - Feature #3682: SNAB - webapp - crucible_process - run_algorithms
                        # if consensus >= CONSENSUS:
                        if consensus >= use_consensus:
                            last_anomaly_timestamp = int(ts)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: falied to process anomalies entry on %s' % str(timeseries_file))

        # @added 20200421 - Feature #3500: webapp - crucible_process_metrics
        #                   Feature #1448: Crucible web UI
        # Added alert_interval_discarded_anomalies
        if alert_interval:
            if alert_interval_discarded_anomalies:
                logger.info('info :: discarded %s anomalies due to them being within the alert_interval period on %s' % (str(len(alert_interval_discarded_anomalies)), str(timeseries_file)))
            else:
                logger.info('info :: no anomalies were discarded due to them being within the alert_interval period on %s' % str(timeseries_file))

        try:
            logger.info('info :: plotting skyline.consensus.anomalies.png for %s' % str(timeseries_file))
            x_vals = np.arange(len(timeseries))
            y_vals = np.array([y[1] for y in timeseries])
            # Match default graphite graph size
            plt.figure(figsize=(5.86, 3.08), dpi=100)
            plt.plot(x_vals, y_vals)
            for index in range(10, len(timeseries)):
                anomaly = False
                sliced = timeseries[:index]
                for i in anomalies_score:
                    if sliced[-1][0] == i[0]:
                        # @modified 20200817 - Feature #3682: SNAB - webapp - crucible_process - run_algorithms
                        # if i[2] >= CONSENSUS:
                        if i[2] >= use_consensus:
                            anomaly = True
                # Point out the datapoint if it is anomalous according to
                # Skyline CONSENSUS
                if anomaly:
                    plt.plot([index], [sliced[-1][1]], 'ro')
            results_filename = join(results_dir + "/skyline.consensus.anomalies.png")
            plt.savefig(results_filename, dpi=100)
            if python_version == 2:
                # @modified 20200327 - Branch #3262: py3
                # os.chmod(results_filename, 0644)
                os.chmod(results_filename, 0o644)
            if python_version == 3:
                os.chmod(results_filename, mode=0o644)
        except:
            logger.error('error :: %s' % (traceback.format_exc()))
            logger.error('error :: failed plotting skyline.consensus.anomalies.png for %s' % str(timeseries_file))

        anomalies_filename = join(results_dir + "/skyline.anomalies_score.txt")
        try:
            logger.info('info :: creating anomalies_filename - %s for %s' % (anomalies_filename, str(timeseries_file)))
            write_data_to_file(skyline_app, anomalies_filename, 'w', str(anomalies_score))
        except:
            logger.error('error :: %s' % (traceback.format_exc()))
            logger.error('error :: failed creating anomalies_filename - %s for %s' % (anomalies_filename, str(timeseries_file)))

        anomalies_csv = join(results_dir + "/skyline.anomalies.csv")
        logger.info('info :: creating anomalies_csv - %s for %s' % (anomalies_csv, str(timeseries_file)))
        try:
            with open(anomalies_csv, 'w') as fh:
                fh.write('timstamp,value,consensus_count,triggered_algorithms\n')
            for ts, value, consensus, algorithms_triggered in anomalies_score:
                try:
                    algos_str = str(algorithms_triggered)
                    triggered_algorithms = algos_str.replace(',', ' ')
                    line = '%s,%s,%s,%s\n' % (str(ts), str(value), str(consensus), str(triggered_algorithms))
                    with open(anomalies_csv, 'a') as fh:
                        fh.write(line)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: could not write to file %s for %s' % (anomalies_csv, str(timeseries_file)))
            if python_version == 2:
                # @modified 20200327 - Branch #3262: py3
                # os.chmod(anomalies_csv, 0644)
                os.chmod(anomalies_csv, 0o644)
            if python_version == 3:
                os.chmod(anomalies_csv, mode=0o644)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: could not write to file %s for %s' % (anomalies_csv, str(timeseries_file)))
        logger.info('info :: created anomalies_csv OK for %s' % str(timeseries_file))
        # @added 20200421 - Feature #3500: webapp - crucible_process_metrics
        #                      Feature #1448: Crucible web UI
        # Added alert_interval_discarded_anomalies
        alert_interval_discarded_anomalies_count = len(alert_interval_discarded_anomalies)
        if alert_interval_discarded_anomalies:
            alert_interval_discarded_anomalies_csv = join(results_dir + '/skyline.alert_interval_discarded_anomalies.csv')
            logger.info('info :: writing %s alert_interval discarded anomalies to %s for %s' % (
                str(len(alert_interval_discarded_anomalies)), alert_interval_discarded_anomalies_csv,
                str(timeseries_file)))
            try:
                with open(alert_interval_discarded_anomalies_csv, 'w') as fh:
                    fh.write('timstamp,value,consensus,triggered_algorithms\n')
                for ts, value, consensus, algorithms_triggered in alert_interval_discarded_anomalies:
                    try:
                        line = '%s,%s,%s,%s\n' % (str(ts), str(value), str(consensus), str(algorithms_triggered))
                        with open(alert_interval_discarded_anomalies_csv, 'a') as fh:
                            fh.write(line)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: could not write to file %s for %s' % (alert_interval_discarded_anomalies_csv, str(timeseries_file)))
                if python_version == 2:
                    os.chmod(alert_interval_discarded_anomalies_csv, 0o644)
                if python_version == 3:
                    os.chmod(alert_interval_discarded_anomalies_csv, mode=0o644)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: could not write to file %s for %s' % (alert_interval_discarded_anomalies_csv, str(timeseries_file)))
    else:
        logger.info('0 anomalies found for %s' % str(timeseries_file))

    return anomalous, triggered_algorithms, alert_interval_discarded_anomalies_count
