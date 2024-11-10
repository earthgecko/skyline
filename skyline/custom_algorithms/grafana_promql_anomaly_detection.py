"""
grafana_promql_anomaly_detection.py
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
import numpy as np
import bottleneck as bn
import pandas as pd

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20241004 - Feature #5480: custom_algorithms - grafana_promql_anomaly_detection
def grafana_promql_anomaly_detection(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

    """
    grafana_promql_anomaly_detection

    A loose Python implementation of the Grafana PromQL Anomaly Detection method
    
    https://github.com/grafana/promql-anomaly-detection/tree/cd5a307ac7e44beb7e42299fe05cd71dd5647237
    
    https://grafana.com/blog/2024/10/03/how-to-use-prometheus-to-efficiently-detect-anomalies-at-scale/

    This implementation does not take into account the alert only if breached
    x times which in a running implementation on Grafana would be provided by
    the rules.

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
        custom_algorithm and algorithm itself.  For the 
        grafana_promql_anomaly_detection custom algorithm no specific
        algorithm_parameters are required apart from an empty dict but
        the algorithm_parameters that can be passed are:

        - ``'anomaly_window'`` (int): The anomaly_window value.
            This specifies how many of the last data points should be considered
            when determining if the metric is anomalous. Only the last
            ``anomaly_window`` data points in the time series will be used to
            determine if the metric is anomalous.  Default is ``1``.
        - ``'min_anomaly_duration_seconds'`` (int):
            The minimum number of a seconds that an anomaly must persist to
            trigger as an anomaly.  If min_anomaly_duration_seconds is set to 0
            or 1 this is VERY chatty algorithm. Default is ``300``.
        - ``'threshold_by_covar'`` (float): The sigma value to use.
            The threshold used for filtering low variability periods, in
            combination with the coefficient of variation.   Default is ``0.5``.
        - ``'sparse_threshold'`` (float):
            A threshold used to filter out sparse metrics, for which anomaly
            detection won't work correctly.  Default is ``0.083333``.
        - ``'stddev_multiplier'`` (int):
            The stddev multiplier for short term and long term bands. sigma.
            Default is ``2``.
        - ``'margin_multiplier'`` (float):
            The stddev multiplier for margin bands.  Default is ``0.5``.
        - ``'short_window'`` (int):
            The mid-line that used as the reference to calculate upper and lower
            bands.  1 hour is a good compromise between smoothness and
            responsiveness.  The short_window defaults to 1h.  Default is ``3600``.
        - ``'smoothing_window_seconds'`` (int):
            The default is 26 hours which takes into account 24 hour patterns.
            This is used to smooth the filtered stddev 1h to get the final short
            term bands and combination with the filtered standard deviation, it
            provides robust and stable bands that expand gently in the presence
            of extreme outliers.  Default is ``93600``.
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
                'min_anomaly_duration_seconds': 300,
                'threshold_by_covar': 0.5,
                'sparse_threshold': 0.083333,
                'stddev_multiplier': 2,
                'margin_multiplier': 0.5,
                'short_window': 3600,
                'smoothing_window_seconds': 93600,
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
    algorithm_name = 'grafana_promql_anomaly_detection'

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = None
    results = {'preprocessing': {}, 'timings': {}}

    current_logger = None

    # If you wanted to log, you can but this should only be done during
    # testing and development
    def get_log(current_skyline_app):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        return current_logger

    def get_resolution(timeseries):
        np_timestamps_array = np.array([t for t, v in timeseries])
        ts_diffs = np.diff(np_timestamps_array)
        resolution_counts = np.unique(ts_diffs, return_counts=True)
        resolution = resolution_counts[0][np.argmax(resolution_counts[1])]
        return resolution

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
    print_debug = None
    try:
        print_debug = algorithm_parameters['print_debug']
    except:
        print_debug = False

    anomaly_window = 1
    try:
        anomaly_window = int(algorithm_parameters['anomaly_window'])
    except:
        anomaly_window = 1
    # The minimum number of a seconds that an anomaly must persist to trigger
    # as an anomaly.  If min_anomaly_duration_seconds is set to 0 or 1 this is
    # VERY chatty algorithm.
    min_anomaly_duration_seconds = 300
    try:
        min_anomaly_duration_seconds = int(algorithm_parameters['min_anomaly_duration_seconds'])
    except:
        min_anomaly_duration_seconds = 300
    # The threshold used for filtering low variability periods, in combination
    # with the coefficient of variation.
    threshold_by_covar = 0.5
    try:
        threshold_by_covar = int(algorithm_parameters['threshold_by_covar'])
    except:
        threshold_by_covar = 1
    # A threshold used to filter out sparse metrics, for which anomaly detection
    # won't work correctly.
    sparse_threshold = 5 / 60
    try:
        sparse_threshold = float(algorithm_parameters['sparse_threshold'])
    except:
        sparse_threshold = 5 / 60
    # The stddev multiplier for short term and long term bands. sigma
    stddev_multiplier = 2
    try:
        stddev_multiplier = int(algorithm_parameters['stddev_multiplier'])
    except:
        stddev_multiplier = 2
    sigma = int(stddev_multiplier)
    # The stddev multiplier for margin bands.
    margin_multiplier = 0.5
    try:
        margin_multiplier = float(algorithm_parameters['margin_multiplier'])
    except:
        margin_multiplier = 0.5
    # The mid-line that used as the reference to calculate upper and lower bands.
    # 1 hour is a good compromise between smoothness and responsiveness.
    # The short_window defaults to 1h
    short_window = 3600
    try:
        short_window = int(algorithm_parameters['short_window'])
    except:
        short_window = 3600
    # The default is 26 hours which takes into account 24 hour patterns.  This
    # is used to smooth the filtered stddev 1h to get the final short term bands
    # and combination with the filtered standard deviation, it provides robust
    # and stable bands that expand gently in the presence of extreme outliers.
    smoothing_window_seconds = 93600
    try:
        smoothing_window_seconds = int(algorithm_parameters['smoothing_window_seconds'])
    except:
        smoothing_window_seconds = 93600

    # Used to determine whether to use the median or mean in any resampling that
    # may be carried out
    low_variance = 0.009

    resolution = 0
    try:
        resolution = get_resolution(timeseries)
    except Exception as err:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if print_debug:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error - on grafana_promql_anomaly_detection get_resolution, err: %s' % err)
            current_logger.debug(traceback.format_exc())

    # Preprocess the timeseries, algining all timestamps to the resolution and
    # fill any missing timestamps and values with the median of the last 3 data
    # points (an arbitary choice).  This is to ensure that the move_mean,
    # move_std and subsequent cov calculations are made on the the same length
    # data and do not have any nan values with will result in any
    # RuntimeWarning: invalid value encountered in divide when calculating
    # cov = moving_std / moving_avg
    original_timeseries = list(timeseries)
    original_resolution = int(resolution)
    aligned_timeseries = [[int(ts // resolution * resolution), v] for ts, v in timeseries]
    if len(aligned_timeseries) > 0:
        timeseries = list(aligned_timeseries)

    timeseries_duration = timeseries[-1][0] - timeseries[0][0]
    if timeseries_duration < (86400 * 5.5):
        if return_results:
            message = 'time series duration of %s seconds to short' % str(int(timeseries_duration))
            results = {'message': message}
            return (None, None, results)
        return (None, None)

    timeseries_np = np.array(timeseries)
    timestamps = timeseries_np[:, 0].astype(np.int64)
    values = timeseries_np[:, 1].astype(np.float64)

    # For algorithms to be quick the data is downsampled if it is at a high
    # resolution. The creation of the df AND then resampling takes on average
    # 33 ms with a timeseries of length 10075 (7 days at 60s)
    timeseries_resampled = False
    resampled_timeseries = []
    mean_resampled_timeseries = []
    data_points_per_period = 1
    if resolution < 600:
        normalised_var = np.nan
        err = None
        try:
            np_max = np.amax(values)
            np_min = np.amin(values)
            norm_np_values = (values - np_min) / (np_max - np_min)
            normalised_var = round(np.var(norm_np_values), 4)
        except Exception as err:
            normalised_var = np.nan
            err = str(err)
        aggregate_on_mean = False
        if isinstance(normalised_var, float):
            if normalised_var <= low_variance:
                aggregate_on_mean = True
        start_df = time()
        df = None
        df = pd.DataFrame(timeseries, columns=['date', 'value'])
        df['date'] = pd.to_datetime(df['date'], unit='s')
        datetime_index = pd.DatetimeIndex(df['date'].values)
        df = df.set_index(datetime_index)
        df.drop('date', axis=1, inplace=True)
        results['timings']['df_creation'] = time() - start_df
        start_df_resample = time()
        if debug_logging:
            current_logger.info('debug :: grafana_promql_anomaly_detection :: resampling timeseries of length: %s, from resolution: %s seconds to 600 seconds' % (
                str(len(timeseries)), str(resolution)))
        data_points_per_period = 600 / resolution
        T = '%sT' % str(data_points_per_period)
        # Just be opinionated and set
        T = '10T'
        # Resampling is done on the median to ensure more robustness to
        # outliers, unless there is very little variance in the data and
        # then median can result in all values being 0, a check is done.
        # Use bfill to ensure no nans are present
        resampled_df = df.resample(T, origin='end').median().bfill()
        resampled_values = resampled_df['value'].to_numpy().tolist()
        # Determine whether the median resample has resulted in all 0s or
        # the data has a low variance, if so use the mean
        non_zero_value_count = len([v for v in resampled_values if v != 0])
        if non_zero_value_count <= 10 or aggregate_on_mean:
            mean_resampled_df = df.resample(T, origin='end').mean().bfill()
            resampled_df = mean_resampled_df.copy()
            mean_resampled_values = mean_resampled_df['value'].to_numpy().tolist()
            mean_resampled_df['ts'] = mean_resampled_df.index.astype(np.int64) // 10**9
            if print_debug:
                print('resampled on mean because median resample was 0s or low_variance: %s' % str(normalised_var))
            if debug_logging:
                current_logger.info('resampled on mean because median resample was 0s or low_variance: %s' % str(normalised_var))
            mean_resampled_timestamps = mean_resampled_df['ts'].to_numpy().tolist()
            mean_resampled_timeseries = [[t, mean_resampled_values[index]] for index, t in enumerate(mean_resampled_timestamps)]
        resampled_ts_df = resampled_df.copy()
        resampled_ts_df['ts'] = resampled_ts_df.index.astype(np.int64) // 10**9

        resampled_timestamps = resampled_ts_df['ts'].to_numpy().tolist()
        resampled_timeseries = [[t, resampled_values[index]] for index, t in enumerate(resampled_timestamps)]
        results['timings']['df_resample'] = time() - start_df_resample
        results['preprocessing']['resampled'] = True

    if resampled_timeseries:
        if print_debug:
            print('resampled_timeseries length:', len(resampled_timeseries))
        if debug_logging:
            current_logger.info('resampled_timeseries length: %s' % str(len(resampled_timeseries)))
        timeseries_resampled = True
        timeseries = list(resampled_timeseries)
        resolution = 600
        timeseries_np = np.array(timeseries)
        timestamps = timeseries_np[:, 0].astype(np.int64)
        values = timeseries_np[:, 1].astype(np.float64)

    expected_timestamps = np.arange(timestamps[0], timestamps[-1] + resolution, resolution)
    expected_values = np.full_like(expected_timestamps, np.nan, dtype=np.float64)
    # Populate the expected_values with existing values
    expected_values[np.isin(expected_timestamps, timestamps)] = values
    # Interpolate missing values using the median of the last 3 data points
    missing_values = False
    for index in range(len(expected_values)):
        value = expected_values[index]
        if np.isnan(value):
            valid_values = expected_values[max(0, index - 3):index][~np.isnan(expected_values[max(0, index - 3):index])]
            if len(valid_values) > 0:
                value = np.median(valid_values)
            expected_values[index] = value
            missing_values = True
    if missing_values:
        # preprocessed_timeseries_array = np.column_stack((expected_timestamps, expected_values))
        timeseries = [[int(ts), float(expected_values[index])] for index, ts in enumerate(expected_timestamps)]
        if debug_logging:
            current_logger.debug('debug :: grafana_promql_anomaly_detection :: interpolated %s missing values' % str(len(timeseries) - len(original_timeseries)))

    if debug_logging:
        current_logger.debug('debug :: grafana_promql_anomaly_detection :: preprocessed timeseries')

    results['timings']['preprocessing'] = time() - start

    try:
        timestamps = np.array([t for t, v in timeseries])
        values = np.array([v for t, v in timeseries])
        # window = int(3600 / resolution)
    
        # Calculate the mid-line 1h mid-line and 26h smoothing window sizes
        avg_window_samples = int(short_window / resolution)
        std_smoothing_window_samples = int(smoothing_window_seconds / resolution)

        # Calculate moving average and std deviation
        moving_avg =  bn.move_mean(values, window=avg_window_samples, min_count=1)
        moving_std = bn.move_std(values, window=avg_window_samples, min_count=1)
        
        # RuntimeWarning: invalid value encountered in divide
        # Handle 0 and nan in moving_avg before division by
        # replacing them with a tiny value constant to replace zeros and NaNs
        moving_avg_cleaned = np.where(np.isnan(moving_avg) | (moving_avg == 0), 1e-10, moving_avg)

        # Coefficient of variation
        cov = moving_std / moving_avg_cleaned

        # High-pass filter
        # Stddev with a one hour time window and a high pass filter applied to it is
        # used as the basis for anomaly detection.
        # The stddev_1h acts as a measure of variability.
        # The high pass filter is used to increase its sensitivity by focusing on
        # periods of high variability. It's calculation is based on the coefficient
        # of variation (relative standard deviation), which allows defining a
        # threshold that will work for metrics of any magnitude.
        filtered_std = np.where(cov > threshold_by_covar, moving_std, 0)

        # Smooth the filtered standard deviation
        # The filtered stddev_1h is smoothed over a 26 hour window to get the final
        # short term bands. In combination with the filtered standard deviation, it
        # provides robust and stable bands that expand gently in the presence of
        # extreme outliers.
        smoothed_std = bn.move_mean(filtered_std, window=std_smoothing_window_samples, min_count=1)

        # Calculate anomaly bands
        # Short term, margin and long term bands are combined to form the final
        # bands used for anomaly detection.
        # Margin bands provide minimum width bands and are purely based on the
        # avg_1h. They act as a fallback band for periods in which short term bands
        # do not exist due to very low variability.
        lower_band = moving_avg - smoothed_std * stddev_multiplier
        upper_band = moving_avg + smoothed_std * stddev_multiplier
        # Apply margin bands where std is zero
        margin = moving_avg * margin_multiplier
        lower_band = np.where(smoothed_std == 0, moving_avg - margin, lower_band)
        upper_band = np.where(smoothed_std == 0, moving_avg + margin, upper_band)
        # Ensure lower_band is not negative
        lower_band = np.clip(lower_band, a_min=0, a_max=None)

        # Handle NaNs by filling them with appropriate values
        values_filled = np.nan_to_num(values, nan=np.nanmean(values))
        lower_band_filled = np.nan_to_num(lower_band, nan=np.nanmean(lower_band))
        upper_band_filled = np.nan_to_num(upper_band, nan=np.nanmean(upper_band))
        # Detect anomalies
        gpad_anomalies = (values_filled < lower_band_filled) | (values_filled > upper_band_filled)
        # Only classify anomalies if anomalies occur for longer than 
        anomaly_periods = []
        i = 0
        n = len(gpad_anomalies)
        while i < n:
            if gpad_anomalies[i]:
                # Start of an anomaly period
                start_time = expected_timestamps[i]
                # Continue until the anomaly condition ends
                while i + 1 < n and gpad_anomalies[i + 1]:
                    i += 1
                end_time = expected_timestamps[i]
                duration = end_time - start_time
                if duration >= min_anomaly_duration_seconds:
                    # Ensure these are coerced to a json friendly int not np.int64
                    anomaly_periods.append((int(start_time), int(end_time)))
            i += 1
        
        anomalies = {}
        indexed_timeseries = {}
        for index, ts in enumerate(expected_timestamps):
            # Ensure these are coerced to a json friendly values not NaN,
            # np.int64 or np.float64
            ts_int = int(ts)
            value = float(values[index])
            indexed_timeseries[ts_int] = {'index': index, 'value': value}
        for start_ts, end_ts in anomaly_periods:
            anomalies[start_ts] = {'index': indexed_timeseries[start_ts]['index'], 'value': indexed_timeseries[start_ts]['value'], 'score': 1}
        results['anomalies'] = anomalies
        anomaly_timestamps = list(anomalies.keys())
        anomalyScore_list = []
        for index, ts in enumerate(expected_timestamps):
            score = 0
            if ts in anomaly_timestamps:
                score = 1
            anomalyScore_list.append(score)

        anomaly_sum = sum(anomalyScore_list[-anomaly_window:])
        if anomaly_sum > 0:
            anomalous = True
        else:
            anomalous = False
        results['anomalous'] = anomalous
        results['anomalyScore_list'] = anomalyScore_list
        results['scores'] = anomalyScore_list
        results['timings']['total'] = time() - start

        if print_debug:
            print('ran grafana_promql_anomaly_detection OK in %.6f seconds' % (time() - start))
        if debug_logging:
            current_logger.debug('debug :: ran grafana_promql_anomaly_detection OK in %.6f seconds' % (time() - start))
        if results:
            if results['anomalous']:
                anomalous = True
                anomalyScore = 1.0
            else:
                anomalous = False
                anomalyScore = 0.0
            if print_debug:
                print('anomalous: %s' % str(anomalous))
            if debug_logging:
                current_logger.debug('debug :: anomalous: %s' % str(anomalous))
        else:
            if print_debug:
                print('error - no results')
            if debug_logging:
                current_logger.debug('debug :: error - no results')

    except StopIteration:
        if print_debug:
            print('warning - StopIteration called on grafana_promql_anomaly_detection')
        if debug_logging:
            current_logger.debug('debug :: warning - StopIteration called on grafana_promql_anomaly_detection')

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
        if print_debug:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error - on grafana_promql_anomaly_detection, err: %s' % err)
            current_logger.debug(traceback.format_exc())

        # Return None and None as the algorithm could not determine True or False
        if return_results:
            return (None, None, None)
        return (None, None)

    if return_results:
        return (anomalous, anomalyScore, results)

    return (anomalous, anomalyScore)
