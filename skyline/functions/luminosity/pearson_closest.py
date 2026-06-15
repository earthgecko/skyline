"""
pearson_closest.py
"""
# REQUIRED Skyline imports.  All custom algorithms MUST have the following two
# imports.  These are required for exception handling and to record algorithm
# errors regardless of debug_logging setting for the custom_algorithm
import copy
from itertools import combinations
import logging
from os import getpid
from random import shuffle
from time import time
import traceback
from custom_algorithms import record_algorithm_error

# Import ALL modules that the custom algorithm requires.  Remember that if a
# requirement is not one that is provided by the Skyline requirements.txt you
# must ensure it is installed in the Skyline virtualenv
import pandas as pd
import numpy as np

from functions.redis.get_metric_timeseries import get_metric_timeseries

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20230803 - Feature #5026: luminosity - pearson_closest
def pearson_closest(
        current_skyline_app, metric1, metric2,
        # @added 20230908
        # Added the ability to pass timestamps and downsample in mins
        from_timestamp=None, until_timestamp=None, downsample=None,
        datapoints=False, print_debug=True, return_results=False,
        return_timeseries=False, metric_timeseries=None, metrics_timeseries={},
        metrics_timeseries_use_key='timeseries'):
    """
    Run correlations on timeseries.
    
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
        custom_algorithm and algorithm itself.  For the irregular_unstable
        custom algorithm no specific algorithm_parameters are required apart
        from an empty dict, example: ``algorithm_parameters={}``.  But the
        number_of_daily_peaks can be passed define how many peaks must exist in
        the window period to be classed as normal.  If this is set to 3 and say
        that we are checking a possible anomaly at 00:05, there need to be 3
        peaks that occur over the past 7 days in the dialy 23:35 to 00:05 window
        if there are not at least 3 then this is considered as anomalous.
        ``algorithm_parameters={'number_of_daily_peaks': 3}``
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: anomalous, anomalyScore
    :rtype: tuple(boolean, float)

    """

    # You MUST define the algorithm_name
    algorithm_name = 'pearson_closest'

    # If you wanted to log, you can but this should only be done during
    # testing and development
    def get_log(current_skyline_app):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        return current_logger

    start = time()

    def get_resolution(np_timestamps_array):
        ts_diffs = np.diff(np_timestamps_array)
        resolution_counts = np.unique(ts_diffs, return_counts=True)
        resolution = resolution_counts[0][np.argmax(resolution_counts[1])]
        return resolution

    pearson_cc = None
    reason = None
    results = {}
    results['pearson_cc'] = pearson_cc
    results['timings'] = {}
    start = time()

    if metrics_timeseries:
        if not metric_timeseries:
            try:
                metric_timeseries = metrics_timeseries[metric1][metrics_timeseries_use_key]
            except:
                pass        

    if not metric_timeseries:
        # @modified 20230908
        # Added the ability to pass timestamps
        # t1 = get_metric_timeseries(current_skyline_app, metric1)
        if from_timestamp and until_timestamp:
            t1 = get_metric_timeseries(current_skyline_app, metric1, from_timestamp=from_timestamp, until_timestamp=until_timestamp)
        else:
            t1 = get_metric_timeseries(current_skyline_app, metric1)
        results['timings']['t1'] = time() - start
    else:
        t1 = list(metric_timeseries)
    metric_timeseries = list(t1)

    # @added 20230908
    # Added the ability to downsample in mins
    if downsample:
        start_t1_downsample = time()
        T = '%sT' % str(downsample)
        # @added 20260227 - Task #5628: Build v5.0.0 and test
        # changed T to min as per warning in pandas 2.2.3
        T = '%smin' % str(downsample)

        df = pd.DataFrame(metric_timeseries, columns=['date', 'value'])
        df['date'] = pd.to_datetime(df['date'], unit='s')
        datetime_index = pd.DatetimeIndex(df['date'].values)
        df = df.set_index(datetime_index)
        df.drop('date', axis=1, inplace=True)
        mean_resampled_df = df.resample(T, origin='end').mean().bfill()
        mean_resampled_values = mean_resampled_df['value'].to_numpy().tolist()
        # @modified 20260224 - Task #5628: Build v5.0.0 and test
        #                      Task #5710: utcfromtimestamp - deprecated datetime and pandas
        #                      Task #5526: Build v5.0.0 and upgrade deps
        #                      Task #5627: v5.0.0 update dependencies
        # Handle pandas deprecation which results in all timestamps being
        # returned as 1.  From pandas changelog:
        # https://pandas.pydata.org/docs/whatsnew/v3.0.0.html#whatsnew-300-prior-deprecations
        # Disallow passing a pandas type to Index.view() (GH 55709)
        # https://github.com/pandas-dev/pandas/issues/55709
        #mean_resampled_df['ts'] = mean_resampled_df.index.astype(np.int64) // 10**9
        mean_resampled_df['ts'] = [int(ts.value // 10**9) for ts in mean_resampled_df.index]

        mean_resampled_timestamps = mean_resampled_df['ts'].to_numpy().tolist()
        t1 = [[t, mean_resampled_values[index]] for index, t in enumerate(mean_resampled_timestamps)]
        results['timings']['t1_downsample'] = time() - start_t1_downsample

    if datapoints:
        t1 = t1[-datapoints:]
    results['len(t1)'] = len(t1)
    if not t1:
        reason = 'no data for %s' % metric1
        results['reason'] = reason
        if print_debug:
            print(results, '(took', (time() - start), 'seconds')
        return None, reason
    start_t1_res = time()
    t1_resolution = get_resolution(np.array([int(t) for t, v in t1]))
    results['t1_resolution'] = t1_resolution
    results['timings']['t1_resolution'] = time() - start_t1_res

    t2 = []
    if metrics_timeseries:
        try:
            t2 = metrics_timeseries[metric2][metrics_timeseries_use_key]
        except:
            pass
    if not t2:
        start_t2 = time()
        # @modified 20230908
        # Added the ability to pass timestamps
        # t2 = get_metric_timeseries(current_skyline_app, metric2)
        if from_timestamp and until_timestamp:
            t2 = get_metric_timeseries(current_skyline_app, metric2, from_timestamp=from_timestamp, until_timestamp=until_timestamp)
        else:
            t2 = get_metric_timeseries(current_skyline_app, metric2)
        results['timings']['t2'] = time() - start_t2

    # @added 20230908
    # Added the ability to downsample in mins
    if downsample:
        start_t2_downsample = time()
        df = pd.DataFrame(t2, columns=['date', 'value'])
        df['date'] = pd.to_datetime(df['date'], unit='s')
        datetime_index = pd.DatetimeIndex(df['date'].values)
        df = df.set_index(datetime_index)
        df.drop('date', axis=1, inplace=True)
        mean_resampled_df = df.resample(T, origin='end').mean().bfill()
        mean_resampled_values = mean_resampled_df['value'].to_numpy().tolist()
        # @modified 20260224 - Task #5628: Build v5.0.0 and test
        #                      Task #5710: utcfromtimestamp - deprecated datetime and pandas
        #                      Task #5526: Build v5.0.0 and upgrade deps
        #                      Task #5627: v5.0.0 update dependencies
        # Handle pandas deprecation which results in all timestamps being
        # returned as 1.  From pandas changelog:
        # https://pandas.pydata.org/docs/whatsnew/v3.0.0.html#whatsnew-300-prior-deprecations
        # Disallow passing a pandas type to Index.view() (GH 55709)
        # https://github.com/pandas-dev/pandas/issues/55709
        #mean_resampled_df['ts'] = mean_resampled_df.index.astype(np.int64) // 10**9
        mean_resampled_df['ts'] = [int(ts.value // 10**9) for ts in mean_resampled_df.index]

        mean_resampled_timestamps = mean_resampled_df['ts'].to_numpy().tolist()
        t2 = [[t, mean_resampled_values[index]] for index, t in enumerate(mean_resampled_timestamps)]
        results['timings']['t2_downsample'] = time() - start_t2_downsample

    if datapoints:
        t2 = t2[-datapoints:]
    results['len(t2)'] = len(t2)
    if not t2:
        reason = 'no data for %s' % metric2
        results['reason'] = reason
        if print_debug:
            print(results, '(took', (time() - start), 'seconds')
        if return_results:
            if return_timeseries:
                return pearson_cc, reason, results, metric_timeseries
            else:
                return pearson_cc, reason, results
        if return_timeseries:
            return pearson_cc, reason, metric_timeseries
        return None, reason
    start_t2_res = time()
    t2_resolution = get_resolution(np.array([int(t) for t, v in t2]))
    results['t2_resolution'] = t2_resolution
    results['timings']['t2_resolution'] = time() - start_t2_res
    if t1_resolution != t2_resolution:
        reason = 'different resolution'
        results['reason'] = reason
        if print_debug:
            print(results, '(took', (time() - start), 'seconds')
        if return_results:
            if return_timeseries:
                return pearson_cc, reason, results, metric_timeseries
            else:
                return pearson_cc, reason, results
        if return_timeseries:
            return pearson_cc, reason, metric_timeseries
        return None, reason

    unaligned_t1 = list(t1)
    unaligned_t2 = list(t2)

    # Using np alignments is quicker than list comprehension
    # start_ts_alignment = time()
    # t1 = [[int(int(t) // t1_resolution * t1_resolution), v] for t, v in t1]
    # t2 = [[int(int(t) // t2_resolution * t2_resolution), v] for t, v in t2]
    # results['timings']['ts_alignment'] = time() - start_ts_alignment

    # Timing samples
    # 'ts_alignment': 0.0044155120849609375,  # seconds
    # 'np_ts_alignment': 0.0022211074829101562,  # seconds
    # 'ts_alignment': 0.0033349990844726562,
    # 'np_ts_alignment': 0.0028951168060302734,
    # Use np vectorised calculation
    start_np_ts_alignment = time()
    t1_np_timestamps = np.array([int(t) for t, v in unaligned_t1])
    t1_np_values = np.array([v for t, v in unaligned_t1])
    t2_np_timestamps = np.array([int(t) for t, v in unaligned_t2])
    t2_np_values = np.array([v for t, v in unaligned_t2])
    t1_np_aligned_timestamps = t1_np_timestamps // (t1_resolution * t1_resolution)
    t2_np_aligned_timestamps = t2_np_timestamps // (t2_resolution * t2_resolution)
    # Use np vectorised calculations
    # t1_np_aligned = zip(t1_np_aligned_timestamps.tolist(), t1_np_values.tolist())
    # t2_np_aligned = zip(t2_np_aligned_timestamps.tolist(), t2_np_values.tolist())
    t1 = list(zip(t1_np_aligned_timestamps.tolist(), t1_np_values.tolist()))
    t2 = list(zip(t2_np_aligned_timestamps.tolist(), t2_np_values.tolist()))
    results['timings']['np_ts_alignment'] = time() - start_np_ts_alignment

    use_align = True
    if use_align:
        start_align = time()
        t2_timestamps_dict = {}
        for index, item in enumerate(t2):
            try:
                ts2 = int(item[0])
            except Exception as err:
                print('err:', err, 'item', str(item))
                print(t2[0:4])
                break
            t2_timestamps_dict[ts2] = index
        t2_timestamps = list(t2_timestamps_dict.keys())
        aligned_t1 = []
        aligned_t2 = []
        for index, item in enumerate(t1):
            ts1 = int(item[0])
            if ts1 in t2_timestamps:
                aligned_t1.append(item)
                t2_index = t2_timestamps_dict[int(item[0])]
                aligned_t2.append(t2[t2_index])
        results['timings']['align'] = time() - start_align

    start_intersection_align = time()
    common_timestamps = list(set([t for t, v in t1]).intersection([t for t, v in t2]))
    np_aligned_t1 = [item for item in t1 if item[0] in common_timestamps]
    np_aligned_t2 = [item for item in t2 if item[0] in common_timestamps]
    results['timings']['intersection_align'] = time() - start_intersection_align

    results['len(aligned_t1)'] = len(aligned_t1)
    results['len(aligned_t2)'] = len(aligned_t2)

    # @added 20230908 - use np_aligned data, not sure why I was not
    results['len(np_aligned_t1)'] = len(np_aligned_t1)
    results['len(np_aligned_t2)'] = len(np_aligned_t2)

    if len(aligned_t1) < 10 or len(aligned_t2) < 10:
        reason = 'too short'
        results['reason'] = reason
        if print_debug:
            print(results, '(took', (time() - start), 'seconds')
        if return_results:
            if return_timeseries:
                return pearson_cc, reason, results, metric_timeseries
            else:
                return pearson_cc, reason, results
        if return_timeseries:
            return pearson_cc, reason, metric_timeseries
        return None, reason
    # minmax not required for Pearson as it is essentially a normalised
    # measurement of the covariance
    # mm_t1 = timeseries_numba_minmax(aligned_t1)
    # mm_t2 = timeseries_numba_minmax(aligned_t2)
    start_pd_corr = time()
    # df = pd.DataFrame([v for t, v in mm_t1], columns=['t1'])
    # df['t2'] = np.array([v for t, v in mm_t2])

    # @modified 20230908 - use np_aligned data, not sure why I was not
    # df = pd.DataFrame([v for t, v in aligned_t1], columns=['t1'])
    # df['t2'] = np.array([v for t, v in aligned_t2])
    df = pd.DataFrame([v for t, v in np_aligned_t1], columns=['t1'])
    df['t2'] = np.array([v for t, v in np_aligned_t2])

    df_corr = df.corr(method='pearson', min_periods=3)
    pearson_cc = df_corr['t1']['t2']
    results['timings']['df.corr'] = time() - start_pd_corr
    results['pearson_cc'] = float(pearson_cc)
    results['timings']['total'] = time() - start
    if print_debug:
        print(results, '(took', (time() - start), 'seconds')
    if return_results:
        if return_timeseries:
            return pearson_cc, reason, results, metric_timeseries
        else:
            return pearson_cc, reason, results
    if return_timeseries:
        return pearson_cc, reason, metric_timeseries
    return pearson_cc, reason
