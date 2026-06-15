"""
determine_data_frequency
"""
import logging
import traceback
import numpy as np
import pandas as pd

from functions.pandas.timeseries_to_datetime_indexed_df import timeseries_to_datetime_indexed_df


# @added 20220414 - Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
#                   Task #3868: POC MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
# @modified 20241111 - Task #5526: Build v5.0.0 and upgrade deps
#                      Branch #5532: v5.0.0-alpha
# Added bfill argument to facilitate replacing Nans to ensure no nans are
# present as some unsupervised algorithms does not accept missing values encoded
# as NaN
def downsample_timeseries(
        current_skyline_app, timeseries, current_resolution,
        required_resolution, method='mean', origin='end', bfill=False):
    """
    Downsample the timeseries to resolution and align to resolution.  This is
    done by default using Panda Backward resample, using origin='end' which was
    introduced in version 1.3, see
    https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#backward-resample
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.resample.html

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param timeseries: the timeseries as a list [[1618761782, 123],...,[1618848182, 78]]
    :param current_resolution: the resolution of the timeseries
    :param required_resolution: the resolution to downsample to
    :param method: the Pandas resample method to use, mean or sum (for derivative metrics)
    :param origin: the Pandas resample origin parameter to use
    :param bfill: whether to apply backfill to fill any NaN values by carrying
        the next valid (non-NaN) value backwards to fill gaps.
    :type current_skyline_app: str
    :type timeseries: list
    :type current_resolution: int
    :type required_resolution: int
    :type method: str
    :type origin: str
    :type log: boolean
    :return: timeseries
    :rtype:  list
    """
    function_str = 'functions.timeseries.downsample'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    resampled_aligned_timeseries = []
    resampled_df = []
    try:
        df = timeseries_to_datetime_indexed_df(current_skyline_app, timeseries, False)
        # T = '%sT' % str(int(600 / 60))
        T = '%sT' % str(int(required_resolution / current_resolution))

        # @added 20240125 - Task #5178: Build and test skyline v4.1.0
        # Handle downsample to 60 seconds as well
        # @modified 20241111 - Task #5526: Build v5.0.0 and upgrade deps
        #                      Branch #5532: v5.0.0-alpha
        # changed T to min as per warning in pandas 2.2.3
        # FutureWarning: 'T' is deprecated and will be removed in a future version, please use 'min' instead.
        if required_resolution == 600:
            T = '10T'
            T = '10min'
        if required_resolution == 900:
            T = '15T'
            T = '15min'
        if required_resolution == 1200:
            T = '20T'
            T = '20min'
        if required_resolution == 1800:
            T = '30T'
            T = '30min'
        if required_resolution == 3600:
            T = '60T'
            T = '60min'
        if required_resolution <= 60 and current_resolution < 60:
            if required_resolution == 60:
                T = '60S'
                T = '1min'

        if method == 'mean':
            try:
                # @modified 20241111 - Task #5526: Build v5.0.0 and upgrade deps
                #                      Branch #5532: v5.0.0-alpha
                # Added bfill argument
                if not bfill:
                    resampled_df = df.resample(T, origin=origin).mean()
                else:
                    resampled_df = df.resample(T, origin=origin).mean().bfill()

            # @added 20231228 - Task #5178: Build and test skyline v4.1.0
            # Handle ZeroDivisionError
            except ZeroDivisionError:
                try:
                    # @modified 20241111 - Task #5526: Build v5.0.0 and upgrade deps
                    #                      Branch #5532: v5.0.0-alpha
                    # Added bfill argument
                    if not bfill:
                        resampled_df = df.resample(T, origin=origin).median()
                    else:
                        resampled_df = df.resample(T, origin=origin).median().bfill()
                except Exception as err:
                    current_logger.error('error :: %s :: failed to resample timeseries on median after mean fail - %s' % (
                        function_str, err))

        if method == 'sum':
            try:
                # @modified 20241111 - Task #5526: Build v5.0.0 and upgrade deps
                #                      Branch #5532: v5.0.0-alpha
                # Added bfill argument
                if not bfill:
                    resampled_df = df.resample(T, origin=origin).sum()
                else:
                    resampled_df = df.resample(T, origin=origin).sum().bfill()
            except Exception as err:
                current_logger.error('error :: %s :: failed to resample timeseries on sum - %s' % (
                    function_str, err))

        if method == 'median':
            try:
                # @modified 20241111 - Task #5526: Build v5.0.0 and upgrade deps
                #                      Branch #5532: v5.0.0-alpha
                # Added bfill argument
                if not bfill:
                    resampled_df = df.resample(T, origin=origin).median()
                else:
                    resampled_df = df.resample(T, origin=origin).median().bfill()
            except Exception as err:
                current_logger.error('error :: %s :: failed to resample timeseries on median - %s' % (
                    function_str, err))

        if len(resampled_df) > 0:
            # @modified 20260224 - Task #5628: Build v5.0.0 and test
            #                      Task #5710: utcfromtimestamp - deprecated datetime and pandas
            #                      Task #5526: Build v5.0.0 and upgrade deps
            #                      Task #5627: v5.0.0 update dependencies
            # Handle pandas deprecation which results in all timestamps being
            # returned as 1.  From pandas changelog:
            # https://pandas.pydata.org/docs/whatsnew/v3.0.0.html#whatsnew-300-prior-deprecations
            # Disallow passing a pandas type to Index.view() (GH 55709)
            # https://github.com/pandas-dev/pandas/issues/55709
            #resampled_timeseries = list(zip(resampled_df.index.view(np.int64) // 10**9, resampled_df['value'].to_list()))
            timestamps = [int(ts.value // 10**9) for ts in resampled_df.index]
            resampled_timeseries = list(zip(timestamps, resampled_df['value'].to_list()))

            # Align the periods to the resolution passed
            for ts, value in resampled_timeseries:
                # aligned_timeseries.append([int(int(ts) // 600 * 600), value])
                resampled_aligned_timeseries.append([int(int(ts) // required_resolution * required_resolution), value])
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to resample timeseries, current_resolution: %s, required_resolution: %s, err: %s' % (
            function_str, str(current_resolution), str(required_resolution), err))

    return resampled_aligned_timeseries
