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
def downsample_timeseries(
        current_skyline_app, timeseries, current_resolution,
        required_resolution, method='mean', origin='end'):
    """
    Downsample the timeseries to resolution and align to resolution.  This is
    done by defualt using Panda Backward resample, using origin='end' which was
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
    try:
        df = timeseries_to_datetime_indexed_df(current_skyline_app, timeseries, False)
        # T = '%sT' % str(int(600 / 60))
        T = '%sT' % str(int(required_resolution / current_resolution))
        if method == 'mean':
            resampled_df = df.resample(T, origin=origin).mean()
        if method == 'sum':
            resampled_df = df.resample(T, origin=origin).sum()
        resampled_timeseries = list(zip(resampled_df.index.view(np.int64) // 10**9, resampled_df['value'].to_list()))
        # Align the periods to the resolution passed
        for ts, value in resampled_timeseries:
            # aligned_timeseries.append([int(int(ts) // 600 * 600), value])
            resampled_aligned_timeseries.append([int(int(ts) // required_resolution * required_resolution), value])
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to resample timeseries - %s' % (
            function_str, err))

    return resampled_aligned_timeseries
