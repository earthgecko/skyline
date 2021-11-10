"""
Convert a timeseries list to a datetime indexed DataFrame
"""
import logging
import traceback

import pandas as pd


# @added 20210627 - Branch #3590: inference
def timeseries_to_datetime_indexed_df(current_skyline_app, timeseries, log=False):
    """
    Convert a timeseries list to a datetime indexed DataFrame

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param timeseries: the timeseries as a list [[1618761782, 123],...,[1618848182, 78]]
    :param log: whether to log or not, optional, defaults to False
    :type current_skyline_app: str
    :type timeseries: list
    :type log: boolean
    :return: df
    :rtype:  DataFrame

    Example df.info() showing the created DataFrame metadata
    <class 'pandas.core.frame.DataFrame'>
    DatetimeIndex: 12959 entries, 2021-03-28 13:40:00 to 2021-06-26 13:20:00
    Data columns (total 1 columns):
     #   Column  Non-Null Count  Dtype
    ---  ------  --------------  -----
     0   value   12959 non-null  float64
    dtypes: float64(1)
    memory usage: 718.5 KB

    """
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    function_str = 'functions.pandas.timeseries_to_datetime_indexed_df'
    df = None

    try:
        df = pd.DataFrame(timeseries, columns=['date', 'value'])
        df['date'] = pd.to_datetime(df['date'], unit='s')
        datetime_index = pd.DatetimeIndex(df['date'].values)
        df = df.set_index(datetime_index)
        df.drop('date', axis=1, inplace=True)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: failed to convert timeseries - %s' % (
            current_skyline_app, function_str, e))
    return df
