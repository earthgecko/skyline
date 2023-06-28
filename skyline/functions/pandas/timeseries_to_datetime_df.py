"""
Convert a timeseries list to a datetime indexed DataFrame
"""
import logging
import traceback

import pandas as pd


# @added 20220519 - Task #4582 - POC ARTime
def timeseries_to_datetime_indexed_df(current_skyline_app, timeseries, log=False):
    """
    Convert a timeseries list to a datetime DataFrame

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
    RangeIndex: 10047 entries, 0 to 10046
    Data columns (total 2 columns):
     #   Column     Non-Null Count  Dtype
    ---  ------     --------------  -----
     0   timestamp  10047 non-null  datetime64[ns]
     1   value      10047 non-null  float64
    dtypes: datetime64[ns](1), float64(1)
    memory usage: 157.1 KB

    """
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    function_str = 'functions.pandas.timeseries_to_datetime_df'
    df = None

    try:
        df = pd.DataFrame(timeseries, columns=['timestamp', 'value'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: failed to convert timeseries - %s' % (
            current_skyline_app, function_str, e))
    return df
