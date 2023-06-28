"""
csv_to_timeseries.py
"""
import logging
import traceback
from os import path

import numpy as np
import pandas as pd


# @added 20221204 - Feature #4754: csv_to_timeseries
#                   Feature #4734: mirage_vortex
#                   Branch #4728: vortex
def csv_to_timeseries(current_skyline_app, csv_file, log=True):
    """
    Convert a csv to a timeseries list. csv format must be:

    timestamp,value
    1670147592,1
    1670147652,2
    1670147712,3

    Or

    date,value
    2022-12-04 09:54:22,1
    2022-12-04 09:55:22,2
    2022-12-04 09:56:22,3

    If dates are passed they are coerced into UTC unix timestamps.

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param csv_file: the csv file with timeseries data
    :param log: whether to log or not, optional, defaults to False
    :type current_skyline_app: str
    :type csv_file: str
    :type log: boolean
    :return: timeseries
    :rtype:  list

    """
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    function_str = 'functions.pandas.csv_to_timeseries'
    timeseries = []

    if not path.isfile(csv_file):
        current_logger.error('error :: %s :: file not found - %s' % (
            function_str, str(csv_file)))
        return timeseries

    df = None
    try:
        df = pd.read_csv(csv_file, header=0)
        if log:
            current_logger.info('%s :: pandas dataframe created from csv_file - %s' % (
                function_str, csv_file))
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to read %s to DataFrame - %s' % (
            function_str, csv_file, err))
        return timeseries

    try:
        df.rename(columns={df.columns[0]: 'timestamp'}, inplace=True)
        unix_timestamps = False
        if isinstance(df['timestamp'][0], (np.int64, np.float64)):
            unix_timestamps = True
            if log:
                current_logger.info('%s :: timestamps are type: %s' % (
                    function_str, str(type(df['timestamp'][0]))))
        if unix_timestamps:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        else:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['timestamp'] = df.timestamp.values.astype(np.int64) // 10 ** 9
        timeseries = df.values.tolist()
        timeseries = [[int(t), v] for t, v in timeseries]
        if log:
            current_logger.info('%s :: converted to timeseries of length: %s' % (
                function_str, str(len(timeseries))))
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to convert DataFrame to timeseries - %s' % (
            function_str, err))
        return 'error: ' + str(err)

    return timeseries
