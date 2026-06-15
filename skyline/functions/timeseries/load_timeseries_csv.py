"""
load_timeseries_csv.py
"""
import gzip
import logging
import os
import shutil

import pandas as pd

# @added 20250111 - Feature #5588: snab.process_algorithm
def load_timeseries_csv(current_skyline_app, csv_file):
    """
    Load the time series data from a csv file (or csv.gz) and return the
    timeseries as a list.  The csv must have a header line with columns
    timestamp (int) and value (float), timestamps must be unix timestamp, like:

    timestamp,value
    1735981200,0.4275234577056879
    1735981800,0.29976536964736344
    1735982400,0.24779413140965248

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param csv_file: the csv path and file name
    :type current_skyline_app: str
    :type csv_file: str
    :return: timeseries
    :rtype: list
    """
    function_str = 'functions.timeseries.load_timeseries_csv'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    current_logger.info('%s :: load time series from: %s' % (
        function_str, str(csv_file)))

    timeseries = []

    archive = False
    uncompressed_data_file = csv_file.replace('.gz', '')

    if csv_file.endswith('.gz'):
        current_logger.info('%s :: decompressing: %s, to %s' % (
            function_str, csv_file, str(uncompressed_data_file)))
        with gzip.open(csv_file, 'rb') as f_in:
            with open(uncompressed_data_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        if os.path.isfile(uncompressed_data_file):
            csv_file = str(uncompressed_data_file)
            archive = True
            current_logger.info('%s :: created %s' % (
                function_str, str(uncompressed_data_file)))
        else:
            current_logger.error('error :: %s :: failed to create %s from gzip archive %s' % (
                function_str, uncompressed_data_file, csv_file))
            return timeseries

    timestamps = []
    try:
        df = pd.read_csv(csv_file, delimiter=',', header=0, names=['timestamp', 'value'])
        df.columns = ['timestamp', 'value']
        timestamps = df['timestamp'].tolist()
        values = df['value'].tolist()
    except Exception as err:
        skyline_app_logger = '%sLog' % current_skyline_app
        logger = logging.getLogger(skyline_app_logger)
        logger.error('error :: %s :: failed to create timestamps and values from %s, err: %s' % (
            function_str, csv_file, err))
        if archive:
            os.remove(csv_file)
        return timeseries

    for index, ts in enumerate(timestamps):
        value = None
        try:
            value = float(values[index])
        except:  # nosec
            continue
        if str(value) in ['NaN','nan','Nan','none']:
            continue
        timeseries.append([int(ts), value])
    current_logger.info('%s :: len(timeseries): %s' % (
        function_str, str(len(timeseries))))

    if archive:
        if os.path.isfile(uncompressed_data_file):
            os.remove(uncompressed_data_file)

    return timeseries
