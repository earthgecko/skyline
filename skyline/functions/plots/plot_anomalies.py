import logging
import os
import traceback

import pandas as pd
from adtk.visualization import plot


def plot_anomalies(
        current_skyline_app, metric, timeseries, anomalies, title, output_file):
    """
    Create a plot of a timeseries with anomalies and return the path and filename

    :param current_skyline_app: skyline_app
    :param metric: the name of the metric
    :param timeseries: the timeseries to plot
    :param anomalies: the anomaly timestamps
    :param title: the plot title
    :param output_file: the full path and filename (including .png extension) to
        save to plot as
    :type current_skyline_app: str
    :type metric: str
    :type timeseries: list
    :type anomalies: list
    :type title: str
    :type output_file: str
    :return: output_file
    :rtype:  str

    """

    function_str = 'plot_anomalies'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if os.path.isfile(output_file):
        current_logger.info('%s :: %s :: plot of %s with %s anomalies exists' % (
            str(current_skyline_app), function_str, metric,
            str(len(anomalies))))
        return output_file

    current_logger.info('%s :: %s :: plotting %s with %s anomalies' % (
        str(current_skyline_app), function_str, metric,
        str(len(anomalies))))

    anomalies_data = []
    last_timestamp = None
    for item in timeseries:
        anomaly_in_period = 0
        if not last_timestamp:
            last_timestamp = int(item[0])
            anomalies_data.append(anomaly_in_period)
            continue
        for anomaly_ts in anomalies:
            if anomaly_ts < last_timestamp:
                continue
            if anomaly_ts > item[0]:
                continue
            if anomaly_ts in list(range(last_timestamp, int(item[0]))):
                anomaly_in_period = 1
                break
        anomalies_data.append(anomaly_in_period)
        last_timestamp = int(item[0])

    try:
        df = pd.DataFrame(timeseries, columns=['date', 'value'])
        df['date'] = pd.to_datetime(df['date'], unit='s')
        datetime_index = pd.DatetimeIndex(df['date'].values)
        df = df.set_index(datetime_index)
        df.drop('date', axis=1, inplace=True)
        df['anomalies'] = anomalies_data
        plot(df['value'], anomaly=df['anomalies'], anomaly_color='red', title=title, save_to_file=output_file)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to plot anomalies - %s' % (
            function_str, err))

    if not os.path.isfile(output_file):
        current_logger.error('error :: %s :: %s :: plotting %s with %s anomalies failed not output_file exists' % (
            str(current_skyline_app), function_str, metric,
            str(len(anomalies))))
        return None

    current_logger.info('%s :: %s :: plotted %s with %s anomalies to %s' % (
        str(current_skyline_app), function_str, metric,
        str(len(anomalies)), output_file))

    return output_file
