"""
Plot anomalies for a metric
"""
import logging
import os
import traceback
from time import time

import pandas as pd
from adtk.visualization import plot

import settings

from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.database.queries.query_anomalies import get_anomalies
from functions.timeseries.determine_data_frequency import determine_data_frequency
from functions.graphite.get_metrics_timeseries import get_metrics_timeseries

skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)


# @added 20211125 - Feature #4326: webapp - panorama_plot_anomalies
def panorama_plot_anomalies(base_name, from_timestamp=None, until_timestamp=None):
    """
    Create a plot of the metric with its anomalies and return the anomalies dict
    and the path and filename

    :param base_name: the name of the metric
    :param from_timestamp: the from timestamp
    :param until_timestamp: the until timestamp
    :type base_name: str
    :type from_timestamp: int
    :type until_timestamp: int
    :return: (anomalies_dict, path and file)
    :rtype:  tuple

    """

    function_str = 'panorama_plot_anomalies'

    logger.info('%s - base_name: %s, from_timestamp: %s, until_timestamp: %s' % (
        function_str, str(base_name), str(from_timestamp),
        str(until_timestamp)))

    if not until_timestamp:
        until_timestamp = int(time())

    save_to_file = '%s/panorama_anomalies_plot.%s.%s.%s.png' % (
        settings.SKYLINE_TMP_DIR, base_name, str(from_timestamp),
        str(until_timestamp))

    try:
        metric_id = get_metric_id_from_base_name(skyline_app, base_name)
        logger.info('%s - %s with metric id:%s' % (
            function_str, str(base_name), str(metric_id)))
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to determine metric id for %s - %s' % (
            function_str, base_name, err))
        raise

    try:
        anomalies_dict = get_anomalies(skyline_app, metric_id, params={'latest': False})
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to determine anomalies for %s - %s' % (
            function_str, base_name, err))
        raise

    if from_timestamp and anomalies_dict:
        for anomaly_id in list(anomalies_dict.keys()):
            if anomalies_dict[anomaly_id]['anomaly_timestamp'] < from_timestamp:
                del anomalies_dict[anomaly_id]

    if until_timestamp and anomalies_dict:
        for anomaly_id in list(anomalies_dict.keys()):
            if anomalies_dict[anomaly_id]['anomaly_timestamp'] > until_timestamp:
                del anomalies_dict[anomaly_id]

    if os.path.isfile(save_to_file):
        return anomalies_dict, save_to_file

    if not from_timestamp and anomalies_dict:
        first_anomaly_id = list(anomalies_dict.keys())[-1]
        first_anomaly_timestamp = anomalies_dict[first_anomaly_id]['anomaly_timestamp']
        from_timestamp = first_anomaly_timestamp - (86400 * 7)
        logger.info('%s :: the from_timestamp was not passed, calculated from the anomalies_dict as %s' % (
            function_str, str(from_timestamp)))
    if not from_timestamp and not anomalies_dict:
        logger.info('%s :: the from_timestamp was not passed and no anomalies found for %s' % (
            function_str, base_name))
        from_timestamp = until_timestamp - (86400 * 7)

    metrics_functions = {}
    metrics_functions[base_name] = {}
    metrics_functions[base_name]['functions'] = None

    try:
        metrics_timeseries = get_metrics_timeseries(skyline_app, metrics_functions, from_timestamp, until_timestamp, log=False)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: get_metrics_timeseries failed - %s' % (
            function_str, err))
        raise

    try:
        timeseries = metrics_timeseries[base_name]['timeseries']
        # Truncate the first and last timestamp, just in case they are not
        # filled buckets
        timeseries = timeseries[1:-1]
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to get timeseries for %s - %s' % (
            function_str, base_name, err))
        raise

    unaligned_anomaly_timestamps = []
    for anomaly_id in list(anomalies_dict.keys()):
        unaligned_anomaly_timestamps.append(anomalies_dict[anomaly_id]['anomaly_timestamp'])

    # Align anomalies to timeseries resolution
    resolution = determine_data_frequency(skyline_app, timeseries, False)
    anomaly_timestamps = []
    for ts in unaligned_anomaly_timestamps:
        anomaly_timestamps.append(int(int(ts) // resolution * resolution))

    try:
        df = pd.DataFrame(timeseries, columns=['date', 'value'])
        df['date'] = pd.to_datetime(df['date'], unit='s')
        datetime_index = pd.DatetimeIndex(df['date'].values)
        df = df.set_index(datetime_index)
        df.drop('date', axis=1, inplace=True)
        anomalies_data = []
        for item in timeseries:
            if int(item[0]) in anomaly_timestamps:
                anomalies_data.append(1)
            else:
                anomalies_data.append(0)
        df['anomalies'] = anomalies_data
        title = '%s\n%s anomalies' % (base_name, str(len(anomaly_timestamps)))
        plot(df['value'], anomaly=df['anomalies'], anomaly_color='red', title=title, save_to_file=save_to_file)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to plot anomalies for %s - %s' % (
            function_str, base_name, err))
        raise

    if not os.path.isfile(save_to_file):
        return anomalies_dict, None

    return anomalies_dict, save_to_file
