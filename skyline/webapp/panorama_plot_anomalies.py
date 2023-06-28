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
# @added 20220519 - Feature #4326: webapp - panorama_plot_anomalies
# Added matches
from functions.database.queries.get_matches import get_matches

# @added 20220801 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.metrics.get_base_name_from_labelled_metrics_name import get_base_name_from_labelled_metrics_name
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric

skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)


# @added 20211125 - Feature #4326: webapp - panorama_plot_anomalies
# @modified 20220519 - Feature #4326: webapp - panorama_plot_anomalies
# Added matches
# def panorama_plot_anomalies(base_name, from_timestamp=None, until_timestamp=None):
def panorama_plot_anomalies(
        base_name, from_timestamp=None, until_timestamp=None, matches=False):
    """
    Create a plot of the metric with its anomalies and return the anomalies dict,
    the path and filename and the matches_dict

    :param base_name: the name of the metric
    :param from_timestamp: the from timestamp
    :param until_timestamp: the until timestamp
    :param matches: whether to plot matches as well
    :type base_name: str
    :type from_timestamp: int
    :type until_timestamp: int
    :type matches: bool
    :return: (anomalies_dict, path and file, matches_dict)
    :rtype:  tuple

    """

    function_str = 'panorama_plot_anomalies'

    logger.info('%s - base_name: %s, from_timestamp: %s, until_timestamp: %s' % (
        function_str, str(base_name), str(from_timestamp),
        str(until_timestamp)))

    if not until_timestamp:
        until_timestamp = int(time())

    # @added 20221115 - Feature #4326: webapp - panorama_plot_anomalies
    # Added timeseries_dict
    timeseries_dict = {}

    # @added 20220801 - Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    data_source = 'graphite'
    metric_id = 0
    labelled_metric_base_name = None
    labelled_metric_name = None
    use_base_name = str(base_name)
    if '{' in base_name and '}' in base_name and '_tenant_id="' in base_name:
        metric_id = 0
        try:
            metric_id = get_metric_id_from_base_name(skyline_app, base_name)
        except Exception as err:
            logger.error('error :: panorama_plot_anomalies :: get_metric_id_from_base_name failed with base_name: %s - %s' % (str(base_name), err))
        if metric_id:
            labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
    if base_name.startswith('labelled_metrics.'):
        labelled_metric_name = str(base_name)
        try:
            metric_name = get_base_name_from_labelled_metrics_name(skyline_app, base_name)
            if metric_name:
                base_name = str(metric_name)
                data_source = 'victoriametrics'
        except Exception as err:
            logger.error('error :: panorama_plot_anomalies :: get_base_name_from_labelled_metrics_name failed for %s - %s' % (
                base_name, err))
    if labelled_metric_name:
        use_base_name = str(labelled_metric_name)
        data_source = 'victoriametrics'

    save_to_file = '%s/panorama_anomalies_plot.%s.%s.%s.png' % (
        settings.SKYLINE_TMP_DIR, use_base_name, str(from_timestamp),
        str(until_timestamp))

    matches_save_to_file = None
    if matches:
        matches_save_to_file = '%s/panorama_matches_plot.%s.%s.%s.png' % (
            settings.SKYLINE_TMP_DIR, use_base_name, str(from_timestamp),
            str(until_timestamp))

    if not metric_id:
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

    # @added 20220519 - Feature #4326: webapp - panorama_plot_anomalies
    # Added matches
    matches_dict = {}
    if matches:
        try:
            matches_dict = get_matches(skyline_app, metric_id, from_timestamp, until_timestamp)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: get_matches failed to determine matches for %s - %s' % (
                function_str, base_name, err))
            raise
        if not matches_dict:
            matches_save_to_file = None

    if not matches:
        if os.path.isfile(save_to_file):
            # @modified 20220519 - Feature #4326: webapp - panorama_plot_anomalies
            # Added matches
            # return anomalies_dict, save_to_file
            return anomalies_dict, save_to_file, matches_dict, matches_save_to_file, labelled_metric_name, timeseries_dict
    else:
        if matches_save_to_file:
            if os.path.isfile(save_to_file) and os.path.isfile(matches_save_to_file):
                return anomalies_dict, save_to_file, matches_dict, matches_save_to_file, labelled_metric_name, timeseries_dict
        else:
            if os.path.isfile(save_to_file):
                return anomalies_dict, save_to_file, matches_dict, matches_save_to_file, labelled_metric_name, timeseries_dict

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
    if data_source == 'graphite':
        metrics_functions[base_name]['functions'] = None
        try:
            metrics_timeseries = get_metrics_timeseries(skyline_app, metrics_functions, from_timestamp, until_timestamp, log=False)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: get_metrics_timeseries failed - %s' % (
                function_str, err))
            raise
    if data_source == 'victoriametrics':
        logger.info('getting victoriametrics data for %s - from_timestamp - %s, until_timestamp - %s' % (base_name, str(from_timestamp), str(until_timestamp)))
        metrics_timeseries = {}
        metrics_timeseries[base_name] = {}
        timeseries = []
        try:
            # get_victoriametrics_metric automatically applies the rate and
            # step required no downsampling or nonNegativeDerivative is
            # required.
            timeseries = get_victoriametrics_metric(
                skyline_app, base_name, from_timestamp, until_timestamp,
                'list', 'object')
        except Exception as err:
            logger.error('error :: %s ::get_victoriametrics_metric failed - %s' % (
                function_str, err))
            raise
        if timeseries:
            metrics_timeseries[base_name]['timeseries'] = timeseries

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

    if data_source == 'victoriametrics':
        aligned_timeseries = []
        for item in timeseries:
            aligned_timeseries.append([int(int(item[0]) // resolution * resolution), item[1]])
        timeseries = aligned_timeseries

    # @added 20221115 - Feature #4326: webapp - panorama_plot_anomalies
    # Added timeseries_dict
    for ts, v in timeseries:
        timeseries_dict[int(ts)] = {'value': v}

    try:
        df = pd.DataFrame(timeseries, columns=['date', 'value'])
        df['date'] = pd.to_datetime(df['date'], unit='s')
        datetime_index = pd.DatetimeIndex(df['date'].values)
        df = df.set_index(datetime_index)
        df.drop('date', axis=1, inplace=True)
        anomalies_data = []
        for item in timeseries:
            if int(item[0]) in anomaly_timestamps:
                # anomalies_data.append(1)
                anomaly_value = 1
            else:
                # anomalies_data.append(0)
                anomaly_value = 0
            anomalies_data.append(anomaly_value)
            timeseries_dict[int(item[0])]['anomaly'] = anomaly_value

        df['anomalies'] = anomalies_data
        title = '%s\n%s anomalies' % (base_name, str(len(anomaly_timestamps)))
        if labelled_metric_name:
            title = '%s\n%s anomalies' % (labelled_metric_name, str(len(anomaly_timestamps)))
        plot(df['value'], anomaly=df['anomalies'], anomaly_markersize=5, anomaly_tag='marker', anomaly_color='red', title=title, save_to_file=save_to_file)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to plot anomalies for %s - %s' % (
            function_str, base_name, err))
        raise

    # @added 20220519 - Feature #4326: webapp - panorama_plot_anomalies
    # Added matches
    if matches and matches_dict:
        unaligned_matches_timestamps = []
        try:
            for match_id in list(matches_dict.keys()):
                try:
                    unaligned_matches_timestamps.append(matches_dict[match_id]['metric_timestamp'])
                except KeyError:
                    # ionosphere_layers_matched use anomaly_timestamp not metric_timestamp
                    unaligned_matches_timestamps.append(matches_dict[match_id]['anomaly_timestamp'])
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: failed on matches_dict: %s' % (
                function_str, str(matches_dict)))
            raise
        matches_timestamps = []

        for ts in unaligned_matches_timestamps:
            matches_timestamps.append(int(int(ts) // resolution * resolution))
        try:
            df = pd.DataFrame(timeseries, columns=['date', 'value'])
            df['date'] = pd.to_datetime(df['date'], unit='s')
            datetime_index = pd.DatetimeIndex(df['date'].values)
            df = df.set_index(datetime_index)
            df.drop('date', axis=1, inplace=True)
            matches_data = []
            for item in timeseries:
                if int(item[0]) in matches_timestamps:
                    # matches_data.append(1)
                    matches_value = 1
                else:
                    # matches_data.append(0)
                    matches_value = 0
                matches_data.append(matches_value)
                timeseries_dict[int(item[0])]['match'] = matches_value

            df['matches'] = matches_data
            title = '%s\n%s matches' % (base_name, str(len(matches_timestamps)))
            if labelled_metric_name:
                title = '%s\n%s matches' % (labelled_metric_name, str(len(matches_timestamps)))
            plot(df['value'], anomaly=df['matches'], anomaly_markersize=5, anomaly_tag='marker', anomaly_color='green', title=title, save_to_file=matches_save_to_file)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: failed to plot matches for %s - %s' % (
                function_str, base_name, err))
            raise

    if not os.path.isfile(save_to_file):
        # @modified 20221115 - Feature #4326: webapp - panorama_plot_anomalies
        # Added timeseries_dict
        return anomalies_dict, None, matches_dict, matches_save_to_file, labelled_metric_name, timeseries_dict

    return anomalies_dict, save_to_file, matches_dict, matches_save_to_file, labelled_metric_name, timeseries_dict
