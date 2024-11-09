"""
algorithm_scores_plot.py
"""
import logging
import traceback
from os import path
from time import time
import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
# @added 20230713 - Task #4996: Improve matplotlib performance
# Improve matplotlib render performance
import matplotlib.style as mplstyle

# @added 20231224 - Feature #5190: Add custom_algorithm results to Mirage and plots
from functions.timeseries.determine_data_frequency import determine_data_frequency

matplotlib.use('Agg')
mplstyle.use('fast')


def get_algorithm_scores_plot(
    current_skyline_app, output_file, timeseries, algorithm, anomalous,
        anomalies, scores, anomaly_window=1, anomalies_in_window=None,
        unreliable=False, low_entropy_value=None):
    """
    Creates a png graph image using the vortex results data.

    :param current_skyline_app: the Skyline app name calling the function
    :param output_file: full path and filename to output where the png image is
        to be saved to
    :param timeseries: the time series
    :param algorithm: the algorithm
    :param anomalous: anomalous
    :param anomalies: the anomalies dict
    :param scores: the scores list
    :param anomaly_window: the anomaly window
    :param anomaly_in_window: the number of anomalies in the anomaly_window
    :param unreliable: unreliable
    :param low_entropy_value: the spectral_entropy low_entropy_value if there is one
    :type current_skyline_app: str
    :type output_file: str
    :type timeseries: list
    :type algorithm: str
    :type anomalous: boolean
    :type anomalies: dict
    :type scores: list
    :type anomaly_window: int
    :type anomaly_in_window: int
    :type unreliable: boolean
    :type low_entropy_value: float
    :return: file
    :rtype: boolean|str

    """

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if path.isfile(output_file):
        current_logger.info('get_algorithm_scores_plot - %s - graph image already exists - %s' % (
            algorithm, output_file))
        return output_file

    start = time()

    # @added 20230713 - Task #4996: Improve matplotlib performance
    # Improve matplotlib render performance
    matplotlib.rcParams['path.simplify_threshold'] = 1.0

    # @added 20231224 - Feature #5190: Add custom_algorithm results to Mirage and plots
    # Align scores because if these are custom_algorithm results rather than snab results
    # the results could be for downsampled data
    aligned_scores = []
    if len(timeseries) > len(scores):
        current_logger.info('get_algorithm_scores_plot - aligning scores because scores appear to be from downsampled data number of scores: %s, len(timeseries): %s' % (
            str(len(scores)), str(len(timeseries))))

        try:
            resolution = determine_data_frequency(current_skyline_app, timeseries, True)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_algorithm_scores_plot :: determine_data_frequency failed, err: %s' % err)

        try:
            for item in timeseries:
                try:
                    ts = int(item[0])
                except:
                    continue
                aligned_ts = int(ts // resolution * resolution)
                score = None
                try:
                    if ts in anomalies:
                        score = float(anomalies[ts]['score'])
                    if aligned_ts in anomalies:
                        score = float(anomalies[aligned_ts]['score'])
                except:
                    pass
                if score is None:
                    # Try a string as it is coerced by JSON
                    try:
                        ts_str = str(ts)
                        if ts_str in anomalies:
                            score = float(anomalies[ts_str]['score'])
                        aligned_ts_str = str(aligned_ts)
                        if aligned_ts_str in anomalies:
                            score = float(anomalies[aligned_ts_str]['score'])
                    except:
                        pass
                if score is None:
                    score = 0
                aligned_scores.append(score)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_algorithm_scores_plot :: failed to align scores, err: %s' % err)
    if aligned_scores:
        scores = list(aligned_scores)
        current_logger.info('get_algorithm_scores_plot - after alignment len(scores): %s' % (
            str(len(scores))))

    try:
        current_logger.info('get_algorithm_scores_plot - %s - creating graph image - %s' % (
            algorithm, output_file))

        params = {
            'axes.labelsize': 10,
            'axes.titlesize': 10,
            'xtick.labelsize': 10,
            # @added 20230713 - Task #4996: Improve matplotlib performance
            # Improve matplotlib render performance
            'path.simplify_threshold': 1.0,
        }
        plt.rcParams.update(params)
        plt.rcParams['xtick.labelsize'] = 8
        plt.rcParams['ytick.labelsize'] = 8

        # @added 20230713 - Task #4996: Improve matplotlib performance
        # Improve matplotlib render performance
        plt.style.use('fast')

        anomalies_indices = []
        data = []
        for index, item in enumerate(timeseries):
            score = 0
            anomaly_value = np.nan
            try:
                score = scores[index]
            except:
                score = np.nan
            try:
                ts = int(item[0])
            except:
                continue
            try:
                if anomalies[ts]:
                    anomalies_indices.append(index)
                    anomaly_value = item[1]
            except:
                # Try a string as it is coerced by JSON
                try:
                    ts_str = str(ts)
                    if anomalies[ts_str]:
                        anomalies_indices.append(index)
                        anomaly_value = item[1]
                except:
                    pass

            data.append([item[0], item[1], score, anomaly_value])

        current_logger.info('get_algorithm_scores_plot - %s - anomalous: %s, total anomalies: %s, requested anomaly window: last %s data points, anomalies in window: %s, anomalies_indices: %s' % (
            algorithm, str(anomalous), str(len(anomalies)), str(anomaly_window),
            str(anomalies_in_window), str(len(anomalies_indices))))

        ylabel = '%s score' % algorithm
        df = pd.DataFrame(data, columns=['date', 'value', 'score', 'anomaly'])
        df['date'] = pd.to_datetime(df['date'], unit='s')
        datetime_index = pd.DatetimeIndex(df['date'].values)
        df = df.set_index(datetime_index)
        df.drop('date', axis=1, inplace=True)
        fig, ax1 = plt.subplots(1, 1, figsize=(8, 4))
        title = '%s - anomalous: %s, total anomalies: %s\nrequested anomaly window: last %s data points, anomalies in window: %s' % (
            algorithm, str(anomalous), str(len(anomalies)), str(anomaly_window),
            str(anomalies_in_window))
        if algorithm == 'sigma':
            title = '%s - anomalous: %s, total anomalies: %s\nrequested anomaly window: last %s data points, anomalies in window: %s\nONLY THE LAST %s DATA POINT ARE ANALYSED FOR ANOMALIES WITH sigma NOT ALL' % (
                algorithm, str(anomalous), str(len(anomalies)), str(anomaly_window),
                str(anomalies_in_window), str(anomaly_window))
        if algorithm == 'spectral_entropy':
            if low_entropy_value or low_entropy_value == 0.0:
                title = '%s (low_entropy_value: %s) - anomalous: %s, total anomalies: %s\nrequested anomaly window: last %s data points, anomalies in window: %s' % (
                    algorithm, str(round(low_entropy_value, 3)), str(anomalous), str(len(anomalies)), str(anomaly_window),
                    str(anomalies_in_window))

        if unreliable:
            title = '%s\nUNRELIABLE RESULTS' % title
        plt.title(title)
        ax1b = ax1.twinx()
        plot1a, = ax1.plot(df.index, df.value, lw=0.4, alpha=1.0, zorder=1)
        plot1b, = ax1b.plot(df.index, df.score, color='orange', lw=0.4, alpha=0.7, zorder=3)
        s = [5 for item in df['anomaly'].tolist()]
        c = ['red' for item in df['anomaly'].tolist()]
        plot1c = ax1.scatter(df.index, df.anomaly, s=s, c=c)
        ax1.set_ylabel('value', fontsize='small')
        ax1b.set_ylabel(ylabel, fontsize='small')
        ax1.set_xlabel('Date', fontsize='small')
        ax1.xaxis.set_tick_params(labelsize='small')
        # @modified 20230713 - Task #4996: Improve matplotlib performance
        # Improve matplotlib render performance
        # plt.legend([plot1a, plot1b, plot1c], df.columns, loc='best')
        plt.legend([plot1a, plot1b, plot1c], df.columns, loc='upper left')

        # defining display layout
        plt.tight_layout()
        plt.savefig(output_file, format='png')
        fig.clf()
        plt.close(fig)
        current_logger.info('get_algorithm_scores_plot - %s - took %s seconds to create graph image - %s' % (
            algorithm, str((time() - start)), output_file))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_algorithm_scores_plot :: failed to create %s - %s' % (output_file, err))
        return False

    return output_file
