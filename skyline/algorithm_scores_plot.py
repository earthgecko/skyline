"""
algorithm_scores_plot.py
"""
import logging
import traceback
from os import path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


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
        current_logger.info('get_algorithm_scores_plot - graph image already exists - %s' % output_file)
        return output_file

    try:
        current_logger.info('get_algorithm_scores_plot - creating graph image - %s' % output_file)

        params = {
            'axes.labelsize': 10,
            'axes.titlesize': 10,
            'xtick.labelsize': 10,
        }
        plt.rcParams.update(params)
        plt.rcParams['xtick.labelsize'] = 8
        plt.rcParams['ytick.labelsize'] = 8

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
        plt.legend([plot1a, plot1b, plot1c], df.columns, loc='best')
        # defining display layout
        plt.tight_layout()
        plt.savefig(output_file, format='png')
        fig.clf()
        plt.close(fig)
        current_logger.info('get_algorithm_scores_plot - created graph image - %s' % output_file)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_algorithm_scores_plot :: failed to create %s - %s' % (output_file, err))
        return False

    return output_file
