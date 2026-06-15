"""
algorithm_scores_plot.py
"""
import logging
import traceback
from os import path
from time import time
import numpy as np
# @modified 20241115 - Task #5526: Build v5.0.0 and upgrade deps
#                      Branch #5532: v5.0.0-alpha
# Use numpy arrays only for performance, no DataFrame required
#import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
# @added 20230713 - Task #4996: Improve matplotlib performance
# Improve matplotlib render performance
import matplotlib.style as mplstyle

# @added 20231224 - Feature #5190: Add custom_algorithm results to Mirage and plots
from functions.timeseries.determine_data_frequency import determine_data_frequency

matplotlib.use('Agg')
mplstyle.use('fast')


# @modified 20241115 - Task #5526: Build v5.0.0 and upgrade deps
#                      Branch #5532: v5.0.0-alpha
# Improve matplotlib render performance - added figsize dpi and plot_parameters
def get_algorithm_scores_plot(
    current_skyline_app, output_file, timeseries, algorithm, anomalous,
        anomalies, scores, anomaly_window=1, anomalies_in_window=None,
        unreliable=False, low_entropy_value=None, figsize=(8, 4), dpi=100,
        plot_paramaters={}):
    """
    Creates a png graph image using results data.

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
    :param figsize: plt figsize to use
    :param dpi: dpi to use, if the default figsize(8, 4) results in
        800 x 400 image with default 100, if dpi is set to 80 the image will be
        640 x 320.
    :param plot_paramaters: a dict with plot parameters that can be used.
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
    :type figsize: tuple
    :type dpi: int
    :type plot_paramaters: dict
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

    # @added 20231224 - Feature #5190: Add custom_algorithm results to Mirage and plots
    # Align scores because if these are custom_algorithm results rather than snab results
    # the results could be for downsampled data
    aligned_scores = []
    if len(timeseries) > len(scores):
        current_logger.info('get_algorithm_scores_plot - aligning scores because scores appear to be from downsampled data number of scores: %s, len(timeseries): %s' % (
            str(len(scores)), str(len(timeseries))))

        start_aligned_scores = time()

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
        current_logger.info('get_algorithm_scores_plot - aligned_scores took: %s seconds' % (
            str((time() - start_aligned_scores))))

    if aligned_scores:
        aligned_scores_fast = []
        start_fast_aligned_scores = time()
        # Convert timeseries and anomalies to NumPy arrays or compatible formats
        np_timestamps = np.array([int(item[0]) for item in timeseries], dtype=np.int64)
        scores_np = np.zeros_like(np_timestamps, dtype=np.float64)
        ts_diffs = np.diff(np_timestamps)
        resolution_counts = np.unique(ts_diffs, return_counts=True)
        resolution = resolution_counts[0][np.argmax(resolution_counts[1])]
        # Align timestamps to the resolution
        aligned_timestamps = (np_timestamps // resolution) * resolution
        aligned_timestamps = [int(ts) for ts in aligned_timestamps]
        # Handle anomalies lookup
        for index, ts in enumerate(np_timestamps):
            score = 0
            ts_int = int(ts)
            if ts_int in anomalies:
                score = float(anomalies[ts_int]['score'])
            elif aligned_timestamps[index] in anomalies:
                score = float(anomalies[aligned_timestamps[index]]['score'])
            else:
                # Try string versions
                ts_str = str(ts_int)
                aligned_ts_str = str(aligned_timestamps[index])
                if ts_str in anomalies:
                    score = float(anomalies[ts_str]['score'])
                elif aligned_ts_str in anomalies:
                    score = float(anomalies[aligned_ts_str]['score'])
            scores_np[index] = score
        # The scores_np array now contains the aligned scores
        aligned_scores_fast = [float(score) for score in scores_np]
        current_logger.info('get_algorithm_scores_plot - aligned_scores_fast took: %s seconds' % (
            str((time() - start_fast_aligned_scores))))
        if set(aligned_scores_fast) == set(aligned_scores):
            current_logger.info('get_algorithm_scores_plot - aligned_scores_fast and aligned_scores are the same')
        else:
            current_logger.info('warning :: get_algorithm_scores_plot - aligned_scores_fast and aligned_scores are the same')

    try:
        current_logger.info('get_algorithm_scores_plot - %s - creating graph image - %s' % (
            algorithm, output_file))
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

        # @modified 20241115 - Task #5526: Build v5.0.0 and upgrade deps
        #                      Branch #5532: v5.0.0-alpha
        # Improve matplotlib render performance
        # Removed using a pandas DataFrame and changed to using numpy arrays
        # this along with specifying some things rather than having matplotlib
        # calculate them and using dpi=100, results in significant improvement
        # in performance.
        # Old pandas method: took 8.021286010742188 seconds
        # New numpy method:  took 0.7356054782867432 seconds
        data = np.array(data, dtype=np.float64)
        timestamps = data[:, 0]
        values = data[:, 1]
        scores = data[:, 2]
        anomalies = data[:, 3]
        anomalies_count = len([s for s in scores if s == 1])
        dates = np.datetime64('1970-01-01') + timestamps.astype('timedelta64[s]')
        params = {
            'axes.labelsize': 10,
            'axes.titlesize': 10,
            'xtick.labelsize': 8,
            'ytick.labelsize': 8,
            # @added 20230713 - Task #4996: Improve matplotlib performance
            # Improve matplotlib render performance
            'path.simplify': True,
            # @modified 20241115 - Task #5526: Build v5.0.0 and upgrade deps
            #                      Branch #5532: v5.0.0-alpha
            # Improve matplotlib render performance
            # 'path.simplify_threshold': 1.0,
            'path.simplify_threshold': 0.5,
        }
        # Call rcParams.update once only
        plt.rcParams.update(params)

        # @added 20230713 - Task #4996: Improve matplotlib performance
        # Improve matplotlib render performance
        plt.style.use('fast')

        if 'ylabel' in plot_paramaters:
            ylabel = plot_paramaters['ylabel']
        else:
            ylabel = '%s score' % algorithm
        # Plotting
        start_plot = time()
        fig, ax1 = plt.subplots(1, 1, figsize=figsize)
        ax1b = ax1.twinx()

        if 'title' in plot_paramaters:
            title = plot_paramaters['title']
        else:
            title = '%s - anomalous: %s, total anomalies: %s\nrequested anomaly window: last %s data points, anomalies in window: %s' % (
                algorithm, str(anomalous), str(anomalies_count), str(anomaly_window),
                str(anomalies_in_window))
            if algorithm == 'sigma':
                title = '%s - anomalous: %s, total anomalies: %s\nrequested anomaly window: last %s data points, anomalies in window: %s\nONLY THE LAST %s DATA POINT ARE ANALYSED FOR ANOMALIES WITH sigma NOT ALL' % (
                    algorithm, str(anomalous), str(anomalies_count), str(anomaly_window),
                    str(anomalies_in_window), str(anomaly_window))
            if algorithm == 'spectral_entropy':
                if low_entropy_value or low_entropy_value == 0.0:
                    title = '%s (low_entropy_value: %s) - anomalous: %s, total anomalies: %s\nrequested anomaly window: last %s data points, anomalies in window: %s' % (
                        algorithm, str(round(low_entropy_value, 3)), str(anomalous), str(anomalies_count), str(anomaly_window),
                        str(anomalies_in_window))
        if unreliable:
            title = '%s\nUNRELIABLE RESULTS' % title
        ax1.set_title(title, fontsize='medium')

        # Line plots
        plot1a, = ax1.plot(dates, values, lw=0.4, alpha=1.0, zorder=1)
        plot1b, = ax1b.plot(dates, scores, color='orange', lw=0.4, alpha=0.7, zorder=3)

        # Scatter plot for anomalies - only plot if anomalies exist
        if np.any(anomalies):
            # Set a constant size for performance
            s = np.full_like(anomalies, 5)
            # Set the colour based on anomaly value
            c = np.where(anomalies > 0, 'red', 'blue')
            ax1.scatter(dates, anomalies, s=s, c=c, zorder=5)

        # Calculate limits for x-axis and y-axis with a 5% margin and ensuring
        # a minimum margin for performance so that matplotlib does not have to
        # try and figure it out
        old_method = False
        if old_method:
            x_margin = max((dates[-1] - dates[0]) * 0.05, 1e-6)
            y_margin = max((values.max() - values.min()) * 0.05, 1e-6)
            # Set limits with fallback expansion
            ax1.set_xlim(dates[0] - x_margin, dates[-1] + x_margin)
            ax1.set_ylim(values.min() - y_margin, values.max() + y_margin)

        # Calculate limits for x-axis and y-axis with a 5% margin and ensuring
        # a minimum margin for performance so that matplotlib does not have to
        # try and figure it out
        # Calculate x-margin for datetime64
        time_diff = (dates[-1] - dates[0]).astype('timedelta64[s]').astype(float)
        if time_diff == 0:
            # If all dates are identical, set a fixed range around the single date
            x_margin_seconds = 1e6  # 1 day in seconds (arbitrary default range)
            x_margin = np.timedelta64(int(x_margin_seconds), 's')
            ax1.set_xlim(dates[0] - x_margin, dates[0] + x_margin)
        else:
            # Normal margin calculation
            x_margin_seconds = max(time_diff * 0.05, 1e-6)
            x_margin = np.timedelta64(int(x_margin_seconds), 's')
            ax1.set_xlim(dates[0] - x_margin, dates[-1] + x_margin)
        # Calculate y-margin for values
        y_diff = values.max() - values.min()
        y_margin = max(y_diff * 0.05, 1e-6)
        # Set y-limits
        ax1.set_ylim(values.min() - y_margin, values.max() + y_margin)

        # Labels
        ax1.set_ylabel('value', fontsize='small')
        ax1b.set_ylabel(ylabel, fontsize='small')
        ax1.set_xlabel('Date', fontsize='small')
        # Legend
        plt.legend([plot1a, plot1b], ['value', 'score'], loc='upper left', fontsize='small')
        # Save
        fig.tight_layout()
        plot_took = time() - start_plot
        start_save = time()
        fig.savefig(output_file, format='png', dpi=dpi)
        save_took = time() - start_save
        fig.clf()
        plt.close(fig)
        current_logger.info('get_algorithm_scores_plot - plot function took %s seconds, save took %s seconds' % (
            str(plot_took), str(save_took)))
        current_logger.info('get_algorithm_scores_plot - %s - took %s seconds to create graph image - %s' % (
            algorithm, str((time() - start)), output_file))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_algorithm_scores_plot :: failed to create %s - %s' % (output_file, err))
        return False

    return output_file
