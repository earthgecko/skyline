"""
fast_basic_timeseries_plot.py
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


# @added 20260801 - Feature #5774: fast_basic_timeseries_plot
# Based on the algorithm_scores_plot.py timeseries only, no scores
def get_fast_basic_timeseries_plot(
    current_skyline_app, output_file, timeseries, figsize=(8, 4), dpi=100,
        plot_parameters={}):
    """
    Creates a png graph image using time series data.

    This is based on the algorithm_scores_plot.py, to create the same graph type
    just with no scores.  The algorithm scores graph was heavily optimised to
    produce fast, clear and small pngs.  The pngs this creates are mostly
    between 36K and 64K, the occasional ones up to ~94K.  For storage and load
    times this is important.  The minimalist image is very detailed being the
    raw data when compared to the Graphite ~44K rendered graphs.  This attempts
    to provide the full granularity at the lowest cost in terms of speed and
    storage, while not losing any detail.

    :param current_skyline_app: the Skyline app name calling the function
    :param output_file: full path and filename to output where the png image is
        to be saved to
    :param timeseries: the time series
    :param figsize: plt figsize to use
    :param dpi: dpi to use, if the default figsize(8, 4) results in
        800 x 400 image with default 100, if dpi is set to 80 the image will be
        640 x 320.
    :param plot_parameters: a dict with plot parameters that can be used.
    :type current_skyline_app: str
    :type output_file: str
    :type timeseries: list
    :type figsize: tuple
    :type dpi: int
    :type plot_paramaters: dict
    :return: file
    :rtype: boolean|str

    """

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if path.isfile(output_file):
        current_logger.info('get_fasic_basic_timeseries_plot - graph image already exists - %s' % (
            output_file))
        return output_file

    start = time()

    try:
        data = np.array(timeseries, dtype=np.float64)
        timestamps = data[:, 0]
        values = data[:, 1]
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

        # Plotting
        start_plot = time()
        fig, ax1 = plt.subplots(1, 1, figsize=figsize)

        if 'title' in plot_parameters:
            title = plot_parameters['title']
            ax1.set_title(title, fontsize='medium')

        # Line plots
        plot1a, = ax1.plot(dates, values, lw=0.4, alpha=1.0, zorder=1)

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
#        ax1b.set_ylabel(ylabel, fontsize='small')
        ax1.set_xlabel('Date', fontsize='small')
        # Legend
        plt.legend([plot1a], ['value'], loc='upper left', fontsize='small')
        # Save
        fig.tight_layout()
        plot_took = time() - start_plot
        start_save = time()
        fig.savefig(output_file, format='png', dpi=dpi)
        save_took = time() - start_save
        fig.clf()
        plt.close(fig)
        current_logger.info('get_fasic_basic_timeseries_plot - plot function took %s seconds, save took %s seconds' % (
            str(plot_took), str(save_took)))
        current_logger.info('get_fasic_basic_timeseries_plot - took %s seconds to create graph image - %s' % (
            str((time() - start)), output_file))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_fasic_basic_timeseries_plot :: failed to create %s - %s' % (output_file, err))
        return False

    return output_file
