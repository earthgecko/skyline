"""
plot_metric_all_events.py
"""
import logging
import os
import traceback
import datetime

import numpy as np
import matplotlib
matplotlib.use('Agg')

load_libraries = True
if load_libraries:
    import matplotlib.pyplot as plt
    from matplotlib.pylab import rcParams
    from matplotlib.dates import DateFormatter
    # @added 20230626 - Task #4962: Build and test skyline v4.0.0
    #                   Task #4778: v4.0.0 - update dependencies
    # As per https://matplotlib.org/stable/api/prev_api_changes/api_changes_3.7.0.html#the-first-parameter-of-axes-grid-and-axis-grid-has-been-renamed-to-visible
    from matplotlib import __version__ as matplotlib_version

    from settings import SKYLINE_TMP_DIR


# @added 20230127 - Feature #4830: webapp - panorama_plot_anomalies - all_events
def plot_metric_all_events(
        current_skyline_app, metric, metric_id,
        from_timestamp, until_timestamp, all_events_dict,
        plot_parameters={
            'title': None,
            'figsize': (14, 7)
        }):
    """
    Creates a png graph image using the all_events_dict and returns the path
    and filename.

    :param current_skyline_app: skyline_app
    :param metric: the name of the metric
    :param metric_id: the name of the metric
    :param from_timestamp: the from_timestamp
    :param until_timestamp: the until_timestamp
    :param all_events_dict: the all_events_dict from functions.metrics.get_metric_all_events
    :param plot_parameters: the polt parameters
    :type current_skyline_app: str
    :type metric: str
    :type metric_id: int
    :type from_timestamp: int
    :type until_timestamp: int
    :type all_events_dict: dict
    :type plot_parameters: dict
    :return: output_file
    :rtype:  str

    """

    function_str = 'functions.plots.plot_metric_all_events'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    output_file = '%s/panorama_all_events_plot.%s.%s.%s.png' % (
        SKYLINE_TMP_DIR, str(metric_id), str(from_timestamp),
        str(until_timestamp))

    if os.path.isfile(output_file):
        current_logger.info('%s :: %s :: image of %s exists' % (
            str(current_skyline_app), function_str, metric))
        return (True, output_file)

    current_logger.info('%s :: %s :: plotting all events for %s ' % (
        str(current_skyline_app), function_str, metric))

    try:
        current_logger.info('%s :: creating graph image - %s' % (function_str, output_file))
        output_dir = os.path.dirname(output_file)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, mode=0o755)

        timestamps = list(all_events_dict.keys())
        timeseries_duration = timestamps[-1] - timestamps[0]
        title = plot_parameters['title']
        if not title:
            if timeseries_duration <= 86400:
                period = timeseries_duration / 60 / 60
                title = '%s all activity over %s hours' % (metric, str(period))
            else:
                period = timeseries_duration / 60 / 60 / 24
                title = '%s all activity over %s days' % (metric, str(period))

        values = []
        triggered_algorithms_counts = []
        a_triggered_algorithms_counts = []
        mirage_counts = []
        match_counts = []
        anomaly_counts = []

        for ts in timestamps:
            value = all_events_dict[ts]['value']
            values.append(value)
            if all_events_dict[ts]['analyzer']['triggered_algorithms_count'] > 0:
                triggered_algorithms_counts.append(value)
            else:
                triggered_algorithms_counts.append(np.nan)
            if all_events_dict[ts]['analyzer_all']['triggered_algorithms_count'] > 0:
                a_triggered_algorithms_counts.append(value)
            else:
                a_triggered_algorithms_counts.append(np.nan)
            if all_events_dict[ts]['mirage']['triggered_algorithms_count'] > 0:
                mirage_counts.append(value)
            else:
                mirage_counts.append(np.nan)
            if all_events_dict[ts]['match'] > 0:
                match_counts.append(value)
            else:
                match_counts.append(np.nan)
            if all_events_dict[ts]['anomaly'] > 0:
                anomaly_counts.append(value)
            else:
                anomaly_counts.append(np.nan)

        timeseries_x = [int(ts) for ts in timestamps]
        timeseries_y = list(values)

        # Plot match
        title = '%s - all activity' % metric
        rcParams['figure.figsize'] = plot_parameters['figsize']
        fig = plt.figure(frameon=False)
        ax = fig.add_subplot(111)
        ax.set_title(title, fontsize='medium')
        ax.set_facecolor('white')
        datetimes = [datetime.datetime.utcfromtimestamp(ts) for ts in timeseries_x]
        plt.xticks(rotation=0, horizontalalignment='center')

        timeseries_duration = timeseries_x[-1] - timeseries_x[0]
        if timeseries_duration > 87000:
            xfmt = DateFormatter('%m/%d')
        else:
            xfmt = DateFormatter('%H:%M')

        plt.gca().xaxis.set_major_formatter(xfmt)
        ax.xaxis.set_major_formatter(xfmt)
        ax.plot(datetimes, timeseries_y, color='blue', lw=0.6, zorder=1, alpha=0.6, label='metric')
        ax.tick_params(axis='both', labelsize='medium')
        ax.scatter(x=datetimes, y=a_triggered_algorithms_counts, c='gold', marker='o', alpha=0.8, s=10, zorder=2, label='1st stage analysis (no CONSENSUS)')
        ax.scatter(x=datetimes, y=triggered_algorithms_counts, c='magenta', marker='o', alpha=0.8, s=10, zorder=3, label='1st stage analysis triggered')
        ax.scatter(x=datetimes, y=mirage_counts, c='cyan', marker='D', zorder=4, s=20, alpha=0.8, label='2nd stage analysis')
        ax.scatter(x=datetimes, y=anomaly_counts, c='red', marker='v', zorder=5, s=100, label='anomaly')
        ax.scatter(x=datetimes, y=match_counts, c='green', marker='P', zorder=6, s=50, label='match')

        plt.legend(loc="upper left")

        ax.get_yaxis().get_major_formatter().set_useOffset(False)
        ax.get_yaxis().get_major_formatter().set_scientific(False)
        box = ax.get_position()
        ax.set_position([box.x0, box.y0 + box.height * 0.1,
                         box.width, box.height * 0.9])
        ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05),
                  fancybox=True, shadow=True, ncol=4, fontsize='medium')
        plt.rc('lines', lw=2, color='grey')
        plt.grid(True)
        # @modified 20230626 - Task #4962: Build and test skyline v4.0.0
        #                      Task #4778: v4.0.0 - update dependencies
        # As per https://matplotlib.org/stable/api/prev_api_changes/api_changes_3.7.0.html#the-first-parameter-of-axes-grid-and-axis-grid-has-been-renamed-to-visible
        if matplotlib_version < '3.7.0':
            ax.grid(b=True, which='both', axis='both', color='lightgray',
                    linestyle='solid', alpha=0.5, linewidth=0.6)
        else:
            ax.grid(visible=True, which='both', axis='both', color='lightgray',
                    linestyle='solid', alpha=0.5, linewidth=0.6)

        ax.set_facecolor('white')
        rcParams['xtick.direction'] = 'out'
        rcParams['ytick.direction'] = 'out'
        ax.margins(y=.02, x=.03)
        plt.savefig(output_file, format='png')
        fig.clf()
        plt.close(fig)
        current_logger.info('%s :: created graph image - %s' % (function_str, output_file))
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to create %s' % (function_str, output_file))
        return (False, None)

    return (True, output_file)
