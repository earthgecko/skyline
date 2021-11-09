"""
create_matplotlib_graph
"""
import logging
import traceback
from os import path
import datetime as dt
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
if True:
    import matplotlib.pyplot as plt
    from matplotlib.pylab import rcParams
    from matplotlib.dates import DateFormatter


# @added 20200512 - Bug #2534: Ionosphere - fluid approximation - IONOSPHERE_MINMAX_SCALING_RANGE_TOLERANCE on low ranges
#                   Feature #2404: Ionosphere - fluid approximation
# @modified 20211103 - Branch #3068: SNAB
#                      Bug #4308: matrixprofile - fN on big drops
# Added anomalies
def create_matplotlib_graph(
        current_skyline_app, output_file, graph_title, timeseries,
        anomalies=[]):
    """
    Creates a png graph image using the features profile time series data
    provided or from the features profile time seires data in the DB if an empty
    list is provided.

    :param current_skyline_app: the Skyline app name calling the function
    :param output_file: full path and filename to output where the png image is
        to be saved to
    :param graph_title: the graph image title
    :param timeseries: the time series
    :param anomalies: the anomaly timestamps [optional to plot anomalies]
    :type current_skyline_app: str
    :type output_file: str
    :type graph_title: str
    :type timeseries: list
    :type anomalies: list
    :return: (status, file)
    :rtype: (boolean, str)

    """

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if path.isfile(output_file):
        current_logger.info('create_matplotlib_graph - graph image already exists - %s' % output_file)
        return (True, output_file)

    try:
        current_logger.info('create_matplotlib_graph - creating graph image - %s' % output_file)

        # @added 20211103 - Branch #3068: SNAB
        #                      Bug #4308: matrixprofile - fN on big drops
        # Added anomalies
        anomalies_indices = []
        if anomalies:
            last_timestamp = None
            for index, item in enumerate(timeseries):
                anomaly_in_period = 0
                if not last_timestamp:
                    last_timestamp = int(item[0])
                    continue
                for anomaly_ts in anomalies:
                    if anomaly_ts < last_timestamp:
                        continue
                    if anomaly_ts > item[0]:
                        continue
                    if anomaly_ts in list(range(last_timestamp, int(item[0]))):
                        anomaly_in_period = 1
                        break
                if anomaly_in_period:
                    anomalies_indices.append(index)
                last_timestamp = int(item[0])


        timeseries_x = [float(item[0]) for item in timeseries]
        timeseries_y = [item[1] for item in timeseries]
        values = pd.Series([x[1] for x in timeseries])
        array_amax = np.amax(values)
        array_amin = np.amin(values)
        mean = values.mean()
        stdDev = values.std()
        sigma3 = 3 * stdDev
        sigma3_upper_bound = mean + sigma3
        try:
            sigma3_lower_bound = mean - sigma3
        except:
            sigma3_lower_bound = 0
        sigma3_upper_series = [sigma3_upper_bound] * len(values)
        sigma3_lower_series = [sigma3_lower_bound] * len(values)
        amax_series = [array_amax] * len(values)
        amin_series = [array_amin] * len(values)
        mean_series = [mean] * len(values)
        rcParams['figure.figsize'] = 8, 4
        fig = plt.figure(frameon=False)
        ax = fig.add_subplot(111)
        ax.set_title(graph_title, fontsize='small')
        if hasattr(ax, 'set_facecolor'):
            ax.set_facecolor('black')
        else:
            ax.set_axis_bgcolor('black')
        datetimes = [dt.datetime.utcfromtimestamp(ts) for ts in timeseries_x]
        plt.xticks(rotation=0, horizontalalignment='center')
        xfmt = DateFormatter('%m/%d')
        plt.gca().xaxis.set_major_formatter(xfmt)
        ax.xaxis.set_major_formatter(xfmt)
        ax.plot(datetimes, timeseries_y, color='orange', lw=0.6, zorder=3)
        ax.tick_params(axis='both', labelsize='x-small')
        max_value_label = 'max - %s' % str(array_amax)
        ax.plot(datetimes, amax_series, lw=1, label=max_value_label, color='m', ls='--', zorder=4)
        min_value_label = 'min - %s' % str(array_amin)
        ax.plot(datetimes, amin_series, lw=1, label=min_value_label, color='b', ls='--', zorder=4)
        mean_value_label = 'mean - %s' % str(mean)
        ax.plot(datetimes, mean_series, lw=1.5, label=mean_value_label, color='g', ls='--', zorder=4)
        sigma3_text = (r'3$\sigma$')
        sigma3_upper_label = '%s upper - %s' % (str(sigma3_text), str(sigma3_upper_bound))
        ax.plot(datetimes, sigma3_upper_series, lw=1, label=sigma3_upper_label, color='r', ls='solid', zorder=4)
        if sigma3_lower_bound > 0:
            sigma3_lower_label = '%s lower - %s' % (str(sigma3_text), str(sigma3_lower_bound))
            ax.plot(datetimes, sigma3_lower_series, lw=1, label=sigma3_lower_label, color='r', ls='solid', zorder=4)

        # @added 20211103 - Branch #3068: SNAB
        #                   Bug #4308: matrixprofile - fN on big drops
        # Added anomalies
        for index in anomalies_indices:
            ax.axvline(x=datetimes[index], color='r', ls='--', lw=6, alpha=0.3, zorder=5)

        ax.get_yaxis().get_major_formatter().set_useOffset(False)
        ax.get_yaxis().get_major_formatter().set_scientific(False)
        box = ax.get_position()
        ax.set_position([box.x0, box.y0 + box.height * 0.1,
                         box.width, box.height * 0.9])
        ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05),
                  fancybox=True, shadow=True, ncol=4, fontsize='x-small')
        plt.rc('lines', lw=2, color='w')
        plt.grid(True)
        ax.grid(b=True, which='both', axis='both', color='lightgray',
                linestyle='solid', alpha=0.5, linewidth=0.6)
        if hasattr(ax, 'set_facecolor'):
            ax.set_facecolor('black')
        else:
            ax.set_axis_bgcolor('black')
        rcParams['xtick.direction'] = 'out'
        rcParams['ytick.direction'] = 'out'
        ax.margins(y=.02, x=.03)
        plt.savefig(output_file, format='png')
        fig.clf()
        plt.close(fig)
        current_logger.info('create_matplotlib_graph - created graph image - %s' % output_file)
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: create_matplotlib_graph :: failed to create %s' % output_file)
        return (False, None)

    return (True, output_file)
