"""
Plot motif related graphs
"""
import logging
import os
import traceback
import datetime as dt
import matplotlib
matplotlib.use('Agg')

load_libraries = True
if load_libraries:
    import matplotlib.pyplot as plt
    # import matplotlib.image as mpimg
    from matplotlib.pylab import rcParams
    from matplotlib.dates import DateFormatter


# @added 20220317 - Feature #4540: Plot matched timeseries
#                   Feature #4014: Ionosphere - inference
def plot_fp_match(
    current_skyline_app, metric, fp_id, fp_values, not_anomalous_timeseries,
        output_file, strip_prefix=False):
    """
    Creates a png graph image using the features profile time seires data and
    the training data data, if it existing otherwise grab it from Graphite.

    :param current_skyline_app: the Skyline app name calling the function
    :param output_file: full path and filename to output where the png image is
        to be saved to
    :param graph_title: the graph image title
    :param timeseries: the time series
    :type current_skyline_app: str
    :type output_file: str
    :type graph_title: str
    :type timeseries: list
    :return: (status, file)
    :rtype: (boolean, str)

    """

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if os.path.isfile(output_file):
        current_logger.info('plot_fp_match - graph image already exists - %s' % output_file)
        return (True, output_file)

    try:
        current_logger.info('plot_fp_match - creating graph image - %s' % output_file)
        matched_timeseries_length = len(not_anomalous_timeseries)

        # Ensure timesereis are same length
        if len(fp_values) > matched_timeseries_length:
            fp_values = fp_values[-matched_timeseries_length:]
        if len(fp_values) < matched_timeseries_length:
            not_anomalous_timeseries = not_anomalous_timeseries[-len(fp_values):]

        not_anomalous_motif = [item[1] for item in not_anomalous_timeseries]

        # Plot match
        rcParams['figure.figsize'] = 8, 4
        fig = plt.figure(frameon=False)
        ax = fig.add_subplot(111)
        not_anomalous_timestamp = int(not_anomalous_timeseries[-1][0])
        not_anomalous_date_str = dt.datetime.fromtimestamp(not_anomalous_timestamp).isoformat()

        # @added 20210415 - Feature #4014: Ionosphere - inference
        # I realised looking through the graphs with Sab that users of Skyline are
        # normally used to 7 days graps mostly, 24hour graphs and 30d graphs.
        # They are not used to minutes.
        # Make the user aware of the specific resolution they are viewing, a new
        # UI resolution for Skyline (LAST X MINUTES).  Thank you my love.
        # For everything.
        graph_period_seconds = not_anomalous_timestamp - int(not_anomalous_timeseries[0][0])
        graph_period_minutes = round(graph_period_seconds / 60)
        graph_period_minutes_str = '%s minutes' % str(graph_period_minutes)

        graph_title = '%s\nMATCHED fp id %s at %s' % (
            metric, str(fp_id), not_anomalous_date_str)
        if strip_prefix:
            metric_elements = metric.split('.')
            metric = '.'.join(metric_elements[1:])
            graph_title = '%s\nMATCHED trained pattern %s at %s' % (
                metric, str(fp_id), not_anomalous_date_str)

        ax.set_title(graph_title, fontsize='medium')
        if hasattr(ax, 'set_facecolor'):
            ax.set_facecolor('white')
        else:
            ax.set_axis_bgcolor('white')
        datetimes = [dt.datetime.utcfromtimestamp(int(item[0])) for item in not_anomalous_timeseries]
        plt.xticks(rotation=0, horizontalalignment='center')
        # if full_duration == FULL_DURATION:
        #     xfmt = DateFormatter('%H:%M:%S')
        # else:
        #     xfmt = DateFormatter('%H:%M')
        xfmt = DateFormatter('%H:%M:%S')

        plt.gca().xaxis.set_major_formatter(xfmt)
        ax.xaxis.set_major_formatter(xfmt)

        not_anomalous_label = 'potential anomalous timeseries'
        not_anomalous_line_color = 'green'

        if len(not_anomalous_timeseries) > 60:
            # Do not plot loads of markers
            ax.plot(
                datetimes, not_anomalous_motif, label=not_anomalous_label,
                color=not_anomalous_line_color, lw=1, linestyle='solid',
                zorder=3)
        else:
            ax.plot(
                datetimes, not_anomalous_motif, 'ro', label=not_anomalous_label,
                color=not_anomalous_line_color, lw=1, marker='o', linestyle='solid',
                markersize=4, zorder=3)
        ax.tick_params(axis='both', labelsize='small')
        matched_label = 'fp_id %s - similar pattern' % (str(fp_id))
        if strip_prefix:
            matched_label = 'trained pattern %s - similar' % (str(fp_id))

        ax.plot(datetimes, fp_values, lw=1, label=matched_label, color='blue', ls='--', zorder=4, alpha=0.4)
        ax.get_yaxis().get_major_formatter().set_useOffset(False)
        ax.get_yaxis().get_major_formatter().set_scientific(False)
        box = ax.get_position()
        ax.set_position([box.x0, box.y0 + box.height * 0.1,
                         box.width, box.height * 0.9])
        ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1),
                  fancybox=True, shadow=True, ncol=2, fontsize='small')
        plt.rc('lines', lw=1, color='black')
        plt.grid(True)
        ax.grid(b=True, which='both', axis='both', color='lightgray',
                linestyle='solid', alpha=0.5, linewidth=0.6)
        if hasattr(ax, 'set_facecolor'):
            ax.set_facecolor('white')
        else:
            ax.set_axis_bgcolor('white')
        rcParams['xtick.direction'] = 'out'
        rcParams['ytick.direction'] = 'out'
        ax.margins(y=.02, x=.03)
        text_x = datetimes[0]
        text_y = max(not_anomalous_motif) - ((max(not_anomalous_motif) / 10) * 1)
        ax_text = 'features profile spans %s\nwith %s data points' % (graph_period_minutes_str, str(len(not_anomalous_motif)))
        if strip_prefix:
            ax_text = 'pattern slice spans %s\nwith %s data points' % (graph_period_minutes_str, str(len(not_anomalous_motif)))

        ax_text_color = 'green'
        ax.text(text_x, text_y, ax_text, size=8, color=ax_text_color, alpha=0.7)
        plt.savefig(output_file, format='png')
        fig.clf()
        plt.close(fig)
        current_logger.info('plot_fp_match - created graph image - %s' % output_file)
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: plot_fp_match :: failed to create %s' % output_file)
        return (False, None)

    return (True, output_file)
