"""
Plot motif related graphs
"""
import logging
import os
import traceback
import datetime as dt
import matplotlib
matplotlib.use('Agg')
if True:
    import matplotlib.pyplot as plt
    # import matplotlib.image as mpimg
    from matplotlib.pylab import rcParams
    from matplotlib.dates import DateFormatter


# @added 20200512 - Bug #2534: Ionosphere - fluid approximation - IONOSPHERE_MINMAX_SCALING_RANGE_TOLERANCE on low ranges
#                   Feature #2404: Ionosphere - fluid approximation
def plot_motif_match(
    current_skyline_app, metric, metric_timestamp, fp_id, full_duration,
        generation_str, motif_id, index, size, distance, type_id, fp_motif,
        not_anomalous_motif_sequence, output_file,
        on_demand_motif_analysis=False):
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
        current_logger.info('plot_motif_match - graph image already exists - %s' % output_file)
        return (True, output_file)

    try:
        current_logger.info('plot_motif_match - creating graph image - %s' % output_file)
        not_anomalous_motif = [item[1] for item in not_anomalous_motif_sequence]
        # Plot match
        rcParams['figure.figsize'] = 8, 4
        fig = plt.figure(frameon=False)
        ax = fig.add_subplot(111)
        not_anomalous_timestamp = int(not_anomalous_motif_sequence[-1][0])
        not_anomalous_date_str = dt.datetime.fromtimestamp(not_anomalous_timestamp).isoformat()

        # @added 20210415 - Feature #4014: Ionosphere - inference
        # I realised looking through the graphs with Sab that users of Skyline are
        # normally used to 7 days graps mostly, 24hour graphs and 30d graphs.
        # They are not used to minutes.
        # Make the user aware of the specific resolution they are viewing, a new
        # UI resolution for Skyline (LAST X MINUTES).  Thank you my love.
        # For everything.
        graph_period_seconds = not_anomalous_timestamp - int(not_anomalous_motif_sequence[0][0])
        graph_period_minutes = round(graph_period_seconds / 60)
        graph_period_minutes_str = '%s minutes' % str(graph_period_minutes)

        if type_id == 1:
            graph_title = '%s\nMATCHED fp id %s - EXACT MATCH - %s (LAST %s MINUTES)' % (
                metric, str(fp_id), not_anomalous_date_str, graph_period_minutes)
        elif type_id == 2:
            graph_title = '%s\nMATCHED fp id %s - ALL IN RANGE - %s (LAST %s MINUTES)' % (
                metric, str(fp_id), not_anomalous_date_str, graph_period_minutes)
        elif type_id == 4:
            graph_title = '%s\nNOT SIMILAR ENOUGH fp id %s - %s (LAST %s MINUTES)' % (
                metric, str(fp_id), not_anomalous_date_str, graph_period_minutes)
        elif type_id == 5:
            graph_title = '%s\nINVALIDATED fp id %s - %s (LAST %s MINUTES)' % (
                metric, str(fp_id), not_anomalous_date_str, graph_period_minutes)
        elif type_id == 6:
            graph_title = '%s\nMATCHED fp id %s - ON DISTANCE - %s (LAST %s MINUTES)' % (
                metric, str(fp_id), not_anomalous_date_str, graph_period_minutes)
        else:
            graph_title = '%s\nMATCHED fp id %s - in_range - %s (LAST %s MINUTES)' % (
                metric, str(fp_id), not_anomalous_date_str, graph_period_minutes)
        ax.set_title(graph_title, fontsize='medium')
        if hasattr(ax, 'set_facecolor'):
            ax.set_facecolor('white')
        else:
            ax.set_axis_bgcolor('white')
        datetimes = [dt.datetime.utcfromtimestamp(int(item[0])) for item in not_anomalous_motif_sequence]
        plt.xticks(rotation=0, horizontalalignment='center')
        # if full_duration == FULL_DURATION:
        #     xfmt = DateFormatter('%H:%M:%S')
        # else:
        #     xfmt = DateFormatter('%H:%M')
        xfmt = DateFormatter('%H:%M:%S')

        plt.gca().xaxis.set_major_formatter(xfmt)
        ax.xaxis.set_major_formatter(xfmt)

        not_anomalous_label = 'potentially anomalous motif'
        # @modified 20210415 - Feature #4014: Ionosphere - inference
        # ax.plot(datetimes, not_anomalous_motif, label=not_anomalous_label, color='red', lw=1 zorder=3)
        # ax.plot(datetimes, not_anomalous_motif, label=not_anomalous_label, color='red', lw=1, markevery=not_anomalous_motif, zorder=3)
        # ax.plot(datetimes, not_anomalous_motif, 'ro', label=not_anomalous_label, color='red', lw=1, zorder=3)
        # ax.plot(datetimes, not_anomalous_motif, 'ro', label=not_anomalous_label, color='red', lw=1, marker='o', linestyle='solid', markersize=4, zorder=3)
        not_anomalous_line_color = 'red'
        if on_demand_motif_analysis:
            if type_id == 1:
                not_anomalous_line_color = 'green'
            if type_id == 2:
                not_anomalous_line_color = 'green'
            if type_id == 3:
                not_anomalous_line_color = 'green'
            if type_id == 4:
                not_anomalous_line_color = 'red'
            if type_id == 5:
                not_anomalous_line_color = 'red'
            if type_id == 6:
                not_anomalous_line_color = 'green'

        if len(not_anomalous_motif) > 60:
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
        matched_label = 'fp_id %s (%s as normal) - similar motif found' % (str(fp_id), generation_str)
        ax.plot(datetimes, fp_motif, lw=1, label=matched_label, color='blue', ls='--', zorder=4)
        ax.get_yaxis().get_major_formatter().set_useOffset(False)
        ax.get_yaxis().get_major_formatter().set_scientific(False)
        box = ax.get_position()
        ax.set_position([box.x0, box.y0 + box.height * 0.1,
                         box.width, box.height * 0.9])
        ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1),
                  fancybox=True, shadow=True, ncol=2, fontsize='x-small')
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
        # @added 20210415 - Feature #4014: Ionosphere - inference
        # bbox = dict(boxstyle='square', lw=3, ec='gray',
        #             fc=(0.9, 0.9, .9, .5), alpha=0.5)
        # plt.text(0.5, 0.5, graph_period_minutes_str, ha='center', va='center',
        #          rotation=0, fontsize=18, color='gray', alpha=0.5, bbox=bbox)
        # plt.text(0.6, 0.7, graph_period_minutes_str, size=18, rotation=0,
        #          ha="center", va="center", color='black', alpha=0.5,
        #          bbox=dict(boxstyle="round",
        #                    ec=(1., 0.5, 0.5),
        #                    fc=(1., 0.8, 0.8),
        #                    )
        #          )
        text_x = datetimes[0]
        text_y = max(not_anomalous_motif) - ((max(not_anomalous_motif) / 10) * 1)
        if type_id == 4:
            ax_text = 'motif spans %s\nwith %s data points\nNOT SIMILAR ENOUGH TO MATCH' % (graph_period_minutes_str, str(len(not_anomalous_motif)))
            ax_text_color = 'red'
        elif type_id == 5:
            ax_text = 'motif spans %s\nwith %s data points\nINVALIDATED' % (graph_period_minutes_str, str(len(not_anomalous_motif)))
            ax_text_color = 'red'
        else:
            ax_text = 'motif spans %s\nwith %s data points' % (graph_period_minutes_str, str(len(not_anomalous_motif)))
            ax_text_color = 'black'
            if on_demand_motif_analysis:
                ax_text = 'motif spans %s\nwith %s data points\nMATCH' % (graph_period_minutes_str, str(len(not_anomalous_motif)))
                ax_text_color = 'green'
        ax.text(text_x, text_y, ax_text, size=8, color=ax_text_color, alpha=0.7)
        plt.savefig(output_file, format='png')
        fig.clf()
        plt.close(fig)
        current_logger.info('plot_motif_match - created graph image - %s' % output_file)
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: plot_motif_match :: failed to create %s' % output_file)
        return (False, None)

    return (True, output_file)
