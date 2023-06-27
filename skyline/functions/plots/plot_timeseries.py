"""
plot_timeseries.py
"""
import logging
import os
import traceback
import datetime as dt
import textwrap
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


def plot_timeseries(
        current_skyline_app, metric, timeseries, output_file,
        plot_parameters={
            'title': None, 'line_color': 'blue', 'bg_color': 'black',
            'figsize': (8, 4)
        }):
    """
    Creates a png graph image using the time series data and returns the path
    and filename.

    :param current_skyline_app: skyline_app
    :param metric: the name of the metric
    :param timeseries: the timeseries to plot
    :param title: the plot title
    :param output_file: the full path and filename (including .png extension) to
        save to plot as
    :type current_skyline_app: str
    :type metric: str
    :type timeseries: list
    :type title: str
    :type output_file: str
    :return: output_file
    :rtype:  str

    """

    function_str = 'functions.plot.plot_timeseries'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if os.path.isfile(output_file):
        current_logger.info('%s :: %s :: image of %s exists' % (
            str(current_skyline_app), function_str, metric))
        return (True, output_file)

    current_logger.info('%s :: %s :: plotting %s ' % (
        str(current_skyline_app), function_str, metric))

    try:
        current_logger.info('%s :: creating graph image - %s' % (function_str, output_file))
        output_dir = os.path.dirname(output_file)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, mode=0o755)

        timeseries_duration = timeseries[-1][0] - timeseries[0][0]
        title = plot_parameters['title']
        if not title:
            if timeseries_duration <= 86400:
                period = timeseries_duration / 60 / 60
                title = '%s hours' % str(period)
            else:
                period = timeseries_duration / 60 / 60 / 24
                title = '%s days' % str(period)

        # @added 20230102 - Task #2732: Prometheus to Skyline
        #                   Branch #4300: prometheus
        # Report correct days for new metrics
        if 'days' in title:
            try:
                replace_days = False
                title_days_str = title.split(' ')[0]
                title_days = float(title_days_str)
                days = round((timeseries_duration / 86400), 1)
                if days < 7.0:
                    replace_days = True
                if '30 days' in title and days < title_days:
                    replace_days = True
                if replace_days:
                    replace_days_str = '%s days' % title_days_str
                    new_days_str = '%s days' % str(days)
                    title = title.replace(replace_days_str, new_days_str)
                    title = '%s - (%s not available)' % (title, replace_days_str)
                    current_logger.info('%s :: %s :: corrected duration in title changed %s days to %s days' % (
                        str(current_skyline_app), function_str, replace_days_str, new_days_str))
            except Exception as err:
                current_logger.error('%s :: %s :: an error occurred checking days duration in title - %s' % (
                    str(current_skyline_app), function_str, err))

        # Plot match
        # rcParams['figure.figsize'] = 8, 4
        rcParams['figure.figsize'] = plot_parameters['figsize']
        fig = plt.figure(frameon=False)
        ax = fig.add_subplot(111)

        graph_title = str(title)
        title_fontsize = 'medium'
        if len(title) > 200:
            graph_title = textwrap.fill(title, width=140, break_long_words=False)
            title_fontsize = 'xx-small'

        fontsize = 'small'
        use_label = str(metric)
        if len(metric) > 200:
            use_label = textwrap.fill(metric, width=140, break_long_words=True)
            fontsize = 'xx-small'

        ax.set_title(graph_title, fontsize=title_fontsize)
        if hasattr(ax, 'set_facecolor'):
            ax.set_facecolor(plot_parameters['bg_color'])
        else:
            ax.set_axis_bgcolor(plot_parameters['bg_color'])
        datetimes = [dt.datetime.utcfromtimestamp(int(item[0])) for item in timeseries]
        plt.xticks(rotation=0, horizontalalignment='center')
        if timeseries_duration <= 86400:
            xfmt = DateFormatter('%H:%M:%S')
        else:
            xfmt = DateFormatter('%Y-%m-%d')

        plt.gca().xaxis.set_major_formatter(xfmt)
        ax.xaxis.set_major_formatter(xfmt)

        values = [float(item[1]) for item in timeseries]
        ax.plot(
            datetimes, values, label=use_label,
            color=plot_parameters['line_color'], lw=1,
            linestyle='solid')
        ax.tick_params(axis='both', labelsize='small')
        try:
            ax.get_yaxis().get_major_formatter().set_useOffset(False)
        except Exception as err:
            current_logger.warning('warning :: %s :: get_major_formatter - %s' % (function_str, err))
        try:
            ax.get_yaxis().get_major_formatter().set_scientific(False)
        except Exception as err:
            current_logger.warning('warning :: %s :: get_major_formatter - %s' % (function_str, err))

        box = ax.get_position()
        ax.set_position([box.x0, box.y0 + box.height * 0.1,
                         box.width, box.height * 0.9])
        ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1),
                  fancybox=True, shadow=True, ncol=2, fontsize=fontsize)
        plt.rc('lines', lw=1, color='black')
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

        if hasattr(ax, 'set_facecolor'):
            ax.set_facecolor(plot_parameters['bg_color'])
        else:
            ax.set_axis_bgcolor(plot_parameters['bg_color'])
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
