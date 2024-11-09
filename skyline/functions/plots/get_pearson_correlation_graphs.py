"""
pearson_correlation_graphs.py
"""
import json
import os
import logging
import sys
import datetime
from time import time
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.style as mplstyle
from matplotlib.pylab import rcParams
from matplotlib.dates import DateFormatter
import numpy as np
import pandas as pd
import pytz

import settings
from functions.graphite.get_metrics_timeseries import get_metrics_timeseries
from functions.luminosity.pearson_closest import pearson_closest
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric
from skyline_functions import mkdir_p

mplstyle.use('fast')

# @added 20240406 - Feature #5322: pearson_correlation_graphs
#                   Feature #5026: luminosity - pearson_closest
def get_pearson_correlation_graphs(
        current_skyline_app, metric, metric_timestamp,
        pearson_correlation_metrics, output_dir, tz='UTC',
        metrics_timeseries={}, metrics_timeseries_use_key='timeseries',
        pearson_correlations={}, abs_threshold=0.5):
    """
    Plot pearson correlation graphs

    :param current_skyline_app: the app calling the function
    :param metric_data_archive: the metric_data_archive, path and file
    :param undownsampled_archive: the undownsampled_archive, path and file
    :type current_skyline_app: str
    :type metric_data_archive: str
    :type undownsampled_archive: str
    :return: do_not_alert_on_stale_metrics_list
    :rtype: list

    """

    pearson_correlation_graphs = []

    function_str = 'functions.plot.pearson_correlation_graphs'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    pearson_correlations_file = '%s/%s.pearson_correlations.json' % (output_dir, metric)
    metric_labelled_metric_name = None
    if '{' in metric and metric.endswith('}'):
        metric_id = 0
        try:
            metric_id = get_metric_id_from_base_name(current_skyline_app, metric)
        except Exception as err:
            current_logger.error('error :: get_metric_id_from_base_name failed with metric: %s - %s' % (str(metric), err))
        labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
        metric_labelled_metric_name = labelled_metric_name
        pearson_correlations_file = '%s/%s.pearson_correlations.json' % (output_dir, labelled_metric_name)

    if os.path.exists(output_dir):
        for dir_path, folders, files in os.walk(output_dir):
            try:
                if files:
                    for i in files:
                        path_and_file = '%s/%s' % (dir_path, i)
                        if i.endswith('.person_correlation.png'):
                            pearson_correlation_graphs.append(path_and_file)
                        if i == pearson_correlations_file:
                            with open(pearson_correlations_file, 'r') as fh:
                                pearson_correlations = json.load(fh)
            except Exception as err:
                current_logger.error('error :: %s :: os.walk failed on %s, err: %s' % (
                    function_str, str(output_dir), err))
    if len(pearson_correlation_graphs) > 0 or len(pearson_correlations) > 0:
        if len(pearson_correlation_graphs) == len(pearson_correlations):
            current_logger.info('%s :: %s pearson correlation plots and pearson_correlations json already exist in %s, returning' % (
                function_str, str(len(pearson_correlation_graphs)), output_dir))
            return pearson_correlation_graphs, pearson_correlations

    local_tz = pytz.timezone(tz)

    start_run = int(time())

    if not os.path.exists(output_dir):
        try:
            mkdir_p(output_dir)
        except Exception as err:
            current_logger.error('error :: %s :: mkdir_p failed, err: %s' % (
                function_str, err))

    current_logger.info('%s :: creating %s pearson correlation plots in %s' % (
        function_str, str(len(pearson_correlation_metrics)), output_dir))

    until_timestamp = int(metric_timestamp)
    from_timestamp = until_timestamp - (86400)

    labelled_metrics = []

    # If the metrics_timeseries dict is not passed create it
    if not metrics_timeseries:
        metrics_timeseries_use_key = 'downsampled_timeseries'
        metrics_functions = {}
        metrics_functions[metric] = {}
        metrics_functions[metric]['functions'] = None
        for base_name in pearson_correlation_metrics:
            if '{' in base_name and base_name.endswith('}'):
                labelled_metrics.append(base_name)
                continue
            if base_name == metric:
                continue
            metrics_functions[base_name] = {}
            metrics_functions[base_name]['functions'] = None
        current_logger.info('%s :: fetching time series data for %s metrics' % (
            function_str, str(len(metrics_functions))))
        try:
            metrics_timeseries = get_metrics_timeseries(current_skyline_app, metrics_functions, from_timestamp, until_timestamp, log=False)
        except Exception as err:
            current_logger.error('error :: %s :: get_metrics_timeseries failed, err: %s' % (
                function_str, err))

        if labelled_metrics:
            for base_name in labelled_metrics:
                try:
                    timeseries = get_victoriametrics_metric(current_skyline_app, base_name, from_timestamp, until_timestamp, 'list', 'object')
                    metrics_timeseries[base_name] = {'timeseries': timeseries}
                except Exception as err:
                    current_logger.error('error :: %s :: get_victoriametrics_metric failed to get time series for %s, err: %s' % (
                        function_str, base_name, err))

        if metrics_timeseries:
            current_logger.info('%s :: fetched time series data for %s metrics in %2f seconds' % (
                function_str, str(len(metrics_timeseries)), (time() - start_run)))
        resolution = 60
        all_aligned_timestamps = []
        for i_metric in list(metrics_timeseries.keys()):
            metric_timeseries = metrics_timeseries[i_metric]['timeseries']
            aligned_metric_timeseries = []
            for t, v in metric_timeseries:
                aligned_timestamp = int(t // resolution * resolution)
                all_aligned_timestamps.append(aligned_timestamp)
                aligned_metric_timeseries.append([aligned_timestamp, v])
            metrics_timeseries[i_metric]['aligned_timeseries'] = aligned_metric_timeseries
        unique_aligned_timestamps = sorted(list(set(all_aligned_timestamps)))
        start = unique_aligned_timestamps[0]
        end = unique_aligned_timestamps[-1]
        ct = int(start)
        aligned_timestamps = []
        while ct < end:
            aligned_timestamps.append(ct)
            ct = ct + resolution
        aligned_timestamps.append(ct)
        start_resample = time()
        for i_metric in list(metrics_timeseries.keys()):
            metric_timeseries_dict = {}
            first_value = None
            for t, v in metrics_timeseries[i_metric]['aligned_timeseries']:
                if isinstance(v, float):
                    if not np.isnan(v):
                        first_value = v
                        break
            last_value = None
            for t, v in metrics_timeseries[i_metric]['aligned_timeseries']:
                metric_timeseries_dict[t] = v
            df_timeseries = []
            # for t in unique_aligned_timestamps:
            aligned_filled_timeseries = []
            for t in aligned_timestamps:
                data_v = np.nan
                try:
                    data_v = float(metric_timeseries_dict[t])
                    if np.isnan(data_v):
                        if last_value:
                            data_v = last_value
                        else:
                            data_v = first_value
                    last_value = data_v
                except:
                    if last_value:
                        data_v = last_value
                    else:
                        data_v = first_value
                df_timeseries.append(data_v)
                aligned_filled_timeseries.append([t, data_v])
            try:
                metrics_timeseries[i_metric]['aligned_filled_timeseries'] = aligned_filled_timeseries
            except Exception as err:
                current_logger.error('error :: %s :: aligned_filled_timeseries failed on %s, err: %s' % (
                    function_str, i_metric, err))

            use_timeseries = list(aligned_filled_timeseries)
            df = None
            df = pd.DataFrame(use_timeseries, columns=['date', 'value'])
            df['date'] = pd.to_datetime(df['date'], unit='s')
            datetime_index = pd.DatetimeIndex(df['date'].values)
            df = df.set_index(datetime_index)
            df.drop('date', axis=1, inplace=True)
            resampled_timeseries = []
            data_points_per_period = 1
            data_points_per_period = 600 / resolution
            T = '%sT' % str(data_points_per_period)
            resampled_df = df.resample(T, origin='end').bfill()
            resampled_values = resampled_df['value'].to_numpy().tolist()
            resampled_ts_df = resampled_df.copy()
            resampled_ts_df['ts'] = resampled_ts_df.index.astype(np.int64) // 10**9
            resampled_timestamps = resampled_ts_df['ts'].to_numpy().tolist()
            resampled_timeseries = [[t, resampled_values[index]] for index, t in enumerate(resampled_timestamps)]
            try:
                metrics_timeseries[i_metric]['downsampled_timeseries'] = resampled_timeseries
            except Exception as err:
                current_logger.error('error :: %s :: downsampled_timeseries failed on %s, err: %s' % (
                    function_str, i_metric, err))
        current_logger.info('%s :: resampling and downsampling took %2f seconds' % (
            function_str, (time() - start_resample)))

    if not metrics_timeseries:
        current_logger.error('error :: %s :: no metrics_timeseries dict nothing to plot' % (
            function_str))
        return pearson_correlation_graphs, pearson_correlations

    start_correlations = time()
    metric_timeseries = None
    all_results = []

    metric_timeseries = []
    try:
        metric_timeseries = metrics_timeseries[metric][metrics_timeseries_use_key]
    except Exception as err:
        current_logger.error('error :: %s :: failed to create metric_timeseries, err: %s' % (
            function_str, err))
        return pearson_correlation_graphs, pearson_correlations
    if not pearson_correlations:
        for base_name in pearson_correlation_metrics:
            if base_name == metric:
                continue
            metric2 = base_name
            pearson_cc = None
            try:
                pearson_cc, reason, results = pearson_closest(current_skyline_app, metric, metric2, from_timestamp=from_timestamp, until_timestamp=until_timestamp, downsample=None, datapoints=None, print_debug=False, return_results=True, return_timeseries=False, metric_timeseries=metric_timeseries, metrics_timeseries=metrics_timeseries, metrics_timeseries_use_key=metrics_timeseries_use_key)
                current_logger.debug('debug :: %s :: pearson_closest %s with %s, pearson_cc: %s' % (
                    function_str, metric, metric2, str(pearson_cc)))

            except Exception as err:
                current_logger.error('error :: %s :: pearson_closest failed to correlate %s with %s, err: %s' % (
                    function_str, metric, metric2, err))
            if isinstance(pearson_cc, float): 
                pearson_correlations[base_name] = {'pearson_cc': pearson_cc}
            else:
                pearson_correlations[base_name] = {'pearson_cc': None, 'reason': reason}
            all_results.append([base_name, results])
        current_logger.info('%s :: %s correlations done, took %2f seconds' % (
            function_str, str(len(pearson_correlations)), (time() - start_correlations)))

        if pearson_correlations:
            if not os.path.isfile(pearson_correlations_file):
                try:
                    with open(pearson_correlations_file, 'w') as fh:
                        json.dump(pearson_correlations, fh)
                    current_logger.info('%s :: added pearson_correlations json file :: %s' % (
                        function_str, pearson_correlations_file))
                except Exception as err:
                    current_logger.error('error :: %s :: failed to add %s, err: %s' % (
                        function_str, pearson_correlations_file, err))

    corr_list = []
    for base_name in list(pearson_correlations.keys()):
        try:
            pearson_cc = pearson_correlations[base_name]['pearson_cc']
        except:
            pearson_cc = None
        if pearson_cc is None:
            pearson_correlations[base_name]['abs_pearson_cc'] = None
            continue
        try:
            if np.isnan(pearson_cc):
                pearson_correlations[base_name]['abs_pearson_cc'] = None
                continue
        except Exception as err:
            print(str(pearson_cc), err)
            break
        pearson_cc = pearson_correlations[base_name]['pearson_cc']
        if pearson_cc < 0:
            abs_pearson_cc = pearson_cc * -1
        else:
            abs_pearson_cc = float(pearson_cc)
        pearson_correlations[base_name]['abs_pearson_cc'] = abs_pearson_cc
        corr_list.append([base_name, abs_pearson_cc, pearson_correlations[base_name]['pearson_cc']])
    sorted_correlations = sorted(corr_list, key=lambda x: x[1], reverse=True)

    mtimeseries = []
    try:
        mtimeseries = metrics_timeseries[metric][metrics_timeseries_use_key]
    except Exception as err:
        current_logger.error('error :: %s :: failed to create mtimeseries, err: %s' % (
            function_str, err))
        return pearson_correlation_graphs, pearson_correlations

    start_plots = time()
    mtimeseries_timestamps = [t for t, v in mtimeseries]
    mtimeseries_values = [v for t, v in mtimeseries]
    datetimes = [datetime.datetime.fromtimestamp(int(item[0]), local_tz) for item in mtimeseries]
    end_index_datestr = datetime.datetime.fromtimestamp(mtimeseries[-1][0], local_tz).strftime('%Y-%m-%d %H:%M:%S')

    for index, item in enumerate(sorted_correlations):
        base_name = item[0]
        if base_name == metric:
            continue
        if item[1] < abs_threshold:
            current_logger.info('%s :: not plotting %s < %s, pearson_cc: %s' % (
                function_str, base_name, str(abs_threshold), str(item[1])))
            continue
        running_for = time() - start_run
        if running_for >= 30:
            current_logger.info('%s :: not plotting any more, max run time reached' % (
                function_str))
            break
        output_file = '%s/%s.%s.pearson_correlation.png' % (output_dir, str(index), base_name)

        labelled_metric_name = None
        if base_name in labelled_metrics:
            metric_id = 0
            try:
                metric_id = get_metric_id_from_base_name(current_skyline_app, base_name)
            except Exception as err:
                current_logger.error('error :: get_metric_id_from_base_name failed with base_name: %s - %s' % (str(base_name), err))
            labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
            output_file = '%s/%s.%s.pearson_correlation.png' % (output_dir, str(index), labelled_metric_name)

        if os.path.isfile(output_file):
            current_logger.info('%s :: plot already exists, %s' % (function_str, output_file))
            if output_file not in pearson_correlation_graphs:
                pearson_correlation_graphs.append(output_file)
            continue
        current_logger.info('%s :: plotting %s' % (function_str, output_file))
        use_metric = str(metric)
        if metric_labelled_metric_name:
            use_metric = str(metric_labelled_metric_name)
        use_base_name = str(base_name)
        if labelled_metric_name:
            use_base_name = str(labelled_metric_name)

        try:
            pearson_cc = pearson_correlations[base_name]
            ctimeseries = metrics_timeseries[base_name][metrics_timeseries_use_key]
            timeseries_dict = {}
            for t, v in ctimeseries:
                timeseries_dict[t] = v
            # The time series must be the same length to plot together
            ctimeseries = []
            for t in mtimeseries_timestamps:
                try:
                    v = timeseries_dict[t]
                except:
                    v = np.nan
                ctimeseries.append([t, v])
            ctimeseries_values = [v for t, v in ctimeseries]
            matplotlib.rcParams['path.simplify_threshold'] = 1.0
            rcParams['figure.figsize'] = 10, 4
            plt.style.use('fast')
            rcParams['path.simplify_threshold'] = 1.0
            fig, ax = plt.subplots(1, 1, figsize=(8, 4), frameon=False)
            axb = ax.twinx()
            plt.xticks(rotation=0, horizontalalignment='center')
            xfmt = DateFormatter('%H:%M')
            plt.gca().xaxis.set_major_formatter(xfmt)
            ax.xaxis.set_major_formatter(xfmt)
            graph_title = '%s\n%s - abs_pearson_cc: %s\ncorrelated with %s' % (
                use_base_name, end_index_datestr, pearson_cc['abs_pearson_cc'], use_metric)
            ax.set_title(graph_title, fontsize='small')
            if hasattr(ax, 'set_facecolor'):
                ax.set_facecolor('white')
            else:
                ax.set_axis_bgcolor('white')
            base_line_color = 'green'
            l1 = ax.plot(datetimes, ctimeseries_values, label=use_base_name, color=base_line_color, lw=0.6, linestyle='solid', zorder=4)
            ax.tick_params(axis='both', labelsize='small')
            l2 = axb.plot(datetimes, mtimeseries_values, lw=0.6, label=use_metric, color='blue', ls='--', zorder=3, alpha=0.5)
            ax.get_yaxis().get_major_formatter().set_useOffset(False)
            ax.get_yaxis().get_major_formatter().set_scientific(False)
            ax.set_ylabel(use_base_name, size=6)
            axb.set_ylabel(use_metric, size=6)
            box = ax.get_position()
            ax.set_position([box.x0, box.y0 + box.height * 0.1,
                            box.width, box.height * 0.9])
            ax.legend(handles=l1+l2, loc='upper center', bbox_to_anchor=(0.5, -0.05),
                    fancybox=True, shadow=True, ncol=2, fontsize='small')
#            fig.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05),
#                    fancybox=True, shadow=True, ncol=2, fontsize='small')
            plt.rc('lines', lw=1, color='black')
            if hasattr(ax, 'set_facecolor'):
                ax.set_facecolor('white')
            else:
                ax.set_axis_bgcolor('white')
            rcParams['xtick.direction'] = 'out'
            rcParams['ytick.direction'] = 'out'
            ax.margins(y=.01, x=.01)
            # plt.tight_layout()
            plt.savefig(output_file, format='png')
            fig.clf()
            plt.close(fig)
            pearson_correlation_graphs.append(output_file)
            pearson_correlations[base_name]['labelled_metric_name'] = labelled_metric_name
            pearson_correlations[base_name]['metric'] = metric
            pearson_correlations[base_name]['metric_labelled_metric_name'] = metric_labelled_metric_name
            pearson_correlations[base_name]['graph'] = output_file
            graph_url = '%s/luminosity_images?image=%s' % (settings.SKYLINE_URL, output_file)
            pearson_correlations[base_name]['graph_url'] = graph_url
        except Exception as err:
            current_logger.error('error :: %s :: failed to create plot for %s, err: %s' % (
                function_str, base_name, err))

    current_logger.info('%s :: plotted %s pearson correlation graphs in %2f seconds' % (
        function_str, str(len(pearson_correlation_graphs)),
        (time() - start_plots)))
    current_logger.info('%s :: completed in %2f seconds' % (
        function_str, (time() - start_run)))
    return pearson_correlation_graphs, pearson_correlations
