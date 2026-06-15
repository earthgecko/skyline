"""
luminosity_pearson_closest.py
"""
import copy
import datetime
import logging
import time
import traceback

from flask import request
import numpy as np
import requests

import settings
from matched_or_regexed_in_list import matched_or_regexed_in_list
from skyline_functions import get_graphite_metric
from functions.luminosity.pearson_closest import pearson_closest
from functions.metrics.get_base_name_from_labelled_metrics_name import get_base_name_from_labelled_metrics_name
from functions.metrics.get_base_names_and_metric_ids import get_base_names_and_metric_ids
from functions.plots.get_pearson_correlation_graphs import get_pearson_correlation_graphs
from functions.timeseries.determine_data_frequency import determine_data_frequency
from functions.timeseries.downsample import downsample_timeseries
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric

skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
try:
    ENABLE_WEBAPP_DEBUG = settings.ENABLE_WEBAPP_DEBUG
except:
    logger.error('error :: cannot determine ENABLE_WEBAPP_DEBUG from settings')
    ENABLE_WEBAPP_DEBUG = False

try:
    full_duration_seconds = int(settings.FULL_DURATION)
except:
    full_duration_seconds = 86400

full_duration_in_hours = full_duration_seconds / 60 / 60

# @added 20240502 - Feature #5026: luminosity - pearson_closest 
#                   Feature #5342: utilities - pearson_closest
def determine_pearson_closest(current_skyline_app):

    """
    Get pearson correlations

    :param current_skyline_app: The skyline app
    :type current_skyline_app: str
    :return: pearson_correlations
    :rtype: dict
    """

    function_str = 'determine_pearson_closest'
    pearson_correlations = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    err = 'None'
    metric = None
    try:
        metric = request.args.get('metric')
        current_logger.info('%s :: with metric: %s' % (function_str, str(metric)))
    except Exception as err:
        err = err
        metric = None
    metric_id = None
    try:
        metric_id = int(request.args.get('metric_id'))
        current_logger.info('%s :: with metric_id: %s' % (function_str, str(metric_id)))
    except Exception as err:
        err = err
        metric_id = None
    if not metric and not metric_id:
        current_logger.info('%s :: no metric or metric_id argument passed' % (
            function_str))
        return pearson_correlations

    err = 'None'
    namespaces = None
    try:
        namespaces_str = request.args.get('namespaces')
        if len(namespaces_str) > 0:
            current_logger.info('%s :: with namespaces parameter: %s' % (function_str, str(namespaces_str)))
            namespaces = namespaces_str.split(',')
            if namespaces_str == 'all':
                namespaces = ['.*']
            current_logger.info('%s :: with namespaces: %s' % (function_str, str(namespaces)))
    except Exception as err:
        current_logger.error('error :: %s request no namespaces argument passed, err: %s' % (
            function_str, err))
        return 400
    if not namespaces:
        current_logger.error('error :: %s request no namespaces argument passed, err: %s' % (
            function_str, err))
        return 400

    err = 'None'
    from_timestamp = None
    try:
        from_timestamp = request.args.get('from_timestamp')
    except Exception as err:
        err = err
        from_timestamp = None
    if ':' in from_timestamp:
        new_from_timestamp = time.mktime(datetime.datetime.strptime(from_timestamp, '%Y-%m-%d %H:%M').timetuple())
        from_timestamp = int(new_from_timestamp)
    try:
        from_timestamp = int(from_timestamp)
    except:
        from_timestamp = None
    if not from_timestamp:
        current_logger.error('error :: %s request no from_timestamp argument passed, err: %s' % (
            function_str, err))
        return 400

    err = 'None'
    until_timestamp = None
    try:
        until_timestamp = request.args.get('until_timestamp')
    except Exception as err:
        err = err
        until_timestamp = None
    if ':' in until_timestamp:
        new_until_timestamp = time.mktime(datetime.datetime.strptime(until_timestamp, '%Y-%m-%d %H:%M').timetuple())
        until_timestamp = int(new_until_timestamp)
    try:
        until_timestamp = int(until_timestamp)
    except:
        until_timestamp = None
    if not until_timestamp:
        current_logger.error('error :: %s request no until_timestamp argument passed, err: %s' % (
            function_str, err))
        return 400

    abs_threshold = 0.5
    try:
        abs_threshold_str = request.args.get('abs_threshold')
        if abs_threshold_str:
            abs_threshold = float(abs_threshold_str)
    except Exception as err:
        current_logger.error('error :: %s request no valid abs_threshold argument passed, err: %s' % (
            function_str, err))
        return 400

    err = 'None'
    max_results = 0
    try:
        if 'max_results' in request.args:
            max_results = int(float(request.args.get('max_results')))
    except Exception as err:
        current_logger.error('error :: %s request invalid max_results argument passed, err: %s' % (
            function_str, err))
        return 400

    plot_graphs = False
    try:
        if 'plot_graphs' in request.args:
            plot_graphs_str = request.args.get('plot_graphs')
            if plot_graphs_str == 'true':
                plot_graphs = True
    except Exception as err:
        current_logger.error('error :: %s request invalid plot_graphs argument passed, err: %s' % (
            function_str, err))

    labelled_metric_name = None
    if metric.startswith('labelled_metrics.'):
        labelled_metric_name = str(metric)
        try:
            base_name = get_base_name_from_labelled_metrics_name(current_skyline_app, labelled_metric_name)
            if base_name:
                metric = str(base_name)
        except Exception as err:
            current_logger.error('error :: %s :: get_base_name_from_labelled_metrics_name failed for %s, err: %s' % (
                function_str, metric, err))

    err = 'None'
    progress_url = None
    try:
        progress_url = request.args.get('progress_url')
        current_logger.info('%s :: with progress_url: %s' % (function_str, str(progress_url)))
    except Exception as err:
        err = err
        progress_url = None

    labelled_metric_name = None
    if metric.startswith('labelled_metrics.'):
        labelled_metric_name = str(metric)
        try:
            base_name = get_base_name_from_labelled_metrics_name(current_skyline_app, labelled_metric_name)
            if base_name:
                metric = str(base_name)
        except Exception as err:
            current_logger.error('error :: %s :: get_base_name_from_labelled_metrics_name failed for %s, err: %s' % (
                function_str, metric, err))

    base_names_with_ids = {}
    ids_with_base_names = {}
    try:
        base_names_with_ids = get_base_names_and_metric_ids(current_skyline_app)
    except Exception as err:
        logger.error('error :: %s :: %s :: get_base_names_and_metric_ids failed - %s' % (
            current_skyline_app, function_str, err))
    if base_names_with_ids:
        for base_name in list(base_names_with_ids.keys()):
            metric_id = int(str(base_names_with_ids[base_name]))
            ids_with_base_names[metric_id] = base_name

    base_names = list(base_names_with_ids.keys())
    correlate_metrics = []
    errs = []
    for base_name in base_names:
        pattern_match = False
        try:
            pattern_match, matched_by = matched_or_regexed_in_list(current_skyline_app, base_name, namespaces)
        except Exception as err:
            errs.append(['matched_or_regexed_in_list', base_name, err])
        if pattern_match:
            correlate_metrics.append(base_name)
    current_logger.info('%s :: determined %s metrics to correlate from namespaces: %s' % (
        function_str, str(len(correlate_metrics)), str(namespaces)))

    if len(correlate_metrics) > 20:
        current_logger.info('%s :: would offload to wind due to the number of metrics to correlate' % function_str)

    metric_timeseries = []
    try:
        if not labelled_metric_name:
            metric_timeseries = get_graphite_metric(current_skyline_app, metric, from_timestamp, until_timestamp, 'list', 'object')
        else:
            metric_timeseries = get_victoriametrics_metric(current_skyline_app, metric, from_timestamp, until_timestamp, 'list', 'object')
    except Exception as err:
        current_logger.error('error :: %s failed to get metric time series, err: %s' % (
            function_str, err))
        return 400

    metric_timeseries_length = len(metric_timeseries)

    if metric_timeseries_length > 2000:
        current_logger.info('%s :: would offload to wind because of long time series, metric_timeseries_length: %s' % (
            function_str, str(metric_timeseries_length)))

    metrics_timeseries_use_key = 'timeseries'
    start_correlations = time.time()
    metrics_timeseries = {metric: {'timeseries': metric_timeseries}}
    for base_name in correlate_metrics:
        metric2_timeseries = []
        try:
            if '{' in base_name and base_name.endswith('}'):
                metric2_timeseries = get_victoriametrics_metric(current_skyline_app, base_name, from_timestamp, until_timestamp, 'list', 'object')
            else:
                metric2_timeseries = get_graphite_metric(current_skyline_app, base_name, from_timestamp, until_timestamp, 'list', 'object')
        except Exception as err:
            current_logger.error('error :: %s failed to get metric time series, err: %s' % (
                function_str, err))
            continue
        if not metric2_timeseries:
            continue
        metrics_timeseries[base_name] = {'timeseries': metric2_timeseries}

    try:
        resolution = determine_data_frequency(current_skyline_app, metric_timeseries, True)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: determine_data_frequency failed, err: %s' % (
            function_str, err))

    downsample = None
    use_resolution = int(resolution)
    if resolution < 600 and metric_timeseries_length > 200:
        downsample = 600
        use_resolution = int(downsample)
        current_logger.info('%s :: downsampling to %s from %s second resolution and %s data points' % (
            function_str, str(downsample), str(resolution), str(metric_timeseries_length)))

    all_aligned_timestamps = []
    for i_metric in list(metrics_timeseries.keys()):
        i_metric_timeseries = metrics_timeseries[i_metric]['timeseries']
        aligned_metric_timeseries = []
        downsampled_timeseries = []
        if downsample:
            try:
                downsampled_timeseries = downsample_timeseries(current_skyline_app, i_metric_timeseries, resolution, downsample, 'mean', 'end')
            except Exception as err:
                current_logger.error('error :: %s :: downsample_timeseries failed, err: %s' % (
                    function_str, err))
            metrics_timeseries[i_metric]['downsampled_timeseries'] = downsampled_timeseries
            i_metric_timeseries = list(downsampled_timeseries)
        for t, v in i_metric_timeseries:
            aligned_timestamp = int(t // use_resolution * use_resolution)
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
        ct = ct + use_resolution
    aligned_timestamps.append(ct)
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
        aligned_filled_timeseries = []
        for t in unique_aligned_timestamps:
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
            aligned_filled_timeseries.append([t, data_v])
        try:
            metrics_timeseries[i_metric]['aligned_filled_timeseries'] = aligned_filled_timeseries
            if i_metric == metric:
                metric_timeseries = list(aligned_filled_timeseries)
            current_logger.debug('%s :: metric: %s, len(aligned_filled_timeseries): %s' % (
                function_str, str(i_metric), len(aligned_filled_timeseries)))
        except Exception as err:
            current_logger.error('error :: %s :: aligned_filled_timeseries failed on %s, err: %s' % (
                function_str, i_metric, err))
        metrics_timeseries_use_key = 'aligned_filled_timeseries'

    to_process_count = len(list(metrics_timeseries.keys()))
    processed_count = 0
    callbacks_made = []

    for base_name in list(metrics_timeseries.keys()):
        processed_count += 1
        pearson_cc = None
        reason = None
        try:
            pearson_cc, reason, results = pearson_closest(current_skyline_app, metric, base_name, from_timestamp=from_timestamp, until_timestamp=until_timestamp, downsample=None, datapoints=None, print_debug=False, return_results=True, return_timeseries=False, metric_timeseries=metric_timeseries, metrics_timeseries=metrics_timeseries, metrics_timeseries_use_key=metrics_timeseries_use_key)
        except Exception as err:
            current_logger.error('error :: %s :: pearson_closest failed to correlate %s with %s, err: %s' % (
                function_str, metric, base_name, err))
            reason = err
        if isinstance(pearson_cc, float): 
            pearson_correlations[base_name] = {'pearson_cc': pearson_cc}
            if np.isnan(pearson_cc):
                pearson_correlations[base_name] = {'pearson_cc': None}
        else:
            pearson_correlations[base_name] = {'pearson_cc': None, 'reason': reason}
        pearson_correlations[base_name]['resolution'] = resolution
        pearson_correlations[base_name]['downsample'] = downsample
        pearson_correlations[base_name]['used_resolution'] = use_resolution
        if progress_url:
            proportion_completed = round(float(processed_count)/float(to_process_count), 1)
            if proportion_completed in [0.1, 0.2, 0.5, 0.7]:
                if proportion_completed not in callbacks_made:
                    use_progress_url = '%s?proportion_completed=%s' % (
                        progress_url, str(proportion_completed))
                    try:
                        r = requests.get(use_progress_url, timeout=1)
                        callbacks_made.append(proportion_completed)
                        current_logger.info('%s :: updated progress_url: %s, got status code: %s' % (
                            function_str, str(use_progress_url), str(r.status_code)))
                    except Exception as err:
                        current_logger.error('error :: %s :: request failed, progress_url: %s, err: %s' % (
                            function_str, use_progress_url, err))

    current_logger.info('%s :: %s correlations done, took %2f seconds' % (
        function_str, str(len(pearson_correlations)), (time.time() - start_correlations)))

    sorted_pearson_correlations = {}
    pearson_ccs = []
    pearson_ccs_none = []
    if pearson_correlations:
        metrics = list(pearson_correlations.keys())
        for i_metric in metrics:
            if pearson_correlations[i_metric]['pearson_cc'] is None:
                if abs_threshold:
                    del pearson_correlations[i_metric]
                    continue
                pearson_correlations[i_metric]['abs_pearson_cc'] = None
                pearson_ccs_none.append(i_metric)
                continue
            abs_pearson_cc = pearson_correlations[i_metric]['pearson_cc']
            if pearson_correlations[i_metric]['pearson_cc'] < 0:
                abs_pearson_cc = float(pearson_correlations[i_metric]['pearson_cc'] * -1)
            if abs_threshold:
                if abs_pearson_cc < abs_threshold:
                    del pearson_correlations[i_metric]
                    continue
            pearson_ccs.append([i_metric, abs_pearson_cc])
            pearson_correlations[i_metric]['abs_pearson_cc'] = abs_pearson_cc
        sorted_pearson_ccs = sorted(pearson_ccs, key=lambda x: x[1], reverse=True)
        for i_metric, abs_pearson_cc in sorted_pearson_ccs:
            sorted_pearson_correlations[i_metric] = dict(pearson_correlations[i_metric])
        for i_metric in pearson_ccs_none:
            sorted_pearson_correlations[i_metric] = dict(pearson_correlations[i_metric])
        pearson_correlations = sorted_pearson_correlations
    max_pearson_correlations = {}
    if pearson_correlations and max_results:
        results_added = 0
        for i_metric, corr_dict in pearson_correlations.items():
            if results_added == max_results:
                break
            max_pearson_correlations[i_metric] = pearson_correlations[i_metric]
    if max_pearson_correlations:
        pearson_correlations = copy.deepcopy(max_pearson_correlations)

    if pearson_correlations and plot_graphs:
        output_dir = '%s/%s/%s/pearson_correlation_graphs' % (
            settings.SKYLINE_TMP_DIR, str(until_timestamp),
            str(metric_id))
        try:
            pearson_correlation_graphs, graph_pearson_correlations = get_pearson_correlation_graphs(
                skyline_app, metric, until_timestamp,
                list(pearson_correlations.keys()), output_dir, tz='UTC',
                metrics_timeseries=metrics_timeseries, metrics_timeseries_use_key=metrics_timeseries_use_key,
                pearson_correlations=pearson_correlations, abs_threshold=abs_threshold)
        except Exception as err:
            logger.info(traceback.format_exc())
            logger.error('error :: get_pearson_correlation_graphs failed, err: %s' % err)
        if graph_pearson_correlations:
            pearson_correlations = copy.deepcopy(graph_pearson_correlations)
    if progress_url:
        use_progress_url = '%s?proportion_completed=1' % progress_url
        try:
            r = requests.get(use_progress_url, timeout=1)
        except Exception as err:
            current_logger.error('error :: %s :: request failed, progress_url: %s, err: %s' % (
                function_str, use_progress_url, err))

    return pearson_correlations
